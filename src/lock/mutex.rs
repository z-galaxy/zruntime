//! A mutual-exclusion lock that a future can hold across an await point.
//!
//! [`Mutex`] keeps a value that one task at a time may use, and hands it out through a
//! [`MutexGuard`], or through a [`MutexGuardArc`] that holds an `Arc` of the mutex rather than a
//! borrow of it. What the locks of this crate do and do not promise is said once, in the
//! [module documentation](super).

use std::{
    cell::UnsafeCell,
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::WaitStart;
use crate::Event;

/// A mutual-exclusion lock whose `lock` future waits without blocking the thread.
///
/// A mutex keeps a value that one task at a time may use, through the [`MutexGuard`] that
/// [`lock`](Mutex::lock) and [`try_lock`](Mutex::try_lock) hand out and that releases the mutex
/// when it is dropped.
///
/// The mutex is built on [`Event`] and needs no runtime: it works under any executor, and may be
/// locked from any thread. Like [`std::sync::Mutex`], it is `Send` and `Sync` wherever `T` is
/// `Send`.
///
/// A panic while a guard is held does not poison the mutex, and tasks waiting for it are not served
/// in the order they came: the [module documentation](crate::lock#no-poisoning) says what both
/// mean.
///
/// # Example
///
/// ```
/// use futures::executor::block_on;
/// use zruntime::lock::Mutex;
///
/// let mutex = Mutex::new(Vec::new());
///
/// block_on(async {
///     // Each guard is dropped at the end of its statement, which releases the lock again.
///     mutex.lock().await.push("first");
///     mutex.lock().await.push("second");
/// });
///
/// assert_eq!(mutex.into_inner(), ["first", "second"]);
/// ```
pub struct Mutex<T>
where
    T: ?Sized,
{
    /// Whether a guard exists, in the [`LOCKED`] bit, and how many waiting `lock` calls hold
    /// newcomers back, in multiples of [`STARVED`].
    ///
    /// Taken with a compare-exchange from `0` by a newcomer, which therefore fails while a waiter
    /// holds newcomers back, and with a `fetch_or` by a `lock` that has waited; either is
    /// `Acquire` where it takes the lock. Given back with a `SeqCst` `fetch_sub`, whose release
    /// half has the next holder see what the one before it did to the value. A release between a
    /// `lock`'s check and its wait is not lost: `lock` takes its listener before each check that
    /// it waits after, as `Event` asks of its callers, so the release's notification either
    /// reaches that listener or came before it was taken, and then the check sees the release.
    /// The release and the checks that fail are `SeqCst` so that the event orders them against its
    /// own `SeqCst` accesses, with no fence, as its `listen_unfenced` and `notify_unfenced` ask.
    ///
    /// A starved waiter counts itself in with a `Relaxed` `fetch_add`: every change to the state
    /// is a read-modify-write, so a newcomer's compare-exchange that succeeds sees the latest
    /// count, and a newcomer whose compare-exchange fails waits as any waiter does. The waiter
    /// counts itself out with a `SeqCst` `fetch_sub`, followed by a notification where that
    /// leaves the mutex free, as [`Starved`] says: like a release, that is a change that a
    /// newcomer's failed check may have missed, and the event orders the two in the same way.
    state: AtomicUsize,
    unlocked: Event,
    value: UnsafeCell<T>,
}

// SAFETY: through a shared reference, which is all that sharing the mutex hands another thread, the
// value is reachable through a guard alone (`get_mut` and `into_inner` need the mutex borrowed
// mutably or owned), and at most one guard exists at a time. Sharing the mutex therefore only ever
// lets threads take turns with the value, which is what `Send` allows.
unsafe impl<T> Sync for Mutex<T> where T: ?Sized + Send {}

impl<T> Mutex<T> {
    /// A new mutex holding `value`, unlocked.
    ///
    /// This is a `const fn`, so a mutex can be a `static`.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::executor::block_on;
    /// use zruntime::lock::Mutex;
    ///
    /// static COUNT: Mutex<u32> = Mutex::new(0);
    ///
    /// *block_on(COUNT.lock()) += 1;
    ///
    /// assert_eq!(*block_on(COUNT.lock()), 1);
    /// ```
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            unlocked: Event::new(),
            value: UnsafeCell::new(value),
        }
    }

    /// Consumes the mutex, handing back its value.
    ///
    /// The mutex is moved, so no guard can be left to keep the value from going.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Mutex;
    ///
    /// let mutex = Mutex::new(String::from("kept"));
    ///
    /// assert_eq!(mutex.into_inner(), "kept");
    /// ```
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T> Mutex<T>
where
    T: ?Sized,
{
    /// Acquires the lock, waiting for the current holder to release it.
    ///
    /// The guard that comes out releases the lock when it is dropped.
    ///
    /// A lock that is free is taken at once, whichever tasks are waiting for it already, unless
    /// one of them has waited for a while: see [fairness](crate::lock#fairness).
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, and no
    /// other task waiting for it is left stranded.
    ///
    /// # Example
    ///
    /// A second thread that waits for a lock this thread holds, and sees what this thread did to
    /// the value once it lets go:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures::executor::block_on;
    /// use zruntime::lock::Mutex;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    /// let mut guard = block_on(mutex.lock());
    /// let waiter = thread::spawn({
    ///     let mutex = mutex.clone();
    ///     move || *block_on(mutex.lock())
    /// });
    ///
    /// *guard = 5;
    /// drop(guard);
    ///
    /// assert_eq!(waiter.join().expect("the other thread did not panic"), 5);
    /// ```
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        self.acquire(|| MutexGuard(self)).await
    }

    /// Acquires the lock if nobody holds it, without waiting.
    ///
    /// Returns `None` while the mutex is held, and while a task that has waited for it for a while
    /// holds newcomers back. Other tasks waiting for it do not count: a lock that is free is taken
    /// ahead of them, as [`lock`](Mutex::lock) takes it. See [fairness](crate::lock#fairness).
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Mutex;
    ///
    /// let mutex = Mutex::new(1);
    ///
    /// let guard = mutex.try_lock().expect("nobody holds a new mutex");
    /// assert!(mutex.try_lock().is_none());
    ///
    /// drop(guard);
    /// assert!(mutex.try_lock().is_some());
    /// ```
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        self.try_acquire().then(|| MutexGuard(self))
    }

    /// Acquires the lock, waiting for the current holder to release it, and hands out a guard
    /// that holds an `Arc` of the mutex rather than a borrow of it.
    ///
    /// This waits, and treats the other tasks waiting for the mutex, exactly as
    /// [`lock`](Mutex::lock) does; only the guard differs. A [`MutexGuardArc`] keeps the mutex
    /// alive for as long as it lives, so it can be kept in a struct, or moved into a spawned task,
    /// with nothing to borrow the mutex from.
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, and no
    /// other task waiting for it is left stranded.
    ///
    /// # Example
    ///
    /// A guard moved onto a thread of its own, which takes only what borrows nothing:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures::executor::block_on;
    /// use zruntime::lock::Mutex;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    /// let mut guard = block_on(mutex.lock_arc());
    ///
    /// thread::spawn(move || *guard += 1)
    ///     .join()
    ///     .expect("the other thread did not panic");
    ///
    /// assert_eq!(*block_on(mutex.lock()), 1);
    /// ```
    pub async fn lock_arc(self: &Arc<Self>) -> MutexGuardArc<T> {
        self.acquire(|| MutexGuardArc(self.clone())).await
    }

    /// Acquires the lock if nobody holds it, without waiting, and hands out a guard that holds an
    /// `Arc` of the mutex rather than a borrow of it.
    ///
    /// Returns `None` exactly where [`try_lock`](Mutex::try_lock) does; only the guard differs, as
    /// [`lock_arc`](Mutex::lock_arc) says.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use zruntime::lock::Mutex;
    ///
    /// let mutex = Arc::new(Mutex::new(1));
    ///
    /// let guard = mutex.try_lock_arc().expect("nobody holds a new mutex");
    /// assert!(mutex.try_lock_arc().is_none());
    ///
    /// drop(guard);
    /// assert!(mutex.try_lock_arc().is_some());
    /// ```
    pub fn try_lock_arc(self: &Arc<Self>) -> Option<MutexGuardArc<T>> {
        self.try_acquire().then(|| MutexGuardArc(self.clone()))
    }

    /// The value, borrowed mutably.
    ///
    /// No guard can exist while the mutex is borrowed mutably, so this reaches the value without
    /// taking the lock.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Mutex;
    ///
    /// let mut mutex = Mutex::new(1);
    /// *mutex.get_mut() += 1;
    ///
    /// assert_eq!(mutex.into_inner(), 2);
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    /// Waits until this call holds the lock, and returns the guard that `guard` makes of it.
    ///
    /// The waiting of [`lock`](Mutex::lock) and [`lock_arc`](Mutex::lock_arc), which differ only
    /// in the guard `guard` makes once this call holds the lock. It makes the guard with no await
    /// in between, so a future that is dropped never leaves the lock taken with no guard to
    /// release it.
    async fn acquire<F, G>(&self, guard: F) -> G
    where
        F: FnOnce() -> G,
    {
        // When this call began to wait, once it has.
        let mut since = None;
        // Counts this call among the starved waiters once it is one, until the call ends.
        let mut starved = None;
        // The listener of the try that took the lock, or `None` where the first try did.
        let listener = loop {
            if self.try_acquire_as(since.is_some()) {
                break None;
            }
            if starved.is_none() && since.is_some_and(WaitStart::waited_long) {
                starved = Some(Starved::new(self));
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.unlocked.listen_unfenced();
            if self.try_acquire_as(since.is_some()) {
                break Some(listener);
            }
            since.get_or_insert_with(WaitStart::now);
            listener.await;
        };
        // The guard is made before the listener and the starved count are dropped. A listener
        // that was notified passes its notification on as it is dropped, and the event re-raises
        // a panic of the waker that wakes. A panic in a drop that runs as a function returns leaks
        // the value it returns, which would leave the lock taken with no guard to release it. Made
        // before the drops, the guard is still a local of this function when one panics, and the
        // unwinding drops it.
        let guard = guard();
        drop(listener);
        drop(starved);

        guard
    }

    /// Takes the mutex as a newcomer if nobody holds it and no starved waiter holds newcomers
    /// back, and tells whether it did: the try of [`try_lock`](Mutex::try_lock) and
    /// [`try_lock_arc`](Mutex::try_lock_arc).
    fn try_acquire(&self) -> bool {
        self.state
            .compare_exchange(0, LOCKED, Ordering::Acquire, Ordering::SeqCst)
            .is_ok()
    }

    /// Takes the mutex if nobody holds it, and tells whether it did: as a newcomer, held back by
    /// starved waiters, until the `lock` call trying has `waited`, and whether or not they hold
    /// newcomers back from then on.
    ///
    /// A newcomer that starved waiters hold back from a free mutex waits all the same, and is not
    /// stranded: a release notifies the event, and so does a starved waiter that stops counting
    /// while the mutex is free; a notified listener that is dropped passes its notification on;
    /// and a call that has waited takes a free mutex on each of its tries, the one after its wake
    /// and the one after it listens again.
    fn try_acquire_as(&self, waited: bool) -> bool {
        if !waited {
            return self.try_acquire();
        }

        self.state.fetch_or(LOCKED, Ordering::SeqCst) & LOCKED == 0
    }

    /// Releases the lock that a guard held, and wakes a task waiting for it, if there is one: what
    /// dropping a [`MutexGuard`] or a [`MutexGuardArc`] does.
    fn unlock(&self) {
        self.state.fetch_sub(LOCKED, Ordering::SeqCst);
        // A notification whose listener is dropped before polling it is passed on to the next
        // listener, so a `lock` future abandoned after being woken strands nobody behind it.
        self.unlocked.notify_unfenced(1);
    }
}

impl<T> Default for Mutex<T>
where
    T: Default,
{
    /// A new mutex holding the default value of `T`, unlocked.
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    /// A new mutex holding `value`, unlocked.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> fmt::Debug for Mutex<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("Mutex");
        // Formats through `try_lock`, so a free mutex that a starved waiter holds newcomers back
        // from prints as `<locked>`, as it does while a guard holds it.
        match self.try_lock() {
            Some(guard) => s.field("value", &&*guard),
            None => s.field("value", &format_args!("<locked>")),
        };

        s.finish()
    }
}

/// The guard of a [`Mutex`] that is held, through which its value is used.
///
/// Made by [`Mutex::lock`] and [`Mutex::try_lock`]. The guard dereferences to the value, mutably
/// too, and dropping it releases the mutex and wakes a task waiting for it, if there is one. A
/// guard that has to outlive a borrow of the mutex is a [`MutexGuardArc`].
///
/// A guard is `Send` wherever `T` is `Send`, and `Sync` wherever `T` is `Sync`, so holding one
/// across an await does not keep a future from moving between threads.
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuard<'a, T>(&'a Mutex<T>)
where
    T: ?Sized;

// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows.
unsafe impl<T> Sync for MutexGuard<'_, T> where T: ?Sized + Sync {}

impl<T> Deref for MutexGuard<'_, T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so `LOCKED` is set and only this guard clears it, and the
        // guard's borrow of the mutex rules out `get_mut` and `into_inner`: no other reference to
        // the cell's contents can be live.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T>
where
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, this guard is the only path to the cell's contents, and `&mut
        // self` rules out a second reference taken through the guard itself.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> fmt::Debug for MutexGuard<'_, T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for MutexGuard<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        self.0.unlock();
    }
}

/// The guard of a [`Mutex`] that is held, which holds an `Arc` of the mutex rather than a borrow of
/// it.
///
/// Made by [`Mutex::lock_arc`] and [`Mutex::try_lock_arc`]. It does what a [`MutexGuard`] does: it
/// dereferences to the value, mutably too, and dropping it releases the mutex and wakes a task
/// waiting for it, if there is one. Unlike a `MutexGuard`, it is not tied to a borrow of the mutex:
/// the `Arc` it holds keeps the mutex alive for as long as the guard lives, so the guard can be
/// kept in a struct, or moved into a spawned task or onto another thread.
///
/// A guard is `Send` wherever `T` is `Send`, and `Sync` wherever `T` is `Sync`, as a `MutexGuard`
/// is.
///
/// # Example
///
/// A guard kept in a struct, which holds the value for as long as the struct lives:
///
/// ```
/// use std::sync::Arc;
///
/// use futures::executor::block_on;
/// use zruntime::lock::{Mutex, MutexGuardArc};
///
/// /// A log that nothing else may write to while it is open.
/// struct OpenLog {
///     lines: MutexGuardArc<Vec<String>>,
/// }
///
/// let log = Arc::new(Mutex::new(Vec::new()));
/// let mut open = OpenLog {
///     lines: block_on(log.lock_arc()),
/// };
/// open.lines.push(String::from("opened"));
/// assert!(log.try_lock().is_none());
///
/// drop(open);
/// assert_eq!(*block_on(log.lock()), ["opened"]);
/// ```
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuardArc<T>(Arc<Mutex<T>>)
where
    T: ?Sized;

// The auto `Sync` of the guard would follow the `Arc`'s, which asks only for `T: Send`: that would
// let two threads share a guard of a `Cell` and use the cell at once through it.
//
// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows: nothing else of
// the guard, the `Arc` it holds included, is reachable through a shared reference to it.
unsafe impl<T> Sync for MutexGuardArc<T> where T: ?Sized + Sync {}

impl<T> Deref for MutexGuardArc<T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so `LOCKED` is set and only this guard clears it, and the
        // guard's clone of the `Arc` rules out `get_mut` and `into_inner`, which no shared `Arc`
        // can reach: no other reference to the cell's contents can be live.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> DerefMut for MutexGuardArc<T>
where
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, this guard is the only path to the cell's contents, and `&mut
        // self` rules out a second reference taken through the guard itself.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> fmt::Debug for MutexGuardArc<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for MutexGuardArc<T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        self.0.unlock();
    }
}

/// Counts a `lock` call among the starved waiters of its mutex for as long as it lives, holding
/// newcomers back: from when the call, having waited for a while, finds the mutex taken after a
/// release woke it, until the call ends, with the mutex taken or given up.
///
/// A call that took the mutex stops counting while it holds it, and its release wakes whoever its
/// count held back. A call that gives up has to wake one of them itself, where it leaves the mutex
/// free: its `lock` future drops its listener before this, so a notification the listener had is
/// passed on while the count still holds newcomers back, and a newcomer that listens in between,
/// finding the mutex free but not for it, would otherwise wait for good.
struct Starved<'a, T>(&'a Mutex<T>)
where
    T: ?Sized;

impl<'a, T> Starved<'a, T>
where
    T: ?Sized,
{
    fn new(mutex: &'a Mutex<T>) -> Self {
        // Cannot overflow: every starved waiter holds a listener in the mutex's event, and there
        // cannot be `usize::MAX / STARVED` of those.
        mutex.state.fetch_add(STARVED, Ordering::Relaxed);

        Self(mutex)
    }
}

impl<T> Drop for Starved<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        if self.0.state.fetch_sub(STARVED, Ordering::SeqCst) & LOCKED != 0 {
            return;
        }

        self.0.unlocked.notify_unfenced(1);
    }
}

/// The bit of [`Mutex::state`] that is set while a guard exists.
const LOCKED: usize = 1;
/// What a starved waiter adds to [`Mutex::state`], above the [`LOCKED`] bit.
const STARVED: usize = 2;
