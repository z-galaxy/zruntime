//! A readers-writer lock that a future can hold across an await point.
//!
//! [`RwLock`] keeps a value that any number of readers may share or one writer may use, and hands
//! it out through an [`RwLockReadGuard`] or an [`RwLockWriteGuard`]. What the locks of this crate
//! do and do not promise is said once, in the [module documentation](super).

use std::{
    cell::UnsafeCell,
    fmt,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    sync::{self, PoisonError},
};

use crate::Event;

/// A readers-writer lock whose `read` and `write` futures wait without blocking the thread.
///
/// The lock keeps a value that is either shared by any number of readers or used by one writer.
/// [`read`](RwLock::read) hands out an [`RwLockReadGuard`], which dereferences to the value and may
/// be held by many tasks at once; [`write`](RwLock::write) hands out an [`RwLockWriteGuard`], which
/// dereferences to it mutably and is held by one task alone, with no reader beside it.
///
/// The lock is built on [`Event`] and needs no runtime: it works under any executor, and may be
/// taken from any thread. Like [`std::sync::RwLock`], it is `Send` wherever `T` is `Send`, and
/// `Sync` wherever `T` is `Send` and `Sync`.
///
/// A panic while a guard is held does not poison the lock, and tasks waiting for it are not served
/// in the order they came: the [module documentation](crate::lock#no-poisoning) says what both
/// mean.
///
/// # Write preference
///
/// The lock prefers writers: while a writer waits for it, new readers wait too, so that a steady
/// stream of readers cannot keep the writer out for good. The other side of it is that writers
/// coming one after the other keep readers out for as long as they keep coming. A writer counts as
/// waiting from the first poll of its [`write`](RwLock::write) future until that future completes
/// or is dropped, so a `write` that is given up, by a timeout say, stops holding readers back: they
/// are let in again unless another writer holds or waits for the lock.
///
/// A task that holds a read guard must therefore not ask for another one. If a writer arrives in
/// between, the second [`read`](RwLock::read) waits for that writer, the writer waits for the first
/// guard, and the task holds the first guard until its second `read` is done: the three wait for
/// each other for good. [`try_read`](RwLock::try_read) never waits, so it cannot deadlock this way,
/// but it fails instead.
///
/// # Example
///
/// ```
/// use futures_lite::future::block_on;
/// use zruntime::lock::RwLock;
///
/// let lock = RwLock::new(0);
///
/// block_on(async {
///     *lock.write().await += 1;
///
///     assert_eq!(*lock.read().await, 1);
/// });
/// ```
pub struct RwLock<T>
where
    T: ?Sized,
{
    state: sync::Mutex<RwState>,
    readers_may_enter: Event,
    writer_may_enter: Event,
    value: UnsafeCell<T>,
}

// SAFETY: through a shared reference, which is all that sharing the lock hands another thread, the
// value is reachable through a guard alone (`get_mut` and `into_inner` need the lock borrowed
// mutably or owned), and the state admits either one writer or any number of readers. Sharing the
// lock therefore lets threads take turns with the value, which `Send` allows, and hold `&T` at the
// same time, which `Sync` allows.
unsafe impl<T> Sync for RwLock<T> where T: ?Sized + Send + Sync {}

impl<T> RwLock<T> {
    /// A new lock holding `value`, held by nobody.
    ///
    /// This is a `const fn`, so a lock can be a `static`.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::RwLock;
    ///
    /// static SETTING: RwLock<&str> = RwLock::new("off");
    ///
    /// *block_on(SETTING.write()) = "on";
    ///
    /// assert_eq!(*block_on(SETTING.read()), "on");
    /// ```
    pub const fn new(value: T) -> Self {
        Self {
            state: sync::Mutex::new(RwState {
                owner: Owner::Unlocked,
                writers_waiting: 0,
            }),
            readers_may_enter: Event::new(),
            writer_may_enter: Event::new(),
            value: UnsafeCell::new(value),
        }
    }

    /// Consumes the lock, handing back its value.
    ///
    /// The lock is moved, so no guard can be left to keep the value from going.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = RwLock::new(String::from("kept"));
    ///
    /// assert_eq!(lock.into_inner(), "kept");
    /// ```
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T> RwLock<T>
where
    T: ?Sized,
{
    /// Acquires shared access, waiting while a writer holds or waits for the lock.
    ///
    /// Any number of readers may hold the lock at once. The guard that comes out gives up its share
    /// when it is dropped.
    ///
    /// A task that holds a read guard must not call this again: see
    /// [write preference](RwLock#write-preference).
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, and no
    /// other task waiting for it is left stranded.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = RwLock::new(String::from("shared"));
    ///
    /// block_on(async {
    ///     let guard = lock.read().await;
    ///
    ///     assert_eq!(guard.len(), 6);
    /// });
    /// ```
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            if let Some(guard) = self.try_read() {
                return guard;
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.readers_may_enter.listen();
            if let Some(guard) = self.try_read() {
                return guard;
            }
            listener.await;
        }
    }

    /// Acquires exclusive access, waiting for every reader and writer to leave.
    ///
    /// While this future waits, it holds new readers back, so that a steady stream of readers
    /// cannot keep it out for good: see [write preference](RwLock#write-preference). The guard
    /// that comes out releases the lock when it is dropped.
    ///
    /// A lock that is free is taken at once, whichever tasks are waiting for it already: see
    /// [no fairness](crate::lock#no-fairness).
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, the future
    /// stops holding readers back, and no other task waiting for the lock is left stranded.
    ///
    /// # Example
    ///
    /// A second thread that waits for a lock this thread holds, and sees what this thread did to
    /// the value once it lets go:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(0));
    /// let mut guard = block_on(lock.write());
    /// let waiter = thread::spawn({
    ///     let lock = lock.clone();
    ///     move || *block_on(lock.read())
    /// });
    ///
    /// *guard = 5;
    /// drop(guard);
    ///
    /// assert_eq!(waiter.join().expect("the other thread did not panic"), 5);
    /// ```
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        let waiting = WaitingWriter::register(self);
        loop {
            if let Some(guard) = self.try_write() {
                waiting.granted();
                return guard;
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.writer_may_enter.listen();
            if let Some(guard) = self.try_write() {
                waiting.granted();
                return guard;
            }
            listener.await;
        }
    }

    /// Acquires shared access if no writer holds or waits for the lock, without waiting.
    ///
    /// Returns `None` while a writer holds the lock, and while one waits for it too: a waiting
    /// writer holds new readers back, as [write preference](RwLock#write-preference) says. Other
    /// readers holding the lock do not stop this, for any number share it. A writer counts as
    /// waiting from the first poll of its `write` future, even one that then takes a free lock at
    /// once, so this can fail on a lock nobody holds while a `write` is polled on another thread.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    ///
    /// let first = lock.try_read().expect("nobody holds a new lock");
    /// let second = lock.try_read().expect("readers share the lock");
    /// assert_eq!(*first + *second, 2);
    ///
    /// // Readers keep writers out, for as long as they hold the lock.
    /// assert!(lock.try_write().is_none());
    /// ```
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = lock(&self.state);
        if state.writers_waiting > 0 {
            return None;
        }
        state.owner = match state.owner {
            Owner::Writing => return None,
            Owner::Unlocked => Owner::Reading(NonZeroUsize::MIN),
            Owner::Reading(readers) => Owner::Reading(
                readers
                    .checked_add(1)
                    .expect("more readers than a usize counts"),
            ),
        };

        Some(RwLockReadGuard(self))
    }

    /// Acquires exclusive access if nobody holds the lock, without waiting.
    ///
    /// Returns `None` while a reader or a writer holds the lock. Writers waiting for it do not
    /// count: a lock that is free is taken ahead of them, as [`write`](RwLock::write) takes it (see
    /// [no fairness](crate::lock#no-fairness)), and one of them is woken once this guard is
    /// dropped.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    ///
    /// let mut guard = lock.try_write().expect("nobody holds a new lock");
    /// *guard = 2;
    /// assert!(lock.try_write().is_none());
    /// assert!(lock.try_read().is_none());
    ///
    /// drop(guard);
    /// assert_eq!(*lock.try_read().expect("the writer is gone"), 2);
    /// ```
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let mut state = lock(&self.state);
        if !matches!(state.owner, Owner::Unlocked) {
            return None;
        }
        state.owner = Owner::Writing;

        Some(RwLockWriteGuard(self))
    }

    /// The value, borrowed mutably.
    ///
    /// No guard can exist while the lock is borrowed mutably, so this reaches the value without
    /// taking the lock.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::RwLock;
    ///
    /// let mut lock = RwLock::new(1);
    /// *lock.get_mut() += 1;
    ///
    /// assert_eq!(lock.into_inner(), 2);
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }
}

impl<T> Default for RwLock<T>
where
    T: Default,
{
    /// A new lock holding the default value of `T`, held by nobody.
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    /// A new lock holding `value`, held by nobody.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> fmt::Debug for RwLock<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("RwLock");
        // Formats through `try_read`, so a lock that a writer merely waits for prints as
        // `<locked>`, as it does while a writer holds it.
        match self.try_read() {
            Some(guard) => s.field("value", &&*guard),
            None => s.field("value", &format_args!("<locked>")),
        };

        s.finish()
    }
}

/// Shared access to the value of an [`RwLock`], for as long as the guard lives.
///
/// Made by [`RwLock::read`] and [`RwLock::try_read`]. The guard dereferences to the value, and
/// dropping it gives up its share of the lock: once the last reader is gone, a writer waiting for
/// the lock is woken.
///
/// A guard is `Send` and `Sync` wherever `T` is `Sync`, so holding one across an await does not
/// keep a future from moving between threads.
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockReadGuard<'a, T>(&'a RwLock<T>)
where
    T: ?Sized;

// SAFETY: a read guard yields `&T` and nothing else, so handing one to another thread hands that
// thread a `&T`, which is what `Sync` allows.
unsafe impl<T> Send for RwLockReadGuard<'_, T> where T: ?Sized + Sync {}
// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows.
unsafe impl<T> Sync for RwLockReadGuard<'_, T> where T: ?Sized + Sync {}

impl<T> Deref for RwLockReadGuard<'_, T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the owner is `Reading`, which keeps every writer out, and
        // its borrow of the lock rules out `get_mut` and `into_inner`: only shared references to
        // the cell's contents can be live.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> fmt::Debug for RwLockReadGuard<'_, T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for RwLockReadGuard<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        let Owner::Reading(readers) = state.owner else {
            unreachable!("a read guard exists only while the owner is reading");
        };
        if let Some(readers) = NonZeroUsize::new(readers.get() - 1) {
            state.owner = Owner::Reading(readers);
            return;
        }
        state.owner = Owner::Unlocked;
        drop(state);

        self.0.writer_may_enter.notify(1);
    }
}

/// Exclusive access to the value of an [`RwLock`], for as long as the guard lives.
///
/// Made by [`RwLock::write`] and [`RwLock::try_write`]. The guard dereferences to the value,
/// mutably too, and dropping it releases the lock and wakes the readers and the writer waiting
/// for it, if there are any.
///
/// A guard is `Send` wherever `T` is `Send`, and `Sync` wherever `T` is `Sync`, so holding one
/// across an await does not keep a future from moving between threads.
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockWriteGuard<'a, T>(&'a RwLock<T>)
where
    T: ?Sized;

// SAFETY: the guard is the only path to the value while it exists, so handing it to another
// thread hands that thread the value, which is what `Send` allows.
unsafe impl<T> Send for RwLockWriteGuard<'_, T> where T: ?Sized + Send {}
// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows.
unsafe impl<T> Sync for RwLockWriteGuard<'_, T> where T: ?Sized + Sync {}

impl<T> Deref for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the owner is `Writing`, which keeps every other guard out,
        // and its borrow of the lock rules out `get_mut` and `into_inner`: nothing else can reach
        // the cell's contents.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, this guard is the only path to the cell's contents, and `&mut
        // self` rules out a second reference taken through the guard itself.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> fmt::Debug for RwLockWriteGuard<'_, T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        lock(&self.0.state).owner = Owner::Unlocked;
        // Who gets in next is settled by `try_read` and `try_write` on the state the waiters find
        // once they wake, so waking both sides can let nobody in early. A notification whose
        // listener is dropped before polling it is passed on to the next listener, so a `write`
        // future abandoned after being woken strands nobody behind it.
        self.0.readers_may_enter.notify(usize::MAX);
        self.0.writer_may_enter.notify(1);
    }
}

/// What every `read` and `write` decides on.
///
/// The blocking mutex around it is held for the few instructions that check and update it, never
/// across an await.
struct RwState {
    owner: Owner,
    writers_waiting: usize,
}

/// Who currently holds the lock, if anyone.
enum Owner {
    /// Nobody holds it.
    Unlocked,
    /// This many readers hold it.
    Reading(NonZeroUsize),
    /// One writer holds it.
    Writing,
}

/// Counts a `write` call as waiting for as long as its future lives, so that readers are held
/// back only while a writer really is waiting: a `write` dropped before it is granted stops
/// counting, and lets the readers it was holding back in again unless another writer holds or waits
/// for the lock.
struct WaitingWriter<'a, T>
where
    T: ?Sized,
{
    rwlock: &'a RwLock<T>,
    counted: bool,
}

impl<'a, T> WaitingWriter<'a, T>
where
    T: ?Sized,
{
    fn register(rwlock: &'a RwLock<T>) -> Self {
        lock(&rwlock.state).writers_waiting += 1;

        Self {
            rwlock,
            counted: true,
        }
    }

    fn granted(mut self) {
        self.counted = false;
        lock(&self.rwlock.state).writers_waiting -= 1;
    }
}

impl<T> Drop for WaitingWriter<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        if !self.counted {
            return;
        }
        let mut state = lock(&self.rwlock.state);
        state.writers_waiting -= 1;
        if state.writers_waiting > 0 || matches!(state.owner, Owner::Writing) {
            return;
        }
        drop(state);

        self.rwlock.readers_may_enter.notify(usize::MAX);
    }
}

/// The state behind the lock, taken whether or not a panic poisoned it.
///
/// Poisoning says nothing here: every path updates the state in a single assignment, so a panic
/// elsewhere cannot leave it half-updated.
fn lock(state: &sync::Mutex<RwState>) -> sync::MutexGuard<'_, RwState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
