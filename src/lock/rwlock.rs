//! A readers-writer lock that a future can hold across an await point.
//!
//! [`RwLock`] keeps a value that any number of readers may share or one writer may use, and hands
//! it out through an [`RwLockReadGuard`] or an [`RwLockWriteGuard`], or through an
//! [`RwLockReadGuardArc`] or an [`RwLockWriteGuardArc`], which hold an `Arc` of the lock rather
//! than a borrow of it. What the locks of this crate do and do not promise is said once, in the
//! [module documentation](super).

use std::{
    cell::UnsafeCell,
    fmt, mem,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    sync::{self, Arc, PoisonError},
};

use super::WaitStart;
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
/// coming one after the other would keep readers out for as long as they kept coming, so a reader
/// that has waited for a while is let in the next time no writer holds the lock, ahead of the
/// writers waiting for it: see [fairness](crate::lock#fairness). Until such a reader is in, no
/// writer takes the lock, even while it is free, so a reader whose task is slow to run keeps the
/// writers waiting, as a waiting writer keeps new readers waiting. A writer counts as waiting from
/// the first poll of its [`write`](RwLock::write) future until that future completes or is
/// dropped, so a `write` that is given up, by a timeout say, stops holding readers back: they are
/// let in again unless another writer holds or waits for the lock.
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
    /// Where readers wait, and `writer_may_enter` where writers wait. Both are listened to and
    /// notified without their fences, through `Event::listen_unfenced` and
    /// `Event::notify_unfenced`: every check of the state is made under its lock, and every change
    /// to it under that lock too, with the notification after, which the lock then orders.
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
                writers_starved: 0,
                readers_starved: 0,
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
    /// [write preference](RwLock#write-preference). A reader that has waited for a while is let in
    /// ahead of the writers waiting for the lock: see [fairness](crate::lock#fairness).
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
        self.acquire_read(|| RwLockReadGuard(self)).await
    }

    /// Acquires exclusive access, waiting for every reader and writer to leave.
    ///
    /// While this future waits, it holds new readers back, so that a steady stream of readers
    /// cannot keep it out for good: see [write preference](RwLock#write-preference). The guard
    /// that comes out releases the lock when it is dropped.
    ///
    /// A lock that is free is taken at once, whichever writers are waiting for it already, unless
    /// a task waiting for it has waited for a while: see [fairness](crate::lock#fairness).
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
        self.acquire_write(|| RwLockWriteGuard(self)).await
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
        self.try_acquire_read().then(|| RwLockReadGuard(self))
    }

    /// Acquires exclusive access if nobody holds the lock, without waiting.
    ///
    /// Returns `None` while a reader or a writer holds the lock, and while a task that has waited
    /// for it for a while holds newcomers back. Other writers waiting for it do not count: a lock
    /// that is free is taken ahead of them, as [`write`](RwLock::write) takes it (see
    /// [fairness](crate::lock#fairness)), and one of them is woken once this guard is dropped.
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
        self.try_acquire_write().then(|| RwLockWriteGuard(self))
    }

    /// Acquires shared access, waiting while a writer holds or waits for the lock, and hands out a
    /// guard that holds an `Arc` of the lock rather than a borrow of it.
    ///
    /// This waits, and treats the other tasks waiting for the lock, exactly as
    /// [`read`](RwLock::read) does, write preference included; only the guard differs. An
    /// [`RwLockReadGuardArc`] keeps the lock alive for as long as it lives, so it can be kept in a
    /// struct, or moved into a spawned task, with nothing to borrow the lock from.
    ///
    /// A task that holds a read guard of either kind must not call this, nor `read`, again: see
    /// [write preference](RwLock#write-preference).
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, and no
    /// other task waiting for it is left stranded.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(String::from("shared")));
    /// let guard = block_on(lock.read_arc());
    ///
    /// let len = thread::spawn(move || guard.len())
    ///     .join()
    ///     .expect("the other thread did not panic");
    ///
    /// assert_eq!(len, 6);
    /// ```
    pub async fn read_arc(self: &Arc<Self>) -> RwLockReadGuardArc<T> {
        self.acquire_read(|| RwLockReadGuardArc(self.clone())).await
    }

    /// Acquires shared access if no writer holds or waits for the lock, without waiting, and hands
    /// out a guard that holds an `Arc` of the lock rather than a borrow of it.
    ///
    /// Returns `None` exactly where [`try_read`](RwLock::try_read) does; only the guard differs, as
    /// [`read_arc`](RwLock::read_arc) says.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    ///
    /// let first = lock.try_read_arc().expect("nobody holds a new lock");
    /// let second = lock.try_read_arc().expect("readers share the lock");
    /// assert_eq!(*first + *second, 2);
    /// assert!(lock.try_write_arc().is_none());
    /// ```
    pub fn try_read_arc(self: &Arc<Self>) -> Option<RwLockReadGuardArc<T>> {
        self.try_acquire_read()
            .then(|| RwLockReadGuardArc(self.clone()))
    }

    /// Acquires exclusive access, waiting for every reader and writer to leave, and hands out a
    /// guard that holds an `Arc` of the lock rather than a borrow of it.
    ///
    /// This waits, holds new readers back while it does, and treats the other tasks waiting for
    /// the lock, exactly as [`write`](RwLock::write) does; only the guard differs. An
    /// [`RwLockWriteGuardArc`] keeps the lock alive for as long as it lives, so it can be kept in
    /// a struct, or moved into a spawned task, with nothing to borrow the lock from.
    ///
    /// Dropping the future before it completes gives up the wait. The lock is not taken, the future
    /// stops holding readers back, and no other task waiting for the lock is left stranded.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(0));
    /// let mut guard = block_on(lock.write_arc());
    ///
    /// thread::spawn(move || *guard += 1)
    ///     .join()
    ///     .expect("the other thread did not panic");
    ///
    /// assert_eq!(*block_on(lock.read()), 1);
    /// ```
    pub async fn write_arc(self: &Arc<Self>) -> RwLockWriteGuardArc<T> {
        self.acquire_write(|| RwLockWriteGuardArc(self.clone()))
            .await
    }

    /// Acquires exclusive access if nobody holds the lock, without waiting, and hands out a guard
    /// that holds an `Arc` of the lock rather than a borrow of it.
    ///
    /// Returns `None` exactly where [`try_write`](RwLock::try_write) does; only the guard differs,
    /// as [`write_arc`](RwLock::write_arc) says.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use zruntime::lock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    ///
    /// let guard = lock.try_write_arc().expect("nobody holds a new lock");
    /// assert!(lock.try_write_arc().is_none());
    /// assert!(lock.try_read_arc().is_none());
    ///
    /// drop(guard);
    /// assert!(lock.try_read_arc().is_some());
    /// ```
    pub fn try_write_arc(self: &Arc<Self>) -> Option<RwLockWriteGuardArc<T>> {
        self.try_acquire_write()
            .then(|| RwLockWriteGuardArc(self.clone()))
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

    /// Waits until this call holds a share of the lock, and returns the guard that `guard` makes
    /// of it.
    ///
    /// The waiting of [`read`](RwLock::read) and [`read_arc`](RwLock::read_arc), which differ only
    /// in the guard `guard` makes once this call holds the share. It makes the guard with no await
    /// in between, so a future that is dropped never leaves a share taken with no guard to give it
    /// up.
    async fn acquire_read<F, G>(&self, guard: F) -> G
    where
        F: FnOnce() -> G,
    {
        let mut reader = WaitingReader {
            rwlock: self,
            since: None,
            starved: false,
        };
        // The listener of the try that took the share, or `None` where the first try did.
        let listener = loop {
            if reader.try_read() {
                break None;
            }
            reader.lost();
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.readers_may_enter.listen_unfenced();
            if reader.try_read() {
                break Some(listener);
            }
            reader.since.get_or_insert_with(WaitStart::now);
            listener.await;
        };
        // The guard is made before the listener and the reader are dropped. A listener that was
        // notified passes its notification on as it is dropped, and the event re-raises a panic of
        // the waker that wakes. A panic in a drop that runs as a function returns leaks the value
        // it returns, which would leave the share taken with no guard to give it up. Made before
        // the drops, the guard is still a local of this function when one panics, and the
        // unwinding drops it.
        let guard = guard();
        drop(listener);
        drop(reader);

        guard
    }

    /// Waits until this call holds the lock alone, and returns the guard that `guard` makes of
    /// it.
    ///
    /// The waiting of [`write`](RwLock::write) and [`write_arc`](RwLock::write_arc), which differ
    /// only in the guard `guard` makes once this call holds the lock. It makes the guard with no
    /// await in between, so a future that is dropped never leaves the lock taken with no guard to
    /// release it.
    async fn acquire_write<F, G>(&self, guard: F) -> G
    where
        F: FnOnce() -> G,
    {
        let mut writer = WaitingWriter::register(self);
        // The listener of the try that took the lock, or `None` where the first try did.
        let listener = loop {
            if writer.try_write() {
                break None;
            }
            writer.lost();
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.writer_may_enter.listen_unfenced();
            if writer.try_write() {
                break Some(listener);
            }
            writer.since.get_or_insert_with(WaitStart::now);
            listener.await;
        };
        // The guard is made before the listener and the writer are dropped, for the reason given
        // in `acquire_read`: a panic in one of those drops would otherwise leak the guard, and
        // leave the lock taken with no guard to release it.
        let guard = guard();
        drop(listener);
        drop(writer);

        guard
    }

    /// Takes a share of the lock if no writer holds or waits for it, and tells whether it did: the
    /// try of [`try_read`](RwLock::try_read) and [`try_read_arc`](RwLock::try_read_arc).
    fn try_acquire_read(&self) -> bool {
        let mut state = lock(&self.state);

        state.writers_waiting == 0 && state.admit_reader()
    }

    /// Takes the lock if nobody holds it and no starved task holds newcomers back, and tells
    /// whether it did: the try of [`try_write`](RwLock::try_write) and
    /// [`try_write_arc`](RwLock::try_write_arc).
    fn try_acquire_write(&self) -> bool {
        let mut state = lock(&self.state);

        state.writers_starved == 0 && state.admit_writer()
    }

    /// Gives up the share of the lock that a read guard held, and wakes a writer waiting for it
    /// once the last reader is gone: what dropping an [`RwLockReadGuard`] or an
    /// [`RwLockReadGuardArc`] does.
    fn read_unlock(&self) {
        let mut state = lock(&self.state);
        let Owner::Reading(readers) = state.owner else {
            unreachable!("a read guard exists only while the owner is reading");
        };
        if let Some(readers) = NonZeroUsize::new(readers.get() - 1) {
            state.owner = Owner::Reading(readers);
            return;
        }
        state.owner = Owner::Unlocked;
        drop(state);

        self.writer_may_enter.notify_unfenced(1);
    }

    /// Releases the lock that a write guard held, and wakes the readers and the writer waiting for
    /// it: what dropping an [`RwLockWriteGuard`] or an [`RwLockWriteGuardArc`] does.
    fn write_unlock(&self) {
        lock(&self.state).owner = Owner::Unlocked;
        // Who gets in next is settled by the waiters' tries on the state they find once they wake,
        // so waking both sides can let nobody in early. A notification whose listener is dropped
        // before polling it is passed on to the next listener, so a `write` future abandoned after
        // being woken strands nobody behind it.
        self.readers_may_enter.notify_unfenced(usize::MAX);
        self.writer_may_enter.notify_unfenced(1);
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
/// the lock is woken. A guard that has to outlive a borrow of the lock is an
/// [`RwLockReadGuardArc`].
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
        self.0.read_unlock();
    }
}

/// Exclusive access to the value of an [`RwLock`], for as long as the guard lives.
///
/// Made by [`RwLock::write`] and [`RwLock::try_write`]. The guard dereferences to the value,
/// mutably too, and dropping it releases the lock and wakes the readers and the writer waiting
/// for it, if there are any. A guard that has to outlive a borrow of the lock is an
/// [`RwLockWriteGuardArc`].
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
        self.0.write_unlock();
    }
}

/// Shared access to the value of an [`RwLock`], for as long as the guard lives, through an `Arc` of
/// the lock rather than a borrow of it.
///
/// Made by [`RwLock::read_arc`] and [`RwLock::try_read_arc`]. It does what an [`RwLockReadGuard`]
/// does: it dereferences to the value, and dropping it gives up its share of the lock, waking a
/// writer waiting for the lock once the last reader is gone. Unlike an `RwLockReadGuard`, it is not
/// tied to a borrow of the lock: the `Arc` it holds keeps the lock alive for as long as the guard
/// lives, so the guard can be kept in a struct, or moved into a spawned task or onto another
/// thread.
///
/// A guard is `Send` wherever `T` is `Send` and `Sync`, which is what it takes for an `Arc` of the
/// lock to go to another thread: the guard holds one, and dropping it there can drop the last
/// `Arc`, and the value with it. It is `Sync` wherever `T` is `Send` and `Sync` too, through the
/// `Arc`, which is more than sharing a guard needs, for that shares a `&T` and nothing else. A
/// guard of a value that is `Sync` but not `Send` therefore cannot be shared by reference with
/// another thread, a scoped one say, as an `RwLockReadGuard` or a `MutexGuardArc` of it can.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use futures_lite::future::block_on;
/// use zruntime::lock::{RwLock, RwLockReadGuardArc};
///
/// /// A view of the settings that keeps them from changing while it lives.
/// struct Snapshot {
///     settings: RwLockReadGuardArc<Vec<&'static str>>,
/// }
///
/// let settings = Arc::new(RwLock::new(vec!["verbose"]));
/// let snapshot = Snapshot {
///     settings: block_on(settings.read_arc()),
/// };
/// assert!(settings.try_write().is_none());
/// assert_eq!(*snapshot.settings, ["verbose"]);
///
/// drop(snapshot);
/// assert!(settings.try_write().is_some());
/// ```
// `Send` and `Sync` are the auto traits' and follow the `Arc`, asking for `T: Send + Sync`. `Send`
// needs no less, as the guard's doc says. `Sync` could ask for `T: Sync` alone, as sharing the
// guard shares a `&T` and nothing more, but only through an unsafe impl, which the guard does
// without. What that costs is a guard of a value that is `Sync` but not `Send`: it cannot be shared
// by reference with another thread, such as a scoped one, as a borrowing guard or a `MutexGuardArc`
// of that value can be.
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockReadGuardArc<T>(Arc<RwLock<T>>)
where
    T: ?Sized;

impl<T> Deref for RwLockReadGuardArc<T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the owner is `Reading`, which keeps every writer out, and
        // its clone of the `Arc` rules out `get_mut` and `into_inner`, which no shared `Arc` can
        // reach: only shared references to the cell's contents can be live.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> fmt::Debug for RwLockReadGuardArc<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for RwLockReadGuardArc<T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        self.0.read_unlock();
    }
}

/// Exclusive access to the value of an [`RwLock`], for as long as the guard lives, through an `Arc`
/// of the lock rather than a borrow of it.
///
/// Made by [`RwLock::write_arc`] and [`RwLock::try_write_arc`]. It does what an
/// [`RwLockWriteGuard`] does: it dereferences to the value, mutably too, and dropping it releases
/// the lock and wakes the readers and the writer waiting for it, if there are any. Unlike an
/// `RwLockWriteGuard`, it is not tied to a borrow of the lock: the `Arc` it holds keeps the lock
/// alive for as long as the guard lives, so the guard can be kept in a struct, or moved into a
/// spawned task or onto another thread.
///
/// A guard is `Send` wherever `T` is `Send` and `Sync`, which is what it takes for an `Arc` of the
/// lock to go to another thread: the guard holds one, and dropping it there can drop the last
/// `Arc`, and the value with it. It is `Sync` wherever `T` is `Send` and `Sync` too, through the
/// `Arc`. Both are more than an `RwLockWriteGuard` asks, which is `Send` wherever `T` is `Send`
/// and `Sync` wherever `T` is `Sync`. So a scoped thread, say, can be given a borrowing guard of a
/// value that is `Send` but not `Sync`, or share one of a value that is `Sync` but not `Send`, and
/// can do neither with this guard.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use futures_lite::future::block_on;
/// use zruntime::lock::{RwLock, RwLockWriteGuardArc};
///
/// /// An edit of a document, which nobody may read until it is done.
/// struct Edit {
///     text: RwLockWriteGuardArc<String>,
/// }
///
/// let document = Arc::new(RwLock::new(String::from("draft")));
/// let mut edit = Edit {
///     text: block_on(document.write_arc()),
/// };
/// edit.text.push_str(", revised");
/// assert!(document.try_read().is_none());
///
/// drop(edit);
/// assert_eq!(*block_on(document.read()), "draft, revised");
/// ```
// `Send` and `Sync` are the auto traits' and follow the `Arc`, asking for `T: Send + Sync`. `Send`
// could ask for `T: Send` alone, as an `RwLockWriteGuard` does, since the guard hands out nothing
// of the `Arc` it holds, and `Sync` for `T: Sync` alone, as sharing the guard shares a `&T` and
// nothing more, but only through unsafe impls, which the guard does without. What that costs is,
// with a scoped thread say, that a guard of a value that is `Send` but not `Sync` cannot be moved
// to it, nor one of a value that is `Sync` but not `Send` be shared with it by reference, as a
// borrowing guard of such a value can.
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockWriteGuardArc<T>(Arc<RwLock<T>>)
where
    T: ?Sized;

impl<T> Deref for RwLockWriteGuardArc<T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the owner is `Writing`, which keeps every other guard out,
        // and its clone of the `Arc` rules out `get_mut` and `into_inner`, which no shared `Arc`
        // can reach: nothing else can reach the cell's contents.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuardArc<T>
where
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, this guard is the only path to the cell's contents, and `&mut
        // self` rules out a second reference taken through the guard itself.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> fmt::Debug for RwLockWriteGuardArc<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Drop for RwLockWriteGuardArc<T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        self.0.write_unlock();
    }
}

/// What every `read` and `write` decides on.
///
/// The blocking mutex around it is held for the few instructions that check and update it, never
/// across an await.
struct RwState {
    owner: Owner,
    /// The `write` calls that wait, from their first poll until they have the lock or give up.
    writers_waiting: usize,
    /// The waiting writers that hold newcomer writers back.
    writers_starved: usize,
    /// The waiting readers that are let in ahead of waiting writers, and hold every writer back.
    readers_starved: usize,
}

impl RwState {
    /// Counts one more reader in, unless a writer holds the lock.
    fn admit_reader(&mut self) -> bool {
        self.owner = match self.owner {
            Owner::Writing => return false,
            Owner::Unlocked => Owner::Reading(NonZeroUsize::MIN),
            Owner::Reading(readers) => Owner::Reading(
                readers
                    .checked_add(1)
                    .expect("more readers than a usize counts"),
            ),
        };

        true
    }

    /// Lets a writer in, unless someone holds the lock or a starved reader waits for it.
    fn admit_writer(&mut self) -> bool {
        if !matches!(self.owner, Owner::Unlocked) || self.readers_starved > 0 {
            return false;
        }
        self.owner = Owner::Writing;

        true
    }
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

/// An [`RwLock::read`] call.
///
/// Once it has waited for a while, and is woken only to find a writer holding the lock or waiting
/// for it, it is starved: it then enters whenever no writer holds the lock, and holds every writer
/// back until it has entered or given up.
struct WaitingReader<'a, T>
where
    T: ?Sized,
{
    rwlock: &'a RwLock<T>,
    /// When the call began to wait, if it has.
    since: Option<WaitStart>,
    /// Whether this reader counts in [`RwState::readers_starved`].
    starved: bool,
}

impl<T> WaitingReader<'_, T>
where
    T: ?Sized,
{
    /// Enters if no writer holds the lock, and, unless this reader is starved, none waits for it,
    /// and tells whether it did.
    fn try_read(&mut self) -> bool {
        let mut state = lock(&self.rwlock.state);
        if (!self.starved && state.writers_waiting > 0) || !state.admit_reader() {
            return false;
        }
        if mem::take(&mut self.starved) {
            state.readers_starved -= 1;
        }

        true
    }

    /// Records that this call could not enter, after a notification woke it if it has waited.
    ///
    /// A reader that has waited for long by then is starved from then on.
    fn lost(&mut self) {
        let Some(since) = self.since else {
            return;
        };
        if self.starved || !since.waited_long() {
            return;
        }
        self.starved = true;
        lock(&self.rwlock.state).readers_starved += 1;
    }
}

impl<T> Drop for WaitingReader<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        if !self.starved {
            return;
        }
        let mut state = lock(&self.rwlock.state);
        state.readers_starved -= 1;
        if state.readers_starved > 0 || !matches!(state.owner, Owner::Unlocked) {
            return;
        }
        drop(state);

        // A writer woken while starved readers held it back has gone back to waiting, for the last
        // of them to let go of the lock. None of them took it, so none will: wake a writer here.
        self.rwlock.writer_may_enter.notify_unfenced(1);
    }
}

/// Counts a `write` call as waiting for as long as its future lives, so that readers are held
/// back only while a writer really is waiting: a `write` dropped before it is granted stops
/// counting, and lets the readers it was holding back in again unless another writer holds or waits
/// for the lock.
///
/// Once it has waited for a while, and is woken only to find the lock taken, or held back by
/// starved readers, it is starved too: it then holds newcomer writers back until it has the lock or
/// gives up.
struct WaitingWriter<'a, T>
where
    T: ?Sized,
{
    rwlock: &'a RwLock<T>,
    /// Whether this writer counts in [`RwState::writers_waiting`].
    counted: bool,
    /// When the call began to wait, if it has.
    since: Option<WaitStart>,
    /// Whether this writer counts in [`RwState::writers_starved`].
    starved: bool,
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
            since: None,
            starved: false,
        }
    }

    /// Takes the lock if nobody holds it, no starved reader waits for it and, until this call has
    /// waited, no starved writer holds newcomers back, and tells whether it did.
    fn try_write(&mut self) -> bool {
        let mut state = lock(&self.rwlock.state);
        if (self.since.is_none() && state.writers_starved > 0) || !state.admit_writer() {
            return false;
        }
        self.counted = false;
        state.writers_waiting -= 1;
        if mem::take(&mut self.starved) {
            state.writers_starved -= 1;
        }

        true
    }

    /// Records that this call could not take the lock, after a notification woke it if it has
    /// waited.
    ///
    /// A writer that has waited for long by then is starved from then on.
    fn lost(&mut self) {
        let Some(since) = self.since else {
            return;
        };
        if self.starved || !since.waited_long() {
            return;
        }
        self.starved = true;
        lock(&self.rwlock.state).writers_starved += 1;
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
        if self.starved {
            state.writers_starved -= 1;
        }
        let wake_readers = state.writers_waiting == 0 && !matches!(state.owner, Owner::Writing);
        // The future drops its listener before this, so a notification the listener had is passed
        // on while this writer still holds newcomer writers back: one that listened in between,
        // finding the lock free but not for it, waits for this wake, with nobody else to wake it.
        let wake_writer = self.starved && matches!(state.owner, Owner::Unlocked);
        drop(state);

        if wake_readers {
            self.rwlock.readers_may_enter.notify_unfenced(usize::MAX);
        }
        if wake_writer {
            self.rwlock.writer_may_enter.notify_unfenced(1);
        }
    }
}

/// The state behind the lock, taken whether or not a panic poisoned it.
///
/// Poisoning says nothing here: no path can panic part of the way through its changes to the
/// state, so a panic elsewhere cannot leave it half-updated.
fn lock(state: &sync::Mutex<RwState>) -> sync::MutexGuard<'_, RwState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
