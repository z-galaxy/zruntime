//! A mutual-exclusion lock that a future can hold across an await point.
//!
//! [`Mutex`] keeps a value that one task at a time may use, and hands it out through a
//! [`MutexGuard`]. What the locks of this crate do and do not promise is said once, in the
//! [module documentation](super).

use std::{
    cell::UnsafeCell,
    fmt,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

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
/// use futures_lite::future::block_on;
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
    /// Whether a guard exists.
    ///
    /// Taken with an `Acquire` compare-exchange and given back with a `Release` store, so that a
    /// holder sees what the one before it did to the value. A release between a `lock`'s check
    /// and its wait is not lost: `lock` takes its listener before its second `try_lock`, as
    /// `Event` asks of its callers, so the release's `notify` either reaches that listener or
    /// came before it was taken, and then the second `try_lock` sees the release.
    locked: AtomicBool,
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
    /// use futures_lite::future::block_on;
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
            locked: AtomicBool::new(false),
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
    /// A lock that is free is taken at once, whichever tasks are waiting for it already: see
    /// [no fairness](crate::lock#no-fairness).
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
    /// use futures_lite::future::block_on;
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
        loop {
            if let Some(guard) = self.try_lock() {
                return guard;
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.unlocked.listen();
            if let Some(guard) = self.try_lock() {
                return guard;
            }
            listener.await;
        }
    }

    /// Acquires the lock if nobody holds it, without waiting.
    ///
    /// Returns `None` while the mutex is held. A lock that is free is taken even while tasks wait
    /// for it, as [`lock`](Mutex::lock) takes it: see [no fairness](crate::lock#no-fairness).
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
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;

        Some(MutexGuard(self))
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
/// too, and dropping it releases the mutex and wakes a task waiting for it, if there is one.
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
        // SAFETY: the guard exists, so `locked` is set and only this guard clears it, and the
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
        self.0.locked.store(false, Ordering::Release);
        // A notification whose listener is dropped before polling it is passed on to the next
        // listener, so a `lock` future abandoned after being woken strands nobody behind it.
        self.0.unlocked.notify(1);
    }
}
