//! A semaphore: a lock that up to a set number of tasks may hold at once.
//!
//! [`Semaphore`] keeps a count of permits, and hands each one out through a [`SemaphoreGuard`], or
//! through a [`SemaphoreGuardArc`] that holds an `Arc` of the semaphore rather than a borrow of it.
//! What the locks of this crate do and do not promise is said once, in the
//! [module documentation](super).

use std::{
    fmt, mem,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::WaitStart;
use crate::Event;

/// A count of permits that tasks take and give back, whose `acquire` future waits without blocking
/// the thread.
///
/// A semaphore caps how many tasks do something at once: hold a connection, have a request in
/// flight, keep a file open. It starts with a number of permits, and
/// [`acquire`](Semaphore::acquire) and [`try_acquire`](Semaphore::try_acquire) each take one and
/// hand it out as a [`SemaphoreGuard`], which gives it back when it is dropped; `acquire` waits
/// while none is free. [`add_permits`](Semaphore::add_permits) adds permits as the program goes,
/// and [`SemaphoreGuard::forget`] keeps one out for good.
///
/// The semaphore is built on [`Event`] and needs no runtime: it works under any executor, and
/// permits may be taken from any thread. It keeps no value, so it is `Send` and `Sync`, and so are
/// its guards.
///
/// Tasks waiting for a permit are not served in the order they came: the
/// [module documentation](crate::lock#fairness) says what that means.
///
/// # Example
///
/// A semaphore that lets two tasks in at a time:
///
/// ```
/// use futures_lite::future::block_on;
/// use zruntime::lock::Semaphore;
///
/// let semaphore = Semaphore::new(2);
///
/// block_on(async {
///     let first = semaphore.acquire().await;
///     let _second = semaphore.acquire().await;
///     // Both permits are out, so a third task waits until one comes back.
///     assert!(semaphore.try_acquire().is_none());
///
///     drop(first);
///     let _third = semaphore.acquire().await;
/// });
/// ```
pub struct Semaphore {
    /// How many permits are free.
    ///
    /// Taken with a compare-exchange that is `Acquire` where it succeeds, so that the taker sees
    /// what the holder that gave the permit back did before it, through the release half of the
    /// `SeqCst` `fetch_add` that gave it back. Added to by `add_permits` with a `SeqCst`
    /// compare-exchange. A permit given back or added between an `acquire`'s check and its wait is
    /// not lost: `acquire` takes its listener before each check that it waits after, as `Event`
    /// asks of its callers, and every change that frees a permit notifies the event after it, so
    /// the notification either reaches that listener or came before it was taken, and then the
    /// check sees the change. The changes are `SeqCst`, and so are the checks that fail, which are
    /// the looks that find no permit free, whether a try's first look or the one a failed
    /// compare-exchange makes, so that the event orders the two against its own `SeqCst` accesses,
    /// with no fence, as its `listen_unfenced` and `notify_additional_unfenced` ask.
    ///
    /// The notifications add to the listeners notified already rather than count them. A counting
    /// notification reaches no new listener while one it reached has not run yet, so two permits
    /// given back in a row would wake one waiter and leave the next waiting beside a free permit.
    /// An additional one wakes one more listener for each permit, and a listener dropped with one
    /// passes it on as additional, so a waiter that gives up strands nobody.
    ///
    /// At most [`Semaphore::MAX_PERMITS`] where `new` or `add_permits` sets it, which keeps it
    /// from ever wrapping. A guard gives its permit back as it is dropped, which can report no
    /// error, so the count must have room for every permit that can come back, and the other half
    /// of what a `usize` counts is more than there can be: each guard takes at least four bytes of
    /// memory, on every target the standard library supports, so fewer than a quarter of what a
    /// `usize` counts can exist at once.
    permits: AtomicUsize,
    /// How many waiting `acquire` calls hold newcomers back, each counted from when, having waited
    /// for a while, it is woken only to find no permit free, until it ends, as [`Starved`] says.
    ///
    /// While it is not zero, a newcomer, which is an `acquire` that has not waited yet or a
    /// `try_acquire`, takes no permit, even a free one. A newcomer reads it with a `SeqCst` load
    /// before it looks at `permits`. The two are not one atomic step, so a newcomer may slip past
    /// a waiter that starts to count just between them, and take a permit it would otherwise have
    /// been held back from. Only newcomers that check at that very moment can, and every one that
    /// checks after it is held back, so the bound on how long newcomers keep a waiter waiting
    /// holds as it does for a mutex.
    ///
    /// A waiter counts itself in with a `SeqCst` `fetch_add`, which puts it in the one order of
    /// `SeqCst` operations ahead of the check of every newcomer that comes after it there. It
    /// counts itself out with a `SeqCst` `fetch_sub`, followed by a notification where that
    /// leaves permits free: like a release, that is a change that the failed check of a newcomer
    /// held back may have missed, and the event orders the two in the same way.
    starved: AtomicUsize,
    /// Notified as permits are given back or added, and as a starved waiter stops counting while
    /// permits are free.
    released: Event,
}

impl Semaphore {
    /// The most permits a semaphore can have free where [`new`](Semaphore::new) and
    /// [`add_permits`](Semaphore::add_permits) set the count: half of what a `usize` counts.
    ///
    /// A guard gives its permit back as it is dropped, which can report no error, so the count of
    /// free permits must have room for every permit that can come back: the other half is left
    /// for those. The count can go past this as guards give back permits that were out when it was
    /// set, and `add_permits` then panics if it is asked to add even one.
    pub const MAX_PERMITS: usize = usize::MAX >> 1;

    /// A new semaphore with `permits` free.
    ///
    /// This is a `const fn`, so a semaphore can be a `static`.
    ///
    /// # Panics
    ///
    /// Panics if `permits` is more than [`MAX_PERMITS`](Semaphore::MAX_PERMITS).
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::Semaphore;
    ///
    /// static DOWNLOADS: Semaphore = Semaphore::new(4);
    ///
    /// let _download = block_on(DOWNLOADS.acquire());
    /// ```
    pub const fn new(permits: usize) -> Self {
        assert!(
            permits <= Self::MAX_PERMITS,
            "more permits than a semaphore can have free"
        );

        Self {
            permits: AtomicUsize::new(permits),
            starved: AtomicUsize::new(0),
            released: Event::new(),
        }
    }

    /// Takes a permit, waiting while none is free.
    ///
    /// The guard that comes out gives the permit back when it is dropped.
    ///
    /// A permit that is free is taken at once, whichever tasks are waiting for one already, unless
    /// one of them has waited for a while: see [fairness](crate::lock#fairness).
    ///
    /// Dropping the future before it completes gives up the wait. No permit is taken, and no other
    /// task waiting for one is left stranded.
    ///
    /// # Example
    ///
    /// A second thread that waits for the only permit, which this thread holds, and gets it once
    /// this thread lets go:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Arc::new(Semaphore::new(1));
    /// let permit = block_on(semaphore.acquire());
    /// let waiter = thread::spawn({
    ///     let semaphore = semaphore.clone();
    ///     move || drop(block_on(semaphore.acquire()))
    /// });
    ///
    /// drop(permit);
    ///
    /// waiter.join().expect("the other thread did not panic");
    /// ```
    pub async fn acquire(&self) -> SemaphoreGuard<'_> {
        self.take(|| SemaphoreGuard(self)).await
    }

    /// Takes a permit if one is free, without waiting.
    ///
    /// Returns `None` while no permit is free, and while a task that has waited for one for a while
    /// holds newcomers back. Other tasks waiting for a permit do not count: a free permit is taken
    /// ahead of them, as [`acquire`](Semaphore::acquire) takes it. See
    /// [fairness](crate::lock#fairness).
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Semaphore::new(1);
    ///
    /// let permit = semaphore.try_acquire().expect("a new semaphore has its permit free");
    /// assert!(semaphore.try_acquire().is_none());
    ///
    /// drop(permit);
    /// assert!(semaphore.try_acquire().is_some());
    /// ```
    #[inline]
    pub fn try_acquire(&self) -> Option<SemaphoreGuard<'_>> {
        self.try_take_as(false).then(|| SemaphoreGuard(self))
    }

    /// Takes a permit, waiting while none is free, and hands out a guard that holds an `Arc` of
    /// the semaphore rather than a borrow of it.
    ///
    /// This waits, and treats the other tasks waiting for a permit, exactly as
    /// [`acquire`](Semaphore::acquire) does; only the guard differs. A [`SemaphoreGuardArc`] keeps
    /// the semaphore alive for as long as it lives, so it can be kept in a struct, or moved into a
    /// spawned task, with nothing to borrow the semaphore from.
    ///
    /// Dropping the future before it completes gives up the wait. No permit is taken, and no other
    /// task waiting for one is left stranded.
    ///
    /// # Example
    ///
    /// A permit moved onto a thread of its own, which takes only what borrows nothing, and given
    /// back there:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Arc::new(Semaphore::new(1));
    /// let permit = block_on(semaphore.acquire_arc());
    ///
    /// thread::spawn(move || drop(permit))
    ///     .join()
    ///     .expect("the other thread did not panic");
    ///
    /// assert!(semaphore.try_acquire().is_some());
    /// ```
    pub async fn acquire_arc(self: &Arc<Self>) -> SemaphoreGuardArc {
        self.take(|| SemaphoreGuardArc(Some(self.clone()))).await
    }

    /// Takes a permit if one is free, without waiting, and hands out a guard that holds an `Arc` of
    /// the semaphore rather than a borrow of it.
    ///
    /// Returns `None` exactly where [`try_acquire`](Semaphore::try_acquire) does; only the guard
    /// differs, as [`acquire_arc`](Semaphore::acquire_arc) says.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Arc::new(Semaphore::new(1));
    ///
    /// let permit = semaphore
    ///     .try_acquire_arc()
    ///     .expect("a new semaphore has its permit free");
    /// assert!(semaphore.try_acquire_arc().is_none());
    ///
    /// drop(permit);
    /// assert!(semaphore.try_acquire_arc().is_some());
    /// ```
    #[inline]
    pub fn try_acquire_arc(self: &Arc<Self>) -> Option<SemaphoreGuardArc> {
        self.try_take_as(false)
            .then(|| SemaphoreGuardArc(Some(self.clone())))
    }

    /// Adds `n` free permits, and wakes as many more tasks waiting for one.
    ///
    /// Adding none does nothing.
    ///
    /// # Panics
    ///
    /// Panics if that would leave more than [`MAX_PERMITS`](Semaphore::MAX_PERMITS) permits free.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Semaphore::new(0);
    /// assert!(semaphore.try_acquire().is_none());
    ///
    /// semaphore.add_permits(2);
    /// let _first = semaphore.try_acquire().expect("two permits were added");
    /// let _second = semaphore.try_acquire().expect("two permits were added");
    /// assert!(semaphore.try_acquire().is_none());
    /// ```
    pub fn add_permits(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut free = self.permits.load(Ordering::SeqCst);
        loop {
            let added = free
                .checked_add(n)
                .filter(|&added| added <= Self::MAX_PERMITS)
                .expect("more permits than a semaphore can have free");
            match self.permits.compare_exchange_weak(
                free,
                added,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(now) => free = now,
            }
        }

        self.released.notify_additional_unfenced(n);
    }

    /// Waits until this call holds a permit, and hands it out in the guard that `guard` makes.
    ///
    /// The waiting of [`acquire`](Semaphore::acquire) and
    /// [`acquire_arc`](Semaphore::acquire_arc), which differ only in the guard they have `guard`
    /// make. The guard is made as soon as the permit is taken, with no await in between, so a
    /// future that is dropped never leaves a permit taken with no guard to give it back. It is made
    /// before the listener and the count of this call among the starved waiters are dropped, too,
    /// as either drop may wake other tasks: a waker that panics there unwinds through the guard,
    /// which gives the permit back.
    async fn take<F, G>(&self, guard: F) -> G
    where
        F: FnOnce() -> G,
    {
        // When this call began to wait, once it has.
        let mut since = None;
        // Counts this call among the starved waiters once it is one, until the call ends.
        let mut starved = None;
        // The listener of the try that takes the permit, where that is the try after listening.
        let listener = loop {
            if self.try_take_as(since.is_some()) {
                break None;
            }
            if starved.is_none() && since.is_some_and(WaitStart::waited_long) {
                starved = Some(Starved::new(self));
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.released.listen_unfenced();
            if self.try_take_as(since.is_some()) {
                break Some(listener);
            }
            since.get_or_insert_with(WaitStart::now);
            listener.await;
        };

        // Made before the listener and `starved` are dropped, as either drop can wake a waker
        // that panics, and they are dropped by hand, not as this returns: the guard, a local then,
        // gives the permit back as the panic unwinds, where a value being returned is leaked.
        let guard = guard();
        drop(listener);
        drop(starved);

        guard
    }

    /// Takes a permit if one is free, and tells whether it did: as a newcomer, held back by
    /// starved waiters, until the `acquire` call trying has `waited`, and whether or not they hold
    /// newcomers back from then on.
    ///
    /// A newcomer that starved waiters hold back from a free permit waits all the same, and is not
    /// stranded: a starved waiter that stops counting while permits are free wakes as many tasks
    /// as there are free permits, as [`Starved`] says; every permit given back or added wakes one
    /// more; a notified listener that is dropped passes its notification on; and a call that has
    /// waited takes a free permit on each of its tries, the one after its wake and the one after
    /// it listens again.
    ///
    /// Inlined, as [`try_acquire`](Semaphore::try_acquire) and
    /// [`try_acquire_arc`](Semaphore::try_acquire_arc) are, into the crates that call those, so
    /// that taking a free permit makes no call, as taking a free `Mutex`, whose methods are
    /// generic, makes none. Giving it back is left out of line: its notification is a call
    /// either way.
    #[inline]
    fn try_take_as(&self, waited: bool) -> bool {
        if !waited && self.starved.load(Ordering::SeqCst) != 0 {
            return false;
        }

        // `Acquire` where it takes a permit, and `SeqCst` where it looks, the look that finds no
        // permit free among them.
        let mut free = self.permits.load(Ordering::SeqCst);
        loop {
            let Some(left) = free.checked_sub(1) else {
                return false;
            };
            match self.permits.compare_exchange_weak(
                free,
                left,
                Ordering::Acquire,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(now) => free = now,
            }
        }
    }

    /// Gives back the permit a guard held, and wakes one more task waiting for a permit, if there
    /// is one: what dropping a [`SemaphoreGuard`] or a [`SemaphoreGuardArc`] does.
    fn release(&self) {
        // Cannot wrap, as `permits` says.
        self.permits.fetch_add(1, Ordering::SeqCst);
        self.released.notify_additional_unfenced(1);
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Semaphore")
            .field("permits", &self.permits.load(Ordering::Relaxed))
            .finish()
    }
}

/// A permit taken from a [`Semaphore`], given back when the guard is dropped.
///
/// Made by [`Semaphore::acquire`] and [`Semaphore::try_acquire`]. Dropping the guard gives the
/// permit back and wakes a task waiting for one, if there is one;
/// [`forget`](SemaphoreGuard::forget) keeps the permit out for good instead. A guard that has to
/// outlive a borrow of the semaphore is a [`SemaphoreGuardArc`].
///
/// A guard is `Send` and `Sync`, by the auto traits alone: it stands for a permit and reaches no
/// value.
#[must_use = "if unused the Semaphore will immediately give the permit back"]
pub struct SemaphoreGuard<'a>(&'a Semaphore);

impl SemaphoreGuard<'_> {
    /// Drops the guard without giving its permit back, which leaves the semaphore with one permit
    /// fewer for good.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Semaphore::new(2);
    ///
    /// semaphore
    ///     .try_acquire()
    ///     .expect("a new semaphore has its permits free")
    ///     .forget();
    ///
    /// let _last = semaphore.try_acquire().expect("one permit is left");
    /// assert!(semaphore.try_acquire().is_none());
    /// ```
    pub fn forget(self) {
        // The guard holds nothing to drop but the permit its drop would give back.
        mem::forget(self);
    }
}

impl fmt::Debug for SemaphoreGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SemaphoreGuard").field(self.0).finish()
    }
}

impl Drop for SemaphoreGuard<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A permit taken from a [`Semaphore`], given back when the guard is dropped, which holds an `Arc`
/// of the semaphore rather than a borrow of it.
///
/// Made by [`Semaphore::acquire_arc`] and [`Semaphore::try_acquire_arc`]. It does what a
/// [`SemaphoreGuard`] does: dropping it gives the permit back and wakes a task waiting for one, if
/// there is one, and [`forget`](SemaphoreGuardArc::forget) keeps the permit out for good instead.
/// Unlike a `SemaphoreGuard`, it is not tied to a borrow of the semaphore: the `Arc` it holds
/// keeps the semaphore alive for as long as the guard lives, so the guard can be kept in a struct,
/// or moved into a spawned task or onto another thread.
///
/// A guard is `Send` and `Sync`, by the auto traits alone: it stands for a permit and reaches no
/// value.
///
/// # Example
///
/// A connection that holds one of a limited number of permits for as long as it is open:
///
/// ```
/// use std::sync::Arc;
///
/// use futures_lite::future::block_on;
/// use zruntime::lock::{Semaphore, SemaphoreGuardArc};
///
/// /// An open connection, one of as many as the semaphore has permits.
/// struct Connection {
///     _permit: SemaphoreGuardArc,
/// }
///
/// let slots = Arc::new(Semaphore::new(1));
/// let connection = Connection {
///     _permit: block_on(slots.acquire_arc()),
/// };
/// assert!(slots.try_acquire().is_none());
///
/// drop(connection);
/// assert!(slots.try_acquire().is_some());
/// ```
#[must_use = "if unused the Semaphore will immediately give the permit back"]
pub struct SemaphoreGuardArc(
    /// The semaphore, until `forget` takes it out so that the drop finds nothing to give back.
    Option<Arc<Semaphore>>,
);

impl SemaphoreGuardArc {
    /// Drops the guard without giving its permit back, which leaves the semaphore with one permit
    /// fewer for good.
    ///
    /// The guard lets go of its `Arc` of the semaphore all the same.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use zruntime::lock::Semaphore;
    ///
    /// let semaphore = Arc::new(Semaphore::new(1));
    ///
    /// semaphore
    ///     .try_acquire_arc()
    ///     .expect("a new semaphore has its permit free")
    ///     .forget();
    ///
    /// assert!(semaphore.try_acquire().is_none());
    /// assert_eq!(Arc::strong_count(&semaphore), 1);
    /// ```
    pub fn forget(mut self) {
        // Lets go of the `Arc` here, so that the drop that follows finds no permit to give back.
        self.0 = None;
    }
}

impl fmt::Debug for SemaphoreGuardArc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_tuple("SemaphoreGuardArc");
        // Only `forget` takes the `Arc` out, and nothing can print the guard after it.
        if let Some(semaphore) = &self.0 {
            s.field(&**semaphore);
        }

        s.finish()
    }
}

impl Drop for SemaphoreGuardArc {
    fn drop(&mut self) {
        let Some(semaphore) = &self.0 else {
            return;
        };

        semaphore.release();
    }
}

/// Counts an `acquire` call among the starved waiters of its semaphore for as long as it lives,
/// holding newcomers back: from when the call, having waited for a while, is woken only to find no
/// permit free, until the call ends, with a permit taken or given up.
///
/// Either way, it wakes as many tasks as there are permits free once it has stopped counting. The
/// newcomers it held back while permits were free wait for those permits, and nothing else may
/// wake them: the releases that freed the permits notified the listeners there were then, and the
/// call's own listener, which its `acquire` future drops before this, passes on a notification it
/// had while the count still holds newcomers back. Waking one task would not do: it would take one
/// permit and wake nobody, and leave the others waiting beside the permits still free. A task
/// woken to find every permit taken again just waits again.
struct Starved<'a>(&'a Semaphore);

impl<'a> Starved<'a> {
    fn new(semaphore: &'a Semaphore) -> Self {
        // Cannot overflow: every starved waiter is an `acquire` future, which takes more than a
        // byte of memory.
        semaphore.starved.fetch_add(1, Ordering::SeqCst);

        Self(semaphore)
    }
}

impl Drop for Starved<'_> {
    fn drop(&mut self) {
        self.0.starved.fetch_sub(1, Ordering::SeqCst);
        let free = self.0.permits.load(Ordering::SeqCst);
        if free == 0 {
            return;
        }

        self.0.released.notify_additional_unfenced(free);
    }
}
