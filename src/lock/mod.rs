//! Async locks whose guards a future can hold across an await: [`Mutex`] and [`RwLock`].
//!
//! Tasks that share state often have to keep hold of it while they wait for something else: a task
//! that writes a message to a shared socket holds the socket until all of the message is out,
//! however often it has to wait for room on the way. The locks of [`std::sync`] cannot serve there.
//! Their guards are `!Send`, so a future holding one cannot move between threads, which rules it
//! out on any executor that moves tasks between them; and a contended lock blocks the thread that
//! waits for it, along with every other future that thread is driving. Waiting for the locks of
//! this module suspends the task instead, and leaves the thread free for other work. A lock that is
//! only ever held for a few instructions, with no await in between, needs none of this, and the
//! locks of [`std::sync`] serve for it.
//!
//! The locks are built on [`Event`](crate::Event) and need no runtime. They work under any
//! executor, and from any thread, inside a task or outside of one: a thread with no task to run can
//! wait for a lock with the `block_on` of any executor. A future that waits for a lock, or that
//! holds a guard across an await, may move between threads as long as the lock may be shared
//! between them, which takes a value that is `Send` for a [`Mutex`], and `Send` and `Sync` for an
//! [`RwLock`].
//!
//! # Example
//!
//! A counter that a task on another thread bumps, with its guard held across an await. A guard of a
//! lock from [`std::sync`] would keep the future from being sent to that thread:
//!
//! ```
//! use std::{sync::Arc, thread};
//!
//! use futures_lite::future::{block_on, yield_now};
//! use zruntime::lock::Mutex;
//!
//! let counter = Arc::new(Mutex::new(0));
//! let task = {
//!     let counter = counter.clone();
//!     async move {
//!         let mut guard = counter.lock().await;
//!         yield_now().await;
//!         *guard += 1;
//!     }
//! };
//!
//! thread::spawn(move || block_on(task))
//!     .join()
//!     .expect("the other thread did not panic");
//!
//! assert_eq!(*block_on(counter.lock()), 1);
//! ```
//!
//! # No poisoning
//!
//! A panic while a guard is held does not poison the lock. The guard is dropped as the panic
//! unwinds, or with the future that holds it, which releases the lock, and the next task to take it
//! finds the value as the panicking code left it, which can be half-way through an update.
//!
//! # Fairness
//!
//! The locks are not strictly fair: tasks do not always get a lock in the order they asked for it.
//! [`Mutex::lock`], [`RwLock::read`] and [`RwLock::write`] each first try to take the lock, so a
//! task whose try succeeds takes it ahead of the tasks that were already waiting for it: a `lock`
//! or a `write` where the lock is free, a `read` where no writer holds or waits for it. A waiting
//! task that is woken and finds the lock taken again goes back to waiting, behind the tasks that
//! began to wait after it did. Under steady contention, this lets the task that is running take a
//! lock as it is released, rather than leave it free until a waiting task has been woken and has
//! run, which keeps a contended lock busy.
//!
//! How long newcomers can keep a task waiting this way is bounded. A task that has waited for a
//! lock for a while, and is woken only to find it taken again, starts holding newcomers back, for
//! as long as it waits:
//!
//! * A [`Mutex`] that such a task waits for is no longer taken by a `lock` that has not waited yet,
//!   nor by [`try_lock`](Mutex::try_lock), even while it is free: those wait behind the task. A
//!   task that was waiting already can still get the mutex ahead of it, and the task, having lost
//!   that race, waits again behind every task that has begun to wait since.
//! * An [`RwLock`] that such a writer waits for is no longer taken by a `write` that has not waited
//!   yet, nor by [`try_write`](RwLock::try_write), in the same way.
//!
//! Where the standard library has no clock, as on `wasm32-unknown-unknown`, a task cannot tell how
//! long it has waited, and holds newcomers back the first time it is woken only to find the lock
//! taken.
//!
//! # Write preference
//!
//! An [`RwLock`] prefers writers: while a writer waits for it, new readers wait too. A task that
//! holds a read guard must therefore not ask for another one, which can wait for good: see
//! [write preference](RwLock#write-preference).
//!
//! # Giving up a wait
//!
//! Dropping the future that [`Mutex::lock`], [`RwLock::read`] or [`RwLock::write`] returned, before
//! it completes, is fine: a timeout may do it, or a `select` that goes another way. The wait is
//! given up, the lock is not taken, and no other task waiting for it is left stranded.

mod mutex;
mod rwlock;

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
use std::time::{Duration, Instant};

pub use mutex::{Mutex, MutexGuard};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// When a `lock`, `read` or `write` call began to wait, so that it can tell once it has waited for
/// long enough to hold newcomers back.
///
/// Where the standard library has no clock, as on `wasm32-unknown-unknown`, it keeps no time, and
/// a call has waited for long enough as soon as it has waited at all: it then holds newcomers back
/// the first time it is woken only to find the lock taken. There is no thread there to hand the
/// lock over to, so nothing is saved by letting the running task take it again first.
#[derive(Clone, Copy)]
struct WaitStart {
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    at: Instant,
}

impl WaitStart {
    fn now() -> Self {
        Self {
            #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
            at: Instant::now(),
        }
    }

    /// Whether the call has waited for long enough to hold newcomers back, the next time it is
    /// woken and cannot get in.
    fn waited_long(self) -> bool {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        return self.at.elapsed() >= PATIENCE;
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        return true;
    }
}

/// How long a task waits for a lock before it may hold newcomers back.
///
/// Long next to a handoff of the lock between two threads, and short next to a delay a task would
/// notice: for this long, a lock under steady contention keeps going to whichever task is running
/// when it is released, rather than to a waiter that first has to be woken and scheduled. The same
/// as async-lock's.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub(crate) const PATIENCE: Duration = Duration::from_micros(500);
