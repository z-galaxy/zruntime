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
//! # No fairness
//!
//! The locks are not fair: tasks do not get a lock in the order they asked for it. [`Mutex::lock`],
//! [`RwLock::read`] and [`RwLock::write`] each first try to take the lock, so a task whose try
//! succeeds takes it ahead of the tasks that were already waiting for it: a `lock` or a `write`
//! where the lock is free, a `read` where no writer holds or waits for it. A waiting task that is
//! woken and finds the lock taken again goes back to waiting, behind the tasks that began to wait
//! after it did. Under steady contention, a task can therefore wait for as long as other tasks keep
//! taking the lock.
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

pub use mutex::{Mutex, MutexGuard};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
