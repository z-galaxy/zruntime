//! Blocking work run on a pool of threads, out of the way of the async tasks, and [`Unblock`], an
//! adapter that runs each operation on a blocking I/O handle as such work.
//!
//! The work needs no runtime: [`unblock()`] hands back a plain future, which works under any
//! executor, and so does the adapter.

mod io;
pub(crate) mod pool;

use std::{
    any::Any,
    fmt,
    future::Future,
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError},
    task::{Context, Poll, Waker},
    time::Duration,
};

pub use io::Unblock;
use pool::Pool;

/// Runs `work` on a thread of a pool kept for blocking work, and hands back a future of what it
/// returns.
///
/// This is for work that blocks: a file read, a host-name lookup, a call into a library that has
/// no async form. Done on the thread that polls a task, such work holds up every other task that
/// thread has to poll for as long as it takes. Done here, it holds up only the thread it runs on,
/// and the task that awaits the future is woken once the work is over.
///
/// The work is handed to the pool before this returns, not on the first poll of the future. A
/// thread of the pool that has nothing to run takes it up. Where none is free, a thread is started
/// for it, as long as the pool has fewer than 500; past that, the work waits its turn, behind the
/// work handed over before it, for a thread to be done with what it runs. The future resolves to
/// the value `work` returned as soon as the thread has handed it over.
///
/// A thread is kept for the work that comes after its own, which spares a steady stream of work
/// the start of a thread for each piece of it. A thread that has had nothing to run for ten seconds
/// ends, so a pool that is no longer used holds no thread. Every thread of the pool is named
/// `zruntime blocking work`.
///
/// Work that blocks for good holds its thread for good, and once every thread of the pool is held
/// that way, the work handed over after it waits for good as well. The pool suits work that ends
/// of its own accord: work that waits on other work handed to the pool, or that runs for as long as
/// the program does, is better off on a thread of its own, from [`std::thread::spawn`].
///
/// The work runs to its end whether or not the future is polled, and even if the future is
/// dropped: dropping it gives up the wait for the outcome and does not cancel the work, which
/// nothing can stop from outside, nor take back from the pool before it starts. Nothing waits for
/// the threads of the pool, either: work that is still going, or still waiting for a thread, when
/// the process ends goes with it.
///
/// A panic in `work` is caught on the thread of the work, which goes on to run other work, and
/// raised again, with the payload it had, by the poll of the future that would have returned the
/// value, which is where whoever awaits the future sees it. A future that is dropped before that
/// poll never sees the panic. The panic hook runs on the thread of the work as the panic happens,
/// and does not run a second time when the panic is raised again.
///
/// The future needs no runtime. It is a plain [`Future`], woken through the waker of whatever
/// polled it last, so it can be awaited under any executor, from a task on any thread.
///
/// # Panics
///
/// Panics if the pool has no thread at all and cannot start one for the work, as
/// [`std::thread::spawn`] does when it cannot. A pool that has threads leaves the work to them
/// instead, to take up in its turn.
///
/// # Example
///
/// A sleep stands in for work that blocks. The future is driven by `block_on` from the
/// `futures-lite` crate, but the `block_on` of any executor would do, as `unblock` needs no
/// runtime:
///
/// ```
/// use std::{thread, time::Duration};
///
/// use futures_lite::future::block_on;
/// use zruntime::unblock;
///
/// let answer = unblock(|| {
///     thread::sleep(Duration::from_millis(10));
///     42
/// });
///
/// assert_eq!(block_on(answer), 42);
/// ```
pub fn unblock<T>(work: impl FnOnce() -> T + Send + 'static) -> BlockingWork<T>
where
    T: Send + 'static,
{
    let state = Arc::new(Mutex::new(State {
        outcome: None,
        waker: None,
    }));

    let job_state = state.clone();
    Pool::submit(
        &POOL,
        Box::new(move || {
            // Constructed before `work` runs and dropped only once this closure returns, so it
            // covers every way out of the work, storing the outcome of it included.
            let _finish = Finish(&job_state);

            let outcome = panic::catch_unwind(AssertUnwindSafe(work));
            lock(&job_state).outcome = Some(outcome);
        }),
    );

    BlockingWork(state)
}

/// The future of the work that [`unblock()`] hands to its pool of threads: it resolves to the
/// value the work returned.
///
/// It needs no runtime, and works under any executor. It is `Send` and `Sync` wherever the value
/// it resolves to is `Send`, so a task that awaits it can be moved between threads.
///
/// Dropping it before it resolves gives up the wait but not the work: the pool runs the work to
/// its end all the same, in its turn if no thread has taken it up yet. While the work waits or
/// runs, the pool holds nothing of the task that waited, or of the executor that ran it, so
/// neither is kept alive by the work. Once the future has resolved, the thread that ran the work
/// holds nothing of theirs either.
pub struct BlockingWork<T>(Arc<Mutex<State<T>>>);

impl<T> Future for BlockingWork<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut guard = lock(&self.0);
        let Some(outcome) = guard.outcome.take() else {
            let waker = cx.waker();
            let replaced = match guard.waker.as_ref() {
                Some(stored) if stored.will_wake(waker) => None,
                _ => guard.waker.replace(waker.clone()),
            };
            drop(guard);
            // Past the lock, which belongs to this hand-over alone and is none of a dropped
            // waker's business.
            drop(replaced);
            return Poll::Pending;
        };
        // Taking the waker here too, whether or not this poll needed it, keeps a future that
        // resolves without ever being woken from leaving one behind for `Finish` to wake later,
        // once whoever it belongs to may already be gone.
        let leftover = guard.waker.take();
        drop(guard);
        drop(leftover);

        match outcome {
            Ok(value) => Poll::Ready(value),
            Err(panic) => panic::resume_unwind(panic),
        }
    }
}

impl<T> Drop for BlockingWork<T> {
    fn drop(&mut self) {
        // Besides this future, only the thread that runs the work takes the lock, and only once
        // the work is over: to store the outcome, and then in `Finish`, which takes the waker out
        // and wakes it. So a lock this drop finds taken leaves the waker with that thread only
        // until `Finish` has woken it, moments later, and waiting for the lock instead could wait
        // for good: a `wake` from inside `Finish` may drop this very future on that thread, as an
        // executor does with a task it can no longer run.
        let mut state = match self.0.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return,
        };
        let waker = state.waker.take();
        drop(state);
        // Past the lock, as in `poll`.
        drop(waker);
    }
}

impl<T> fmt::Debug for BlockingWork<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingWork").finish_non_exhaustive()
    }
}

/// The pool that [`unblock()`] hands work to.
static POOL: Pool = Pool::new(MAX_THREADS, IDLE_TIMEOUT);

/// The most threads [`POOL`] has at once: as many as the pool of smol's `blocking` crate has by
/// default. That is room for a burst of slow lookups or file reads to run side by side, while a
/// flood of work that blocks for good queues up rather than starting threads without end.
const MAX_THREADS: NonZeroUsize = NonZeroUsize::new(500).unwrap();

/// How long a thread of [`POOL`] waits for more work before it ends: as long as tokio keeps an
/// idle thread of its own pool for blocking work. That spans the gaps in a steady stream of work,
/// and lets the threads that a burst of it started go soon after the burst is over.
#[cfg(not(miri))]
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Under Miri, a thread of [`POOL`] ends as soon as it finds no work to run. Miri ends a program
/// with an error if a thread other than the main one is still running as the main one returns, and
/// a thread that waited for more work would be.
#[cfg(miri)]
const IDLE_TIMEOUT: Duration = Duration::ZERO;

/// Where the thread leaves the outcome of the work, and the waker it hands back to.
///
/// Both live behind the one lock, so that seeing the outcome and taking the waker to wake it are
/// never two separate steps from the other side's point of view: see [`Finish`] for what that
/// buys.
struct State<T> {
    /// The outcome of the work, once the thread has produced it.
    outcome: Option<Outcome<T>>,
    /// The waker of the task that polled for that outcome last.
    waker: Option<Waker>,
}

/// What the work returned, or what it panicked with.
type Outcome<T> = Result<T, Box<dyn Any + Send>>;

/// Wakes whoever is waiting for the outcome as the work ends, before letting go of the lock the
/// waiting side has to take to see that outcome.
///
/// While this drop holds [`State`]'s lock, [`BlockingWork`] cannot take it and so cannot see the
/// outcome stored under it; by the time it can, the waker this drop took out has been woken and,
/// with nothing else left holding it, dropped. A poll that reaches the lock first, in the gap
/// between the outcome being stored and this drop running, takes the waker itself instead,
/// leaving this drop nothing to wake. Either way, the awaiting task can only complete once the
/// thread of the work holds nothing that came from the runtime that polled it, and that runtime is
/// then free to be torn down with nothing of it left on that thread.
///
/// Waking while still holding the lock cannot deadlock here: the lock is private to this
/// hand-over, reachable from nowhere but the job that [`unblock()`] hands the pool and the future
/// it wakes. A `wake` that drops that future, as an executor may with a task it can no longer run,
/// finds the lock taken and leaves it be, the waker having been taken out already: see
/// [`BlockingWork`]'s drop.
struct Finish<'a, T>(&'a Mutex<State<T>>);

impl<T> Drop for Finish<'_, T> {
    fn drop(&mut self) {
        let mut state = lock(self.0);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

/// The state behind its lock, taken whether or not a panic poisoned it.
fn lock<T>(state: &Mutex<State<T>>) -> MutexGuard<'_, State<T>> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
