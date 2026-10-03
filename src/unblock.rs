//! Blocking work run on a thread of its own, out of the way of the async tasks.
//!
//! The work needs no runtime: [`unblock()`] hands back a plain future, which works under any
//! executor.

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError},
    task::{Context, Poll, Waker},
};

/// Runs `work` on a thread of its own, and hands back a future of what it returns.
///
/// This is for work that blocks: a file read, a host-name lookup, a call into a library that has
/// no async form. Done on the thread that polls a task, such work holds up every other task that
/// thread has to poll for as long as it takes. Done here, it holds up only the thread it runs on,
/// and the task that awaits the future is woken once the work is over.
///
/// The thread starts before this returns, not on the first poll of the future, and ends with the
/// work. The future resolves to the value `work` returned as soon as the thread has handed it
/// over.
///
/// The work runs to its end whether or not the future is polled, and even if the future is
/// dropped: dropping it gives up the wait for the outcome and does not cancel the work, which
/// nothing can stop from outside. Nothing waits for the thread, either: work that is still going
/// when the process ends goes with it.
///
/// A panic in `work` is caught on the thread of the work, and raised again, with the payload it
/// had, by the poll of the future that would have returned the value, which is where whoever
/// awaits the future sees it. A future that is dropped before that poll never sees the panic. The
/// panic hook runs on the thread of the work as the panic happens, and does not run a second time
/// when the panic is raised again.
///
/// The future needs no runtime. It is a plain [`Future`], woken through the waker of whatever
/// polled it last, so it can be awaited under any executor, from a task on any thread.
///
/// Each call starts a thread of its own, named `zruntime blocking work`, and there is no pool of
/// them to share. That suits work that comes now and then, such as the odd file read or lookup. A
/// steady stream of it is better served by threads that are kept and reused.
///
/// # Panics
///
/// Panics if the thread cannot be started, as [`std::thread::spawn`] does.
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

    let thread_state = state.clone();
    std::thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || {
            // Constructed before `work` runs and dropped only once this closure returns, so it
            // covers every way out of the work, storing the outcome of it included.
            let _finish = Finish(&thread_state);

            let outcome = panic::catch_unwind(AssertUnwindSafe(work));
            lock(&thread_state).outcome = Some(outcome);
        })
        .expect("failed to spawn a thread for blocking work");

    BlockingWork(state)
}

/// The future of the work that [`unblock()`] runs on a thread of its own: it resolves to the value
/// the work returned.
///
/// It needs no runtime, and works under any executor. It is `Send` and `Sync` wherever the value
/// it resolves to is `Send`, so a task that awaits it can be moved between threads.
///
/// Dropping it before it resolves gives up the wait but not the work: the thread runs the work to
/// its end all the same. While it does, it holds nothing of the task that waited, or of the
/// executor that ran it, so neither is kept alive by the work. Once the future has resolved, the
/// thread holds nothing of theirs either.
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
        // Besides this future, only the thread takes the lock, and only once the work is over: to
        // store the outcome, and then in `Finish`, which takes the waker out and wakes it. So a
        // lock this drop finds taken leaves the waker with the thread for no longer than the
        // thread has left to run, and waiting for the lock instead could wait for good: a `wake`
        // from inside `Finish` may drop this very future on that thread, as an executor does with
        // a task it can no longer run.
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

/// The name of every thread started here.
const THREAD_NAME: &str = "zruntime blocking work";

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

/// Wakes whoever is waiting for the outcome as the thread leaves, before letting go of the lock
/// the waiting side has to take to see that outcome.
///
/// While this drop holds [`State`]'s lock, [`BlockingWork`] cannot take it and so cannot see the
/// outcome stored under it; by the time it can, the waker this drop took out has been woken and,
/// with nothing else left holding it, dropped. A poll that reaches the lock first, in the gap
/// between the outcome being stored and this drop running, takes the waker itself instead,
/// leaving this drop nothing to wake. Either way, the awaiting task can only complete once this
/// thread holds nothing that came from the runtime that polled it, and that runtime is then free
/// to be torn down with nothing of it left on this thread.
///
/// Waking while still holding the lock cannot deadlock here: the lock is private to this
/// hand-over, reachable from nowhere but the closure above and the future it wakes. A `wake` that
/// drops that future, as an executor may with a task it can no longer run, finds the lock taken
/// and leaves it be, the waker having been taken out already: see [`BlockingWork`]'s drop.
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
