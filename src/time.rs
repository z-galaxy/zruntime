//! The timers a runtime hands out, each built on a deadline its reactor keeps.
//!
//! A timer is a future that completes once a moment on the clock has come. The reactor holds the
//! deadline and wakes the task polling the timer once it has passed; it takes the deadline on the
//! timer's first poll rather than where the timer is made, moves it, waker and all, when the timer
//! is reset, and lets it go when the timer is dropped. What a timer adds to that is the part the
//! reactor cannot do for itself: a poll that leaves it waiting also sees to it that some thread
//! will be there to fire it, which a runtime from `SharedRuntime::current` does by starting a
//! helper thread where nobody is inside `block_on` on it.

use std::{
    error, fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{Local, Mode, reactor};

/// A future run against a deadline on a [`Runtime`](crate::Runtime): it resolves to what the
/// future produced where that comes first, or to [`TimedOut`] once the deadline has passed.
///
/// Each poll gives the future its turn first, and looks at the deadline only where the future is
/// still waiting, so a future that completes on the very poll the deadline passes on hands back
/// its output rather than losing it. Like a [`Sleep`], a timeout costs nothing until a poll leaves
/// it waiting: one whose future completes on its first poll never looks at its deadline at all,
/// and so never asks for a thread to fire it.
///
/// The future is not dropped when the deadline passes. It lives on inside the timeout until that
/// is dropped, or is handed back by [`into_inner`](Self::into_inner), to be retried, or driven on
/// with no deadline at all.
#[must_use = "futures do nothing unless .awaited"]
pub struct Timeout<F, M = Local>
where
    M: Mode,
{
    future: F,
    sleep: Sleep<M>,
}

impl<F, M> Timeout<F, M>
where
    M: Mode,
{
    /// The future this runs.
    pub fn get_ref(&self) -> &F {
        &self.future
    }

    /// The future this runs, to change in place.
    pub fn get_mut(&mut self) -> &mut F {
        &mut self.future
    }

    /// The future this runs, handed back with the deadline let go of: what a caller reaches for
    /// after a time-out, to retry the future, or to recover a stream or a socket it was reading.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let slow = runtime.sleep(Duration::from_secs(60));
    /// let mut timeout = runtime.timeout(Duration::from_millis(1), slow);
    ///
    /// assert!(runtime.block_on(&mut timeout).is_err());
    /// // The timer the timeout gave up on is still there, to be brought forward and awaited.
    /// let mut slow = timeout.into_inner();
    /// slow.reset_after(Duration::from_millis(1));
    /// runtime.block_on(slow);
    /// ```
    pub fn into_inner(self) -> F {
        self.future
    }

    /// A timeout running `future` against the deadline of `sleep`.
    pub(crate) fn new(future: F, sleep: Sleep<M>) -> Self {
        Self { future, sleep }
    }
}

impl<F, M> fmt::Debug for Timeout<F, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout")
            .field("deadline", &self.sleep.deadline())
            .finish_non_exhaustive()
    }
}

impl<F, M> Future for Timeout<F, M>
where
    F: Future,
    M: Mode,
{
    type Output = Result<F::Output, TimedOut>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `future` is pinned structurally, and nothing moves it out of a pinned `Timeout`.
        // `Timeout` has no `Drop` impl, which could move it in `drop`. It has no `Unpin` impl of
        // its own, so it is `Unpin` only where `F` is. It is not `#[repr(packed)]`, so the field
        // is never moved to be aligned. And the only methods that hand out `&mut F` or `F` itself,
        // `get_mut` and `into_inner`, take `&mut self` and `self`, which a pinned `Timeout` gives
        // only where it is `Unpin`, and `F` with it. `sleep` is not pinned structurally: it is
        // `Unpin`, as `Pin::new` below checks, and is only ever reached through a plain `&mut`.
        let (future, sleep) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.future), &mut this.sleep)
        };
        if let Poll::Ready(output) = future.poll(cx) {
            return Poll::Ready(Ok(output));
        }
        // Polled only for a future still waiting, and through `Sleep`'s own poll, which asks for
        // a thread to fire the deadline as a bare timer's does.
        if Pin::new(sleep).poll(cx).is_ready() {
            return Poll::Ready(Err(TimedOut));
        }

        Poll::Pending
    }
}

/// The error a [`Timeout`] resolves to where its deadline passed before its future completed.
///
/// Only this crate makes one. It converts into an [`io::Error`] of the kind
/// [`TimedOut`](io::ErrorKind::TimedOut), so that code returning an `io::Result`, as network code
/// so often does, can hand a time-out on with `?`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TimedOut;

impl fmt::Display for TimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the deadline passed before the future completed")
    }
}

impl error::Error for TimedOut {}

impl From<TimedOut> for io::Error {
    fn from(timed_out: TimedOut) -> Self {
        io::Error::new(io::ErrorKind::TimedOut, timed_out)
    }
}

/// A timer on a [`Runtime`](crate::Runtime), which keeps a thread on it for as long as it has a
/// deadline.
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, and
/// the thread that is to fire it has to be there from that poll onwards, however long ago the
/// timer was asked for. So it is the poll that asks for one — a helper thread, on a runtime from
/// `SharedRuntime::current` that nobody is inside `block_on` on — and a timer nobody ever polls
/// costs nothing at all. A timer can be moved to another deadline in place, by
/// [`reset`](Self::reset) or [`reset_after`](Self::reset_after), and a reset asks for no thread of
/// its own: a timer the reactor holds a deadline of has one already, and any other takes its new
/// deadline on its next poll, which asks as a first poll does. A timer holds its runtime, so a task
/// holding one keeps that runtime alive: nothing here takes a runtime down while it has work.
#[must_use = "futures do nothing unless .awaited"]
pub struct Sleep<M = Local>(pub(crate) reactor::Sleep<M>)
where
    M: Mode;

impl<M> Sleep<M>
where
    M: Mode,
{
    /// When this timer comes due, or `None` for one that never does: one asked for further ahead
    /// than the clock can name, as `sleep(Duration::MAX)` is.
    pub fn deadline(&self) -> Option<Instant> {
        self.0.deadline()
    }

    /// Moves this timer to `deadline`, in place: what a keep-alive or an idle timeout needs, which
    /// pushes its deadline back on every message rather than making a fresh timer for each.
    ///
    /// The timer completes once `deadline` has passed, whether or not it was polled before the
    /// reset, and whether or not it had already completed: one that has completes again at its
    /// new deadline, so it can be awaited again. A task already waiting on the timer is woken at
    /// the new deadline without polling the timer again, so the part of a task that resets a timer
    /// need not be the part that awaits it. That takes a deadline before the reset as well as
    /// after it: a timer that had none, as one made by `sleep(Duration::MAX)` has none, takes its
    /// new deadline on its next poll, and a task waiting on it is not woken to make that poll.
    ///
    /// A pinned timer, in a `pin!` or behind a `Pin<&mut Sleep>`, is reset all the same: a timer
    /// is `Unpin`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let mut sleep = runtime.sleep(Duration::from_secs(60));
    /// let deadline = Instant::now() + Duration::from_millis(2);
    ///
    /// // Brought forward from a minute ahead to a moment ahead.
    /// sleep.reset(deadline);
    /// runtime.block_on(sleep);
    ///
    /// assert!(Instant::now() >= deadline);
    /// ```
    pub fn reset(&mut self, deadline: Instant) {
        self.0.reset(Some(deadline));
    }

    /// Moves this timer to `duration` from now, in place, as [`reset`](Self::reset) does: a length
    /// of time further ahead than the clock can name, as `Duration::MAX` is, makes it a timer that
    /// never comes due.
    ///
    /// # Example
    ///
    /// An idle timeout, pushed back by each message that comes in before it expires.
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let idle = Duration::from_millis(4);
    /// let gap = Duration::from_millis(1);
    /// let started = Instant::now();
    ///
    /// runtime.block_on(async {
    ///     let mut idle_timeout = runtime.sleep(idle);
    ///     for _message in 0..3 {
    ///         runtime.sleep(gap).await;
    ///         idle_timeout.reset_after(idle);
    ///     }
    ///     idle_timeout.await;
    /// });
    ///
    /// // Expired one idle period after the last message, not after the first.
    /// assert!(started.elapsed() >= 3 * gap + idle);
    /// ```
    pub fn reset_after(&mut self, duration: Duration) {
        // A length of time is a deadline on the clock, where the clock has a moment that far
        // ahead, as in `Runtime::sleep`.
        self.0.reset(Instant::now().checked_add(duration));
    }
}

impl<M> Future for Sleep<M>
where
    M: Mode,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if Pin::new(&mut this.0).poll(cx).is_ready() {
            return Poll::Ready(());
        }
        // A timer that never comes due leaves no deadline behind and needs no thread: one
        // started for it would find nothing to wait on and retire in the round it started.
        if this.0.never_fires() {
            return Poll::Pending;
        }
        // Asked for once the deadline is in the reactor's map, so that a helper starting here
        // waits on it.
        M::ensure_progress(this.0.core());

        Poll::Pending
    }
}
