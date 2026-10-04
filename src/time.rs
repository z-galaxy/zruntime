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
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{Local, Mode, reactor};

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
