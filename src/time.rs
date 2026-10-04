//! The timers a runtime hands out, each built on a deadline its reactor keeps.
//!
//! A timer is a future that completes once a moment on the clock has come. The reactor holds the
//! deadline and wakes the task polling the timer once it has passed; it takes the deadline on the
//! timer's first poll rather than where the timer is made, and lets it go when the timer is
//! dropped. What a timer adds to that is the part the reactor cannot do for itself: a poll that
//! leaves it waiting also sees to it that some thread will be there to fire it, which a runtime
//! from `SharedRuntime::current` does by starting a helper thread where nobody is inside
//! `block_on` on it.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Local, Mode, reactor};

/// A timer on a [`Runtime`](crate::Runtime), which keeps a thread on it for as long as it has a
/// deadline.
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, and
/// the thread that is to fire it has to be there from that poll onwards, however long ago the
/// timer was asked for. So it is the poll that asks for one — a helper thread, on a runtime from
/// `SharedRuntime::current` that nobody is inside `block_on` on — and a timer nobody ever polls
/// costs nothing at all. A timer holds its runtime, so a task holding one keeps that runtime
/// alive: nothing here takes a runtime down while it has work.
#[must_use = "futures do nothing unless .awaited"]
pub struct Sleep<M = Local>(pub(crate) reactor::Sleep<M>)
where
    M: Mode;

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
