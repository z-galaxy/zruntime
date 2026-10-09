//! The timers a runtime hands out, each built on a deadline its reactor keeps.
//!
//! A timer is a future that completes once a moment on the clock has come. The reactor holds the
//! deadline and wakes the task polling the timer once it has passed; it takes the deadline on the
//! timer's first poll rather than where the timer is made, moves it, waker and all, when the timer
//! is reset, and lets it go when the timer is dropped. What a timer adds to that is the part the
//! reactor cannot do for itself: a poll that leaves it waiting also sees to it that some thread
//! will be there to fire it, which a runtime from `SharedRuntime::current` does by starting a
//! helper thread where nobody is inside `block_on` on it. An interval is one timer, reset to the
//! deadline of its next tick each time it ticks.

use std::{
    error, fmt,
    future::{Future, poll_fn},
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_core::Stream;

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

/// A timer on a [`Runtime`](crate::Runtime) that ticks once every period, rather than once: what a
/// heartbeat, a poll of some outside state or any other piece of work done on a schedule reaches
/// for.
///
/// Each tick hands out the moment it was scheduled for, rather than the moment it was seen, so a
/// loop that does its work at each tick keeps to the schedule however long a round of that work
/// takes, and however late the task gets round to the next tick. A tick that comes due while
/// nobody waits for it is not lost: the next wait for a tick finds it at once. Where a whole period
/// or more goes by in that way, ticks are missed, and what the interval does about them is its
/// [`MissedTickBehavior`].
///
/// An interval is a [`Stream`] of the instants it ticks at, which never ends, so the extension
/// traits of [`futures`] drive it as well as [`tick`](Self::tick) does. Like a [`Sleep`], it costs
/// nothing until it is first polled, and holds its runtime, so a task holding one keeps that
/// runtime alive.
///
/// [`Stream`]: futures_core::Stream
/// [`futures`]: https://docs.rs/futures
pub struct Interval<M = Local>
where
    M: Mode,
{
    /// The timer of the next tick, whose deadline is that tick's.
    sleep: Sleep<M>,
    period: Duration,
    missed_tick_behavior: MissedTickBehavior,
}

impl<M> Interval<M>
where
    M: Mode,
{
    /// Waits for the next tick, and completes with the moment it was scheduled for.
    ///
    /// Dropping the future this hands back before it completes loses no tick: the next wait finds
    /// it all the same.
    pub async fn tick(&mut self) -> Instant {
        poll_fn(|cx| self.poll_tick(cx)).await
    }

    /// Polls for the next tick, which completes with the moment that tick was scheduled for.
    ///
    /// Where the tick has not come due yet, the waker of `cx` is woken once it has; only the waker
    /// of the last poll is, so one task at a time waits on an interval. An interval asked for
    /// further ahead than the clock can name never ticks, and stays pending for good.
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        // A timer with no deadline never completes, and its poll would do nothing at all.
        let Some(deadline) = self.sleep.deadline() else {
            return Poll::Pending;
        };
        // Through `Sleep`'s own poll, which asks for a thread to fire the deadline, as a bare
        // timer's does.
        if Pin::new(&mut self.sleep).poll(cx).is_pending() {
            return Poll::Pending;
        }
        let next = next_deadline(
            deadline,
            Instant::now(),
            self.period,
            self.missed_tick_behavior,
        );
        // Straight to the reactor's timer, which takes no deadline at all for a tick the clock
        // cannot name, and so never fires again.
        self.sleep.0.reset(next);

        Poll::Ready(deadline)
    }

    /// Starts the schedule over, with the next tick one period from now, or none at all where the
    /// clock cannot name that moment.
    ///
    /// A tick that had come due, and that nobody had waited for yet, is dropped. This is what an
    /// idle timer reaches for, which only has to tick once a period has gone by with nothing else
    /// happening.
    pub fn reset(&mut self) {
        self.sleep.reset_after(self.period);
    }

    /// How long this interval waits from one tick to the next.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// What this interval does about ticks it missed.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    /// Changes what this interval does about ticks it missed, from the next tick it hands out on.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// An interval whose first tick is the deadline of `sleep`, and each tick after it `period`
    /// after the one before.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero.
    pub(crate) fn new(sleep: Sleep<M>, period: Duration) -> Self {
        // A zero period would tick on every poll without end, and `next_deadline` divides by it.
        assert!(!period.is_zero(), "an interval's period must not be zero");

        Self {
            sleep,
            period,
            missed_tick_behavior: MissedTickBehavior::default(),
        }
    }
}

impl<M> fmt::Debug for Interval<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Interval")
            .field("next_tick", &self.sleep.deadline())
            .field("period", &self.period)
            .field("missed_tick_behavior", &self.missed_tick_behavior)
            .finish()
    }
}

impl<M> Stream for Interval<M>
where
    M: Mode,
{
    type Item = Instant;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
        self.get_mut().poll_tick(cx).map(Some)
    }
}

/// What an [`Interval`] does about ticks it missed because nobody waited for them in time: the
/// task polling it was busy with something else, or the thread it runs on was blocked.
///
/// A tick is missed where, by the time the tick before it is handed out, its own moment has passed
/// already. Each variant below is shown on the same timeline: a period of 10 ms, ticks scheduled at
/// 10, 20, 30 ms and so on, the tick at 10 handed out on time, and the task then held up until 35,
/// when it hands out the tick at 20 and finds the one at 30 missed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum MissedTickBehavior {
    /// The missed ticks are handed out one after the other, at once, until the interval has caught
    /// up, and the schedule is kept: the ticks at 20 and 30 are both handed out at 35, and the next
    /// one at 40, then 50.
    ///
    /// This is what a piece of work that has to happen once for each period that went by, however
    /// late, reaches for.
    #[default]
    Burst,
    /// The next tick comes one full period after the late one was handed out, and the schedule
    /// moves with it: the tick at 20 is handed out at 35, and the next one at 45, then 55.
    ///
    /// This is what a piece of work that needs a full period of rest between two rounds of it
    /// reaches for.
    Delay,
    /// The missed ticks are dropped, and the next tick is the first one of the schedule still to
    /// come: the tick at 20 is handed out at 35, the one at 30 never is, and the next one comes at
    /// 40, then 50.
    ///
    /// This is what a piece of work that only has to keep to the schedule, and gains nothing from
    /// making up for a round it missed, reaches for.
    Skip,
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

/// When the tick after one scheduled for `deadline` and handed out at `now` is due, with ticks
/// `period` apart and `behavior` for any that were missed, or nothing where the clock cannot name
/// that moment, which leaves the interval never to tick again.
///
/// `period` is never zero: an interval is never made with one.
fn next_deadline(
    deadline: Instant,
    now: Instant,
    period: Duration,
    behavior: MissedTickBehavior,
) -> Option<Instant> {
    let next = deadline.checked_add(period)?;
    // Nothing missed, whatever the behaviour.
    if next > now {
        return Some(next);
    }

    match behavior {
        MissedTickBehavior::Burst => Some(next),
        MissedTickBehavior::Delay => now.checked_add(period),
        MissedTickBehavior::Skip => {
            // The first tick of the schedule after `now`, worked out in nanoseconds rather than
            // by stepping through every missed one. `now` is no earlier than `next`, and so later
            // than `deadline`, and the remainder is less than the period, which no `Duration`'s
            // count of whole seconds overflows a `u64` for.
            let period = period.as_nanos();
            let rem = (now - deadline).as_nanos() % period;
            let gap = period - rem;
            let gap = Duration::new((gap / NANOS_PER_SEC) as u64, (gap % NANOS_PER_SEC) as u32);

            now.checked_add(gap)
        }
    }
}

/// How many nanoseconds there are in a second.
const NANOS_PER_SEC: u128 = 1_000_000_000;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ntest::timeout;

    use super::{MissedTickBehavior, next_deadline};

    const PERIOD: Duration = Duration::from_millis(10);

    /// A tick handed out before the next one's moment has come misses nothing, and the next one
    /// keeps to the schedule, whatever the behaviour.
    #[test]
    #[timeout(15000)]
    fn nothing_missed_keeps_the_schedule() {
        let base = Instant::now();
        let deadline = base + ms(10);
        let now = base + ms(15);

        for behavior in BEHAVIORS {
            assert_eq!(
                next_deadline(deadline, now, PERIOD, behavior),
                Some(base + ms(20)),
                "{behavior:?}",
            );
        }
    }

    /// A tick handed out exactly at the next one's moment has missed that one.
    #[test]
    #[timeout(15000)]
    fn a_tick_handed_out_at_the_next_ones_moment_has_missed_it() {
        let base = Instant::now();
        let deadline = base + ms(10);
        let now = base + ms(20);

        assert_eq!(
            next_deadline(deadline, now, PERIOD, MissedTickBehavior::Delay),
            Some(base + ms(30)),
        );
        assert_eq!(
            next_deadline(deadline, now, PERIOD, MissedTickBehavior::Skip),
            Some(base + ms(30)),
        );
    }

    #[test]
    #[timeout(15000)]
    fn burst_hands_out_each_missed_tick() {
        let base = Instant::now();

        // Missed by one period, and by several.
        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(35),
                PERIOD,
                MissedTickBehavior::Burst
            ),
            Some(base + ms(30)),
        );
        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(75),
                PERIOD,
                MissedTickBehavior::Burst
            ),
            Some(base + ms(30)),
        );
    }

    #[test]
    #[timeout(15000)]
    fn delay_moves_the_schedule_to_a_period_after_now() {
        let base = Instant::now();

        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(35),
                PERIOD,
                MissedTickBehavior::Delay
            ),
            Some(base + ms(45)),
        );
        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(75),
                PERIOD,
                MissedTickBehavior::Delay
            ),
            Some(base + ms(85)),
        );
    }

    #[test]
    #[timeout(15000)]
    fn skip_moves_on_to_the_first_tick_still_to_come() {
        let base = Instant::now();

        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(35),
                PERIOD,
                MissedTickBehavior::Skip
            ),
            Some(base + ms(40)),
        );
        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(75),
                PERIOD,
                MissedTickBehavior::Skip
            ),
            Some(base + ms(80)),
        );
        // Not by whole milliseconds either.
        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(75) + Duration::from_nanos(1),
                PERIOD,
                MissedTickBehavior::Skip,
            ),
            Some(base + ms(80)),
        );
    }

    /// Handed out exactly on a tick of the schedule, that tick is itself one missed, and the next
    /// one to come is a full period on.
    #[test]
    #[timeout(15000)]
    fn skip_on_a_tick_of_the_schedule_moves_a_full_period_on() {
        let base = Instant::now();

        assert_eq!(
            next_deadline(
                base + ms(20),
                base + ms(50),
                PERIOD,
                MissedTickBehavior::Skip
            ),
            Some(base + ms(60)),
        );
    }

    /// A period of seconds and nanoseconds both, which the gap to the next tick is rebuilt from
    /// without losing either.
    #[test]
    #[timeout(15000)]
    fn skip_keeps_the_seconds_of_a_long_period() {
        let base = Instant::now();
        let period = Duration::new(3, 500_000_000);
        let deadline = base + period;
        // Two periods and a half later, a second and three quarters short of the next tick.
        let now = deadline + period * 2 + Duration::from_millis(1_750);

        assert_eq!(
            next_deadline(deadline, now, period, MissedTickBehavior::Skip),
            Some(deadline + period * 3),
        );
    }

    /// Where the clock has no moment a period after the tick, there is no next tick, whatever the
    /// behaviour.
    #[test]
    #[timeout(15000)]
    fn no_next_tick_beyond_the_clock() {
        let base = Instant::now();

        for behavior in BEHAVIORS {
            assert_eq!(
                next_deadline(base, base, Duration::MAX, behavior),
                None,
                "{behavior:?}",
            );
        }
    }

    /// Every behaviour there is.
    const BEHAVIORS: [MissedTickBehavior; 3] = [
        MissedTickBehavior::Burst,
        MissedTickBehavior::Delay,
        MissedTickBehavior::Skip,
    ];

    /// `millis` milliseconds.
    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }
}
