//! What watches a runtime's sockets and keeps its timers.
//!
//! A reactor holds every source registered on it and every timer taken from it, and the thread
//! in the runtime's seat hands it that thread through [`Reactor::wait`]: one wait on
//! the platform's poll, bounded by the nearest deadline, and then the wakes for whatever that
//! wait found ready and for whatever timer has come due.
//!
//! The poll is level-triggered, and nothing here arms or disarms a source as readiness comes and
//! goes. Each wait is told afresh what to watch, worked out from the wakers stored for each
//! source, so a source nobody waits for any more is simply left out of the next wait, and one
//! that became ready between two waits is reported by the second. That is also why a waiter which
//! stores a waker breaks the wait under way: that wait was built before the waker existed, and
//! only the wait after it takes the source in.
//!
//! Two locks guard the sources and the timers. Where both a source's place in the map and the
//! wakers of that source are needed, the map is taken first and never the other way round; the
//! timers are taken on their own. Neither lock is held across the wait, which may last until a
//! deadline, nor across a wake, which runs a task's code and may come straight back here to
//! register a source, to ask for a timer or to let either go. The flag that spares a `notify`
//! its write while a wake-up is on its way needs no lock: it is an atomic of its own.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    io, mem,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use super::{
    Inner,
    poll::{Poller, Want},
};
use crate::{Interest, Source};

/// The sockets one runtime watches and the timers it keeps.
pub(super) struct Reactor {
    poller: Poller,
    sources: Mutex<Sources>,
    timers: Mutex<Timers>,
    /// Whether a wake-up is on its way to the thread in the seat, so that a `notify` which
    /// finds it up leaves its own write out: one wake-up in the channel ends a wait as surely as
    /// a hundred do.
    ///
    /// Raised by the `notify` that writes, before it writes, and lowered by `wait` once the poll
    /// returns, whether that poll took the wake-up out or ended some other way and left it to
    /// end the next wait at once. Either way, whatever a `notify` turned away in the meantime
    /// had to say is looked at by the thread in the seat after the flag is down. The raising swap
    /// acquires the lowering store, so that a `notify` which finds the flag down writes after
    /// the wait that brought it down.
    wake_pending: AtomicBool,
}

impl Reactor {
    /// A reactor watching nothing, with the channel its `notify` writes to open.
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            sources: Mutex::new(Sources::default()),
            timers: Mutex::new(Timers::default()),
            wake_pending: AtomicBool::new(false),
        })
    }

    /// Wakes the thread in this reactor's seat inside its wait, unless called from that thread,
    /// which sees every change before its next wait anyway.
    ///
    /// One wake-up in the channel ends a wait as surely as a hundred do, so the rest are left
    /// unwritten: a burst of spawns, or a task cancelled alongside them, costs one write between
    /// two waits rather than one apiece.
    pub(super) fn notify(&self) {
        if super::driver::on_driver_thread(self) {
            return;
        }
        // A previous `true` means a wake-up is on its way already, so this call is turned away
        // without writing to it again.
        if self.wake_pending.swap(true, Ordering::AcqRel) {
            return;
        }

        if self.poller.notify().is_err() {
            // The flag stands for a wake-up in the channel, so one that never got there takes it
            // down again and the next caller tries afresh. A channel that cannot be written to
            // leaves the seat's holder waiting until its timeout, and there is nobody here to
            // tell about it who could do any better.
            self.wake_pending.store(false, Ordering::Release);
        }
    }

    /// Whether a wake-up this reactor wrote is in the channel, waiting to be taken out.
    #[cfg(test)]
    pub(super) fn wake_pending(&self) -> bool {
        self.wake_pending.load(Ordering::Acquire)
    }

    /// One wait on the poller, bounded by `at_most` and by the nearest deadline, then the wakes
    /// for what it found ready and for the timers that are due.
    pub(super) fn wait(&self, at_most: Option<Duration>) -> io::Result<()> {
        let wants: Vec<(Arc<dyn Source>, Want)> = lock(&self.sources)
            .states
            .values()
            .filter_map(|state| {
                let wakers = lock(&state.wakers);
                let want = Want {
                    key: state.key,
                    readable: wakers.readable.is_some(),
                    writable: wakers.writable.is_some(),
                };

                (want.readable || want.writable).then(|| (state.source.clone(), want))
            })
            .collect();
        let deadline = lock(&self.timers)
            .pending
            .keys()
            .next()
            .map(|(deadline, _)| *deadline);
        let until_deadline = deadline.map(|at| at.saturating_duration_since(Instant::now()));
        let timeout = match (at_most, until_deadline) {
            (Some(at_most), Some(until_deadline)) => Some(at_most.min(until_deadline)),
            (bound, None) | (None, bound) => bound,
        };

        let ready = self.poller.wait(&wants, timeout)?;
        // The wait takes whatever wake-up it finds out of the channel, so the flag comes down
        // here and the next caller writes again. One that came between the two is turned away
        // without a write, and loses nothing by it: what it had to say — a task queued, a source
        // registered, a deadline stored — it said before it called, and the rounds below and the
        // ones the seat's holder makes look at all three afresh. The other way about, a wake-up
        // left in the channel by a wait that ended some other way only ends the next one at once.
        self.wake_pending.store(false, Ordering::Release);

        let woken = {
            let sources = lock(&self.sources);
            let mut woken = Vec::new();
            for event in &ready {
                // A source let go of while the wait ran has nobody left to wake.
                let Some(state) = sources.states.get(&event.key) else {
                    continue;
                };
                let mut wakers = lock(&state.wakers);
                if event.readable {
                    woken.extend(wakers.readable.take());
                }
                if event.writable {
                    woken.extend(wakers.writable.take());
                }
            }

            woken
        };
        for waker in woken {
            waker.wake();
        }

        let due = {
            let mut timers = lock(&self.timers);
            // Ids are handed out from zero upwards, so no timer carries `u64::MAX` and the split
            // leaves behind exactly the deadlines that have passed.
            let later = timers.pending.split_off(&(Instant::now(), u64::MAX));

            mem::replace(&mut timers.pending, later)
        };
        for waker in due.into_values() {
            waker.wake();
        }

        Ok(())
    }

    /// Wakes every stored waker, sources and timers alike; what a failed wait falls back on, so
    /// that each waiter retries its operation and sees its own error.
    pub(super) fn wake_everything(&self) {
        let mut woken: Vec<Waker> = lock(&self.sources)
            .states
            .values()
            .flat_map(|state| {
                let mut wakers = lock(&state.wakers);

                [wakers.readable.take(), wakers.writable.take()]
            })
            .flatten()
            .collect();
        woken.extend(mem::take(&mut lock(&self.timers).pending).into_values());

        for waker in woken {
            waker.wake();
        }
    }

    /// No registered source and no pending timer.
    pub(super) fn is_idle(&self) -> bool {
        let no_sources = lock(&self.sources).states.is_empty();

        no_sources && lock(&self.timers).pending.is_empty()
    }
}

impl Inner {
    /// Puts `source` under the reactor's watch, with a key of its own to report it by.
    pub(super) fn register<S>(self: &Arc<Self>, source: S) -> io::Result<Registration>
    where
        S: Source,
    {
        let mut sources = lock(&self.reactor.sources);
        // One `select` takes a fixed number of sockets, one place of which is spoken for by the
        // channel that breaks the wait.
        #[cfg(windows)]
        if sources.states.len() >= super::poll::MAX_SOURCES {
            return Err(io::Error::other(format!(
                "this runtime watches at most {} sockets",
                super::poll::MAX_SOURCES
            )));
        }
        let key = sources.next_key;
        let state = Arc::new(SourceState {
            source: Arc::new(source),
            wakers: Mutex::default(),
            key,
        });
        sources.next_key += 1;
        sources.states.insert(key, state.clone());

        Ok(Registration {
            runtime: self.clone(),
            state,
        })
    }

    /// A timer that is due once `duration` has passed, or one that never comes due where the
    /// clock cannot reach that far.
    pub(super) fn sleep(self: &Arc<Self>, duration: Duration) -> Sleep {
        Sleep {
            runtime: self.clone(),
            // This reactor's timers run on the standard clock, so a length of time is a deadline
            // on it — where the clock has a moment that far ahead. `Duration::MAX`, which a
            // wait of "however long it takes" comes to, has none, and asks for a timer that
            // never fires rather than for a moment the clock cannot name.
            deadline: Instant::now().checked_add(duration),
            id: None,
        }
    }
}

/// One source this reactor watches, and what its owner does its I/O through.
pub struct Registration {
    runtime: Arc<Inner>,
    state: Arc<SourceState>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration").finish_non_exhaustive()
    }
}

impl Registration {
    /// Runs `operation` while this registration's source is ready for `interest`, retrying it as
    /// new readiness arrives.
    ///
    /// The first success `operation` returns, a partial write included, and the first error
    /// other than [`WouldBlock`](io::ErrorKind::WouldBlock) are each what this call resolves to.
    /// A `WouldBlock` means the source was not ready after all: `cx`'s waker is arranged to be
    /// woken once it is, and [`Poll::Pending`] is returned.
    ///
    /// A registration keeps one waker per interest, so only one operation at a time may be
    /// waiting on each: a second one waiting to read, say, takes the first one's place, which is
    /// then never woken. One reader and one writer waiting together is fine; more of either
    /// have to take turns, behind a lock of their own.
    ///
    /// Never spins while the source is not ready and, the source being nonblocking as
    /// [`Runtime::register`](crate::Runtime::register) requires, never blocks the calling thread.
    pub fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        match operation() {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            result => return Poll::Ready(result),
        }

        let replaced = {
            let mut wakers = lock(&self.state.wakers);
            let stored = match interest {
                Interest::Readable => &mut wakers.readable,
                Interest::Writable => &mut wakers.writable,
            };

            stored.replace(cx.waker().clone())
        };
        // Clear of the lock: dropping a waker can drop a task, and the future of that task may
        // hold a registration or a timer of this reactor, each of which takes a lock as it goes.
        drop(replaced);
        // The wait under way was built before this waker was stored, so it is broken here and
        // the one that follows it watches this source.
        self.runtime.reactor.notify();

        Poll::Pending
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // The state goes with this registration, and with it the reactor's clone of the source;
        // a wait holding a clone of that source keeps the descriptor open until it returns, so
        // the watch is over before the descriptor can close.
        let removed = {
            let mut sources = lock(&self.runtime.reactor.sources);

            sources.states.remove(&self.state.key)
        };
        // Clear of the lock: the state that came out of the map may hold a waker, and dropping
        // a waker can drop a task whose future holds a registration or a timer of this reactor,
        // each of which takes a lock as it goes.
        drop(removed);
        // Clear of the lock as well: the wait built from a source that has gone is broken, so
        // that the wait after it leaves that source out.
        self.runtime.reactor.notify();
    }
}

/// A timer this reactor wakes once its deadline has passed.
pub(super) struct Sleep {
    runtime: Arc<Inner>,
    /// When this timer comes due, and nothing where the clock has no such moment: a timer that
    /// never fires.
    deadline: Option<Instant>,
    /// Handed out on the first poll, which is when the timer joins the reactor's map.
    id: Option<u64>,
}

impl Sleep {
    /// The runtime this timer belongs to.
    pub(super) fn runtime(&self) -> &Arc<Inner> {
        &self.runtime
    }

    /// Whether this timer has no deadline the clock can name, and so never comes due.
    pub(super) fn never_fires(&self) -> bool {
        self.deadline.is_none()
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // A timer that never comes due registers nothing and stores no waker: there is nothing
        // that could ever wake it, which is what was asked for.
        let Some(deadline) = this.deadline else {
            return Poll::Pending;
        };
        if Instant::now() >= deadline {
            return Poll::Ready(());
        }

        let (first_deadline, replaced) = {
            let mut timers = lock(&this.runtime.reactor.timers);
            let id = match this.id {
                Some(id) => id,
                None => {
                    let id = timers.next_id;
                    timers.next_id += 1;
                    this.id = Some(id);

                    id
                }
            };
            let key = (deadline, id);
            // A later poll of the same timer replaces the waker under the key it already has.
            let replaced = timers.pending.insert(key, cx.waker().clone());

            (timers.pending.keys().next() == Some(&key), replaced)
        };
        // Whether this poll put the deadline in rather than found it there, read before the
        // waker it replaced is let go of.
        let stored = replaced.is_none();
        // Clear of the lock: dropping a waker can drop a task, and the future of that task may
        // hold a timer of this reactor, whose own drop takes this very lock.
        drop(replaced);
        // A deadline behind one the map already holds changes nothing about the wait under way,
        // and neither does a poll that only puts a fresh waker under a key the map has. A poll
        // that stores a deadline the map had lost — a failed wait drops every timer it holds —
        // brings in an earliest deadline just as a first poll does.
        if stored && first_deadline {
            this.runtime.reactor.notify();
        }

        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        // Nothing in the map for a timer nobody ever polled, and none either for one that never
        // comes due, which no poll of it registers.
        let (Some(deadline), Some(id)) = (self.deadline, self.id) else {
            return;
        };
        let key = (deadline, id);
        let (was_first, removed) = {
            let mut timers = lock(&self.runtime.reactor.timers);
            let was_first = timers.pending.keys().next() == Some(&key);

            (was_first, timers.pending.remove(&key))
        };
        // Clear of the lock: dropping a waker can drop a task whose future holds a timer of
        // this reactor, whose own drop takes this very lock.
        drop(removed);
        // A thread whose wait is bounded by nothing but this deadline can retire at once rather
        // than sit out a deadline nobody waits for any more.
        if was_first {
            self.runtime.reactor.notify();
        }
    }
}

/// The sources the reactor watches, under the keys it reports them by.
#[derive(Default)]
struct Sources {
    states: HashMap<usize, Arc<SourceState>>,
    next_key: usize,
}

/// One watched source: the descriptor, and who to wake for each direction of it.
struct SourceState {
    source: Arc<dyn Source>,
    wakers: Mutex<Wakers>,
    key: usize,
}

/// Who waits for each direction of one source.
#[derive(Default)]
struct Wakers {
    readable: Option<Waker>,
    writable: Option<Waker>,
}

/// The timers waiting for their deadline, keyed so that two of the same deadline stay apart.
#[derive(Default)]
struct Timers {
    pending: BTreeMap<(Instant, u64), Waker>,
    next_id: u64,
}

/// The value behind a lock, taken whether or not a panic poisoned it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, pin::pin, task::Wake, thread};

    #[cfg(unix)]
    use std::os::fd::OwnedFd;
    #[cfg(windows)]
    use std::os::windows::io::OwnedSocket;

    use ntest::timeout;
    use socket2::{SockRef, Socket};

    use super::*;

    #[test]
    #[timeout(15000)]
    fn a_readable_source_wakes_its_waker() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(counter.count(), 1);
        assert!(matches!(
            read_one(&registration, &source, &mut cx),
            Poll::Ready(Ok(1))
        ));
    }

    #[test]
    #[timeout(15000)]
    fn a_source_written_before_registration_is_seen() {
        let runtime = runtime();
        let (source, mut peer) = pair();
        peer.write_all(&[7]).unwrap();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();

        let read = read_one(&registration, &source, &mut Context::from_waker(&waker));

        assert!(matches!(read, Poll::Ready(Ok(1))));
        assert_eq!(counter.count(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn notify_breaks_a_wait() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let breaker = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                let reactor = &runtime.reactor;
                thread::sleep(Duration::from_millis(50));
                reactor.notify();
            })
        };

        let started = Instant::now();
        reactor.wait(None).unwrap();

        assert!(started.elapsed() < Duration::from_secs(1));
        breaker.join().unwrap();
    }

    /// A burst of wakes with no wait between them puts one wake-up in the channel, not ten.
    ///
    /// The wait that follows is unbounded, so it can only end on the wake-up the first of them
    /// wrote; that it ends at all is what says the burst was not swallowed along with the nine
    /// writes it saved.
    #[test]
    #[timeout(15000)]
    fn many_notifies_between_waits_write_once() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        // From a thread of its own: the thread running a reactor is the one caller that needs
        // no telling, and this test has no such thread at all.
        let notifying = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                let reactor = &runtime.reactor;
                reactor.notify();
                assert!(reactor.wake_pending(), "the first wake reached the channel");

                for _ in 0..9 {
                    reactor.notify();
                }
                assert!(reactor.wake_pending(), "the wake-up is there to be taken");
            })
        };
        notifying.join().unwrap();

        reactor.wait(None).unwrap();

        assert!(!reactor.wake_pending(), "the wait took the wake-up out");
    }

    #[test]
    #[timeout(15000)]
    fn a_wait_ends_at_the_nearest_deadline() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (counter, waker) = counting_waker();
        // Far enough ahead that the deadline lies past the wait under test even where the
        // machine stalls between that wait and the poll below.
        let mut sleep = pin!(runtime.sleep(Duration::from_millis(200)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
        // That poll broke the wait a thread running the reactor would have been in, and this
        // stands in for it, so that the wait measured below is bounded by the deadline and by
        // nothing else.
        reactor.wait(Some(Duration::ZERO)).unwrap();

        let started = Instant::now();
        // The wake-up written when the deadline was stored may reach the channel after the drain
        // above. A wait that ends on it finds nothing due, and the next one is bounded by the
        // deadline alone.
        while counter.count() == 0 {
            reactor.wait(None).unwrap();
        }

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(counter.count(), 1);
        assert!(reactor.is_idle());
    }

    #[test]
    #[timeout(15000)]
    fn a_dropped_sleep_leaves_no_deadline() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (counter, waker) = counting_waker();
        {
            let mut sleep = pin!(runtime.sleep(Duration::from_secs(60)));
            let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
            assert!(polled.is_pending());
            assert!(!reactor.is_idle());
        }

        assert!(reactor.is_idle());
        assert_eq!(counter.count(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn a_dropped_registration_stops_the_watch() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());

        drop(registration);
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_millis(50))).unwrap();

        assert_eq!(counter.count(), 0);
        assert!(reactor.is_idle());
    }

    #[test]
    #[timeout(15000)]
    fn two_sleeps_with_one_deadline_both_fire() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        // Built by hand from the one deadline: `Inner::sleep` reads the clock afresh for each
        // timer, and two deadlines nanoseconds apart would keep the pair apart on their own.
        let deadline = Instant::now() + Duration::from_millis(5);
        let mut one = pin!(Sleep {
            runtime: runtime.clone(),
            deadline: Some(deadline),
            id: None,
        });
        let mut other = pin!(Sleep {
            runtime: runtime.clone(),
            deadline: Some(deadline),
            id: None,
        });
        assert!(
            one.as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            other
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );

        // Two entries under the one deadline: what tells them apart is the id beside it.
        assert_eq!(lock(&reactor.timers).pending.len(), 2);
        // A wait ends at the deadline or at the notification the first poll left behind, so it
        // takes as many as it takes for both of these to come due.
        while first.count() == 0 || second.count() == 0 {
            reactor.wait(None).unwrap();
        }

        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 1);
        assert!(reactor.is_idle());
    }

    #[test]
    #[timeout(15000)]
    fn a_failed_wait_wakes_everything() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (source, _peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (reader, reader_waker) = counting_waker();
        let (timer, timer_waker) = counting_waker();
        let mut cx = Context::from_waker(&reader_waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&timer_waker));
        assert!(polled.is_pending());

        reactor.wake_everything();

        assert_eq!(reader.count(), 1);
        assert_eq!(timer.count(), 1);
    }

    /// A timer for longer than the clock reaches never comes due.
    ///
    /// `Duration::MAX` is what a wait of "however long it takes" comes to, and the moment it
    /// names lies past the end of the standard clock. Such a timer registers nothing and stores
    /// no waker: nothing is ever going to fire it, which is what was asked for.
    #[test]
    #[timeout(15000)]
    fn a_sleep_beyond_the_clock_never_fires() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::MAX));

        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));

        assert!(polled.is_pending());
        assert_eq!(counter.count(), 0);
        // Nothing went into the map, so nothing here keeps a thread on this reactor.
        assert!(reactor.is_idle());
    }

    /// A deadline stored again after a failed wait dropped it breaks the wait under way.
    ///
    /// A wait that fails wakes every timer and leaves the map empty, so each timer that had not
    /// come due is polled afresh and stores its deadline again. The thread in the seat may be in
    /// a wait that a source of its own keeps alive by then, and a deadline put back that way is
    /// news to that wait just as one stored for the first time would be.
    #[test]
    #[timeout(15000)]
    fn a_deadline_stored_again_breaks_the_wait() {
        let runtime = runtime();
        let reactor = &runtime.reactor;
        let (first, first_waker) = counting_waker();
        // Far enough ahead that it is still to come while the wait below is under way.
        let mut sleep = pin!(runtime.sleep(Duration::from_millis(500)));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        // The wake-up that poll wrote, taken out of the channel so that the wait below can only
        // end on one written after it.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        // What a failed wait falls back on: every timer woken, and the map left empty.
        reactor.wake_everything();
        assert_eq!(first.count(), 1);
        assert!(reactor.is_idle());

        let waiting = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                let reactor = &runtime.reactor;
                let started = Instant::now();
                reactor.wait(None).unwrap();

                started.elapsed()
            })
        };
        // Long enough for the thread above to reach its wait, which has nothing to watch and no
        // deadline to end at.
        thread::sleep(Duration::from_millis(50));

        let (second, second_waker) = counting_waker();
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );

        assert!(waiting.join().unwrap() < Duration::from_secs(1));
        // And the deadline that wait learned of is the one the waits after it end at.
        while second.count() == 0 {
            reactor.wait(None).unwrap();
        }
        assert!(reactor.is_idle());
    }

    /// A reactor, held the way the thread running a runtime holds one.
    fn runtime() -> Arc<Inner> {
        Inner::new().unwrap()
    }

    /// A counter and the waker that counts into it.
    fn counting_waker() -> (Arc<Counter>, Waker) {
        let counter = Arc::new(Counter::default());
        let waker = Waker::from(counter.clone());

        (counter, waker)
    }

    /// A source shareable between the registration a test makes and the reads or writes it
    /// makes directly: an owned descriptor of its own, rather than `Arc<dyn Source>`, so that
    /// registering a clone of it needs no unsized coercion of an already-erased source.
    #[cfg(unix)]
    type TestSource = Arc<OwnedFd>;
    #[cfg(windows)]
    type TestSource = Arc<OwnedSocket>;

    /// A connected pair: the source to register, and the far end to drive it from.
    fn pair() -> (TestSource, Socket) {
        let (near, far) = connected();
        near.set_nonblocking(true).unwrap();
        far.set_nonblocking(true).unwrap();

        (owned_source(near), far)
    }

    /// Wraps `socket` as a [`TestSource`]: one clone goes to [`Inner::register`], the other is
    /// kept for the reads and writes the tests make directly.
    #[cfg(unix)]
    fn owned_source(socket: Socket) -> TestSource {
        let owned: OwnedFd = socket.into();

        Arc::new(owned)
    }

    /// Wraps `socket` as a [`TestSource`]: one clone goes to [`Inner::register`], the other is
    /// kept for the reads and writes the tests make directly.
    #[cfg(windows)]
    fn owned_source(socket: Socket) -> TestSource {
        let owned: OwnedSocket = socket.into();

        Arc::new(owned)
    }

    /// Two sockets connected to one another.
    #[cfg(unix)]
    fn connected() -> (Socket, Socket) {
        socket2::Socket::pair(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap()
    }

    /// Two sockets connected to one another.
    ///
    /// Winsock has no socket pair, so this is a loopback connection which a listener of its own
    /// accepts and then has no further use for.
    #[cfg(windows)]
    fn connected() -> (Socket, Socket) {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let far = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let near = loop {
            let (accepted, _) = listener.accept().unwrap();
            // A loopback listener is reachable by anything else on the machine, so a connection
            // that is not the one made just above is turned away rather than taken for it.
            if accepted.peer_addr().unwrap() == far.local_addr().unwrap() {
                break accepted;
            }
        };

        // One-byte messages travel this pair, as they do the poller's wake pair, so neither
        // end holds a send back for the peer's acknowledgement of the one before it.
        near.set_nodelay(true).unwrap();
        far.set_nodelay(true).unwrap();

        (near.into(), far.into())
    }

    /// Reads one byte from `source`, the way a caller reads its socket.
    fn read_one(
        registration: &Registration,
        source: &TestSource,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];

        registration.poll_io(cx, Interest::Readable, || {
            SockRef::from(source).recv(&mut byte)
        })
    }

    /// A waker that counts how often it has been woken.
    #[derive(Default)]
    struct Counter(Mutex<usize>);

    impl Counter {
        /// How often this waker has been woken.
        fn count(&self) -> usize {
            *lock(&self.0)
        }
    }

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *lock(&self.0) += 1;
        }
    }
}
