//! What watches a runtime's sockets and keeps its timers.
//!
//! A reactor holds every source registered on it and every timer taken from it, and the thread
//! driving the runtime hands it that thread through [`Reactor::wait`]: one wait on the platform's
//! poll, bounded by the nearest deadline, and then the wakes for whatever that wait found ready
//! and for whatever timer has come due.
//!
//! The poll is level-triggered, and nothing here arms or disarms a source as readiness comes and
//! goes. Each wait is told afresh what to watch, worked out from the wakers stored for each
//! source, so a source nobody waits for any more is simply left out of the next wait, and one
//! that became ready between two waits is reported by the second. That is also why a waiter which
//! stores a waker breaks the wait under way: that wait was built before the waker existed, and
//! only the wait after it takes the source in.
//!
//! Two locks guard the sources and the timers, and neither is taken while the other is held. A
//! source's wakers live in its entry in the map, under the map's lock. Neither lock is held
//! across the wait, which may last until a deadline, nor across a wake or the drop of a waker or
//! a source, each of which runs somebody else's code and may come straight back here to register
//! a source, to ask for a timer or to let either go. The flag that spares a `notify` its write
//! while a wake-up is on its way needs no lock: it is an atomic of the runtime's remote.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    io, mem,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::{
    Interest, Local, Mode,
    mode::sealed::Lock,
    poll::Want,
    runtime::{Core, Remote},
};

/// The sockets one runtime watches and the timers it keeps.
pub(crate) struct Reactor<M>
where
    M: Mode,
{
    sources: M::Lock<Sources<M>>,
    timers: M::Lock<Timers>,
    /// The poller a wait is made on, and the flag that says a wake-up is on its way to it.
    remote: Arc<Remote>,
}

impl<M> Reactor<M>
where
    M: Mode,
{
    /// A reactor watching nothing, which waits on `remote`'s poller.
    pub(crate) fn new(remote: Arc<Remote>) -> Self {
        Self {
            sources: Lock::new(Sources {
                states: HashMap::new(),
                next_key: 0,
            }),
            timers: Lock::new(Timers::default()),
            remote,
        }
    }

    /// One wait on the poller, bounded by `at_most` and by the nearest deadline, then the wakes
    /// for what it found ready and for the timers that are due.
    pub(crate) fn wait(&self, at_most: Option<Duration>) -> io::Result<()> {
        // Clones of the sources rather than borrows of them: the wait is made with no lock held,
        // and a registration let go of while it runs on another thread leaves its descriptor
        // open until the wait returns.
        let wants: Vec<(M::SourcePtr, Want)> = self
            .sources
            .lock()
            .states
            .iter()
            .filter_map(|(&key, state)| {
                let want = Want {
                    key,
                    readable: state.wakers.readable.is_some(),
                    writable: state.wakers.writable.is_some(),
                };

                (want.readable || want.writable).then(|| (state.source.clone(), want))
            })
            .collect();
        let deadline = self
            .timers
            .lock()
            .pending
            .keys()
            .next()
            .map(|(deadline, _)| *deadline);
        let until_deadline = deadline.map(|at| at.saturating_duration_since(Instant::now()));
        let timeout = match (at_most, until_deadline) {
            (Some(at_most), Some(until_deadline)) => Some(at_most.min(until_deadline)),
            (bound, None) | (None, bound) => bound,
        };

        let ready = self.remote.poller.wait(&wants, M::as_source, timeout)?;
        // The wait takes whatever wake-up it finds out of the channel, so the flag comes down
        // here and the next caller writes again. One that came between the two is turned away
        // without a write, and loses nothing by it: what it had to say — a task queued, a source
        // registered, a deadline stored — it said before it called, and the rounds below and the
        // ones the driving thread makes look at all three afresh. The other way about, a wake-up
        // left in the channel by a wait that ended some other way only ends the next one at once.
        self.remote.wake_pending.store(false, Ordering::Release);
        // Clear of every lock: the last clone of a source let go of during the wait closes it,
        // which runs the destructor of whatever was registered.
        drop(wants);

        let woken = {
            let mut sources = self.sources.lock();
            let mut woken = Vec::new();
            for event in &ready {
                // A source let go of while the wait ran has nobody left to wake.
                let Some(state) = sources.states.get_mut(&event.key) else {
                    continue;
                };
                if event.readable {
                    woken.extend(state.wakers.readable.take());
                }
                if event.writable {
                    woken.extend(state.wakers.writable.take());
                }
            }

            woken
        };
        for waker in woken {
            waker.wake();
        }

        let due = {
            let mut timers = self.timers.lock();
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
    pub(crate) fn wake_everything(&self) {
        let mut woken: Vec<Waker> = self
            .sources
            .lock()
            .states
            .values_mut()
            .flat_map(|state| [state.wakers.readable.take(), state.wakers.writable.take()])
            .flatten()
            .collect();
        woken.extend(mem::take(&mut self.timers.lock().pending).into_values());

        for waker in woken {
            waker.wake();
        }
    }

    /// No registered source and no pending timer.
    #[cfg(any(test, feature = "helper"))]
    pub(crate) fn is_idle(&self) -> bool {
        let no_sources = self.sources.lock().states.is_empty();

        no_sources && self.timers.lock().pending.is_empty()
    }
}

/// Puts `source` under the watch of `core`'s reactor, with a key of its own to report it by.
pub(crate) fn register<M>(
    core: &M::Ptr<Core<M>>,
    source: M::SourcePtr,
) -> io::Result<Registration<M>>
where
    M: Mode,
{
    let key = {
        let mut sources = core.reactor.sources.lock();
        // One `select` takes a fixed number of sockets, one place of which is spoken for by the
        // channel that breaks the wait.
        #[cfg(windows)]
        if sources.states.len() >= crate::poll::MAX_SOURCES {
            return Err(io::Error::other(format!(
                "this runtime watches at most {} sockets",
                crate::poll::MAX_SOURCES
            )));
        }
        let key = sources.next_key;
        sources.next_key += 1;
        sources.states.insert(
            key,
            SourceState {
                source,
                wakers: Wakers::default(),
            },
        );

        key
    };

    Ok(Registration {
        core: core.clone(),
        key,
    })
}

/// A timer of `core`'s reactor that is due once `duration` has passed, or one that never comes
/// due where the clock cannot reach that far.
pub(crate) fn sleep<M>(core: &M::Ptr<Core<M>>, duration: Duration) -> Sleep<M>
where
    M: Mode,
{
    Sleep {
        core: core.clone(),
        // This reactor's timers run on the standard clock, so a length of time is a deadline on
        // it — where the clock has a moment that far ahead. `Duration::MAX`, which a wait of
        // "however long it takes" comes to, has none, and asks for a timer that never fires
        // rather than for a moment the clock cannot name.
        deadline: Instant::now().checked_add(duration),
        id: None,
    }
}

/// A source a [`Runtime`](crate::Runtime) watches, and what its owner does its I/O through.
///
/// A registration holds its runtime, so a task holding one keeps that runtime alive: nothing
/// here takes a runtime down while it has work.
pub struct Registration<M = Local>
where
    M: Mode,
{
    core: M::Ptr<Core<M>>,
    key: usize,
}

impl<M> fmt::Debug for Registration<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration").finish_non_exhaustive()
    }
}

impl<M> Registration<M>
where
    M: Mode,
{
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
    /// [`Runtime::register`](crate::Runtime#method.register) requires, never blocks the calling
    /// thread.
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

        // Cloned before the lock is taken: a waker's clone is somebody else's code, which is not
        // to run with a lock of this runtime's held.
        let waker = cx.waker().clone();
        let replaced = {
            let mut sources = self.core.reactor.sources.lock();
            let Some(state) = sources.states.get_mut(&self.key) else {
                unreachable!("a source stays in the map for as long as its registration lives");
            };
            let stored = match interest {
                Interest::Readable => &mut state.wakers.readable,
                Interest::Writable => &mut state.wakers.writable,
            };

            stored.replace(waker)
        };
        // Clear of the lock: dropping a waker can drop a task, and the future of that task may
        // hold a registration or a timer of this reactor, each of which takes a lock as it goes.
        drop(replaced);
        // The wait under way was built before this waker was stored, so it is broken here and
        // the one that follows it watches this source.
        self.core.remote.notify();

        Poll::Pending
    }
}

impl<M> Drop for Registration<M>
where
    M: Mode,
{
    fn drop(&mut self) {
        // The entry goes with this registration, and with it the reactor's clone of the source;
        // a wait holding a clone of that source keeps the descriptor open until it returns, so
        // the watch is over before the descriptor can close.
        let removed = self.core.reactor.sources.lock().states.remove(&self.key);
        // Clear of the lock: the entry that came out of the map may hold a waker, and dropping a
        // waker can drop a task whose future holds a registration or a timer of this reactor,
        // each of which takes a lock as it goes. The source itself may be the last clone of
        // what was registered, whose destructor is somebody else's code.
        drop(removed);
        // Clear of the lock as well: the wait built from a source that has gone is broken, so
        // that the wait after it leaves that source out.
        self.core.remote.notify();
    }
}

/// A timer this reactor wakes once its deadline has passed.
pub(crate) struct Sleep<M>
where
    M: Mode,
{
    core: M::Ptr<Core<M>>,
    /// When this timer comes due, and nothing where the clock has no such moment: a timer that
    /// never fires.
    deadline: Option<Instant>,
    /// Handed out on the first poll, which is when the timer joins the reactor's map.
    id: Option<u64>,
}

impl<M> Sleep<M>
where
    M: Mode,
{
    /// The runtime this timer belongs to.
    pub(crate) fn core(&self) -> &M::Ptr<Core<M>> {
        &self.core
    }

    /// Whether this timer has no deadline the clock can name, and so never comes due.
    pub(crate) fn never_fires(&self) -> bool {
        self.deadline.is_none()
    }
}

impl<M> Future for Sleep<M>
where
    M: Mode,
{
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

        // Cloned before the lock is taken: a waker's clone is somebody else's code, which is not
        // to run with a lock of this runtime's held.
        let waker = cx.waker().clone();
        let (first_deadline, replaced) = {
            let mut timers = this.core.reactor.timers.lock();
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
            let replaced = timers.pending.insert(key, waker);

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
            this.core.remote.notify();
        }

        Poll::Pending
    }
}

impl<M> Drop for Sleep<M>
where
    M: Mode,
{
    fn drop(&mut self) {
        // Nothing in the map for a timer nobody ever polled, and none either for one that never
        // comes due, which no poll of it registers.
        let (Some(deadline), Some(id)) = (self.deadline, self.id) else {
            return;
        };
        let key = (deadline, id);
        let (was_first, removed) = {
            let mut timers = self.core.reactor.timers.lock();
            let was_first = timers.pending.keys().next() == Some(&key);

            (was_first, timers.pending.remove(&key))
        };
        // Clear of the lock: dropping a waker can drop a task whose future holds a timer of
        // this reactor, whose own drop takes this very lock.
        drop(removed);
        // A thread whose wait is bounded by nothing but this deadline can stop waiting at once
        // rather than sit out a deadline nobody waits for any more.
        if was_first {
            self.core.remote.notify();
        }
    }
}

/// The sources the reactor watches, under the keys it reports them by.
struct Sources<M>
where
    M: Mode,
{
    states: HashMap<usize, SourceState<M>>,
    next_key: usize,
}

/// One watched source: the descriptor, and who to wake for each direction of it.
struct SourceState<M>
where
    M: Mode,
{
    source: M::SourcePtr,
    wakers: Wakers,
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

#[cfg(test)]
mod tests {
    use std::{io::Write, pin::pin, sync::atomic::AtomicUsize, task::Wake, thread};

    #[cfg(unix)]
    use std::os::fd::OwnedFd;
    #[cfg(windows)]
    use std::os::windows::io::OwnedSocket;

    use ntest::timeout;
    use socket2::{SockRef, Socket};

    use super::*;
    use crate::{Shared, SharedRuntime};

    #[test]
    #[timeout(15000)]
    fn a_readable_source_wakes_its_waker() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
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
        let reactor = &runtime.core.reactor;
        let breaker = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                runtime.core.remote.notify();
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
        let reactor = &runtime.core.reactor;
        // From a thread of its own: the thread driving a runtime is the one caller that needs
        // no telling, and this test has no such thread at all.
        let notifying = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                let remote = &runtime.core.remote;
                remote.notify();
                assert!(remote.wake_pending(), "the first wake reached the channel");

                for _ in 0..9 {
                    remote.notify();
                }
                assert!(remote.wake_pending(), "the wake-up is there to be taken");
            })
        };
        notifying.join().unwrap();

        reactor.wait(None).unwrap();

        assert!(
            !runtime.core.remote.wake_pending(),
            "the wait took the wake-up out"
        );
    }

    #[test]
    #[timeout(15000)]
    fn a_wait_ends_at_the_nearest_deadline() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
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
        let reactor = &runtime.core.reactor;
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
        let reactor = &runtime.core.reactor;
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
        let reactor = &runtime.core.reactor;
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        // Built by hand from the one deadline: `sleep` reads the clock afresh for each
        // timer, and two deadlines nanoseconds apart would keep the pair apart on their own.
        let deadline = Instant::now() + Duration::from_millis(5);
        let mut one = pin!(Sleep::<Shared> {
            core: runtime.core.clone(),
            deadline: Some(deadline),
            id: None,
        });
        let mut other = pin!(Sleep::<Shared> {
            core: runtime.core.clone(),
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
        assert_eq!(Lock::lock(&reactor.timers).pending.len(), 2);
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
        let reactor = &runtime.core.reactor;
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
        let reactor = &runtime.core.reactor;
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
    /// come due is polled afresh and stores its deadline again. The thread driving the runtime
    /// may be in a wait that a source of its own keeps alive by then, and a deadline put back that
    /// way is news to that wait just as one stored for the first time would be.
    #[test]
    #[timeout(15000)]
    fn a_deadline_stored_again_breaks_the_wait() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
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
                let reactor = &runtime.core.reactor;
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

    /// A runtime of the test's own, whose reactor the test waits on as the thread driving it
    /// would. Shared, so that a test can hand it to a thread of its own.
    fn runtime() -> SharedRuntime {
        SharedRuntime::new().unwrap()
    }

    /// A counter and the waker that counts into it.
    fn counting_waker() -> (Arc<Counter>, Waker) {
        let counter = Arc::new(Counter::default());
        let waker = Waker::from(counter.clone());

        (counter, waker)
    }

    /// A source shareable between the registration a test makes and the reads or writes it
    /// makes directly: an owned descriptor of its own, behind an `Arc` of the test's own.
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

    /// Wraps `socket` as a [`TestSource`]: one clone goes to [`register`], the other is
    /// kept for the reads and writes the tests make directly.
    #[cfg(unix)]
    fn owned_source(socket: Socket) -> TestSource {
        let owned: OwnedFd = socket.into();

        Arc::new(owned)
    }

    /// Wraps `socket` as a [`TestSource`]: one clone goes to [`register`], the other is
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
        registration: &Registration<Shared>,
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
    struct Counter(AtomicUsize);

    impl Counter {
        /// How often this waker has been woken.
        fn count(&self) -> usize {
            self.0.load(Ordering::Acquire)
        }
    }

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
}
