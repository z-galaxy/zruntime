//! What watches a runtime's sockets and keeps its timers.
//!
//! A reactor holds every source registered on it and every timer taken from it, and the thread
//! driving the runtime hands it that thread through [`Reactor::wait`]: one wait on the platform's
//! poller, bounded by the nearest deadline, and then the wakes for whatever that wait found ready
//! and for whatever timer has come due. Where the runtime was asked to busy-poll, the wait is
//! preceded by a spin (see [`BusyPoll`]).
//!
//! The poller watches each source in the directions the reactor last told it to, and is told of a
//! change rather than handed every source on each wait, so that a wait costs what the sources
//! found ready cost and no more, where the platform's poller allows. A direction is watched from
//! the moment somebody waits in it: the waiter that stores the first waker of a direction has the
//! poller told there and then, and breaks the wait under way where the platform's poller sees the
//! change only in the wait after it. A direction nobody waits in any more is let go of later, and
//! only where nobody has come back to wait in it again by then: a task woken by readiness usually
//! stores its waker again as it runs out of what to read, and that costs the poller nothing. A
//! source a wait found ready, or one a waiter gave up on, is listed for that look.
//!
//! When the look comes depends on how the poller reports a source. One that keeps its list in user
//! space is level-triggered: it reports a source watched in a direction it is ready in at every
//! wait until it is not, so every source listed is looked at before every wait, which costs no
//! system call there. One that keeps what it watches in the kernel, epoll or kqueue, is
//! edge-triggered ([`Poller::EDGE`]): it reports a source as it turns ready in a direction, or as
//! more comes in there, and not again while it only stays ready. A source a wait found ready costs
//! the waits after it nothing, then, until there is news of it, whether or not the task it woke has
//! been polled by then, and a wait that cannot sleep, as a task is queued, leaves it as it is:
//! letting a direction go and taking it in again as the task comes back to it would cost two
//! system calls. News that nobody waits for would wake a thread that sleeps, though, as a peer
//! reading what was written to a socket that nobody writes to any more makes room in it, so every
//! source listed is looked at before a wait that may sleep, by when every task queued has been
//! polled. And as no wait reports a source again unless there is news, the reactor counts what
//! the waits report of it, and keeps that count as of the latest operation that found it not
//! ready (see [`SourceState::reports`]).
//!
//! Two locks guard the sources and the timers, and neither is taken while the other is held. A
//! source's wakers live in its entry in the map, under the map's lock, and every change the poller
//! is told of is made under that lock too, so that two of them never reach it out of order. Neither
//! lock is held across the wait, which may last until a deadline, nor across a wake or the drop of
//! a waker or of the last clone of a source, each of which runs somebody else's code and may come
//! straight back here to register a source, to ask for a timer or to let either go. The flag that
//! spares a `notify` its write while a wake-up is on its way needs no lock: it is an atomic of the
//! runtime's remote.
//!
//! The state of busy polling is behind a lock of its own, a third. The limit is set from whichever
//! thread the caller is on, and the history is the driving thread's: the lock is taken briefly
//! before the wait and after it, never across it, and never while either of the other two is
//! held, so that a limit can be set while the driving thread is in the middle of a wait.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    future::Future,
    io, mem,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Waker, ready},
    time::{Duration, Instant},
};

use crate::{
    Interest, Local, Mode, coop,
    log::error,
    mode::sealed::Lock,
    poll::{self, Directions, Poller, RawSource},
    runtime::{Core, Remote, drives},
};

/// The sockets one runtime watches and the timers it keeps.
pub(crate) struct Reactor<M>
where
    M: Mode,
{
    sources: M::Lock<Sources<M>>,
    timers: M::Lock<Timers>,
    /// When a wait spins before it sleeps, and for how long.
    busy_poll: M::Lock<BusyPoll>,
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
                descriptors: HashSet::new(),
                next_key: 0,
                next_waiter: 0,
                stale: Vec::new(),
                waiting: false,
                retired: Vec::new(),
            }),
            timers: Lock::new(Timers::default()),
            busy_poll: Lock::new(BusyPoll::default()),
            remote,
        }
    }

    /// One wait on the poller, bounded by `at_most` and by the nearest deadline, and preceded by a
    /// spin where the runtime busy-polls and the spin is likely to pay; then the wakes for what it
    /// found ready and for the timers that are due.
    ///
    /// An `at_most` of zero says that the caller has something to get back to, a task queued or
    /// the future of `block_on` woken, which may come back to a source it waited on.
    pub(crate) fn wait(&self, at_most: Option<Duration>) -> io::Result<()> {
        {
            let mut sources = self.sources.lock();
            // An edge-triggered poller reports a listed source no more unless there is news of it,
            // so a wait that cannot sleep loses nothing by leaving it as it is, while a look now
            // would let go of it under the task that is about to come back to it. A wait that may
            // sleep follows the polls of every task there was, and would be woken for nothing by
            // news that nobody waits for.
            if !Poller::EDGE || at_most != Some(Duration::ZERO) {
                sources.reconcile(&self.remote.poller)?;
            }
            // A poller that copies its list as a wait starts watches, until the wait returns,
            // descriptors whose registrations may go in the meantime: those are kept open until
            // then, which `Registration::drop` reads this for.
            sources.waiting = !Poller::LIVE;
        }
        let timeout = self.timeout(at_most);

        let ready = match self.wait_on_poller(timeout) {
            Ok(ready) => ready,
            Err(e) => {
                self.end_wait();

                return Err(e);
            }
        };
        // The wait takes whatever wake-up it finds out of the channel, so the flag comes down
        // here and the next caller writes again. One that came between the two is turned away
        // without a write, and loses nothing by it: what it had to say — a task queued, a source
        // registered, a deadline stored — it said before it called, and the rounds below and the
        // ones the driving thread makes look at all three afresh. The other way about, a wake-up
        // left in the channel by a wait that ended some other way only ends the next one at once.
        self.remote.wake_pending.store(false, Ordering::Release);

        let (woken, retired) = {
            let mut sources = self.sources.lock();
            sources.waiting = false;
            let Sources {
                states,
                stale,
                retired,
                ..
            } = &mut *sources;
            let mut woken = Vec::new();
            for event in &ready {
                // A source let go of while the wait ran has nobody left to wake.
                let Some(state) = states.get_mut(&event.key) else {
                    continue;
                };
                state.wakers.take_into(event.directions, &mut woken);
                // A direction the poller let go of by itself is watched no more: the next waiter
                // in it has the poller watch it again.
                state.armed.readable &= !event.dropped.readable;
                state.armed.writable &= !event.dropped.writable;
                // Ready, for all the reactor will hear, where no wait reports it again unless
                // there is news of it.
                if Poller::EDGE {
                    state.reports.count(event.directions);
                }
                // Watched still in what it was found ready in, which nobody may wait in any
                // more: looked at again before a later wait.
                state.mark_stale(event.key, stale);
            }

            (woken, mem::take(retired))
        };
        // Clear of every lock: the last clone of a source whose registration went while the wait
        // ran closes the source, which runs the destructor of whatever was registered.
        drop(retired);
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

    /// Has each wait from the next one on spin for up to `limit` before it sleeps, or, with a
    /// `limit` of zero, not spin at all. The runtime starts over from what it knows of how long
    /// its waits last: it spins once it has seen a wait that lasted no longer than `limit`.
    ///
    /// Callable from any thread, the driving one or not: a wait under way, spin or sleep, carries
    /// on as it began.
    pub(crate) fn set_busy_poll(&self, limit: Duration) {
        self.busy_poll.lock().set_limit(limit);
    }

    /// How long a wait may last: no longer than `at_most`, than until the nearest deadline, or
    /// than [`poll::MAX_TIMEOUT`], and without limit where none of those bounds it.
    fn timeout(&self, at_most: Option<Duration>) -> Option<Duration> {
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

        timeout.map(|timeout| timeout.min(poll::MAX_TIMEOUT))
    }

    /// One wait on the poller, after a spin where the runtime busy-polls and [`BusyPoll`] says
    /// that the spin is likely to pay.
    ///
    /// The spin asks the poller what the wait would, whether a source is ready, again and again
    /// and with a timeout of zero, so that the thread does not go to sleep, and need waking, when
    /// work comes in at short intervals. Only if nothing came while the spin lasted does the wait
    /// go on as it would have without one, for what is left of its timeout.
    ///
    /// A wait is timed from the start of the spin to the end of the sleep after it, and the
    /// history is told how long it lasted, so that a runtime that does not spin yet, or no more,
    /// finds out when its waits come to be short. A wait that fails is not told: it says nothing
    /// of how long work takes to come in.
    fn wait_on_poller(&self, timeout: Option<Duration>) -> io::Result<Vec<poll::Ready>> {
        let poller = &self.remote.poller;
        // No sleep to spare, and nothing for the history to learn from: it keeps how long the
        // waits that could sleep lasted.
        if timeout == Some(Duration::ZERO) {
            return poller.wait(timeout);
        }
        let (on, spin) = {
            let busy_poll = self.busy_poll.lock();
            (busy_poll.is_on(), busy_poll.spin_for(timeout))
        };
        // The default, which reads no clock and keeps nothing.
        if !on {
            return poller.wait(timeout);
        }

        let started = Instant::now();
        let spun = if spin.is_zero() {
            None
        } else {
            self.spin(started, spin)?
        };
        let ready = match spun {
            Some(ready) => ready,
            // The spin ran out, or there was none: the wait goes on for what is left of its
            // timeout, which is none at all where the timeout was what the spin ran out against.
            // A wait without a timeout stays one.
            None => {
                poller.wait(timeout.map(|timeout| timeout.saturating_sub(started.elapsed())))?
            }
        };
        self.busy_poll.lock().record(started.elapsed());

        Ok(ready)
    }

    /// Asks the poller what is ready, again and again and without sleeping, until `spin` has
    /// passed since `started`. Gives back what it found as soon as it finds a source ready or a
    /// wake-up on its way to this thread, which is what would have ended a sleep; `None` where
    /// the time ran out first.
    ///
    /// Each pass is a system call, so the loop needs no pause or hint of its own. Nor does it
    /// reconcile the sources between passes, as `wait` did before it began: this thread runs
    /// nothing meanwhile, so nothing here goes stale, and what other threads change reaches the
    /// poller all the same. A poller that keeps what it watches in the kernel sees a change at
    /// once. One that copies its list as a wait starts copies it afresh on every pass, and the
    /// `notify` a change to it makes raises the flag below, as it breaks a sleep.
    ///
    /// That flag, `Remote::wake_pending`, is what tells that a wake-up is on its way, and the
    /// poller is not asked about it. Every `notify` that reaches the poller raises it first, and
    /// every reason to break a sleep is a `notify`: a task queued, the future of `block_on` woken,
    /// a source armed where the poller sees the change only in the wait after the one under way,
    /// the seat asked for. A pass that finds the wake-up written takes it out of the channel and
    /// reports nothing for it, but the flag stays up until `wait` lowers it, which is after
    /// the spin, so the check that follows the pass sees it. A check that sees the flag before the
    /// write has landed ends the spin with the byte still on its way, which ends the next wait at
    /// once, as it does after a wait that ended some other way (see the end of `wait`).
    /// Nothing is lost by that: what the caller of `notify` had to say it said before it raised
    /// the flag, and the driving thread looks at all of it afresh in the rounds that follow.
    fn spin(&self, started: Instant, spin: Duration) -> io::Result<Option<Vec<poll::Ready>>> {
        loop {
            let ready = self.remote.poller.wait(Some(Duration::ZERO))?;
            if !ready.is_empty() || self.remote.wake_pending.load(Ordering::Acquire) {
                return Ok(Some(ready));
            }
            if started.elapsed() >= spin {
                return Ok(None);
            }
        }
    }

    /// Ends a wait that failed: lets go, clear of every lock, of the sources whose registrations
    /// went while it ran. A wait that succeeds does the same under the lock it takes to wake the
    /// waiters of what it found ready.
    ///
    /// The drop of the last clone of a source closes it, which runs the destructor of whatever
    /// was registered: somebody else's code, which may come straight back here.
    fn end_wait(&self) {
        let retired = {
            let mut sources = self.sources.lock();
            sources.waiting = false;

            mem::take(&mut sources.retired)
        };
        drop(retired);
    }

    /// Wakes every stored waker, sources and timers alike; what a failed wait falls back on, so
    /// that each waiter retries its operation and sees its own error.
    pub(crate) fn wake_everything(&self) {
        let mut woken = Vec::new();
        {
            let mut sources = self.sources.lock();
            let Sources { states, stale, .. } = &mut *sources;
            for (&key, state) in states.iter_mut() {
                let every = Directions {
                    readable: true,
                    writable: true,
                };
                state.wakers.take_into(every, &mut woken);
                // Watched still for the waiters just woken, who may not wait again.
                state.mark_stale(key, stale);
            }
        }
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

    /// Whether a wait with no timeout would spin before it sleeps.
    #[cfg(test)]
    pub(crate) fn would_spin(&self) -> bool {
        !self.busy_poll.lock().spin_for(None).is_zero()
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
    // Read here, once, and never again: a wait watches the descriptor the source lends now, so
    // that it runs no code of the source's. Clear of the lock, as that is somebody else's code.
    #[cfg(any(unix, target_os = "wasi"))]
    let descriptor = std::os::fd::AsRawFd::as_raw_fd(&M::as_source(&source));
    #[cfg(windows)]
    let descriptor = std::os::windows::io::AsRawSocket::as_raw_socket(&M::as_source(&source));
    let key = {
        let mut sources = core.reactor.sources.lock();
        // One descriptor under two keys is more than some of the systems' pollers take: epoll
        // turns the second away, and kqueue would have it take the place of the first. A runtime
        // turns it away on every platform, so that a program does on each what it does on any.
        if !sources.descriptors.insert(descriptor) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "this runtime watches the source's descriptor already",
            ));
        }
        let key = sources.next_key;
        if let Err(e) = core.remote.poller.add(key, descriptor) {
            sources.descriptors.remove(&descriptor);

            return Err(e);
        }
        sources.next_key += 1;
        sources.states.insert(
            key,
            SourceState {
                source,
                descriptor,
                wakers: Wakers::default(),
                armed: Directions::NONE,
                reports: Reports::default(),
                spent: Reports::default(),
                stale: false,
            },
        );

        key
    };

    Ok(Registration {
        core: core.clone(),
        key,
    })
}

/// A timer of `core`'s reactor that comes due at `deadline`, or one that never comes due where
/// there is none.
pub(crate) fn sleep<M>(core: &M::Ptr<Core<M>>, deadline: Option<Instant>) -> Sleep<M>
where
    M: Mode,
{
    Sleep {
        core: core.clone(),
        deadline,
        id: None,
    }
}

/// A source that a [`Runtime`](crate::Runtime) watches, and the handle to do its I/O through.
///
/// A registration holds its runtime, so a task that holds one keeps the runtime alive.
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
    /// Runs `operation` while the source of this registration is ready for `interest`, and retries
    /// it when new readiness arrives.
    ///
    /// Returns the first success of `operation`, a partial write included, or its first error other
    /// than [`WouldBlock`](io::ErrorKind::WouldBlock). A `WouldBlock` means the source was not
    /// ready after all. In that case, this arranges for the waker of `cx` to be woken once it is,
    /// and returns [`Poll::Pending`]. If the OS then refuses to watch the source for `interest`,
    /// this returns that error instead.
    ///
    /// A registration keeps one waker per interest for this, so only one operation at a time can
    /// wait in each direction. If a second operation waits to read, for example, it replaces the
    /// first one, which is then never woken. One reader and one writer can wait together. More of
    /// either must take turns, for example behind a lock, or wait through
    /// [`ready`](Registration::ready), which any number of tasks can do at once, and run their
    /// operation once it completes.
    ///
    /// This never spins while the source is not ready. Since the source is non-blocking, as
    /// [`Runtime::register`](crate::Runtime#method.register) requires, it never blocks the thread
    /// either.
    ///
    /// Nor does it let a source that is always ready keep the thread from the other tasks. A poll
    /// of a task, or of the future passed to `block_on`, may complete 128 I/O operations
    /// through this and through [`AsyncIo`](crate::AsyncIo), which counts them the same way.
    /// The next one yields, and so does every one after it until the poll is over: this wakes the
    /// task itself and returns [`Poll::Pending`] without running `operation`, which sends the task
    /// to the back of the queue. The tasks queued before it have their turn then, and the reactor
    /// has its own once the batch of polls the task is in is over. An `operation` that fails with
    /// `WouldBlock` is not counted.
    ///
    /// The budget belongs to the thread, not to the future. Whatever is polled inside the poll of
    /// a task shares what is left of the task's budget, a blocking executor of another crate
    /// included. Such an executor polls again as soon as it is woken, inside the same poll of the
    /// task, where its operation yields again, so once the budget is spent it never gets its
    /// operation done. Its future has to run inside [`unconstrained`](crate::unconstrained),
    /// which lifts the budget for each poll of it. Polled there, or outside the poll of any runtime
    /// of this crate, this has no limit.
    pub fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        loop {
            ready!(coop::poll_proceed(cx));
            let since = self.reports(interest);
            match operation() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                result => {
                    coop::spend();

                    return Poll::Ready(result);
                }
            }

            // Cloned before the lock is taken: a waker's clone is somebody else's code, which is
            // not to run with a lock of this runtime's held.
            let waker = cx.waker().clone();
            let (stored, unused) = {
                let mut sources = self.core.reactor.sources.lock();
                let Some(state) = sources.states.get_mut(&self.key) else {
                    unreachable!("a source stays in the map for as long as its registration lives");
                };
                if state.found_not_ready(interest, since) {
                    match state.arm(self.key, interest, &self.core.remote.poller) {
                        Ok(told) => (
                            Some(Ok(told)),
                            state.wakers.get_mut(interest).operation.replace(waker),
                        ),
                        Err(e) => (Some(Err(e)), Some(waker)),
                    }
                } else {
                    (None, Some(waker))
                }
            };
            // Clear of the lock: dropping a waker can drop a task, and the future of that task may
            // hold a registration or a timer of this reactor, each of which takes a lock as it
            // goes.
            drop(unused);
            match stored {
                Some(Ok(told)) => {
                    // A poller that sees a change only in the wait after the one under way has
                    // that wait broken here, so that the one after it watches this direction.
                    if told && !Poller::LIVE {
                        self.core.remote.notify();
                    }

                    return Poll::Pending;
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                // A wait on another thread found the source ready while the operation ran, which
                // no wait reports again: the operation is run again rather than left to wait for a
                // report that is not coming.
                None => {}
            }
        }
    }

    /// Waits until the source of this registration is ready for `interest`, without running any
    /// operation on it.
    ///
    /// Use this for I/O done some other way: through a library that the source is handed to, for
    /// example, or with an operation that should run once after the wait rather than on every poll.
    ///
    /// The wait completes once the runtime finds the source ready for `interest`, or once a wait of
    /// the runtime fails. Nothing else completes it. Its first poll only stores the waker, and a
    /// poll for any other reason, such as another future in the same task, leaves it waiting.
    ///
    /// Any number of these can wait at once, on one source and in one direction, alongside an
    /// operation of [`poll_io`](Registration::poll_io). Readiness wakes them all. Dropping one
    /// gives up its wait.
    ///
    /// Readiness is only a hint: another task may take the bytes, or the room, first. A failed wait
    /// of the runtime also ends every wait for readiness, so that each waiter tries its operation
    /// and sees the result for itself. So an operation run after this completes must still expect
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), and wait again when it gets one.
    ///
    /// # Errors
    ///
    /// Fails on its first poll if the OS refuses to watch the source for `interest`.
    pub fn ready(&self, interest: Interest) -> Readiness<'_, M> {
        Readiness {
            registration: self,
            interest,
            before: Before::Unknown,
            id: None,
        }
    }

    /// [`ready`](Registration::ready), for an operation run on the source without this
    /// registration that has just found the source not ready for `interest`, having noted `since`
    /// from [`reports`](Registration::reports) before it ran: the first poll takes that in, as
    /// `poll_io` takes in what its own operation finds, rather than have the poller look at the
    /// source afresh, and completes at once where a wait reported the source while the operation
    /// ran, so that the operation runs again.
    pub(crate) fn ready_again(&self, interest: Interest, since: Option<u32>) -> Readiness<'_, M> {
        Readiness {
            registration: self,
            interest,
            before: Before::NotReady(since),
            id: None,
        }
    }

    /// What an operation in `interest` notes before it runs, so that, should it find the source
    /// not ready, the reactor can tell whether a wait reported the source while it ran: how many
    /// reports of the source in `interest` the waits have made so far.
    ///
    /// That is needed only on an edge-triggered poller ([`Poller::EDGE`]), which reports the source
    /// no more unless there is news, and only off the thread that drives the runtime, where a wait
    /// may run at the same time as the operation. Elsewhere this notes nothing and takes no lock:
    /// any other poller reports a ready source at every wait, and on the thread that drives the
    /// runtime no wait runs between the operation and the storing of its waiter, so whatever a
    /// wait found came before the operation, which found it spent. A thread that does not drive
    /// the runtime cannot tell whether one does at the moment, so it notes the reports even where
    /// nothing drives the runtime, which costs it a look under the lock.
    ///
    /// Inlined into each operation, so that the look at which thread this is costs an operation
    /// on the thread that drives the runtime no call: [`Registration::reports_under_lock`] keeps
    /// the lock out of line.
    #[inline]
    pub(crate) fn reports(&self, interest: Interest) -> Option<u32> {
        if !Poller::EDGE || drives(&self.core.remote) {
            return None;
        }

        Some(self.reports_under_lock(interest))
    }

    /// A handle on the runtime this registration's source is watched by.
    #[cfg(any(
        feature = "tcp",
        all(feature = "unix", unix),
        all(
            feature = "process",
            any(
                target_vendor = "apple",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly"
            )
        )
    ))]
    pub(crate) fn runtime(&self) -> crate::Runtime<M> {
        crate::Runtime {
            core: self.core.clone(),
        }
    }

    /// [`Registration::reports`] off the thread that drives the runtime, under the lock of the
    /// sources.
    ///
    /// Kept out of line: in line, its call would have every operation save registers on the way
    /// in, though one on the thread that drives the runtime never makes it.
    #[inline(never)]
    fn reports_under_lock(&self, interest: Interest) -> u32 {
        let sources = self.core.reactor.sources.lock();
        let Some(state) = sources.states.get(&self.key) else {
            unreachable!("a source stays in the map for as long as its registration lives");
        };

        state.reports.get(interest)
    }
}

impl<M> Drop for Registration<M>
where
    M: Mode,
{
    fn drop(&mut self) {
        // The entry goes with this registration, and with it the reactor's clone of the source.
        let (removed, deleted) = {
            let mut sources = self.core.reactor.sources.lock();
            let removed = sources.states.remove(&self.key);
            let deleted = match &removed {
                Some(state) => {
                    sources.descriptors.remove(&state.descriptor);
                    // A poller that copied its list as the wait under way started watches the
                    // descriptor until that wait returns, and the source is kept until then, so
                    // that the descriptor stays open for it.
                    if sources.waiting {
                        sources.retired.push(state.source.clone());
                    }
                    // While the entry still holds the source, so that the poller lets go of the
                    // descriptor before it can close, and under the lock, so that no change to
                    // what the source is watched for reaches the poller after this.
                    self.core
                        .remote
                        .poller
                        .delete(self.key, state.descriptor, state.armed)
                }
                None => Ok(()),
            };
            // The key of a source listed as stale stays in the list until the next look, which
            // only a wait that may sleep takes on an edge-triggered poller: on a runtime whose
            // waits never sleep, the keys of the sources let go of would pile up. Those are taken
            // out once they may outnumber the sources there are, which a source is listed for
            // once at most, so that each pass takes out at least as many as it keeps.
            if sources.stale.len() > 2 * sources.states.len() {
                let Sources { states, stale, .. } = &mut *sources;
                stale.retain(|key| states.contains_key(key));
            }

            (removed, deleted)
        };
        // Clear of the lock: the entry that came out of the map may hold a waker, and dropping a
        // waker can drop a task whose future holds a registration or a timer of this reactor,
        // each of which takes a lock as it goes. The source itself may be the last clone of
        // what was registered, whose destructor is somebody else's code.
        drop(removed);
        if let Err(e) = deleted {
            error!("The runtime failed to stop watching a source: {}", e);
        }
        // Clear of the lock as well: a wait that copied the source in is broken, so that the
        // source it keeps open goes, and a helper waiting with nothing else left to watch hears
        // that it has nothing left to wait for.
        self.core.remote.notify();
    }
}

/// A wait for a registered source to be ready for an [`Interest`], returned by
/// [`Registration::ready`].
///
/// It completes once the runtime finds the source ready, or once a wait of the runtime fails.
/// Dropping it before then gives up the wait. It fails on its first poll if the OS refuses to watch
/// the source for the interest.
#[must_use = "futures do nothing unless polled"]
pub struct Readiness<'a, M = Local>
where
    M: Mode,
{
    registration: &'a Registration<M>,
    interest: Interest,
    /// What the first poll takes in of the operations run on the source before it.
    before: Before,
    /// The id this wait is stored under in the reactor's map: taken on the first poll, and given
    /// up once a poll finds that the reactor took the wait out, which is what readiness does.
    id: Option<u64>,
}

impl<M> fmt::Debug for Readiness<'_, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readiness")
            .field("interest", &self.interest)
            .finish_non_exhaustive()
    }
}

impl<M> Future for Readiness<'_, M>
where
    M: Mode,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Cloned before the lock is taken: a waker's clone is somebody else's code, which is not
        // to run with a lock of this runtime's held.
        let waker = cx.waker().clone();
        let (done, told, unused) = {
            let mut sources = this.registration.core.reactor.sources.lock();
            let Sources {
                states,
                next_waiter,
                ..
            } = &mut *sources;
            let key = this.registration.key;
            let Some(state) = states.get_mut(&key) else {
                unreachable!("a source stays in the map for as long as its registration lives");
            };
            match this.id {
                None => {
                    let poller = &this.registration.core.remote.poller;
                    let watched = match this.before {
                        // An operation that found the source not ready has to run again where a
                        // wait reported the source while it ran.
                        Before::NotReady(since) => state
                            .found_not_ready(this.interest, since)
                            .then(|| state.arm(key, this.interest, poller)),
                        Before::Unknown => Some(state.arm_afresh(key, this.interest, poller)),
                    };
                    match watched {
                        Some(Ok(told)) => {
                            let id = *next_waiter;
                            *next_waiter += 1;
                            let waiting = &mut state.wakers.get_mut(this.interest).readiness;
                            waiting.insert(id, waker);
                            this.id = Some(id);

                            (None, told, None)
                        }
                        // The runtime cannot watch the source for this, which the caller hears of
                        // rather than waits for readiness that nothing would report.
                        Some(Err(e)) => (Some(Err(e)), false, Some(waker)),
                        None => (Some(Ok(())), false, Some(waker)),
                    }
                }
                Some(id) => match state.wakers.get_mut(this.interest).readiness.get_mut(&id) {
                    // Still there, so the reactor has not found the source ready since: this
                    // poll came from somewhere else, and the wait goes on with its waker.
                    Some(stored) => (None, false, Some(mem::replace(stored, waker))),
                    // Taken out by the reactor, along with the waker it then woke.
                    None => (Some(Ok(())), false, Some(waker)),
                },
            }
        };
        // Clear of the lock: dropping a waker can drop a task, and the future of that task may
        // hold a registration or a timer of this reactor, each of which takes a lock as it goes.
        drop(unused);
        if let Some(outcome) = done {
            this.id = None;

            return Poll::Ready(outcome);
        }
        // A poller that sees a change only in the wait after the one under way has that wait
        // broken here, so that the one after it watches this direction. A waiter already stored
        // is watched for already.
        if told && !Poller::LIVE {
            this.registration.core.remote.notify();
        }

        Poll::Pending
    }
}

impl<M> Drop for Readiness<'_, M>
where
    M: Mode,
{
    fn drop(&mut self) {
        // Nothing in the map for a wait nobody polled, nor for one the source was found ready for.
        let Some(id) = self.id else {
            return;
        };
        let removed = {
            let mut sources = self.registration.core.reactor.sources.lock();
            let Sources { states, stale, .. } = &mut *sources;
            let key = self.registration.key;
            let Some(state) = states.get_mut(&key) else {
                unreachable!("a source stays in the map for as long as its registration lives");
            };
            // Watched still for this waiter, who may have been the last in its direction: looked
            // at again before a later wait.
            state.mark_stale(key, stale);

            state.wakers.get_mut(self.interest).readiness.remove(&id)
        };
        // Clear of the lock: dropping a waker can drop a task, and the future of that task may
        // hold a registration or a timer of this reactor, each of which takes a lock as it goes.
        // No wake-up for the wait under way, which may still watch the source for this waiter:
        // it costs that wait no more than one early return, and the source is let go of when it
        // is next looked at, unless somebody else waits on it by then.
        drop(removed);
    }
}

/// What the first poll of a [`Readiness`] knows of the operations run on its source before it.
#[derive(Clone, Copy)]
enum Before {
    /// Nothing: its caller may have run operations on the source that the reactor did not hear
    /// of, as one of [`Registration::ready`]'s may (see [`SourceState::arm_afresh`]).
    Unknown,
    /// That an operation in its direction has just found the source not ready, which noted what
    /// [`Registration::reports`] returned before it ran (see [`Registration::ready_again`]).
    NotReady(Option<u32>),
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

    /// When this timer comes due, and nothing where the clock has no such moment.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Whether this timer has no deadline the clock can name, and so never comes due.
    pub(crate) fn never_fires(&self) -> bool {
        self.deadline.is_none()
    }

    /// Moves this timer to `deadline`, or makes it one that never comes due where there is none.
    ///
    /// The waker stored for the old deadline moves with it, so a task already waiting on this
    /// timer is woken at the new deadline without being polled again. A reset only ever moves an
    /// entry a poll put in the map, and never stores one of its own, which is why it never needs
    /// to ask for a thread to fire it: the poll that stored the entry did, and the entry has kept
    /// the reactor busy ever since.
    pub(crate) fn reset(&mut self, deadline: Option<Instant>) {
        let old = self.deadline.zip(self.id);
        self.deadline = deadline;
        // Nothing in the map for a timer nobody ever polled, nor for one that never came due: the
        // next poll stores the new deadline as a first poll does, notification and all.
        let Some((old_deadline, id)) = old else {
            return;
        };
        let old_key = (old_deadline, id);

        let (notify, dropped) = {
            let mut timers = self.core.reactor.timers.lock();
            // The earliest deadline before the reset, which is what the wait under way, if any,
            // is bounded by.
            let bound = timers.pending.keys().next().copied();
            match (timers.pending.remove(&old_key), deadline) {
                (Some(waker), Some(new)) => {
                    let new_key = (new, id);
                    // The id is this timer's own, so the key cannot be anybody else's.
                    let replaced = timers.pending.insert(new_key, waker);
                    // A deadline no earlier than the one the wait under way ends at gets no
                    // wake-up: that wait ends at the old deadline, finds nothing due and waits
                    // again, which costs less than a write on every reset a keep-alive makes.
                    let earlier = bound.is_none_or(|bound| new_key < bound);

                    (earlier, replaced)
                }
                // A timer that is never to come due leaves the map, and a thread whose wait was
                // bounded by nothing but its deadline can stop waiting at once.
                (Some(waker), None) => (bound == Some(old_key), Some(waker)),
                // The timer has fired, and the reactor took it out, or a failed wait cleared the
                // map: its task was woken, or is being, and its next poll stores the new deadline.
                (None, _) => (false, None),
            }
        };
        // Clear of the lock: dropping a waker can drop a task whose future holds a timer of this
        // reactor, whose own drop takes this very lock.
        drop(dropped);
        if notify {
            self.core.remote.notify();
        }
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
    /// The descriptor of each source in `states`, which no other source may lend as well.
    descriptors: HashSet<RawSource>,
    next_key: usize,
    /// The id the next [`Readiness`] to store itself takes.
    next_waiter: u64,
    /// The keys of the sources that may be watched in a direction nobody waits in any more, each
    /// once: those a wait found ready, and those a waiter gave up on, to be looked at before a
    /// later wait (see [`Reactor::wait`]).
    stale: Vec<usize>,
    /// Whether a wait is under way on a poller that copied its list as it started, which watches
    /// every descriptor it copied until it returns.
    waiting: bool,
    /// The sources whose registrations went while such a wait ran, kept so that their
    /// descriptors stay open until it returns.
    retired: Vec<M::SourcePtr>,
}

impl<M> Sources<M>
where
    M: Mode,
{
    /// Has `poller` stop watching each stale source in the directions nobody waits in any more.
    ///
    /// A source that fails to be told keeps being watched as it was, and is looked at again once a
    /// wait reports it; the first failure is what this returns, once every source has been looked
    /// at.
    fn reconcile(&mut self, poller: &Poller) -> io::Result<()> {
        let Self { states, stale, .. } = self;
        let mut result = Ok(());
        for key in stale.drain(..) {
            // A source let go of since has been let go of by the poller as well.
            let Some(state) = states.get_mut(&key) else {
                continue;
            };
            state.stale = false;
            let wanted = state.wakers.directions();
            if wanted == state.armed {
                continue;
            }
            match poller.modify(key, state.descriptor, state.armed, wanted) {
                Ok(()) => state.armed = wanted,
                Err(e) => {
                    if result.is_ok() {
                        result = Err(e);
                    }
                }
            }
        }

        result
    }
}

/// One watched source: the descriptor, who to wake for each direction of it, and what the poller
/// watches it for.
struct SourceState<M>
where
    M: Mode,
{
    source: M::SourcePtr,
    /// The descriptor the source lent when it was registered, which is what a wait watches.
    descriptor: RawSource,
    wakers: Wakers,
    /// The directions the poller watches the source in, as it was last told to, less any it
    /// reported it let go of by itself: every direction somebody waits in, and maybe more, until
    /// the source is next looked at.
    armed: Directions,
    /// How many times a wait has reported the source ready in each direction, on an
    /// edge-triggered poller ([`Poller::EDGE`]), which reports that no more unless there is news;
    /// none on any other poller.
    ///
    /// An operation off the thread that drives the runtime notes this for its direction before it
    /// runs ([`Registration::reports`]), and should it find the source not ready, runs again where
    /// the count has moved since: a wait reported the source while it ran, which no wait reports
    /// again. Each operation compares with what it noted itself, so a report that one of several
    /// operations takes in is not lost to the others.
    reports: Reports,
    /// [`SourceState::reports`] as of the latest operation in each direction that found the source
    /// not ready and had its waiter stored, or of the latest wait for readiness that had the
    /// poller look at the source afresh: where the two counts differ, a wait reported the source
    /// ready in the direction since.
    ///
    /// An operation of [`Registration::poll_io`] that has its waiter stored sets this, and so does
    /// the first poll of a [`Readiness`]: one after an operation of its own, as `poll_io` does,
    /// and any other after asking the poller to look at the source afresh where the counts differ,
    /// which reports it to the next wait where it is ready. An operation that runs again, as a
    /// wait reported the source while it ran, leaves this as it is: what it then finds says
    /// nothing of what the report is for, as it may take part of that only. A poller looks at a
    /// source as it is first told to watch it in a direction too, so a direction whose counts are
    /// the same is one the source is not ready in, or one a report is on its way for, and a waiter
    /// stored in it is woken in time without a system call. One whose counts differ is one the
    /// source may be ready in, or one an operation the reactor did not hear of found it not ready
    /// in since. They stay as they are where the direction is let go of, which loses nothing: the
    /// poller looks at the source again as the next waiter has it watch that direction.
    spent: Reports,
    /// Whether the source's key is in [`Sources::stale`].
    stale: bool,
}

impl<M> SourceState<M>
where
    M: Mode,
{
    /// Has `poller` watch this source, whose key is `key`, in `interest` as well, unless it does
    /// already: what a waiter in `interest` is stored after. Whether the poller had to be told.
    fn arm(&mut self, key: usize, interest: Interest, poller: &Poller) -> io::Result<bool> {
        let mut wanted = self.armed;
        match interest {
            Interest::Readable => wanted.readable = true,
            Interest::Writable => wanted.writable = true,
        }
        if wanted == self.armed {
            return Ok(false);
        }
        poller.modify(key, self.descriptor, self.armed, wanted)?;
        self.armed = wanted;

        Ok(true)
    }

    /// [`SourceState::arm`], for a wait for readiness whose caller may have run an operation the
    /// reactor did not hear of: a direction a wait found ready may be ready still, or may have been
    /// found not ready since. Ready, it is no news that an edge-triggered poller would report by
    /// itself, so the poller is asked to look at the source afresh, unless it was just told to
    /// watch the direction, which it looks at the source for.
    fn arm_afresh(&mut self, key: usize, interest: Interest, poller: &Poller) -> io::Result<bool> {
        let reports = self.reports.get(interest);
        let ready = Poller::EDGE && reports != self.spent.get(interest);
        let told = self.arm(key, interest, poller)?;
        if ready && !told {
            poller.recheck(key, self.descriptor, self.armed)?;
        }
        self.spent.set(interest, reports);

        Ok(told)
    }

    /// Takes in that an operation in `interest` found the source not ready, having noted `since`
    /// before it ran ([`Registration::reports`]): whether the operation's waiter is to wait for the
    /// next report, which it is not where a wait reported the source while the operation ran.
    ///
    /// That is so only on an edge-triggered poller ([`Poller::EDGE`]), which reports that no more
    /// unless there is news, and only off the thread that drives the runtime: a wait on that
    /// thread may report the source between the operation and this, and the operation has to run
    /// again, as the report it would wait for may never come. On the thread that drives the
    /// runtime, which notes nothing, no wait runs between the two, so whatever a wait found came
    /// before the operation, which found it spent.
    fn found_not_ready(&mut self, interest: Interest, since: Option<u32>) -> bool {
        if !Poller::EDGE {
            return true;
        }
        let reports = self.reports.get(interest);
        if since.is_some_and(|since| since != reports) {
            return false;
        }
        self.spent.set(interest, reports);

        true
    }

    /// Lists this source, whose key is `key`, in `stale`, unless it is there already.
    fn mark_stale(&mut self, key: usize, stale: &mut Vec<usize>) {
        if !self.stale {
            self.stale = true;
            stale.push(key);
        }
    }
}

/// A count of reports for each direction of a source (see [`SourceState::reports`]).
///
/// The counts wrap around, and are only ever compared for equality, so a count that comes back to
/// what an operation noted would have it miss news only after 2<sup>32</sup> reports in its
/// direction while it ran.
#[derive(Clone, Copy, Default)]
struct Reports {
    readable: u32,
    writable: u32,
}

impl Reports {
    /// Counts a report in each of `directions`.
    fn count(&mut self, directions: Directions) {
        self.readable = self.readable.wrapping_add(u32::from(directions.readable));
        self.writable = self.writable.wrapping_add(u32::from(directions.writable));
    }

    /// The count of the direction `interest` names.
    fn get(&self, interest: Interest) -> u32 {
        match interest {
            Interest::Readable => self.readable,
            Interest::Writable => self.writable,
        }
    }

    /// Sets the count of the direction `interest` names to `count`.
    fn set(&mut self, interest: Interest, count: u32) {
        match interest {
            Interest::Readable => self.readable = count,
            Interest::Writable => self.writable = count,
        }
    }
}

/// Who waits for each direction of one source.
#[derive(Default)]
struct Wakers {
    readable: Waiters,
    writable: Waiters,
}

impl Wakers {
    /// Who waits for the direction `interest` names.
    fn get_mut(&mut self, interest: Interest) -> &mut Waiters {
        match interest {
            Interest::Readable => &mut self.readable,
            Interest::Writable => &mut self.writable,
        }
    }

    /// The directions somebody waits in.
    fn directions(&self) -> Directions {
        Directions {
            readable: self.readable.any(),
            writable: self.writable.any(),
        }
    }

    /// Takes every waker of `directions` out, into `woken`.
    fn take_into(&mut self, directions: Directions, woken: &mut Vec<Waker>) {
        if directions.readable {
            self.readable.take_into(woken);
        }
        if directions.writable {
            self.writable.take_into(woken);
        }
    }
}

/// Who waits for one direction of one source.
#[derive(Default)]
struct Waiters {
    /// The operation [`Registration::poll_io`] keeps waiting, which the next one to wait in this
    /// direction takes the place of.
    operation: Option<Waker>,
    /// The waits for readiness alone, each under the id it took on its first poll: as many as
    /// there are [`Readiness`] futures waiting, each of which finds its own entry, or its
    /// absence, without a look at the others.
    readiness: HashMap<u64, Waker>,
}

impl Waiters {
    /// Whether anybody waits in this direction, which is what has the poller watch for it.
    fn any(&self) -> bool {
        self.operation.is_some() || !self.readiness.is_empty()
    }

    /// Takes every waker out, into `woken`: the direction is ready, or a failed wait has every
    /// waiter try again. A [`Readiness`] that finds itself gone on its next poll completes.
    fn take_into(&mut self, woken: &mut Vec<Waker>) {
        woken.extend(self.operation.take());
        woken.extend(self.readiness.drain().map(|(_, waker)| waker));
    }
}

/// The timers waiting for their deadline, keyed so that two of the same deadline stay apart.
#[derive(Default)]
struct Timers {
    pending: BTreeMap<(Instant, u64), Waker>,
    next_id: u64,
}

/// When a wait spins before it sleeps, and for how long: the limit the runtime was given, and a
/// history of its latest waits to say whether a spin is likely to pay.
///
/// A thread that has run out of work sleeps in the poller's wait, and the next piece of work has
/// to wake it. Where work comes in at short intervals, polling without sleeping for a moment
/// first finds it with no sleep and no wake-up to pay for on either side. Where it does not, that
/// moment is CPU time spent for nothing, and it would be spent before every sleep of a thread
/// that has gone idle, which a mostly idle runtime makes after nearly every piece of work. So a
/// wait spins only where what came before it says that a spin would have caught what ended the
/// wait.
///
/// The history keeps one bit for each of the latest eight waits that could sleep, the latest in
/// the lowest bit. A bit is set where its wait, the spin and the sleep after it together, lasted
/// no longer than the limit: where a spin of the limit would have caught what ended it, or did.
/// A wait spins if the limit is above zero and at least one bit is set. So:
///
/// * A runtime does not spin until it has seen a short wait, and a fresh one has not.
/// * A busy one spins before every wait.
/// * One that has gone idle stops after eight long waits, which bounds what a burst of work can
///   waste on spins to eight times the limit.
/// * A pattern of short and long waits in turn, as a stream of requests and replies makes, keeps it
///   spinning, as the short ones keep a bit set.
///
/// A wait that cannot sleep, because a task is ready or the future of `block_on` has been woken,
/// has no sleep to save by a spin and says nothing about how long work takes to come in, and so is
/// not counted. Neither is one that failed. A new limit starts the history over: its bits say how
/// the waits compared with the old one.
#[derive(Default)]
struct BusyPoll {
    /// The longest a wait spins, and zero where the runtime does not busy-poll.
    limit: Duration,
    /// One bit for each of the latest eight waits that could sleep: set for a wait that lasted no
    /// longer than `limit`. The latest is the lowest bit.
    history: u8,
}

impl BusyPoll {
    /// Whether the runtime busy-polls, and so times its waits.
    fn is_on(&self) -> bool {
        !self.limit.is_zero()
    }

    /// Spins for up to `limit` from now on, or, with a `limit` of zero, spins no more, and forgets
    /// the history.
    fn set_limit(&mut self, limit: Duration) {
        self.limit = limit;
        self.history = 0;
    }

    /// How long a wait that may last up to `timeout` spins before it sleeps: nothing where the
    /// runtime does not busy-poll or has seen no short wait lately, and otherwise the limit, cut
    /// to the timeout where the wait would be over by then anyway. A wait without a timeout,
    /// `None`, is not cut.
    fn spin_for(&self, timeout: Option<Duration>) -> Duration {
        if !self.is_on() || self.history == 0 {
            return Duration::ZERO;
        }

        timeout.map_or(self.limit, |timeout| self.limit.min(timeout))
    }

    /// Notes that a wait that could sleep lasted `waited`, from its start to its end.
    fn record(&mut self, waited: Duration) {
        if !self.is_on() {
            return;
        }
        self.history = (self.history << 1) | u8::from(waited <= self.limit);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        pin::pin,
        sync::{Mutex, atomic::AtomicUsize},
        task::Wake,
        thread,
    };

    #[cfg(unix)]
    use std::os::fd::OwnedFd;
    #[cfg(windows)]
    use std::os::windows::io::OwnedSocket;

    use ntest::timeout;
    use socket2::{SockRef, Socket};

    use super::*;
    use crate::{Shared, SharedRuntime, runtime::set_driving};

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

    /// A wait that comes between a `notify` raising the flag and its write to the channel finds
    /// the flag up and nothing to take out, and is no worse for it.
    ///
    /// The flag says a wake-up is on its way, not that it has arrived. A wait that took it for one
    /// to be read, and read the channel on its say-so, would find it empty and fail where it ought
    /// to have woken for nothing: the wait reads the channel only where the poll found it readable,
    /// and the read stops at the first error, an empty channel's included.
    #[test]
    #[timeout(15000)]
    fn a_wake_up_flagged_and_not_yet_written_fails_no_wait() {
        let runtime = runtime();
        let remote = &runtime.core.remote;
        remote.wake_pending.store(true, Ordering::Release);

        runtime.core.reactor.wait(Some(Duration::ZERO)).unwrap();

        assert!(!remote.wake_pending(), "the wait took the flag down");
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

    /// A deadline further off than some platforms' waits take a timeout for, about 24 days in
    /// milliseconds, bounds a wait to what every platform's wait takes.
    #[test]
    #[timeout(15000)]
    fn a_deadline_further_off_than_a_wait_takes_bounds_it_to_what_a_wait_takes() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (_counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(30 * 24 * 60 * 60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());

        assert_eq!(reactor.timeout(None), Some(poll::MAX_TIMEOUT));
        assert_eq!(
            reactor.timeout(Some(Duration::from_secs(1))),
            Some(Duration::from_secs(1))
        );
    }

    /// A wait bounded by a deadline further off than some platforms' waits take a timeout for
    /// runs, rather than failing.
    #[test]
    #[timeout(15000)]
    fn a_deadline_further_off_than_a_wait_takes_lets_the_wait_run() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(30 * 24 * 60 * 60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());

        // Bounded by the deadline alone, and ended at once by the wake-up the poll wrote.
        reactor.wait(None).unwrap();

        assert_eq!(counter.count(), 0);
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
        // Both made from the one deadline: `Runtime::sleep` reads the clock afresh for each
        // timer, and two deadlines nanoseconds apart would keep the pair apart on their own.
        let deadline = Instant::now() + Duration::from_millis(5);
        let mut one = pin!(sleep::<Shared>(&runtime.core, Some(deadline)));
        let mut other = pin!(sleep::<Shared>(&runtime.core, Some(deadline)));
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

    /// A reset moves a deadline the reactor holds, waker and all: the task waiting on the timer
    /// is woken at the new deadline without polling the timer again.
    #[test]
    #[timeout(15000)]
    fn a_reset_moves_the_deadline_and_keeps_the_waker() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
        let started = Instant::now();

        sleep.reset_after(Duration::from_millis(5));
        // A wait may end on the wake-up the poll or the reset wrote, find nothing due and wait
        // again: what ends the loop is the deadline the reset moved the waker to.
        while counter.count() == 0 {
            reactor.wait(None).unwrap();
        }

        assert!(started.elapsed() >= Duration::from_millis(5));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(counter.count(), 1);
        assert!(reactor.is_idle());
    }

    /// A reset to a deadline earlier than any the reactor holds breaks the wait under way, which
    /// was bounded by the old one.
    #[test]
    #[timeout(15000)]
    fn a_reset_to_an_earlier_deadline_breaks_the_wait() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
        // The wake-up that poll wrote, taken out of the channel so that the wait below can only
        // end on one written after it.
        reactor.wait(Some(Duration::ZERO)).unwrap();

        let waiting = {
            let runtime = runtime.clone();
            thread::spawn(move || {
                let reactor = &runtime.core.reactor;
                let started = Instant::now();
                reactor.wait(None).unwrap();

                started.elapsed()
            })
        };
        // Long enough for the thread above to reach its wait, bounded by nothing but the
        // deadline a minute ahead.
        thread::sleep(Duration::from_millis(50));

        sleep.reset_after(Duration::from_millis(5));

        assert!(waiting.join().unwrap() < Duration::from_secs(1));
        while counter.count() == 0 {
            reactor.wait(None).unwrap();
        }
        assert!(reactor.is_idle());
    }

    /// A reset to a later deadline writes no wake-up: the wait under way ends at the old
    /// deadline, finds nothing due and waits again, which costs less than a write on every reset
    /// a keep-alive makes.
    #[test]
    #[timeout(15000)]
    fn a_reset_to_a_later_deadline_writes_no_wake_up() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (_counter, waker) = counting_waker();
        let mut sleep = runtime.sleep(Duration::from_millis(500));
        let polled = Pin::new(&mut sleep).poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
        // The wake-up that poll wrote, taken out of the channel, and the flag down with it.
        reactor.wait(Some(Duration::ZERO)).unwrap();

        // From this thread, which drives nothing: the driving thread is spared every write
        // anyway, so it could not tell a reset that writes from one that does not.
        sleep.reset_after(Duration::from_secs(60));

        assert!(!runtime.core.remote.wake_pending());
        // The deadline did move, and it is the new one that the timer's drop takes out.
        let earliest = Lock::lock(&reactor.timers)
            .pending
            .keys()
            .next()
            .map(|(deadline, _)| *deadline);
        assert_eq!(earliest, sleep.deadline());
        drop(sleep);
        assert!(reactor.is_idle());
    }

    /// A reset of a timer nobody polled stores nothing and writes no wake-up: the reactor has no
    /// deadline of it to move, and the timer's first poll takes the new one.
    #[test]
    #[timeout(15000)]
    fn a_reset_of_an_unpolled_sleep_stores_nothing() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let mut sleep = runtime.sleep(Duration::from_secs(60));

        sleep.reset_after(Duration::from_secs(30));

        assert!(reactor.is_idle());
        assert!(!runtime.core.remote.wake_pending());
    }

    /// A reset to a deadline further ahead than the clock can name takes the timer out of the
    /// reactor and lets its waker go: nothing is ever going to fire it. It breaks the wait that the
    /// old deadline bounded, as dropping the timer does.
    #[test]
    #[timeout(15000)]
    fn a_reset_beyond_the_clock_drops_the_waker() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(60)));
        let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
        drop(waker);
        // The test's own handle on the counter, and the waker the reactor keeps.
        assert_eq!(Arc::strong_count(&counter), 2);
        // The wake-up that poll wrote, taken out of the channel, and the flag down with it.
        reactor.wait(Some(Duration::ZERO)).unwrap();

        sleep.reset_after(Duration::MAX);
        // The timer bounded the wait under way, so the reset breaks it.
        assert!(runtime.core.remote.wake_pending());

        assert!(sleep.deadline().is_none());
        assert!(reactor.is_idle());
        assert_eq!(Arc::strong_count(&counter), 1);
        assert_eq!(counter.count(), 0);
    }

    /// A timer that fired can be reset and fires again, at its new deadline.
    ///
    /// The reactor let the timer go when it fired it, so the reset has nothing to move and the
    /// poll after it stores the new deadline, as a first poll does.
    #[test]
    #[timeout(15000)]
    fn a_sleep_that_fired_fires_again_once_reset() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        let mut sleep = pin!(runtime.sleep(Duration::from_millis(5)));
        assert!(sleep.as_mut().poll(&mut cx).is_pending());
        while counter.count() == 0 {
            reactor.wait(None).unwrap();
        }
        assert!(sleep.as_mut().poll(&mut cx).is_ready());

        // Far enough ahead that the deadline is still to come at the poll just below.
        sleep.reset_after(Duration::from_millis(100));
        assert!(reactor.is_idle());
        assert!(sleep.as_mut().poll(&mut cx).is_pending());
        while counter.count() == 1 {
            reactor.wait(None).unwrap();
        }

        assert_eq!(counter.count(), 2);
        assert!(sleep.as_mut().poll(&mut cx).is_ready());
        assert!(reactor.is_idle());
    }

    /// A wait for readiness completes once the source is ready, and the reactor takes it out of
    /// its map as it finds that, so that the wait has nothing left to give up.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_completes_once_the_source_is_ready() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &waker).is_pending());
        assert!(waiters(&registration, Interest::Readable, Waiters::any));

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(counter.count(), 1);
        assert!(!waiters(&registration, Interest::Readable, Waiters::any));
        assert!(poll_with(&mut ready, &waker).is_ready());
    }

    /// A wait for readiness stays pending on a poll the source did not cause.
    ///
    /// A future that shares its task with others is polled whenever any of them wakes the task, a
    /// timer's wake included, and it must not take that for the source being ready. Neither the
    /// polls nor the waits that follow them, which find the source quiet, complete it or wake it.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_stays_pending_on_a_poll_the_source_did_not_cause() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);

        assert!(poll_with(&mut ready, &waker).is_pending());
        assert!(poll_with(&mut ready, &waker).is_pending());
        // The wake-up the first poll wrote, taken out of the channel.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        // Nothing was written to the peer, so this wait ends at its bound, or on a wake-up that
        // reached the channel late, and finds the source quiet either way.
        reactor.wait(Some(Duration::from_millis(50))).unwrap();

        assert_eq!(counter.count(), 0);
        assert!(poll_with(&mut ready, &waker).is_pending());
        assert!(waiters(&registration, Interest::Readable, Waiters::any));
    }

    /// Every wait for readiness in one direction is woken by that direction's readiness, each
    /// once, and each then completes.
    #[test]
    #[timeout(15000)]
    fn every_readiness_wait_in_one_direction_is_woken() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let wakers = [counting_waker(), counting_waker(), counting_waker()];
        let mut waits = [
            registration.ready(Interest::Readable),
            registration.ready(Interest::Readable),
            registration.ready(Interest::Readable),
        ];
        for (wait, (_, waker)) in waits.iter_mut().zip(&wakers) {
            assert!(poll_with(wait, waker).is_pending());
        }
        let stored = waiters(&registration, Interest::Readable, |waiters| {
            waiters.readiness.len()
        });
        assert_eq!(stored, 3);

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        for (wait, (counter, waker)) in waits.iter_mut().zip(&wakers) {
            assert_eq!(counter.count(), 1);
            assert!(poll_with(wait, waker).is_ready());
        }
    }

    /// A wait for readiness and an operation of `poll_io` waiting together are both woken by one
    /// readiness, and neither takes the other's place.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_and_an_operation_are_woken_together() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (operation, operation_waker) = counting_waker();
        let (readiness, readiness_waker) = counting_waker();
        let mut operation_cx = Context::from_waker(&operation_waker);
        let mut ready = registration.ready(Interest::Readable);
        assert!(read_one(&registration, &source, &mut operation_cx).is_pending());
        assert!(poll_with(&mut ready, &readiness_waker).is_pending());
        // The operation waiting again, as a poll the source did not cause has it do, takes the
        // place of its own waker and of no other.
        assert!(read_one(&registration, &source, &mut operation_cx).is_pending());
        let stored = waiters(&registration, Interest::Readable, |waiters| {
            (waiters.operation.is_some(), waiters.readiness.len())
        });
        assert_eq!(stored, (true, 1));

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(operation.count(), 1);
        assert_eq!(readiness.count(), 1);
        assert!(poll_with(&mut ready, &readiness_waker).is_ready());
        assert!(matches!(
            read_one(&registration, &source, &mut operation_cx),
            Poll::Ready(Ok(1))
        ));
    }

    /// A dropped wait for readiness leaves nothing behind: its entry is out of the map, so the
    /// source is no longer watched on its account, and the waker it stored is let go of.
    #[test]
    #[timeout(15000)]
    fn a_dropped_readiness_wait_leaves_nothing_behind() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &waker).is_pending());
        // The test's own handle on the counter, its waker, and the one the reactor keeps.
        assert_eq!(Arc::strong_count(&counter), 3);
        assert!(waiters(&registration, Interest::Readable, Waiters::any));

        drop(ready);

        assert!(!waiters(&registration, Interest::Readable, Waiters::any));
        assert_eq!(Arc::strong_count(&counter), 2);
        drop(waker);
        assert_eq!(Arc::strong_count(&counter), 1);
        // Nobody is left to wake for what the source now has to say.
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_millis(50))).unwrap();
        assert_eq!(counter.count(), 0);
    }

    /// Dropping one of two waits for readiness takes its own entry out and no other: the wait left
    /// is still woken, and the dropped one's waker is let go of.
    #[test]
    #[timeout(15000)]
    fn dropping_the_first_of_two_readiness_waits_leaves_the_second_waiting() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        let mut dropped = registration.ready(Interest::Readable);
        let mut kept = registration.ready(Interest::Readable);
        assert!(poll_with(&mut dropped, &first_waker).is_pending());
        assert!(poll_with(&mut kept, &second_waker).is_pending());

        drop(dropped);

        // The test's own handle on each counter and its waker, and for the second, the one the
        // reactor keeps.
        assert_eq!(Arc::strong_count(&first), 2);
        assert_eq!(Arc::strong_count(&second), 3);
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(first.count(), 0);
        assert_eq!(second.count(), 1);
        assert!(poll_with(&mut kept, &second_waker).is_ready());
    }

    /// A wait for readiness that was never polled stores nothing, and writes no wake-up, as there
    /// is no wait under way that it could be news to.
    #[test]
    #[timeout(15000)]
    fn an_unpolled_readiness_wait_stores_nothing() {
        let runtime = runtime();
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();

        let ready = registration.ready(Interest::Readable);
        assert!(!waiters(&registration, Interest::Readable, Waiters::any));
        drop(ready);

        assert!(!waiters(&registration, Interest::Readable, Waiters::any));
        assert!(!runtime.core.remote.wake_pending());
    }

    /// Only the poll that stores a wait for readiness writes a wake-up, and only where the
    /// platform's poller sees the change it makes in the wait after the one under way alone:
    /// that wait is broken, so that the next one watches the source. A later poll finds the
    /// wait stored, and so watched for already.
    #[test]
    #[timeout(15000)]
    fn only_the_first_poll_of_a_readiness_wait_writes_a_wake_up() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let remote = &runtime.core.remote;
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (_counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(!remote.wake_pending());

        assert!(poll_with(&mut ready, &waker).is_pending());
        assert_eq!(remote.wake_pending(), !Poller::LIVE);
        // The wake-up taken out of the channel, and the flag down with it.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert!(!remote.wake_pending());

        assert!(poll_with(&mut ready, &waker).is_pending());
        assert!(!remote.wake_pending());
    }

    /// A failed wait ends the waits for readiness: it wakes them with every other waiter, so that
    /// each tries its operation and sees the outcome for itself.
    #[test]
    #[timeout(15000)]
    fn a_failed_wait_ends_a_readiness_wait() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &waker).is_pending());

        reactor.wake_everything();

        assert_eq!(counter.count(), 1);
        assert!(!waiters(&registration, Interest::Readable, Waiters::any));
        assert!(poll_with(&mut ready, &waker).is_ready());
    }

    /// A wait for writability completes after one wait on a fresh connected pair, whose buffers
    /// have room.
    #[test]
    #[timeout(15000)]
    fn a_writable_readiness_wait_completes_on_a_fresh_pair() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (counter, waker) = counting_waker();
        let mut ready = registration.ready(Interest::Writable);
        assert!(poll_with(&mut ready, &waker).is_pending());

        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(counter.count(), 1);
        assert!(poll_with(&mut ready, &waker).is_ready());
    }

    /// A wait for readiness is woken by the direction it waits for and by no other.
    ///
    /// A fresh pair is writable and not readable, so the wait for writability is woken by the
    /// first wait and the one for readability only by the second, once a byte has been written,
    /// and neither is woken again by the readiness the other found.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_is_not_woken_by_the_other_direction() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (readable, readable_waker) = counting_waker();
        let (writable, writable_waker) = counting_waker();
        let mut reading = registration.ready(Interest::Readable);
        let mut writing = registration.ready(Interest::Writable);
        assert!(poll_with(&mut reading, &readable_waker).is_pending());
        assert!(poll_with(&mut writing, &writable_waker).is_pending());

        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(writable.count(), 1);
        assert_eq!(readable.count(), 0);
        assert!(poll_with(&mut writing, &writable_waker).is_ready());
        assert!(poll_with(&mut reading, &readable_waker).is_pending());

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(readable.count(), 1);
        assert_eq!(writable.count(), 1);
        assert!(poll_with(&mut reading, &readable_waker).is_ready());
    }

    /// A wait for readiness polled again before the readiness is woken through the waker of that
    /// poll, and the waker it replaced is let go of.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_is_woken_through_the_waker_of_its_last_poll() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &first_waker).is_pending());
        assert!(poll_with(&mut ready, &second_waker).is_pending());
        // The first poll's waker is gone from the reactor, and the second's is there.
        drop(first_waker);
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(Arc::strong_count(&second), 3);

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(first.count(), 0);
        assert_eq!(second.count(), 1);
        assert!(poll_with(&mut ready, &second_waker).is_ready());
    }

    /// A wait for readiness polled with another waker once the readiness has come completes: it
    /// was woken through the waker it stored, and the one it is polled with now is let go of.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_polled_with_another_waker_after_readiness_completes() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &first_waker).is_pending());

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(first.count(), 1);

        assert!(poll_with(&mut ready, &second_waker).is_ready());
        assert_eq!(second.count(), 0);
        // The test's own handle on the counter and its waker: the poll kept no clone of it.
        assert_eq!(Arc::strong_count(&second), 2);
    }

    /// A direction the poller watches for a waiter is watched still once a wait has woken that
    /// waiter, and let go of before the next wait that may sleep, where nobody waits in it again by
    /// then. A wait that cannot sleep leaves it to an edge-triggered poller, which does not report
    /// it again, while a level-triggered one lets go of it before every wait (see `held`).
    #[test]
    #[timeout(15000)]
    fn a_direction_nobody_waits_in_again_is_let_go_of_before_a_wait_that_may_sleep() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(armed(&registration), Directions::NONE);
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        assert_eq!(armed(&registration), READABLE);

        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(counter.count(), 1);
        assert_eq!(armed(&registration), READABLE);

        // As with a task queued, which may come back to the source: the byte is still there, and
        // costs the waits nothing all the same.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert_eq!(armed(&registration), held(READABLE));
        assert!(!reported(&registration));

        reactor.wait(Some(Duration::from_millis(1))).unwrap();
        assert_eq!(armed(&registration), Directions::NONE);
    }

    /// A source a wait found ready stays watched for as long as the task it woke is queued,
    /// through the waits that cannot sleep, none of which reports it again: let go of, it would be
    /// taken in again as soon as the task read it empty and waited on it again. A level-triggered
    /// poller lets go of it all the same, which costs it no system call (see `held`).
    #[test]
    #[timeout(15000)]
    fn a_source_stays_watched_while_the_task_it_woke_is_queued() {
        let runtime = runtime();
        let (source, mut peer) = pair();
        let registration = Arc::new(runtime.register(source.clone()).unwrap());
        let reader = {
            let registration = registration.clone();
            runtime.spawn("a reader that reads twice", async move {
                for _ in 0..2 {
                    std::future::poll_fn(|cx| read_one(&registration, &source, cx))
                        .await
                        .unwrap();
                }
            })
        };
        // The first poll finds nothing to read, and waits.
        round_of_one(&runtime, Duration::ZERO);
        assert_eq!(armed(&registration), READABLE);

        // Queued ahead of the reader when the wait wakes it, and never done, so that no wait
        // finds the queue empty.
        let _busy = runtime.spawn("a task always ready", always_ready());
        peer.write_all(&[7]).unwrap();
        round_of_one(&runtime, Duration::from_secs(1));
        assert_eq!(runtime.core.remote.ready_len(), 2);
        round_of_one(&runtime, Duration::ZERO);
        assert_eq!(armed(&registration), held(READABLE));
        assert!(!reported(&registration));

        // Polled, the task reads the byte and waits again, and the source stays watched.
        round_of_one(&runtime, Duration::ZERO);
        assert_eq!(armed(&registration), READABLE);
        drop(reader);
    }

    /// A source whose woken task, once polled, does not wait on it again is let go of before the
    /// next wait that may sleep, and not before one that cannot, other tasks queued or not, on an
    /// edge-triggered poller, which does not report it again in the meantime (see `held`).
    #[test]
    #[timeout(15000)]
    fn a_source_is_let_go_of_once_the_task_it_woke_was_polled_and_left_it() {
        let runtime = runtime();
        let (source, mut peer) = pair();
        let registration = Arc::new(runtime.register(source.clone()).unwrap());
        let reader = {
            let registration = registration.clone();
            runtime.spawn("a reader that reads once", async move {
                std::future::poll_fn(|cx| read_one(&registration, &source, cx))
                    .await
                    .unwrap();
            })
        };
        round_of_one(&runtime, Duration::ZERO);
        let busy = runtime.spawn("a task always ready", always_ready());
        peer.write_all(&[7]).unwrap();
        round_of_one(&runtime, Duration::from_secs(1));
        round_of_one(&runtime, Duration::ZERO);
        assert_eq!(armed(&registration), held(READABLE));

        round_of_one(&runtime, Duration::ZERO);
        assert!(reader.is_finished());
        assert_eq!(armed(&registration), held(READABLE));

        // With nothing left queued, the next wait may sleep.
        drop(busy);
        round_of_one(&runtime, Duration::from_millis(1));
        assert_eq!(armed(&registration), Directions::NONE);
    }

    /// A source that one wait finds ready in both directions, waking a waiter in each, is let go
    /// of in both once its tasks have been polled and have left it, before the next wait that may
    /// sleep.
    #[test]
    #[timeout(15000)]
    fn a_source_found_ready_both_ways_by_one_wait_is_let_go_of_once_its_tasks_ran() {
        let runtime = runtime();
        let (source, mut peer) = pair();
        let registration = Arc::new(runtime.register(source.clone()).unwrap());
        let reader = {
            let registration = registration.clone();
            runtime.spawn("a reader that reads once", async move {
                std::future::poll_fn(|cx| read_one(&registration, &source, cx))
                    .await
                    .unwrap();
            })
        };
        round_of_one(&runtime, Duration::ZERO);
        let writer = {
            let registration = registration.clone();
            runtime.spawn("a task that waits once to write", async move {
                registration.ready(Interest::Writable).await.unwrap();
            })
        };
        let busy = runtime.spawn("a task always ready", always_ready());
        peer.write_all(&[7]).unwrap();
        // The writer waits, and the wait after it wakes the reader and the writer both.
        round_of_one(&runtime, Duration::from_secs(1));
        assert_eq!(runtime.core.remote.ready_len(), 3);

        // The task always ready, the reader, which reads and is done, and the writer, which is
        // done too, in turn.
        for _ in 0..3 {
            round_of_one(&runtime, Duration::ZERO);
        }
        assert!(reader.is_finished());
        assert!(writer.is_finished());
        let both = Directions {
            readable: true,
            writable: true,
        };
        assert_eq!(armed(&registration), held(both));

        drop(busy);
        round_of_one(&runtime, Duration::from_millis(1));
        assert_eq!(armed(&registration), Directions::NONE);
    }

    /// A waiter that comes back to a direction before the next wait, as a task woken to read does
    /// once it has read all there was, keeps the direction watched, and the poller is told
    /// nothing.
    #[test]
    #[timeout(15000)]
    fn a_waiter_back_before_the_next_wait_keeps_its_direction_watched() {
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
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        reactor.wait(Some(Duration::from_millis(1))).unwrap();

        assert_eq!(armed(&registration), READABLE);
        assert_eq!(counter.count(), 1);
    }

    /// A wait for readiness given up leaves its direction watched until the next wait that may
    /// sleep, which lets it go where nobody else waits in it, and a wait that cannot sleep leaves
    /// it to an edge-triggered poller (see `held`).
    #[test]
    #[timeout(15000)]
    fn a_dropped_readiness_wait_lets_its_direction_go_before_a_wait_that_may_sleep() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, _peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (_counter, waker) = counting_waker();
        let mut first = registration.ready(Interest::Readable);
        let mut second = registration.ready(Interest::Readable);
        assert!(poll_with(&mut first, &waker).is_pending());
        assert!(poll_with(&mut second, &waker).is_pending());

        drop(first);
        reactor.wait(Some(Duration::from_millis(1))).unwrap();
        assert_eq!(armed(&registration), READABLE);

        drop(second);
        assert_eq!(armed(&registration), READABLE);
        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert_eq!(armed(&registration), held(READABLE));
        reactor.wait(Some(Duration::from_millis(1))).unwrap();
        assert_eq!(armed(&registration), Directions::NONE);
    }

    /// The keys of the sources let go of do not pile up in the list of those to look at on a
    /// runtime whose waits never sleep, which looks at none of them on an edge-triggered poller.
    #[test]
    #[timeout(15000)]
    fn the_listed_keys_of_sources_let_go_of_do_not_pile_up() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        // One source at a time, so that each drop leaves no source to keep a key listed for, and
        // the list is emptied every time.
        for _ in 0..64 {
            let (source, _peer) = pair();
            let registration = runtime.register(source).unwrap();
            let (_counter, waker) = counting_waker();
            let mut ready = registration.ready(Interest::Readable);
            assert!(poll_with(&mut ready, &waker).is_pending());
            // Listed as it is given up, and left listed by the wait, which cannot sleep, as with
            // a task always queued.
            drop(ready);
            reactor.wait(Some(Duration::ZERO)).unwrap();
            drop(registration);
        }

        assert!(Lock::lock(&reactor.sources).stale.is_empty());
    }

    /// An operation on the thread that drives the runtime that finds a source empty, which a wait
    /// found ready before it ran, has its waiter wait for the next report at once, without being
    /// run again: no wait can run on that thread between the operation and the waiter's storing,
    /// so the readiness that wait found was spent by then. The next byte wakes it.
    #[test]
    #[timeout(15000)]
    fn an_operation_on_the_driving_thread_waits_for_news_without_running_again() {
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

        let operations = {
            let _driving = driving(&runtime);
            assert!(matches!(
                read_one(&registration, &source, &mut cx),
                Poll::Ready(Ok(1))
            ));
            let mut operations = 0;
            let read = read_counting(&registration, &source, &mut cx, &mut operations);
            assert!(read.is_pending());

            operations
        };

        assert_eq!(operations, 1);
        peer.write_all(&[8]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(counter.count(), 2);
    }

    /// An operation off the thread that drives the runtime that finds a source empty, which a wait
    /// found ready before it ran, has its waiter wait for the next report without being run again,
    /// as on that thread: it noted the reports before it ran, and no wait reported the source
    /// while it did, so the readiness that wait found was spent by then. The next byte wakes it.
    #[test]
    #[timeout(15000)]
    fn an_operation_off_the_driving_thread_waits_for_news_after_the_reports_before_it() {
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

        let mut operations = 0;
        let read = read_counting(&registration, &source, &mut cx, &mut operations);

        assert!(read.is_pending());
        assert_eq!(operations, 1);
        peer.write_all(&[8]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(counter.count(), 2);
    }

    /// An operation off the thread that drives the runtime that finds a source empty while a wait
    /// on that thread finds it ready again, with nobody waiting on it, is not left to wait for a
    /// report that an edge-triggered poller would not make: it runs again, and reads the byte.
    #[test]
    #[timeout(15000)]
    fn an_operation_off_the_driving_thread_reads_what_a_wait_found_while_it_ran() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert!(matches!(
            read_one(&registration, &source, &mut cx),
            Poll::Ready(Ok(1))
        ));

        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        let mut news = false;
        let read = registration.poll_io(&mut cx, Interest::Readable, || {
            let read = SockRef::from(&source).recv(&mut byte);
            let empty = read
                .as_ref()
                .is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock);
            if empty && !news {
                news = true;
                // What the thread that drives the runtime may do while the operation runs.
                peer.write_all(&[8]).unwrap();
                reactor.wait(Some(Duration::ZERO)).unwrap();
            }

            read
        });

        if Poller::EDGE {
            assert!(matches!(read, Poll::Ready(Ok(1))));
        } else {
            // Reported by every wait while it is ready, the byte wakes the waiter.
            assert!(read.is_pending());
            reactor.wait(Some(Duration::from_secs(1))).unwrap();
            assert_eq!(counter.count(), 2);
        }
    }

    /// Two operations off the thread that drives the runtime that both find a source empty before
    /// a wait finds two bytes in it each run again, the second after the first has read one of
    /// them: neither waits for a report of what the other leaves, which an edge-triggered poller
    /// would not make, as each compares the reports with what it noted before it ran.
    #[test]
    #[timeout(15000)]
    fn two_operations_off_the_driving_thread_both_run_again_on_one_report() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (_counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // A waiter, so that the poller watches the source, as an earlier wait has it do.
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        let first = registration.reports(Interest::Readable);
        assert!(empty(SockRef::from(&source).recv(&mut byte)));
        let second = registration.reports(Interest::Readable);
        assert!(empty(SockRef::from(&source).recv(&mut byte)));
        peer.write_all(&[7, 8]).unwrap();
        reactor.wait(Some(Duration::ZERO)).unwrap();

        for since in [first, second] {
            let mut ready = registration.ready_again(Interest::Readable, since);
            completes(&mut ready, &waker, reactor);
            assert_eq!(SockRef::from(&source).recv(&mut byte).unwrap(), 1);
        }
    }

    /// A wait for readiness after an operation off the thread that drives the runtime ran again,
    /// as a wait reported the source while it ran, and read one of the two bytes it then found,
    /// completes after the next wait: an operation that runs again leaves the report unspent, so
    /// an edge-triggered poller, which would report the byte left no more, is asked to look at the
    /// source afresh.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_after_an_operation_that_ran_again_and_left_a_byte_completes() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (_counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());

        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        let mut news = false;
        let read = registration.poll_io(&mut cx, Interest::Readable, || {
            let read = SockRef::from(&source).recv(&mut byte);
            if !news
                && read
                    .as_ref()
                    .is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock)
            {
                news = true;
                // What the thread that drives the runtime may do while the operation runs.
                peer.write_all(&[7, 8]).unwrap();
                reactor.wait(Some(Duration::ZERO)).unwrap();
            }

            read
        });
        if Poller::EDGE {
            assert!(matches!(read, Poll::Ready(Ok(1))));
        } else {
            // Reported by every wait while it is ready, the bytes wake the waiter.
            assert!(read.is_pending());
            reactor.wait(Some(Duration::from_secs(1))).unwrap();
            assert!(matches!(
                read_one(&registration, &source, &mut cx),
                Poll::Ready(Ok(1))
            ));
        }

        let mut ready = registration.ready(Interest::Readable);
        assert!(poll_with(&mut ready, &waker).is_pending());
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert!(poll_with(&mut ready, &waker).is_ready());
    }

    /// A wait for readiness on a source a wait found ready already, whose byte nobody has read,
    /// completes after the next wait, as it would on a source that had just turned ready: an
    /// edge-triggered poller, which would report the source no more by itself, is asked to look at
    /// it afresh.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_on_a_source_found_ready_before_completes_after_the_next_wait() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source).unwrap();
        let (_first, first_waker) = counting_waker();
        let mut first = registration.ready(Interest::Readable);
        assert!(poll_with(&mut first, &first_waker).is_pending());
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert!(poll_with(&mut first, &first_waker).is_ready());

        let (second, second_waker) = counting_waker();
        let mut again = registration.ready(Interest::Readable);
        assert!(poll_with(&mut again, &second_waker).is_pending());
        reactor.wait(Some(Duration::from_secs(1))).unwrap();

        assert_eq!(second.count(), 1);
        assert!(poll_with(&mut again, &second_waker).is_ready());
    }

    /// A wait for readiness on a source a wait found ready, which an operation the reactor did not
    /// hear of has read empty since, waits for news of the source rather than completing again
    /// and again, and the next byte wakes it.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_after_an_operation_found_the_source_empty_waits_for_news() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (_first, first_waker) = counting_waker();
        let mut first = registration.ready(Interest::Readable);
        assert!(poll_with(&mut first, &first_waker).is_pending());
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert!(poll_with(&mut first, &first_waker).is_ready());
        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        assert_eq!(SockRef::from(&source).recv(&mut byte).unwrap(), 1);

        let (second, second_waker) = counting_waker();
        let mut again = registration.ready(Interest::Readable);
        assert!(poll_with(&mut again, &second_waker).is_pending());
        reactor.wait(Some(Duration::from_millis(50))).unwrap();
        assert_eq!(second.count(), 0);

        peer.write_all(&[8]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(second.count(), 1);
        assert!(poll_with(&mut again, &second_waker).is_ready());
    }

    /// A wait for readiness after an operation of its own found the source empty, which a wait
    /// found ready before the operation ran, waits for news, on the thread that drives the runtime
    /// and off it, but completes at once off it on an edge-triggered poller where a wait reported
    /// the source after the operation noted the reports, so that the operation runs again.
    #[test]
    #[timeout(15000)]
    fn a_readiness_wait_after_its_own_operation_waits_unless_a_wait_reported_the_source_since() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut first = registration.ready(Interest::Readable);
        assert!(poll_with(&mut first, &waker).is_pending());
        peer.write_all(&[7]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert!(poll_with(&mut first, &waker).is_ready());
        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        assert_eq!(SockRef::from(&source).recv(&mut byte).unwrap(), 1);

        // On the thread that drives the runtime, which notes nothing.
        let mut on = {
            let _driving = driving(&runtime);
            let since = registration.reports(Interest::Readable);
            assert!(empty(SockRef::from(&source).recv(&mut byte)));
            let mut on = registration.ready_again(Interest::Readable, since);
            assert!(poll_with(&mut on, &waker).is_pending());

            on
        };
        peer.write_all(&[8]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(counter.count(), 2);
        assert!(poll_with(&mut on, &waker).is_ready());
        assert_eq!(SockRef::from(&source).recv(&mut byte).unwrap(), 1);

        // Off it, having noted the reports after the latest of them.
        let since = registration.reports(Interest::Readable);
        assert!(empty(SockRef::from(&source).recv(&mut byte)));
        let mut late = registration.ready_again(Interest::Readable, since);
        assert!(poll_with(&mut late, &waker).is_pending());
        peer.write_all(&[9]).unwrap();
        reactor.wait(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(counter.count(), 3);
        assert!(poll_with(&mut late, &waker).is_ready());
        assert_eq!(SockRef::from(&source).recv(&mut byte).unwrap(), 1);

        // Off it, having noted the reports before a wait reported the source again.
        let since = registration.reports(Interest::Readable);
        assert!(empty(SockRef::from(&source).recv(&mut byte)));
        peer.write_all(&[10]).unwrap();
        reactor.wait(Some(Duration::ZERO)).unwrap();
        let mut early = registration.ready_again(Interest::Readable, since);
        assert_eq!(poll_with(&mut early, &waker).is_ready(), Poller::EDGE);
    }

    /// A source whose peer has hung up, and that nobody waits on, keeps no wait from blocking:
    /// the poller does not report it, or reports it once, and a wait after that sits out its
    /// timeout.
    #[test]
    #[timeout(15000)]
    fn a_hung_up_source_nobody_waits_on_lets_the_wait_block() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, peer) = pair();
        let _registration = runtime.register(source).unwrap();
        drop(peer);
        reactor.wait(Some(Duration::ZERO)).unwrap();
        reactor.wait(Some(Duration::ZERO)).unwrap();

        let started = Instant::now();
        reactor.wait(Some(Duration::from_millis(100))).unwrap();

        assert!(started.elapsed() >= Duration::from_millis(90));
    }

    /// The source of a registration dropped while a wait is under way is kept until that wait
    /// returns, which is what a wait on a poller that copied the source's descriptor in as it
    /// started has a registration's drop do; and the wait lets go of it as it returns.
    ///
    /// The wait under way is one this test marks as such itself, so that the drop path is tested
    /// on every poller and with no race against a wait on another thread.
    #[test]
    #[timeout(15000)]
    fn a_source_let_go_of_during_a_wait_is_kept_until_the_wait_returns() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        let (source, _peer) = pair();
        let dropped = Arc::new(Mutex::new(false));
        let tracked = Tracked {
            source,
            dropped: dropped.clone(),
        };
        let registration = runtime.register(tracked).unwrap();
        Lock::lock(&reactor.sources).waiting = true;

        drop(registration);
        assert!(!*dropped.lock().unwrap());
        assert_eq!(Lock::lock(&reactor.sources).retired.len(), 1);

        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert!(*dropped.lock().unwrap());
        assert!(Lock::lock(&reactor.sources).retired.is_empty());
    }

    /// A runtime does not busy-poll unless it is asked to: the limit is zero, so no wait is timed,
    /// spun for or kept in the history.
    #[test]
    #[timeout(15000)]
    fn busy_polling_is_off_by_default() {
        let mut busy_poll = BusyPoll::default();

        assert!(!busy_poll.is_on());
        busy_poll.record(SHORT);
        assert_eq!(busy_poll.history, 0);
        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);
        assert_eq!(busy_poll.spin_for(Some(LONG)), Duration::ZERO);
    }

    /// A limit spins for nothing until a wait has been short, and for the limit after one.
    #[test]
    #[timeout(15000)]
    fn a_limit_spins_only_once_a_wait_has_been_short() {
        let mut busy_poll = on();
        assert!(busy_poll.is_on());
        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);

        busy_poll.record(LONG);
        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);
        busy_poll.record(SHORT);
        assert_eq!(busy_poll.spin_for(None), LIMIT);
    }

    /// A wait as long as the limit is a short one, as a spin of the limit would have caught what
    /// ended it, and one a moment longer is not.
    #[test]
    #[timeout(15000)]
    fn a_wait_as_long_as_the_limit_is_a_short_one() {
        let mut as_long = on();
        as_long.record(LIMIT);
        let mut longer = on();
        longer.record(LIMIT + Duration::from_nanos(1));

        assert_eq!(as_long.spin_for(None), LIMIT);
        assert_eq!(longer.spin_for(None), Duration::ZERO);
    }

    /// A spin is cut to the timeout of its wait, which is over by then anyway; a wait with no
    /// timeout is not cut, and one that cannot sleep spins for nothing.
    #[test]
    #[timeout(15000)]
    fn a_spin_is_cut_to_the_timeout_of_its_wait() {
        let mut busy_poll = on();
        busy_poll.record(SHORT);

        assert_eq!(busy_poll.spin_for(Some(SHORT)), SHORT);
        assert_eq!(busy_poll.spin_for(Some(LIMIT)), LIMIT);
        assert_eq!(busy_poll.spin_for(Some(LONG)), LIMIT);
        assert_eq!(busy_poll.spin_for(None), LIMIT);
        assert_eq!(busy_poll.spin_for(Some(Duration::ZERO)), Duration::ZERO);
    }

    /// The history keeps eight waits: one that has gone idle spins before each of the eight long
    /// waits after its last short one, and not before the ninth.
    #[test]
    #[timeout(15000)]
    fn eight_long_waits_stop_the_spinning() {
        let mut busy_poll = on();
        busy_poll.record(SHORT);

        for _ in 0..u8::BITS {
            assert_eq!(busy_poll.spin_for(None), LIMIT);
            busy_poll.record(LONG);
        }

        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);
    }

    /// However many short waits come first, and however few, a burst of work wastes no more than
    /// eight spins of the limit on a runtime that goes idle after it.
    #[test]
    #[timeout(15000)]
    fn a_burst_wastes_no_more_than_eight_spins() {
        for burst in [1, 8, 100] {
            let mut busy_poll = on();
            for _ in 0..burst {
                busy_poll.record(SHORT);
            }

            let mut wasted = Duration::ZERO;
            for _ in 0..100 {
                wasted += busy_poll.spin_for(None);
                busy_poll.record(LONG);
            }

            assert_eq!(wasted, 8 * LIMIT, "{burst} short waits");
        }
    }

    /// Short waits and long ones in turn keep the spinning going, as a stream of requests and
    /// replies makes them, and so do seven long ones in a row between two short ones.
    #[test]
    #[timeout(15000)]
    fn short_and_long_waits_in_turn_keep_the_spinning_going() {
        let mut turns = on();
        turns.record(SHORT);
        let mut sevens = on();
        sevens.record(SHORT);

        for _ in 0..100 {
            assert_eq!(turns.spin_for(None), LIMIT);
            turns.record(LONG);
            assert_eq!(turns.spin_for(None), LIMIT);
            turns.record(SHORT);

            for _ in 0..7 {
                assert_eq!(sevens.spin_for(None), LIMIT);
                sevens.record(LONG);
            }
            assert_eq!(sevens.spin_for(None), LIMIT);
            sevens.record(SHORT);
        }
    }

    /// A short wait after the spinning stopped starts it again: the waits are timed whether or
    /// not they were spun for.
    #[test]
    #[timeout(15000)]
    fn a_short_wait_starts_the_spinning_again() {
        let mut busy_poll = on();
        busy_poll.record(SHORT);
        for _ in 0..u8::BITS {
            busy_poll.record(LONG);
        }
        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);

        busy_poll.record(SHORT);

        assert_eq!(busy_poll.spin_for(None), LIMIT);
    }

    /// A new limit starts the history over: the bits say how the waits compared with the old one.
    #[test]
    #[timeout(15000)]
    fn a_new_limit_forgets_the_history() {
        let mut busy_poll = on();
        busy_poll.record(SHORT);
        let longer = LIMIT * 2;

        busy_poll.set_limit(longer);

        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);
        busy_poll.record(SHORT);
        assert_eq!(busy_poll.spin_for(None), longer);
    }

    /// A limit of zero turns the polling off, and then keeps nothing, not even a wait that took no
    /// time at all.
    #[test]
    #[timeout(15000)]
    fn a_limit_of_zero_turns_the_polling_off() {
        let mut busy_poll = on();
        busy_poll.record(SHORT);
        assert_eq!(busy_poll.spin_for(None), LIMIT);

        busy_poll.set_limit(Duration::ZERO);
        busy_poll.record(Duration::ZERO);

        assert!(!busy_poll.is_on());
        assert_eq!(busy_poll.history, 0);
        assert_eq!(busy_poll.spin_for(None), Duration::ZERO);
    }

    /// No wait is timed unless busy polling is on.
    #[test]
    #[timeout(15000)]
    fn a_wait_is_not_counted_unless_busy_polling_is_on() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;

        reactor.wait(Some(Duration::from_millis(1))).unwrap();

        assert_eq!(history(reactor), 0);
    }

    /// A wait that could sleep is counted, and one that cannot is not: a task ready or a future
    /// woken, which is what a timeout of zero comes of, says nothing of how long work takes.
    #[test]
    #[timeout(15000)]
    fn only_a_wait_that_could_sleep_is_counted() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        reactor.set_busy_poll(Duration::from_secs(10));

        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert_eq!(history(reactor), 0);
        assert!(!reactor.would_spin());

        reactor.wait(Some(Duration::from_millis(1))).unwrap();
        assert_eq!(history(reactor), 0b1);
        assert!(reactor.would_spin());

        reactor.wait(Some(Duration::ZERO)).unwrap();
        assert_eq!(history(reactor), 0b1);
    }

    /// A wait that lasts longer than the limit is counted as a long one, behind the short one that
    /// had it spin.
    #[test]
    #[timeout(15000)]
    fn a_wait_longer_than_the_limit_is_counted_as_a_long_one() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_millis(20));
        let short = history(reactor);

        // Spins for the limit, then sleeps for the rest of its timeout.
        reactor.wait(Some(Duration::from_millis(60))).unwrap();

        assert_eq!(history(reactor), short << 1);
    }

    /// A spin ends at a wake-up that is on its way, whether or not it has reached the channel.
    ///
    /// The flag is up and nothing has been written, as it is between a `notify` raising the flag
    /// and writing. A sleep looks at the channel alone and would wait for good, so that the wait
    /// ends at all is what says the spin looks at the flag.
    #[test]
    #[timeout(15000)]
    fn a_spin_ends_at_a_wake_up_that_is_on_its_way() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_secs(10));
        runtime
            .core
            .remote
            .wake_pending
            .store(true, Ordering::Release);

        let started = Instant::now();
        reactor.wait(None).unwrap();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            !runtime.core.remote.wake_pending(),
            "the wait took the flag down"
        );
    }

    /// A spin ends at a wake-up from another thread, whose write a pass of the spin takes out of
    /// the channel.
    #[test]
    #[timeout(15000)]
    fn a_spin_ends_at_a_wake_up_from_another_thread() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_secs(10));
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
        assert!(
            !runtime.core.remote.wake_pending(),
            "the wait took the flag down"
        );
        breaker.join().unwrap();
    }

    /// A spin ends at a source that is ready, and the waiter of it is woken as a sleep would
    /// have it.
    #[test]
    #[timeout(15000)]
    fn a_spin_ends_at_a_source_that_is_ready() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_secs(10));
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        // The wake-up that arming the source wrote, taken out of the channel so that the wait
        // below can only end on the source.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            peer.write_all(&[7]).unwrap();
        });

        let started = Instant::now();
        reactor.wait(None).unwrap();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(counter.count(), 1);
        writer.join().unwrap();
    }

    /// A spin is bounded by the nearest deadline, and the timer comes due when it ends, though
    /// the limit is far longer.
    #[test]
    #[timeout(15000)]
    fn a_spin_ends_at_the_nearest_deadline() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_secs(10));
        let (counter, waker) = counting_waker();
        let mut sleep = pin!(runtime.sleep(Duration::from_millis(20)));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let started = Instant::now();
        // The first wait may end on the wake-up that storing the deadline wrote, and find
        // nothing due. The one after it is bounded by the deadline alone.
        while counter.count() == 0 {
            reactor.wait(None).unwrap();
        }

        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(counter.count(), 1);
        assert!(reactor.is_idle());
    }

    /// A wait goes on once its spin has run out, and ends at what the sleep after it finds, far
    /// later than the spin did.
    #[test]
    #[timeout(15000)]
    fn a_wait_goes_on_once_the_spin_has_run_out() {
        let runtime = runtime();
        let reactor = &runtime.core.reactor;
        busy_polling(reactor, Duration::from_millis(10));
        let (source, mut peer) = pair();
        let registration = runtime.register(source.clone()).unwrap();
        let (counter, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read_one(&registration, &source, &mut cx).is_pending());
        // The wake-up that arming the source wrote, taken out of the channel so that the wait
        // below can only end on the source.
        reactor.wait(Some(Duration::ZERO)).unwrap();
        let started = Instant::now();
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            peer.write_all(&[7]).unwrap();
        });

        reactor.wait(None).unwrap();

        assert!(started.elapsed() >= Duration::from_millis(200));
        assert_eq!(counter.count(), 1);
        // A long wait, behind the short ones that had it spin.
        assert_eq!(history(reactor) & 1, 0);
        assert_ne!(history(reactor), 0);
        writer.join().unwrap();
    }

    /// A runtime of the test's own, whose reactor the test waits on as the thread driving it
    /// would. Shared, so that a test can hand it to a thread of its own.
    fn runtime() -> SharedRuntime {
        SharedRuntime::new().unwrap()
    }

    /// One round of the thread driving `runtime`, as `Core::round` makes one, but with a batch of
    /// one task, polled on a thread marked as the one that drives the runtime, so that the test
    /// says which task is polled when, and with a wait bounded by `at_most` rather than by whether
    /// a task is queued: one of zero cannot sleep, as with a task queued, and any other may.
    fn round_of_one(runtime: &SharedRuntime, at_most: Duration) {
        let core = &runtime.core;
        {
            let _driving = driving(runtime);
            core.scheduler.run_batch(&mut [0; 1]);
        }

        core.reactor.wait(Some(at_most)).unwrap();
    }

    /// Marks the calling thread as the one that drives `runtime`, as `block_on` does, until what
    /// this returns is dropped.
    fn driving(runtime: &SharedRuntime) -> impl Drop {
        struct Unmark;

        impl Drop for Unmark {
            fn drop(&mut self) {
                set_driving::<Shared>(None);
            }
        }

        set_driving::<Shared>(Some(&runtime.core));

        Unmark
    }

    /// A future that is never done and wakes its task at every poll: the future of a task that is
    /// ready at every turn, so that the queue is never empty.
    fn always_ready() -> impl Future<Output = ()> + Send {
        std::future::poll_fn(|cx| {
            cx.waker().wake_by_ref();

            Poll::Pending
        })
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
        let mut operations = 0;

        read_counting(registration, source, cx, &mut operations)
    }

    /// [`read_one`], which adds to `operations` each time it runs the read.
    fn read_counting(
        registration: &Registration<Shared>,
        source: &TestSource,
        cx: &mut Context<'_>,
        operations: &mut usize,
    ) -> Poll<io::Result<usize>> {
        let mut byte = [std::mem::MaybeUninit::<u8>::uninit(); 1];

        registration.poll_io(cx, Interest::Readable, || {
            *operations += 1;

            SockRef::from(source).recv(&mut byte)
        })
    }

    /// Polls `ready` once with `waker`, as a task whose waker that is would.
    ///
    /// The tests' sources are ones every poller watches, so the wait never fails.
    fn poll_with(ready: &mut Readiness<'_, Shared>, waker: &Waker) -> Poll<()> {
        Pin::new(ready)
            .poll(&mut Context::from_waker(waker))
            .map(|outcome| outcome.expect("the runtime watches the source"))
    }

    /// Polls `ready`, a wait after an operation that found its source not ready before a wait of
    /// `reactor` reported the source, until it completes: at once on an edge-triggered poller, so
    /// that the operation runs again, and after the next wait on a level-triggered one, which
    /// reports the source at every wait while it is ready.
    fn completes(ready: &mut Readiness<'_, Shared>, waker: &Waker, reactor: &Reactor<Shared>) {
        if !Poller::EDGE {
            assert!(poll_with(ready, waker).is_pending());
            reactor.wait(Some(Duration::from_secs(1))).unwrap();
        }

        assert!(poll_with(ready, waker).is_ready());
    }

    /// Whether `read` found its source empty.
    fn empty(read: io::Result<usize>) -> bool {
        read.is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock)
    }

    /// What `read` makes of who waits for `interest` of `registration`'s source.
    ///
    /// The lock of the sources is let go of as this returns, before the test polls or drops
    /// anything that takes it.
    fn waiters<T>(
        registration: &Registration<Shared>,
        interest: Interest,
        read: impl FnOnce(&Waiters) -> T,
    ) -> T {
        let mut sources = Lock::lock(&registration.core.reactor.sources);
        let Some(state) = sources.states.get_mut(&registration.key) else {
            unreachable!("a source stays in the map for as long as its registration lives");
        };

        read(state.wakers.get_mut(interest))
    }

    /// What the poller watches `registration`'s source for.
    fn armed(registration: &Registration<Shared>) -> Directions {
        let sources = Lock::lock(&registration.core.reactor.sources);
        let Some(state) = sources.states.get(&registration.key) else {
            unreachable!("a source stays in the map for as long as its registration lives");
        };

        state.armed
    }

    /// What a source that a wait found ready, and that nobody waits on any more, is watched for
    /// across a wait that cannot sleep: still `directions` on an edge-triggered poller, which
    /// reports the source no more unless there is news, and nothing on a level-triggered one,
    /// which has every listed source looked at before every wait (see `Reactor::wait`).
    fn held(directions: Directions) -> Directions {
        if Poller::EDGE {
            directions
        } else {
            Directions::NONE
        }
    }

    /// Whether a wait reports `registration`'s source, asked of the poller itself, with no look at
    /// the listed sources before it. What the wait finds, a wake-up included, is taken, and the
    /// reactor never hears of it.
    fn reported(registration: &Registration<Shared>) -> bool {
        let poller = &registration.core.remote.poller;
        let ready = poller.wait(Some(Duration::ZERO)).unwrap();

        ready.iter().any(|ready| ready.key == registration.key)
    }

    /// The history of `reactor`'s waits, a bit for each of the latest eight that could sleep.
    fn history(reactor: &Reactor<Shared>) -> u8 {
        Lock::lock(&reactor.busy_poll).history
    }

    /// Has `reactor` busy-poll for up to `limit`, and waits on it until the next wait would spin,
    /// which takes one that could sleep and lasted no longer than `limit`.
    fn busy_polling(reactor: &Reactor<Shared>, limit: Duration) {
        reactor.set_busy_poll(limit);
        while !reactor.would_spin() {
            reactor.wait(Some(Duration::from_micros(100))).unwrap();
        }
    }

    /// The rule with busy polling on for [`LIMIT`] and no history.
    fn on() -> BusyPoll {
        let mut busy_poll = BusyPoll::default();
        busy_poll.set_limit(LIMIT);

        busy_poll
    }

    /// The readable direction alone.
    const READABLE: Directions = Directions {
        readable: true,
        writable: false,
    };

    /// The limit of the rule's tests.
    const LIMIT: Duration = Duration::from_micros(20);

    /// A wait that is over before [`LIMIT`] is.
    const SHORT: Duration = Duration::from_micros(5);

    /// A wait that outlasts [`LIMIT`].
    const LONG: Duration = Duration::from_millis(5);

    /// A source that records that it has been dropped.
    struct Tracked {
        source: TestSource,
        dropped: Arc<Mutex<bool>>,
    }

    #[cfg(unix)]
    impl std::os::fd::AsFd for Tracked {
        fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
            self.source.as_fd()
        }
    }

    #[cfg(windows)]
    impl std::os::windows::io::AsSocket for Tracked {
        fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
            self.source.as_socket()
        }
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            *self.dropped.lock().unwrap() = true;
        }
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
