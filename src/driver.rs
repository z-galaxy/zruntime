//! Who runs a runtime: the thread inside a [`block_on`](crate::block_on), or a helper thread
//! where no such thread is there to do it.
//!
//! The scheduler and the reactor are run by one thread at a time, the one in the driver's seat.
//! A thread that enters `block_on` takes the seat if it is free and keeps it until its future is
//! done: between two polls of that future it runs a batch of ready tasks and then waits on the
//! reactor, so a program that drives its work through `block_on` runs it on its own thread and
//! starts none. A thread that finds the seat taken parks instead, to be polled again when its
//! future is woken or the seat is freed.
//!
//! Work that outlives every `block_on` — a task, a registered source or a timer polled from some
//! other executor, or a task left running once `block_on` has returned — is run by a helper
//! thread, started where that work is found with nobody in the seat, and gone once nothing is
//! left to run, watch or time. A `block_on` that arrives while the helper is in the seat is given
//! it, the helper parking until that call leaves, so that a program calling `block_on` once per
//! operation runs each of them on its own thread rather than behind a thread of the runtime's
//! own.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::Future,
    pin::pin,
    ptr::NonNull,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread, ThreadId},
    time::Duration,
};

use super::{Inner, lock, reactor::Reactor};
use crate::log::error;

/// Whether the calling thread is in the seat of the runtime `reactor` belongs to.
///
/// The reactor is named by its address, which stands for it alone while a thread is in its
/// seat: that thread holds the runtime, so the reactor cannot be dropped and its place taken by
/// another until the thread has left.
pub(super) fn on_driver_thread(reactor: &Reactor) -> bool {
    DRIVER_REACTOR.with(Cell::get) == Some(NonNull::from(reactor))
}

/// The runtime the calling thread is in the seat of, if it is in one.
///
/// Nothing, too, once this thread's `DRIVING` local is gone, even where the thread still holds the
/// seat: a destructor can reach [`Driving::enter`] after `DRIVING` has been torn down, and takes
/// the seat with nowhere left here to record it.
pub(super) fn driven() -> Option<Arc<Inner>> {
    DRIVING
        .try_with(|driving| driving.borrow().upgrade())
        .ok()
        .flatten()
}

/// Whether the calling thread is inside a `block_on`, and so has a thread of its own for what it
/// builds and for the work it hands over.
///
/// `false` on a thread whose locals are being destroyed, which is a thread outside `block_on`
/// unless a `block_on` is what that destruction is running, and the count says so where it is.
pub(super) fn in_block_on() -> bool {
    IN_BLOCK_ON
        .try_with(Cell::get)
        .is_ok_and(|inside| inside > 0)
}

/// The runtime a `block_on` is to drive, looked up afresh at each turn of its loop, because the
/// future it polls may be the very thing that brings the runtime into being. Asked on the
/// calling thread alone, so a lookup in that thread's own registry is what it is.
pub(super) type Resolve<'a> = &'a dyn Fn() -> Option<Arc<Inner>>;

/// Runs `future` to completion on the calling thread, running the runtime `resolve` names
/// alongside it whenever the seat is free.
///
/// Panics when called from a thread that is in the seat already, which is a call from inside a
/// task the runtime is running: such a call could only wait for the thread it is on.
pub(super) fn block_on<F>(resolve: Resolve<'_>, future: F) -> F::Output
where
    F: Future,
{
    let _inside = InBlockOn::enter();
    // The seat outlives the future, which [`drive`] drops as it returns: whatever that future
    // held — a task, a registration, a timer — is gone before the seat is given up, so only
    // work that outlives this call is handed on to a helper.
    let mut leaving = Leaving {
        resolve,
        driving: None,
    };

    drive(resolve, &mut leaving.driving, future)
}

/// What a `block_on` leaves as it goes: the seat it took, where it took one, and the work it was
/// running with nobody to run it.
///
/// A guard rather than a step after the poll loop, so that a panic out of the future is seen to
/// exactly as a return is. A future that panicked may have spawned tasks, registered sources or
/// armed timers that outlive it, and what drives those from here on is decided here.
struct Leaving<'a> {
    resolve: Resolve<'a>,
    /// The seat this call took, where it took one, given up as this is dropped.
    driving: Option<Driving>,
}

impl Drop for Leaving<'_> {
    /// Gives the seat up and hands on what the call leaves behind.
    ///
    /// A thread inside `block_on` asks for no helper for its own runtime, on the promise that it
    /// takes the seat on the next turn of its loop and runs the work itself. A thread that leaves
    /// without ever having taken the seat has no next turn to keep that promise on, so what its
    /// last poll handed over is handed on here; one that had the seat did as much where it gave
    /// it up.
    ///
    /// The thread is taken out of the list of those waiting for the seat either way: where it
    /// had the seat, freeing it empties that list; where it had not, it is taken out here.
    fn drop(&mut self) {
        if self.driving.take().is_some() {
            return;
        }
        let Some(inner) = (self.resolve)() else {
            return;
        };
        let mut seat = lock(&inner.seat);
        seat.stop_waiting();
        // The helper gives the seat up to a call that asks for it and parks; a call that asks
        // and then finds its future done without ever taking the seat up leaves nobody in it,
        // and the helper is the one to rouse.
        if matches!(seat.holder, Holder::Nobody { .. }) {
            seat.rouse_helper();
        }
        hand_over(&inner, &mut seat);
    }
}

/// Starts the helper unless a thread is in the seat or about to take it. Called after the work
/// it is to see is in place (a task queued, a source registered, a deadline stored), never
/// before.
///
/// That order is what makes the hand-off safe either way round: a helper that starts here finds
/// the work, and a thread in the seat either sees it in the round it is in or finds it where it
/// decides whether to leave, which it does under this very lock. A thread inside `block_on`
/// takes the seat on the next turn of its loop and finds the work then.
pub(super) fn ensure_helper(inner: &Arc<Inner>) {
    // A thread inside `block_on` on its own runtime asks for no helper: it takes the seat on the
    // next turn of its loop and runs the work itself. Work handed to another thread's runtime
    // gets a helper as from any thread outside `block_on`, because this thread will never sit in
    // that seat.
    if in_block_on() && Inner::is_own(inner) {
        return;
    }
    let mut seat = lock(&inner.seat);
    // Whoever is in the seat finds the work, and so does a helper parked with the seat left to
    // nobody, which was roused as the seat was left.
    if !matches!(
        seat.holder,
        Holder::Nobody {
            parked_helper: None
        }
    ) {
        return;
    }
    spawn_helper(inner, &mut seat);
}

/// Who is running a runtime, and who is waiting to.
pub(super) struct Seat {
    holder: Holder,
    /// The threads parked in `block_on` for want of the seat, unparked whenever it is freed.
    ///
    /// Keyed by thread, so that a thread which enters `block_on` again and again while another
    /// keeps the seat has one place here rather than one per call. An entry goes where its
    /// thread takes the seat or leaves `block_on`, so that what is held here is the threads
    /// there may be something to unpark, and no more.
    waiting: HashMap<ThreadId, Thread>,
}

impl Seat {
    /// A seat nobody is in.
    pub(super) fn new() -> Self {
        Self {
            holder: Holder::Nobody {
                parked_helper: None,
            },
            waiting: HashMap::new(),
        }
    }

    /// Whether the helper thread is up, in the seat or parked.
    pub(super) fn helper_running(&self) -> bool {
        matches!(self.holder, Holder::Helper) || self.parked_helper().is_some()
    }

    /// Whether the helper thread is up and parked, having left the seat to a `block_on`.
    #[cfg(test)]
    pub(super) fn helper_parked(&self) -> bool {
        self.parked_helper().is_some()
    }

    /// How many threads are down as waiting for the seat.
    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.waiting.len()
    }

    /// Takes the calling thread out of the list of those waiting for the seat.
    fn stop_waiting(&mut self) {
        self.waiting.remove(&thread::current().id());
    }

    /// Frees the seat, and unparks every thread waiting for it: to take it, or to find its
    /// future done. A helper that parked for want of it is roused too, a freed seat being the
    /// very thing it parked for.
    fn free(&mut self) {
        let parked_helper = match &mut self.holder {
            Holder::Nobody { parked_helper } | Holder::BlockOn { parked_helper } => {
                parked_helper.take()
            }
            Holder::Helper => None,
        };
        self.holder = Holder::Nobody { parked_helper };
        DRIVER_REACTOR.with(|reactor| reactor.set(None));
        // Gone already on a thread whose locals are being destroyed, which [`Driving::take`]
        // leaves nothing in for that very reason.
        let _ = DRIVING.try_with(|driving| *driving.borrow_mut() = Weak::new());
        for (_, thread) in self.waiting.drain() {
            thread.unpark();
        }
        self.rouse_helper();
    }

    /// Rouses the helper where it is parked, so that it looks at the seat again: to take it
    /// back where work is left, and to leave where none is.
    ///
    /// The helper parks for the sake of a `block_on` that wants the seat, so whoever takes the
    /// last such call away — by freeing the seat, or by leaving the list of those waiting for
    /// it empty with nobody in it — owes the helper this.
    fn rouse_helper(&self) {
        if let Some(thread) = self.parked_helper() {
            thread.unpark();
        }
    }

    /// The helper thread, where it is parked with the seat left to a `block_on`.
    fn parked_helper(&self) -> Option<&Thread> {
        match &self.holder {
            Holder::Nobody { parked_helper } | Holder::BlockOn { parked_helper } => {
                parked_helper.as_ref()
            }
            Holder::Helper => None,
        }
    }
}

/// Who is in the seat, and the helper thread where it is parked outside it.
enum Holder {
    /// Nobody. The parked helper, if there is one, gave the seat up to a `block_on` that has
    /// not taken it yet, or that took it and already left again; it is roused to look at the
    /// seat once more.
    Nobody { parked_helper: Option<Thread> },
    /// A thread inside `block_on`, with the helper parked behind it if the helper is up.
    BlockOn { parked_helper: Option<Thread> },
    /// The helper thread.
    Helper,
}

/// The kind of thread a [`Driving`] belongs to. Unlike [`Holder`], it cannot be nobody: a guard
/// exists only while its thread is in the seat.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Driver {
    /// A thread inside `block_on`.
    BlockOn,
    /// The helper thread.
    Helper,
}

/// Polls `future` to completion, running the runtime `resolve` names alongside it whenever the
/// seat is free, and leaving in `driving` the seat it took, where it took one.
///
/// The future is this call's own, and is dropped where it returns, before the caller gives the
/// seat up.
fn drive<F>(resolve: Resolve<'_>, driving: &mut Option<Driving>, future: F) -> F::Output
where
    F: Future,
{
    let mut future = pin!(future);
    let signal = Arc::new(Signal {
        thread: thread::current(),
        woken: AtomicBool::new(false),
        runtime: Mutex::new(Weak::new()),
    });
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let take_seat = |driving: &mut Option<Driving>| {
        *driving = resolve().and_then(|inner| Driving::take(inner, Driver::BlockOn));
        // Set before the round that follows, so that a wake arriving during its wait finds the
        // runtime whose wait to break.
        if let Some(driving) = driving {
            *lock(&signal.runtime) = Arc::downgrade(&driving.inner);
        }
    };
    let mut failed_waits = 0u32;
    loop {
        if driving.is_none() {
            take_seat(driving);
        }
        // Lowered before the poll, so that a wake during it is seen by the round or the park
        // that follows, and with a swap, so that the poll sees what the wakes before it were
        // for.
        signal.woken.swap(false, Ordering::AcqRel);
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        // Looked for again, because the poll may have been what brought the runtime into being,
        // and a thread inside `block_on` asks for no helper: where nobody else is in the seat,
        // the work that poll left behind is this thread's to run.
        if driving.is_none() {
            take_seat(driving);
        }
        match &*driving {
            Some(driving) => driving.round(&signal.woken, &mut failed_waits),
            None => {
                if !signal.woken.load(Ordering::Acquire) {
                    thread::park();
                }
            }
        }
    }
}

/// Starts a helper for what is left to run, watch or time on `inner`, unless a thread is in the
/// seat or a helper is up already. Under the seat lock, which `seat` is the guard of.
///
/// Unlike [`ensure_helper`], this pays no heed to the caller being inside `block_on`: it is for
/// the two places where a thread that was running the work stops being able to.
fn hand_over(inner: &Arc<Inner>, seat: &mut Seat) {
    if !matches!(
        seat.holder,
        Holder::Nobody {
            parked_helper: None
        }
    ) || !inner.is_busy()
    {
        return;
    }
    spawn_helper(inner, seat);
}

/// Starts the helper thread. Under the seat lock, which `seat` is the guard of.
fn spawn_helper(inner: &Arc<Inner>, seat: &mut Seat) {
    let inner = inner.clone();
    thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || helper(inner))
        .expect("the thread a runtime's helper runs on");
    // The seat is the helper's from here, under the lock this is called with, so there is no
    // window in which the thread is on its way and the seat looks free to a second spawn or to a
    // `block_on`. Set once the thread is there: a spawn that fails panics with the seat free, so
    // that the next call tries again rather than wait on a thread that was never started.
    seat.holder = Holder::Helper;
}

/// The name the helper thread carries: eight bytes, which fits the fifteen Linux keeps for one.
pub(super) const THREAD_NAME: &str = "zruntime";

/// What the helper thread does: the seat, and rounds until nothing is left.
///
/// Each round polls what is ready, a batch at a time, and then asks whether anything is left to
/// run, watch or time. Where nothing is, the thread leaves there and then, rather than sit in a
/// wait until something comes along to tell it what it could have worked out for itself. Where
/// something is, the round ends in one wait on the reactor — unless a `block_on` is waiting for
/// the seat, in which case the seat is that call's and this thread parks until it is free again.
fn helper(inner: Arc<Inner>) {
    let mut failed_waits = 0u32;
    // The seat was made this thread's by whoever started it.
    let mut driving = Some(Driving::enter(inner.clone(), Driver::Helper));
    loop {
        if let Some(driving) = driving {
            loop {
                driving.run_batch();
                if driving.leave_if_idle() {
                    return;
                }
                if driving.yield_if_wanted() {
                    break;
                }
                driving.wait(false, &mut failed_waits);
            }
        }
        // Both ways of getting here put this thread down as parked, with the seat a
        // `block_on`'s to take or to hand back, and that call is what ends the park. A park
        // that ends of its own accord costs no more than another look at the seat.
        thread::park();
        driving = Driving::take(inner.clone(), Driver::Helper);
    }
}

/// A thread's time in the seat: taken here, given up when this is dropped.
struct Driving {
    inner: Arc<Inner>,
    who: Driver,
    /// Whether the seat has been given up already, which [`Driving::leave_if_idle`] and
    /// [`Driving::yield_if_wanted`] are the two things that do before the drop.
    left: Cell<bool>,
}

impl Driving {
    /// Takes the seat as `who` if it is free.
    ///
    /// A `block_on` that finds it taken is put down as waiting, to be unparked when the seat is
    /// freed, and tells the helper where the helper is the one in it: the helper looks for
    /// waiters between a batch and a wait, so the notification is what brings its wait to an
    /// end. A `block_on` in the seat frees it when its future is done and needs no telling.
    ///
    /// A helper that finds the seat taken parks rather than leaves, and puts itself down as
    /// parked under the very lock the seat's holder frees the seat under: whoever is in the
    /// seat rouses it as it goes.
    fn take(inner: Arc<Inner>, who: Driver) -> Option<Self> {
        let mut seat = lock(&inner.seat);
        match (&mut seat.holder, who) {
            (Holder::Nobody { parked_helper }, Driver::BlockOn) => {
                let parked_helper = parked_helper.take();
                seat.holder = Holder::BlockOn { parked_helper };
                // A turn of this call's loop before this one may have found the seat taken and
                // put the thread down as waiting for it, which a thread in the seat is not.
                seat.stop_waiting();
            }
            // The parked thread, if any, was this very helper.
            (Holder::Nobody { .. }, Driver::Helper) => seat.holder = Holder::Helper,
            (Holder::BlockOn { parked_helper }, Driver::Helper) => {
                *parked_helper = Some(thread::current());

                return None;
            }
            // One helper runs per runtime, so a helper cannot find another already there.
            (Holder::Helper, Driver::Helper) => {
                unreachable!("a second helper for the same runtime")
            }
            (holder, Driver::BlockOn) => {
                let helper_in_seat = matches!(holder, Holder::Helper);
                let thread = thread::current();
                seat.waiting.insert(thread.id(), thread);
                if helper_in_seat {
                    drop(seat);
                    inner.reactor.notify();
                }

                return None;
            }
        }
        drop(seat);

        Some(Self::enter(inner, who))
    }

    /// Marks the calling thread as the one in the seat of `inner`, which the caller has made it:
    /// [`Driving::take`] under the seat lock, or the spawn of the helper thread.
    fn enter(inner: Arc<Inner>, who: Driver) -> Self {
        DRIVER_REACTOR.with(|reactor| reactor.set(Some(NonNull::from(&inner.reactor))));
        // Written down where there is still somewhere to write it: a thread whose locals are
        // being destroyed reaches here from the destructor of one of them, and the order those
        // run in is not this runtime's to choose. Nothing built on a thread on its way out looks
        // this up anyway — [`driven`] comes back empty there, and the lookup falls through to
        // the registry, which names this very runtime — so the seat is held all the same.
        let _ = DRIVING.try_with(|driving| *driving.borrow_mut() = Arc::downgrade(&inner));

        Self {
            inner,
            who,
            left: Cell::new(false),
        }
    }

    /// Polls up to `BATCH` ready tasks.
    fn run_batch(&self) {
        for _ in 0..BATCH {
            if !self.inner.scheduler.run_one() {
                break;
            }
        }
    }

    /// One round for `block_on`: a batch, then one wait on the reactor, which waits for nothing
    /// where the future this thread is polling has been woken in the meantime, so that the poll
    /// of it comes next and what a source has for the tasks is reported all the same.
    fn round(&self, woken: &AtomicBool, failed_waits: &mut u32) {
        self.run_batch();
        // Read before the wait, so a wake that already landed is passed on as `at_once`.
        let woken = woken.load(Ordering::Acquire);

        self.wait(woken, failed_waits);
    }

    /// One wait on the reactor: bounded by no time at all where a task is ready or `at_once`
    /// says the caller has something of its own to get back to, so that the round after it polls
    /// what is ready, and by nothing where neither holds, so that the thread sleeps until a
    /// source, a deadline or a notification has something for it.
    ///
    /// `failed_waits` counts the failures in a row, for the pause that keeps a wait which fails
    /// every time from becoming a spin; every waiter retries its own operation and sees its own
    /// error.
    fn wait(&self, at_once: bool, failed_waits: &mut u32) {
        let at_most = (at_once || self.inner.scheduler.has_ready()).then_some(Duration::ZERO);
        match self.inner.reactor.wait(at_most) {
            Ok(()) => *failed_waits = 0,
            Err(e) => {
                *failed_waits += 1;
                if *failed_waits == 1 {
                    error!("The runtime's wait failed: {}", e);
                }
                self.inner.reactor.wake_everything();
                thread::sleep(Duration::from_millis(1 << (*failed_waits).min(10)));
            }
        }
    }

    /// Gives the seat up if nothing is left to run, watch or time; what the helper does before
    /// each wait. Under the seat lock, so that a spawn or a registration racing with it either
    /// is seen here or starts a helper itself once the lock is released.
    fn leave_if_idle(&self) -> bool {
        let mut seat = lock(&self.inner.seat);
        if self.inner.is_busy() {
            return false;
        }
        seat.free();
        self.left.set(true);

        true
    }

    /// Gives the seat up to the `block_on` calls waiting for it and puts this thread down as
    /// parked; what the helper asks before each wait. `true` where it did, and the caller parks.
    ///
    /// Freed first, with the holder still `Helper` and so no parked helper for [`Seat::free`] to
    /// rouse, and marked parked after: a rouse of this very thread would only cut short the park
    /// it is about to make.
    fn yield_if_wanted(&self) -> bool {
        let mut seat = lock(&self.inner.seat);
        if seat.waiting.is_empty() {
            return false;
        }
        seat.free();
        seat.holder = Holder::Nobody {
            parked_helper: Some(thread::current()),
        };
        self.left.set(true);

        true
    }
}

/// How many ready tasks run between two looks at the reactor, so a busy task queue cannot
/// starve a socket or a timer.
const BATCH: usize = 32;

impl Drop for Driving {
    /// Gives the seat up, unless [`Driving::leave_if_idle`] or [`Driving::yield_if_wanted`] did
    /// already, which is what the flag this reads says: the seat may be somebody else's by now,
    /// and what is asked here is whether it is still this thread's to give up, not who is in it.
    ///
    /// A `block_on` that leaves work behind starts a helper for it, because whatever is left
    /// may be awaited by a thread that is not going to drive; a helper that unwinds out of the
    /// loop puts itself down as gone, so that the next spawn, registration or timer poll starts
    /// another.
    fn drop(&mut self) {
        if self.left.get() {
            return;
        }
        let mut seat = lock(&self.inner.seat);
        seat.free();
        if self.who == Driver::BlockOn {
            hand_over(&self.inner, &mut seat);
        }
    }
}

/// The waker of the future a `block_on` polls.
struct Signal {
    thread: Thread,
    /// Whether the future has been woken since it was last polled.
    ///
    /// Raised by each wake and lowered before each poll, both with `AcqRel` swaps rather than
    /// stores. The lowering acquires every wake since the one before it, so that the poll sees
    /// what they were for: a store reads nothing, and a store that raised the flag would cut the
    /// wakes before it off from the lowering. A wake that comes after a lowering acquires it in
    /// turn, and with it the end of the reactor wait before it, so that the `notify` the wake
    /// makes cannot take the wake-up that wait took out for one still to come.
    woken: AtomicBool,
    /// The runtime the thread is in the seat of, set by that thread as it takes the seat: a wake
    /// may come from any thread, and only the driving thread knows which runtime's wait it is in.
    runtime: Mutex<Weak<Inner>>,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// Marks the future woken and rouses its thread, wherever that thread is: parked, or in the
    /// seat inside the reactor's wait, which is what the notification ends. A wake from the
    /// thread itself writes no notification, because that thread looks at the flag before its
    /// next wait; a wake of a thread in no seat needs none, because that thread is parked.
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.swap(true, Ordering::AcqRel);
        self.thread.unpark();
        if let Some(inner) = lock(&self.runtime).upgrade() {
            inner.reactor.notify();
        }
    }
}

/// What says a thread is inside `block_on`, for as long as it is.
struct InBlockOn;

impl InBlockOn {
    /// Marks the calling thread as inside `block_on`.
    ///
    /// Panics where the thread is in a seat already: the call comes from inside a task the
    /// runtime is running on this thread, and could only ever wait for itself.
    fn enter() -> Self {
        assert!(
            DRIVER_REACTOR.with(Cell::get).is_none(),
            "block_on called from a task this runtime is running: the call would wait for the \
             very thread it is on"
        );
        IN_BLOCK_ON.with(|inside| inside.set(inside.get() + 1));

        Self
    }
}

impl Drop for InBlockOn {
    fn drop(&mut self) {
        IN_BLOCK_ON.with(|inside| inside.set(inside.get() - 1));
    }
}

thread_local! {
    /// The reactor of the runtime this thread is in the seat of, and nothing on a thread that
    /// is in no seat at all.
    static DRIVER_REACTOR: Cell<Option<NonNull<Reactor>>> = const { Cell::new(None) };

    /// The runtime this thread is in the seat of, for whatever is built on this thread while it
    /// is: a task run by a helper thread builds its own tasks, timers and registrations on the
    /// runtime the helper runs.
    static DRIVING: RefCell<Weak<Inner>> = const { RefCell::new(Weak::new()) };

    /// How many `block_on` calls this thread is inside of, in the seat or waiting for it. A
    /// count rather than a flag, so that one call returning does not unmark the call it was
    /// made from.
    static IN_BLOCK_ON: Cell<u32> = const { Cell::new(0) };
}
