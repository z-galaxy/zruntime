#![cfg_attr(feature = "runtime", doc = include_str!("../README.md"))]
// The README's examples need the runtime, so a build without it gets an overview of its own.
#![cfg_attr(
    all(feature = "event", not(feature = "runtime")),
    doc = include_str!("event-only.md")
)]
#![deny(rust_2018_idioms)]
#![doc(test(attr(
    warn(unused),
    deny(warnings),
    allow(dead_code),
    // W/o this, we seem to get some bogus warning about `extern crate zbus`.
    allow(unused_extern_crates),
)))]

#[cfg(feature = "runtime")]
mod async_io;
#[cfg(feature = "broadcast")]
pub mod broadcast;
#[cfg(feature = "helper")]
mod driver;
#[cfg(feature = "event")]
mod event;
#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "lock")]
pub mod lock;
#[cfg(feature = "runtime")]
mod log;
#[cfg(feature = "runtime")]
mod mode;
#[cfg(feature = "mpmc")]
pub mod mpmc;
#[cfg(any(feature = "tcp", feature = "udp", all(feature = "unix", unix)))]
pub mod net;
#[cfg(feature = "runtime")]
mod poll;
#[cfg(feature = "process")]
pub mod process;
#[cfg(feature = "runtime")]
mod reactor;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod scheduler;
#[cfg(feature = "runtime")]
mod time;
#[cfg(feature = "unblock")]
mod unblock;

#[cfg(all(feature = "runtime", unix))]
use std::os::fd::AsFd as AsSource;
#[cfg(all(feature = "runtime", windows))]
use std::os::windows::io::AsSocket as AsSource;
#[cfg(feature = "runtime")]
use std::{
    borrow::Cow,
    fmt,
    future::{Future, IntoFuture},
    io,
    panic::Location,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

#[cfg(feature = "runtime")]
pub use async_io::AsyncIo;
#[cfg(feature = "event")]
pub use event::{Event, EventListener};
#[cfg(feature = "runtime")]
use mode::sealed::Sealed as _;
#[cfg(feature = "runtime")]
pub use mode::{Local, Mode, Shared, Source};
#[cfg(feature = "runtime")]
pub use reactor::{Readiness, Registration};
#[cfg(feature = "runtime")]
use runtime::Core;
#[cfg(feature = "runtime")]
use scheduler::{JoinHandle, Name};
#[cfg(feature = "runtime")]
pub use time::{Interval, MissedTickBehavior, Sleep, TimedOut, Timeout};
#[cfg(feature = "unblock")]
pub use unblock::{BlockingWork, Unblock, unblock};

/// Runs `future` to completion on the calling thread, running that thread's runtime alongside it.
///
/// This is for a program that has no async runtime of its own. Put the async code in one call to
/// this function; the call blocks the thread until the future completes. In between polls of the
/// future, the calling thread also runs the scheduler and the reactor of everything built on the
/// runtime this call drives — every task spawned, source registered or timer armed from inside
/// it on [`SharedRuntime::current`] — and two threads that each call this drive their own
/// runtime, in parallel. If the call returns while some of that work is still alive, a helper
/// thread takes it over until the next call, or until the work is gone.
///
/// The scheduler and the reactor of a runtime are run by one thread at a time, the one in the
/// runtime's seat. A call takes the seat if it is free and keeps it until its future is done, so
/// a program that drives its work through this function runs it on its own thread and starts no
/// helper. A call that finds the seat taken parks instead, to be polled again when its future is
/// woken or the seat is freed. A call that arrives while the helper is in the seat is given it,
/// the helper parking until that call leaves, so that a program calling this once per operation
/// runs each of them on its own thread rather than behind a thread of the runtime's own.
///
/// Because that work runs on the calling thread in between polls, the future must not block that
/// thread waiting for it. A synchronous wait for a task's result, or a busy loop until a signal
/// arrives, never finishes.
///
/// Do not call this from inside a task this runtime is running. It panics there, because it
/// would be waiting for the very thread it is on. Do not call it from another runtime's task
/// either: it blocks that task's thread until the future completes, which deadlocks the program
/// if the future needs that thread to make progress.
///
/// This is only available when the `helper` feature is enabled.
#[cfg(feature = "helper")]
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    driver::block_on(driver::Target::Own, &driver::own, future)
}

/// Queues `future` on the shared runtime the calling code is running on, and hands back the task
/// that joins or cancels it.
///
/// This is `SharedRuntime::spawn` for code that has no runtime to name: a function that is
/// called from a task, say, and was handed no clone of the runtime running it, nor a name to give
/// the task. The task is named by where it was spawned instead, for the message logged if it
/// panics and for its `Debug`. Dropping the task cancels it, and [`Task::detach`] lets it run on,
/// as for a task spawned on a runtime by name.
///
/// The task goes on the first of these that exists:
///
/// 1. The shared runtime the calling thread drives. That is the one it is inside
///    [`Runtime::block_on`] on, whether it was made by [`Runtime::new`] or handed out by
///    `SharedRuntime::current`; the one whose seat it is in, where the `helper` feature gives it
///    one; and the one whose task it is running, on a thread inside `block_on` as on the helper
///    thread.
/// 2. With the `helper` feature, the runtime `SharedRuntime::current` hands out: the thread's own
///    inside the free `block_on`, and the one the whole process shares everywhere else, which a
///    helper thread runs.
///
/// The first differs on purpose from `SharedRuntime::current`, which does not count a `block_on`
/// on a runtime made by [`Runtime::new`], and hands out the one the process shares there. A task
/// spawned here goes on the runtime that runs the code spawning it, so that the thread inside
/// `block_on` runs it, as it runs every other task of its runtime, rather than a helper thread
/// that is running some other runtime.
///
/// A future that need not be `Send` is spawned with [`spawn_local`], on a local runtime.
///
/// # Panics
///
/// Panics where the calling thread drives no shared runtime and the `helper` feature, which
/// gives it a runtime to spawn on in that case, is not enabled. With the feature, it panics only
/// where `SharedRuntime::current` would fail: where the reactor of the runtime cannot be made.
///
/// # Example
///
/// ```
/// use zruntime::{Shared, SharedRuntime, Task};
///
/// // A function that is given no runtime to spawn on, and spawns on whichever one is running it.
/// fn double(number: u32) -> Task<u32, Shared> {
///     zruntime::spawn(async move { number * 2 })
/// }
///
/// let runtime = SharedRuntime::new().unwrap();
/// let doubled = runtime.block_on(async { double(21).await.unwrap() });
///
/// assert_eq!(doubled, 42);
/// ```
#[cfg(feature = "runtime")]
#[track_caller]
pub fn spawn<F>(future: F) -> Task<F::Output, Shared>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Taken here rather than in a helper, which `track_caller` would have to be on all the way
    // down for the location to be the caller's.
    let name = Name::SpawnedAt(Location::caller());

    spawn_on_shared(&shared_to_spawn_on(), name, future)
}

/// Queues `future` on the local runtime the calling code is running on, and hands back the task
/// that joins or cancels it.
///
/// This is `LocalRuntime::spawn` for code that has no runtime to name, as [`spawn`] is for a
/// shared one: the task is named by where it was spawned, and goes on the local runtime the
/// calling thread drives. That is the one it is inside [`Runtime::block_on`] on, or the one whose
/// task it is running. A local runtime stays on the thread that made it, so the thread running the
/// spawning code is the only one that can be driving it, and there is no other runtime to look
/// for in its place.
///
/// # Panics
///
/// Panics where the calling thread drives no local runtime: outside any `block_on`, and inside
/// the `block_on` of a shared runtime, whose tasks may run on any thread and have no local runtime
/// to go on.
///
/// # Example
///
/// ```
/// use std::{cell::Cell, rc::Rc};
///
/// use zruntime::LocalRuntime;
///
/// let runtime = LocalRuntime::new().unwrap();
/// let total = Rc::new(Cell::new(0));
///
/// runtime.block_on(async {
///     // A future that holds an `Rc` is no problem here, and nothing names the runtime.
///     let adding = zruntime::spawn_local({
///         let total = total.clone();
///         async move { total.set(total.get() + 42) }
///     });
///
///     adding.await.unwrap();
/// });
///
/// assert_eq!(total.get(), 42);
/// ```
#[cfg(feature = "runtime")]
#[track_caller]
pub fn spawn_local<F>(future: F) -> Task<F::Output, Local>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let name = Name::SpawnedAt(Location::caller());
    let Some(core) = Local::driven() else {
        panic!(
            "spawn_local called where no local runtime is running: call it from inside \
             `LocalRuntime::block_on`, or from a task of that runtime"
        );
    };

    Task(scheduler::spawn_local(&core, name, future))
}

/// A runtime that stays on the thread it was made on, and runs futures that need not be `Send`.
#[cfg(feature = "runtime")]
pub type LocalRuntime = Runtime<Local>;

/// A runtime that may be reached from, and driven on, any thread, and runs `Send` futures.
#[cfg(feature = "runtime")]
pub type SharedRuntime = Runtime<Shared>;

/// A handle to a runtime: a scheduler and a reactor, driven by whichever thread is inside
/// [`Runtime::block_on`] on it.
///
/// Cloning it is cheap, and every clone reaches the same scheduler and reactor. The runtime goes
/// once the last clone, and the last timer and registration built on it, are gone; a task left
/// unfinished then is dropped, and its handle fails. A task handle does not keep its runtime
/// alive, but a task whose future holds a clone, a timer or a registration does, for as long as
/// the task lives.
///
/// `M` is the runtime's flavour: [`Local`], the default, for a runtime that stays on its thread,
/// or [`Shared`] for one that may be reached from any thread.
///
/// Reach for [`Local`] unless a handle, a task or a future built on this runtime must cross
/// threads or be polled by another executor: it is the cheaper of the two, with nothing behind
/// atomics or locks beyond what a [`Waker`](std::task::Waker) forces. [`Shared`] costs an `Arc`
/// and a `Mutex` where `Local` costs an `Rc` and a `RefCell`, and only runs `Send` futures, in
/// return for being usable from, and drivable on, any thread.
///
/// Either way, a runtime is driven by one thread at a time, so all of its tasks share one core.
/// Work that needs more runs on several runtimes, one per thread, as
/// [Running on several threads](crate#running-on-several-threads) describes.
#[cfg(feature = "runtime")]
pub struct Runtime<M = Local>
where
    M: Mode,
{
    core: M::Ptr<Core<M>>,
}

#[cfg(feature = "runtime")]
impl<M> Runtime<M>
where
    M: Mode,
{
    /// A fresh runtime, with nothing to do.
    ///
    /// The runtime has no thread of its own: what is spawned, registered or timed on it runs
    /// only while some thread is inside [`Runtime::block_on`] on it, and waits for the next such
    /// call in between.
    ///
    /// A task whose future holds a timer, a registration or a clone of the runtime keeps the
    /// runtime, and the descriptors its reactor holds, alive until the task ends. No helper thread
    /// ever runs a runtime made here, so a detached task that never ends is never let go of: drive
    /// such a task to completion, or keep its [`Task`] and cancel it, by dropping that or through
    /// [`Task::cancel`].
    ///
    /// What can fail is the reactor: it opens the channel a wait is broken through.
    ///
    /// Called as `Runtime::new()`, this leaves `M` for the compiler to guess at, which it cannot
    /// do from an empty argument list: name [`LocalRuntime::new`] or [`SharedRuntime::new`]
    /// instead, or give `new` a turbofish.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            core: Core::<M>::new()?,
        })
    }

    /// Runs `future` to completion on the calling thread, driving this runtime alongside it.
    ///
    /// The call blocks the thread until the future completes. In between polls of the future,
    /// the calling thread also runs the runtime's scheduler and reactor: every task spawned,
    /// source registered or timer armed on it runs on this thread for as long as the call lasts.
    ///
    /// Because that work runs on the calling thread in between polls, the future must not block
    /// that thread waiting for it. A synchronous wait for a task's result, or a busy loop until a
    /// signal arrives, never finishes.
    ///
    /// # Panics
    ///
    /// Panics where the calling thread is driving a runtime already: from inside a task a runtime
    /// is running, or from inside the future of another `block_on`. Such a call would be waiting
    /// for the very thread it is on. With the `helper` feature, a call on a runtime made by
    /// [`Runtime::new`] panics, too, from inside the future of a call of the free `block_on` or of
    /// a `block_on` on a runtime from `SharedRuntime::current`, whether or not that call is driving
    /// its runtime at that moment. A shared runtime made by [`Runtime::new`] is driven by one
    /// thread at a time, so this panics, too, where another thread is inside `block_on` on the
    /// same runtime. One handed out by `SharedRuntime::current`, which the `helper` feature adds,
    /// has a seat for whoever drives it instead: a call on it waits for the seat, and is given it
    /// by the helper thread, as a call of the free `block_on` is.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        M::block_on(&self.core, future)
    }

    /// A future that completes once `duration` has passed. Dropping it cancels the timer.
    pub fn sleep(&self, duration: Duration) -> Sleep<M> {
        // This runtime's timers run on the standard clock, so a length of time is a deadline on
        // it — where the clock has a moment that far ahead. `Duration::MAX`, which a wait of
        // "however long it takes" comes to, has none, and asks for a timer that never fires
        // rather than for a moment the clock cannot name.
        let deadline = Instant::now().checked_add(duration);

        Sleep(reactor::sleep::<M>(&self.core, deadline))
    }

    /// A future that completes once `deadline` has passed: at once, on its first poll, where it
    /// already has. Dropping it cancels the timer.
    ///
    /// This is what a loop that has to keep to a schedule reaches for. One that sleeps for a
    /// period in each round drifts by however long the work of that round took, and the error
    /// adds up with every round. One that adds the period to a deadline of its own and sleeps
    /// until that deadline starts each round when it was due, however long the rounds before it
    /// took.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let period = Duration::from_millis(2);
    /// let started = Instant::now();
    ///
    /// runtime.block_on(async {
    ///     let mut deadline = started;
    ///     for _ in 0..3 {
    ///         deadline += period;
    ///         runtime.sleep_until(deadline).await;
    ///     }
    /// });
    ///
    /// assert!(started.elapsed() >= 3 * period);
    /// ```
    pub fn sleep_until(&self, deadline: Instant) -> Sleep<M> {
        Sleep(reactor::sleep::<M>(&self.core, Some(deadline)))
    }

    /// A future that runs `future` until it completes or `duration` has passed, whichever comes
    /// first: it resolves to what `future` produced, or to [`TimedOut`] where the time ran out.
    /// Dropping it drops `future` and cancels the timer.
    ///
    /// The clock starts here, at the call, rather than at the first poll: a timeout made well
    /// before it is awaited has had that much of its time already. A duration further ahead than
    /// the clock can name, as `Duration::MAX` is, gives a timeout that never fires. Each poll
    /// gives `future` its turn before it looks at the clock, so a future that completes on the
    /// very poll its time runs out on still hands back its output. A future that runs out of time
    /// is not dropped there: it lives on inside the timeout, which [`Timeout::into_inner`] hands
    /// it back from, to be retried or driven on.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{future, io, time::Duration};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    ///
    /// runtime.block_on(async {
    ///     let in_time = runtime.timeout(Duration::from_secs(60), async { 7 }).await;
    ///     assert_eq!(in_time, Ok(7));
    ///
    ///     let late = runtime.timeout(Duration::from_millis(2), future::pending::<()>());
    ///     assert!(late.await.is_err());
    /// });
    ///
    /// // A time-out is an I/O error of its own kind, for code that returns `io::Result`.
    /// fn wait(runtime: &LocalRuntime) -> io::Result<()> {
    ///     runtime.block_on(runtime.timeout(Duration::from_millis(2), future::pending::<()>()))?;
    ///
    ///     Ok(())
    /// }
    /// assert_eq!(wait(&runtime).unwrap_err().kind(), io::ErrorKind::TimedOut);
    /// ```
    pub fn timeout<F>(&self, duration: Duration, future: F) -> Timeout<F::IntoFuture, M>
    where
        F: IntoFuture,
    {
        Timeout::new(future.into_future(), self.sleep(duration))
    }

    /// A future that runs `future` until it completes or `deadline` has passed, whichever comes
    /// first, as [`timeout`](Self::timeout) does: where `deadline` has passed already, a future
    /// that is not ready on its first poll times out on that poll. Dropping it drops `future` and
    /// cancels the timer.
    ///
    /// This is what a piece of work made of several steps reaches for, to keep all of them to
    /// one deadline rather than give each a length of time of its own.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let deadline = Instant::now() + Duration::from_millis(5);
    ///
    /// runtime.block_on(async {
    ///     // Steps of 2 ms each, for as long as each finishes before the deadline.
    ///     while runtime
    ///         .timeout_at(deadline, runtime.sleep(Duration::from_millis(2)))
    ///         .await
    ///         .is_ok()
    ///     {}
    /// });
    ///
    /// // However many steps fit, the loop ended once the deadline had passed.
    /// assert!(Instant::now() >= deadline);
    /// ```
    pub fn timeout_at<F>(&self, deadline: Instant, future: F) -> Timeout<F::IntoFuture, M>
    where
        F: IntoFuture,
    {
        Timeout::new(future.into_future(), self.sleep_until(deadline))
    }

    /// A timer that ticks once every `period`, the first time one period from now. An interval
    /// asked for further ahead than the clock can name never ticks.
    ///
    /// Each tick hands out the moment it was scheduled for, and the ticks keep to the schedule
    /// however long the work done at each of them takes. A tick missed because the task was busy,
    /// or its thread blocked, for a period or more is made up for as the interval's
    /// [`MissedTickBehavior`] says: by default, at once.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero: such an interval would tick on every poll, without end.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let period = Duration::from_millis(2);
    /// let started = Instant::now();
    ///
    /// let ticks = runtime.block_on(async {
    ///     let mut interval = runtime.interval(period);
    ///     [interval.tick().await, interval.tick().await, interval.tick().await]
    /// });
    ///
    /// // Each tick is the moment it was scheduled for, a period after the one before.
    /// assert!(ticks[0] >= started + period);
    /// assert_eq!(ticks[1] - ticks[0], period);
    /// assert_eq!(ticks[2] - ticks[1], period);
    /// ```
    pub fn interval(&self, period: Duration) -> Interval<M> {
        Interval::new(self.sleep(period), period)
    }

    /// A timer that ticks once every `period`, as [`interval`](Self::interval) does, the first
    /// time at `start`: at once, on its first poll, where `start` has passed already, as it has
    /// for `interval_at(Instant::now(), period)`.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero: such an interval would tick on every poll, without end.
    pub fn interval_at(&self, start: Instant, period: Duration) -> Interval<M> {
        Interval::new(self.sleep_until(start), period)
    }
}

#[cfg(feature = "runtime")]
impl Runtime<Local> {
    /// Queues `future` under the diagnostic name `name` and hands back the task that joins or
    /// cancels it.
    ///
    /// The task runs concurrently with the caller, on the thread inside [`Runtime::block_on`] on
    /// this runtime. The handle resolves to what `future` produced, or to `Err` where the runtime
    /// lost the task — after it panicked, say. Dropping the handle cancels the task;
    /// [`Task::detach`] lets it run on.
    ///
    /// `name` says what the task is there for — `"socket reader"`, say. It is for diagnostics
    /// only: it is what the message logged if the task panics names it by. Code with no runtime
    /// to call this on spawns with [`spawn_local`], which takes no name.
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + 'static,
    ) -> Task<T, Local>
    where
        T: 'static,
    {
        Task(scheduler::spawn_local(
            &self.core,
            Name::Given(name.into()),
            future,
        ))
    }

    /// Watches `source` for readiness.
    ///
    /// `source` must be in nonblocking mode, which this leaves to the caller to set: the runtime
    /// waits for readiness, not for I/O, so a read or write on a blocking source blocks the
    /// thread running every other task along with it. The runtime asks `source` for its
    /// descriptor once, here, and watches that one from then on, so `source` keeps it the same,
    /// and open, for as long as it lives, as every type of std's does.
    ///
    /// The registration this hands back stops watching `source` once it is dropped. A read or
    /// write on `source` should go through [`Registration::poll_io`], so that a `WouldBlock`
    /// becomes a wait for the readiness that would clear it rather than a busy loop. `source`
    /// itself is kept by the registration, so the I/O is done through another handle on the
    /// same socket: an `Rc` of it, say, or a clone of its descriptor.
    ///
    /// A runtime watches a descriptor through one registration at a time: a source whose
    /// descriptor it watches already is turned away with
    /// [`AlreadyExists`](io::ErrorKind::AlreadyExists), until the registration that watches it is
    /// dropped. A clone of a descriptor, such as `try_clone` makes, is a descriptor of its own.
    ///
    /// [`AsyncIo`] is the handle that registers a source, keeps it and does the I/O for the caller;
    /// [`Registration::ready`] waits for readiness alone, for a caller that does its I/O some
    /// other way.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{
    ///     future::poll_fn,
    ///     io::{Read, Write},
    ///     net::{TcpListener, TcpStream},
    ///     rc::Rc,
    /// };
    ///
    /// use zruntime::{Interest, LocalRuntime};
    ///
    /// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    /// let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    /// let stream = Rc::new(listener.accept().unwrap().0);
    /// stream.set_nonblocking(true).unwrap();
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let received = runtime.block_on(async {
    ///     let registration = runtime.register(stream.clone()).unwrap();
    ///     peer.write_all(b"!").unwrap();
    ///
    ///     let mut byte = [0];
    ///     poll_fn(|cx| {
    ///         registration.poll_io(cx, Interest::Readable, || (&*stream).read(&mut byte))
    ///     })
    ///     .await
    ///     .unwrap();
    ///
    ///     byte
    /// });
    ///
    /// assert_eq!(&received, b"!");
    /// ```
    pub fn register<S>(&self, source: S) -> io::Result<Registration<Local>>
    where
        S: AsSource + 'static,
    {
        reactor::register::<Local>(&self.core, Rc::new(source))
    }
}

#[cfg(feature = "runtime")]
impl Runtime<Shared> {
    /// A handle on the runtime for what this thread builds, brought into being here if none is
    /// alive.
    ///
    /// Which runtime that is depends on where the call is made. On a thread in the seat of a
    /// runtime — inside [`block_on`], or on the helper thread running a task —
    /// it is that runtime. On a thread inside `block_on` but not in any seat, it is the runtime
    /// the innermost such call drives: the thread's own runtime for a call of the free
    /// [`block_on`], and the runtime it was called on for a [`Runtime::block_on`]. Anywhere else,
    /// it is one runtime the whole process shares, for work some other executor polls: such
    /// work has no thread of its own to look to, so a helper thread runs it, and one runtime
    /// for all of it is one helper and one pair of descriptors rather than a set per thread.
    ///
    /// A runtime handed out here goes once its last handle, and the last of the work built on
    /// it, are gone; the next call brings a fresh one into being. Work built on it runs on the
    /// thread in its seat: one inside `block_on` on it, where there is one, and a helper thread,
    /// started where that work is found with nobody in the seat and gone once nothing is left to
    /// run, watch or time, where there is not.
    ///
    /// A [`LocalRuntime::block_on`], or a `block_on` on a runtime made by [`Runtime::new`], does
    /// not count as a `block_on` here: it drives the runtime it was called on and no other. A call
    /// made inside one gives the runtime the process shares, and a helper thread runs what is
    /// built on it, while the thread inside that `block_on` is left to its own runtime. (A
    /// `block_on` of that kind made the other way round, inside the future of a `block_on` this
    /// feature adds, panics.)
    ///
    /// What can fail is the reactor: it opens the channel a wait is broken through.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{
    ///     future::poll_fn,
    ///     io::{Read, Write},
    ///     net::{TcpListener, TcpStream},
    ///     sync::Arc,
    /// };
    ///
    /// use zruntime::{Interest, SharedRuntime};
    ///
    /// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    /// let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    /// let stream = Arc::new(listener.accept().unwrap().0);
    /// stream.set_nonblocking(true).unwrap();
    ///
    /// let received = zruntime::block_on(async {
    ///     let runtime = SharedRuntime::current().expect("a runtime for this thread");
    ///     let registration = runtime.register(stream.clone()).unwrap();
    ///     peer.write_all(b"!").unwrap();
    ///
    ///     let mut byte = [0];
    ///     poll_fn(|cx| {
    ///         registration.poll_io(cx, Interest::Readable, || (&*stream).read(&mut byte))
    ///     })
    ///     .await
    ///     .unwrap();
    ///
    ///     byte
    /// });
    ///
    /// assert_eq!(&received, b"!");
    /// ```
    ///
    /// This is only available when the `helper` feature is enabled.
    #[cfg(feature = "helper")]
    pub fn current() -> io::Result<Self> {
        Ok(Self {
            core: driver::current()?,
        })
    }

    /// Queues `future` under the diagnostic name `name` and hands back the task that joins or
    /// cancels it.
    ///
    /// The task runs concurrently with the caller, on whichever thread is inside
    /// [`Runtime::block_on`] on this runtime, or, on a runtime from `SharedRuntime::current`, on
    /// a helper thread where no such thread is there to run it. The handle resolves to what
    /// `future` produced, or to `Err` where the runtime lost the task — after it panicked, say.
    /// Dropping the handle cancels the task; [`Task::detach`] lets it run on.
    ///
    /// `name` says what the task is there for — `"socket reader"`, say. It is for diagnostics
    /// only: it is what the message logged if the task panics names it by. Code with no runtime
    /// to call this on spawns with [`spawn`], which takes no name.
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T, Shared>
    where
        T: Send + 'static,
    {
        spawn_on_shared(&self.core, Name::Given(name.into()), future)
    }

    /// Watches `source` for readiness.
    ///
    /// `source` must be in nonblocking mode, which this leaves to the caller to set: the runtime
    /// waits for readiness, not for I/O, so a read or write on a blocking source blocks the
    /// thread running every other task along with it. The runtime asks `source` for its
    /// descriptor once, here, and watches that one from then on, so `source` keeps it the same,
    /// and open, for as long as it lives, as every type of std's does.
    ///
    /// The registration this hands back stops watching `source` once it is dropped. A read or
    /// write on `source` should go through [`Registration::poll_io`], so that a `WouldBlock`
    /// becomes a wait for the readiness that would clear it rather than a busy loop. `source`
    /// itself is kept by the registration, so the I/O is done through another handle on the
    /// same socket: an `Arc` of it, say, or a clone of its descriptor.
    ///
    /// A runtime watches a descriptor through one registration at a time: a source whose
    /// descriptor it watches already is turned away with
    /// [`AlreadyExists`](io::ErrorKind::AlreadyExists), until the registration that watches it is
    /// dropped. A clone of a descriptor, such as `try_clone` makes, is a descriptor of its own.
    ///
    /// [`AsyncIo`] is the handle that registers a source, keeps it and does the I/O for the caller;
    /// [`Registration::ready`] waits for readiness alone, for a caller that does its I/O some
    /// other way.
    pub fn register<S>(&self, source: S) -> io::Result<Registration<Shared>>
    where
        S: AsSource + Send + Sync + 'static,
    {
        let registered = reactor::register::<Shared>(&self.core, Arc::new(source))?;
        // Asked for once the source is in the reactor's map, so that a helper starting here
        // takes it into its very first wait.
        self.core.ensure_progress();

        Ok(registered)
    }

    /// A handle on `core`, whatever registry it is or is not in.
    #[cfg(all(test, feature = "helper"))]
    pub(crate) fn from_inner(core: Arc<Core<Shared>>) -> Self {
        Self { core }
    }

    /// What this handle is on.
    #[cfg(all(test, feature = "helper"))]
    pub(crate) fn inner(&self) -> &Arc<Core<Shared>> {
        &self.core
    }

    /// Whether the helper thread is running.
    #[cfg(all(test, feature = "helper"))]
    pub(crate) fn helper_running(&self) -> bool {
        driver::seat(&self.core).helper_running()
    }

    /// Whether the helper thread is parked for want of the seat.
    ///
    /// Only tests that wait for an `Event` ask this, so it is built with the `event` feature.
    #[cfg(all(test, feature = "helper", feature = "event"))]
    pub(crate) fn helper_parked(&self) -> bool {
        driver::seat(&self.core).helper_parked()
    }
}

#[cfg(feature = "runtime")]
impl<M> Clone for Runtime<M>
where
    M: Mode,
{
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

#[cfg(feature = "runtime")]
impl<M> fmt::Debug for Runtime<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// The readiness an I/O operation waits for.
#[cfg(feature = "runtime")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Interest {
    /// The source has bytes to be read, or has reached its end.
    Readable,
    /// The source has room for bytes to be written, or a connect under way has settled.
    Writable,
}

/// A task spawned on a [`Runtime`], which cancels that task when dropped.
///
/// [`Task::cancel`] cancels it as well, and waits for it to stop.
#[cfg(feature = "runtime")]
pub struct Task<T, M = Local>(JoinHandle<T, M>)
where
    M: Mode;

#[cfg(feature = "runtime")]
impl<T, M> Task<T, M>
where
    M: Mode,
{
    /// Lets the task run to completion on its own.
    ///
    /// A detached task is the runtime's to keep: it runs until it ends, whenever a thread is
    /// inside [`Runtime::block_on`] on its runtime, and on a runtime from
    /// `SharedRuntime::current`, on a helper thread where no such thread is there to run it. A
    /// task that never ends is kept, with everything its future holds, for as long as the runtime
    /// lives, and where a helper runs it, keeps that helper for the life of the process.
    pub fn detach(self) {
        self.0.detach();
    }

    /// Cancels the task, and hands back a future that resolves once the task's future is gone.
    ///
    /// Dropping a task cancels it too, but does not wait: a task that another thread is polling
    /// at that moment is left to finish that poll, and its future, with everything it holds, goes
    /// only then. Cancelling it here, and awaiting what this hands back, is for a caller that has
    /// to know when that is — to bind a socket to the address of one the task held, say, or to
    /// take a lock it held — or that wants the output of a task that may have finished already.
    ///
    /// The cancellation is made here, in the call, whether or not the future handed back is ever
    /// polled. A task that is waiting, or has not been polled yet, has its future dropped right
    /// here, on the calling thread. One that is being polled at that moment has it dropped once
    /// that poll returns: by another thread, or by this one where a task cancels itself. And
    /// where the runtime is going just then, its last handle being dropped, perhaps on another
    /// thread, the runtime drops the future as it goes, which may be after this call has
    /// returned. It is awaiting the future handed back that tells when the task's future is
    /// gone: that resolves once it has been dropped, however that came about, to the task's
    /// output where the task had finished before the cancellation reached it, and to `None`
    /// otherwise. A task that panicked, went with its runtime, or had its output taken already
    /// by being awaited has none to hand back either. Dropping that future is the same as
    /// dropping the task.
    ///
    /// A task's future whose destructor panics as it is dropped here panics out of this call, as
    /// it would out of a drop of the task.
    ///
    /// # Example
    ///
    /// ```
    /// use std::{future, rc::Rc, time::Duration};
    ///
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let connection = Rc::new("a connection");
    ///
    /// runtime.block_on(async {
    ///     let answer = runtime.spawn("an answer", async { 42 });
    ///     let held = connection.clone();
    ///     let reader = runtime.spawn("a reader that never finishes", async move {
    ///         let _connection = held;
    ///         future::pending::<u32>().await
    ///     });
    ///     // Each wait gives the tasks a turn, until the first one has finished.
    ///     while !answer.is_finished() {
    ///         runtime.sleep(Duration::from_millis(1)).await;
    ///     }
    ///
    ///     // A task that finished hands its output back...
    ///     assert_eq!(answer.cancel().await, Some(42));
    ///     // ...and one that did not has let go of what it held by the time the wait is over.
    ///     assert_eq!(reader.cancel().await, None);
    ///     assert_eq!(Rc::strong_count(&connection), 1);
    /// });
    /// ```
    pub fn cancel(self) -> impl Future<Output = Option<T>> {
        self.0.cancel()
    }

    /// Whether the task has ended, told without polling it or taking its output.
    ///
    /// A task has ended once its future has completed or panicked, or has gone with the runtime
    /// it was on. Either way, the future has been dropped, and everything it held with it, by the
    /// time this says so.
    ///
    /// This stays `true` after the output has been taken by awaiting the task. It is how a caller
    /// that has no use for the output yet, or ever, finds out that a task is over, where awaiting
    /// the task would take that output.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::LocalRuntime;
    ///
    /// let runtime = LocalRuntime::new().unwrap();
    /// let mut task = runtime.spawn("an answer", async { 42 });
    /// assert!(!task.is_finished());
    ///
    /// assert_eq!(runtime.block_on(&mut task).unwrap(), 42);
    /// assert!(task.is_finished());
    /// ```
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[cfg(feature = "runtime")]
impl<T, M> fmt::Debug for Task<T, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Task").field(&self.0).finish()
    }
}

#[cfg(feature = "runtime")]
impl<T, M> Future for Task<T, M>
where
    M: Mode,
{
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_join(cx)
    }
}

/// Queues `future` on the shared runtime `core` as a task named `name`: what [`Runtime::spawn`]
/// and the free [`spawn`] have in common, so that a task spawned either way is seen to alike.
#[cfg(feature = "runtime")]
fn spawn_on_shared<F>(core: &Arc<Core<Shared>>, name: Name, future: F) -> Task<F::Output, Shared>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = Task(scheduler::spawn_shared(core, name, future));
    // Asked for once the task is on the scheduler's queue, so that a helper starting here finds it
    // there.
    core.ensure_progress();

    task
}

/// The shared runtime the free [`spawn`] puts a task on: the one the calling thread drives, and
/// where it drives none, the one `SharedRuntime::current` hands out, if the `helper` feature is
/// there to make it.
#[cfg(feature = "runtime")]
#[track_caller]
fn shared_to_spawn_on() -> Arc<Core<Shared>> {
    if let Some(core) = Shared::driven() {
        return core;
    }
    #[cfg(feature = "helper")]
    return match driver::current_to_spawn_on() {
        Ok(core) => core,
        Err(error) => panic!("spawn found no runtime to spawn on, and could not make one: {error}"),
    };
    #[cfg(not(feature = "helper"))]
    panic!(
        "spawn called where no shared runtime is running: call it from inside \
         `SharedRuntime::block_on`, or from a task of that runtime, or enable the `helper` feature"
    );
}

#[cfg(any(test, doctest))]
mod tests;
