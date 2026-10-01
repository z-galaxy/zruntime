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

#[cfg(feature = "broadcast")]
pub mod broadcast;
#[cfg(feature = "helper")]
mod driver;
#[cfg(feature = "event")]
mod event;
#[cfg(feature = "lock")]
pub mod lock;
#[cfg(feature = "runtime")]
mod log;
#[cfg(feature = "runtime")]
mod mode;
#[cfg(feature = "runtime")]
mod poll;
#[cfg(feature = "runtime")]
mod reactor;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod scheduler;
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
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(feature = "event")]
pub use event::{Event, EventListener};
#[cfg(feature = "runtime")]
pub use mode::{Local, Mode, Shared};
#[cfg(feature = "runtime")]
pub use reactor::Registration;
#[cfg(feature = "runtime")]
use runtime::Core;
#[cfg(feature = "runtime")]
use scheduler::JoinHandle;
#[cfg(feature = "unblock")]
pub use unblock::{Unblock, unblock};

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
    /// such a task to completion, or keep its [`Task`] and cancel it by dropping that.
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
        Sleep(reactor::sleep::<M>(&self.core, duration))
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
    /// only: it is what the message logged if the task panics names it by.
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + 'static,
    ) -> Task<T, Local>
    where
        T: 'static,
    {
        Task(scheduler::spawn_local(&self.core, name.into(), future))
    }

    /// Watches `source` for readiness.
    ///
    /// `source` must be in nonblocking mode, which this leaves to the caller to set: the runtime
    /// waits for readiness, not for I/O, so a read or write on a blocking source blocks the
    /// thread running every other task along with it.
    ///
    /// The registration this hands back stops watching `source` once it is dropped. A read or
    /// write on `source` should go through [`Registration::poll_io`], so that a `WouldBlock`
    /// becomes a wait for the readiness that would clear it rather than a busy loop. `source`
    /// itself is kept by the registration, so the I/O is done through another handle on the
    /// same socket: an `Rc` of it, say, or a clone of its descriptor.
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
    /// only: it is what the message logged if the task panics names it by.
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T, Shared>
    where
        T: Send + 'static,
    {
        let task = Task(scheduler::spawn_shared(&self.core, name.into(), future));
        // Asked for once the task is on the scheduler's queue, so that a helper starting here
        // finds it there.
        self.core.ensure_progress();

        task
    }

    /// Watches `source` for readiness.
    ///
    /// `source` must be in nonblocking mode, which this leaves to the caller to set: the runtime
    /// waits for readiness, not for I/O, so a read or write on a blocking source blocks the
    /// thread running every other task along with it.
    ///
    /// The registration this hands back stops watching `source` once it is dropped. A read or
    /// write on `source` should go through [`Registration::poll_io`], so that a `WouldBlock`
    /// becomes a wait for the readiness that would clear it rather than a busy loop. `source`
    /// itself is kept by the registration, so the I/O is done through another handle on the
    /// same socket: an `Arc` of it, say, or a clone of its descriptor.
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

/// A timer on a [`Runtime`], which keeps a thread on it for as long as it has a deadline.
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, and
/// the thread that is to fire it has to be there from that poll onwards, however long ago the
/// timer was asked for. So it is the poll that asks for one — a helper thread, on a runtime from
/// `SharedRuntime::current` that nobody is inside `block_on` on — and a timer nobody ever polls
/// costs nothing at all. A timer holds its runtime, so a task holding one keeps that runtime
/// alive: nothing here takes a runtime down while it has work.
#[cfg(feature = "runtime")]
pub struct Sleep<M = Local>(reactor::Sleep<M>)
where
    M: Mode;

#[cfg(feature = "runtime")]
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

#[cfg(any(test, doctest))]
mod tests;
