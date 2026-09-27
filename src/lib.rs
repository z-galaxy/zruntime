#![doc = include_str!("../README.md")]
#![deny(rust_2018_idioms)]
#![doc(test(attr(
    warn(unused),
    deny(warnings),
    allow(dead_code),
    // W/o this, we seem to get some bogus warning about `extern crate zbus`.
    allow(unused_extern_crates),
)))]

mod log;
mod mode;
mod poll;
mod reactor;
mod runtime;
mod scheduler;

#[cfg(unix)]
use std::os::fd::AsFd as AsSource;
#[cfg(windows)]
use std::os::windows::io::AsSocket as AsSource;
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

pub use mode::{Local, Mode, Shared};
pub use reactor::Registration;
use runtime::Core;
use scheduler::JoinHandle;

/// A runtime that stays on the thread it was made on, and runs futures that need not be `Send`.
pub type LocalRuntime = Runtime<Local>;

/// A runtime that may be reached from, and driven on, any thread, and runs `Send` futures.
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
pub struct Runtime<M = Local>
where
    M: Mode,
{
    core: M::Ptr<Core<M>>,
}

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
    /// What can fail is the reactor: it opens the channel a wait is broken through.
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
    /// for the very thread it is on. A shared runtime is driven by one thread at a time, so this
    /// panics, too, where another thread is inside `block_on` on the same runtime.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.core.block_on(future)
    }

    /// A future that completes once `duration` has passed. Dropping it cancels the timer.
    pub fn sleep(&self, duration: Duration) -> Sleep<M> {
        Sleep(reactor::sleep::<M>(&self.core, duration))
    }
}

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
        let (join, task) = scheduler::task::<Local, _>(name.into(), future);

        Task(scheduler::spawn::<Local, T>(
            &self.core,
            join,
            Box::pin(task),
        ))
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

impl Runtime<Shared> {
    /// Queues `future` under the diagnostic name `name` and hands back the task that joins or
    /// cancels it.
    ///
    /// The task runs concurrently with the caller, on whichever thread is inside
    /// [`Runtime::block_on`] on this runtime. The handle resolves to what `future` produced, or to
    /// `Err` where the runtime lost the task — after it panicked, say. Dropping the handle cancels
    /// the task; [`Task::detach`] lets it run on.
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
        let (join, task) = scheduler::task::<Shared, _>(name.into(), future);

        Task(scheduler::spawn::<Shared, T>(
            &self.core,
            join,
            Box::pin(task),
        ))
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
        reactor::register::<Shared>(&self.core, Arc::new(source))
    }
}

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

impl<M> fmt::Debug for Runtime<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// The readiness an I/O operation waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Interest {
    /// The source has bytes to be read, or has reached its end.
    Readable,
    /// The source has room for bytes to be written, or a connect under way has settled.
    Writable,
}

/// A task spawned on a [`Runtime`], which cancels that task when dropped.
pub struct Task<T, M = Local>(JoinHandle<T, M>)
where
    M: Mode;

impl<T, M> Task<T, M>
where
    M: Mode,
{
    /// Lets the task run to completion on its own.
    ///
    /// A detached task is the runtime's to keep: it runs until it ends, whenever a thread is
    /// inside [`Runtime::block_on`] on its runtime. A task that never ends is kept, with
    /// everything its future holds, for as long as the runtime lives.
    pub fn detach(self) {
        self.0.detach();
    }
}

impl<T, M> fmt::Debug for Task<T, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Task").field(&self.0).finish()
    }
}

impl<T, M> Future for Task<T, M>
where
    M: Mode,
{
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_join(cx)
    }
}

/// A timer on a [`Runtime`].
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, so
/// a timer nobody ever polls costs nothing at all. A timer holds its runtime, so a task holding
/// one keeps that runtime alive: nothing here takes a runtime down while it has work.
pub struct Sleep<M = Local>(reactor::Sleep<M>)
where
    M: Mode;

impl<M> Future for Sleep<M>
where
    M: Mode,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

#[cfg(any(test, doctest))]
mod tests;
