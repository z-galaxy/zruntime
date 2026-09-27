#![doc = include_str!("../README.md")]

mod driver;
mod log;
mod poll;
mod reactor;
mod scheduler;

#[cfg(unix)]
use std::os::fd::AsFd;
#[cfg(windows)]
use std::os::windows::io::AsSocket;
use std::{
    borrow::Cow,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    task::{Context, Poll},
    time::Duration,
};

use reactor::Reactor;
pub use reactor::Registration;
use scheduler::{JoinHandle, Scheduler};

/// Runs `future` to completion on the calling thread, running that thread's runtime alongside it.
///
/// This is for a program that has no async runtime of its own. Put the async code in one call to
/// this function; the call blocks the thread until the future completes. In between polls of the
/// future, the calling thread also runs the scheduler and the reactor of everything built on the
/// runtime this call drives — every task spawned, source registered or timer armed from inside
/// it — and two threads that each call this drive their own runtime, in parallel. If the call
/// returns while some of that work is still alive, a helper thread takes it over until the next
/// call, or until the work is gone.
///
/// Because that work runs on the calling thread in between polls, the future must not block that
/// thread waiting for it. A synchronous wait for a task's result, or a busy loop until a signal
/// arrives, never finishes.
///
/// Do not call this from inside a task this runtime is running. It panics there, because it
/// would be waiting for the very thread it is on. Do not call it from another runtime's task
/// either: it blocks that task's thread until the future completes, which deadlocks the program
/// if the future needs that thread to make progress.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    driver::block_on(
        &|| OWN.try_with(|own| lock(own).upgrade()).ok().flatten(),
        future,
    )
}

/// A handle to a runtime: a scheduler, a reactor, and a seat for whoever runs them.
///
/// Cloning it is cheap, and every clone reaches the same scheduler and reactor.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}

impl Runtime {
    /// A handle on the runtime for what this thread builds, brought into being here if none is
    /// alive.
    ///
    /// What can fail is the reactor: it opens the channel a wait is broken through.
    pub fn current() -> io::Result<Self> {
        Ok(Self {
            inner: Inner::current()?,
        })
    }

    /// Queues `future` under the diagnostic name `name` and hands back the task that joins or
    /// cancels it.
    ///
    /// The task runs concurrently with the caller, on the runtime's own thread or a helper's. The
    /// handle resolves to what `future` produced, or to `Err` where the runtime lost the task —
    /// after it panicked, say. Dropping the handle cancels the task; [`Task::detach`] lets it run
    /// on.
    ///
    /// `name` says what the task is there for — `"socket reader"`, say. It is for diagnostics
    /// only: it is what the message logged if the task panics names it by.
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T>
    where
        T: Send + 'static,
    {
        let task = Task(self.inner.spawn(name.into(), future));
        // Asked for once the task is on the scheduler's queue, so that a helper starting here
        // finds it there.
        self.inner.ensure_progress();

        task
    }

    /// A future that completes once `duration` has passed. Dropping it cancels the timer.
    pub fn sleep(&self, duration: Duration) -> Sleep {
        Sleep(self.inner.sleep(duration))
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
    /// use zruntime::{Interest, Runtime};
    ///
    /// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    /// let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    /// let stream = Arc::new(listener.accept().unwrap().0);
    /// stream.set_nonblocking(true).unwrap();
    ///
    /// let received = zruntime::block_on(async {
    ///     let runtime = Runtime::current().expect("a runtime for this thread");
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
    pub fn register<S>(&self, source: S) -> io::Result<Registration>
    where
        S: Source,
    {
        let registered = self.inner.register(source)?;
        // Asked for once the source is in the reactor's map, so that a helper starting here
        // takes it into its very first wait.
        self.inner.ensure_progress();

        Ok(registered)
    }

    /// A handle on `inner`, whatever registry it is or is not in.
    #[cfg(test)]
    pub(crate) fn from_inner(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// What this handle is on.
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Whether the helper thread is running.
    #[cfg(test)]
    pub(crate) fn helper_running(&self) -> bool {
        lock(&self.inner.seat).helper_running()
    }

    /// Whether the helper thread is parked for want of the seat.
    #[cfg(test)]
    pub(crate) fn helper_parked(&self) -> bool {
        lock(&self.inner.seat).helper_parked()
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("helper", &lock(&self.inner.seat).helper_running())
            .finish_non_exhaustive()
    }
}

/// A socket or pipe a [`Runtime`] can watch for readiness.
///
/// Implemented for every type that owns its descriptor and can be shared between threads: any
/// `T: `[`AsFd`]` + Send + Sync + 'static` on unix.
#[cfg(unix)]
pub trait Source: AsFd + Send + Sync + 'static {}

#[cfg(unix)]
impl<T> Source for T where T: AsFd + Send + Sync + 'static {}

/// A socket a [`Runtime`] can watch for readiness.
///
/// Implemented for every type that owns its socket and can be shared between threads: any
/// `T: `[`AsSocket`]` + Send + Sync + 'static`.
#[cfg(windows)]
pub trait Source: AsSocket + Send + Sync + 'static {}

#[cfg(windows)]
impl<T> Source for T where T: AsSocket + Send + Sync + 'static {}

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
pub struct Task<T>(JoinHandle<T>);

impl<T> Task<T> {
    /// Lets the task run to completion on its own.
    ///
    /// A detached task is the runtime's to keep: it runs until it ends, on a helper thread where
    /// no thread inside [`block_on`] is there to run it. A task that never ends keeps that helper,
    /// and everything its future holds, for the life of the process.
    pub fn detach(self) {
        self.0.detach();
    }
}

impl<T> fmt::Debug for Task<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Task").field(&self.0).finish()
    }
}

impl<T> Future for Task<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

/// A timer on a [`Runtime`], which keeps a thread on it for as long as it has a deadline.
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, and
/// the thread that is to fire it has to be there from that poll onwards, however long ago the
/// timer was asked for. So it is the poll that asks for one, and a timer nobody ever polls costs
/// nothing at all.
pub struct Sleep(reactor::Sleep);

impl Future for Sleep {
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
        this.0.runtime().ensure_progress();

        Poll::Pending
    }
}

/// What a runtime is made of, shared by every handle on it and by the thread that runs it.
pub(crate) struct Inner {
    scheduler: Scheduler,
    reactor: Reactor,
    /// Who runs this runtime; the lock the hand-over decisions are made under.
    seat: Mutex<driver::Seat>,
}

impl Inner {
    /// The runtime for what the calling thread builds, made here if none is alive: the one it is
    /// in the seat of, where it is in one; its own, where it is inside a `block_on` and so has a
    /// thread to run what it builds; and the process's otherwise.
    ///
    /// Once this thread's `OWN` local is gone, there is no registry left to hold what it builds:
    /// inside a `block_on`, that goes on a runtime in no registry, which a helper thread runs;
    /// outside one, it still goes on the process's shared registry as before.
    fn current() -> io::Result<Arc<Self>> {
        if let Some(inner) = driver::driven() {
            return Ok(inner);
        }
        if !driver::in_block_on() {
            return Self::shared_in(&SHARED);
        }
        match OWN.try_with(Self::shared_in) {
            Ok(inner) => inner,
            Err(_) => Self::new(),
        }
    }

    /// Whether `inner` is the calling thread's own runtime, the one its `block_on` drives.
    ///
    /// Nothing is a thread's own once its locals are gone, so work handed over then gets a
    /// helper, as work handed to any runtime the caller does not drive does.
    pub(crate) fn is_own(inner: &Arc<Self>) -> bool {
        OWN.try_with(|own| std::ptr::eq(lock(own).as_ptr(), Arc::as_ptr(inner)))
            .unwrap_or(false)
    }

    /// The runtime `registry` names, made here if none is alive.
    pub(crate) fn shared_in(registry: &Mutex<Weak<Self>>) -> io::Result<Arc<Self>> {
        let mut shared = lock(registry);
        if let Some(inner) = shared.upgrade() {
            return Ok(inner);
        }
        let inner = Self::new()?;
        *shared = Arc::downgrade(&inner);

        Ok(inner)
    }

    /// A runtime with a scheduler and a reactor of its own, in no registry.
    pub(crate) fn new() -> io::Result<Arc<Self>> {
        let reactor = Reactor::new()?;
        // The hook is made before the runtime it belongs to, so it can only hold the runtime
        // weakly; a wake that finds it gone has no wait left to break.
        Ok(Arc::new_cyclic(|runtime: &Weak<Self>| {
            let runtime = runtime.clone();
            Self::assemble(reactor, move || {
                if let Some(inner) = runtime.upgrade() {
                    inner.reactor.notify();
                }
            })
        }))
    }

    /// A runtime whose scheduler calls `notify` in place of the reactor, for tests of the
    /// scheduler alone.
    #[cfg(test)]
    pub(crate) fn with_notify(notify: impl Fn() + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self::assemble(Reactor::new().unwrap(), notify))
    }

    fn assemble(reactor: Reactor, notify: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            scheduler: Scheduler::new(notify),
            reactor,
            seat: Mutex::new(driver::Seat::new()),
        }
    }

    /// Whether anything is left to run, watch or time.
    pub(crate) fn is_busy(&self) -> bool {
        self.scheduler.has_ready() || self.scheduler.live_tasks() > 0 || !self.reactor.is_idle()
    }

    /// Sees to it that the work just handed over is run: a helper is started unless a thread is
    /// in the seat or about to take it. Called after that work is in place, never before.
    ///
    /// Whoever runs the work holds a runtime of its own while it does, so what has been handed
    /// over runs to completion even once every [`Runtime`] clone is gone.
    fn ensure_progress(self: &Arc<Self>) {
        driver::ensure_helper(self);
    }
}

/// Makes `inner` the calling thread's own runtime until the value handed back is dropped, which
/// puts back whatever was there before, whether the call returns or unwinds.
///
/// For a test that drives a runtime of its own making: the runtime a `block_on` resolves to is
/// always the one its thread builds on, so a spawn from inside such a call asks for no helper,
/// and a test that drives a runtime nobody's registry names would be told otherwise.
#[cfg(test)]
pub(crate) fn own_for_the_call(inner: &Arc<Inner>) -> impl Drop {
    struct Restore(Weak<Inner>);

    impl Drop for Restore {
        fn drop(&mut self) {
            OWN.with(|own| *lock(own) = std::mem::take(&mut self.0));
        }
    }

    OWN.with(|own| Restore(std::mem::replace(&mut *lock(own), Arc::downgrade(inner))))
}

thread_local! {
    /// This thread's runtime, if one is alive: the one a `block_on` on this thread drives, and
    /// the one work built inside such a call goes on.
    ///
    /// A `Weak`, so that the runtime and the two descriptors its reactor holds go once the last
    /// handle and any thread running it are gone, and the next handle brings a fresh one. A
    /// thread that ends with work alive leaves it to the helper thread that took it over when
    /// its last `block_on` returned.
    static OWN: Mutex<Weak<Inner>> = const { Mutex::new(Weak::new()) };
}

/// The runtime for work built on a thread that is inside no `block_on` and in no seat: work some
/// other executor polls, wherever in the process it is built.
///
/// Such work has no thread of its own to look to, so a helper runs it, and one runtime for all
/// of it is one helper and one pair of descriptors rather than a set per thread. A `Weak`, for
/// the same reason [`OWN`] is.
static SHARED: Mutex<Weak<Inner>> = Mutex::new(Weak::new());

/// The value behind a lock, taken whether or not a panic poisoned it.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
