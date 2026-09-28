//! Tests of a runtime driven by nothing but the `block_on` calls made on it: the tasks, timers
//! and registrations it runs on the thread inside such a call, the wakes that reach that thread
//! from others, and the rules on who may drive it.
//!
//! Most of these run in both flavours, through [`in_both_modes`]; the ones that only mean
//! something in one flavour — a local task holding an `Rc`, a second thread asking to drive a
//! shared runtime — are written for that flavour alone.

use std::{
    cell::RefCell,
    future::{Future, pending, poll_fn},
    io::{self, Write},
    mem::MaybeUninit,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use futures_lite::future::yield_now;
use ntest::timeout;
use socket2::{SockRef, Socket};

use crate::{
    Event, Interest, Local, LocalRuntime, Mode, Registration, Runtime, Shared, SharedRuntime,
    Sleep, Task,
};

/// Writes the test that follows once per flavour: a module named after it, holding a `local`
/// and a `shared` test that run its body with the mode parameter set to [`Local`] and to
/// [`Shared`].
///
/// The body reaches spawning and registering through [`TestMode`], whose bounds both flavours
/// meet, and everything else through [`Runtime`] itself.
macro_rules! in_both_modes {
    ($(#[$attr:meta])* fn $name:ident<$mode:ident>() $body:block) => {
        $(#[$attr])*
        mod $name {
            use super::*;

            #[test]
            #[timeout(15000)]
            fn local() {
                run::<Local>();
            }

            #[test]
            #[timeout(15000)]
            fn shared() {
                run::<Shared>();
            }

            fn run<$mode>()
            where
                $mode: TestMode,
            $body
        }
    };
}

in_both_modes! {
    fn a_spawned_task_hands_its_output_back<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let task = M::spawn(&runtime, "an answer", async { 42 });

        assert_eq!(runtime.block_on(task).unwrap(), 42);
    }
}

in_both_modes! {
    /// A task runs on the thread inside `block_on`, whether it was spawned from inside the call
    /// or before it.
    fn tasks_run_on_the_thread_inside_block_on<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let before = M::spawn(&runtime, "a task spawned before the call", async {
            thread::current().id()
        });

        let (before, inside) = runtime.block_on(async {
            let inside = M::spawn(&runtime, "a task spawned inside the call", async {
                thread::current().id()
            });

            (before.await.unwrap(), inside.await.unwrap())
        });

        assert_eq!(before, thread::current().id());
        assert_eq!(inside, thread::current().id());
    }
}

in_both_modes! {
    /// Work handed to a runtime nobody is driving waits for the next `block_on` on it.
    fn work_waits_for_the_next_block_on<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let task = {
            let ran = ran.clone();
            M::spawn(&runtime, "a task that sets a flag", async move {
                ran.store(true, Ordering::Release);
            })
        };

        // Long enough for a thread of the runtime's own, were there one, to have run the task.
        thread::sleep(Duration::from_millis(50));
        assert!(!ran.load(Ordering::Acquire));

        runtime.block_on(task).unwrap();
        assert!(ran.load(Ordering::Acquire));
    }
}

in_both_modes! {
    fn a_detached_task_runs_to_completion<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let done = Arc::new(Event::new());
        // Taken before the task is spawned, so that the announcement cannot be missed.
        let announced = done.listen();

        M::spawn(&runtime, "a detached task", async move {
            // Left waiting once, so that it is the detached task's later poll that ends it.
            yield_now().await;
            done.notify(1);
        })
        .detach();

        runtime.block_on(announced);
    }
}

in_both_modes! {
    fn dropping_a_task_cancels_and_drops_its_future<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let task = {
            let marker = SetOnDrop(dropped.clone());
            M::spawn(&runtime, "a task that never finishes", async move {
                let _marker = marker;
                pending::<()>().await;
            })
        };
        // A yield is one round of the loop, in which the task is polled and left waiting.
        runtime.block_on(yield_now());
        assert!(!dropped.load(Ordering::Acquire));

        drop(task);

        assert!(dropped.load(Ordering::Acquire));
    }
}

in_both_modes! {
    fn a_panicking_task_fails_its_handle_and_the_runtime_carries_on<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let panicking = M::spawn(&runtime, "a task that panics", async {
            panic!("the task panicked on purpose");
        });

        let error = runtime.block_on(panicking).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        let next = M::spawn(&runtime, "the task after it", async { 7 });
        assert_eq!(runtime.block_on(next).unwrap(), 7);
    }
}

in_both_modes! {
    /// A future whose destructor panics takes nothing with it: not a value it had already
    /// produced, not the runtime that drops it, and not the handle that cancels it, which sees
    /// the panic itself.
    fn a_panicking_destructor_is_contained<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        // A future of its own rather than an async block holding the marker: the block would drop
        // the marker inside its last poll, which is a panic of that poll.
        let going = M::spawn(&runtime, "a task that panics as it goes", ReadyThenPanicOnDrop(7));

        assert_eq!(runtime.block_on(going).unwrap(), 7);

        let cancelled = M::spawn(&runtime, "a task that never finishes", async {
            let _marker = PanicOnDrop;
            pending::<()>().await;
        });
        runtime.block_on(yield_now());
        // Idle when it is dropped, so the drop of its future is this thread's to make, and the
        // panic is the caller's to see.
        assert!(catch_unwind(AssertUnwindSafe(|| drop(cancelled))).is_err());

        let next = M::spawn(&runtime, "the task after them", async { 7 });
        assert_eq!(runtime.block_on(next).unwrap(), 7);
    }
}

in_both_modes! {
    /// A wake from another thread ends the wait the thread inside `block_on` is in.
    ///
    /// Nothing to watch and no deadline, so the wait is bounded by nothing but the notification
    /// the wake writes.
    fn a_wake_from_another_thread_ends_the_wait<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let wakers = Arc::new(Mutex::new(None::<Waker>));
        let waker_thread = {
            let wakers = wakers.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                let waker = wakers.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(waker) = waker {
                    waker.wake();
                }
            })
        };
        let started = Instant::now();

        runtime.block_on(async {
            let mut polled = false;
            poll_fn(|cx| {
                if polled {
                    return Poll::Ready(());
                }
                polled = true;
                *wakers.lock().unwrap_or_else(PoisonError::into_inner) = Some(cx.waker().clone());

                Poll::Pending
            })
            .await
        });

        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(started.elapsed() < Duration::from_secs(5));
        waker_thread.join().unwrap();
    }
}

in_both_modes! {
    fn sleep_resolves_once_the_duration_has_passed<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let started = Instant::now();

        runtime.block_on(runtime.sleep(Duration::from_millis(50)));

        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}

in_both_modes! {
    fn a_registration_reports_readiness<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let (source, mut peer) = pair();
        let registration = M::register(&runtime, source.clone()).unwrap();
        // Written from another thread once the read below is waiting, so that the byte is one the
        // wait reports rather than one the first attempt to read finds for itself.
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            peer.write_all(&[7]).unwrap();
        });

        let read = runtime.block_on(read_one(&registration, &source));

        assert_eq!(read.unwrap(), 1);
        writer.join().unwrap();
    }
}

in_both_modes! {
    /// A socket of one protocol family is watched beside a wake channel of another.
    ///
    /// Winsock takes every socket of one `select` call to come from a single service provider,
    /// and a runtime's wake channel is a loopback TCP connection while a caller may register an
    /// `AF_UNIX` socket directly. A wait therefore holds sockets of both families, and this is
    /// where that mixture is asked for: an `AF_UNIX` socket registered on a runtime is reported
    /// readable just as a socket of the wake channel's own family is.
    #[cfg(windows)]
    fn an_af_unix_socket_is_watched_beside_the_tcp_wake_socket<M>() {
        use std::{
            env, fs,
            os::windows::io::{AsSocket, OwnedSocket},
            process,
            time::SystemTime,
        };

        use uds_windows::{UnixListener, UnixStream};

        let runtime = Runtime::<M>::new().unwrap();
        // Named after this process, this moment and the flavour under test, so that two runs of
        // the suite, or the two flavours of this test running side by side, cannot meet over one
        // path.
        let since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let path = env::temp_dir().join(format!(
            "zruntime-{}-{}-{}.sock",
            process::id(),
            since_epoch.as_nanos(),
            std::any::type_name::<M>().rsplit("::").next().unwrap_or_default(),
        ));
        let listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();
        server.set_nonblocking(true).unwrap();
        // The stream keeps the handle it was made with, so what the source is given is a
        // duplicate of it.
        let owned: OwnedSocket = client.as_socket().try_clone_to_owned().unwrap();
        let source: TestSource = Arc::new(owned);
        let registration = M::register(&runtime, source.clone()).unwrap();
        // Written from another thread once the read below is waiting, so that the byte is one the
        // wait reports rather than one the first attempt to read finds for itself.
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            server.write_all(&[7]).unwrap();
        });

        let read = runtime.block_on(read_one(&registration, &source));

        assert_eq!(read.unwrap(), 1);
        writer.join().unwrap();
        drop(listener);
        fs::remove_file(&path).unwrap();
    }
}

in_both_modes! {
    /// A runtime that goes with tasks still on it drops their futures and fails their handles,
    /// whether they were polled before it went or never were.
    fn dropping_the_runtime_fails_pending_handles<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let polled = M::spawn(&runtime, "a task that never finishes", pending::<()>());
        runtime.block_on(yield_now());
        let dropped = Arc::new(AtomicBool::new(false));
        let never_polled = {
            let marker = SetOnDrop(dropped.clone());
            M::spawn(&runtime, "a task that is never polled", async move {
                let _marker = marker;
            })
        };

        drop(runtime);

        assert!(dropped.load(Ordering::Acquire));
        for task in [polled, never_polled] {
            let error = futures_lite::future::block_on(task).unwrap_err();
            assert_eq!(error.to_string(), "the task's runtime is gone");
        }
    }
}

in_both_modes! {
    /// A `block_on` from a thread that is driving a runtime already panics rather than hangs:
    /// from inside the future of a `block_on` on the same runtime, or on another runtime of
    /// either flavour. The call that panics leaves the one it was made from driving as before.
    fn a_nested_block_on_panics<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let local = LocalRuntime::new().unwrap();
        let shared = SharedRuntime::new().unwrap();

        let nested = runtime.block_on(async {
            let panicked = [
                catch_unwind(AssertUnwindSafe(|| runtime.block_on(async {}))).is_err(),
                catch_unwind(AssertUnwindSafe(|| local.block_on(async {}))).is_err(),
                catch_unwind(AssertUnwindSafe(|| shared.block_on(async {}))).is_err(),
            ];
            // Still driving: a task and a timer spawned after the panics run as ever.
            let task = M::spawn(&runtime, "a task after the panics", async { 7 });
            runtime.sleep(Duration::from_millis(1)).await;

            (panicked, task.await.unwrap())
        });

        assert_eq!(nested, ([true; 3], 7));
        // And the thread drives each of them in turn once it has left the first.
        assert_eq!(local.block_on(async { 7 }), 7);
        assert_eq!(shared.block_on(async { 7 }), 7);
    }
}

in_both_modes! {
    /// A `block_on` whose future panics leaves the runtime to be driven again, by this thread or
    /// another.
    fn a_panic_in_the_future_leaves_the_runtime_drivable<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async { panic!("a future that panics") });
        }));
        assert!(panicked.is_err());

        let task = M::spawn(&runtime, "the task after it", async { 7 });
        assert_eq!(runtime.block_on(task).unwrap(), 7);
    }
}

/// A local runtime runs futures that are not `Send`: a task can hold an `Rc` and borrow a
/// `RefCell` across its awaits.
#[test]
#[timeout(15000)]
fn a_local_task_can_hold_an_rc() {
    let runtime = LocalRuntime::new().unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));
    let task = {
        let log = log.clone();
        let sleeper = runtime.clone();
        runtime.spawn("a task holding an `Rc`", async move {
            log.borrow_mut().push("before");
            sleeper.sleep(Duration::from_millis(1)).await;
            log.borrow_mut().push("after");

            Rc::strong_count(&log)
        })
    };

    let count = runtime.block_on(task).unwrap();

    assert_eq!(count, 2);
    assert_eq!(*log.borrow(), ["before", "after"]);
}

/// A thread of its own wakes a local task through the task's waker, and breaks the wait the
/// thread inside `block_on` is in to do so.
///
/// A waker is `Send` whatever the task it wakes is, so a local task waiting on a thread's work is
/// woken by that thread as any other task is.
#[test]
#[timeout(15000)]
fn a_thread_wakes_a_local_task() {
    let runtime = LocalRuntime::new().unwrap();
    let event = Arc::new(Event::new());
    let listener = event.listen();
    let local = Rc::new(7);
    let task = runtime.spawn("a local task waiting on a thread", async move {
        listener.await;
        *local
    });
    let notifier = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        event.notify(1);
    });
    let started = Instant::now();

    assert_eq!(runtime.block_on(task).unwrap(), 7);

    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(started.elapsed() < Duration::from_secs(5));
    notifier.join().unwrap();
}

/// A task spawned from another thread onto a shared runtime breaks the wait of the thread inside
/// `block_on` on it, which then runs the task.
#[test]
#[timeout(15000)]
fn a_spawn_from_another_thread_reaches_the_driving_thread() {
    let runtime = SharedRuntime::new().unwrap();
    let ran = Arc::new(Event::new());
    // Taken before the thread starts, so that the announcement cannot be missed.
    let announced = ran.listen();
    let spawner = {
        let runtime = runtime.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            runtime
                .spawn("a task spawned from another thread", async move {
                    ran.notify(1);
                })
                .detach();
        })
    };

    runtime.block_on(announced);

    spawner.join().unwrap();
}

/// A shared runtime is driven by one thread at a time, so a second thread asking to drive it
/// while the first does is told so rather than left to race it.
#[test]
#[timeout(15000)]
fn a_second_concurrent_block_on_panics() {
    let runtime = SharedRuntime::new().unwrap();
    let inside = Arc::new(Event::new());
    let release = Arc::new(Event::new());
    // Both taken before the thread starts, so that neither notification can be missed.
    let announced = inside.listen();
    let released = release.listen();
    let driver = {
        let runtime = runtime.clone();
        thread::spawn(move || {
            runtime.block_on(async move {
                inside.notify(1);
                released.await;
            })
        })
    };
    futures_lite::future::block_on(announced);

    let panicked = catch_unwind(AssertUnwindSafe(|| runtime.block_on(async {}))).unwrap_err();

    let message = panicked
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panicked.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default();
    assert!(message.contains("another thread is driving"), "{message}");
    release.notify(1);
    driver.join().unwrap();
    // And once the first thread has left, this one may drive it.
    assert_eq!(runtime.block_on(async { 7 }), 7);
}

/// What a shared runtime's handles are shared as: a task handle and a registration live in state
/// that several threads reach, and a runtime handle is cloned into the tasks it spawns.
#[test]
fn shared_handles_cross_threads() {
    fn sent_and_shared<T>()
    where
        T: Send + Sync + Unpin,
    {
    }

    sent_and_shared::<SharedRuntime>();
    sent_and_shared::<Task<(), Shared>>();
    sent_and_shared::<Registration<Shared>>();
    sent_and_shared::<Sleep<Shared>>();
}

/// Spawning and registering, through whichever of the two flavours' own methods `Self` names.
///
/// The two take different bounds — a shared runtime's tasks and sources have to be `Send` — so
/// this takes the stricter of each, which the tests written for both flavours meet.
trait TestMode: Mode {
    /// Spawns `future` on `runtime`.
    fn spawn<T>(
        runtime: &Runtime<Self>,
        name: &'static str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T, Self>
    where
        T: Send + 'static;

    /// Registers `source` on `runtime`.
    fn register(runtime: &Runtime<Self>, source: TestSource) -> io::Result<Registration<Self>>;
}

impl TestMode for Local {
    fn spawn<T>(
        runtime: &LocalRuntime,
        name: &'static str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T, Local>
    where
        T: Send + 'static,
    {
        runtime.spawn(name, future)
    }

    fn register(runtime: &LocalRuntime, source: TestSource) -> io::Result<Registration<Local>> {
        runtime.register(source)
    }
}

impl TestMode for Shared {
    fn spawn<T>(
        runtime: &SharedRuntime,
        name: &'static str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T, Shared>
    where
        T: Send + 'static,
    {
        runtime.spawn(name, future)
    }

    fn register(runtime: &SharedRuntime, source: TestSource) -> io::Result<Registration<Shared>> {
        runtime.register(source)
    }
}

/// Reads one byte from `source` through `registration`, the way a caller reads its socket.
async fn read_one<M>(registration: &Registration<M>, source: &TestSource) -> io::Result<usize>
where
    M: Mode,
{
    poll_fn(|cx| {
        let mut byte = [MaybeUninit::<u8>::uninit(); 1];

        registration.poll_io(cx, Interest::Readable, || {
            SockRef::from(source).recv(&mut byte)
        })
    })
    .await
}

/// Sets the flag it holds when it is dropped.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Panics when it is dropped.
struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("dropped with a panic on purpose");
    }
}

/// A future that is ready at once with the value it holds, and panics when it is dropped.
struct ReadyThenPanicOnDrop(u32);

impl Future for ReadyThenPanicOnDrop {
    type Output = u32;

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<u32> {
        Poll::Ready(self.0)
    }
}

impl Drop for ReadyThenPanicOnDrop {
    fn drop(&mut self) {
        panic!("dropped with a panic on purpose");
    }
}

/// A source shareable between the registration a test makes and the reads or writes it makes
/// directly: an owned descriptor of its own, behind an `Arc` of the test's own.
#[cfg(unix)]
pub(super) type TestSource = Arc<std::os::fd::OwnedFd>;
#[cfg(windows)]
pub(super) type TestSource = Arc<std::os::windows::io::OwnedSocket>;

/// A connected pair: the source to register, and the far end to drive it from.
pub(super) fn pair() -> (TestSource, Socket) {
    let (near, far) = connected();
    near.set_nonblocking(true).unwrap();
    far.set_nonblocking(true).unwrap();

    (owned_source(near), far)
}

/// Wraps `socket` as a [`TestSource`]: one clone goes to the registration, the other is kept
/// for the reads and writes the tests make directly.
#[cfg(unix)]
fn owned_source(socket: Socket) -> TestSource {
    let owned: std::os::fd::OwnedFd = socket.into();

    Arc::new(owned)
}

/// Wraps `socket` as a [`TestSource`]: one clone goes to the registration, the other is kept
/// for the reads and writes the tests make directly.
#[cfg(windows)]
fn owned_source(socket: Socket) -> TestSource {
    let owned: std::os::windows::io::OwnedSocket = socket.into();

    Arc::new(owned)
}

/// Two sockets connected to one another.
#[cfg(unix)]
fn connected() -> (Socket, Socket) {
    Socket::pair(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap()
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
        // A loopback listener is reachable by anything else on the machine, so a connection that
        // is not the one made just above is turned away rather than taken for it.
        if accepted.peer_addr().unwrap() == far.local_addr().unwrap() {
            break accepted;
        }
    };

    // One-byte messages travel this pair, as they do the poller's wake pair, so neither end
    // holds a send back for the peer's acknowledgement of the one before it.
    near.set_nodelay(true).unwrap();
    far.set_nodelay(true).unwrap();

    (near.into(), far.into())
}
