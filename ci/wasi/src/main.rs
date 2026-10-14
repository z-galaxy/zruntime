//! Runs zruntime on WASI, where no thread can be started and nothing but the thread that waits on
//! the runtime can wake it.
//!
//! Each check runs in turn and prints its name, and the first one that fails panics, which traps
//! the module and fails the run. The sockets need preview 2 and a host that grants them: run the
//! `wasm32-wasip2` build with `wasmtime run -S inherit-network`. Preview 1 cannot create a socket,
//! and its build checks that the sockets say so.
//!
//! A panic aborts on WASI rather than unwinds, so what panics by design, such as a `block_on`
//! called from inside a task, is not checked here: the check would end the run.

use std::{
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use zruntime::{Event, LocalRuntime, SharedRuntime};

fn main() {
    run("block_on", block_on);
    run("spawn", spawn);
    run("sleep", sleep);
    run("timeout", timeout);
    run("event", event);
    run("shared", shared);
    run("free_block_on", free_block_on);
    run("spawn_outside_block_on", spawn_outside_block_on);
    run("work_left_by_block_on", work_left_by_block_on);
    run("nested_block_on", nested_block_on);
    #[cfg(target_env = "p2")]
    run("tcp_async_io", tcp_async_io);
    #[cfg(target_env = "p2")]
    run("tcp", tcp);
    #[cfg(target_env = "p2")]
    run("tcp_refused", tcp_refused);
    #[cfg(target_env = "p2")]
    run("udp", udp);
    #[cfg(target_env = "p1")]
    run("sockets_unsupported", sockets_unsupported);
}

fn run(name: &str, check: fn()) {
    println!("{name}...");
    check();
    println!("{name}: ok");
}

/// A future runs to its end on the calling thread.
fn block_on() {
    let runtime = LocalRuntime::new().unwrap();

    assert_eq!(runtime.block_on(async { 1 + 1 }), 2);
}

/// Tasks run, and hand back their output, while `block_on` waits for them.
fn spawn() {
    let runtime = LocalRuntime::new().unwrap();
    let sum = runtime.block_on(async {
        let tasks: Vec<_> = (0..10)
            .map(|i| runtime.spawn("adder", async move { i }))
            .collect();
        let mut sum = 0;
        for task in tasks {
            sum += task.await.unwrap();
        }

        sum
    });

    assert_eq!(sum, 45);
}

/// A timer ends the wait on the reactor, which watches no descriptor at all.
fn sleep() {
    let runtime = LocalRuntime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(runtime.sleep(Duration::from_millis(50)));

    assert!(start.elapsed() >= Duration::from_millis(50));
}

/// A future that never ends is given up on once its time is up.
fn timeout() {
    let runtime = LocalRuntime::new().unwrap();
    let result =
        runtime.block_on(runtime.timeout(Duration::from_millis(20), std::future::pending::<()>()));

    assert!(result.is_err());
}

/// A task waiting on an `Event` is woken by another task's notification.
fn event() {
    let runtime = LocalRuntime::new().unwrap();
    let event = Rc::new(Event::new());
    runtime.block_on(async {
        let listener = event.listen();
        let notifier = runtime.spawn("notifier", {
            let runtime = runtime.clone();
            let event = event.clone();
            async move {
                runtime.sleep(Duration::from_millis(10)).await;
                event.notify(1);
            }
        });
        listener.await;
        notifier.await.unwrap();
    });
}

/// A shared runtime runs `Send` tasks and timers like a local one.
fn shared() {
    let runtime = SharedRuntime::new().unwrap();
    let value = runtime.block_on(async {
        let task = runtime.spawn("sleeper", {
            let runtime = runtime.clone();
            async move {
                runtime.sleep(Duration::from_millis(10)).await;
                7
            }
        });

        task.await.unwrap()
    });

    assert_eq!(value, 7);
}

/// The free `block_on` runs the calling thread's own runtime, which `SharedRuntime::current`
/// hands out inside it: a task of the free `spawn` and a timer of that runtime run on this thread,
/// with no helper thread to start.
fn free_block_on() {
    let value = zruntime::block_on(async {
        let runtime = SharedRuntime::current().unwrap();
        let task = zruntime::spawn(async move {
            runtime.sleep(Duration::from_millis(10)).await;
            7
        });

        task.await.unwrap()
    });

    assert_eq!(value, 7);
}

/// A task of the free `spawn` from outside any `block_on` goes on the runtime the process shares.
/// With no helper thread to run it, it waits for a `block_on` on that runtime, and completes in
/// there. The handle held across the spawn is what keeps that runtime, and the task, alive.
fn spawn_outside_block_on() {
    let runtime = SharedRuntime::current().unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let task = zruntime::spawn({
        let ran = ran.clone();
        async move {
            ran.store(true, Ordering::Release);
            7
        }
    });
    assert!(!ran.load(Ordering::Acquire));

    assert_eq!(runtime.block_on(task).unwrap(), 7);
    assert!(ran.load(Ordering::Acquire));
}

/// A task left on the thread's own runtime as the free `block_on` returns waits, with no helper
/// thread to run it, for the next call, which runs it to its end. The handle kept out of the first
/// call is what keeps that runtime, and the task, alive in between.
fn work_left_by_block_on() {
    let ran = Arc::new(AtomicBool::new(false));
    let (runtime, task) = zruntime::block_on({
        let ran = ran.clone();
        async move {
            let runtime = SharedRuntime::current().unwrap();
            let task = zruntime::spawn(async move {
                ran.store(true, Ordering::Release);
                7
            });

            (runtime, task)
        }
    });
    assert!(!ran.load(Ordering::Acquire));

    assert_eq!(zruntime::block_on(task).unwrap(), 7);
    assert!(ran.load(Ordering::Acquire));
    drop(runtime);
}

/// A `block_on` nested inside the future of the free `block_on` runs, where the outer call holds
/// no seat: the thread has no runtime of its own alive as the outer call starts, and its future
/// brings none into being before the nested calls. The first nested call is the free `block_on`,
/// whose spawn brings the thread's own runtime into being, and which drives it; the second is a
/// `block_on` on the runtime `SharedRuntime::current` hands out, which is that thread's own too.
///
/// Every other nesting on one thread panics, as it does on every platform: one inside a call that
/// is in a seat or that drives a runtime made by `Runtime::new`, a `LocalRuntime` among them, and
/// one on such a runtime inside any call.
fn nested_block_on() {
    let (first, second) = zruntime::block_on(async {
        let first = zruntime::block_on(async { zruntime::spawn(async { 3 }).await.unwrap() });
        let second = SharedRuntime::current()
            .unwrap()
            .block_on(async { zruntime::spawn(async { 4 }).await.unwrap() });

        (first, second)
    });

    assert_eq!((first, second), (3, 4));
}

/// A TCP listener and stream, registered through `AsyncIo`, carry bytes both ways.
#[cfg(target_env = "p2")]
fn tcp_async_io() {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
    };

    use zruntime::AsyncIo;

    let runtime = LocalRuntime::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let listener = AsyncIo::new(&runtime, listener).unwrap();
    let client = TcpStream::connect(address).unwrap();
    let client = AsyncIo::new(&runtime, client).unwrap();

    runtime.block_on(async {
        let (server, _) = listener
            .read_with(|listener| listener.accept())
            .await
            .unwrap();
        let server = AsyncIo::new(&runtime, server).unwrap();

        let reader = runtime.spawn("server", async move {
            let mut buffer = [0; 4];
            let mut read = 0;
            while read < buffer.len() {
                read += server
                    .read_with(|mut stream| stream.read(&mut buffer[read..]))
                    .await
                    .unwrap();
            }
            server
                .write_with(|mut stream| stream.write(b"pong"))
                .await
                .unwrap();

            buffer
        });
        client
            .write_with(|mut stream| stream.write(b"ping"))
            .await
            .unwrap();
        assert_eq!(&reader.await.unwrap(), b"ping");
        let mut buffer = [0; 4];
        let mut read = 0;
        while read < buffer.len() {
            read += client
                .read_with(|mut stream| stream.read(&mut buffer[read..]))
                .await
                .unwrap();
        }
        assert_eq!(&buffer, b"pong");
    });
}

/// A listener and a stream of the `net` module carry bytes both ways, and the end of the stream
/// when one side closes it.
#[cfg(target_env = "p2")]
fn tcp() {
    use std::net::Ipv4Addr;

    use futures::{AsyncReadExt, AsyncWriteExt};
    use zruntime::net::{TcpListener, TcpStream};

    let runtime = LocalRuntime::new().unwrap();
    let listener = TcpListener::bind(&runtime, (Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();

    runtime.block_on(async {
        let server = runtime.spawn("server", async move {
            let (mut stream, peer) = listener.accept().await.unwrap();
            let mut buffer = [0; 4];
            stream.read_exact(&mut buffer).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            // The client closes its end after it has read the answer.
            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).await.unwrap();

            (buffer, peer, rest)
        });

        let mut client = TcpStream::connect(&runtime, address).await.unwrap();
        assert_eq!(client.peer_addr().unwrap(), address);
        client.write_all(b"ping").await.unwrap();
        let mut buffer = [0; 4];
        client.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"pong");
        client.close().await.unwrap();

        let (received, peer, rest) = server.await.unwrap();
        assert_eq!(&received, b"ping");
        assert_eq!(peer, client.local_addr().unwrap());
        assert!(rest.is_empty());
    });
}

/// A connect to a port nobody listens on fails with `ConnectionRefused`, once the runtime has
/// seen the connection settle.
#[cfg(target_env = "p2")]
fn tcp_refused() {
    use std::{io::ErrorKind, net::Ipv4Addr};

    use zruntime::net::TcpStream;

    let runtime = LocalRuntime::new().unwrap();
    // A port that was free a moment ago, and that nothing else is likely to take in between.
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let error = runtime
        .block_on(TcpStream::connect(&runtime, address))
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::ConnectionRefused);
}

/// A datagram goes from one UDP socket of the `net` module to another, with the address it came
/// from.
#[cfg(target_env = "p2")]
fn udp() {
    use std::net::Ipv4Addr;

    use zruntime::net::UdpSocket;

    let runtime = LocalRuntime::new().unwrap();
    let sender = UdpSocket::bind(&runtime, (Ipv4Addr::LOCALHOST, 0)).unwrap();
    let receiver = UdpSocket::bind(&runtime, (Ipv4Addr::LOCALHOST, 0)).unwrap();

    runtime.block_on(async {
        let sent = sender
            .send_to(b"hello", receiver.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(sent, 5);

        let mut buffer = [0; 16];
        let (len, from) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..len], b"hello");
        assert_eq!(from, sender.local_addr().unwrap());
    });
}

/// Preview 1 cannot create a socket: the constructors that would say so with an `Unsupported`
/// error, rather than trap.
#[cfg(target_env = "p1")]
fn sockets_unsupported() {
    use std::{io::ErrorKind, net::Ipv4Addr};

    use zruntime::net::{TcpListener, TcpStream, UdpSocket};

    let runtime = LocalRuntime::new().unwrap();
    let address = (Ipv4Addr::LOCALHOST, 0);

    let error = runtime
        .block_on(TcpStream::connect(&runtime, address))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let error = TcpListener::bind(&runtime, address).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let error = UdpSocket::bind(&runtime, address).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}
