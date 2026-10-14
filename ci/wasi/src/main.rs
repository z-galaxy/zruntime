//! Runs zruntime on WASI, where no thread can be started and nothing but the thread that waits on
//! the runtime can wake it.
//!
//! Each check runs in turn and prints its name, and the first one that fails panics, which traps
//! the module and fails the run. The sockets need preview 2 and a host that grants them: run the
//! `wasm32-wasip2` build with `wasmtime run -S inherit-network`. Preview 1 cannot create a socket,
//! and its build checks that the sockets say so.
//!
//! The filesystem checks, on both previews, need a directory that the host lends the program as
//! `/data`: pass `--dir <host directory>::/data` to `wasmtime run`. Each works in a directory of
//! its own under `/data/zruntime-wasi-check`, and removes it at the end, and the last check removes
//! that directory too, so the host directory can be a new, empty one. `set_permissions` is not
//! checked, as WASI has no permissions to set, and neither is `read_link`: the only function of std
//! that makes a symbolic link on WASI is deprecated.
//!
//! A panic aborts on WASI rather than unwinds, so what panics by design, such as a `block_on`
//! called from inside a task, is not checked here: the check would end the run.

use std::{
    cell::Cell,
    io::{ErrorKind, SeekFrom},
    ops::Deref,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, StreamExt};
use zruntime::{
    Event, LocalRuntime, SharedRuntime,
    fs::{self, DirBuilder, File, OpenOptions},
};

/// The directory of this program in the one that the host lends as `/data`.
const ROOT: &str = "/data/zruntime-wasi-check";

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
    run("fs_read_write", fs_read_write);
    run("fs_directories", fs_directories);
    run("fs_move_copy_link", fs_move_copy_link);
    run("fs_remove_dir_all", fs_remove_dir_all);
    run("fs_open_options", fs_open_options);
    run("fs_file", fs_file);
    run("fs_file_descriptor", fs_file_descriptor);
    run("fs_tasks", fs_tasks);
    run("fs_budget", fs_budget);
    run("fs_cleaned_up", fs_cleaned_up);
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

/// A directory of a check's own under [`ROOT`], which is empty when it is made and is removed with
/// all it holds when it is dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = Path::new(ROOT).join(name);
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => panic!("cannot clear {}: {error}", path.display()),
        }
        std::fs::create_dir_all(&path).unwrap_or_else(|error| {
            panic!(
                "cannot make {}: {error}; lend a directory with \
                 `wasmtime run --dir <host directory>::/data`",
                path.display(),
            )
        });

        Self(path)
    }
}

impl Deref for Scratch {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

/// Runs a future to its end on a runtime of its own, which is all the `fs` module needs of one.
fn drive<F: Future>(future: F) -> F::Output {
    LocalRuntime::new().unwrap().block_on(future)
}

/// Whole files are written and read back, and the metadata of a path tells what is there.
fn fs_read_write() {
    let dir = Scratch::new("read_write");
    let path = dir.join("greeting.txt");

    drive(async {
        fs::write(&path, "hello, world").await.unwrap();
        assert_eq!(fs::read(&path).await.unwrap(), b"hello, world");
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "hello, world");
        // A second write replaces the first.
        fs::write(&path, b"bye").await.unwrap();
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "bye");

        let metadata = fs::metadata(&path).await.unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 3);
        let metadata = fs::symlink_metadata(&path).await.unwrap();
        assert!(metadata.is_file());
        assert!(!metadata.file_type().is_symlink());
        assert!(fs::metadata(&*dir).await.unwrap().is_dir());

        let missing = dir.join("missing");
        let error = fs::read(&missing).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        let error = fs::metadata(&missing).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    });
}

/// Directories are made, listed and removed, and every entry of a listing tells what it is.
fn fs_directories() {
    let dir = Scratch::new("directories");

    drive(async {
        fs::create_dir(dir.join("one")).await.unwrap();
        let error = fs::create_dir(dir.join("one")).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        let error = fs::create_dir(dir.join("missing").join("below"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        fs::create_dir_all(dir.join("two").join("deep").join("deeper"))
            .await
            .unwrap();
        // The second one finds its directory made already, which is no error here.
        fs::create_dir_all(dir.join("two").join("deep"))
            .await
            .unwrap();
        DirBuilder::new()
            .recursive(true)
            .create(dir.join("three").join("nested"))
            .await
            .unwrap();
        fs::write(dir.join("file.txt"), "12345").await.unwrap();

        let mut entries = fs::read_dir(&*dir).await.unwrap();
        let mut found = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            assert_eq!(entry.path(), dir.join(&name));
            let file_type = entry.file_type().await.unwrap();
            let metadata = entry.metadata().await.unwrap();
            assert_eq!(file_type.is_dir(), metadata.is_dir());
            assert_eq!(file_type.is_file(), metadata.is_file());
            if file_type.is_file() {
                assert_eq!(metadata.len(), 5);
            }
            found.push((name, file_type.is_dir()));
        }
        found.sort();
        let expected = [
            ("file.txt", false),
            ("one", true),
            ("three", true),
            ("two", true),
        ];
        assert_eq!(
            found,
            expected.map(|(name, is_dir)| (name.to_owned(), is_dir)),
        );

        // A directory with nothing in it lists nothing.
        let mut entries = fs::read_dir(dir.join("one")).await.unwrap();
        assert!(entries.next().await.is_none());
        let error = fs::read_dir(dir.join("missing")).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::NotFound);

        fs::remove_dir(dir.join("one")).await.unwrap();
        let error = fs::metadata(dir.join("one")).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        // One with something in it stays.
        assert!(fs::remove_dir(dir.join("two")).await.is_err());
        assert!(fs::metadata(dir.join("two")).await.unwrap().is_dir());
    });
}

/// Files are renamed, copied, hard-linked, canonicalized and removed.
fn fs_move_copy_link() {
    let dir = Scratch::new("move_copy_link");
    let (original, renamed) = (dir.join("original"), dir.join("renamed"));
    let (copied, linked) = (dir.join("copied"), dir.join("linked"));

    drive(async {
        fs::write(&original, "contents").await.unwrap();
        fs::rename(&original, &renamed).await.unwrap();
        let error = fs::metadata(&original).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(fs::read_to_string(&renamed).await.unwrap(), "contents");

        assert_eq!(fs::copy(&renamed, &copied).await.unwrap(), 8);
        assert_eq!(fs::read_to_string(&copied).await.unwrap(), "contents");
        fs::hard_link(&renamed, &linked).await.unwrap();
        // The two names are one file, so a write through one shows through the other. The copy is
        // a file of its own.
        fs::write(&linked, "changed").await.unwrap();
        assert_eq!(fs::read_to_string(&renamed).await.unwrap(), "changed");
        assert_eq!(fs::read_to_string(&copied).await.unwrap(), "contents");

        let roundabout = dir.join("..").join(dir.file_name().unwrap()).join("copied");
        assert_eq!(fs::canonicalize(roundabout).await.unwrap(), copied);

        fs::remove_file(&renamed).await.unwrap();
        let error = fs::metadata(&renamed).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        // The other name still reaches the file.
        assert_eq!(fs::read_to_string(&linked).await.unwrap(), "changed");
        fs::remove_file(&linked).await.unwrap();
        fs::remove_file(&copied).await.unwrap();
        let error = fs::remove_file(&copied).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    });
}

/// A directory with files and directories in it, several levels down, is removed in one call.
fn fs_remove_dir_all() {
    let dir = Scratch::new("remove_dir_all");
    let tree = dir.join("tree");

    drive(async {
        fs::create_dir_all(tree.join("a").join("b")).await.unwrap();
        fs::create_dir_all(tree.join("c")).await.unwrap();
        fs::write(tree.join("top.txt"), "top").await.unwrap();
        fs::write(tree.join("a").join("middle.txt"), "middle")
            .await
            .unwrap();
        fs::write(tree.join("a").join("b").join("bottom.txt"), "bottom")
            .await
            .unwrap();

        fs::remove_dir_all(&tree).await.unwrap();

        let error = fs::metadata(&tree).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        let error = fs::remove_dir_all(&tree).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    });
}

/// `OpenOptions` appends, truncates, creates and refuses to create a file that is there.
fn fs_open_options() {
    let dir = Scratch::new("open_options");
    let path = dir.join("log.txt");

    drive(async {
        let error = OpenOptions::new().read(true).open(&path).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"ab").await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        let error = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);

        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        file.write_all(b"cd").await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "abcd");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"e").await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "e");

        let other = dir.join("made.txt");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&other)
            .await
            .unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 0);
    });
}

/// A file is written, read, seeked, resized, synced and measured through the `futures` I/O traits
/// and its own methods.
fn fs_file() {
    let dir = Scratch::new("file");
    let path = dir.join("notes.txt");

    drive(async {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .unwrap();

        file.write_all(b"hello, world").await.unwrap();
        file.flush().await.unwrap();
        // A write goes straight to the file, so its length counts the bytes written.
        assert_eq!(file.metadata().await.unwrap().len(), 12);
        assert_eq!(file.seek(SeekFrom::Current(0)).await.unwrap(), 12);

        assert_eq!(file.seek(SeekFrom::Start(7)).await.unwrap(), 7);
        let mut word = [0; 5];
        file.read_exact(&mut word).await.unwrap();
        assert_eq!(&word, b"world");
        // Nothing is left to read after that.
        assert_eq!(file.read(&mut word).await.unwrap(), 0);

        file.seek(SeekFrom::Start(0)).await.unwrap();
        file.write_all(b"HELLO").await.unwrap();
        file.sync_data().await.unwrap();
        file.sync_all().await.unwrap();
        file.set_len(5).await.unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 5);
        assert_eq!(file.seek(SeekFrom::End(-2)).await.unwrap(), 3);
        let mut tail = String::new();
        file.read_to_string(&mut tail).await.unwrap();
        assert_eq!(tail, "LO");

        // The file grows again, with zeros in the gap.
        file.set_len(7).await.unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut all = Vec::new();
        file.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"HELLO\0\0");
        file.close().await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        let mut start = [0; 5];
        file.read_exact(&mut start).await.unwrap();
        assert_eq!(&start, b"HELLO");
        // A file opened for reading refuses a write.
        assert!(file.write(b"x").await.is_err());
        drop(file);

        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"new").await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "new");
        let error = File::open(dir.join("missing")).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    });
}

/// A file lends its descriptor, and takes one in, as it does on unix.
fn fs_file_descriptor() {
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};

    let dir = Scratch::new("file_descriptor");
    let path = dir.join("data");
    std::fs::write(&path, "descriptor").unwrap();

    drive(async {
        let file = File::from(std::fs::File::open(&path).unwrap());
        // The two ways to borrow the descriptor give the one descriptor.
        assert_eq!(file.as_fd().as_raw_fd(), file.as_raw_fd());

        let owned: OwnedFd = std::fs::File::open(&path).unwrap().into();
        let mut file = File::from(owned);
        let mut text = String::new();
        file.read_to_string(&mut text).await.unwrap();
        assert_eq!(text, "descriptor");
    });
}

/// A task writes a file and reads it back, on a local runtime and on a shared one.
fn fs_tasks() {
    let dir = Scratch::new("tasks");

    let local = LocalRuntime::new().unwrap();
    let text = local
        .block_on(local.spawn("local file", write_and_read(dir.join("local"))))
        .unwrap();
    assert_eq!(text, "written by a task");

    let shared = SharedRuntime::new().unwrap();
    let text = shared
        .block_on(shared.spawn("shared file", write_and_read(dir.join("shared"))))
        .unwrap();
    assert_eq!(text, "written by a task");

    // Both files are there for anyone to see.
    drive(async {
        assert_eq!(fs::read_to_string(dir.join("local")).await.unwrap(), text);
        assert_eq!(fs::read_to_string(dir.join("shared")).await.unwrap(), text);
    });
}

/// Writes a file, then reads it back through the same handle. The future is `Send`, so that a
/// shared runtime can run it.
async fn write_and_read(path: PathBuf) -> String {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .await
        .unwrap();
    file.write_all(b"written by a task").await.unwrap();
    file.flush().await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
    let mut text = String::new();
    file.read_to_string(&mut text).await.unwrap();

    text
}

/// A task that makes more `fs` calls than the budget of a poll allows gives the task spawned after
/// it its turn part of the way, though the runtime has one thread to run both on.
fn fs_budget() {
    // More than the 128 operations that a poll may complete.
    const CALLS: usize = 300;

    let dir = Scratch::new("budget");
    let path = dir.join("file");
    std::fs::write(&path, "").unwrap();
    let runtime = LocalRuntime::new().unwrap();
    let (done, seen) = (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));
    let caller = runtime.spawn("caller", {
        let done = done.clone();
        async move {
            for _ in 0..CALLS {
                fs::metadata(&path).await.unwrap();
                done.set(done.get() + 1);
            }
        }
    });
    // Spawned right after the caller, so that it is next in the queue.
    let looker = runtime.spawn("looker", {
        let (done, seen) = (done.clone(), seen.clone());
        async move { seen.set(done.get()) }
    });

    runtime.block_on(async {
        caller.await.unwrap();
        looker.await.unwrap();
    });

    assert!(
        (1..=128).contains(&seen.get()),
        "the other task saw {} calls done",
        seen.get(),
    );
    assert_eq!(done.get(), CALLS);
}

/// Every check removed its directory, so what is left of the program's own is empty, and goes.
fn fs_cleaned_up() {
    assert!(std::fs::read_dir(ROOT).unwrap().next().is_none());
    std::fs::remove_dir(ROOT).unwrap();
}
