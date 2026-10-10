//! Unix-domain sockets, on unix platforms only: [`UnixListener`], [`UnixStream`] and
//! [`UnixDatagram`], and the [`Incoming`] stream of the connections a listener accepts.
//!
//! A unix-domain socket is named by a path in the file system. Binding a listener or a datagram
//! socket to a path creates a socket file there, and a stream connects to a listener, or a
//! datagram socket sends to another, by the path it is bound to. Nothing removes the file once the
//! socket that made it is gone, and binding to a path that has a file already fails with
//! [`AddrInUse`](std::io::ErrorKind::AddrInUse), so whoever owns a path removes its file when done
//! with it. The system limits how long a path may be, to about a hundred bytes, and a longer one
//! fails with [`InvalidInput`](std::io::ErrorKind::InvalidInput). On Linux and Android a socket
//! can instead be named in the abstract namespace, by a string of bytes that has no file behind it,
//! which std's `SocketAddrExt` makes and reads: [`UnixStream::connect_addr`] connects to one.
//!
//! The sockets run on a runtime, as the sockets of the [parent module](super) do, and what its
//! documentation says of those holds for these: which threads drive a socket's operations, how a
//! socket's type carries the flavour of its runtime, and how many tasks may wait on one socket at
//! once.

#[cfg(target_os = "android")]
use std::os::android::net::SocketAddrExt;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::{
    fmt,
    io::{self, Read},
    net::Shutdown,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
        unix::{ffi::OsStrExt, net::SocketAddr},
    },
    path::Path,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};
use socket2::SockAddr;

use super::{connect, set_nosigpipe};
use crate::{AsyncIo, Local, Mode, Runtime};

/// A unix-domain socket server, listening for connections.
///
/// A listener is bound to a path with [`UnixListener::bind`], and hands out the connections made
/// to that path, each as a [`UnixStream`], through [`accept`](UnixListener::accept), or as a
/// stream of them through [`incoming`](UnixListener::incoming). It is the async counterpart of
/// [`std::os::unix::net::UnixListener`]: waiting for a connection leaves the thread to the other
/// tasks instead of blocking it.
///
/// The listener runs on the runtime given to the constructor that made it, whose reactor watches
/// it from then on, and so do the streams it accepts. Its operations make progress while some
/// thread is inside [`Runtime::block_on`] on that runtime, or, on a runtime from
/// `SharedRuntime::current`, while the helper thread runs it. The listener's type carries the
/// flavour of that runtime: one built on a [`LocalRuntime`](crate::LocalRuntime) is a
/// `UnixListener<Local>`, [`Local`] being the default, and stays on the thread it was made on; one
/// built on a [`SharedRuntime`](crate::SharedRuntime) is a `UnixListener<Shared>`, which may be
/// sent to, and used from, any thread.
///
/// Any number of tasks may wait for a connection through [`accept`](UnixListener::accept) at once,
/// each through a reference to the listener, and each connection goes to one of them. Waiting for
/// the next item of an [`Incoming`] stream is not like that: at most one task at a time may do it,
/// counting every stream of the listener, and the tasks in `accept` do not count against it.
///
/// # Example
///
/// A listener that accepts a connection and reads a greeting from it, and a client that makes the
/// connection and sends the greeting. The socket file is in a directory of its own, which is
/// removed at the end:
///
/// ```
/// use futures::{AsyncReadExt, AsyncWriteExt};
/// use zruntime::{
///     LocalRuntime,
///     net::unix::{UnixListener, UnixStream},
/// };
///
/// let directory = std::env::temp_dir()
///     .join(format!("zruntime-unix-listener-doc-{}", std::process::id()));
/// std::fs::create_dir(&directory)?;
/// let path = directory.join("socket");
///
/// let runtime = LocalRuntime::new()?;
/// let listener = UnixListener::bind(&runtime, &path)?;
///
/// runtime.block_on(async {
///     let mut client = UnixStream::connect(&runtime, &path).await?;
///     client.write_all(b"hello").await?;
///
///     let (mut server, _) = listener.accept().await?;
///     let mut greeting = [0; 5];
///     server.read_exact(&mut greeting).await?;
///
///     assert_eq!(&greeting, b"hello");
///     assert_eq!(server.local_addr()?.as_pathname(), Some(path.as_path()));
///     # Ok::<_, std::io::Error>(())
/// })?;
///
/// std::fs::remove_dir_all(&directory)?;
/// # Ok::<_, std::io::Error>(())
/// ```
pub struct UnixListener<M = Local>
where
    M: Mode,
{
    io: AsyncIo<std::os::unix::net::UnixListener, M>,
}

impl<M> UnixListener<M>
where
    M: Mode,
{
    /// A listener bound to `path`, on `runtime`.
    ///
    /// Binding creates a socket file at `path`, which must have no file already: where one is
    /// there, a stale socket file left by an earlier listener included, this fails with
    /// [`AddrInUse`](io::ErrorKind::AddrInUse), and removing that file first is the caller's to do.
    /// The file the listener makes stays where it is when the listener is dropped, so whoever binds
    /// a path removes the file when done with it. A path too long to name a socket, which is about
    /// a hundred bytes, fails with [`InvalidInput`](io::ErrorKind::InvalidInput).
    ///
    /// The listener is bound and listening once this returns. What can fail is that, or what
    /// [`from_std`](UnixListener::from_std) can fail at.
    pub fn bind<P>(runtime: &Runtime<M>, path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        Self::from_std(runtime, std::os::unix::net::UnixListener::bind(path)?)
    }

    /// A listener on `runtime` that accepts the connections `listener` would.
    ///
    /// `listener` is switched to non-blocking mode, which every listener of this type is in. The
    /// mode belongs to the open socket rather than to a handle on it, so a duplicate of `listener`
    /// made with [`try_clone`](std::os::unix::net::UnixListener::try_clone) is switched with it,
    /// and an `accept` on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited. On Apple's platforms the socket also gets `SO_NOSIGPIPE`, which the
    /// streams it accepts have as well, so that a write to a peer that has gone fails rather than
    /// raising `SIGPIPE`.
    ///
    /// What can fail is the switch to non-blocking mode, setting that option, and the runtime
    /// taking the socket under its watch.
    pub fn from_std(
        runtime: &Runtime<M>,
        listener: std::os::unix::net::UnixListener,
    ) -> io::Result<Self> {
        listener.set_nonblocking(true)?;
        set_nosigpipe(&listener)?;

        Ok(Self {
            io: AsyncIo::from_nonblocking(runtime, listener)?,
        })
    }

    /// Waits for a connection to this listener, and accepts it.
    ///
    /// Resolves to the stream of the connection, which runs on the listener's runtime and is in
    /// non-blocking mode, and the address of the peer that made the connection, which is unnamed
    /// unless the peer bound its socket to a path before it connected.
    ///
    /// Dropping the future before it completes gives up the wait. No connection is lost with it:
    /// one that comes in meanwhile stays queued for the next call.
    ///
    /// An accept that fails resolves to the error. Where the error leaves the connection queued,
    /// as the system running out of file descriptors (`EMFILE`) does on Linux and the BSDs, the
    /// next call fails again at once, and so does each one after it for as long as the connection
    /// stays queued: a loop that goes on accepting straight after an error spins the thread. On
    /// Apple's platforms an accept that finds no descriptor left closes the connection it took off
    /// the queue instead, so the error does not repeat, but the connection is lost. A caller that
    /// goes on after an error backs off first, with [`Runtime::sleep`], say.
    pub async fn accept(&self) -> io::Result<(UnixStream<M>, SocketAddr)> {
        let (stream, address) = self.io.read_with(|listener| listener.accept()).await?;

        self.accepted(stream, address)
    }

    /// A stream of the connections made to this listener.
    ///
    /// Each item is the [`UnixStream`] of a connection, accepted as
    /// [`accept`](UnixListener::accept) accepts it, or the error of an accept that failed. The
    /// stream never ends, and an error is not its end either. Nor does an error leave it pending
    /// until the next connection comes in: polled again, it accepts again. Where the error left
    /// the connection queued, as the system running out of file descriptors (`EMFILE`) does on
    /// Linux and the BSDs, the next item is that error again at once, and so is each one after it
    /// for as long as the connection stays queued, so a loop that goes on taking items straight
    /// after an error spins the thread. On Apple's platforms an accept that finds no descriptor
    /// left closes the connection it took off the queue instead, so the error does not repeat,
    /// but the connection is lost. A caller that goes on after an error backs off first, with
    /// [`Runtime::sleep`], say.
    ///
    /// At most one task at a time may wait for the next item, counting every stream this listener
    /// hands out, and a task waiting in [`accept`](UnixListener::accept) does not count against
    /// it, as [the stream's documentation](Incoming) says.
    pub fn incoming(&self) -> Incoming<'_, M> {
        Incoming { listener: self }
    }

    /// The local socket address this listener is bound to.
    ///
    /// It is the path the listener was bound to, which [`SocketAddr::as_pathname`] gives.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }
}

impl<M> fmt::Debug for UnixListener<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

impl<M> AsFd for UnixListener<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

impl<M> AsRawFd for UnixListener<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

/// A unix-domain connection, to read from and write to.
///
/// A stream is made by connecting to the path a [`UnixListener`] is bound to with
/// [`UnixStream::connect`], or to its address with [`UnixStream::connect_addr`], by a listener
/// accepting a connection, or as one of a connected pair with [`UnixStream::pair`]. It is the async
/// counterpart of [`std::os::unix::net::UnixStream`]: a read or a write that has to wait leaves the
/// thread to the other tasks instead of blocking it.
///
/// The stream implements the `AsyncRead` and `AsyncWrite` traits of [`futures-io`], so the
/// extension traits of [`futures`] read from and write to it. So does a shared reference to it,
/// `&UnixStream`, which lets a reader and a writer share one stream, as std's `Read` and `Write` do
/// for a `&std::os::unix::net::UnixStream`. A write sends as much as the kernel takes at once,
/// which may be less than the whole buffer, and nothing is held back, so flushing has nothing to
/// do. A write to a peer that has gone away fails with [`BrokenPipe`](io::ErrorKind::BrokenPipe).
///
/// Closing the stream, as the `close` of `AsyncWriteExt` does, shuts down the write half of the
/// socket, as [`shutdown`](UnixStream::shutdown) with [`Shutdown::Write`] does: the peer reads the
/// end of the stream once it has read what was sent, while the read half stays open. It is the
/// socket that is shut down, so closing the stream through one `&UnixStream` ends it for every
/// handle on it, and closing a stream that is closed already is fine.
///
/// The stream runs on the runtime given to the constructor that made it, whose reactor watches it
/// from then on; a stream a [`UnixListener`] accepted is on the listener's runtime. Its operations
/// make progress while some thread is inside [`Runtime::block_on`] on that runtime, or, on a
/// runtime from `SharedRuntime::current`, while the helper thread runs it. The stream's type
/// carries the flavour of that runtime: one built on a
/// [`LocalRuntime`](crate::LocalRuntime) is a `UnixStream<Local>`, [`Local`] being the default,
/// and stays on the thread it was made on; one built on a [`SharedRuntime`](crate::SharedRuntime)
/// is a `UnixStream<Shared>`, which may be sent to, and used from, any thread.
///
/// At most one task at a time may wait to read from a stream through its `AsyncRead`
/// implementation, and at most one to write to it through its `AsyncWrite` one, counting every
/// reference to the stream: where a second task waits in the same direction, the one that waited
/// first may never be woken. Tasks that read, or write, together take turns, behind a lock of their
/// own.
///
/// # Example
///
/// A pair of connected streams: one sends a message and closes its end of the stream, the other
/// reads the message up to the end of the stream, and the answer that still gets from the other to
/// the one:
///
/// ```
/// use futures::{AsyncReadExt, AsyncWriteExt};
/// use zruntime::{LocalRuntime, net::unix::UnixStream};
///
/// let runtime = LocalRuntime::new()?;
/// let (mut client, mut server) = UnixStream::pair(&runtime)?;
///
/// runtime.block_on(async {
///     client.write_all(b"ping").await?;
///     // Closing shuts the client's write half down: the server reads the end of the stream after
///     // the bytes the client sent.
///     client.close().await?;
///
///     let mut message = Vec::new();
///     server.read_to_end(&mut message).await?;
///     assert_eq!(message, b"ping");
///
///     // The other direction is open still.
///     server.write_all(b"pong").await?;
///     let mut answer = [0; 4];
///     client.read_exact(&mut answer).await?;
///     assert_eq!(&answer, b"pong");
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok::<_, std::io::Error>(())
/// ```
///
/// [`futures-io`]: https://docs.rs/futures-io
/// [`futures`]: https://docs.rs/futures
pub struct UnixStream<M = Local>
where
    M: Mode,
{
    io: AsyncIo<std::os::unix::net::UnixStream, M>,
}

impl<M> UnixStream<M>
where
    M: Mode,
{
    /// A stream connected to the socket at `path`, on `runtime`.
    ///
    /// `path` is the path a [`UnixListener`] is bound to. The connection does not block the
    /// thread. It fails with [`NotFound`](io::ErrorKind::NotFound) where there is no file at
    /// `path`, and with [`ConnectionRefused`](io::ErrorKind::ConnectionRefused) where the file is
    /// that of a socket nothing listens at any more. A path too long to name a socket, which is
    /// about a hundred bytes, fails with [`InvalidInput`](io::ErrorKind::InvalidInput), as does
    /// one with a zero byte in it, which std refuses wherever it takes a path for a socket.
    ///
    /// On Linux and Android, a listener whose backlog is full has no room for another connection,
    /// and a connect waits for room to open up, without blocking the thread: it tries again every
    /// 20 milliseconds for as long as the caller awaits it. Nothing but the caller bounds that
    /// wait, with a timeout of its own or by dropping the future, which gives up the attempt.
    /// Other platforms refuse a connection to a listener whose backlog is full, which fails with
    /// [`ConnectionRefused`](io::ErrorKind::ConnectionRefused) at once.
    ///
    /// To connect to a socket by its address as std gives it rather than by a path, which on
    /// Linux and Android is the only way to reach one in the abstract namespace, see
    /// [`connect_addr`](UnixStream::connect_addr).
    pub async fn connect<P>(runtime: &Runtime<M>, path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        Self::connect_sockaddr(runtime, &pathname_sockaddr(path.as_ref())?).await
    }

    /// A stream connected to the socket at `address`, on `runtime`.
    ///
    /// `address` is a socket address as std gives it, such as the
    /// [`local_addr`](UnixListener::local_addr) of a listener. It names a socket by the path it is
    /// bound to, which [`connect`](UnixStream::connect) takes as it is, or, on Linux and Android,
    /// by a name in the abstract namespace. Such a socket has no file behind it, and its name is a
    /// string of bytes, any of them a zero byte: std's [`SocketAddrExt`] makes an address of a
    /// name, and tells the name an address holds. An unnamed address, which is that of a socket
    /// bound to nothing, such as either end of a [pair](UnixStream::pair), names no socket to
    /// connect to, and this fails with [`InvalidInput`](io::ErrorKind::InvalidInput).
    ///
    /// The connection does not block the thread. For a path it fails as
    /// [`connect`](UnixStream::connect) does: with [`NotFound`](io::ErrorKind::NotFound) where
    /// there is no file at it, and with [`ConnectionRefused`](io::ErrorKind::ConnectionRefused)
    /// where the file is that of a socket nothing listens at any more. A name in the abstract
    /// namespace has no file to be missing, so a name nothing listens at fails with
    /// [`ConnectionRefused`](io::ErrorKind::ConnectionRefused).
    ///
    /// On Linux and Android, a listener whose backlog is full has no room for another connection,
    /// and a connect waits for room to open up, without blocking the thread: it tries again every
    /// 20 milliseconds for as long as the caller awaits it. Nothing but the caller bounds that
    /// wait, with a timeout of its own or by dropping the future, which gives up the attempt.
    /// Other platforms refuse a connection to a listener whose backlog is full, which fails with
    /// [`ConnectionRefused`](io::ErrorKind::ConnectionRefused) at once.
    ///
    /// [`SocketAddrExt`]: https://doc.rust-lang.org/std/os/linux/net/trait.SocketAddrExt.html
    pub async fn connect_addr(runtime: &Runtime<M>, address: &SocketAddr) -> io::Result<Self> {
        Self::connect_sockaddr(runtime, &named_sockaddr(address)?).await
    }

    /// A connected pair of streams, on `runtime`.
    ///
    /// What is written to one of them is read from the other, and the other way round, as for the
    /// two ends of a connection made through a listener, but with no path to name them by. Both are
    /// in non-blocking mode, as every stream of this type is.
    ///
    /// What can fail is making the pair, and the runtime taking its sockets under its watch.
    pub fn pair(runtime: &Runtime<M>) -> io::Result<(Self, Self)> {
        let (first, second) = std::os::unix::net::UnixStream::pair()?;

        Ok((
            Self::from_std(runtime, first)?,
            Self::from_std(runtime, second)?,
        ))
    }

    /// A stream on `runtime` that reads and writes the connection `stream` does.
    ///
    /// `stream` is switched to non-blocking mode, which every stream of this type is in. The mode
    /// belongs to the open socket rather than to a handle on it, so a duplicate of `stream` made
    /// with [`try_clone`](std::os::unix::net::UnixStream::try_clone) is switched with it, and a
    /// read or a write on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited. On Apple's platforms the socket also gets `SO_NOSIGPIPE`, so that a
    /// write to a peer that has gone fails rather than raising `SIGPIPE`.
    ///
    /// What can fail is the switch to non-blocking mode, setting that option, and the runtime
    /// taking the socket under its watch.
    pub fn from_std(
        runtime: &Runtime<M>,
        stream: std::os::unix::net::UnixStream,
    ) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        set_nosigpipe(&stream)?;

        Ok(Self {
            io: AsyncIo::from_nonblocking(runtime, stream)?,
        })
    }

    /// The local socket address of the connection: this end of it.
    ///
    /// It names the path this end is bound to, if it is bound to one: the stream a listener
    /// accepted is on the listener's path, while one that connected to a path, or is one of a
    /// pair, is on none, and its address is unnamed.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The socket address of the peer the stream is connected to: the other end of the connection.
    ///
    /// It names the path the peer is bound to, if it is bound to one, as
    /// [`local_addr`](UnixStream::local_addr) does for this end.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Stops watching the stream, and hands the socket back as a std stream, still in non-blocking
    /// mode.
    ///
    /// The runtime lets go of the socket, and the connection is left as it was: the std stream
    /// reads the bytes that arrived and were not read yet. The socket stays in non-blocking mode,
    /// as every stream of this type is, so a read or a write on the std stream fails with
    /// [`WouldBlock`](io::ErrorKind::WouldBlock) where it would have waited, until
    /// [`set_nonblocking`](std::os::unix::net::UnixStream::set_nonblocking) switches the mode
    /// back. Handing the socket to [`from_std`](UnixStream::from_std) takes it up again, on this
    /// runtime or on another.
    ///
    /// On a shared runtime this may wait for a moment, while a thread driving the runtime returns
    /// from a wait that watched the socket.
    pub fn into_std(self) -> std::os::unix::net::UnixStream {
        self.io.into_inner()
    }

    /// Shuts down the read half, the write half or both halves of the connection, as `how` says.
    ///
    /// Shutting down the write half makes the peer read the end of the stream, once it has read
    /// what was sent, which is what closing the stream does. It is the socket that is shut
    /// down, so it holds for every handle on the stream.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.io.get_ref().shutdown(how)
    }
}

impl<M> AsyncRead for UnixStream<M>
where
    M: Mode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_read(cx, buf)
    }
}

impl<M> AsyncWrite for UnixStream<M>
where
    M: Mode,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self).poll_close(cx)
    }
}

impl<M> AsyncRead for &UnixStream<M>
where
    M: Mode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.io.poll_read_with(cx, |mut stream| stream.read(buf))
    }
}

// A write is a `send`, not std's `Write for &UnixStream`, which sends with `MSG_NOSIGNAL` only
// from a release later than this crate's MSRV: a write to a peer that has gone must be an error
// and never a `SIGPIPE`. The vectored methods are left to the traits' defaults, which write the
// first buffer that is not empty, so that they send as this does: std's vectored write sends
// without the flag.
impl<M> AsyncWrite for &UnixStream<M>
where
    M: Mode,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io
            .poll_write_with(cx, |stream| Ok(rustix::net::send(stream, buf, SEND_FLAGS)?))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Nothing is buffered here: a write that reports success has gone to the kernel.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.io.get_ref().shutdown(Shutdown::Write) {
            // A stream whose connection is gone has nothing left to close, and some BSDs report
            // `NotConnected` for a second shutdown: either way the stream is closed already, and
            // closing it again is not an error, on any platform.
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Poll::Ready(Ok(())),
            result => Poll::Ready(result),
        }
    }
}

impl<M> fmt::Debug for UnixStream<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

impl<M> AsFd for UnixStream<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

impl<M> AsRawFd for UnixStream<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

/// A unix-domain datagram socket, to send datagrams from and receive datagrams on.
///
/// A datagram socket exchanges datagrams, each a message of its own, rather than a stream of
/// bytes. It is the async counterpart of [`std::os::unix::net::UnixDatagram`]: a receive or a send
/// that has to wait leaves the thread to the other tasks instead of blocking it.
///
/// A socket is bound to a path with [`UnixDatagram::bind`], so that others can send to it by that
/// path; made [unbound](UnixDatagram::unbound), it can send but has no address to be sent to; and
/// the two of a [pair](UnixDatagram::pair) are connected to each other. [`send_to`] sends a
/// datagram to the socket bound to a path, and [`connect`] fixes the one socket to send to, and to
/// receive from, which [`send`] and [`recv`] then use. A datagram keeps its boundaries: one
/// receive takes one datagram, and what does not fit into the buffer it is given is discarded.
///
/// The socket runs on the runtime given to the constructor that made it, whose reactor watches it
/// from then on. Its operations make progress while some thread is inside [`Runtime::block_on`] on
/// that runtime, or, on a runtime from `SharedRuntime::current`, while the helper thread runs it.
/// The socket's type carries the flavour of that runtime: one built on a
/// [`LocalRuntime`](crate::LocalRuntime) is a `UnixDatagram<Local>`, [`Local`] being the default,
/// and stays on the thread it was made on; one built on a [`SharedRuntime`](crate::SharedRuntime)
/// is a `UnixDatagram<Shared>`, which may be sent to, and used from, any thread.
///
/// Any number of tasks may wait to receive at once, through [`recv`](UnixDatagram::recv) or
/// [`recv_from`](UnixDatagram::recv_from), and any number to send, through
/// [`send`](UnixDatagram::send) or [`send_to`](UnixDatagram::send_to), each through a reference to
/// the socket. Each receive takes a datagram of its own, so tasks that receive together get one
/// each.
///
/// # Example
///
/// A pair of datagram sockets, one sending a datagram and the other receiving it, and the answer
/// going the other way:
///
/// ```
/// use zruntime::{LocalRuntime, net::unix::UnixDatagram};
///
/// let runtime = LocalRuntime::new()?;
/// let (client, server) = UnixDatagram::pair(&runtime)?;
///
/// runtime.block_on(async {
///     client.send(b"ping").await?;
///     let mut datagram = [0; 16];
///     let received = server.recv(&mut datagram).await?;
///     assert_eq!(&datagram[..received], b"ping");
///
///     server.send(b"pong").await?;
///     let received = client.recv(&mut datagram).await?;
///     assert_eq!(&datagram[..received], b"pong");
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok::<_, std::io::Error>(())
/// ```
///
/// [`send_to`]: UnixDatagram::send_to
/// [`connect`]: UnixDatagram::connect
/// [`send`]: UnixDatagram::send
/// [`recv`]: UnixDatagram::recv
pub struct UnixDatagram<M = Local>
where
    M: Mode,
{
    io: AsyncIo<std::os::unix::net::UnixDatagram, M>,
}

impl<M> UnixDatagram<M>
where
    M: Mode,
{
    /// A datagram socket bound to `path`, on `runtime`.
    ///
    /// Binding creates a socket file at `path`, which must have no file already: where one is
    /// there, a stale socket file left by an earlier socket included, this fails with
    /// [`AddrInUse`](io::ErrorKind::AddrInUse), and removing that file first is the caller's to do.
    /// The file the socket makes stays where it is when the socket is dropped, so whoever binds a
    /// path removes the file when done with it. A path too long to name a socket, which is about a
    /// hundred bytes, fails with [`InvalidInput`](io::ErrorKind::InvalidInput).
    ///
    /// What can fail is that, or what [`from_std`](UnixDatagram::from_std) can fail at.
    pub fn bind<P>(runtime: &Runtime<M>, path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        Self::from_std(runtime, std::os::unix::net::UnixDatagram::bind(path)?)
    }

    /// A datagram socket on `runtime` that is bound to no path.
    ///
    /// Such a socket can send, to a path through [`send_to`](UnixDatagram::send_to) or to the
    /// socket it is [connected](UnixDatagram::connect) to, but it has no address of its own, so
    /// nothing can send to it, and the socket that receives its datagrams sees the address they
    /// came from as unnamed.
    ///
    /// What can fail is making the socket, and what [`from_std`](UnixDatagram::from_std) can fail
    /// at.
    pub fn unbound(runtime: &Runtime<M>) -> io::Result<Self> {
        Self::from_std(runtime, std::os::unix::net::UnixDatagram::unbound()?)
    }

    /// A connected pair of datagram sockets, on `runtime`.
    ///
    /// The datagrams one of them sends are received by the other, and the other way round, with no
    /// path to name them by.
    ///
    /// What can fail is making the pair, and what [`from_std`](UnixDatagram::from_std) can fail
    /// at.
    pub fn pair(runtime: &Runtime<M>) -> io::Result<(Self, Self)> {
        let (first, second) = std::os::unix::net::UnixDatagram::pair()?;

        Ok((
            Self::from_std(runtime, first)?,
            Self::from_std(runtime, second)?,
        ))
    }

    /// A datagram socket on `runtime` that sends and receives the datagrams `socket` does.
    ///
    /// `socket` is switched to non-blocking mode, which every socket of this type is in. The mode
    /// belongs to the open socket rather than to a handle on it, so a duplicate of `socket` made
    /// with [`try_clone`](std::os::unix::net::UnixDatagram::try_clone) is switched with it, and a
    /// receive or a send on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited. On Apple's platforms the socket also gets `SO_NOSIGPIPE`, so that a send
    /// to a peer that has gone fails rather than raising `SIGPIPE`.
    ///
    /// What can fail is the switch to non-blocking mode, setting that option, and the runtime
    /// taking the socket under its watch.
    pub fn from_std(
        runtime: &Runtime<M>,
        socket: std::os::unix::net::UnixDatagram,
    ) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        set_nosigpipe(&socket)?;

        Ok(Self {
            io: AsyncIo::from_nonblocking(runtime, socket)?,
        })
    }

    /// Connects the socket to the socket bound to `path`.
    ///
    /// From then on [`send`](UnixDatagram::send) sends to that socket, and
    /// [`recv`](UnixDatagram::recv) and [`recv_from`](UnixDatagram::recv_from) take only the
    /// datagrams it sends. A datagram another socket sent before the connect may still be queued,
    /// and is received like any other. Datagram sockets have no connection to wait for, so this
    /// does not block: it fails where `path` names no socket that is bound, and it may be called
    /// again to connect the socket to another one.
    pub fn connect<P>(&self, path: P) -> io::Result<()>
    where
        P: AsRef<Path>,
    {
        self.io.get_ref().connect(path)
    }

    /// Sends `buf` as a datagram to the socket bound to `path`.
    ///
    /// Resolves to the number of bytes sent. It fails where `path` names no socket that is bound:
    /// with [`NotFound`](io::ErrorKind::NotFound) where there is no file there, and with
    /// [`ConnectionRefused`](io::ErrorKind::ConnectionRefused) where the file is that of a socket
    /// that is gone.
    ///
    /// A datagram that the system turns away with [`WouldBlock`](io::ErrorKind::WouldBlock), as
    /// Linux does where the receiving socket has no room for it, is retried every 20 milliseconds,
    /// on the runtime's timer and without blocking the thread, for as long as the caller awaits
    /// the send. Nothing but the caller bounds that, with a timeout of its own or by dropping the
    /// future, which gives up the send. A [connected](UnixDatagram::connect) socket's
    /// [`send`](UnixDatagram::send) waits for room instead, and sends as soon as there is some.
    ///
    /// Any number of tasks may send at once, through this or [`send`](UnixDatagram::send), as [the
    /// socket's documentation](UnixDatagram) says.
    pub async fn send_to<P>(&self, buf: &[u8], path: P) -> io::Result<usize>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        // Not a readiness wait: see `SEND_TO_INTERVAL`.
        loop {
            match self.io.get_ref().send_to(buf, path) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.io.runtime().sleep(SEND_TO_INTERVAL).await;
                }
                result => return result,
            }
        }
    }

    /// Waits for a datagram to arrive, and receives it into `buf`.
    ///
    /// Resolves to the number of bytes received, and the address of the socket that sent the
    /// datagram, which is unnamed unless that socket is bound to a path. A datagram longer than
    /// `buf` is cut short, and the rest of it is discarded.
    ///
    /// Any number of tasks may wait to receive at once, each taking a datagram of its own, through
    /// this or [`recv`](UnixDatagram::recv), as [the socket's documentation](UnixDatagram) says.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.io.read_with(|socket| socket.recv_from(buf)).await
    }

    /// Waits for room in the socket, and sends `buf` as a datagram to the socket this one is
    /// connected to.
    ///
    /// Resolves to the number of bytes sent. The socket has to be connected, to a path with
    /// [`connect`](UnixDatagram::connect), or as one of a [pair](UnixDatagram::pair): a send
    /// without a peer to send to fails.
    ///
    /// Any number of tasks may wait to send at once, through this or
    /// [`send_to`](UnixDatagram::send_to), as [the socket's documentation](UnixDatagram) says.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        // Not std's `send`: that is a plain `write(2)`, which raises `SIGPIPE` on the BSDs and on
        // Apple's platforms after a `shutdown` of the write half. `send(2)` takes `MSG_NOSIGNAL`
        // where there is one, as a stream's writes do (see `SEND_FLAGS`). `send_to` stays std's,
        // which sends with that flag where there is one.
        self.io
            .write_with(|socket| Ok(rustix::net::send(socket, buf, SEND_FLAGS)?))
            .await
    }

    /// Waits for a datagram to arrive, and receives it into `buf`, without the address it came
    /// from.
    ///
    /// Resolves to the number of bytes received. A datagram longer than `buf` is cut short, and the
    /// rest of it is discarded. This is [`recv_from`](UnixDatagram::recv_from) for a socket that is
    /// connected, to a path with [`connect`](UnixDatagram::connect) or as one of a
    /// [pair](UnixDatagram::pair), and takes only its peer's datagrams from then on: it has no use
    /// for the address of the sender. A datagram another socket sent before the connect may still
    /// be queued, and is received all the same, without saying which socket sent it.
    ///
    /// Any number of tasks may wait to receive at once, each taking a datagram of its own, through
    /// this or [`recv_from`](UnixDatagram::recv_from), as [the socket's
    /// documentation](UnixDatagram) says.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.read_with(|socket| socket.recv(buf)).await
    }

    /// The local socket address of the socket: the path it is bound to, which
    /// [`SocketAddr::as_pathname`] gives, or an unnamed address where it is bound to none.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The socket address of the socket this one is connected to, which is unnamed where that
    /// socket is bound to no path.
    ///
    /// It fails with [`NotConnected`](io::ErrorKind::NotConnected) where the socket is not
    /// connected.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Shuts down the receiving half, the sending half or both halves of the socket, as `how`
    /// says.
    ///
    /// A send after the sending half is shut down fails with
    /// [`BrokenPipe`](io::ErrorKind::BrokenPipe).
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.io.get_ref().shutdown(how)
    }
}

/// How long a `send_to` that the receiver has no room for waits before it tries again.
///
/// The wait is on the timer, and not for the socket to become writable as a `send`'s is, because
/// the socket's readiness says nothing here. Linux reports a socket that sends to an address of
/// its choosing as writable whether or not the receiver has room (`unix_dgram_poll` clears
/// writability only for a connected peer), and arms no wake-up for the receiver having room either
/// (`unix_dgram_sendmsg` does so only for a connected peer). So a wait for writability returns at
/// once, and the send would be tried over and over, a core spinning, for as long as the receiver's
/// queue stays full.
const SEND_TO_INTERVAL: Duration = Duration::from_millis(20);

impl<M> fmt::Debug for UnixDatagram<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

impl<M> AsFd for UnixDatagram<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

impl<M> AsRawFd for UnixDatagram<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

/// A stream of the connections a [`UnixListener`] accepts, made by [`UnixListener::incoming`].
///
/// Each item is the [`UnixStream`] of a connection, on the listener's runtime, or the error of an
/// accept that failed. The stream never ends: it is pending while no connection waits, and yields
/// the next one when it comes, for as long as it is polled. It implements the [`Stream`] trait of
/// [`futures-core`], so the extension traits of [`futures`] drive it.
///
/// An error is not the end of the stream either, and it does not leave the stream pending until
/// the next connection comes in: polled again, the stream accepts again. Where the error left the
/// connection queued, as the system running out of file descriptors (`EMFILE`) does on Linux and
/// the BSDs, the next item is that error again at once, and so is each one after it for as long
/// as the connection stays queued, so a loop that goes on taking items straight after an error
/// spins the thread. On Apple's platforms an accept that finds no descriptor left closes the
/// connection it took off the queue instead, so the error does not repeat, but the connection is
/// lost. A caller that goes on after an error backs off first, with [`Runtime::sleep`], say.
///
/// At most one task at a time may wait for the next item, counting every `Incoming` of the
/// listener: where a second task waits as well, the one that waited first may never be woken. Tasks
/// that take items from one listener's streams take turns, behind a lock of their own. Tasks
/// waiting in [`accept`](UnixListener::accept) do not count against that, nor does it count against
/// them, as [the listener's documentation](UnixListener) says.
///
/// [`futures-core`]: https://docs.rs/futures-core
/// [`futures`]: https://docs.rs/futures
pub struct Incoming<'a, M = Local>
where
    M: Mode,
{
    listener: &'a UnixListener<M>,
}

impl<M> Stream for Incoming<'_, M>
where
    M: Mode,
{
    type Item = io::Result<UnixStream<M>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.listener
            .poll_accept(cx)
            .map(|accepted| Some(accepted.map(|(stream, _address)| stream)))
    }
}

impl<M> fmt::Debug for Incoming<'_, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", &self.listener)
            .finish()
    }
}

impl<M> UnixListener<M>
where
    M: Mode,
{
    /// Accepts a connection if one is waiting, and otherwise arranges for `cx`'s waker to be woken
    /// once one is.
    ///
    /// What `accept` and the `Incoming` stream both poll.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(UnixStream<M>, SocketAddr)>> {
        let (stream, address) = ready!(self.io.poll_read_with(cx, |listener| listener.accept()))?;

        Poll::Ready(self.accepted(stream, address))
    }

    /// The stream of a connection the std listener accepted, on this listener's runtime, with the
    /// address of its peer.
    fn accepted(
        &self,
        stream: std::os::unix::net::UnixStream,
        address: SocketAddr,
    ) -> io::Result<(UnixStream<M>, SocketAddr)> {
        // std's `accept` leaves the accepted socket in blocking mode on Linux, which `from_std`
        // sets right.
        UnixStream::from_std(&self.io.runtime(), stream).map(|stream| (stream, address))
    }
}

impl<M> UnixStream<M>
where
    M: Mode,
{
    /// A stream connected to the socket at `address`, on `runtime`.
    ///
    /// What `connect` and `connect_addr` both end in, so that the wait for room in a full backlog
    /// is the same for either.
    async fn connect_sockaddr(runtime: &Runtime<M>, address: &SockAddr) -> io::Result<Self> {
        let io = connect::connect_unix(runtime, address).await?;

        Ok(Self { io })
    }
}

/// The address of the socket that `address` names, as the system takes it to connect to.
///
/// A path is taken as `connect` takes one. A name in the abstract namespace, which only Linux and
/// Android have, is taken without the checks of a path, and `SockAddr::unix` takes it for what it
/// is by the zero byte it leads with. An unnamed address names nothing to connect to.
fn named_sockaddr(address: &SocketAddr) -> io::Result<SockAddr> {
    if let Some(path) = address.as_pathname() {
        return pathname_sockaddr(path);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Some(name) = address.as_abstract_name() {
        // The zero byte in front is what makes the name abstract, and the name follows it as it
        // is, a zero byte inside included: socket2 counts the whole of it into the address's
        // length, with no terminator after it, as an abstract name has none.
        let path = [&[0u8][..], name].concat();

        return SockAddr::unix(std::ffi::OsStr::from_bytes(&path));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "an unnamed address names no socket to connect to",
    ))
}

/// The address of the socket file at `path`, as the system takes it to connect to.
///
/// `SockAddr::unix` takes a zero byte as it comes: one that leads the path names a socket in
/// Linux's abstract namespace instead, and one inside it cuts the path short. Neither is what a
/// path means anywhere else, so a path with a zero byte in it is refused, as std refuses it.
fn pathname_sockaddr(path: &Path) -> io::Result<SockAddr> {
    if path.as_os_str().as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "paths must not contain interior null bytes",
        ));
    }

    SockAddr::unix(path)
}

/// The flags every write to a stream carries.
///
/// `MSG_NOSIGNAL` makes a write to a peer that has gone away an error rather than a signal.
/// Apple's platforms and Redox have no such flag, so a write there goes without it.
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
    target_os = "redox"
)))]
const SEND_FLAGS: rustix::net::SendFlags = rustix::net::SendFlags::NOSIGNAL;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
    target_os = "redox"
))]
const SEND_FLAGS: rustix::net::SendFlags = rustix::net::SendFlags::empty();
