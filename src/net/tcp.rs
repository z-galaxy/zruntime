//! The TCP sockets: [`TcpListener`], [`TcpStream`], and the [`Incoming`] stream of the connections
//! a listener accepts.

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};
use std::{
    fmt,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, ready},
};

use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};
use socket2::{Domain, SockAddr};

use super::{connect, set_nosigpipe};
use crate::{Async, Local, Mode, Runtime};

/// A TCP socket server, listening for connections.
///
/// A listener is bound to a socket address with [`TcpListener::bind`], and hands out the
/// connections made to that address, each as a [`TcpStream`], through
/// [`accept`](TcpListener::accept), or as a stream of them through
/// [`incoming`](TcpListener::incoming). It is the async counterpart of [`std::net::TcpListener`]:
/// waiting for a connection leaves the thread to the other tasks instead of blocking it.
///
/// The listener runs on the runtime given to the constructor that made it, whose reactor watches
/// it from then on, and so do the streams it accepts. Its operations make progress while some
/// thread is inside [`Runtime::block_on`] on that runtime, or, on a runtime from
/// `SharedRuntime::current`, while the helper thread runs it. The listener's type carries the
/// flavour of that runtime: one built on a [`LocalRuntime`](crate::LocalRuntime) is a
/// `TcpListener<Local>`, [`Local`] being the default, and stays on the thread it was made on; one
/// built on a [`SharedRuntime`](crate::SharedRuntime) is a `TcpListener<Shared>`, which may be
/// sent to, and used from, any thread.
///
/// Any number of tasks may wait for a connection through [`accept`](TcpListener::accept) at once,
/// each through a reference to the listener, and each connection goes to one of them. Waiting for
/// the next item of an [`Incoming`] stream is not like that: at most one task at a time may do it,
/// counting every stream of the listener, and the tasks in `accept` do not count against it.
///
/// # Example
///
/// A listener that accepts a connection and reads a greeting from it, and a client that makes the
/// connection and sends the greeting:
///
/// ```
/// use std::net::Ipv4Addr;
///
/// use futures_lite::{AsyncReadExt, AsyncWriteExt};
/// use zruntime::{
///     LocalRuntime,
///     net::{TcpListener, TcpStream},
/// };
///
/// let runtime = LocalRuntime::new()?;
/// // Port `0` has the system pick a free port, which the listener then reports.
/// let listener = TcpListener::bind(&runtime, (Ipv4Addr::LOCALHOST, 0))?;
/// let address = listener.local_addr()?;
///
/// runtime.block_on(async {
///     let mut client = TcpStream::connect(&runtime, address).await?;
///     client.write_all(b"hello").await?;
///
///     let (mut server, peer) = listener.accept().await?;
///     let mut greeting = [0; 5];
///     server.read_exact(&mut greeting).await?;
///
///     assert_eq!(&greeting, b"hello");
///     assert_eq!(peer, client.local_addr()?);
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok::<_, std::io::Error>(())
/// ```
pub struct TcpListener<M = Local>
where
    M: Mode,
{
    io: Async<std::net::TcpListener, M>,
}

impl<M> TcpListener<M>
where
    M: Mode,
{
    /// A listener bound to `addr`, on `runtime`.
    ///
    /// `addr` is a socket address: a [`SocketAddr`], or anything that converts into one, such as a
    /// pair of an [`Ipv4Addr`](std::net::Ipv4Addr) and a port. It is never a host name. Looking a
    /// name up, through std's [`ToSocketAddrs`](std::net::ToSocketAddrs), blocks the thread for as
    /// long as the resolver takes, which a task must not do to the thread it shares with the
    /// others, so a caller with a name looks it up first, on a thread of its own, and binds to the
    /// address it finds. Binding to port `0` has the system pick a free port, which
    /// [`local_addr`](TcpListener::local_addr) then tells.
    ///
    /// The listener is bound and listening once this returns. What can fail is that, or what
    /// [`from_std`](TcpListener::from_std) can fail at.
    pub fn bind<A>(runtime: &Runtime<M>, addr: A) -> io::Result<Self>
    where
        A: Into<SocketAddr>,
    {
        Self::from_std(runtime, std::net::TcpListener::bind(addr.into())?)
    }

    /// A listener on `runtime` that accepts the connections `listener` would.
    ///
    /// `listener` is switched to non-blocking mode, which every listener of this type is in. On
    /// Unix the mode belongs to the open socket rather than to a handle on it, so a duplicate of
    /// `listener` made with [`try_clone`](std::net::TcpListener::try_clone) is switched with it,
    /// and an `accept` on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited.
    ///
    /// What can fail is the switch to non-blocking mode, and the runtime taking the socket under
    /// its watch, which on Windows it does for a limited number of sockets: see the
    /// [module documentation](super).
    pub fn from_std(runtime: &Runtime<M>, listener: std::net::TcpListener) -> io::Result<Self> {
        listener.set_nonblocking(true)?;

        Ok(Self {
            io: Async::from_nonblocking(runtime, listener)?,
        })
    }

    /// Waits for a connection to this listener, and accepts it.
    ///
    /// Resolves to the stream of the connection, which runs on the listener's runtime and is in
    /// non-blocking mode, and the address of the peer that made the connection.
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
    ///
    /// On Windows, where a runtime watches a limited number of sockets (see the
    /// [module documentation](super)), an accept that would take it past the limit takes the
    /// connection off the queue and then fails to register it: the connection is closed, which its
    /// peer sees as its end, and the accept fails.
    pub async fn accept(&self) -> io::Result<(TcpStream<M>, SocketAddr)> {
        let (stream, address) = self.io.read_with(|listener| listener.accept()).await?;

        self.accepted(stream, address)
    }

    /// A stream of the connections made to this listener.
    ///
    /// Each item is the [`TcpStream`] of a connection, accepted as [`accept`](TcpListener::accept)
    /// accepts it, or the error of an accept that failed. The stream never ends, and an error is
    /// not its end either. Nor does an error leave it pending until the next connection comes in:
    /// polled again, it accepts again. Where the error left the connection queued, as the system
    /// running out of file descriptors (`EMFILE`) does on Linux and the BSDs, the next item is
    /// that error again at once, and so is each one after it for as long as the connection stays
    /// queued, so a loop that goes on taking items straight after an error spins the thread. On
    /// Apple's platforms an accept that finds no descriptor left closes the connection it took off
    /// the queue instead, so the error does not repeat, but the connection is lost. A caller that
    /// goes on after an error backs off first, with [`Runtime::sleep`], say.
    ///
    /// On Windows, where a runtime watches a limited number of sockets (see the
    /// [module documentation](super)), an item that would take it past the limit is the error of
    /// an accept that took the connection off the queue and closed it, as
    /// [`accept`](TcpListener::accept) says.
    ///
    /// At most one task at a time may wait for the next item, counting every stream this listener
    /// hands out, and a task waiting in [`accept`](TcpListener::accept) does not count against it,
    /// as [the stream's documentation](Incoming) says.
    ///
    /// # Example
    ///
    /// The first two connections made to a listener, in the order they were made:
    ///
    /// ```
    /// use std::net::Ipv4Addr;
    ///
    /// use futures_lite::StreamExt;
    /// use zruntime::{
    ///     LocalRuntime,
    ///     net::{TcpListener, TcpStream},
    /// };
    ///
    /// let runtime = LocalRuntime::new()?;
    /// let listener = TcpListener::bind(&runtime, (Ipv4Addr::LOCALHOST, 0))?;
    /// let address = listener.local_addr()?;
    ///
    /// runtime.block_on(async {
    ///     let first = TcpStream::connect(&runtime, address).await?;
    ///     let second = TcpStream::connect(&runtime, address).await?;
    ///
    ///     let mut incoming = listener.incoming();
    ///     let first_accepted = incoming.next().await.expect("the stream never ends")?;
    ///     let second_accepted = incoming.next().await.expect("the stream never ends")?;
    ///
    ///     assert_eq!(first_accepted.peer_addr()?, first.local_addr()?);
    ///     assert_eq!(second_accepted.peer_addr()?, second.local_addr()?);
    ///     # Ok::<_, std::io::Error>(())
    /// })?;
    /// # Ok::<_, std::io::Error>(())
    /// ```
    pub fn incoming(&self) -> Incoming<'_, M> {
        Incoming { listener: self }
    }

    /// The local socket address this listener is bound to.
    ///
    /// After binding to port `0`, this is how to find the port the system picked.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The value of the `IP_TTL` option of this listener's socket: the time-to-live field of the
    /// IP packets sent from it.
    ///
    /// See [`set_ttl`](TcpListener::set_ttl).
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the value of the `IP_TTL` option of this listener's socket: the time-to-live field of
    /// the IP packets sent from it.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }
}

impl<M> fmt::Debug for TcpListener<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

#[cfg(unix)]
impl<M> AsFd for TcpListener<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

#[cfg(unix)]
impl<M> AsRawFd for TcpListener<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

#[cfg(windows)]
impl<M> AsSocket for TcpListener<M>
where
    M: Mode,
{
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.io.get_ref().as_socket()
    }
}

#[cfg(windows)]
impl<M> AsRawSocket for TcpListener<M>
where
    M: Mode,
{
    fn as_raw_socket(&self) -> RawSocket {
        self.io.get_ref().as_raw_socket()
    }
}

/// A TCP connection, to read from and write to.
///
/// A stream is made by connecting to a socket address with [`TcpStream::connect`], or by a
/// [`TcpListener`] accepting a connection. It is the async counterpart of [`std::net::TcpStream`]:
/// a read or a write that has to wait leaves the thread to the other tasks instead of blocking it.
///
/// The stream implements the `AsyncRead` and `AsyncWrite` traits of [`futures-io`], so the
/// extension traits of [`futures-lite`] or [`futures-util`] read from and write to it. So does a
/// shared reference to it, `&TcpStream`, which lets a reader and a writer share one stream, as
/// std's `Read` and `Write` do for a `&std::net::TcpStream`. A write sends as much as the kernel
/// takes at once, which may be less than the whole buffer, and nothing is held back, so flushing
/// has nothing to do.
///
/// Closing the stream, as the `close` of `AsyncWriteExt` does, shuts down the write half of the
/// socket, as [`shutdown`](TcpStream::shutdown) with [`Shutdown::Write`] does: the peer reads the
/// end of the stream once it has read what was sent, while the read half stays open. It is the
/// socket that is shut down, so closing the stream through one `&TcpStream` ends it for every
/// handle on it. Closing a stream that is closed already is fine, on every platform: the write
/// half is not shut down a second time, so the read half stays open. That goes for a stream whose
/// write half was shut down with `shutdown` before, too.
///
/// The stream runs on the runtime given to the constructor that made it, whose reactor watches it
/// from then on; a stream a [`TcpListener`] accepted is on the listener's runtime. Its operations
/// make progress while some thread is inside [`Runtime::block_on`] on that runtime, or, on a
/// runtime from `SharedRuntime::current`, while the helper thread runs it. The stream's type
/// carries the flavour of that runtime: one built on a [`LocalRuntime`](crate::LocalRuntime) is a
/// `TcpStream<Local>`, [`Local`] being the default, and stays on the thread it was made on; one
/// built on a [`SharedRuntime`](crate::SharedRuntime) is a `TcpStream<Shared>`, which may be sent
/// to, and used from, any thread.
///
/// At most one task at a time may wait to read from a stream through its `AsyncRead`
/// implementation, and at most one to write to it through its `AsyncWrite` one, counting every
/// reference to the stream: where a second task waits in the same direction, the one that waited
/// first may never be woken. Tasks that read, or write, together take turns, behind a lock of their
/// own. [`peek`](TcpStream::peek) is not subject to that: any number of tasks may wait in it at
/// once, and one that does never takes the place of a task waiting to read, nor the other way
/// round.
///
/// # Example
///
/// A client that sends a message and closes its end of the stream, a server that reads the message
/// up to the end of the stream, and the answer that still gets from the server to the client:
///
/// ```
/// use std::net::Ipv4Addr;
///
/// use futures_lite::{AsyncReadExt, AsyncWriteExt};
/// use zruntime::{
///     LocalRuntime,
///     net::{TcpListener, TcpStream},
/// };
///
/// let runtime = LocalRuntime::new()?;
/// let listener = TcpListener::bind(&runtime, (Ipv4Addr::LOCALHOST, 0))?;
///
/// runtime.block_on(async {
///     let mut client = TcpStream::connect(&runtime, listener.local_addr()?).await?;
///     let (mut server, _) = listener.accept().await?;
///
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
/// [`futures-lite`]: https://docs.rs/futures-lite
/// [`futures-util`]: https://docs.rs/futures-util
pub struct TcpStream<M = Local>
where
    M: Mode,
{
    io: Async<std::net::TcpStream, M>,
    /// Whether the write half of the socket is shut down already, by a close or by a `shutdown`
    /// of it, so that a close finding it so shuts nothing down again.
    ///
    /// A second shutdown of the write half is harmless on most platforms, but not on FreeBSD and
    /// NetBSD. Once the peer has acknowledged the first, the connection is in the state
    /// `FIN_WAIT_2`, and there `tcp_usrclosed` takes the socket for disconnected, as it does in
    /// every state from `FIN_WAIT_2` on (it calls `soisdisconnected`), which ends the read half
    /// too. Linux and OpenBSD ignore the second shutdown, and macOS answers it with `ENOTCONN`,
    /// which a close reads as success. So closing a stream that is closed already is fine, as the
    /// docs promise, only where the close does not reach the system again.
    ///
    /// The flag is set before the system is asked, and it stays set whatever the answer is: a
    /// shutdown that fails may have shut the write half down all the same. On FreeBSD and macOS
    /// the error comes from sending the FIN (`tcp_output`), after the half is marked shut, so
    /// clearing the flag on an error would allow exactly the second shutdown the flag is there to
    /// stop. Setting it first also leaves no gap, between the call and the flag, for a close
    /// through another handle to slip into.
    ///
    /// An atomic because a close goes through a shared reference, and a `TcpStream<Shared>` is
    /// `Sync`. `Relaxed` is enough: nothing is published through the flag, which only decides
    /// whether a call is made to the system.
    write_shut: AtomicBool,
}

impl<M> TcpStream<M>
where
    M: Mode,
{
    /// A stream connected to `addr`, on `runtime`.
    ///
    /// `addr` is a socket address: a [`SocketAddr`], or anything that converts into one, such as a
    /// pair of an [`Ipv4Addr`](std::net::Ipv4Addr) and a port. It is never a host name. Looking a
    /// name up, through std's [`ToSocketAddrs`](std::net::ToSocketAddrs), blocks the thread for as
    /// long as the resolver takes, which a task must not do to the thread it shares with the
    /// others, so a caller with a name looks it up first, on a thread of its own, and connects to
    /// the addresses it finds, one after the other, until one of them takes the connection.
    ///
    /// The connection does not block the thread either: the future waits for the runtime to report
    /// that the connection is made, or that it failed, in which case it resolves to the error.
    /// Dropping the future before it completes gives up the attempt.
    ///
    /// On Windows, where a runtime watches a limited number of sockets (see the
    /// [module documentation](super)), a connect that would take it past the limit fails.
    pub async fn connect<A>(runtime: &Runtime<M>, addr: A) -> io::Result<Self>
    where
        A: Into<SocketAddr>,
    {
        let address = addr.into();
        let io = connect::connect(
            runtime,
            Domain::for_address(address),
            &SockAddr::from(address),
        )
        .await?;

        Ok(Self {
            io,
            write_shut: AtomicBool::new(false),
        })
    }

    /// A stream on `runtime` that reads and writes the connection `stream` does.
    ///
    /// `stream` is switched to non-blocking mode, which every stream of this type is in. On Unix
    /// the mode belongs to the open socket rather than to a handle on it, so a duplicate of
    /// `stream` made with [`try_clone`](std::net::TcpStream::try_clone) is switched with it, and a
    /// read or a write on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited. On Apple's platforms the socket also gets `SO_NOSIGPIPE`, so that a
    /// write to a peer that has gone fails rather than raising `SIGPIPE`.
    ///
    /// What can fail is the switch to non-blocking mode, setting that option, and the runtime
    /// taking the socket under its watch, which on Windows it does for a limited number of
    /// sockets: see the [module documentation](super).
    pub fn from_std(runtime: &Runtime<M>, stream: std::net::TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        set_nosigpipe(&stream)?;

        Ok(Self {
            io: Async::from_nonblocking(runtime, stream)?,
            write_shut: AtomicBool::new(false),
        })
    }

    /// The local socket address of the connection: this end of it.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The socket address of the peer the stream is connected to: the other end of the connection.
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
    /// [`set_nonblocking`](std::net::TcpStream::set_nonblocking) switches the mode back. Handing
    /// the socket to [`from_std`](TcpStream::from_std) takes it up again, on this runtime or on
    /// another. A stream made that way does not know that the write half of the socket was shut
    /// down before, as [`shutdown`](TcpStream::shutdown) says.
    ///
    /// On a shared runtime this may wait for a moment, while a thread driving the runtime returns
    /// from a wait that watched the socket.
    pub fn into_std(self) -> std::net::TcpStream {
        self.io.into_inner()
    }

    /// Shuts down the read half, the write half or both halves of the connection, as `how` says.
    ///
    /// Shutting down the write half makes the peer read the end of the stream, once it has read
    /// what was sent, which is what closing the stream does. It is the socket that is shut
    /// down, so it holds for every handle on the stream.
    ///
    /// Closing the stream after its write half was shut down here, alone or with the read half,
    /// does nothing more. A second `shutdown` of the write half does reach the system, unlike a
    /// second close: that is harmless on most platforms, but on FreeBSD and NetBSD it ends the
    /// read half too, once the peer has acknowledged the first. And a stream made with
    /// [`from_std`](TcpStream::from_std) from a socket whose write half was shut down through std
    /// does not know it: closing that stream shuts the write half down again.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        if matches!(how, Shutdown::Write | Shutdown::Both) {
            // Noted before the system is asked, whatever it answers: see `write_shut`. Nothing
            // else is done with the flag here: a second `shutdown` goes to the system as it is.
            self.write_shut.store(true, Ordering::Relaxed);
        }

        self.io.get_ref().shutdown(how)
    }

    /// Waits for bytes to arrive, and copies them into `buf` without taking them off the stream.
    ///
    /// Resolves to the number of bytes copied, which is zero only where `buf` is empty or the peer
    /// has closed its end and nothing is left to read. The next read, or `peek`, finds the same
    /// bytes again.
    ///
    /// Any number of tasks may wait to peek at once, alongside the one that waits to read, as [the
    /// stream's documentation](TcpStream) says.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.read_with(|stream| stream.peek(buf)).await
    }

    /// The value of the `TCP_NODELAY` option of this stream's socket.
    ///
    /// See [`set_nodelay`](TcpStream::set_nodelay).
    pub fn nodelay(&self) -> io::Result<bool> {
        self.io.get_ref().nodelay()
    }

    /// Sets the value of the `TCP_NODELAY` option of this stream's socket.
    ///
    /// With the option set, a write is sent as soon as it is made, rather than held back so that it
    /// can be sent together with the writes that follow it (Nagle's algorithm).
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.io.get_ref().set_nodelay(nodelay)
    }

    /// The value of the `IP_TTL` option of this stream's socket: the time-to-live field of the IP
    /// packets sent from it.
    ///
    /// See [`set_ttl`](TcpStream::set_ttl).
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the value of the `IP_TTL` option of this stream's socket: the time-to-live field of the
    /// IP packets sent from it.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }
}

impl<M> AsyncRead for TcpStream<M>
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

impl<M> AsyncWrite for TcpStream<M>
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

impl<M> AsyncRead for &TcpStream<M>
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

// The vectored methods are left to the traits' defaults, which write the first buffer that is not
// empty: std's vectored write sends without `MSG_NOSIGNAL`, which its plain one sends with.
impl<M> AsyncWrite for &TcpStream<M>
where
    M: Mode,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io.poll_write_with(cx, |mut stream| stream.write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Nothing is buffered here: a write that reports success has gone to the kernel.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The write half is shut down once: see `write_shut` for what a second shutdown does on
        // some platforms, and for why the flag stays set where this one fails.
        if self.write_shut.swap(true, Ordering::Relaxed) {
            return Poll::Ready(Ok(()));
        }

        match self.io.get_ref().shutdown(Shutdown::Write) {
            // A stream whose connection is gone has nothing left to close: the system answers a
            // shutdown there with `NotConnected`, which means the stream is closed already, and
            // closing it is not an error, on any platform.
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Poll::Ready(Ok(())),
            result => Poll::Ready(result),
        }
    }
}

impl<M> fmt::Debug for TcpStream<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

#[cfg(unix)]
impl<M> AsFd for TcpStream<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

#[cfg(unix)]
impl<M> AsRawFd for TcpStream<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

#[cfg(windows)]
impl<M> AsSocket for TcpStream<M>
where
    M: Mode,
{
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.io.get_ref().as_socket()
    }
}

#[cfg(windows)]
impl<M> AsRawSocket for TcpStream<M>
where
    M: Mode,
{
    fn as_raw_socket(&self) -> RawSocket {
        self.io.get_ref().as_raw_socket()
    }
}

/// A stream of the connections a [`TcpListener`] accepts, made by [`TcpListener::incoming`].
///
/// Each item is the [`TcpStream`] of a connection, on the listener's runtime, or the error of an
/// accept that failed. The stream never ends: it is pending while no connection waits, and yields
/// the next one when it comes, for as long as it is polled. It implements the [`Stream`] trait of
/// [`futures-core`], so the extension traits of [`futures-lite`] or [`futures-util`] drive it.
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
/// waiting in [`accept`](TcpListener::accept) do not count against that, nor does it count against
/// them, as [the listener's documentation](TcpListener) says.
///
/// [`futures-core`]: https://docs.rs/futures-core
/// [`futures-lite`]: https://docs.rs/futures-lite
/// [`futures-util`]: https://docs.rs/futures-util
pub struct Incoming<'a, M = Local>
where
    M: Mode,
{
    listener: &'a TcpListener<M>,
}

impl<M> Stream for Incoming<'_, M>
where
    M: Mode,
{
    type Item = io::Result<TcpStream<M>>;

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

impl<M> TcpListener<M>
where
    M: Mode,
{
    /// Accepts a connection if one is waiting, and otherwise arranges for `cx`'s waker to be woken
    /// once one is.
    ///
    /// What `accept` and the `Incoming` stream both poll.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream<M>, SocketAddr)>> {
        let (stream, address) = ready!(self.io.poll_read_with(cx, |listener| listener.accept()))?;

        Poll::Ready(self.accepted(stream, address))
    }

    /// The stream of a connection the std listener accepted, on this listener's runtime, with the
    /// address of its peer.
    fn accepted(
        &self,
        stream: std::net::TcpStream,
        address: SocketAddr,
    ) -> io::Result<(TcpStream<M>, SocketAddr)> {
        // std's `accept` leaves the accepted socket in blocking mode on Linux, which `from_std`
        // sets right.
        TcpStream::from_std(&self.io.runtime(), stream).map(|stream| (stream, address))
    }
}
