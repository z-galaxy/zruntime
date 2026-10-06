//! [`Async`]: a non-blocking I/O handle that waits for its readiness on a runtime, and the I/O
//! traits it implements.

#[cfg(all(unix, any(feature = "tcp", feature = "udp", feature = "unix")))]
use std::os::fd::AsFd as AsSource;
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
#[cfg(all(windows, any(feature = "tcp", feature = "udp")))]
use std::os::windows::io::AsSocket as AsSource;
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};
use std::{
    fmt,
    io::{self, IoSlice, IoSliceMut, Read, Write},
    pin::Pin,
    task::{Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::{
    FIONBIO, SOCKET, SOCKET_ERROR, WSAGetLastError, ioctlsocket,
};

use crate::{Interest, Local, Mode, Readiness, Registration, Runtime, Source, mode, reactor};

/// The async counterpart of a non-blocking I/O handle, as smol has in `smol::Async`.
///
/// It wraps a source the runtime can watch for readiness, and runs I/O on it without blocking the
/// thread: an operation that would block waits for the runtime to report the source ready instead,
/// leaving the thread to the other tasks in the meantime. On unix the source is anything with a
/// file descriptor: a pipe, a terminal, an eventfd, an inotify instance, the standard I/O of a
/// child process, or a socket of a type the `net` module has none for. On Windows it is a socket,
/// and nothing else, as that is all the runtime's `select` can watch there. The handle also serves
/// to give a descriptor to another library that does its own I/O: [`readable`](Async::readable)
/// and [`writable`](Async::writable) wait for the readiness that library's operation needs, with
/// no operation of their own.
///
/// The handle runs on the runtime given to the constructor that made it, whose reactor watches
/// the source from then on. Its operations make progress while some thread is inside
/// [`Runtime::block_on`] on that runtime, or, on a runtime from `SharedRuntime::current`, while
/// the helper thread runs it. The handle's type carries the flavour of that runtime: one built on
/// a [`LocalRuntime`](crate::LocalRuntime) is an `Async<T, Local>`, [`Local`] being
/// the default, and stays on the thread it was made on; one built on a
/// [`SharedRuntime`](crate::SharedRuntime) is an `Async<T, Shared>`, which may be sent to, and
/// used from, any thread. A shared runtime watches only a source that is `Send` and `Sync`, as
/// [`Source`] says.
///
/// # Waiting
///
/// [`readable`](Async::readable), [`writable`](Async::writable),
/// [`read_with`](Async::read_with) and [`write_with`](Async::write_with) let any number of tasks
/// wait at once, in either direction, each through a shared reference to the handle.
///
/// The poll-based [`poll_read_with`](Async::poll_read_with) and
/// [`poll_write_with`](Async::poll_write_with), and the `AsyncRead` and `AsyncWrite`
/// implementations built on them, keep one waiting task per direction instead: where a second task
/// waits to read, say, it takes the first one's place, which is then never woken. Tasks that share
/// a direction through them take turns, behind a lock of their own.
///
/// # The I/O traits
///
/// `Async<T, M>` implements the `AsyncRead` and `AsyncWrite` traits of [`futures-io`], so the
/// extension traits of [`futures-lite`] or [`futures-util`] read from and write to it. So does a
/// shared reference to it, `&Async<T, M>`, which lets a reader and a writer share one handle. Both
/// are there wherever `&T` implements `Read`, for the first trait, and `Write`, for the second,
/// as it does for a `File`, for `std::io::PipeReader` and `PipeWriter`, and for std's stream
/// sockets, `TcpStream` and `UnixStream`: a listener or a datagram socket implements neither.
///
/// The source sits behind a pointer that the runtime's reactor shares, so no `&mut T` is ever
/// handed out. A type that implements `Read` or `Write` only for `&mut self`, such as
/// `std::process::ChildStdout` and `ChildStdin`, has to be converted first. On unix that is
/// through `OwnedFd`: a `ChildStdout` becomes a `PipeReader`, and a `ChildStdin` a `PipeWriter`.
///
/// A regular file is always reported ready by the system's poll, so a read of one blocks the thread
/// whatever the mode it is in. A regular file is not what this is for: `zruntime::Unblock` and
/// `zruntime::fs` run each operation on a thread of their own instead.
///
/// # Example
///
/// The reading end of a pipe, read to its end while a thread writes to the other end:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> std::io::Result<()> {
/// use std::{io::Write, thread};
///
/// use futures_lite::AsyncReadExt;
/// use zruntime::{Async, LocalRuntime};
///
/// let runtime = LocalRuntime::new()?;
/// let (reader, mut writer) = std::io::pipe()?;
/// // The reading end is switched to non-blocking mode, and watched by the runtime from here on.
/// let mut reader = Async::new(&runtime, reader)?;
///
/// // The writer is dropped as the thread ends, which is the end of what the reader reads.
/// let writing = thread::spawn(move || writer.write_all(b"hello"));
///
/// let mut message = Vec::new();
/// runtime.block_on(reader.read_to_end(&mut message))?;
/// writing.join().expect("the writer does not panic")?;
///
/// assert_eq!(message, b"hello");
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
///
/// [`futures-io`]: https://docs.rs/futures-io
/// [`futures-lite`]: https://docs.rs/futures-lite
/// [`futures-util`]: https://docs.rs/futures-util
pub struct Async<T, M = Local>
where
    M: Mode,
{
    // Fields drop in the order they are declared: the watch ends before this handle on the
    // source goes.
    registration: Registration<M>,
    io: M::Ptr<T>,
}

impl<T, M> Async<T, M>
where
    M: Mode,
{
    /// An `Async` on `runtime` that does its I/O on `io`, which this switches to non-blocking
    /// mode.
    ///
    /// On unix the mode belongs to the open file description rather than to a handle on it, so it
    /// is shared with every duplicate of the descriptor, in this process or in another: the
    /// standard input, where it is a terminal, is shared with the shell that started the program,
    /// say, and is left in non-blocking mode for it. A caller that must leave the mode alone for
    /// them puts the source in non-blocking mode itself, where it knows that is safe, and makes
    /// the handle with [`new_nonblocking`](Async::new_nonblocking).
    ///
    /// What can fail is the switch to non-blocking mode, and the runtime taking the source under
    /// its watch. A runtime watches a descriptor through one handle at a time, and turns away a
    /// source whose descriptor it watches already with
    /// [`AlreadyExists`](io::ErrorKind::AlreadyExists). On Windows it watches a limited number of
    /// sockets: at most 1023 at a time, which its reactor waits on in a single `select` call.
    pub fn new(runtime: &Runtime<M>, io: T) -> io::Result<Self>
    where
        T: Source<M>,
    {
        set_nonblocking(&io)?;

        Self::new_nonblocking(runtime, io)
    }

    /// An `Async` on `runtime` that does its I/O on `io`, as it is.
    ///
    /// `io` must be in non-blocking mode already, which is the caller's to see to: each operation
    /// is tried on the source at once, and waits for readiness only once it reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), so an operation on a blocking source with nothing
    /// ready would hold up the thread it runs on, and every other task with it.
    ///
    /// What can fail is the runtime taking the source under its watch. A runtime watches a
    /// descriptor through one handle at a time, and turns away a source whose descriptor it
    /// watches already with [`AlreadyExists`](io::ErrorKind::AlreadyExists). On Windows it watches
    /// a limited number of sockets: at most 1023 at a time, which its reactor waits on in a single
    /// `select` call.
    pub fn new_nonblocking(runtime: &Runtime<M>, io: T) -> io::Result<Self>
    where
        T: Source<M>,
    {
        let io = M::new_ptr(io);
        let registration = reactor::register::<M>(
            &runtime.core,
            <T as mode::sealed::IntoSource<M>>::source_ptr(io.clone()),
        )?;
        // Asked for once the source is in the reactor's map, so that a helper starting here takes
        // it into its very first wait.
        M::ensure_progress(&runtime.core);

        Ok(Self { registration, io })
    }

    /// The source.
    pub fn get_ref(&self) -> &T {
        &self.io
    }

    /// Stops watching the source, and hands it back, still in non-blocking mode.
    ///
    /// On a shared runtime this may wait for a moment, while a thread driving the runtime returns
    /// from a wait that watched the source.
    pub fn into_inner(self) -> T {
        let Self { registration, io } = self;
        // The watch ends first: nothing the reactor holds is left to share the source then, but
        // for a wait under way on another thread, which this lets go of by breaking it.
        drop(registration);

        M::into_inner(io)
    }

    /// A future that completes once the source is ready to be read from, without running any
    /// operation on it.
    ///
    /// Any number of tasks may wait at once. Readiness is a hint rather than a promise: another
    /// task may take the bytes before this one gets to them, so an operation run after the wait
    /// still has to expect [`WouldBlock`](io::ErrorKind::WouldBlock), and to wait again when it
    /// gets one, as [`read_with`](Async::read_with) does. The wait fails where the runtime cannot
    /// start to watch the source, which the system's poller may refuse to. See
    /// [`Registration::ready`] for what the wait is.
    pub fn readable(&self) -> Readiness<'_, M> {
        self.registration.ready(Interest::Readable)
    }

    /// A future that completes once the source is ready to be written to, without running any
    /// operation on it.
    ///
    /// Any number of tasks may wait at once. Readiness is a hint rather than a promise: another
    /// task may take the room before this one gets to it, so an operation run after the wait
    /// still has to expect [`WouldBlock`](io::ErrorKind::WouldBlock), and to wait again when it
    /// gets one, as [`write_with`](Async::write_with) does. The wait fails where the runtime cannot
    /// start to watch the source, which the system's poller may refuse to. See
    /// [`Registration::ready`] for what the wait is.
    pub fn writable(&self) -> Readiness<'_, M> {
        self.registration.ready(Interest::Writable)
    }

    /// Runs `operation` on the source until it no longer reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), waiting for the source to be readable in
    /// between.
    ///
    /// Resolves to the first success `operation` returns, and to the first error other than
    /// `WouldBlock`, or to the error of a wait for readiness, as [`readable`](Async::readable)
    /// says. A call the kernel interrupted is made again straight away.
    ///
    /// `operation` must not block: the source is in non-blocking mode so that a call on it
    /// returns at once, and `operation` runs on the thread that every task of the runtime shares.
    ///
    /// Any number of tasks may do this at once, on one handle. Dropping the future gives up the
    /// wait, and leaves no operation half done: each call of `operation` runs to its end before
    /// the future can be dropped.
    pub async fn read_with<R>(
        &self,
        mut operation: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            match operation(&self.io) {
                // A call the kernel interrupted is made again straight away: that is not a
                // readiness question, so it never reaches the reactor.
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                result => return result,
            }
            self.readable().await?;
        }
    }

    /// Runs `operation` on the source until it no longer reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), waiting for the source to be writable in
    /// between.
    ///
    /// Resolves to the first success `operation` returns, a partial write included, and to the
    /// first error other than `WouldBlock`, or to the error of a wait for readiness, as
    /// [`writable`](Async::writable) says. A call the kernel interrupted is made again straight
    /// away.
    ///
    /// `operation` must not block: the source is in non-blocking mode so that a call on it
    /// returns at once, and `operation` runs on the thread that every task of the runtime shares.
    ///
    /// Any number of tasks may do this at once, on one handle. Dropping the future gives up the
    /// wait, and leaves no operation half done: each call of `operation` runs to its end before
    /// the future can be dropped.
    pub async fn write_with<R>(
        &self,
        mut operation: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            match operation(&self.io) {
                // A call the kernel interrupted is made again straight away: that is not a
                // readiness question, so it never reaches the reactor.
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                result => return result,
            }
            self.writable().await?;
        }
    }

    /// [`read_with`](Async::read_with), polled: what an implementation of a poll-based trait runs
    /// its read through.
    ///
    /// Runs `operation` on the source, and where it reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), arranges for `cx`'s waker to be woken once the
    /// source is readable and returns [`Poll::Pending`].
    ///
    /// Unlike `read_with`, this keeps one waiting task for reading: a second one waiting takes
    /// the first one's place, which is then never woken. See [`Registration::poll_io`].
    pub fn poll_read_with<R>(
        &self,
        cx: &mut Context<'_>,
        operation: impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Interest::Readable, operation)
    }

    /// [`write_with`](Async::write_with), polled: what an implementation of a poll-based trait
    /// runs its write through.
    ///
    /// Runs `operation` on the source, and where it reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), arranges for `cx`'s waker to be woken once the
    /// source is writable and returns [`Poll::Pending`].
    ///
    /// Unlike `write_with`, this keeps one waiting task for writing: a second one waiting takes
    /// the first one's place, which is then never woken. See [`Registration::poll_io`].
    pub fn poll_write_with<R>(
        &self,
        cx: &mut Context<'_>,
        operation: impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Interest::Writable, operation)
    }

    /// An `Async` on `runtime` that does its I/O on `io`, as it is: the constructor the sockets of
    /// the `net` module are built with.
    ///
    /// Those sockets are generic over the flavour, and a [`Source<M>`](Source) bound cannot be met
    /// for an abstract `M`, so this takes what both flavours need of a source instead, `Send` and
    /// `Sync` included. `io` must be in non-blocking mode already, as for
    /// [`new_nonblocking`](Async::new_nonblocking).
    #[cfg(any(feature = "tcp", feature = "udp", all(feature = "unix", unix)))]
    pub(crate) fn from_nonblocking(runtime: &Runtime<M>, io: T) -> io::Result<Self>
    where
        T: AsSource + Send + Sync + 'static,
    {
        let io = M::new_ptr(io);
        let registration = reactor::register::<M>(&runtime.core, M::source_ptr(io.clone()))?;
        // Asked for once the source is in the reactor's map, so that a helper starting here takes
        // it into its very first wait.
        M::ensure_progress(&runtime.core);

        Ok(Self { registration, io })
    }

    /// A handle on the runtime the source is registered on.
    #[cfg(any(feature = "tcp", all(feature = "unix", unix)))]
    pub(crate) fn runtime(&self) -> Runtime<M> {
        self.registration.runtime()
    }

    /// Runs `operation` on the source while it is ready for `interest`: see
    /// [`Registration::poll_io`].
    ///
    /// A call the kernel interrupts is made again straight away: that is not a readiness
    /// question, so it never reaches the reactor.
    fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        let io = &*self.io;

        self.registration.poll_io(cx, interest, || {
            loop {
                match operation(io) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    result => return result,
                }
            }
        })
    }
}

impl<T, M> fmt::Debug for Async<T, M>
where
    M: Mode,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Async")
            .field("io", self.get_ref())
            .finish_non_exhaustive()
    }
}

#[cfg(unix)]
impl<T, M> AsFd for Async<T, M>
where
    M: Mode,
    T: AsFd,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.get_ref().as_fd()
    }
}

#[cfg(unix)]
impl<T, M> AsRawFd for Async<T, M>
where
    M: Mode,
    T: AsRawFd,
{
    fn as_raw_fd(&self) -> RawFd {
        self.get_ref().as_raw_fd()
    }
}

#[cfg(windows)]
impl<T, M> AsSocket for Async<T, M>
where
    M: Mode,
    T: AsSocket,
{
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.get_ref().as_socket()
    }
}

#[cfg(windows)]
impl<T, M> AsRawSocket for Async<T, M>
where
    M: Mode,
    T: AsRawSocket,
{
    fn as_raw_socket(&self) -> RawSocket {
        self.get_ref().as_raw_socket()
    }
}

impl<T, M> AsyncRead for Async<T, M>
where
    M: Mode,
    for<'a> &'a T: Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_read_vectored(cx, bufs)
    }
}

/// Closing flushes, and leaves the source open: shutting a socket down, or closing the
/// descriptor, is the source's business, and is done by dropping the `Async` or by taking the
/// source back with [`into_inner`](Async::into_inner).
impl<T, M> AsyncWrite for Async<T, M>
where
    M: Mode,
    for<'a> &'a T: Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self).poll_close(cx)
    }
}

impl<T, M> AsyncRead for &Async<T, M>
where
    M: Mode,
    for<'a> &'a T: Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_read_with(cx, |mut io| io.read(buf))
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_read_with(cx, |mut io| io.read_vectored(bufs))
    }
}

/// Closing flushes, and leaves the source open: shutting a socket down, or closing the
/// descriptor, is the source's business, and is done by dropping the `Async` or by taking the
/// source back with [`into_inner`](Async::into_inner).
impl<T, M> AsyncWrite for &Async<T, M>
where
    M: Mode,
    for<'a> &'a T: Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(cx, |mut io| io.write(buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(cx, |mut io| io.write_vectored(bufs))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_write_with(cx, |mut io| io.flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

/// Switches `io` to non-blocking mode.
#[cfg(unix)]
fn set_nonblocking<T>(io: &T) -> io::Result<()>
where
    T: AsFd,
{
    Ok(rustix::io::ioctl_fionbio(io, true)?)
}

/// Switches `io` to non-blocking mode.
#[cfg(windows)]
fn set_nonblocking<T>(io: &T) -> io::Result<()>
where
    T: AsSocket,
{
    let mut enable = 1u32;
    // SAFETY: `ioctlsocket` is given a socket that `io` keeps open for the call, and a pointer to
    // a `u32` that outlives it, which is all that `FIONBIO` reads or writes.
    let result = unsafe {
        ioctlsocket(
            io.as_socket().as_raw_socket() as SOCKET,
            FIONBIO,
            &mut enable,
        )
    };
    if result == SOCKET_ERROR {
        // SAFETY: `WSAGetLastError` takes nothing and reads this thread's last Winsock error.
        return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
    }

    Ok(())
}
