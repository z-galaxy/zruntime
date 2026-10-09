//! The pipes to a child: [`ChildStdin`], [`ChildStdout`] and [`ChildStderr`].

use std::{
    fmt,
    future::poll_fn,
    io::{self, IoSlice, IoSliceMut},
    pin::Pin,
    process::Stdio,
    task::{Context, Poll, ready},
};
#[cfg(unix)]
use std::{
    io::{PipeReader, PipeWriter, Read, Write},
    os::fd::{AsFd, OwnedFd},
};
#[cfg(windows)]
use std::{
    io::{Read, Write},
    marker::PhantomData,
};

use futures_io::{AsyncRead, AsyncWrite};

#[cfg(unix)]
use crate::Async;
#[cfg(windows)]
use crate::Unblock;
use crate::{Local, Mode, Runtime};

/// The pipe to a child's standard input, to write to.
///
/// It is made for a child whose command asked for it with `Stdio::piped()`, and is found in the
/// [`stdin`](super::Child::stdin) of the [`Child`](super::Child). It is the async counterpart of
/// [`std::process::ChildStdin`]: a write that has to wait leaves the thread to the other tasks
/// instead of blocking it. It implements the `AsyncWrite` trait of [`futures-io`], so the extension
/// traits of [`futures`] write to it.
///
/// Closing it, as the `close` of `AsyncWriteExt` does, flushes it and then closes the pipe, and so
/// does dropping it, but for the flush: either way, the child reads the end of its input once it
/// has read what was written. A child that reads its input to the end can only finish after that,
/// which code that writes to any `AsyncWrite` and closes it once it is done sees to. A write after
/// the close fails with [`BrokenPipe`](io::ErrorKind::BrokenPipe), and a flush or a close after it
/// has nothing left to do. [`Child::status`](super::Child::status) and
/// [`Child::output`](super::Child::output) drop the pipe themselves before they wait.
///
/// On unix the pipe is in non-blocking mode, and a write that finds it full waits for the runtime
/// to report room in it. A write goes to the pipe as it is made, so flushing has nothing to do. On
/// Windows, where the runtime cannot watch a pipe, a write hands its bytes over to blocking work on
/// a thread of the pool of [`unblock()`](crate::unblock()), and a flush waits for the bytes handed
/// over so far to be written. Close or flush before dropping the pipe to learn of an error in
/// those writes: bytes the pipe is dropped with are written all the same, but an error they run
/// into goes unreported.
///
/// The pipe belongs to the runtime the child was spawned on, and its type carries that runtime's
/// flavour: one of a child spawned on a [`LocalRuntime`](crate::LocalRuntime) is a
/// `ChildStdin<Local>`, [`Local`] being the default, and stays on the thread it was made on; one of
/// a child spawned on a [`SharedRuntime`](crate::SharedRuntime) is a `ChildStdin<Shared>`, which
/// may be sent to, and used from, any thread. See the [module documentation](super) for what
/// drives it.
///
/// # Example
///
/// A child that sorts the lines it reads, given its input and then the end of it:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> std::io::Result<()> {
/// use futures::{AsyncReadExt, AsyncWriteExt};
/// use zruntime::{
///     LocalRuntime,
///     process::{Command, Stdio},
/// };
///
/// let runtime = LocalRuntime::new()?;
///
/// runtime.block_on(async {
///     let mut child = Command::new("sort")
///         .stdin(Stdio::piped())
///         .stdout(Stdio::piped())
///         .spawn(&runtime)?;
///
///     let mut stdin = child.stdin.take().expect("stdin is piped");
///     stdin.write_all(b"pear\napple\n").await?;
///     // The child reads the end of its input once the pipe is closed, which dropping it does.
///     drop(stdin);
///
///     let mut sorted = String::new();
///     let mut stdout = child.stdout.take().expect("stdout is piped");
///     stdout.read_to_string(&mut sorted).await?;
///
///     assert_eq!(sorted, "apple\npear\n");
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
///
/// [`futures-io`]: https://docs.rs/futures-io
/// [`futures`]: https://docs.rs/futures
pub struct ChildStdin<M = Local>
where
    M: Mode,
{
    /// The pipe, until it is closed.
    pipe: Option<Pipe<StdinPipe, M>>,
}

impl<M> ChildStdin<M>
where
    M: Mode,
{
    /// The pipe to a child's standard input, on `runtime`.
    pub(super) fn new(runtime: &Runtime<M>, pipe: std::process::ChildStdin) -> io::Result<Self> {
        Ok(Self {
            pipe: Some(Pipe::new(runtime, pipe)?),
        })
    }

    /// Hands the pipe over to be the standard output or error of another child.
    ///
    /// This is how the processes of a pipeline are connected: the child that is given the pipe
    /// writes into the input of the child the pipe was made for, with no byte passing through the
    /// code that holds it. The pipe is written to no more once it is handed over. It is flushed
    /// first: on Windows, the bytes written to it may not have reached the pipe yet, and an error
    /// in writing them is reported here rather than lost.
    ///
    /// The pipe is made blocking again, as a program that is handed one expects it to be: unix has
    /// the mode as a property of the open pipe, which the other child shares, not of the handle.
    ///
    /// What can fail is the flush, the switch back to blocking mode, on unix, and a pipe that has
    /// been closed already, with [`BrokenPipe`](io::ErrorKind::BrokenPipe): there is no pipe left
    /// to hand over.
    pub async fn into_stdio(self) -> io::Result<Stdio> {
        let Some(mut pipe) = self.pipe else {
            return Err(closed());
        };
        poll_fn(|cx| Pin::new(&mut pipe).poll_flush(cx)).await?;

        pipe.into_stdio().await
    }

    /// The pipe, or the error of a write to a pipe that has been closed.
    fn pipe(&mut self) -> io::Result<&mut Pipe<StdinPipe, M>> {
        self.pipe.as_mut().ok_or_else(closed)
    }
}

impl<M> AsyncWrite for ChildStdin<M>
where
    M: Mode,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let pipe = match self.get_mut().pipe() {
            Ok(pipe) => pipe,
            Err(e) => return Poll::Ready(Err(e)),
        };

        Pin::new(pipe).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let pipe = match self.get_mut().pipe() {
            Ok(pipe) => pipe,
            Err(e) => return Poll::Ready(Err(e)),
        };

        Pin::new(pipe).poll_write_vectored(cx, bufs)
    }

    /// A pipe that has been closed has nothing left to flush.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(pipe) = &mut self.get_mut().pipe else {
            return Poll::Ready(Ok(()));
        };

        Pin::new(pipe).poll_flush(cx)
    }

    /// Flushes the pipe and then closes it, which the child reads the end of its input at.
    ///
    /// The pipe is closed once the flush is over, whether or not it succeeded, and the flush's
    /// outcome is what this returns: a close that fails has closed the pipe all the same. A pipe
    /// that has been closed already has nothing left to do.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(pipe) = &mut this.pipe else {
            return Poll::Ready(Ok(()));
        };

        let flushed = ready!(Pin::new(pipe).poll_flush(cx));
        this.pipe = None;

        Poll::Ready(flushed)
    }
}

impl<M> fmt::Debug for ChildStdin<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildStdin").finish_non_exhaustive()
    }
}

/// The pipe from a child's standard output, to read from.
///
/// It is made for a child whose command asked for it with `Stdio::piped()`, and is found in the
/// [`stdout`](super::Child::stdout) of the [`Child`](super::Child). It is the async counterpart of
/// [`std::process::ChildStdout`]: a read that has to wait leaves the thread to the other tasks
/// instead of blocking it. It implements the `AsyncRead` trait of [`futures-io`], so the extension
/// traits of [`futures`] read from it. A read finds the end of the pipe, and returns zero bytes,
/// once every byte the child wrote has been read and no process holds the other end of the pipe
/// open any more: the child exits or closes its standard output, whichever comes first, and any
/// process it started that inherited the pipe does the same.
///
/// Dropping it closes the pipe, and a child that writes to it after that fails to, or is
/// terminated by the signal that says so.
///
/// On unix the pipe is in non-blocking mode, and a read that finds it empty waits for the runtime
/// to report bytes in it. On Windows, where the runtime cannot watch a pipe, a read is blocking
/// work on a thread of the pool of [`unblock()`](crate::unblock()), which reads ahead of what the
/// reader asked for, up to 8 KiB. Such a read runs to its end once it has started, whether or not
/// its future is dropped: a pipe dropped while one is under way stays open, and the thread
/// held, until the read returns, with the next bytes the child writes, which are lost, or with the
/// end of the pipe.
///
/// The pipe belongs to the runtime the child was spawned on, and its type carries that runtime's
/// flavour: one of a child spawned on a [`LocalRuntime`](crate::LocalRuntime) is a
/// `ChildStdout<Local>`, [`Local`] being the default, and stays on the thread it was made on; one
/// of a child spawned on a [`SharedRuntime`](crate::SharedRuntime) is a `ChildStdout<Shared>`,
/// which may be sent to, and used from, any thread. See the [module documentation](super) for what
/// drives it.
///
/// # Example
///
/// The lines a child prints, read one at a time as it prints them:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> std::io::Result<()> {
/// use futures::{AsyncBufReadExt, StreamExt, io::BufReader};
/// use zruntime::{
///     LocalRuntime,
///     process::{Command, Stdio},
/// };
///
/// let runtime = LocalRuntime::new()?;
///
/// runtime.block_on(async {
///     let mut child = Command::new("echo")
///         .arg("hello")
///         .stdout(Stdio::piped())
///         .spawn(&runtime)?;
///
///     let stdout = child.stdout.take().expect("stdout is piped");
///     let mut lines = BufReader::new(stdout).lines();
///     while let Some(line) = lines.next().await {
///         assert_eq!(line?, "hello");
///     }
///
///     assert!(child.status().await?.success());
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
///
/// [`futures-io`]: https://docs.rs/futures-io
/// [`futures`]: https://docs.rs/futures
pub struct ChildStdout<M = Local>
where
    M: Mode,
{
    pipe: Pipe<StdoutPipe, M>,
}

impl<M> ChildStdout<M>
where
    M: Mode,
{
    /// The pipe from a child's standard output, on `runtime`.
    pub(super) fn new(runtime: &Runtime<M>, pipe: std::process::ChildStdout) -> io::Result<Self> {
        Ok(Self {
            pipe: Pipe::new(runtime, pipe)?,
        })
    }

    /// Hands the pipe over to be the standard input of another child.
    ///
    /// This is how the processes of a pipeline are connected: the child that the pipe was made for
    /// writes into it, and the child that is given it reads what that one wrote, with no byte
    /// passing through the code that holds it. The pipe is read from no more once it is handed
    /// over, and what was read from it before is gone from it: on Windows, that includes the bytes
    /// the handle read ahead of what was asked for, which are dropped. A read under way there, one
    /// that was given up on included, is waited for first, which takes until the child writes
    /// again, or the pipe ends, and what it reads is dropped as well.
    ///
    /// The pipe is made blocking again, as a program that is handed one expects it to be: unix has
    /// the mode as a property of the open pipe, which the other child shares, not of the handle.
    ///
    /// What can fail is the switch back to blocking mode, on unix.
    ///
    /// # Example
    ///
    /// `echo hello | cat`, with the output of the second child read back:
    ///
    /// ```
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use futures::AsyncReadExt;
    /// use zruntime::{
    ///     LocalRuntime,
    ///     process::{Command, Stdio},
    /// };
    ///
    /// let runtime = LocalRuntime::new()?;
    ///
    /// runtime.block_on(async {
    ///     let mut echo = Command::new("echo")
    ///         .arg("hello")
    ///         .stdout(Stdio::piped())
    ///         .spawn(&runtime)?;
    ///     let pipe = echo.stdout.take().expect("stdout is piped");
    ///
    ///     let mut cat = Command::new("cat")
    ///         .stdin(pipe.into_stdio().await?)
    ///         .stdout(Stdio::piped())
    ///         .spawn(&runtime)?;
    ///
    ///     let mut text = String::new();
    ///     let mut stdout = cat.stdout.take().expect("stdout is piped");
    ///     stdout.read_to_string(&mut text).await?;
    ///
    ///     assert_eq!(text, "hello\n");
    ///     assert!(echo.status().await?.success());
    ///     assert!(cat.status().await?.success());
    ///     # Ok::<_, std::io::Error>(())
    /// })?;
    /// # Ok(())
    /// # }
    /// # #[cfg(not(unix))]
    /// # fn main() {}
    /// ```
    pub async fn into_stdio(self) -> io::Result<Stdio> {
        self.pipe.into_stdio().await
    }
}

impl<M> AsyncRead for ChildStdout<M>
where
    M: Mode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_read_vectored(cx, bufs)
    }
}

impl<M> fmt::Debug for ChildStdout<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildStdout").finish_non_exhaustive()
    }
}

/// The pipe from a child's standard error, to read from.
///
/// It is made for a child whose command asked for it with `Stdio::piped()`, and is found in the
/// [`stderr`](super::Child::stderr) of the [`Child`](super::Child). It is the async counterpart of
/// [`std::process::ChildStderr`], and is read from exactly as the pipe of the standard output,
/// [`ChildStdout`], is: see there for how reads wait, what the end of the pipe is, and what the
/// type's flavour means.
///
/// A child that writes a lot to both its output and its error blocks, once the pipe of one of
/// them is full, until that pipe is read. A task that reads one of the pipes to its end before it
/// reads the other waits for good where the child is stuck that way, so the two are read at the
/// same time, as [`Child::output`](super::Child::output) reads them.
pub struct ChildStderr<M = Local>
where
    M: Mode,
{
    pipe: Pipe<StderrPipe, M>,
}

impl<M> ChildStderr<M>
where
    M: Mode,
{
    /// The pipe from a child's standard error, on `runtime`.
    pub(super) fn new(runtime: &Runtime<M>, pipe: std::process::ChildStderr) -> io::Result<Self> {
        Ok(Self {
            pipe: Pipe::new(runtime, pipe)?,
        })
    }

    /// Hands the pipe over to be the standard input of another child.
    ///
    /// It is as [`ChildStdout::into_stdio`] is, for the pipe of the standard error.
    pub async fn into_stdio(self) -> io::Result<Stdio> {
        self.pipe.into_stdio().await
    }
}

impl<M> AsyncRead for ChildStderr<M>
where
    M: Mode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_read_vectored(cx, bufs)
    }
}

impl<M> fmt::Debug for ChildStderr<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildStderr").finish_non_exhaustive()
    }
}

/// What a pipe's I/O runs on, by the end of the pipe: the standard library's ends of an anonymous
/// pipe on unix, where the runtime watches them, and the types of a child's own standard streams
/// on Windows, which no runtime can watch and so are handed to blocking work.
#[cfg(unix)]
type StdinPipe = PipeWriter;
#[cfg(unix)]
type StdoutPipe = PipeReader;
#[cfg(unix)]
type StderrPipe = PipeReader;
#[cfg(windows)]
type StdinPipe = std::process::ChildStdin;
#[cfg(windows)]
type StdoutPipe = std::process::ChildStdout;
#[cfg(windows)]
type StderrPipe = std::process::ChildStderr;

/// A pipe to or from a child, with the I/O of the platform it runs on: the three public pipes are
/// this, over the type of their own end.
///
/// An `Async` of the runtime on unix, which watches the pipe for readiness.
#[cfg(unix)]
struct Pipe<T, M>(Async<T, M>)
where
    M: Mode;

/// A pipe to or from a child, with the I/O of the platform it runs on: the three public pipes are
/// this, over the type of their own end.
///
/// An `Unblock` on Windows, which runs each operation as blocking work. The pointer a runtime of
/// the flavour shares its state by is what the type is tied to its flavour through, so that a
/// pipe built on a local runtime stays on its thread on every platform, as the `Async` of unix
/// does of itself.
#[cfg(windows)]
struct Pipe<T, M>(Unblock<T>, PhantomData<M::Ptr<()>>)
where
    M: Mode;

#[cfg(unix)]
impl<T, M> Pipe<T, M>
where
    M: Mode,
    T: AsFd + From<OwnedFd> + Send + Sync + 'static,
{
    /// A pipe on `runtime` that does its I/O on `pipe`, one of std's ends of a pipe to a child.
    ///
    /// The end is switched to non-blocking mode here, which is shared by every descriptor of the
    /// open pipe end but is no concern of the child: what the child has is the other end of the
    /// pipe, an open file description of its own.
    fn new<S>(runtime: &Runtime<M>, pipe: S) -> io::Result<Self>
    where
        S: Into<OwnedFd>,
    {
        let pipe = T::from(pipe.into());
        rustix::io::ioctl_fionbio(&pipe, true)?;

        Ok(Self(Async::from_nonblocking(runtime, pipe)?))
    }
}

#[cfg(windows)]
impl<T, M> Pipe<T, M>
where
    M: Mode,
{
    /// A pipe that does its I/O on `pipe`, one of std's standard streams of a child, as blocking
    /// work.
    ///
    /// Nothing of the runtime is needed for that.
    fn new(_runtime: &Runtime<M>, pipe: T) -> io::Result<Self> {
        Ok(Self(Unblock::new(pipe), PhantomData))
    }
}

#[cfg(unix)]
impl<T, M> Pipe<T, M>
where
    M: Mode,
    T: AsFd + Into<Stdio>,
{
    /// The pipe as a `Stdio` for another child, in blocking mode.
    async fn into_stdio(self) -> io::Result<Stdio> {
        let pipe = self.0.into_inner();
        // The mode belongs to the open pipe, which the other child is given a descriptor of, so it
        // is put back as the child expects it to be.
        rustix::io::ioctl_fionbio(&pipe, false)?;

        Ok(pipe.into())
    }
}

#[cfg(windows)]
impl<T, M> Pipe<T, M>
where
    M: Mode,
    T: Into<Stdio> + Send + 'static,
{
    /// The pipe as a `Stdio` for another child, once the operation in flight is over.
    async fn into_stdio(self) -> io::Result<Stdio> {
        Ok(self.0.into_inner().await.into())
    }
}

#[cfg(unix)]
impl<T, M> AsyncRead for Pipe<T, M>
where
    M: Mode,
    for<'a> &'a T: Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &self.0).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &self.0).poll_read_vectored(cx, bufs)
    }
}

#[cfg(windows)]
impl<T, M> AsyncRead for Pipe<T, M>
where
    M: Mode,
    T: Read + Send + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_read_vectored(cx, bufs)
    }
}

#[cfg(unix)]
impl<T, M> AsyncWrite for Pipe<T, M>
where
    M: Mode,
    for<'a> &'a T: Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &self.0).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &self.0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &self.0).poll_close(cx)
    }
}

#[cfg(windows)]
impl<T, M> AsyncWrite for Pipe<T, M>
where
    M: Mode,
    T: Write + Send + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_close(cx)
    }
}

/// The error of a write to the pipe to a child's input once it has been closed.
fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the pipe to the child's standard input is closed",
    )
}
