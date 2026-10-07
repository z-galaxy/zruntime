//! Async child processes on a [`Runtime`].
//!
//! The module has the shape of [`std::process`], and of `smol::process` of the async-process crate.
//! A [`Command`] builds the process to spawn, and the [`Child`] it spawns has the pipes to the
//! process, [`ChildStdin`], [`ChildStdout`] and [`ChildStderr`], to write to and read from, and
//! futures that wait for the process to exit or to hand over everything it wrote. None of them
//! blocks the thread: where one has to wait, the thread is left to the other tasks in the
//! meantime.
//!
//! The pipes implement the `AsyncWrite` and `AsyncRead` traits of [`futures-io`], so the extension
//! traits of [`futures-lite`] or [`futures-util`] write to and read from them.
//!
//! # The runtime
//!
//! A child is spawned on a runtime, which [`Command::spawn`], [`Command::status`] and
//! [`Command::output`] take as an argument. On unix that runtime's reactor watches the pipes to the
//! child from then on: their operations make progress while some thread is inside
//! [`Runtime::block_on`] on that runtime, or, on a runtime from `SharedRuntime::current`, while the
//! helper thread runs it. On Windows, where the reactor cannot watch a pipe, each read from and
//! write to one runs as blocking work on a thread of the pool of [`unblock()`](crate::unblock()),
//! and makes progress with no thread inside `block_on` at all.
//!
//! The types carry the flavour of the runtime, on every platform. A child spawned on a
//! [`LocalRuntime`] is a `Child<Local>`, `Local` being the default, and stays on the thread it was
//! made on, with its pipes; one spawned on a [`SharedRuntime`] is a `Child<Shared>`, which may be
//! sent to, and used from, any thread.
//!
//! # Waiting for a child
//!
//! [`Child::status`] and [`Child::output`] resolve to the exit status once the process has exited.
//! A process that has exited stays in the system's process table, as a zombie that holds on to its
//! process ID, until its status is collected. Resolving a status does that, and so does
//! [`Child::try_status`], which never waits.
//!
//! # Dropping a child
//!
//! Dropping a [`Child`] closes its pipes, and leaves the process running, as dropping a std `Child`
//! does. [`Command::kill_on_drop`] has the process killed instead. A process that exits with
//! nobody to collect its status stays a zombie, and std's `Child` leaves it so. On unix, a `Child`
//! dropped while its process runs hands the process over to a thread of the pool of
//! [`unblock()`](crate::unblock()) instead, which collects its status once it has exited, unless
//! [`Command::reap_on_drop`] says not to. That thread is held until the process exits, so a
//! program that lets go of many processes that run for long holds as many threads. The pool is the
//! one every piece of blocking work of the process shares, that of `unblock` and of the `fs`
//! module among them, so once such processes hold all of its threads, the rest of that work waits
//! its turn behind them, for as long as they run. Killing them, waiting for them, or turning
//! `reap_on_drop` off, avoids that. Windows leaves no zombie, so there is nothing to collect there,
//! and `reap_on_drop` does nothing.
//!
//! # Example
//!
//! The output of a command, run on a local runtime:
//!
//! ```
//! # #[cfg(unix)]
//! # fn main() -> std::io::Result<()> {
//! use zruntime::{LocalRuntime, process::Command};
//!
//! let runtime = LocalRuntime::new()?;
//!
//! let output = runtime.block_on(Command::new("echo").arg("hello").output(&runtime))?;
//!
//! assert!(output.status.success());
//! assert_eq!(output.stdout, b"hello\n");
//! # Ok(())
//! # }
//! # #[cfg(not(unix))]
//! # fn main() {}
//! ```
//!
//! [`futures-io`]: https://docs.rs/futures-io
//! [`futures-lite`]: https://docs.rs/futures-lite
//! [`futures-util`]: https://docs.rs/futures-util
//! [`LocalRuntime`]: crate::LocalRuntime
//! [`Runtime`]: crate::Runtime
//! [`Runtime::block_on`]: crate::Runtime::block_on
//! [`SharedRuntime`]: crate::SharedRuntime

mod exit;
mod stdio;

use std::{
    ffi::OsStr,
    fmt,
    future::{Future, poll_fn},
    io,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures_io::AsyncRead;

use self::exit::Exit;
pub use self::stdio::{ChildStderr, ChildStdin, ChildStdout};
#[cfg(unix)]
use crate::unblock;
use crate::{Local, Mode, Runtime};
pub use std::process::{ExitStatus, Output, Stdio};

/// A builder of a process to spawn, as [`std::process::Command`] is, whose methods that spawn it
/// take the runtime to spawn it on.
///
/// The command is configured as std's is: with its arguments, its environment, its working
/// directory, and what the standard streams of the process are connected to. A command built
/// with `Command::new` starts from the environment and working directory of this process, and
/// [`as_std`](Command::as_std) gives the std command inside for what only that has: the getters
/// that tell what was configured, and, through [`as_std_mut`](Command::as_std_mut), the extension
/// traits of the platform, such as std's `CommandExt` of unix.
///
/// A command may be spawned any number of times. The standard streams that this builder was not
/// told about are decided by the method that runs the command, afresh each time:
/// [`spawn`](Command::spawn) and [`status`](Command::status) inherit them from this process, and
/// [`output`](Command::output) connects the standard input to nothing and the other two to pipes,
/// which it reads to their ends. So one command may be run by one of them after another, each
/// finding the streams as it expects.
///
/// The process is spawned by the call of `spawn`, `status` or `output` itself, before anything is
/// polled, and runs from then on: the futures of `status` and `output` only wait for it. A spawn
/// that fails is reported by the call of `spawn`, and by the future of the other two.
///
/// # Example
///
/// The exit status of a command, with its output left to this process's own:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> std::io::Result<()> {
/// use zruntime::{LocalRuntime, process::Command};
///
/// let runtime = LocalRuntime::new()?;
///
/// let status = runtime.block_on(Command::new("true").status(&runtime))?;
///
/// assert!(status.success());
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
pub struct Command {
    inner: std::process::Command,
    // Whether each stream was set through this builder: one that was not gets the default of
    // whichever of `spawn`, `status` and `output` runs the command, every time, so that a command
    // run by one of them and then by another does not carry the first one's defaults along.
    stdin: bool,
    stdout: bool,
    stderr: bool,
    kill_on_drop: bool,
    reap_on_drop: bool,
}

impl Command {
    /// A command that runs `program`, with no arguments, the environment and working directory of
    /// this process, and none of the standard streams configured.
    ///
    /// A `program` that has no path in it is looked for on the `PATH` of the process, as std's
    /// [`Command::new`](std::process::Command::new) says.
    pub fn new<S>(program: S) -> Self
    where
        S: AsRef<OsStr>,
    {
        Self::from(std::process::Command::new(program))
    }

    /// Adds an argument to pass to the program.
    pub fn arg<S>(&mut self, arg: S) -> &mut Self
    where
        S: AsRef<OsStr>,
    {
        self.inner.arg(arg);
        self
    }

    /// Adds arguments to pass to the program.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    /// Sets an environment variable of the process.
    ///
    /// Names of environment variables are case-insensitive, though case-preserving, on Windows,
    /// and case-sensitive on every other platform.
    pub fn env<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.env(key, value);
        self
    }

    /// Sets environment variables of the process.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.envs(vars);
        self
    }

    /// Removes an environment variable from the environment of the process, whether this process
    /// has it or the command set it.
    pub fn env_remove<K>(&mut self, key: K) -> &mut Self
    where
        K: AsRef<OsStr>,
    {
        self.inner.env_remove(key);
        self
    }

    /// Clears the environment of the process, which then has none of this process's variables.
    pub fn env_clear(&mut self) -> &mut Self {
        self.inner.env_clear();
        self
    }

    /// Sets the working directory of the process.
    pub fn current_dir<P>(&mut self, dir: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.inner.current_dir(dir);
        self
    }

    /// Configures what the standard input of the process is connected to.
    ///
    /// With `Stdio::piped()`, the child's [`stdin`](Child::stdin) is the pipe to it.
    pub fn stdin<T>(&mut self, cfg: T) -> &mut Self
    where
        T: Into<Stdio>,
    {
        self.stdin = true;
        self.inner.stdin(cfg);
        self
    }

    /// Configures what the standard output of the process is connected to.
    ///
    /// With `Stdio::piped()`, the child's [`stdout`](Child::stdout) is the pipe from it.
    pub fn stdout<T>(&mut self, cfg: T) -> &mut Self
    where
        T: Into<Stdio>,
    {
        self.stdout = true;
        self.inner.stdout(cfg);
        self
    }

    /// Configures what the standard error of the process is connected to.
    ///
    /// With `Stdio::piped()`, the child's [`stderr`](Child::stderr) is the pipe from it.
    pub fn stderr<T>(&mut self, cfg: T) -> &mut Self
    where
        T: Into<Stdio>,
    {
        self.stderr = true;
        self.inner.stderr(cfg);
        self
    }

    /// Sets whether the process is killed when its [`Child`] is dropped, which it is not by
    /// default.
    ///
    /// Without it, dropping the `Child` leaves the process running: see the
    /// [module documentation](self#dropping-a-child). A process killed this way is waited for
    /// like any other that is let go of, which `reap_on_drop` can turn off.
    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Self {
        self.kill_on_drop = kill_on_drop;
        self
    }

    /// Sets whether the status of the process is collected for the [`Child`], once it exits, when
    /// the `Child` is dropped while the process runs, which it is by default.
    ///
    /// A process that exits with nobody to collect its status stays a zombie, which holds on to its
    /// process ID, until something collects it. With this on, dropping a `Child` on unix hands a
    /// process that is still running to a thread of the pool of [`unblock()`](crate::unblock()),
    /// which waits for it to exit and collects the status. That is a thread held until the process
    /// exits, which the rest of the pool's blocking work goes without, and which turning this off
    /// spares, at the price of the zombie: see the [module documentation](self#dropping-a-child).
    ///
    /// This does nothing on Windows, where a process that exits leaves nothing to collect.
    pub fn reap_on_drop(&mut self, reap_on_drop: bool) -> &mut Self {
        self.reap_on_drop = reap_on_drop;
        self
    }

    /// The std command inside this one, for what it has getters for: the program, the arguments,
    /// the environment and the working directory.
    pub fn as_std(&self) -> &std::process::Command {
        &self.inner
    }

    /// The std command inside this one, to configure with what only it has: the extension traits
    /// of std for the platform, such as `uid`, `pre_exec` and `process_group` of unix, or
    /// `creation_flags` of Windows.
    ///
    /// The standard streams are not for this. A stream configured on the std command, not through
    /// the builder this is a part of, is not known to it, and is overridden by the default that the
    /// method that spawns it gives it. Configure them with [`stdin`](Command::stdin),
    /// [`stdout`](Command::stdout) and [`stderr`](Command::stderr).
    pub fn as_std_mut(&mut self) -> &mut std::process::Command {
        &mut self.inner
    }

    /// Spawns the process on `runtime`, and hands back the [`Child`] that is its handle.
    ///
    /// The standard streams that the command was not told about are inherited from this process.
    /// A stream that it was told to pipe is the pipe of that name in the child, ready to be
    /// written to or read from, which is watched by `runtime` on unix and runs on a thread of the
    /// pool of [`unblock()`](crate::unblock()) on Windows.
    ///
    /// The process is running by the time this returns. What can fail is the spawn, for the reasons
    /// std's [`spawn`](std::process::Command::spawn) gives: a program that is not there, say, or
    /// not allowed to run. It can fail too at setting up what `runtime` is to watch: the pipes,
    /// and, where the runtime watches for the exit of the process, the descriptor that tells of
    /// it. The system's poller may refuse to watch any of them, and on Apple's platforms and the
    /// BSDs the system may refuse to make that descriptor, as it does for want of descriptors,
    /// for there is no wait on a thread of the pool there to fall back on. A process that has
    /// been spawned by then is killed, and its status collected, rather than left running with
    /// nobody to hold it.
    pub fn spawn<M>(&mut self, runtime: &Runtime<M>) -> io::Result<Child<M>>
    where
        M: Mode,
    {
        self.start(runtime, Stdio::inherit, Stdio::inherit, Stdio::inherit)
    }

    /// Spawns the process on `runtime`, waits for it to exit, and resolves to its exit status.
    ///
    /// The standard streams that the command was not told about are inherited from this process,
    /// as for [`spawn`](Command::spawn). The pipe to the standard input of the process, if the
    /// command asked for one, is closed before the wait, as [`Child::status`] closes it. A pipe
    /// from the process that nothing reads from may fill, and stop the process from ever exiting:
    /// [`output`](Command::output) is for a process whose output is wanted.
    ///
    /// The process is spawned by this call, before the future is polled, and runs whether or not
    /// the future is: a spawn that fails is the error the future resolves to. Dropping the future
    /// gives up the wait, and drops the child, which leaves the process running unless the command
    /// said [`kill_on_drop`](Command::kill_on_drop).
    pub fn status<M>(
        &mut self,
        runtime: &Runtime<M>,
    ) -> impl Future<Output = io::Result<ExitStatus>> + use<M>
    where
        M: Mode,
    {
        let child = self.start(runtime, Stdio::inherit, Stdio::inherit, Stdio::inherit);

        async move {
            let mut child = child?;

            child.status().await
        }
    }

    /// Spawns the process on `runtime`, and resolves to what it wrote to its standard output and
    /// its standard error, and to its exit status, once it has exited.
    ///
    /// The standard streams that the command was not told about are connected to nothing, for the
    /// standard input, and to a pipe each for the standard output and error, which are read to
    /// their ends. A stream the command was told about otherwise is as it was told, and what is not
    /// piped is not captured: the output holds nothing of it. See [`Child::output`] for how the
    /// pipes are read.
    ///
    /// The process is spawned by this call, before the future is polled, and runs whether or not
    /// the future is: a spawn that fails is the error the future resolves to. Dropping the future
    /// gives up the wait, and drops the child, which leaves the process running unless the command
    /// said [`kill_on_drop`](Command::kill_on_drop).
    pub fn output<M>(
        &mut self,
        runtime: &Runtime<M>,
    ) -> impl Future<Output = io::Result<Output>> + use<M>
    where
        M: Mode,
    {
        let child = self.start(runtime, Stdio::null, Stdio::piped, Stdio::piped);

        async move { child?.output().await }
    }

    /// Spawns the process on `runtime`, with each standard stream that this builder was not told
    /// about connected to what the function for it makes.
    fn start<M>(
        &mut self,
        runtime: &Runtime<M>,
        stdin: fn() -> Stdio,
        stdout: fn() -> Stdio,
        stderr: fn() -> Stdio,
    ) -> io::Result<Child<M>>
    where
        M: Mode,
    {
        // Set every time rather than once, as the std command keeps what a run before this one
        // gave it.
        if !self.stdin {
            self.inner.stdin(stdin());
        }
        if !self.stdout {
            self.inner.stdout(stdout());
        }
        if !self.stderr {
            self.inner.stderr(stderr());
        }

        let child = self.inner.spawn()?;

        Child::new(runtime, child, self.kill_on_drop, self.reap_on_drop)
    }
}

/// A command that wraps `command`, whose standard streams count as not configured, whatever
/// `command` says of them. As for [`Command::new`], it does not kill the process when its
/// [`Child`] is dropped, and does collect its status where the platform needs that.
impl From<std::process::Command> for Command {
    fn from(command: std::process::Command) -> Self {
        Self {
            inner: command,
            stdin: false,
            stdout: false,
            stderr: false,
            kill_on_drop: false,
            reap_on_drop: true,
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

/// A spawned child process, with the pipes to it that its command asked for.
///
/// It is made by [`Command::spawn`], on a runtime that its type carries the flavour of: a
/// `Child<Local>`, [`Local`] being the default, stays on the thread it was made on, and a
/// `Child<Shared>` may be sent to, and used from, any thread. See the
/// [module documentation](self) for what drives it.
///
/// The pipes are fields, `Some` for each of the standard streams that its command asked to
/// `Stdio::piped()` and `None` for each of the others, so a pipe can be taken out and moved
/// elsewhere, as in `let Child { stdout, .. } = child;` or `child.stdout.take()`. A `Child` has no
/// `Drop` of its own to stand in the way, so that works for every field.
///
/// What dropping it does to the process, which is nothing unless the command said otherwise, is
/// described in the [module documentation](self#dropping-a-child).
///
/// # Example
///
/// The output of a child as it is written, then its exit status:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> std::io::Result<()> {
/// use futures_lite::AsyncReadExt;
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
///     let mut greeting = String::new();
///     let mut stdout = child.stdout.take().expect("stdout is piped");
///     stdout.read_to_string(&mut greeting).await?;
///
///     assert_eq!(greeting, "hello\n");
///     assert!(child.status().await?.success());
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
pub struct Child<M = Local>
where
    M: Mode,
{
    /// The pipe to the standard input of the process, if its command asked for one.
    ///
    /// Dropping it closes the pipe, and a child that reads its input to the end reads that end
    /// then. [`status`](Child::status) and [`output`](Child::output) drop it themselves.
    pub stdin: Option<ChildStdin<M>>,
    /// The pipe from the standard output of the process, if its command asked for one.
    pub stdout: Option<ChildStdout<M>>,
    /// The pipe from the standard error of the process, if its command asked for one.
    pub stderr: Option<ChildStderr<M>>,
    // Declared after the pipes, so that they close before the guard kills the process or hands it
    // on.
    guard: Guard,
    exit: Exit<M>,
}

impl<M> Child<M>
where
    M: Mode,
{
    /// The process ID of the child.
    ///
    /// The ID identifies the process until it has exited and its status has been collected, by
    /// [`status`](Child::status) or [`try_status`](Child::try_status): the system may give it to
    /// another process after that.
    pub fn id(&self) -> u32 {
        self.guard.get().id()
    }

    /// Forces the process to exit, and returns without waiting for it to.
    ///
    /// This is `SIGKILL` on unix and `TerminateProcess` on Windows, and what it takes to wait for
    /// the process to be gone is [`status`](Child::status) after it. Killing a process whose status
    /// has been collected already is no error, as it is not for std's
    /// [`kill`](std::process::Child::kill).
    pub fn kill(&mut self) -> io::Result<()> {
        self.guard.get_mut().kill()
    }

    /// The exit status of the process if it has exited, and `None` if it is still running.
    ///
    /// This never waits. Unlike [`status`](Child::status), it leaves the pipe to the standard input
    /// of the process open. It collects the status of a process that has exited, and the status
    /// is the same each time it is asked for after that.
    pub fn try_status(&mut self) -> io::Result<Option<ExitStatus>> {
        self.guard.get_mut().try_wait()
    }

    /// Waits for the process to exit, and resolves to its exit status.
    ///
    /// The pipe to the standard input of the process is dropped first, as std's `wait` drops it: a
    /// process that reads its input to the end would otherwise wait for this pipe to be closed,
    /// and this for the process to exit, for good. The pipes from the process are left as they
    /// are, and a process that fills one that nothing reads from waits for that in turn, so read
    /// them while waiting, or use [`output`](Child::output), which does.
    ///
    /// A process that has exited resolves at once, and so does a call after the status was
    /// collected, to the same status again. The future may be dropped, which loses nothing: the
    /// next call takes up the wait, and a process that exited meanwhile is found as it is.
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        drop(self.stdin.take());

        loop {
            if let Some(status) = self.guard.get_mut().try_wait()? {
                return Ok(status);
            }
            // The wait is for the exit alone, and leaves the status to be collected by the
            // `try_wait` above, so a wait that ends is followed by a look at the status, and by
            // another wait only in the unlikely event that it is not there.
            self.exit.wait(self.guard.get()).await?;
        }
    }

    /// Collects what the process writes to its standard output and its standard error, and waits
    /// for it to exit.
    ///
    /// The pipe to the standard input of the process is dropped first, so that a process that
    /// reads its input to the end is not left waiting for this to close it. Then the pipes from
    /// the process, those it has, are read to their ends, both at the same time: a process that
    /// fills one while this waits to read the other would otherwise never get to write the rest.
    /// What is not piped is not captured, and the output holds nothing of it. Only then is the
    /// process waited for, as [`status`](Child::status) waits.
    ///
    /// The pipes are those of the child: a command that is run for its output is better off with
    /// [`Command::output`], which pipes the right ones.
    ///
    /// If reading a pipe fails, so does this, with the error, and the process is not waited for.
    pub async fn output(mut self) -> io::Result<Output> {
        drop(self.stdin.take());

        let mut stdout = Capture::new(self.stdout.take());
        let mut stderr = Capture::new(self.stderr.take());
        poll_fn(|cx| {
            // Both are polled each time, so that each is woken for its own pipe.
            let stdout = stdout.poll_end(cx);
            let stderr = stderr.poll_end(cx);

            match (stdout, stderr) {
                (Poll::Ready(Err(error)), _) | (_, Poll::Ready(Err(error))) => {
                    Poll::Ready(Err(error))
                }
                (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                _ => Poll::Pending,
            }
        })
        .await?;

        let status = self.status().await?;

        Ok(Output {
            status,
            stdout: stdout.data,
            stderr: stderr.data,
        })
    }

    /// A child for `child`, the process just spawned on `runtime`.
    ///
    /// The pipes of `child`, and where the runtime watches for the exit of its process, the
    /// descriptor that tells of it, are put under the watch of `runtime`, and what the child is to
    /// do when it is dropped is set to `kill_on_drop` and `reap_on_drop`.
    fn new(
        runtime: &Runtime<M>,
        child: std::process::Child,
        kill_on_drop: bool,
        reap_on_drop: bool,
    ) -> io::Result<Self> {
        // The process is killed, and its status collected, should anything below fail, so that a
        // spawn that fails leaves no process behind, with nobody holding it. What the command
        // asked for applies once the child is whole.
        let mut guard = Guard::new(child, true, true);
        let child = guard.get_mut();
        let stdin = child
            .stdin
            .take()
            .map(|pipe| ChildStdin::new(runtime, pipe))
            .transpose()?;
        let stdout = child
            .stdout
            .take()
            .map(|pipe| ChildStdout::new(runtime, pipe))
            .transpose()?;
        let stderr = child
            .stderr
            .take()
            .map(|pipe| ChildStderr::new(runtime, pipe))
            .transpose()?;
        let exit = Exit::new(runtime, child)?;
        guard.kill_on_drop = kill_on_drop;
        guard.reap_on_drop = reap_on_drop;

        Ok(Self {
            stdin,
            stdout,
            stderr,
            guard,
            exit,
        })
    }
}

impl<M> fmt::Debug for Child<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Child")
            .field("id", &self.id())
            .field("stdin", &self.stdin)
            .field("stdout", &self.stdout)
            .field("stderr", &self.stderr)
            .finish_non_exhaustive()
    }
}

/// The std child, and what is to be done with its process when the `Child` is dropped.
///
/// A guard of its own rather than a `Drop` of `Child`, which would keep the pipes from being moved
/// out of the `Child` by the code that holds it.
struct Guard {
    /// Always there, but while the guard is dropped, which takes it out to hand it on.
    child: Option<std::process::Child>,
    /// Whether the process is killed.
    kill_on_drop: bool,
    /// Whether a process that is still running is waited for, so that its status is collected
    /// once it has exited, where the platform has a status to collect.
    reap_on_drop: bool,
}

impl Guard {
    /// A guard of `child`, which kills its process on drop if `kill_on_drop` says so, and hands it
    /// on to be collected if `reap_on_drop` does.
    fn new(child: std::process::Child, kill_on_drop: bool, reap_on_drop: bool) -> Self {
        Self {
            child: Some(child),
            kill_on_drop,
            reap_on_drop,
        }
    }

    /// The std child.
    fn get(&self) -> &std::process::Child {
        let Some(child) = &self.child else {
            unreachable!("the child is there until the guard is dropped");
        };

        child
    }

    /// The std child, to kill it, or to look at whether it has exited.
    fn get_mut(&mut self) -> &mut std::process::Child {
        let Some(child) = &mut self.child else {
            unreachable!("the child is there until the guard is dropped");
        };

        child
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };

        if self.kill_on_drop {
            // It may have exited already, or be one that cannot be killed, and either way there is
            // nobody left to tell.
            let _ = child.kill();
        }
        if self.reap_on_drop {
            reap(child);
        }
    }
}

/// Sees to it that the status of `child`'s process is collected, once it has exited.
///
/// A process that has exited already is collected by the look at it. One that is still running is
/// handed to blocking work that waits for it, and the work runs to its end whether or not anything
/// waits for it.
#[cfg(unix)]
fn reap(mut child: std::process::Child) {
    if matches!(child.try_wait(), Ok(None)) {
        drop(unblock(move || child.wait()));
    }
}

/// A process that exits leaves no status on Windows to be collected, so there is nothing to do.
#[cfg(windows)]
fn reap(child: std::process::Child) {
    drop(child);
}

/// What [`Child::output`] reads one of the child's pipes into.
struct Capture<R> {
    /// The pipe, until it has been read to its end.
    reader: Option<R>,
    /// What the pipe held so far.
    data: Vec<u8>,
    /// What a read of the pipe reads into.
    chunk: Box<[u8]>,
}

impl<R> Capture<R>
where
    R: AsyncRead + Unpin,
{
    /// A capture of `reader`, which is none for a pipe the child does not have: such a capture is
    /// at the end of its pipe from the start.
    fn new(reader: Option<R>) -> Self {
        let chunk = if reader.is_some() {
            vec![0; CHUNK].into_boxed_slice()
        } else {
            Box::default()
        };

        Self {
            reader,
            data: Vec::new(),
            chunk,
        }
    }

    /// Reads the pipe until it has nothing more at the moment, or has ended, and arranges for
    /// `cx`'s waker to be woken where it has nothing more yet.
    ///
    /// Ready once the pipe has ended, and every call after that finds it so, or has failed.
    fn poll_end(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(reader) = &mut self.reader else {
            return Poll::Ready(Ok(()));
        };

        loop {
            match Pin::new(&mut *reader).poll_read(cx, &mut self.chunk) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => {
                    self.reader = None;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(read)) => self.data.extend_from_slice(&self.chunk[..read]),
                Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
    }
}

/// How much a read of a pipe takes at most: as much as the reads of `Unblock` do by default.
const CHUNK: usize = 8 * 1024;
