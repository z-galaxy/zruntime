//! The wait everywhere but on Apple's platforms and the BSDs: blocking work on a thread of the pool
//! of [`unblock()`](crate::unblock()).
//!
//! The work is a thread held for as long as the child runs, so it is started by the first wait for
//! the exit rather than by the child's creation, and a child that nobody waits for holds none.

#[cfg(windows)]
use std::os::windows::io::{AsHandle, AsRawHandle};
use std::{io, marker::PhantomData};

#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{Pid, WaitId, WaitIdOptions, waitid},
};
#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::WAIT_FAILED,
    System::Threading::{INFINITE, WaitForSingleObject},
};

use crate::{BlockingWork, Mode, Runtime, unblock};

/// What tells a [`Child`](crate::process::Child) that its process has exited: see the
/// [module documentation](super) of what every platform's does.
pub(crate) struct Exit<M>
where
    M: Mode,
{
    /// The blocking work that waits, from the first wait until it has resolved.
    ///
    /// Kept across a wait that is given up, so that the next one takes up the same work instead of
    /// starting another thread's worth of it.
    work: Option<BlockingWork<io::Result<()>>>,
    /// Ties the type to its runtime's flavour, whose pointer a local runtime's handles cannot send
    /// to another thread, as the pipes of the child cannot be sent either.
    _mode: PhantomData<M::Ptr<()>>,
}

impl<M> Exit<M>
where
    M: Mode,
{
    /// The wait for `child`, a process spawned on `runtime`, to exit, not started yet.
    ///
    /// Nothing is started here: the first call of [`wait`](Exit::wait) starts the work, which gets
    /// the child from that call, so neither the runtime nor the child is used yet. The result is
    /// there for a wait that has something to set up, and can fail at it. This one has nothing,
    /// and never fails.
    pub(crate) fn new(_runtime: &Runtime<M>, _child: &std::process::Child) -> io::Result<Self> {
        Ok(Self {
            work: None,
            _mode: PhantomData,
        })
    }

    /// Waits for `child`, the process this was made for, to exit, without collecting its status.
    ///
    /// Resolves once the process has exited, or to the error that stopped the wait: a child that
    /// some other code has collected the status of, say. The first call starts the work that
    /// waits, and dropping the future leaves that work running, for the next call to pick up where
    /// the dropped one left off, so that calls given up on do not pile up threads.
    pub(crate) async fn wait(&mut self, child: &std::process::Child) -> io::Result<()> {
        let running = match &mut self.work {
            Some(running) => running,
            none @ None => none.insert(start(child)?),
        };
        let outcome = running.await;
        // Only once the work has resolved, so that a future dropped before then leaves it for the
        // next call.
        self.work = None;

        outcome
    }
}

/// Blocking work that returns once `child` has exited, with the child left as it is.
///
/// `waitid` with `WNOWAIT` leaves the child waitable for the `try_wait` of std's `Child`, which
/// collects it: the work never reaps, so the process ID is not free to be reused for as long as
/// the child's owner holds the `Child`.
#[cfg(unix)]
fn start(child: &std::process::Child) -> io::Result<BlockingWork<io::Result<()>>> {
    let pid = Pid::from_child(child);

    Ok(unblock(move || {
        loop {
            match waitid(
                WaitId::Pid(pid),
                WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
            ) {
                // A call a signal interrupted is made again: the child has not exited yet.
                Err(Errno::INTR) => {}
                Ok(_) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
    }))
}

/// Blocking work that returns once `child` has exited.
///
/// The work holds a handle of its own on the process, which keeps it to wait on whatever becomes
/// of the `Child` it was made from: the work runs to its end whether or not anything waits for its
/// outcome.
#[cfg(windows)]
fn start(child: &std::process::Child) -> io::Result<BlockingWork<io::Result<()>>> {
    let handle = child.as_handle().try_clone_to_owned()?;

    Ok(unblock(move || {
        // SAFETY: `WaitForSingleObject` is given a handle that `handle`, which this closure owns,
        // keeps open for the call, and a timeout that has it wait for as long as it takes. It
        // reads and writes nothing else.
        let waited = unsafe { WaitForSingleObject(handle.as_raw_handle(), INFINITE) };
        if waited == WAIT_FAILED {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }))
}
