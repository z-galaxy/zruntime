//! The wait on unix: `poll(2)` over the sources' descriptors and a pipe that breaks the wait.

use std::{
    io,
    os::fd::{BorrowedFd, OwnedFd},
    time::Duration,
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::{Errno, read, write},
};

use super::{Ready, Want};

pub(crate) struct Poller {
    wake_read: OwnedFd,
    wake_write: OwnedFd,
}

impl Poller {
    pub(crate) fn new() -> io::Result<Self> {
        let (wake_read, wake_write) = wake_pipe()?;

        Ok(Self {
            wake_read,
            wake_write,
        })
    }

    /// Breaks a `wait` in progress or the next one; callable from any thread.
    pub(crate) fn notify(&self) -> io::Result<()> {
        match write(&self.wake_write, &[1]) {
            // A full pipe holds a wake-up already.
            Ok(_) | Err(Errno::AGAIN) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Waits until a wanted source is ready, `notify` is called or `timeout` passes; `None`
    /// waits without limit. The sources are held for the whole call, so no descriptor in the
    /// set can close under it.
    pub(crate) fn wait<S>(
        &self,
        sources: &[(S, Want)],
        timeout: Option<Duration>,
    ) -> io::Result<Vec<Ready>> {
        let mut fds = Vec::with_capacity(sources.len() + 1);
        fds.push(PollFd::new(&self.wake_read, PollFlags::IN));
        for (_source, want) in sources {
            let mut flags = PollFlags::empty();
            if want.readable {
                flags |= PollFlags::IN;
            }
            if want.writable {
                flags |= PollFlags::OUT;
            }
            // SAFETY: the descriptor is the one the source lent when it was registered, which a
            // source keeps open for as long as it lives, as `Source` says and as nothing done
            // through a shared reference to it can undo in safe code; and `sources` holds the
            // source for the length of this call, across which the borrow lives.
            let fd = unsafe { BorrowedFd::borrow_raw(want.descriptor) };
            fds.push(PollFd::from_borrowed_fd(fd, flags));
        }
        // A duration too long for a `Timespec` is as good as no limit.
        let timeout = timeout.and_then(|t| Timespec::try_from(t).ok());
        match poll(&mut fds, timeout.as_ref()) {
            Ok(_) => {}
            Err(Errno::INTR) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        }
        if fds[0].revents().contains(PollFlags::IN) {
            let mut buf = [0u8; 64];
            while read(&self.wake_read, &mut buf).is_ok_and(|n| n == buf.len()) {}
        }

        Ok(sources
            .iter()
            .zip(&fds[1..])
            .filter_map(|((_, want), fd)| {
                let revents = fd.revents();
                let hung_up = revents.intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL);
                let readable = revents.contains(PollFlags::IN) || hung_up;
                let writable = revents.contains(PollFlags::OUT) || hung_up;

                (readable || writable).then_some(Ready {
                    key: want.key,
                    readable,
                    writable,
                })
            })
            .collect())
    }
}

/// A pipe whose two ends are both non-blocking and neither of which a child process inherits.
///
/// Both ends have to be non-blocking: a `notify` that finds the pipe full is to be turned away
/// rather than left waiting for room, and the drain after a wait is to stop at the last byte
/// rather than wait for one more.
#[cfg(not(target_vendor = "apple"))]
fn wake_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    use rustix::pipe::{PipeFlags, pipe_with};

    Ok(pipe_with(PipeFlags::CLOEXEC | PipeFlags::NONBLOCK)?)
}

/// A pipe whose two ends are both non-blocking and neither of which a child process inherits.
///
/// Darwin has no `pipe2`, so both settings are made on the descriptors once the pipe exists.
#[cfg(target_vendor = "apple")]
fn wake_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    use rustix::{
        io::{FdFlags, fcntl_setfd, ioctl_fionbio},
        pipe::pipe,
    };

    let (wake_read, wake_write) = pipe()?;
    for end in [&wake_read, &wake_write] {
        fcntl_setfd(end, FdFlags::CLOEXEC)?;
        ioctl_fionbio(end, true)?;
    }

    Ok((wake_read, wake_write))
}
