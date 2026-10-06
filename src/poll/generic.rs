//! The wait on any unix the other pollers do not serve: `poll(2)` over the sources' descriptors and
//! a pipe that breaks the wait.
//!
//! `poll(2)` takes the whole set of descriptors on every call, so this poller keeps the list it is
//! told about and hands it over afresh on each wait.

use std::{io, os::fd::BorrowedFd, time::Duration};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::Errno,
};

use super::{Directions, RawSource, Ready, list::List, pipe::Pipe};

pub(crate) struct Poller {
    wake: Pipe,
    list: List,
}

impl Poller {
    /// Whether a change reaches a wait under way: it does not, as each wait copies the list as it
    /// starts.
    pub(crate) const LIVE: bool = false;

    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            wake: Pipe::new()?,
            list: List::new(),
        })
    }

    /// Breaks a `wait` in progress or the next one; callable from any thread.
    pub(crate) fn notify(&self) -> io::Result<()> {
        self.wake.notify()
    }

    /// Takes the source `key` in, watched for nothing yet.
    pub(crate) fn add(&self, _key: usize, _descriptor: RawSource) -> io::Result<()> {
        self.list.add();

        Ok(())
    }

    /// Watches the source `key` for `to` rather than for `from`, from the next wait on.
    pub(crate) fn modify(
        &self,
        key: usize,
        descriptor: RawSource,
        _from: Directions,
        to: Directions,
    ) -> io::Result<()> {
        self.list.modify(key, descriptor, to);

        Ok(())
    }

    /// Lets go of the source `key`, watched for `from`, from the next wait on.
    pub(crate) fn delete(
        &self,
        key: usize,
        _descriptor: RawSource,
        _from: Directions,
    ) -> io::Result<()> {
        self.list.delete(key);

        Ok(())
    }

    /// Waits until a watched source is ready, `notify` is called or `timeout` passes; `None`
    /// waits without limit.
    pub(crate) fn wait(&self, timeout: Option<Duration>) -> io::Result<Vec<Ready>> {
        let watched = self.list.snapshot();
        let mut fds = Vec::with_capacity(watched.len() + 1);
        fds.push(PollFd::from_borrowed_fd(
            self.wake.read_end(),
            PollFlags::IN,
        ));
        for &(_key, descriptor, directions) in &watched {
            let mut flags = PollFlags::empty();
            if directions.readable {
                flags |= PollFlags::IN;
            }
            if directions.writable {
                flags |= PollFlags::OUT;
            }
            // SAFETY: the descriptor is the one a source lent when it was registered, which the
            // source keeps open for as long as it lives, as `Source` says and as nothing done
            // through a shared reference to it can undo in safe code. The reactor holds the
            // source while it is in the list, and keeps it past its registration, until this
            // wait returns, where the registration goes while this wait runs: the borrow, which
            // lives no longer than this call, is of an open descriptor throughout.
            let fd = unsafe { BorrowedFd::borrow_raw(descriptor) };
            fds.push(PollFd::from_borrowed_fd(fd, flags));
        }
        // The reactor bounds every timeout to `MAX_TIMEOUT`, which a `Timespec` holds.
        let timeout = timeout.and_then(|t| Timespec::try_from(t).ok());
        match poll(&mut fds, timeout.as_ref()) {
            Ok(_) => {}
            Err(Errno::INTR) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        }
        if fds[0].revents().contains(PollFlags::IN) {
            self.wake.drain();
        }

        Ok(watched
            .iter()
            .zip(&fds[1..])
            .filter_map(|(&(key, _, _), fd)| {
                let revents = fd.revents();
                let hung_up = revents.intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL);
                let directions = Directions {
                    readable: revents.contains(PollFlags::IN) || hung_up,
                    writable: revents.contains(PollFlags::OUT) || hung_up,
                };

                (!directions.is_empty()).then_some(Ready {
                    key,
                    directions,
                    dropped: Directions::NONE,
                })
            })
            .collect())
    }
}
