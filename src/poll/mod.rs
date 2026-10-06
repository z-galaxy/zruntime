//! The wait a platform is asked for, and what it is told to watch.
//!
//! Each implementation watches a list of sources for the length of one wait and reports which of
//! them were found ready, and each keeps a channel of its own that a `notify` writes to, so that
//! a wait can be broken from another thread. The sources are lent to the wait as shared pointers
//! the caller cloned for it and holds for the whole call, so no descriptor in the set can be
//! closed while the platform is looking at it. What the wait watches is the descriptor each
//! source lent when it was registered, which the caller hands over beside it: the wait asks no
//! source for anything, and so runs no code of one.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::Poller;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::{MAX_SOURCES, Poller};

/// What to watch a source for.
pub(super) struct Want {
    pub(super) key: usize,
    /// The source's descriptor, as the source lent it when it was registered.
    pub(super) descriptor: RawSource,
    pub(super) readable: bool,
    pub(super) writable: bool,
}

/// The descriptor of a source, as the platform's wait takes it: a file descriptor on unix, and a
/// socket on Windows.
#[cfg(unix)]
pub(super) type RawSource = std::os::fd::RawFd;
#[cfg(windows)]
pub(super) type RawSource = std::os::windows::io::RawSocket;

/// What a source was found ready for; `Want`'s shape.
pub(super) struct Ready {
    pub(super) key: usize,
    pub(super) readable: bool,
    pub(super) writable: bool,
}

/// The longest a wait is asked to last.
///
/// Some of the platforms' waits take their timeout as a count of milliseconds in an `int`, and
/// fail one longer than that holds, about 24 days, rather than wait for it. A wait bounded here
/// returns early, and the reactor's next wait goes on from there.
pub(super) const MAX_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(i32::MAX as u64);
