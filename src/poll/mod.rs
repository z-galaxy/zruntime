//! The wait a platform is asked for, and what it is told to watch.
//!
//! A poller watches each source in the directions it was last told to, and a wait on it reports
//! which of them it found ready. Each poller keeps a channel of its own that a `notify` writes to,
//! so that a wait can be broken from another thread.
//!
//! What a source is watched for is told to the poller as it changes, rather than on every wait: a
//! source is added once, as it is registered, its directions are changed from then on, and it is
//! deleted as its registration goes. Every call names the source by the key the reactor reports it
//! by, and by the descriptor it lent when it was registered, which the caller holds open across the
//! call: a poller asks no source for anything, and so runs no code of one.
//!
//! There are two kinds of poller, and [`Poller::LIVE`] says which kind a platform has. One keeps
//! what it watches in the kernel, which sees a change at once, a wait under way included, and lets
//! go of a descriptor deleted from it there and then. The other takes what to watch as a list on
//! every wait, which it keeps for itself and copies as a wait starts: a change reaches only the
//! waits that start after it, and a descriptor a wait under way copied has to stay open until that
//! wait returns.

// Which poller a platform waits on: epoll on Linux and Android, kqueue on the BSDs, `select(2)` on
// Apple's platforms, where neither `poll(2)` nor kqueue can watch a terminal, `poll(2)` on any
// other unix and Winsock's `select` on Windows.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod epoll;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) use epoll::Poller;

#[cfg(any(
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
))]
mod kqueue;
#[cfg(any(
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
))]
pub(super) use kqueue::Poller;

#[cfg(target_vendor = "apple")]
mod select;
#[cfg(target_vendor = "apple")]
pub(super) use select::Poller;

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_vendor = "apple",
    )),
))]
mod generic;
#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_vendor = "apple",
    )),
))]
pub(super) use generic::Poller;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::Poller;

// The list that `poll(2)` and both `select`s keep of what they watch.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
)))]
mod list;
// The channel that breaks the wait of every unix poller.
#[cfg(unix)]
mod pipe;

/// Which directions of a source are watched, or were found ready.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Directions {
    pub(super) readable: bool,
    pub(super) writable: bool,
}

impl Directions {
    /// Neither direction.
    pub(super) const NONE: Self = Self {
        readable: false,
        writable: false,
    };

    /// Whether this names neither direction.
    pub(super) fn is_empty(self) -> bool {
        !self.readable && !self.writable
    }
}

/// A source a wait found ready, the directions it was found ready in, and those of them it is no
/// longer watched in.
pub(super) struct Ready {
    pub(super) key: usize,
    pub(super) directions: Directions,
    /// The directions the poller stopped watching the source in as it reported them, which it
    /// has to be told to watch again: kqueue lets go of a filter that the kernel drops, as
    /// FreeBSD does the write filter of a pipe whose reader closed.
    pub(super) dropped: Directions,
}

/// The descriptor of a source, as the platform's wait takes it: a file descriptor on unix, and a
/// socket on Windows.
#[cfg(unix)]
pub(super) type RawSource = std::os::fd::RawFd;
#[cfg(windows)]
pub(super) type RawSource = std::os::windows::io::RawSocket;

/// The longest a wait is asked to last.
///
/// Some of the platforms' waits take their timeout as a count of milliseconds in an `int`, and
/// fail one longer than that holds, about 24 days, rather than wait for it. A wait bounded here
/// returns early, and the reactor's next wait goes on from there.
pub(super) const MAX_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(i32::MAX as u64);
