//! What every socket of this module is built on: the socket, and its registration on a runtime.

#[cfg(unix)]
use std::os::fd::AsFd as AsSource;
#[cfg(windows)]
use std::os::windows::io::AsSocket as AsSource;
use std::{
    future::poll_fn,
    io,
    task::{Context, Poll},
};

use crate::{Interest, Mode, Registration, Runtime, reactor};

/// A non-blocking socket, and the registration its I/O waits on.
///
/// The socket is shared with the runtime's reactor, which keeps a clone of the pointer to it for
/// as long as the registration lives, and a wait under way on another thread keeps one of its own
/// until it returns: so the descriptor stays open for as long as anything may still be watching
/// it, and closes once the last of them, and this, have let go of it.
pub(crate) struct Io<T, M>
where
    M: Mode,
{
    // Fields drop in the order they are declared: the watch ends before this handle on the socket
    // goes.
    registration: Registration<M>,
    socket: M::Ptr<T>,
}

impl<T, M> Io<T, M>
where
    M: Mode,
{
    /// Registers `socket` on `runtime`.
    ///
    /// `socket` must be in non-blocking mode already, which is the caller's to see to: the runtime
    /// waits for readiness and then runs the operation, so an operation on a blocking socket that
    /// turns out not to be ready after all would hold up the thread it runs on, and every other
    /// task with it.
    pub(crate) fn new(runtime: &Runtime<M>, socket: T) -> io::Result<Self>
    where
        T: AsSource + Send + Sync + 'static,
    {
        let socket = M::new_ptr(socket);
        let registration = reactor::register::<M>(&runtime.core, M::source_ptr(socket.clone()))?;
        // Asked for once the source is in the reactor's map, so that a helper starting here takes
        // it into its very first wait.
        M::ensure_progress(&runtime.core);

        Ok(Self {
            registration,
            socket,
        })
    }

    /// The socket.
    pub(crate) fn get_ref(&self) -> &T {
        &self.socket
    }

    /// A handle on the runtime the socket is registered on.
    pub(crate) fn runtime(&self) -> Runtime<M> {
        self.registration.runtime()
    }

    /// Runs `operation` on the socket once it is readable, until it no longer reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock).
    pub(crate) async fn read_with<R>(
        &self,
        mut operation: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        poll_fn(|cx| self.poll_read_with(cx, &mut operation)).await
    }

    /// Runs `operation` on the socket once it is writable, until it no longer reports
    /// [`WouldBlock`](io::ErrorKind::WouldBlock).
    pub(crate) async fn write_with<R>(
        &self,
        mut operation: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        poll_fn(|cx| self.poll_write_with(cx, &mut operation)).await
    }

    /// [`Io::read_with`], polled.
    pub(crate) fn poll_read_with<R>(
        &self,
        cx: &mut Context<'_>,
        operation: impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Interest::Readable, operation)
    }

    /// [`Io::write_with`], polled.
    pub(crate) fn poll_write_with<R>(
        &self,
        cx: &mut Context<'_>,
        operation: impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Interest::Writable, operation)
    }

    /// Runs `operation` on the socket while it is ready for `interest`: see
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
        let socket = &*self.socket;

        self.registration.poll_io(cx, interest, || {
            loop {
                match operation(socket) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    result => return result,
                }
            }
        })
    }
}

/// Makes a write to `socket`, once its peer has gone, fail rather than raise `SIGPIPE`, on
/// Apple's platforms, and does nothing elsewhere.
///
/// Those platforms have no `MSG_NOSIGNAL` for a write to ask for that with, so the socket sees to
/// it itself, through its `SO_NOSIGPIPE` option. std and socket2 set the option on the sockets
/// they make, but a socket handed to a `from_std` may come from elsewhere.
#[cfg(feature = "tcp")]
pub(crate) fn set_nosigpipe<S>(socket: &S) -> io::Result<()>
where
    S: AsSource,
{
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos"
    ))]
    {
        Ok(rustix::net::sockopt::set_socket_nosigpipe(socket, true)?)
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos"
    )))]
    {
        let _ = socket;

        Ok(())
    }
}
