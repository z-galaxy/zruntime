//! Async sockets on a [`Runtime`].
//!
//! Each family of socket is behind a cargo feature of its own, none of them on by default: `tcp`
//! brings `TcpListener` and `TcpStream`, `udp` brings `UdpSocket`, and `unix` brings the `unix`
//! module, with `UnixListener`, `UnixStream` and `UnixDatagram`, on unix platforms only.
//!
//! # The runtime
//!
//! A socket is built on a runtime, which its constructor takes as its first argument, and that
//! runtime's reactor watches it from then on: its operations make progress while some thread is
//! inside [`Runtime::block_on`] on that runtime, or, on a runtime from `SharedRuntime::current`,
//! while the helper thread runs it. The socket's type carries the runtime's flavour. One built on
//! a [`LocalRuntime`] is, say, a `TcpStream<Local>`, `Local` being the default, and stays on the
//! thread it was made on; one built on a [`SharedRuntime`] is a `TcpStream<Shared>`, which may be
//! sent to, and used from, any thread.
//!
//! # Operations
//!
//! A socket is in non-blocking mode for as long as it is one of these, and an operation on it
//! that would block waits for the runtime to report the socket ready instead, leaving the thread
//! to the other tasks in the meantime. Connecting is one such operation, so a `connect` never
//! blocks the thread either.
//!
//! A TCP or UDP socket is bound or connected to a socket address, never to a host name: looking a
//! name up, through std's `ToSocketAddrs` say, blocks the thread for as long as the resolver
//! takes, which a task must not do to the thread it shares with the others. A caller with a name
//! to connect to looks it up on a thread of its own (`unblock`, with the `unblock` feature, runs
//! such work), and tries the addresses it finds in turn.
//!
//! The streams, `TcpStream` and `UnixStream`, implement the `AsyncRead` and `AsyncWrite` traits
//! of [`futures-io`], so the extension traits of [`futures-lite`] or [`futures-util`] read from
//! and write to them. So does a shared reference to one, which lets a reader and a writer share a
//! stream. Closing a stream shuts its write half down, and the peer reads the end of the stream.
//!
//! A socket wakes one task per direction: one waiting to read and one waiting to write. Two tasks
//! waiting in the same direction on one socket at once — to read, peek, accept or receive through
//! two references to it, or to write or send — is one too many: the one that waited first may
//! never be woken. Tasks that share a direction take turns, behind a lock of their own.
//!
//! On Windows, a runtime watches at most 1023 sockets at a time, which its reactor waits on in a
//! single `select` call: a server there holds at most that many sockets on one runtime, its
//! listener included.
//!
//! [`futures-io`]: https://docs.rs/futures-io
//! [`futures-lite`]: https://docs.rs/futures-lite
//! [`futures-util`]: https://docs.rs/futures-util
//! [`LocalRuntime`]: crate::LocalRuntime
//! [`Runtime`]: crate::Runtime
//! [`Runtime::block_on`]: crate::Runtime::block_on
//! [`SharedRuntime`]: crate::SharedRuntime

#[cfg(any(feature = "tcp", all(feature = "unix", unix)))]
mod connect;
mod io;
#[cfg(feature = "tcp")]
mod tcp;
#[cfg(feature = "udp")]
mod udp;
#[cfg(all(feature = "unix", unix))]
pub mod unix;

#[cfg(feature = "tcp")]
pub use tcp::{Incoming, TcpListener, TcpStream};
#[cfg(feature = "udp")]
pub use udp::UdpSocket;
