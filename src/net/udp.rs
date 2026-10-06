//! The UDP socket, [`UdpSocket`].

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};
use std::{
    fmt, io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use crate::{Async, Local, Mode, Runtime};

/// A UDP socket, to send datagrams from and to receive them on.
///
/// A socket is bound to a socket address with [`UdpSocket::bind`], and receives the datagrams
/// that are sent to that address. It sends a datagram to an address of the caller's choosing
/// through [`send_to`](UdpSocket::send_to), and receives one together with the address it came
/// from through [`recv_from`](UdpSocket::recv_from). Once the socket is
/// [connected](UdpSocket::connect) to a peer, [`send`](UdpSocket::send) sends to that peer, with
/// no address to give, and from then on the socket receives only the datagrams that peer sends,
/// which [`recv`](UdpSocket::recv) hands over without the address they came from. The socket is
/// the async counterpart of [`std::net::UdpSocket`]: an operation that has to wait leaves the
/// thread to the other tasks instead of blocking it.
///
/// A datagram is one message, sent and received whole. The addresses a socket is bound or
/// connected to, and sends to, are socket addresses, never host names: a name is the caller's to
/// look up first, as [`UdpSocket::bind`] says.
///
/// The socket runs on the runtime given to the constructor that made it, whose reactor watches it
/// from then on. Its operations make progress while some thread is inside [`Runtime::block_on`]
/// on that runtime, or, on a runtime from `SharedRuntime::current`, while the helper thread runs
/// it. The socket's type carries the flavour of that runtime: one built on a
/// [`LocalRuntime`](crate::LocalRuntime) is a `UdpSocket<Local>`, [`Local`] being the default, and
/// stays on the thread it was made on; one built on a [`SharedRuntime`](crate::SharedRuntime) is a
/// `UdpSocket<Shared>`, which may be sent to, and used from, any thread.
///
/// Any number of tasks may wait to receive from a socket at once, through
/// [`recv`](UdpSocket::recv), [`recv_from`](UdpSocket::recv_from), [`peek`](UdpSocket::peek) or
/// [`peek_from`](UdpSocket::peek_from), and any number to send on it, through
/// [`send`](UdpSocket::send) or [`send_to`](UdpSocket::send_to), each through a reference to the
/// socket. Each receive takes a datagram of its own, so tasks that receive together get one each,
/// while a peek leaves the datagram on the socket for the next receive.
///
/// # Example
///
/// Two sockets, one sending a greeting to the other, which receives it together with the address
/// it came from:
///
/// ```
/// use std::net::Ipv4Addr;
///
/// use zruntime::{LocalRuntime, net::UdpSocket};
///
/// let runtime = LocalRuntime::new()?;
/// // Port `0` has the system pick a free port, which the socket then reports.
/// let sender = UdpSocket::bind(&runtime, (Ipv4Addr::LOCALHOST, 0))?;
/// let receiver = UdpSocket::bind(&runtime, (Ipv4Addr::LOCALHOST, 0))?;
///
/// runtime.block_on(async {
///     sender.send_to(b"hello", receiver.local_addr()?).await?;
///
///     let mut greeting = [0; 16];
///     let (len, from) = receiver.recv_from(&mut greeting).await?;
///
///     assert_eq!(&greeting[..len], b"hello");
///     assert_eq!(from, sender.local_addr()?);
///     # Ok::<_, std::io::Error>(())
/// })?;
/// # Ok::<_, std::io::Error>(())
/// ```
pub struct UdpSocket<M = Local>
where
    M: Mode,
{
    io: Async<std::net::UdpSocket, M>,
}

impl<M> UdpSocket<M>
where
    M: Mode,
{
    /// A socket bound to `addr`, on `runtime`.
    ///
    /// `addr` is a socket address: a [`SocketAddr`], or anything that converts into one, such as a
    /// pair of an [`Ipv4Addr`] and a port. It is never a host name. Looking a name up, through
    /// std's [`ToSocketAddrs`](std::net::ToSocketAddrs), blocks the thread for as long as the
    /// resolver takes, which a task must not do to the thread it shares with the others, so a
    /// caller with a name looks it up first, on a thread of its own, and binds to the address it
    /// finds. Binding to port `0` has the system pick a free port, which
    /// [`local_addr`](UdpSocket::local_addr) then tells.
    ///
    /// The socket is bound once this returns, and takes the datagrams sent to its address from
    /// then on. What can fail is that, or what [`from_std`](UdpSocket::from_std) can fail at.
    pub fn bind<A>(runtime: &Runtime<M>, addr: A) -> io::Result<Self>
    where
        A: Into<SocketAddr>,
    {
        Self::from_std(runtime, std::net::UdpSocket::bind(addr.into())?)
    }

    /// A socket on `runtime` that sends and receives the datagrams `socket` does.
    ///
    /// `socket` is switched to non-blocking mode, which every socket of this type is in. On Unix
    /// the mode belongs to the open socket rather than to a handle on it, so a duplicate of
    /// `socket` made with [`try_clone`](std::net::UdpSocket::try_clone) is switched with it, and a
    /// receive or a send on that one fails with [`WouldBlock`](io::ErrorKind::WouldBlock) where it
    /// would have waited.
    ///
    /// What can fail is the switch to non-blocking mode, and the runtime taking the socket under
    /// its watch, which on Windows it does for a limited number of sockets: see the
    /// [module documentation](super).
    pub fn from_std(runtime: &Runtime<M>, socket: std::net::UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;

        Ok(Self {
            io: Async::from_nonblocking(runtime, socket)?,
        })
    }

    /// The local socket address this socket is bound to.
    ///
    /// After binding to port `0`, this is how to find the port the system picked.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The socket address of the peer this socket is connected to.
    ///
    /// Fails with [`NotConnected`](io::ErrorKind::NotConnected) where the socket is not connected:
    /// see [`connect`](UdpSocket::connect).
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Connects this socket to `addr`, making that address the peer it sends to and, from then on,
    /// the only one it receives from.
    ///
    /// `addr` is a socket address, never a host name: see [`bind`](UdpSocket::bind). Nothing goes
    /// over the network and nothing waits, which is why this is not an `async` function. What
    /// connecting does is fix the address that [`send`](UdpSocket::send) sends to, and make the
    /// socket take only the datagrams that address sends from then on, and drop the ones any other
    /// address sends. A datagram from another address that arrived before the connect is not
    /// dropped: it is queued still, and a receive takes it. The socket may be connected again, to
    /// another address.
    pub fn connect<A>(&self, addr: A) -> io::Result<()>
    where
        A: Into<SocketAddr>,
    {
        self.io.get_ref().connect(addr.into())
    }

    /// Sends `buf` as one datagram to `addr`.
    ///
    /// `addr` is a socket address, never a host name: see [`bind`](UdpSocket::bind). Resolves to
    /// the number of bytes sent, which is the length of `buf`: a datagram goes whole or not at all.
    /// The send waits only where the system has no room for the datagram, until it has made some.
    ///
    /// Any number of tasks may wait to send at once, through this or [`send`](UdpSocket::send), as
    /// [the socket's documentation](UdpSocket) says.
    pub async fn send_to<A>(&self, buf: &[u8], addr: A) -> io::Result<usize>
    where
        A: Into<SocketAddr>,
    {
        let target = addr.into();

        self.io
            .write_with(|socket| socket.send_to(buf, target))
            .await
    }

    /// Waits for a datagram, and copies it into `buf`.
    ///
    /// Resolves to the number of bytes copied and the address the datagram came from. Where the
    /// datagram is too long for `buf`, the bytes that do not fit may be discarded. On Windows the
    /// receive fails instead, with the error Winsock reports for that (`WSAEMSGSIZE`), and the
    /// whole datagram is lost.
    ///
    /// Dropping the future before it completes gives up the wait. No datagram is lost with it: one
    /// that arrives meanwhile stays queued for the next receive.
    ///
    /// Any number of tasks may wait to receive at once, each taking a datagram of its own, through
    /// this or [`recv`](UdpSocket::recv), as [the socket's documentation](UdpSocket) says.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.io.read_with(|socket| socket.recv_from(buf)).await
    }

    /// Waits for a datagram, and copies it into `buf` without taking it off the socket.
    ///
    /// Resolves to the number of bytes copied and the address the datagram came from. The next
    /// receive, or `peek_from`, finds the same datagram again. Where the datagram is too long for
    /// `buf`, only as much of it as fits is copied. On Windows the peek fails instead, with the
    /// error Winsock reports for that (`WSAEMSGSIZE`), though the datagram stays queued.
    ///
    /// Any number of tasks may wait to peek at once, through this or [`peek`](UdpSocket::peek), as
    /// [the socket's documentation](UdpSocket) says.
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.io.read_with(|socket| socket.peek_from(buf)).await
    }

    /// Sends `buf` as one datagram to the peer the socket is connected to.
    ///
    /// Resolves to the number of bytes sent, which is the length of `buf`: a datagram goes whole
    /// or not at all. Fails where the socket is not connected: see
    /// [`connect`](UdpSocket::connect). The send waits only where the system has no room for the
    /// datagram, until it has made some.
    ///
    /// Any number of tasks may wait to send at once, through this or
    /// [`send_to`](UdpSocket::send_to), as [the socket's documentation](UdpSocket) says.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.io.write_with(|socket| socket.send(buf)).await
    }

    /// Waits for a datagram, and copies it into `buf`.
    ///
    /// Resolves to the number of bytes copied. This is [`recv_from`](UdpSocket::recv_from) without
    /// the address the datagram came from, which suits a socket [connected](UdpSocket::connect) to
    /// a peer: a datagram that arrives from then on can only have come from there. One that
    /// arrived before the connect may have come from any address, and is handed over all the same,
    /// without saying which. Where the datagram is too long for `buf`, the bytes that do not fit
    /// may be discarded. On Windows the receive fails instead, with the error Winsock reports for
    /// that (`WSAEMSGSIZE`), and the whole datagram is lost.
    ///
    /// Dropping the future before it completes gives up the wait, and loses no datagram, as it
    /// does for [`recv_from`](UdpSocket::recv_from).
    ///
    /// Any number of tasks may wait to receive at once, each taking a datagram of its own, through
    /// this or [`recv_from`](UdpSocket::recv_from), as [the socket's documentation](UdpSocket)
    /// says.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.read_with(|socket| socket.recv(buf)).await
    }

    /// Waits for a datagram, and copies it into `buf` without taking it off the socket.
    ///
    /// Resolves to the number of bytes copied. This is [`peek_from`](UdpSocket::peek_from) without
    /// the address the datagram came from. The next receive, or peek, finds the same datagram
    /// again. Where the datagram is too long for `buf`, only as much of it as fits is copied. On
    /// Windows the peek fails instead, with the error Winsock reports for that (`WSAEMSGSIZE`),
    /// though the datagram stays queued.
    ///
    /// Any number of tasks may wait to peek at once, through this or
    /// [`peek_from`](UdpSocket::peek_from), as [the socket's documentation](UdpSocket) says.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.read_with(|socket| socket.peek(buf)).await
    }

    /// The value of the `SO_BROADCAST` option of this socket: whether it may send datagrams to a
    /// broadcast address.
    ///
    /// See [`set_broadcast`](UdpSocket::set_broadcast).
    pub fn broadcast(&self) -> io::Result<bool> {
        self.io.get_ref().broadcast()
    }

    /// Sets the value of the `SO_BROADCAST` option of this socket: whether it may send datagrams
    /// to a broadcast address, the address that reaches every host on the local network.
    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.io.get_ref().set_broadcast(on)
    }

    /// The value of the `IP_MULTICAST_LOOP` option of this socket: whether the multicast datagrams
    /// it sends are looped back to the local host. On Windows the option is the receiving
    /// socket's, not the sending socket's.
    ///
    /// See [`set_multicast_loop_v4`](UdpSocket::set_multicast_loop_v4).
    pub fn multicast_loop_v4(&self) -> io::Result<bool> {
        self.io.get_ref().multicast_loop_v4()
    }

    /// Sets the value of the `IP_MULTICAST_LOOP` option of this socket: whether the multicast
    /// datagrams it sends are looped back to the local host, where the sockets that joined the
    /// group, this one included, receive them.
    ///
    /// On Windows the option is the receiving socket's instead: Winsock applies it to the receive
    /// path, so it decides whether this socket receives the multicast datagrams that applications
    /// on the local host send, where POSIX systems apply it to the sending socket.
    ///
    /// This is for a socket of an IPv4 address; for an IPv6 one, see
    /// [`set_multicast_loop_v6`](UdpSocket::set_multicast_loop_v6).
    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.io.get_ref().set_multicast_loop_v4(on)
    }

    /// The value of the `IP_MULTICAST_TTL` option of this socket: the time-to-live field of the
    /// multicast IP packets sent from it.
    ///
    /// See [`set_multicast_ttl_v4`](UdpSocket::set_multicast_ttl_v4).
    pub fn multicast_ttl_v4(&self) -> io::Result<u32> {
        self.io.get_ref().multicast_ttl_v4()
    }

    /// Sets the value of the `IP_MULTICAST_TTL` option of this socket: the time-to-live field of
    /// the multicast IP packets sent from it, which says how far they may travel. The default,
    /// `1`, keeps them on the local network.
    ///
    /// This is for a socket of an IPv4 address.
    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_multicast_ttl_v4(ttl)
    }

    /// The value of the `IPV6_MULTICAST_LOOP` option of this socket: whether the multicast
    /// datagrams it sends are looped back to the local host. On Windows the option is the
    /// receiving socket's, not the sending socket's.
    ///
    /// See [`set_multicast_loop_v6`](UdpSocket::set_multicast_loop_v6).
    pub fn multicast_loop_v6(&self) -> io::Result<bool> {
        self.io.get_ref().multicast_loop_v6()
    }

    /// Sets the value of the `IPV6_MULTICAST_LOOP` option of this socket: whether the multicast
    /// datagrams it sends are looped back to the local host, where the sockets that joined the
    /// group, this one included, receive them.
    ///
    /// On Windows the option is the receiving socket's instead: Winsock applies it to the receive
    /// path, so it decides whether this socket receives the multicast datagrams that applications
    /// on the local host send, where POSIX systems apply it to the sending socket.
    ///
    /// This is for a socket of an IPv6 address; for an IPv4 one, see
    /// [`set_multicast_loop_v4`](UdpSocket::set_multicast_loop_v4).
    pub fn set_multicast_loop_v6(&self, on: bool) -> io::Result<()> {
        self.io.get_ref().set_multicast_loop_v6(on)
    }

    /// Joins the IPv4 multicast group `multiaddr`, which makes the socket receive the datagrams
    /// sent to that group, as the `IP_ADD_MEMBERSHIP` option does.
    ///
    /// `multiaddr` must be a multicast address. `interface` is the address of the local interface
    /// to join the group on, or [`Ipv4Addr::UNSPECIFIED`] to let the system choose one. The group
    /// is left with [`leave_multicast_v4`](UdpSocket::leave_multicast_v4), with the same
    /// arguments.
    pub fn join_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.io.get_ref().join_multicast_v4(&multiaddr, &interface)
    }

    /// Leaves the IPv4 multicast group `multiaddr`, as the `IP_DROP_MEMBERSHIP` option does.
    ///
    /// The arguments are those the group was joined with, through
    /// [`join_multicast_v4`](UdpSocket::join_multicast_v4).
    pub fn leave_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.io.get_ref().leave_multicast_v4(&multiaddr, &interface)
    }

    /// Joins the IPv6 multicast group `multiaddr`, which makes the socket receive the datagrams
    /// sent to that group, as the `IPV6_ADD_MEMBERSHIP` option does.
    ///
    /// `multiaddr` must be a multicast address. `interface` is the index of the local interface to
    /// join the group on, or `0` to let the system choose one. The group is left with
    /// [`leave_multicast_v6`](UdpSocket::leave_multicast_v6), with the same arguments.
    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.io.get_ref().join_multicast_v6(multiaddr, interface)
    }

    /// Leaves the IPv6 multicast group `multiaddr`, as the `IPV6_DROP_MEMBERSHIP` option does.
    ///
    /// The arguments are those the group was joined with, through
    /// [`join_multicast_v6`](UdpSocket::join_multicast_v6).
    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.io.get_ref().leave_multicast_v6(multiaddr, interface)
    }

    /// The value of the `IP_TTL` option of this socket: the time-to-live field of the IP packets
    /// sent from it.
    ///
    /// See [`set_ttl`](UdpSocket::set_ttl).
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the value of the `IP_TTL` option of this socket: the time-to-live field of the IP
    /// packets sent from it.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }
}

impl<M> fmt::Debug for UdpSocket<M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.io.get_ref(), f)
    }
}

#[cfg(unix)]
impl<M> AsFd for UdpSocket<M>
where
    M: Mode,
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
    }
}

#[cfg(unix)]
impl<M> AsRawFd for UdpSocket<M>
where
    M: Mode,
{
    fn as_raw_fd(&self) -> RawFd {
        self.io.get_ref().as_raw_fd()
    }
}

#[cfg(windows)]
impl<M> AsSocket for UdpSocket<M>
where
    M: Mode,
{
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.io.get_ref().as_socket()
    }
}

#[cfg(windows)]
impl<M> AsRawSocket for UdpSocket<M>
where
    M: Mode,
{
    fn as_raw_socket(&self) -> RawSocket {
        self.io.get_ref().as_raw_socket()
    }
}
