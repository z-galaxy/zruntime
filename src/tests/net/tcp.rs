//! Tests of [`TcpListener`], [`TcpStream`] and [`Incoming`], over loopback.
//!
//! Each test makes the sockets it needs from a listener of its own, bound to a port the system
//! picks, so that tests running side by side never meet. The ones that run in both flavours go
//! through [`in_both_modes`]; the ones that spawn tasks, or send a stream to another thread, only
//! mean something on a shared runtime and are written for that.
//!
//! The first tests are of making a connection: that a connection carries bytes both ways, that a
//! refused one reports why, that one the listener has no room for yet is waited for, and that one
//! which is waited for and then fails reports why. Then come the tests of the mode the sockets are
//! in: that the stream a listener accepts, and the sockets made from std's, never block the
//! thread. Closing and shutting down follow, closing twice included, then the ways of sharing a
//! stream, and then what a call that has to wait does: a write waits for room and a read for
//! bytes, without spinning, and giving up an `accept` loses no connection. After them come the
//! tests of `incoming`, `peek` and the socket options, of a shared runtime's sockets crossing
//! threads and serving tasks, and last, of the sockets' `Debug`, descriptors and auto traits.

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;
use std::{
    cell::Cell,
    future::poll_fn,
    io::{self, Read, Write},
    net::Shutdown,
    pin::{Pin, pin},
    task::Poll,
    thread,
};

use futures_lite::{
    AsyncReadExt, AsyncWriteExt, StreamExt,
    future::{block_on, poll_once, zip},
    io::{AsyncWrite, copy},
};
use ntest::timeout;
use socket2::SockRef;

#[cfg(any(target_os = "linux", target_os = "android"))]
use super::listener_with_room_for_one;
use super::{DELAY, RefusedPort, after, counting_polls, in_both_modes, loopback, pattern};
use crate::{
    Local, Mode, Runtime, Shared, SharedRuntime,
    net::{Incoming, TcpListener, TcpStream},
};

in_both_modes! {
    /// A connection carries bytes both ways, and each end names the other as its peer.
    fn a_connection_echoes_both_ways<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let listener = TcpListener::bind(&runtime, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        runtime.block_on(async {
            let mut client = TcpStream::connect(&runtime, address).await.unwrap();
            let (mut server, peer) = listener.accept().await.unwrap();

            client.write_all(b"ping").await.unwrap();
            let mut received = [0; 4];
            server.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, b"ping");

            server.write_all(b"pong").await.unwrap();
            client.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, b"pong");

            assert_eq!(client.peer_addr().unwrap(), address);
            assert_eq!(server.local_addr().unwrap(), address);
            assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
            assert_eq!(peer, client.local_addr().unwrap());
        });
    }
}

in_both_modes! {
    /// A connection to a port nobody listens on fails with the refusal the kernel reports for it,
    /// which the connect reads off the socket once the runtime says the attempt has settled.
    fn a_refused_connection_reports_connection_refused<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let refused = RefusedPort::new();

        let error = runtime
            .block_on(TcpStream::connect(&runtime, refused.address()))
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }
}

in_both_modes! {
    /// A connection the listener has no room for yet is waited for, not reported: the connect is
    /// still pending after a poll, and completes once the listener takes the connection in its
    /// queue out of it, which makes the room.
    ///
    /// The kernel leaves a connection it has no room for unanswered, and the client asks again a
    /// moment later, so this takes a second or two. The listener is a plain socket that accepts
    /// by blocking, which returns at once here, as the connection it takes is the one already
    /// queued.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn a_pending_connect_resolves_once_accepted<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let (listener, address) = listener_with_room_for_one();

        runtime.block_on(async {
            // The queue holds this one, so the kernel has nowhere to put the next connection
            // until the listener takes this one out.
            let _queued = TcpStream::connect(&runtime, address).await.unwrap();

            let mut pending = pin!(TcpStream::connect(&runtime, address));
            assert!(
                poll_once(pending.as_mut()).await.is_none(),
                "the connection was reported before the listener had room for it",
            );

            let (_accepted, _) = listener.accept().unwrap();
            let stream = pending.await.unwrap();

            assert_eq!(stream.peer_addr().unwrap(), address);
        });
    }
}

in_both_modes! {
    /// A connect that has to wait, and then fails, reports the failure: the first check finds the
    /// connection under way, and the check that follows the wait finds the reason it failed on the
    /// socket, rather than taking the connection for one that is made, or for one that is still
    /// under way.
    ///
    /// The listener has room for one connection, which the first connect takes, so the kernel
    /// leaves the second unanswered and its connect pending, as in the test above. Then the
    /// listener goes away, and the connection request that the client sends again a moment later
    /// finds the port closed and is refused, which takes about a second.
    ///
    /// What the connect waits for is not what this pins down: a refusal raises an error condition,
    /// which the runtime reports for a wait on either readiness, so a connect that waited to read
    /// would be woken just the same. `a_pending_connect_resolves_once_accepted` tells the two
    /// apart, since a connection that is made raises writability alone.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn a_connect_that_waits_and_then_fails_reports_the_failure<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let (listener, address) = listener_with_room_for_one();

        runtime.block_on(async {
            // The queue holds this one, so the kernel has nowhere to put the next connection
            // until the listener takes this one out, which it never does.
            let _queued = TcpStream::connect(&runtime, address).await.unwrap();

            let mut pending = pin!(TcpStream::connect(&runtime, address));
            assert!(
                poll_once(pending.as_mut()).await.is_none(),
                "the connection was reported before the listener had room for it",
            );

            drop(listener);
            let error = pending.await.unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        });
    }
}

in_both_modes! {
    /// A stream the listener accepted is in non-blocking mode, though std's `accept` leaves the
    /// socket it makes blocking on Linux: a read that has nothing to read yet waits for the
    /// runtime, which is what lets the write that is due after a delay, from this very thread,
    /// happen. A blocking socket would hold the thread in the read for good, and the test's
    /// timeout would fail it.
    fn an_accepted_stream_is_non_blocking<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let listener = TcpListener::bind(&runtime, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        runtime.block_on(async {
            let mut client = TcpStream::connect(&runtime, address).await.unwrap();
            let (mut server, _) = listener.accept().await.unwrap();

            let mut received = [0; 4];
            let (written, read) = zip(
                after(&runtime, client.write_all(b"late")),
                server.read_exact(&mut received),
            )
            .await;

            written.unwrap();
            read.unwrap();
            assert_eq!(&received, b"late");
        });
    }
}

in_both_modes! {
    /// A listener made from a std one in blocking mode is switched to non-blocking mode: an
    /// `accept` with no connection to take waits for the runtime, which is what lets the
    /// connection that is made after a delay, from this very thread, come in. A blocking listener
    /// would hold the thread in `accept` for good, and the test's timeout would fail it.
    fn from_std_makes_a_blocking_listener_non_blocking<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let std_listener = std::net::TcpListener::bind(loopback()).unwrap();
        let address = std_listener.local_addr().unwrap();
        let listener = TcpListener::from_std(&runtime, std_listener).unwrap();

        runtime.block_on(async {
            let (accepted, client) = zip(
                listener.accept(),
                after(&runtime, TcpStream::connect(&runtime, address)),
            )
            .await;

            let (_server, peer) = accepted.unwrap();
            assert_eq!(peer, client.unwrap().local_addr().unwrap());
        });
    }
}

in_both_modes! {
    /// A stream made from a std one in blocking mode is switched to non-blocking mode: a read
    /// with nothing to read yet waits for the runtime, which is what lets the write that is due
    /// after a delay, from this very thread, happen. A blocking stream would hold the thread in
    /// the read for good, and the test's timeout would fail it.
    ///
    /// Both ends are std streams to begin with, in blocking mode as std makes them, and the one
    /// that is read from is the one std's `accept` returned.
    fn from_std_makes_a_blocking_stream_non_blocking<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let std_listener = std::net::TcpListener::bind(loopback()).unwrap();
        let mut peer = std::net::TcpStream::connect(std_listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = std_listener.accept().unwrap();
        let mut stream = TcpStream::from_std(&runtime, accepted).unwrap();

        runtime.block_on(async {
            let mut received = [0; 4];
            let (written, read) = zip(
                after(&runtime, async { peer.write_all(b"late") }),
                stream.read_exact(&mut received),
            )
            .await;

            written.unwrap();
            read.unwrap();
            assert_eq!(&received, b"late");
        });
    }
}

in_both_modes! {
    /// Closing a stream makes the peer read the end of the stream once it has read what was sent,
    /// leaves the other direction open, and closing it again is fine.
    fn close_makes_the_peer_read_the_end_of_the_stream<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;

            client.write_all(b"bye").await.unwrap();
            client.close().await.unwrap();

            let mut received = Vec::new();
            server.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, b"bye");

            // Only the write half is shut down: the answer still gets to the client.
            server.write_all(b"ack").await.unwrap();
            let mut answer = [0; 3];
            client.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"ack");

            client.close().await.unwrap();
        });
    }
}

in_both_modes! {
    /// Closing a stream that is closed already leaves its read half open: what the peer writes
    /// after the second close still reaches it.
    ///
    /// FreeBSD and NetBSD take a socket for disconnected when its write half is shut down a second
    /// time, once the peer has acknowledged the first shutdown, and that ends the read half too, so
    /// the second close must not shut anything down. Elsewhere a second shutdown does no harm:
    /// Linux and OpenBSD ignore it, and macOS answers it with `ENOTCONN`, which a close reads as
    /// success. So this guards FreeBSD and NetBSD, where CI runs no tests, and it passes with or
    /// without the `write_shut` flag on the platforms CI does test.
    ///
    /// The peer reads the end of the stream before the second close, so that the first shutdown
    /// has got to it, and been acknowledged, by then.
    fn closing_a_stream_again_leaves_its_read_half_open<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;

            client.close().await.unwrap();
            let mut received = Vec::new();
            server.read_to_end(&mut received).await.unwrap();
            assert!(received.is_empty());

            client.close().await.unwrap();

            server.write_all(b"still open").await.unwrap();
            let mut answer = [0; 10];
            client.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"still open");
        });
    }
}

in_both_modes! {
    /// Closing through a shared reference shuts the socket down, so it ends the stream for every
    /// handle on it: a write through the stream itself fails afterwards, and the peer reads the
    /// end.
    fn closing_through_a_reference_closes_the_stream_for_every_handle<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;

            (&client).close().await.unwrap();

            assert!(client.write_all(b"too late").await.is_err());
            let mut received = Vec::new();
            server.read_to_end(&mut received).await.unwrap();
            assert!(received.is_empty());
        });
    }
}

in_both_modes! {
    /// Closing a stream whose connection is gone is not an error: the peer reset the connection,
    /// so there is nothing left to shut down, and the `NotConnected` that the system answers a
    /// shutdown with there is taken for a stream that is closed already.
    ///
    /// The peer is dropped with bytes it has not read, which is what makes it reset the
    /// connection rather than end it. What a shutdown after a reset reports differs between
    /// platforms, and this checks Linux's, which Android shares.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn closing_a_stream_whose_connection_was_reset_succeeds<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, server) = connected(&runtime).await;

            client.write_all(b"unread").await.unwrap();
            // Waits for the bytes to be there, so that dropping the peer leaves them unread.
            server.peek(&mut [0; 1]).await.unwrap();
            drop(server);

            let error = client.read(&mut [0; 1]).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);

            client.close().await.unwrap();
        });
    }
}

in_both_modes! {
    /// Shutting the write half down makes the peer read the end of the stream, after the bytes
    /// that were sent before it.
    fn shutdown_of_the_write_half_makes_the_peer_read_the_end<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;

            client.write_all(b"last words").await.unwrap();
            client.shutdown(Shutdown::Write).unwrap();

            let mut received = Vec::new();
            server.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, b"last words");
        });
    }
}

in_both_modes! {
    /// A reader and a writer share a stream through two references to it, in one task: 1 MiB
    /// written through one comes back through the other, echoed by the peer, which does so through
    /// two references to its own stream, and arrives intact.
    ///
    /// The echo ends when the client closes its end, as the copy of the peer's reads ends at the
    /// end of the stream, and the peer closes its own end then, which is what ends the client's
    /// reads in turn.
    fn a_reader_and_a_writer_share_a_stream<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (client, server) = connected(&runtime).await;
            let data = pattern(1 << 20);

            let writer = async {
                (&client).write_all(&data).await.unwrap();
                (&client).close().await.unwrap();
            };
            let reader = async {
                let mut echoed = Vec::new();
                (&client).read_to_end(&mut echoed).await.unwrap();

                echoed
            };
            let echo = async {
                copy(&server, &server).await.unwrap();
                (&server).close().await.unwrap();
            };

            let (((), echoed), ()) = zip(zip(writer, reader), echo).await;

            assert_eq!(echoed.len(), data.len());
            // Not `assert_eq!`, which would print a megabyte on a failure.
            assert!(echoed == data, "the echo differs from what was sent");
        });
    }
}

in_both_modes! {
    /// A write that finds no room in the socket waits until the peer makes some by reading, and
    /// then completes: its wait is for the socket to become writable, which a socket whose
    /// buffers are full is not.
    ///
    /// The socket is filled first, with writes that are polled once each, until one of them has
    /// to wait. The peer is a std stream that another thread reads from, after a delay, everything
    /// that was written and the waiting write too. Nothing but the socket becoming writable can
    /// wake the write then: a peer read in the same task would wake it by the reads' own
    /// readiness, whatever the write waited for.
    fn a_write_that_waits_resumes_once_the_peer_reads<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let std_listener = std::net::TcpListener::bind(loopback()).unwrap();
        let address = std_listener.local_addr().unwrap();
        let chunk = pattern(64 * 1024);

        runtime.block_on(async {
            let client = TcpStream::connect(&runtime, address).await.unwrap();
            let (mut peer, _) = std_listener.accept().unwrap();

            let mut writer = &client;
            let mut filled = 0;
            poll_fn(|cx| loop {
                match Pin::new(&mut writer).poll_write(cx, &chunk) {
                    Poll::Ready(written) => filled += written.unwrap(),
                    Poll::Pending => break Poll::Ready(()),
                }
            })
            .await;

            let total = filled + chunk.len();
            let reading = thread::spawn(move || {
                thread::sleep(DELAY);
                peer.read_exact(&mut vec![0; total]).unwrap();
            });

            writer.write_all(&chunk).await.unwrap();
            reading.join().unwrap();
        });
    }
}

in_both_modes! {
    /// A peek or a read that has to wait is polled when something changes, not over and over: it
    /// waits for the socket to become readable, which a connected socket with nothing to read is
    /// not, whereas it is writable all the while, so a wait for that instead would spin the thread.
    ///
    /// A call that waits is polled when it begins to, and again each time its task is woken
    /// while it waits: by the timer that ends the delay, since the task polls both of the calls
    /// that are zipped, and by the bytes arriving. A thread that spun would poll it hundreds of
    /// times.
    fn a_peek_or_a_read_that_waits_does_not_spin<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;
            let mut buffer = [0; 1];

            let peek_polls = Cell::new(0);
            let (peeked, written) = zip(
                counting_polls(&peek_polls, server.peek(&mut buffer)),
                after(&runtime, client.write_all(b"1")),
            )
            .await;
            assert_eq!(peeked.unwrap(), 1);
            written.unwrap();
            assert!(peek_polls.get() <= 10, "the peek was polled {} times", peek_polls.get());

            // The byte that was peeked at is read, and a read that has to wait comes next.
            server.read_exact(&mut buffer).await.unwrap();
            let read_polls = Cell::new(0);
            let (read, written) = zip(
                counting_polls(&read_polls, server.read_exact(&mut buffer)),
                after(&runtime, client.write_all(b"2")),
            )
            .await;
            read.unwrap();
            written.unwrap();
            assert!(read_polls.get() <= 10, "the read was polled {} times", read_polls.get());
        });
    }
}

in_both_modes! {
    /// Giving up an `accept` that is waiting loses no connection that came in while it waited,
    /// though the runtime woke it for that connection: the next `accept` takes it.
    ///
    /// The connection is made while the `accept` waits, and the task sleeps long enough for the
    /// runtime to see the listener readable, which wakes the task the `accept` waited for, before
    /// the future is dropped without being polled again.
    fn a_dropped_accept_loses_no_connection<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let listener = TcpListener::bind(&runtime, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        runtime.block_on(async {
            // The `accept` is dropped at the end of this block, without being polled again.
            let client = {
                let mut accept = pin!(listener.accept());
                assert!(poll_once(accept.as_mut()).await.is_none());

                let client = TcpStream::connect(&runtime, address).await.unwrap();
                runtime.sleep(DELAY).await;

                client
            };

            let (_server, peer) = listener.accept().await.unwrap();

            assert_eq!(peer, client.local_addr().unwrap());
        });
    }
}

in_both_modes! {
    /// `incoming` waits while no connection does, and yields the stream of each connection that
    /// is made, in the order they were made.
    fn incoming_yields_one_stream_per_connection<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let listener = TcpListener::bind(&runtime, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        runtime.block_on(async {
            let mut incoming = listener.incoming();
            assert!(poll_once(incoming.next()).await.is_none());

            // The first connection is made after a delay, with the stream already waiting for it.
            let (first_accepted, first) = zip(
                incoming.next(),
                after(&runtime, TcpStream::connect(&runtime, address)),
            )
            .await;
            let first_accepted = first_accepted.expect("the stream never ends").unwrap();
            let first = first.unwrap();

            let second = TcpStream::connect(&runtime, address).await.unwrap();
            let third = TcpStream::connect(&runtime, address).await.unwrap();
            let second_accepted = incoming.next().await.expect("the stream never ends").unwrap();
            let third_accepted = incoming.next().await.expect("the stream never ends").unwrap();

            assert_eq!(first_accepted.peer_addr().unwrap(), first.local_addr().unwrap());
            assert_eq!(second_accepted.peer_addr().unwrap(), second.local_addr().unwrap());
            assert_eq!(third_accepted.peer_addr().unwrap(), third.local_addr().unwrap());
        });
    }
}

in_both_modes! {
    /// `peek` waits for bytes and copies them out without taking them off the stream, so the
    /// next read finds the same bytes again.
    fn peek_leaves_the_bytes_for_the_next_read<M>() {
        let runtime = Runtime::<M>::new().unwrap();

        runtime.block_on(async {
            let (mut client, mut server) = connected(&runtime).await;

            // Written after a delay, with the `peek` already waiting for it.
            let mut peeked = [0; 16];
            let (written, peeked_len) = zip(
                after(&runtime, client.write_all(b"peeked")),
                server.peek(&mut peeked),
            )
            .await;
            written.unwrap();
            let peeked_len = peeked_len.unwrap();

            assert!(peeked_len > 0);
            assert_eq!(peeked[..peeked_len], b"peeked"[..peeked_len]);

            let mut read = [0; 6];
            server.read_exact(&mut read).await.unwrap();
            assert_eq!(&read, b"peeked");
        });
    }
}

in_both_modes! {
    /// The time-to-live and the `TCP_NODELAY` option read back what was set.
    fn ttl_and_nodelay_read_back_what_was_set<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let listener = TcpListener::bind(&runtime, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        listener.set_ttl(42).unwrap();
        assert_eq!(listener.ttl().unwrap(), 42);

        runtime.block_on(async {
            let stream = TcpStream::connect(&runtime, address).await.unwrap();

            stream.set_ttl(43).unwrap();
            assert_eq!(stream.ttl().unwrap(), 43);

            stream.set_nodelay(true).unwrap();
            assert!(stream.nodelay().unwrap());
            stream.set_nodelay(false).unwrap();
            assert!(!stream.nodelay().unwrap());
        });
    }
}

/// A stream built on a shared runtime moves to another thread, which uses it there: that thread
/// drives the runtime with a `block_on` of its own, after the thread that made the stream has left
/// the one it made it in, since a runtime made by `new` is driven by one thread at a time.
#[test]
#[timeout(15000)]
fn a_shared_stream_crosses_threads() {
    let runtime = SharedRuntime::new().unwrap();
    let (client, mut server) = runtime.block_on(connected(&runtime));

    let user = thread::spawn({
        let runtime = runtime.clone();
        move || {
            runtime.block_on(async move {
                let mut client = client;
                let mut request = [0; 5];
                // Waits for the bytes, with the reactor of the runtime driven by this thread.
                client.read_exact(&mut request).await.unwrap();
                client
                    .write_all(&request.to_ascii_uppercase())
                    .await
                    .unwrap();
            });
        }
    });

    // This thread is inside no `block_on`, so it leaves the runtime to the other one: a write
    // that has room goes to the kernel without the runtime, and any executor can poll it.
    thread::sleep(DELAY);
    block_on(server.write_all(b"hello")).unwrap();
    user.join().unwrap();

    let mut response = [0; 5];
    runtime.block_on(server.read_exact(&mut response)).unwrap();
    assert_eq!(&response, b"HELLO");
}

/// The tasks of a shared runtime use sockets: their futures are `Send`, which spawning one
/// checks, and the runtime runs them to the end.
#[test]
#[timeout(15000)]
fn shared_tasks_use_sockets() {
    let runtime = SharedRuntime::new().unwrap();
    let listener = TcpListener::bind(&runtime, loopback()).unwrap();
    let address = listener.local_addr().unwrap();

    runtime.block_on(async {
        let server = runtime.spawn("an echo server", async move {
            let (stream, _) = listener.accept().await.unwrap();
            copy(&stream, &stream).await.unwrap();
            (&stream).close().await.unwrap();
        });
        let client = runtime.spawn("a client", {
            let runtime = runtime.clone();
            async move {
                let mut stream = TcpStream::connect(&runtime, address).await.unwrap();
                stream.write_all(b"hello").await.unwrap();
                stream.close().await.unwrap();

                let mut echoed = Vec::new();
                stream.read_to_end(&mut echoed).await.unwrap();

                echoed
            }
        });

        assert_eq!(client.await.unwrap(), b"hello");
        server.await.unwrap();
    });
}

in_both_modes! {
    /// A socket prints as the std socket it was made from does, and its descriptor is that
    /// socket's: the one `AsRawFd` or `AsRawSocket` names, and the one a borrow of it reaches.
    fn sockets_print_and_expose_the_descriptors_of_the_std_sockets_they_wrap<M>() {
        let runtime = Runtime::<M>::new().unwrap();
        let std_listener = std::net::TcpListener::bind(loopback()).unwrap();
        let address = std_listener.local_addr().unwrap();
        let std_stream = std::net::TcpStream::connect(address).unwrap();
        let printed_listener = format!("{std_listener:?}");
        let printed_stream = format!("{std_stream:?}");
        #[cfg(unix)]
        let (raw_listener, raw_stream) = (std_listener.as_raw_fd(), std_stream.as_raw_fd());
        #[cfg(windows)]
        let (raw_listener, raw_stream) =
            (std_listener.as_raw_socket(), std_stream.as_raw_socket());

        let listener = TcpListener::from_std(&runtime, std_listener).unwrap();
        let stream = TcpStream::from_std(&runtime, std_stream).unwrap();

        assert_eq!(format!("{listener:?}"), printed_listener);
        assert_eq!(format!("{stream:?}"), printed_stream);
        assert_eq!(
            format!("{:?}", listener.incoming()),
            format!("Incoming {{ listener: {printed_listener} }}"),
        );

        #[cfg(unix)]
        {
            assert_eq!(listener.as_raw_fd(), raw_listener);
            assert_eq!(stream.as_raw_fd(), raw_stream);
        }
        #[cfg(windows)]
        {
            assert_eq!(listener.as_raw_socket(), raw_listener);
            assert_eq!(stream.as_raw_socket(), raw_stream);
        }
        // `SockRef` asks the system about the descriptor it is given, which is how the borrow is
        // seen to be of the socket itself: it finds the addresses that socket has.
        let listener_address = SockRef::from(&listener).local_addr().unwrap();
        assert_eq!(listener_address.as_socket(), Some(address));
        let stream_peer = SockRef::from(&stream).peer_addr().unwrap();
        assert_eq!(stream_peer.as_socket(), Some(address));
    }
}

/// A shared runtime's sockets are `Send` and `Sync`, and `Unpin`, which the extension traits of
/// futures-lite ask of what they read from, write to or iterate over.
///
/// Only the bounds are checked here, at compile time. That a stream does cross threads, and that
/// the tasks of a shared runtime use sockets, is shown by `a_shared_stream_crosses_threads` and
/// `shared_tasks_use_sockets`.
#[test]
#[timeout(15000)]
fn shared_sockets_are_send_sync_and_unpin() {
    fn sent_and_shared<T>()
    where
        T: Send + Sync + Unpin,
    {
    }

    sent_and_shared::<TcpListener<Shared>>();
    sent_and_shared::<TcpStream<Shared>>();
    sent_and_shared::<Incoming<'static, Shared>>();
}

/// A local runtime's sockets are `Unpin` too. That they are neither `Send` nor `Sync` is shown by
/// the compile-fail doc tests, since only code that does not compile can show it.
#[test]
#[timeout(15000)]
fn local_sockets_are_unpin() {
    fn unpin<T>()
    where
        T: Unpin,
    {
    }

    unpin::<TcpListener<Local>>();
    unpin::<TcpStream<Local>>();
    unpin::<Incoming<'static, Local>>();
}

/// A connected pair: the end that connected, and the end the listener accepted.
async fn connected<M>(runtime: &Runtime<M>) -> (TcpStream<M>, TcpStream<M>)
where
    M: Mode,
{
    let listener = TcpListener::bind(runtime, loopback()).unwrap();
    let client = TcpStream::connect(runtime, listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();

    (client, server)
}
