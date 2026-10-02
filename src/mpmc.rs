//! An async multi-producer multi-consumer channel, each of whose messages one receiver gets.
//!
//! A channel is made by [`bounded`] or [`unbounded`], which hand out its first [`Sender`] and
//! [`Receiver`]. Both sides can be cloned. A sender or a receiver can be sent to another thread,
//! and shared with it, as long as the messages can be sent: `T` has to be
//! [`Send`](std::marker::Send). A message is moved into the channel by a sender and out of it by a
//! receiver, and never cloned, so it need not be [`Clone`]. Messages come out in the order they
//! went in, and each of them goes to one receiver.
//!
//! A bounded channel holds at most as many messages as its capacity. A [`send`](Sender::send) into
//! a full one waits until a receiver has made room, and a [`try_send`](Sender::try_send) fails. An
//! unbounded channel holds any number of messages, and a send into it never waits.
//!
//! A receiver receives through `&self`, so one receiver can be shared by reference among tasks that
//! each wait for the next message. It is also a [`Stream`] of the messages it receives, which ends
//! once the channel is closed and has none left.
//!
//! A channel is closed by [`Sender::close`] or [`Receiver::close`], by its last sender being
//! dropped, and by its last receiver being dropped. No message can be sent into a closed channel: a
//! send fails and hands its message back. The messages in it can still be received, and only once
//! none is left does a receive fail as well. When the last receiver is dropped, the messages left
//! in the channel are dropped there and then, as nothing can receive them any more.
//!
//! The channel is built on [`Event`] and needs no runtime. It works under any executor, and from
//! any thread, inside a task or outside of one: a thread with no task to run can wait for a message
//! with the `block_on` of any executor, as the example below does.
//!
//! # Example
//!
//! Two senders and two receivers, on a channel that holds two messages:
//!
//! ```
//! use std::num::NonZeroUsize;
//!
//! use futures_lite::future::block_on;
//! use zruntime::mpmc::{RecvError, TrySendError, bounded};
//!
//! block_on(async {
//!     let (s1, r1) = bounded(NonZeroUsize::new(2).unwrap());
//!     let s2 = s1.clone();
//!     let r2 = r1.clone();
//!
//!     // Two messages, from two different senders.
//!     s1.send("hello").await.unwrap();
//!     s2.send("world").await.unwrap();
//!
//!     // The channel is full, so a third message has to wait for room, or be tried again later.
//!     assert_eq!(s1.try_send("again"), Err(TrySendError::Full("again")));
//!
//!     // Each message goes to one receiver, the oldest first.
//!     assert_eq!(r1.recv().await, Ok("hello"));
//!     assert_eq!(r2.recv().await, Ok("world"));
//!     assert!(r1.try_recv().unwrap_err().is_empty());
//!
//!     // The last sender being dropped closes the channel, which the receivers find out once
//!     // there is no message left in it.
//!     drop(s1);
//!     drop(s2);
//!     assert_eq!(r1.recv().await, Err(RecvError));
//!     assert_eq!(r2.recv().await, Err(RecvError));
//! });
//! ```
//!
//! # Giving up a wait
//!
//! Dropping the future that [`Sender::send`] or [`Receiver::recv`] returned, before it completes,
//! is fine: a timeout may do it, or a `select` that goes another way. A [`Recv`] that is dropped
//! has taken no message, and a [`Send`] that is dropped has sent none: its message is dropped with
//! it. A task that was woken for a message, or for room, and gives up passes that on to the next
//! task waiting for it, so none is left stranded.
//!
//! # Difference with `broadcast`
//!
//! The [`broadcast`] module of this crate, which has a feature of its own, has the same two sides.
//! There, each message is delivered to every receiver, as a clone of it, so the messages have to be
//! [`Clone`]; here, each message is delivered to one receiver only, and moved to it. Use that one
//! where every receiver is to see everything, and this one where the receivers share the work.
//!
//! # Difference with `async-channel`
//!
//! This channel is modelled on [`async-channel`], and differs from it in these places:
//!
//! * The capacity of [`bounded`] is a [`NonZeroUsize`]. A channel that holds no message is a type
//!   error here, where `bounded(0)` of `async-channel` panics.
//! * The last receiver being dropped drops the messages left in the channel. In `async-channel`, it
//!   only closes the channel, and the messages stay in it until the channel itself is freed.
//! * There is no blocking API. Where `async-channel` has `send_blocking` and `recv_blocking`, a
//!   thread with no task to run waits with the `block_on` of an executor.
//!
//! # Performance
//!
//! This channel was timed against [`async-channel`] 2.5.0 and against the [`mpsc`] channel of
//! tokio 1.53.1, which has one receiver and so takes no part where there are more. The messages are
//! `u64`s, and the machine a shared x86-64 one of four cores. Each thread runs its whole part
//! inside one `block_on` of `futures-lite`, and the tasks run on a tokio runtime of four workers.
//! With threads or tasks, each sender sends 10,000 messages and a row times them all, except the
//! one that says it times a message. The times are medians: those on one thread held to within 3%
//! from run to run, the others moved by up to 30%. A send to tokio's unbounded channel is not a
//! future, which spares it a poll.
//!
//! | What is timed                                             | `mpmc` | async-channel |  tokio |
//! |-----------------------------------------------------------|-------:|--------------:|-------:|
//! | A send and a receive, one thread, capacity 1              |  55 ns |        112 ns | 100 ns |
//! | A send and a receive, one thread, unbounded               |  54 ns |        123 ns |  56 ns |
//! | 1024 `try_send`s then 1024 `try_recv`s, capacity 1024     |  36 µs |         99 µs |  67 µs |
//! | 1024 `try_send`s then 1024 `try_recv`s, unbounded         |  36 µs |        112 µs |  47 µs |
//! | 4 sender and 4 receiver threads, capacity 16              |  72 ms |        240 ms |        |
//! | 4 sender and 4 receiver threads, unbounded                | 9.7 ms |         16 ms |        |
//! | 4 sender tasks and 1 receiver task, capacity 16           |  12 ms |         33 ms |  13 ms |
//! | 4 sender and 4 receiver tasks, capacity 16                |  18 ms |         24 ms |        |
//! | 1 sender task and 1 receiver task, capacity 16, a message | 280 ns |        550 ns | 540 ns |
//! | 1 sender thread and 1 receiver thread, capacity 16        |  23 ms |         20 ms |  22 ms |
//! | 1 sender thread and 1 receiver thread, capacity 1024      | 2.3 ms |        1.7 ms | 2.8 ms |
//! | 4 sender threads and 1 receiver thread, capacity 16       | 470 ms |        500 ms | 460 ms |
//!
//! In the last three rows, threads hand messages to each other through a channel that keeps
//! filling or emptying, so a side has to wait every few messages. A wait parks the thread, and the
//! time the OS takes to wake one, tens of microseconds on that machine, makes up nearly all of the
//! time: there, the channels differ in how often a side ended up waiting, not in what an operation
//! costs. Between tasks, whose wakes are cheap, the same exchange takes this channel half as long
//! as the other two, as the row above them shows.
//!
//! [`Event`]: crate::Event
//! [`Stream`]: futures_core::Stream
//! [`NonZeroUsize`]: std::num::NonZeroUsize
//! [`broadcast`]: https://docs.rs/zruntime/latest/zruntime/broadcast/index.html
//! [`async-channel`]: https://crates.io/crates/async-channel
//! [`mpsc`]: https://docs.rs/tokio/1.53.1/tokio/sync/mpsc/index.html

use std::{
    any::Any,
    collections::VecDeque,
    error, fmt,
    future::Future,
    mem,
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll},
};

use futures_core::{
    ready,
    stream::{FusedStream, Stream},
};

use crate::{Event, EventListener};

/// Creates a channel that holds at most `cap` messages, and hands out its first sender and
/// receiver.
///
/// A [`send`](Sender::send) into a full channel waits until a receiver has made room, and a
/// [`try_send`](Sender::try_send) fails. The channel does not allocate room for `cap` messages up
/// front, but as they come, so a very large capacity costs nothing until the messages are there.
///
/// # Example
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use zruntime::mpmc::{TrySendError, bounded};
///
/// let (s, r) = bounded(NonZeroUsize::MIN);
///
/// s.try_send(1).unwrap();
/// // The channel holds one message at most.
/// assert_eq!(s.try_send(2), Err(TrySendError::Full(2)));
///
/// assert_eq!(r.try_recv(), Ok(1));
/// s.try_send(2).unwrap();
/// ```
pub fn bounded<T>(cap: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    channel(Some(cap))
}

/// Creates a channel that holds any number of messages, and hands out its first sender and
/// receiver.
///
/// A send into the channel never waits for room.
///
/// # Example
///
/// ```
/// use zruntime::mpmc::unbounded;
///
/// let (s, r) = unbounded();
///
/// for i in 0..100 {
///     s.try_send(i).unwrap();
/// }
/// assert_eq!(s.len(), 100);
/// assert_eq!(r.try_recv(), Ok(0));
/// ```
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(None)
}

/// The sending side of a channel.
///
/// Senders can be cloned and shared among threads. When the last sender is dropped, the channel is
/// closed: no more messages can be sent, and the ones in it can still be received.
///
/// The channel can also be closed by calling [`Sender::close`].
pub struct Sender<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Sender<T> {
    /// Sends a message into the channel.
    ///
    /// The returned future completes once the message is in the channel. If the channel is full,
    /// it waits until a receiver has made room, and an unbounded channel is never full. If the
    /// channel is closed, whether it was when this was called or is by the time there is room, the
    /// future fails with a [`SendError`], which hands the message back.
    ///
    /// Dropping the future before it completes sends nothing, and drops the message.
    ///
    /// # Example
    ///
    /// A send into a full channel, which waits for the receive beside it to make room:
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use futures_lite::future::{block_on, zip};
    /// use zruntime::mpmc::bounded;
    ///
    /// let (s, r) = bounded(NonZeroUsize::MIN);
    ///
    /// block_on(async {
    ///     s.send(1).await.unwrap();
    ///
    ///     let (sent, received) = zip(s.send(2), r.recv()).await;
    ///     assert_eq!(sent, Ok(()));
    ///     assert_eq!(received, Ok(1));
    ///
    ///     assert_eq!(r.recv().await, Ok(2));
    /// });
    /// ```
    pub fn send(&self, msg: T) -> Send<'_, T> {
        Send {
            sender: self,
            listener: None,
            msg: Some(msg),
        }
    }

    /// Tries to send a message into the channel, without waiting.
    ///
    /// Fails with [`TrySendError::Full`] if the channel is full, and with [`TrySendError::Closed`]
    /// if it is closed, whether it has room or not. Either hands the message back.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use zruntime::mpmc::{TrySendError, bounded};
    ///
    /// let (s, r) = bounded(NonZeroUsize::MIN);
    ///
    /// assert_eq!(s.try_send(1), Ok(()));
    /// assert_eq!(s.try_send(2), Err(TrySendError::Full(2)));
    ///
    /// drop(r);
    /// assert_eq!(s.try_send(3), Err(TrySendError::Closed(3)));
    /// ```
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        self.channel.try_send(msg)
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call closed the channel, and `false` if it was closed already.
    ///
    /// No more messages can be sent into a closed channel: the sends waiting for room fail, and so
    /// does every send after. The messages in it can still be received.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::{TryRecvError, TrySendError, unbounded};
    ///
    /// let (s, r) = unbounded();
    /// s.try_send(1).unwrap();
    ///
    /// assert!(s.close());
    /// assert!(!s.close());
    /// assert_eq!(s.try_send(2), Err(TrySendError::Closed(2)));
    ///
    /// assert_eq!(r.try_recv(), Ok(1));
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Closed));
    /// ```
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Whether the channel is closed.
    ///
    /// A channel is closed by [`Sender::close`] or [`Receiver::close`], by its last sender being
    /// dropped, and by its last receiver being dropped. A closed channel can still hold messages
    /// for its receivers.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r) = unbounded::<()>();
    /// assert!(!s.is_closed());
    ///
    /// drop(r);
    /// assert!(s.is_closed());
    /// ```
    pub fn is_closed(&self) -> bool {
        lock(&self.channel.state).closed
    }

    /// Whether the channel holds no message.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, _r) = unbounded();
    /// assert!(s.is_empty());
    ///
    /// s.try_send(1).unwrap();
    /// assert!(!s.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        lock(&self.channel.state).queue.is_empty()
    }

    /// Whether the channel is full: it holds as many messages as its capacity.
    ///
    /// An unbounded channel is never full.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use zruntime::mpmc::{bounded, unbounded};
    ///
    /// let (s, _r) = bounded(NonZeroUsize::MIN);
    /// assert!(!s.is_full());
    /// s.try_send(1).unwrap();
    /// assert!(s.is_full());
    ///
    /// let (s, _r) = unbounded();
    /// s.try_send(1).unwrap();
    /// assert!(!s.is_full());
    /// ```
    pub fn is_full(&self) -> bool {
        lock(&self.channel.state).is_full()
    }

    /// The number of messages in the channel.
    ///
    /// Other threads may send and receive in the meantime, so the number may be out of date by the
    /// time this returns.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r) = unbounded();
    /// assert_eq!(s.len(), 0);
    ///
    /// s.try_send(1).unwrap();
    /// s.try_send(2).unwrap();
    /// assert_eq!(s.len(), 2);
    ///
    /// r.try_recv().unwrap();
    /// assert_eq!(s.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        lock(&self.channel.state).queue.len()
    }

    /// The number of messages the channel holds at most, or `None` if it is unbounded.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use zruntime::mpmc::{bounded, unbounded};
    ///
    /// let (s, _r) = bounded::<()>(NonZeroUsize::new(5).unwrap());
    /// assert_eq!(s.capacity(), NonZeroUsize::new(5));
    ///
    /// let (s, _r) = unbounded::<()>();
    /// assert_eq!(s.capacity(), None);
    /// ```
    pub fn capacity(&self) -> Option<NonZeroUsize> {
        lock(&self.channel.state).capacity
    }

    /// The number of senders of the channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s1, _r) = unbounded::<()>();
    /// assert_eq!(s1.sender_count(), 1);
    ///
    /// let s2 = s1.clone();
    /// assert_eq!(s1.sender_count(), 2);
    ///
    /// drop(s2);
    /// assert_eq!(s1.sender_count(), 1);
    /// ```
    pub fn sender_count(&self) -> usize {
        lock(&self.channel.state).senders
    }

    /// The number of receivers of the channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r1) = unbounded::<()>();
    /// assert_eq!(s.receiver_count(), 1);
    ///
    /// let r2 = r1.clone();
    /// assert_eq!(s.receiver_count(), 2);
    ///
    /// drop((r1, r2));
    /// assert_eq!(s.receiver_count(), 0);
    /// ```
    pub fn receiver_count(&self) -> usize {
        lock(&self.channel.state).receivers
    }
}

impl<T> Clone for Sender<T> {
    /// Makes another sender of the same channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::{TryRecvError, unbounded};
    ///
    /// let (s1, r) = unbounded();
    /// let s2 = s1.clone();
    ///
    /// // The channel stays open for as long as a sender is left.
    /// drop(s1);
    /// s2.try_send(1).unwrap();
    /// assert_eq!(r.try_recv(), Ok(1));
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    ///
    /// drop(s2);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Closed));
    /// ```
    fn clone(&self) -> Self {
        lock(&self.channel.state).senders += 1;

        Sender {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let closed = {
            let mut state = lock(&self.channel.state);
            state.senders -= 1;

            state.senders == 0 && state.close()
        };

        if closed {
            self.channel.notify_closed();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.channel.fmt_end("Sender", f)
    }
}

/// The receiving side of a channel.
///
/// Receivers can be cloned and shared among threads, or by reference, as [`Receiver::recv`] takes
/// `&self`. Each message in the channel goes to one of them. When the last receiver is dropped, the
/// channel is closed, and the messages left in it are dropped, as nothing can receive them any
/// more.
///
/// The channel can also be closed by calling [`Receiver::close`].
///
/// A receiver is also a [`Stream`] of the messages it receives, which ends once the channel is
/// closed and has none left, and a [`FusedStream`].
///
/// # Example
///
/// ```
/// use futures_lite::{future::block_on, stream::StreamExt};
/// use zruntime::mpmc::unbounded;
///
/// let (s, r) = unbounded();
/// s.try_send(1).unwrap();
/// s.try_send(2).unwrap();
/// drop(s);
///
/// assert_eq!(block_on(r.collect::<Vec<_>>()), [1, 2]);
/// ```
///
/// [`Stream`]: futures_core::Stream
/// [`FusedStream`]: futures_core::stream::FusedStream
pub struct Receiver<T> {
    channel: Arc<Channel<T>>,
    /// What the stream of this receiver waits on, kept from one poll of it to the next. A `recv`
    /// keeps its own listener in its future, so the two never share one.
    listener: Option<EventListener>,
}

impl<T> Receiver<T> {
    /// Receives a message from the channel.
    ///
    /// The returned future completes with the oldest message in the channel. If there is none, it
    /// waits until one is sent. If the channel is closed, the messages left in it can still be
    /// received, and the future fails with a [`RecvError`] once none is.
    ///
    /// Dropping the future before it completes takes no message from the channel.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::mpmc::{RecvError, unbounded};
    ///
    /// let (s, r) = unbounded();
    /// s.try_send(1).unwrap();
    /// drop(s);
    ///
    /// block_on(async {
    ///     assert_eq!(r.recv().await, Ok(1));
    ///     // The channel is closed, and there is no message left in it.
    ///     assert_eq!(r.recv().await, Err(RecvError));
    /// });
    /// ```
    pub fn recv(&self) -> Recv<'_, T> {
        Recv {
            receiver: self,
            listener: None,
        }
    }

    /// Tries to receive a message from the channel, without waiting.
    ///
    /// Fails with [`TryRecvError::Empty`] if the channel holds no message and is not closed, and
    /// with [`TryRecvError::Closed`] if it holds none and is closed. A closed channel still hands
    /// out the messages left in it.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::{TryRecvError, unbounded};
    ///
    /// let (s, r) = unbounded();
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    ///
    /// s.try_send(1).unwrap();
    /// assert_eq!(r.try_recv(), Ok(1));
    ///
    /// drop(s);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Closed));
    /// ```
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.channel.try_recv()
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call closed the channel, and `false` if it was closed already.
    ///
    /// No more messages can be sent into a closed channel: the sends waiting for room fail, and so
    /// does every send after. The messages in it can still be received.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::{TryRecvError, TrySendError, unbounded};
    ///
    /// let (s, r) = unbounded();
    /// s.try_send(1).unwrap();
    ///
    /// assert!(r.close());
    /// assert!(!r.close());
    /// assert_eq!(s.try_send(2), Err(TrySendError::Closed(2)));
    ///
    /// assert_eq!(r.try_recv(), Ok(1));
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Closed));
    /// ```
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Whether the channel is closed.
    ///
    /// See [`Sender::is_closed`] for what closes a channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r) = unbounded::<()>();
    /// assert!(!r.is_closed());
    ///
    /// drop(s);
    /// assert!(r.is_closed());
    /// ```
    pub fn is_closed(&self) -> bool {
        lock(&self.channel.state).closed
    }

    /// Whether the channel holds no message.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r) = unbounded();
    /// assert!(r.is_empty());
    ///
    /// s.try_send(1).unwrap();
    /// assert!(!r.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        lock(&self.channel.state).queue.is_empty()
    }

    /// Whether the channel is full: it holds as many messages as its capacity.
    ///
    /// An unbounded channel is never full.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use zruntime::mpmc::bounded;
    ///
    /// let (s, r) = bounded(NonZeroUsize::MIN);
    /// assert!(!r.is_full());
    ///
    /// s.try_send(1).unwrap();
    /// assert!(r.is_full());
    /// ```
    pub fn is_full(&self) -> bool {
        lock(&self.channel.state).is_full()
    }

    /// The number of messages in the channel.
    ///
    /// Other threads may send and receive in the meantime, so the number may be out of date by the
    /// time this returns.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s, r) = unbounded();
    /// s.try_send(1).unwrap();
    /// s.try_send(2).unwrap();
    /// assert_eq!(r.len(), 2);
    ///
    /// r.try_recv().unwrap();
    /// assert_eq!(r.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        lock(&self.channel.state).queue.len()
    }

    /// The number of messages the channel holds at most, or `None` if it is unbounded.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use zruntime::mpmc::{bounded, unbounded};
    ///
    /// let (_s, r) = bounded::<()>(NonZeroUsize::new(5).unwrap());
    /// assert_eq!(r.capacity(), NonZeroUsize::new(5));
    ///
    /// let (_s, r) = unbounded::<()>();
    /// assert_eq!(r.capacity(), None);
    /// ```
    pub fn capacity(&self) -> Option<NonZeroUsize> {
        lock(&self.channel.state).capacity
    }

    /// The number of senders of the channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (s1, r) = unbounded::<()>();
    /// let s2 = s1.clone();
    /// assert_eq!(r.sender_count(), 2);
    ///
    /// drop((s1, s2));
    /// assert_eq!(r.sender_count(), 0);
    /// ```
    pub fn sender_count(&self) -> usize {
        lock(&self.channel.state).senders
    }

    /// The number of receivers of the channel.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::unbounded;
    ///
    /// let (_s, r1) = unbounded::<()>();
    /// assert_eq!(r1.receiver_count(), 1);
    ///
    /// let r2 = r1.clone();
    /// assert_eq!(r1.receiver_count(), 2);
    ///
    /// drop(r2);
    /// assert_eq!(r1.receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        lock(&self.channel.state).receivers
    }
}

impl<T> Clone for Receiver<T> {
    /// Makes another receiver of the same channel.
    ///
    /// The messages in the channel are not copied: they are shared by all the receivers, and each
    /// of them goes to one.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::mpmc::{TryRecvError, unbounded};
    ///
    /// let (s, r1) = unbounded();
    /// let r2 = r1.clone();
    /// s.try_send(1).unwrap();
    ///
    /// // The message goes to the receiver that asks for it first, and to no other.
    /// assert_eq!(r2.try_recv(), Ok(1));
    /// assert_eq!(r1.try_recv(), Err(TryRecvError::Empty));
    /// ```
    fn clone(&self) -> Self {
        lock(&self.channel.state).receivers += 1;

        Receiver {
            channel: self.channel.clone(),
            listener: None,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // The receiver is counted out, and the channel closed if it was the last, before any of
        // somebody else's code runs below: the waker the stream's listener holds, the wakers of
        // the operations waiting, and the `Drop` of the messages left. Each may panic, and none is
        // to leave the channel counting a receiver that is gone.
        let (closed, left) = {
            let mut state = lock(&self.channel.state);
            state.receivers -= 1;
            if state.receivers > 0 {
                (false, VecDeque::new())
            } else {
                // Nothing can receive the messages any more, and no sender can make a receiver.
                (state.close(), mem::take(&mut state.queue))
            }
        };

        let listener = self.listener.take();
        run_both(
            // Let go of the stream's listener first: closing the channel would notify it, and
            // wake the task that last polled the stream, for a receiver that is gone.
            || drop(listener),
            || {
                run_both(
                    || {
                        if closed {
                            self.channel.notify_closed();
                        }
                    },
                    // The messages left go last, once every operation waiting has been told.
                    || drop(left),
                )
            },
        );
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.channel.fmt_end("Receiver", f)
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();

        loop {
            // A listener this stream holds is polled before anything else, as `Recv::poll` does.
            if let Some(listener) = &mut this.listener {
                ready!(Pin::new(listener).poll(cx));
                this.listener = None;
            }

            loop {
                match this.channel.try_recv() {
                    Ok(msg) => {
                        this.listener = None;

                        return Poll::Ready(Some(msg));
                    }
                    Err(TryRecvError::Closed) => {
                        this.listener = None;

                        return Poll::Ready(None);
                    }
                    Err(TryRecvError::Empty) => {}
                }

                // Nothing to receive yet: listen, then try again, and wait on the listener if that
                // finds nothing too.
                match this.listener {
                    None => this.listener = Some(this.channel.stream_ops.listen_unfenced()),
                    Some(_) => break,
                }
            }
        }
    }
}

impl<T> FusedStream for Receiver<T> {
    fn is_terminated(&self) -> bool {
        let state = lock(&self.channel.state);

        state.closed && state.queue.is_empty()
    }
}

/// The future that [`Sender::send`] returns.
///
/// It completes with `Ok(())` once the message is in the channel, and with a [`SendError`] that
/// hands the message back if the channel is closed.
///
/// # Panics
///
/// Polling it again after it completed panics: the message is gone by then.
#[must_use = "futures do nothing unless .awaited"]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    /// What this future waits on while the channel is full, taken before its last try.
    listener: Option<EventListener>,
    /// The message, until it is in the channel or handed back.
    msg: Option<T>,
}

// The message is never pinned, so the future is `Unpin` whether the message is or not.
impl<T> Unpin for Send<'_, T> {}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // A listener this future holds is polled before anything else. One that completes has
            // taken its notification. One dropped while notified, as it would be by a try that
            // succeeds, passes it on to the next operation waiting, which would be woken for
            // nothing at every ordinary wakeup.
            if let Some(listener) = &mut this.listener {
                ready!(Pin::new(listener).poll(cx));
                this.listener = None;
            }

            loop {
                let msg = this
                    .msg
                    .take()
                    .expect("a `Send` is not polled again once it completed: its message is gone");

                match this.sender.try_send(msg) {
                    Ok(()) => {
                        // Let go of a listener this future took just now, which may have been
                        // notified since: it passes the notification on.
                        this.listener = None;

                        return Poll::Ready(Ok(()));
                    }
                    Err(TrySendError::Closed(msg)) => {
                        // Let go of a listener, as on success.
                        this.listener = None;

                        return Poll::Ready(Err(SendError(msg)));
                    }
                    Err(TrySendError::Full(msg)) => this.msg = Some(msg),
                }

                // No room yet: listen, then try again, and wait on the listener if that finds none
                // either.
                match this.listener {
                    None => this.listener = Some(this.sender.channel.send_ops.listen_unfenced()),
                    Some(_) => break,
                }
            }
        }
    }
}

impl<T> fmt::Debug for Send<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The message is left out: `T` need not be `Debug`.
        f.debug_struct("Send")
            .field("sender", self.sender)
            .field("waiting", &self.listener.is_some())
            .finish_non_exhaustive()
    }
}

/// The future that [`Receiver::recv`] returns.
///
/// It completes with the message it received, or with a [`RecvError`] if the channel is closed and
/// has none left.
#[must_use = "futures do nothing unless .awaited"]
pub struct Recv<'a, T> {
    receiver: &'a Receiver<T>,
    /// What this future waits on while the channel has no message, taken before its last try.
    listener: Option<EventListener>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // A listener this future holds is polled before anything else, as `Send::poll` does.
            if let Some(listener) = &mut this.listener {
                ready!(Pin::new(listener).poll(cx));
                this.listener = None;
            }

            loop {
                match this.receiver.try_recv() {
                    Ok(msg) => {
                        // Let go of a listener this future took just now, which may have been
                        // notified since: it passes the notification on.
                        this.listener = None;

                        return Poll::Ready(Ok(msg));
                    }
                    Err(TryRecvError::Closed) => {
                        // Let go of a listener, as on success.
                        this.listener = None;

                        return Poll::Ready(Err(RecvError));
                    }
                    Err(TryRecvError::Empty) => {}
                }

                // Nothing to receive yet: listen, then try again, and wait on the listener if that
                // finds nothing too.
                match this.listener {
                    None => {
                        this.listener = Some(this.receiver.channel.recv_ops.listen_unfenced());
                    }
                    Some(_) => break,
                }
            }
        }
    }
}

impl<T> fmt::Debug for Recv<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recv")
            .field("receiver", self.receiver)
            .field("waiting", &self.listener.is_some())
            .finish_non_exhaustive()
    }
}

/// The error of a [`Sender::send`] that failed: the channel is closed.
///
/// It holds the message that was not sent.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// The message that was not sent.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> error::Error for SendError<T> {}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the channel is closed")
    }
}

/// The error of a [`Sender::try_send`] that failed, which holds the message that was not sent.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The channel is full, and not closed.
    Full(T),
    /// The channel is closed.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// The message that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(msg) | TrySendError::Closed(msg) => msg,
        }
    }

    /// Whether the channel is full, and not closed.
    pub fn is_full(&self) -> bool {
        matches!(self, TrySendError::Full(_))
    }

    /// Whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        matches!(self, TrySendError::Closed(_))
    }
}

impl<T> error::Error for TrySendError<T> {}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("the channel is full"),
            TrySendError::Closed(_) => f.write_str("the channel is closed"),
        }
    }
}

/// The error of a [`Receiver::recv`] that failed: the channel is closed and has no message left.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct RecvError;

impl error::Error for RecvError {}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the channel is closed and has no message left")
    }
}

/// The error of a [`Receiver::try_recv`] that failed.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// The channel holds no message, and is not closed.
    Empty,
    /// The channel is closed and has no message left.
    Closed,
}

impl TryRecvError {
    /// Whether the channel holds no message, and is not closed.
    pub fn is_empty(&self) -> bool {
        matches!(self, TryRecvError::Empty)
    }

    /// Whether the channel is closed and has no message left.
    pub fn is_closed(&self) -> bool {
        matches!(self, TryRecvError::Closed)
    }
}

impl error::Error for TryRecvError {}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("the channel has no message"),
            TryRecvError::Closed => f.write_str("the channel is closed and has no message left"),
        }
    }
}

/// Makes a channel with room for `capacity` messages, or for any number if it is `None`, and its
/// first sender and receiver.
fn channel<T>(capacity: Option<NonZeroUsize>) -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel {
        state: Mutex::new(State {
            // Empty and unallocated, whatever the capacity. It says how many messages the queue may
            // hold, not how much room to get for them up front, which would fail for a capacity
            // as large as `NonZeroUsize::MAX`.
            queue: VecDeque::new(),
            capacity,
            senders: 1,
            receivers: 1,
            closed: false,
        }),
        send_ops: Event::new(),
        recv_ops: Event::new(),
        stream_ops: Event::new(),
    });

    let sender = Sender {
        channel: channel.clone(),
    };
    let receiver = Receiver {
        channel,
        listener: None,
    };

    (sender, receiver)
}

/// What the ends of a channel share: its state, behind a lock, and the three events that tell the
/// operations waiting on it that the state changed.
///
/// The events sit beside the lock and not behind it, because an operation notifies them only once
/// it has let go of the lock. A notification wakes tasks, and waking one runs its waker, which is
/// somebody else's code: it may panic, and it may come straight back to the channel. Run under the
/// lock, a waker that panics would leave the operation half done, with the poison-tolerant [`lock`]
/// hiding it, and a waker that came back would deadlock. So each operation locks, changes the
/// state and decides what to notify, lets go of the lock, notifies, and only then drops any message
/// that left the channel, which is somebody else's code too.
///
/// A panic in any of that leaves the rest of it done. The state is settled before the lock is let
/// go of, so a sender or receiver dropped is counted out, and the channel closed, whatever panics
/// after. And every notification is sent, and every message and listener dropped, even where one
/// before it panics, through [`run_both`]: a waker that panics on one event does not leave an
/// operation waiting on another for good.
///
/// Listening takes no lock either. [`Event`] keeps a notification from slipping in between a
/// listener being taken and its caller checking what it waits for, as long as whoever notifies has
/// changed that before it notifies, which is the order every operation here keeps. And every
/// operation that waits listens first and checks after, as [`Event`] asks: it takes a listener,
/// tries again, and waits on the listener only if that fails too.
///
/// The channel listens and notifies without the event's fences, through `Event::listen_unfenced`,
/// `Event::notify_unfenced` and `Event::notify_additional_unfenced`. Every operation checks what it
/// waits for under this lock, and every one that changes it does so under this lock and notifies
/// after: the lock then orders a check after the change, or the listener taken before the check
/// ahead of the notification.
///
/// What a change notifies, and how:
///
/// * A message sent is one more for a receiver to take. It notifies `recv_ops` with
///   `notify_additional(1)`, which reaches one more waiting [`Recv`]. The counting `notify(1)`
///   would not do: it reaches nobody where a `Recv` was notified already and has not been polled
///   yet, and that `Recv` takes one message at most, so another one would go on waiting with a
///   message in the channel. It notifies `stream_ops` as well, in full, for the reason below.
/// * A message received from a bounded channel is room for one more message. It notifies `send_ops`
///   with `notify_additional(1)`, for the same reason: one more waiting [`Send`], and whether or
///   not an earlier one is notified already. An unbounded channel has no sender waiting for room,
///   so a receive from it notifies nobody.
/// * A channel closed is news for every operation waiting on it, which are all notified, in full,
///   on all three events. Only the call that closed it notifies: an operation that listens after
///   that finds the channel closed by its check, and does not wait.
///
/// A receiver's stream waits on `stream_ops`, and not on `recv_ops` as a [`Recv`] does, and every
/// send notifies every stream waiting there. A stream keeps its listener in the [`Receiver`], from
/// one poll to the next, and so beyond the life of any one `next()` future that polled it. One that
/// is notified and then not polled again, because its `next()` lost a `select!` or its task is busy
/// elsewhere, keeps that notification until it is. Had it waited on `recv_ops`, the notification
/// could be the one a waiting `Recv` needed for the message sent, which would then wait with the
/// message in the channel, possibly for good. With the streams on an event of their own, and all of
/// them notified by every send, a stream never holds a notification anybody else needs: each one is
/// woken for each message, the one that polls first gets it, and the others find the channel empty
/// again and wait for the next.
///
/// A [`Send`] or a [`Recv`] that completes, either way, lets go of its listener before it returns,
/// so a future kept after that holds no notification. One dropped before it completes passes on the
/// notification its listener holds, as [`Event`] does for a listener dropped while notified, which
/// is what a cancelled operation is to do. And one that holds a listener polls it before it tries
/// again, and not after: a listener that completes has taken its notification, where one dropped
/// as the try succeeds would pass it on to the next operation waiting, for a wakeup it has no use
/// for.
struct Channel<T> {
    state: Mutex<State<T>>,
    /// Send operations waiting for room, in a bounded channel.
    send_ops: Event,
    /// [`Recv`] operations waiting for a message.
    recv_ops: Event,
    /// Receivers polled as streams, waiting for a message.
    stream_ops: Event,
}

impl<T> Channel<T> {
    /// Puts `msg` in the channel if it is open and has room, and tells the receivers waiting for a
    /// message if it did. Otherwise hands `msg` back, in the error.
    fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        {
            let mut state = lock(&self.state);
            if state.closed {
                return Err(TrySendError::Closed(msg));
            }
            if state.is_full() {
                return Err(TrySendError::Full(msg));
            }

            state.queue.push_back(msg);
        }

        // Both, even if a waker panics on the first: a stream left unwoken would wait with the
        // message in the channel.
        run_both(
            || {
                self.recv_ops.notify_additional_unfenced(1);
            },
            || {
                self.stream_ops.notify_unfenced(usize::MAX);
            },
        );

        Ok(())
    }

    /// Takes the oldest message out of the channel, if there is one, and tells a sender waiting for
    /// room if that made some.
    fn try_recv(&self) -> Result<T, TryRecvError> {
        let (msg, bounded) = {
            let mut state = lock(&self.state);
            let Some(msg) = state.queue.pop_front() else {
                return Err(if state.closed {
                    TryRecvError::Closed
                } else {
                    TryRecvError::Empty
                });
            };

            (msg, state.capacity.is_some())
        };

        if bounded {
            self.send_ops.notify_additional_unfenced(1);
        }

        Ok(msg)
    }

    /// Closes the channel, and tells the operations waiting on it.
    ///
    /// Returns `true` if this call closed the channel, and `false` if it was closed already.
    fn close(&self) -> bool {
        let closed = lock(&self.state).close();

        if closed {
            self.notify_closed();
        }

        closed
    }

    /// Notifies every operation waiting on the channel that it is closed, on all three events
    /// even if a waker panics on one of them.
    fn notify_closed(&self) {
        run_both(
            || {
                self.send_ops.notify_unfenced(usize::MAX);
            },
            || {
                run_both(
                    || {
                        self.recv_ops.notify_unfenced(usize::MAX);
                    },
                    || {
                        self.stream_ops.notify_unfenced(usize::MAX);
                    },
                )
            },
        );
    }

    /// Writes what an end of the channel, named `end`, says of the channel for its `Debug`.
    ///
    /// The numbers are copied out under the lock, and written once it is let go of: a `Formatter`
    /// is somebody else's code, which may panic, and may come back to the channel.
    fn fmt_end(&self, end: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (len, capacity, senders, receivers, closed) = {
            let state = lock(&self.state);

            (
                state.queue.len(),
                state.capacity,
                state.senders,
                state.receivers,
                state.closed,
            )
        };

        f.debug_struct(end)
            .field("len", &len)
            .field("capacity", &capacity)
            .field("senders", &senders)
            .field("receivers", &receivers)
            .field("closed", &closed)
            .finish()
    }
}

/// What a channel's lock keeps.
struct State<T> {
    /// The messages in the channel, the oldest first.
    queue: VecDeque<T>,
    /// How many messages the channel holds at most, or `None` for any number.
    capacity: Option<NonZeroUsize>,
    /// How many [`Sender`]s there are.
    senders: usize,
    /// How many [`Receiver`]s there are.
    receivers: usize,
    /// Whether the channel is closed.
    closed: bool,
}

impl<T> State<T> {
    /// Whether the channel holds as many messages as it may.
    fn is_full(&self) -> bool {
        self.capacity
            .is_some_and(|capacity| self.queue.len() >= capacity.get())
    }

    /// Closes the channel, and tells whether this call did, and not an earlier one.
    ///
    /// The caller that closed it is to notify every waiting operation once it has let go of the
    /// lock.
    fn close(&mut self) -> bool {
        !mem::replace(&mut self.closed, true)
    }
}

/// The value behind a lock, taken whether or not a panic poisoned it.
///
/// No user code runs under the channel's lock. A message is moved, never cloned, so there is no
/// `T::clone` to run. A message that leaves the channel is dropped once the lock is let go of, and
/// the wakers of the operations waiting are woken, cloned and dropped by [`Event`], clear of it.
/// Not even a `Formatter` is written to under it. What does run is this module's own code and the
/// `VecDeque`'s, which can panic only on a capacity that overflows, before it has changed anything.
/// So no panic can have left the state half-changed, and a lock that one poisoned is as good as one
/// that none did.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `first`, and then `second`, even where `first` panics.
///
/// For what the channel runs once it has let go of its lock: the notifications of its events and
/// the drops of its messages and listeners. Each runs somebody else's code, a waker or a message's
/// `Drop`, and one that panics is not to leave the rest undone, an operation waiting for a
/// notification that never comes or a message never dropped. Three steps nest as
/// `run_both(a, || run_both(b, c))`.
///
/// Where `first` panics, `second` runs as that panic unwinds, under `catch_unwind`: a panic of its
/// own is caught there, rather than unwinding out of a drop already unwinding, which would abort
/// the process, and the first panic goes on to the caller. Where `first` does not panic, `second`
/// runs as any call does, and so does a panic of its own. That path costs nothing beyond the two
/// calls, which inline: a send makes it on every message.
fn run_both<A, B>(first: A, second: B)
where
    A: FnOnce(),
    B: FnOnce(),
{
    let mut on_unwind = OnUnwind(Some(second));
    first();
    if let Some(second) = on_unwind.0.take() {
        second();
    }
}

/// A step that runs if a panic unwinds past it, unless it was taken out to run before.
///
/// A panic of the step's own is caught, and its payload disposed of: the first panic is the one
/// that goes on to the caller.
struct OnUnwind<F>(Option<F>)
where
    F: FnOnce();

impl<F> Drop for OnUnwind<F>
where
    F: FnOnce(),
{
    fn drop(&mut self) {
        // Still here only as a panic unwinds: [`run_both`] takes it out to run otherwise.
        if let Some(step) = self.0.take()
            && let Err(panic) = panic::catch_unwind(AssertUnwindSafe(step))
        {
            dispose(panic);
        }
    }
}

/// Drops the payload of a panic that is to go no further, with a panic of its destructor caught.
///
/// The payload is somebody else's value, and its `Drop` may panic in turn: out of the drop of an
/// [`OnUnwind`], as the first panic unwinds, that would abort the process. The payload of such a
/// second panic is dropped too, the same way, and only one that panics a third time is leaked, so
/// as not to follow a chain of destructors that each panic. The scheduler has a function like this
/// one too, but this module does not use it: the runtime may be left out of the build.
fn dispose(payload: Box<dyn Any + std::marker::Send>) {
    let Err(payload) = panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) else {
        return;
    };
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        mem::forget(payload);
    }
}
