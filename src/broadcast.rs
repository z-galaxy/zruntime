// Moved from the async-broadcast crate; see `LICENSE` for its copyright notices.

//! An async multi-producer multi-consumer broadcast channel.
//!
//! Each consumer gets a clone of every message sent on the channel. For obvious reasons, the
//! channel can only be used to broadcast types that implement [`Clone`].
//!
//! A channel has the [`Sender`] and [`Receiver`] side. Both sides are cloneable and can be shared
//! among multiple threads.
//!
//! When all `Sender`s or all `Receiver`s are dropped, the channel becomes closed. When a channel is
//! closed, no more messages can be sent, but remaining messages can still be received.
//!
//! The channel can also be closed manually by calling [`Sender::close()`] or [`Receiver::close()`].
//!
//! # Examples
//!
//! ```
//! use std::num::NonZeroUsize;
//!
//! use futures::{executor::block_on, stream::StreamExt};
//! use zruntime::broadcast::{TryRecvError, channel};
//!
//! block_on(async move {
//!     let (s1, mut r1) = channel(NonZeroUsize::new(2).unwrap());
//!     let s2 = s1.clone();
//!     let mut r2 = r1.clone();
//!
//!     // Send 2 messages from two different senders.
//!     s1.broadcast(7).await.unwrap();
//!     s2.broadcast(8).await.unwrap();
//!
//!     // Channel is now at capacity so sending more messages will result in an error.
//!     assert!(s2.try_broadcast(9).unwrap_err().is_full());
//!     assert!(s1.try_broadcast(10).unwrap_err().is_full());
//!
//!     // We can use the `next` method of the `Stream` implementation to receive messages.
//!     assert_eq!(r1.next().await.unwrap(), 7);
//!     assert_eq!(r1.recv().await.unwrap(), 8);
//!     assert_eq!(r2.next().await.unwrap(), 7);
//!     assert_eq!(r2.recv().await.unwrap(), 8);
//!
//!     // All receivers got all messages so channel is now empty.
//!     assert_eq!(r1.try_recv(), Err(TryRecvError::Empty));
//!     assert_eq!(r2.try_recv(), Err(TryRecvError::Empty));
//!
//!     // Drop both senders, which closes the channel.
//!     drop(s1);
//!     drop(s2);
//!
//!     assert_eq!(r1.try_recv(), Err(TryRecvError::Closed));
//!     assert_eq!(r2.try_recv(), Err(TryRecvError::Closed));
//! })
//! ```
//!
//! # Difference with `async-channel`
//!
//! This channel is similar to [`async-channel`] in that both provide an MPMC channel. The main
//! difference is that in `async-channel`, each message sent on the channel is only received by one
//! of the receivers, while this channel delivers each message to every receiver (in other words,
//! it broadcasts) by cloning it for each receiver.
//!
//! A channel of that kind, built on [`Event`] like this one, is this crate's `mpmc` module,
//! behind the `mpmc` feature.
//!
//! # Difference with other broadcast channels
//!
//! * [`broadcaster`]: It has no sender and receiver split: both sides use clones of the same
//!   `BroadcastChannel` instance, and a message sent on one is sent to all of the clones. While
//!   this can work for many cases, without the split you will often find yourself having to drain
//!   the channel on the sending side yourself.
//!
//! * [`postage`]: It provides a [broadcast API][pba] similar to this one. However, it:
//!   - Does not support overflow mode, nor has the concept of inactive receivers, so a slow or
//!     inactive receiver blocking the whole channel is not a solvable problem.
//!   - Provides all kinds of channels, which is generally good but if you just need a broadcast
//!     channel, this module is probably a better choice.
//!
//! * [`tokio::sync`]: Tokio's `sync` module provides a [broadcast channel][tbc] API. The
//!   differences here are:
//!   - Tokio's channel has [overflow mode][tom] as its only behavior: a sender never waits for
//!     room. In this channel it is opt-in, through [`Sender::set_overflow`], and by default a
//!     sender waits until there is room.
//!   - There is no equivalent of inactive receivers.
//!   - The channel is part of a much larger crate, which you may not otherwise need, while this
//!     module needs nothing but [`Event`] and the standard library, and no runtime.
//!
//! [`async-channel`]: https://crates.io/crates/async-channel
//! [`broadcaster`]: https://crates.io/crates/broadcaster
//! [`postage`]: https://crates.io/crates/postage
//! [pba]: https://docs.rs/postage/latest/postage/broadcast/fn.channel.html
//! [`tokio::sync`]: https://docs.rs/tokio/latest/tokio/sync/index.html
//! [tbc]: https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html
//! [tom]: https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging

use std::{
    collections::VecDeque,
    error, fmt,
    future::Future,
    mem,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll},
};

use futures_core::{ready, stream::Stream};

use crate::{Event, EventListener};

/// Creates a new broadcast channel.
///
/// The created channel has space to hold at most `cap` messages at a time.
///
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use std::num::NonZeroUsize;
/// use zruntime::broadcast::{TryRecvError, TrySendError, channel};
///
/// let (s, mut r1) = channel(NonZeroUsize::new(1).unwrap());
/// let mut r2 = r1.clone();
///
/// assert_eq!(s.broadcast(10).await, Ok(None));
/// assert_eq!(s.try_broadcast(20), Err(TrySendError::Full(20)));
///
/// assert_eq!(r1.recv().await, Ok(10));
/// assert_eq!(r2.recv().await, Ok(10));
/// assert_eq!(r1.try_recv(), Err(TryRecvError::Empty));
/// assert_eq!(r2.try_recv(), Err(TryRecvError::Empty));
/// # });
/// ```
pub fn channel<T>(cap: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel {
        inner: Mutex::new(Inner {
            queue: VecDeque::with_capacity(cap.get()),
            capacity: cap,
            overflow: false,
            await_active: true,
            receiver_count: 1,
            inactive_receiver_count: 0,
            sender_count: 1,
            head_pos: 0,
            is_closed: false,
        }),
        send_ops: Event::new(),
        recv_ops: Event::new(),
    });

    let s = Sender {
        channel: channel.clone(),
    };
    let r = Receiver {
        channel,
        pos: 0,
        listener: None,
    };

    (s, r)
}

/// The sending side of the broadcast channel.
///
/// Senders can be cloned and shared among threads. When all senders associated with a channel are
/// dropped, the channel becomes closed.
///
/// The channel can also be closed manually by calling [`Sender::close()`].
#[derive(Debug)]
pub struct Sender<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Sender<T> {
    /// The channel's capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert_eq!(s.capacity().get(), 5);
    /// ```
    pub fn capacity(&self) -> NonZeroUsize {
        lock(&self.channel.inner).capacity
    }

    /// Sets the channel capacity.
    ///
    /// There are times when you need to change the channel's capacity after creating it. If the
    /// `new_cap` is less than the number of messages in the channel, the oldest messages will be
    /// dropped to shrink the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TryRecvError, TrySendError, channel};
    ///
    /// let (mut s, mut r) = channel::<i32>(NonZeroUsize::new(3).unwrap());
    /// assert_eq!(s.capacity().get(), 3);
    /// s.try_broadcast(1).unwrap();
    /// s.try_broadcast(2).unwrap();
    /// s.try_broadcast(3).unwrap();
    ///
    /// s.set_capacity(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.capacity().get(), 1);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Overflowed(2)));
    /// assert_eq!(r.try_recv().unwrap(), 3);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    /// s.try_broadcast(1).unwrap();
    /// assert_eq!(s.try_broadcast(2), Err(TrySendError::Full(2)));
    ///
    /// s.set_capacity(NonZeroUsize::new(2).unwrap());
    /// assert_eq!(s.capacity().get(), 2);
    /// s.try_broadcast(2).unwrap();
    /// assert_eq!(s.try_broadcast(2), Err(TrySendError::Full(2)));
    /// ```
    pub fn set_capacity(&mut self, new_cap: NonZeroUsize) {
        self.channel.set_capacity(new_cap);
    }

    /// Whether overflow mode is enabled on this channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert!(!s.overflow());
    /// ```
    pub fn overflow(&self) -> bool {
        lock(&self.channel.inner).overflow
    }

    /// Sets overflow mode on the channel.
    ///
    /// When overflow mode is set, broadcasting to the channel will succeed even if the channel is
    /// full. It achieves that by removing the oldest message from the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TryRecvError, TrySendError, channel};
    ///
    /// let (mut s, mut r) = channel::<i32>(NonZeroUsize::new(2).unwrap());
    /// s.try_broadcast(1).unwrap();
    /// s.try_broadcast(2).unwrap();
    /// assert_eq!(s.try_broadcast(3), Err(TrySendError::Full(3)));
    /// s.set_overflow(true);
    /// assert_eq!(s.try_broadcast(3).unwrap(), Some(1));
    /// assert_eq!(s.try_broadcast(4).unwrap(), Some(2));
    ///
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Overflowed(2)));
    /// assert_eq!(r.try_recv().unwrap(), 3);
    /// assert_eq!(r.try_recv().unwrap(), 4);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    /// ```
    pub fn set_overflow(&mut self, overflow: bool) {
        self.channel.set_overflow(overflow);
    }

    /// Whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert!(s.await_active());
    /// ```
    pub fn await_active(&self) -> bool {
        lock(&self.channel.inner).await_active
    }

    /// Sets whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{SendError, channel};
    ///
    /// let (mut s, r) = channel::<i32>(NonZeroUsize::new(2).unwrap());
    /// s.broadcast(1).await.unwrap();
    ///
    /// // The inactive receiver keeps the channel open, but there is no active one.
    /// let _inactive = r.deactivate();
    /// s.set_await_active(false);
    /// assert_eq!(s.broadcast(2).await, Err(SendError(2)));
    /// assert!(!s.is_closed());
    /// # });
    /// ```
    pub fn set_await_active(&mut self, await_active: bool) {
        self.channel.set_await_active(await_active);
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already.
    ///
    /// The remaining messages can still be received.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r) = channel(NonZeroUsize::new(1).unwrap());
    /// s.broadcast(1).await.unwrap();
    /// assert!(s.close());
    ///
    /// assert_eq!(r.recv().await.unwrap(), 1);
    /// assert_eq!(r.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Whether the channel is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert!(!s.is_closed());
    ///
    /// drop(r);
    /// assert!(s.is_closed());
    /// # });
    /// ```
    pub fn is_closed(&self) -> bool {
        lock(&self.channel.inner).is_closed
    }

    /// Whether the channel is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert!(s.is_empty());
    /// let _ = s.broadcast(1).await;
    /// assert!(!s.is_empty());
    /// # });
    /// ```
    pub fn is_empty(&self) -> bool {
        lock(&self.channel.inner).queue.is_empty()
    }

    /// Whether the channel is full.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert!(!s.is_full());
    /// let _ = s.broadcast(1).await;
    /// assert!(s.is_full());
    /// # });
    /// ```
    pub fn is_full(&self) -> bool {
        let inner = lock(&self.channel.inner);

        inner.queue.len() == inner.capacity.get()
    }

    /// The number of messages in the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(2).unwrap());
    /// assert_eq!(s.len(), 0);
    ///
    /// let _ = s.broadcast(1).await;
    /// let _ = s.broadcast(2).await;
    /// assert_eq!(s.len(), 2);
    /// # });
    /// ```
    pub fn len(&self) -> usize {
        lock(&self.channel.inner).queue.len()
    }

    /// The number of receivers for the channel.
    ///
    /// This does not include inactive receivers. Use [`Sender::inactive_receiver_count`] if you
    /// are interested in that.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        lock(&self.channel.inner).receiver_count
    }

    /// The number of inactive receivers for the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn inactive_receiver_count(&self) -> usize {
        lock(&self.channel.inner).inactive_receiver_count
    }

    /// The number of senders for the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.sender_count(), 1);
    ///
    /// let _s2 = s.clone();
    /// assert_eq!(s.sender_count(), 2);
    /// # });
    /// ```
    pub fn sender_count(&self) -> usize {
        lock(&self.channel.inner).sender_count
    }

    /// Creates a new [`Receiver`] for this channel.
    ///
    /// The new receiver starts with zero messages available. This will not re-open the channel if
    /// it was closed due to all receivers being dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r1) = channel(NonZeroUsize::new(2).unwrap());
    ///
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    ///
    /// let mut r2 = s.new_receiver();
    ///
    /// assert_eq!(s.broadcast(2).await, Ok(None));
    /// drop(s);
    ///
    /// assert_eq!(r1.recv().await, Ok(1));
    /// assert_eq!(r1.recv().await, Ok(2));
    /// assert_eq!(r1.recv().await, Err(RecvError::Closed));
    ///
    /// assert_eq!(r2.recv().await, Ok(2));
    /// assert_eq!(r2.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn new_receiver(&self) -> Receiver<T> {
        self.channel.new_receiver()
    }
}

impl<T: Clone> Sender<T> {
    /// Broadcasts a message on the channel.
    ///
    /// If the channel is full, this method waits until there is space for a message, unless
    /// overflow mode (set through [`Sender::set_overflow`]) is enabled, in which case it removes
    /// the oldest message from the channel to make room for the new message. The removed message
    /// is returned to the caller.
    ///
    /// If there are no active receivers, only inactive ones, this method waits until there is an
    /// active one, unless [`Sender::set_await_active`] has turned that off, in which case it
    /// resolves with a [`SendError`] immediately.
    ///
    /// If the channel is closed, this method returns an error.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{SendError, channel};
    ///
    /// let (s, r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    /// drop(r);
    /// assert_eq!(s.broadcast(2).await, Err(SendError(2)));
    /// # });
    /// ```
    pub fn broadcast(&self, msg: T) -> Send<'_, T> {
        Send {
            sender: self,
            listener: None,
            msg: Some(msg),
        }
    }

    /// Attempts to broadcast a message on the channel.
    ///
    /// If the channel is full, this method returns an error unless overflow mode (set through
    /// [`Sender::set_overflow`]) is enabled. If the overflow mode is enabled, it removes the
    /// oldest message from the channel to make room for the new message. The removed message
    /// is returned to the caller.
    ///
    /// If the channel is closed, this method returns an error.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TrySendError, channel};
    ///
    /// let (s, r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert_eq!(s.try_broadcast(1), Ok(None));
    /// assert_eq!(s.try_broadcast(2), Err(TrySendError::Full(2)));
    ///
    /// drop(r);
    /// assert_eq!(s.try_broadcast(3), Err(TrySendError::Closed(3)));
    /// ```
    pub fn try_broadcast(&self, msg: T) -> Result<Option<T>, TrySendError<T>> {
        let mut ret = None;
        let mut inner = lock(&self.channel.inner);

        if inner.is_closed {
            return Err(TrySendError::Closed(msg));
        } else if inner.receiver_count == 0 {
            assert!(inner.inactive_receiver_count != 0);

            return Err(TrySendError::Inactive(msg));
        } else if inner.queue.len() == inner.capacity.get() {
            if inner.overflow {
                // Make room by popping a message.
                ret = inner.queue.pop_front().map(|(m, _)| m);
            } else {
                return Err(TrySendError::Full(msg));
            }
        }
        let receiver_count = inner.receiver_count;
        inner.queue.push_back((msg, receiver_count));
        if ret.is_some() {
            inner.head_pos += 1;
        }
        drop(inner);

        // Notify all awaiting receive operations.
        self.channel.recv_ops.notify_unfenced(usize::MAX);

        Ok(ret)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let closed = {
            let mut inner = lock(&self.channel.inner);

            inner.sender_count -= 1;

            inner.sender_count == 0 && inner.close()
        };

        if closed {
            self.channel.notify_closed();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        lock(&self.channel.inner).sender_count += 1;

        Sender {
            channel: self.channel.clone(),
        }
    }
}

/// The receiving side of a channel.
///
/// Receivers can be cloned and shared among threads. When all (active) receivers associated with a
/// channel are dropped, the channel becomes closed. You can deactivate a receiver using
/// [`Receiver::deactivate`] if you would like the channel to remain open without keeping active
/// receivers around.
#[derive(Debug)]
pub struct Receiver<T> {
    channel: Arc<Channel<T>>,
    pos: u64,

    /// Listens for a send or close event to unblock this stream.
    listener: Option<EventListener>,
}

impl<T> Receiver<T> {
    /// The channel's capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (_s, r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert_eq!(r.capacity().get(), 5);
    /// ```
    pub fn capacity(&self) -> NonZeroUsize {
        lock(&self.channel.inner).capacity
    }

    /// Sets the channel capacity.
    ///
    /// There are times when you need to change the channel's capacity after creating it. If the
    /// `new_cap` is less than the number of messages in the channel, the oldest messages will be
    /// dropped to shrink the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TryRecvError, TrySendError, channel};
    ///
    /// let (s, mut r) = channel::<i32>(NonZeroUsize::new(3).unwrap());
    /// assert_eq!(r.capacity().get(), 3);
    /// s.try_broadcast(1).unwrap();
    /// s.try_broadcast(2).unwrap();
    /// s.try_broadcast(3).unwrap();
    ///
    /// r.set_capacity(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(r.capacity().get(), 1);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Overflowed(2)));
    /// assert_eq!(r.try_recv().unwrap(), 3);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    /// s.try_broadcast(1).unwrap();
    /// assert_eq!(s.try_broadcast(2), Err(TrySendError::Full(2)));
    ///
    /// r.set_capacity(NonZeroUsize::new(2).unwrap());
    /// assert_eq!(r.capacity().get(), 2);
    /// s.try_broadcast(2).unwrap();
    /// assert_eq!(s.try_broadcast(2), Err(TrySendError::Full(2)));
    /// ```
    pub fn set_capacity(&mut self, new_cap: NonZeroUsize) {
        self.channel.set_capacity(new_cap);
    }

    /// Whether overflow mode is enabled on this channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (_s, r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert!(!r.overflow());
    /// ```
    pub fn overflow(&self) -> bool {
        lock(&self.channel.inner).overflow
    }

    /// Sets overflow mode on the channel.
    ///
    /// When overflow mode is set, broadcasting to the channel will succeed even if the channel is
    /// full. It achieves that by removing the oldest message from the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TryRecvError, TrySendError, channel};
    ///
    /// let (s, mut r) = channel::<i32>(NonZeroUsize::new(2).unwrap());
    /// s.try_broadcast(1).unwrap();
    /// s.try_broadcast(2).unwrap();
    /// assert_eq!(s.try_broadcast(3), Err(TrySendError::Full(3)));
    /// r.set_overflow(true);
    /// assert_eq!(s.try_broadcast(3).unwrap(), Some(1));
    /// assert_eq!(s.try_broadcast(4).unwrap(), Some(2));
    ///
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Overflowed(2)));
    /// assert_eq!(r.try_recv().unwrap(), 3);
    /// assert_eq!(r.try_recv().unwrap(), 4);
    /// assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    /// ```
    pub fn set_overflow(&mut self, overflow: bool) {
        self.channel.set_overflow(overflow);
    }

    /// Whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (_, r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// assert!(r.await_active());
    /// ```
    pub fn await_active(&self) -> bool {
        lock(&self.channel.inner).await_active
    }

    /// Sets whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{SendError, channel};
    ///
    /// let (s, mut r) = channel::<i32>(NonZeroUsize::new(2).unwrap());
    /// s.broadcast(1).await.unwrap();
    ///
    /// r.set_await_active(false);
    /// // The inactive receiver keeps the channel open, but there is no active one.
    /// let _inactive = r.deactivate();
    /// assert_eq!(s.broadcast(2).await, Err(SendError(2)));
    /// assert!(!s.is_closed());
    /// # });
    /// ```
    pub fn set_await_active(&mut self, await_active: bool) {
        self.channel.set_await_active(await_active);
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already.
    ///
    /// The remaining messages can still be received.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r) = channel(NonZeroUsize::new(1).unwrap());
    /// s.broadcast(1).await.unwrap();
    /// assert!(s.close());
    ///
    /// assert_eq!(r.recv().await.unwrap(), 1);
    /// assert_eq!(r.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Whether the channel is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert!(!s.is_closed());
    ///
    /// drop(r);
    /// assert!(s.is_closed());
    /// # });
    /// ```
    pub fn is_closed(&self) -> bool {
        lock(&self.channel.inner).is_closed
    }

    /// Whether the channel is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert!(s.is_empty());
    /// let _ = s.broadcast(1).await;
    /// assert!(!s.is_empty());
    /// # });
    /// ```
    pub fn is_empty(&self) -> bool {
        lock(&self.channel.inner).queue.is_empty()
    }

    /// Whether the channel is full.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert!(!s.is_full());
    /// let _ = s.broadcast(1).await;
    /// assert!(s.is_full());
    /// # });
    /// ```
    pub fn is_full(&self) -> bool {
        let inner = lock(&self.channel.inner);

        inner.queue.len() == inner.capacity.get()
    }

    /// The number of messages in the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel(NonZeroUsize::new(2).unwrap());
    /// assert_eq!(s.len(), 0);
    ///
    /// let _ = s.broadcast(1).await;
    /// let _ = s.broadcast(2).await;
    /// assert_eq!(s.len(), 2);
    /// # });
    /// ```
    pub fn len(&self) -> usize {
        lock(&self.channel.inner).queue.len()
    }

    /// The number of receivers for the channel.
    ///
    /// This does not include inactive receivers. Use [`Receiver::inactive_receiver_count`] if you
    /// are interested in that.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        lock(&self.channel.inner).receiver_count
    }

    /// The number of inactive receivers for the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn inactive_receiver_count(&self) -> usize {
        lock(&self.channel.inner).inactive_receiver_count
    }

    /// The number of senders for the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, _r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.sender_count(), 1);
    ///
    /// let _s2 = s.clone();
    /// assert_eq!(s.sender_count(), 2);
    /// # });
    /// ```
    pub fn sender_count(&self) -> usize {
        lock(&self.channel.inner).sender_count
    }

    /// Downgrades to an [`InactiveReceiver`].
    ///
    /// An inactive receiver is one that can not and does not receive any messages. Its only purpose
    /// is to keep the associated channel open even when there are no (active) receivers. An
    /// inactive receiver can be upgraded into a [`Receiver`] using [`InactiveReceiver::activate`]
    /// or [`InactiveReceiver::activate_cloned`].
    ///
    /// [`Sender::try_broadcast`] will return [`TrySendError::Inactive`] if only inactive
    /// receivers exist for the associated channel and [`Sender::broadcast`] will wait until an
    /// active receiver is available.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TrySendError, channel};
    ///
    /// let (s, r) = channel(NonZeroUsize::new(1).unwrap());
    /// let inactive = r.deactivate();
    /// assert_eq!(s.try_broadcast(10), Err(TrySendError::Inactive(10)));
    ///
    /// let mut r = inactive.activate();
    /// assert_eq!(s.broadcast(10).await, Ok(None));
    /// assert_eq!(r.recv().await, Ok(10));
    /// # });
    /// ```
    pub fn deactivate(self) -> InactiveReceiver<T> {
        // Drop::drop impl of Receiver will take care of `receiver_count`.
        lock(&self.channel.inner).inactive_receiver_count += 1;

        InactiveReceiver {
            channel: self.channel.clone(),
        }
    }
}

impl<T: Clone> Receiver<T> {
    /// Receives a message from the channel.
    ///
    /// If there is no message for this receiver yet, this method waits until there is one or the
    /// channel is closed.
    ///
    /// If the channel is closed, this method still receives the messages that are left for this
    /// receiver, and returns [`RecvError::Closed`] once there are none.
    ///
    /// If this receiver has missed messages, because they were dropped from the channel before it
    /// got to them (in overflow mode, or by a reduction of the capacity), this method returns
    /// [`RecvError::Overflowed`] and readjusts its cursor to point to the first available
    /// message.
    ///
    /// A clone of the message that panics loses this receiver that message; the panic reaches the
    /// caller, and the channel stays usable.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r1) = channel(NonZeroUsize::new(1).unwrap());
    /// let mut r2 = r1.clone();
    ///
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    /// drop(s);
    ///
    /// assert_eq!(r1.recv().await, Ok(1));
    /// assert_eq!(r1.recv().await, Err(RecvError::Closed));
    /// assert_eq!(r2.recv().await, Ok(1));
    /// assert_eq!(r2.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv {
            receiver: self,
            listener: None,
        }
    }

    /// Attempts to receive a message from the channel.
    ///
    /// If there is no message for this receiver, this method returns an error:
    /// [`TryRecvError::Closed`] if the channel is closed, and [`TryRecvError::Empty`] otherwise. A
    /// closed channel still hands out the messages that are left for this receiver.
    ///
    /// If this receiver has missed messages, because they were dropped from the channel before it
    /// got to them (in overflow mode, or by a reduction of the capacity), this method returns
    /// [`TryRecvError::Overflowed`] and readjusts its cursor to point to the first available
    /// message.
    ///
    /// A clone of the message that panics loses this receiver that message; the panic reaches the
    /// caller, and the channel stays usable.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TryRecvError, channel};
    ///
    /// let (s, mut r1) = channel(NonZeroUsize::new(1).unwrap());
    /// let mut r2 = r1.clone();
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    ///
    /// assert_eq!(r1.try_recv(), Ok(1));
    /// assert_eq!(r1.try_recv(), Err(TryRecvError::Empty));
    /// assert_eq!(r2.try_recv(), Ok(1));
    /// assert_eq!(r2.try_recv(), Err(TryRecvError::Empty));
    ///
    /// drop(s);
    /// assert_eq!(r1.try_recv(), Err(TryRecvError::Closed));
    /// assert_eq!(r2.try_recv(), Err(TryRecvError::Closed));
    /// # });
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut inner = lock(&self.channel.inner);
        let overflow = inner.overflow;
        let received = inner.try_recv_at(&mut self.pos);
        // A message that came out by value was popped off the front of the queue, which made room.
        let popped = matches!(received, Ok(Ok(_)));
        // The only user code that runs under the lock: the clone of a message that other receivers
        // have yet to get.
        let received = received.map(|cow| cow.unwrap_or_else(T::clone));
        drop(inner);

        if popped && !overflow {
            // Notify 1 awaiting sender that there is now room. If there is still room in the
            // queue, the notified operation will notify another awaiting sender.
            self.channel.send_ops.notify_unfenced(1);
        }

        received
    }

    /// Creates a new [`Sender`] for this channel.
    ///
    /// This will not re-open the channel if it was closed due to all senders being dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s1, mut r) = channel(NonZeroUsize::new(2).unwrap());
    ///
    /// assert_eq!(s1.broadcast(1).await, Ok(None));
    ///
    /// let s2 = r.new_sender();
    ///
    /// assert_eq!(s2.broadcast(2).await, Ok(None));
    /// drop(s1);
    /// drop(s2);
    ///
    /// assert_eq!(r.recv().await, Ok(1));
    /// assert_eq!(r.recv().await, Ok(2));
    /// assert_eq!(r.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn new_sender(&self) -> Sender<T> {
        lock(&self.channel.inner).sender_count += 1;

        Sender {
            channel: self.channel.clone(),
        }
    }

    /// Creates a new [`Receiver`] for this channel.
    ///
    /// Unlike [`Receiver::clone`], this method creates a new receiver that starts with zero
    /// messages available. This is slightly faster than a real clone.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r1) = channel(NonZeroUsize::new(2).unwrap());
    ///
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    ///
    /// let mut r2 = r1.new_receiver();
    ///
    /// assert_eq!(s.broadcast(2).await, Ok(None));
    /// drop(s);
    ///
    /// assert_eq!(r1.recv().await, Ok(1));
    /// assert_eq!(r1.recv().await, Ok(2));
    /// assert_eq!(r1.recv().await, Err(RecvError::Closed));
    ///
    /// assert_eq!(r2.recv().await, Ok(2));
    /// assert_eq!(r2.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    pub fn new_receiver(&self) -> Self {
        self.channel.new_receiver()
    }

    /// A low level poll method that is similar to [`Receiver::recv()`], and can be useful for
    /// building stream implementations which use a [`Receiver`] under the hood and want to know if
    /// the stream has overflowed.
    ///
    /// Prefer to use [`Receiver::recv()`] when otherwise possible.
    ///
    /// # Errors
    ///
    /// If messages were dropped from the channel before this receiver got to them (in overflow
    /// mode, or by a reduction of the capacity), a [`RecvError::Overflowed`] variant is returned
    /// containing the number of messages that were lost.
    ///
    /// # Examples
    ///
    /// This example shows how the [`Receiver::poll_recv`] method can be used to allow a custom
    /// stream implementation to internally make use of a [`Receiver`]. This example implementation
    /// differs from the stream implementation of [`Receiver`] because it returns an error if
    /// the channel capacity overflows, which the built in [`Receiver`] stream doesn't do.
    ///
    /// ```
    /// use std::{
    ///     pin::Pin,
    ///     task::{Context, Poll},
    /// };
    ///
    /// use futures_core::Stream;
    /// use zruntime::broadcast::{Receiver, RecvError};
    ///
    /// struct MyStream(Receiver<i32>);
    ///
    /// impl Stream for MyStream {
    ///     type Item = Result<i32, RecvError>;
    ///     fn poll_next(
    ///         mut self: Pin<&mut Self>,
    ///         cx: &mut Context<'_>,
    ///     ) -> Poll<Option<Self::Item>> {
    ///         self.0.poll_recv(cx)
    ///     }
    /// }
    /// ```
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, RecvError>>> {
        loop {
            // If this stream is listening for events, first wait for a notification.
            if let Some(listener) = self.listener.as_mut() {
                ready!(Pin::new(listener).poll(cx));
                self.listener = None;
            }

            loop {
                // Attempt to receive a message.
                match self.try_recv() {
                    Ok(msg) => {
                        // The stream is not blocked on an event - drop the listener.
                        self.listener = None;
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Err(TryRecvError::Closed) => {
                        // The stream is not blocked on an event - drop the listener.
                        self.listener = None;
                        return Poll::Ready(None);
                    }
                    Err(TryRecvError::Overflowed(n)) => {
                        // The stream is not blocked on an event - drop the listener.
                        self.listener = None;
                        return Poll::Ready(Some(Err(RecvError::Overflowed(n))));
                    }
                    Err(TryRecvError::Empty) => {}
                }

                // Receiving failed - now start listening for notifications or wait for one.
                match self.listener.as_mut() {
                    None => {
                        // Start listening and then try receiving again.
                        self.listener = Some(self.channel.recv_ops.listen_unfenced());
                    }
                    Some(_) => {
                        // Go back to the outer loop to poll the listener.
                        break;
                    }
                }
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // The messages this receiver was the last to leave, which are dropped once the lock is let
        // go of: a message's `Drop` is somebody else's code, and may come back to the channel.
        let mut last = Vec::new();

        // What the senders and receivers waiting are to be told, decided under the lock and told
        // once it is let go of.
        let (closed, senders_fail, room) = {
            let mut inner = lock(&self.channel.inner);

            // Remove ourself from each item's counter.
            loop {
                match inner.try_recv_at(&mut self.pos) {
                    Ok(Ok(msg)) => last.push(msg),
                    Ok(Err(_)) | Err(TryRecvError::Overflowed(_)) => continue,
                    Err(TryRecvError::Closed | TryRecvError::Empty) => break,
                }
            }

            inner.receiver_count -= 1;

            let closed = inner.close_channel();

            (
                closed,
                // Only inactive receivers are left, and a sender is not to wait for an active one:
                // one that waits for room would wait for good.
                !inner.is_closed && inner.receiver_count == 0 && !inner.await_active,
                // A message was popped, which made room.
                !last.is_empty() && !inner.overflow,
            )
        };

        if closed {
            self.channel.notify_closed();
        } else if senders_fail {
            // Every waiting sender is to find out, and each one that does not send passes nothing
            // on.
            self.channel.send_ops.notify_unfenced(usize::MAX);
        } else if room {
            // Notify 1 awaiting sender that there is now room. If there is still room in the
            // queue, the notified operation will notify another awaiting sender.
            self.channel.send_ops.notify_unfenced(1);
        }

        // Dropped after the notification, so that a message whose `Drop` panics cannot keep a
        // sender from being woken. A panic in a waker drops them as it unwinds.
        drop(last);
    }
}

impl<T> Clone for Receiver<T> {
    /// Produces a clone of this [`Receiver`] that has the same messages queued.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{RecvError, channel};
    ///
    /// let (s, mut r1) = channel(NonZeroUsize::new(1).unwrap());
    ///
    /// assert_eq!(s.broadcast(1).await, Ok(None));
    /// drop(s);
    ///
    /// let mut r2 = r1.clone();
    ///
    /// assert_eq!(r1.recv().await, Ok(1));
    /// assert_eq!(r1.recv().await, Err(RecvError::Closed));
    /// assert_eq!(r2.recv().await, Ok(1));
    /// assert_eq!(r2.recv().await, Err(RecvError::Closed));
    /// # });
    /// ```
    fn clone(&self) -> Self {
        let mut inner = lock(&self.channel.inner);
        inner.receiver_count += 1;
        // Increment the waiter count on all items not yet received by this object.
        let n = self.pos.saturating_sub(inner.head_pos) as usize;
        for (_elt, waiters) in inner.queue.iter_mut().skip(n) {
            *waiters += 1;
        }
        Receiver {
            channel: self.channel.clone(),
            pos: self.pos,
            listener: None,
        }
    }
}

impl<T: Clone> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.poll_recv(cx)) {
                Some(Ok(val)) => return Poll::Ready(Some(val)),
                // If overflowed, we expect future operations to succeed so try again.
                Some(Err(RecvError::Overflowed(_))) => continue,
                // RecvError::Closed should never appear here, but handle it anyway.
                None | Some(Err(RecvError::Closed)) => return Poll::Ready(None),
            }
        }
    }
}

impl<T: Clone> futures_core::stream::FusedStream for Receiver<T> {
    fn is_terminated(&self) -> bool {
        let inner = lock(&self.channel.inner);

        // Whether nothing is left for this receiver, and not for all of them: `poll_next` ends the
        // stream as soon as this receiver has taken its last message, while the others may still
        // hold theirs. Messages it missed are skipped, as `poll_next` does.
        inner.is_closed && self.pos.max(inner.head_pos) == inner.head_pos + inner.queue.len() as u64
    }
}

/// An inactive receiver.
///
/// An inactive receiver is a receiver that is unable to receive messages. It's only useful for
/// keeping a channel open even when no associated active receivers exist.
#[derive(Debug)]
pub struct InactiveReceiver<T> {
    channel: Arc<Channel<T>>,
}

impl<T> InactiveReceiver<T> {
    /// Converts to an active [`Receiver`].
    ///
    /// Consumes `self`. Use [`InactiveReceiver::activate_cloned`] if you want to keep `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TrySendError, channel};
    ///
    /// let (s, r) = channel(NonZeroUsize::new(1).unwrap());
    /// let inactive = r.deactivate();
    /// assert_eq!(s.try_broadcast(10), Err(TrySendError::Inactive(10)));
    ///
    /// let mut r = inactive.activate();
    /// assert_eq!(s.try_broadcast(10), Ok(None));
    /// assert_eq!(r.try_recv(), Ok(10));
    /// ```
    pub fn activate(self) -> Receiver<T> {
        self.activate_cloned()
    }

    /// Creates an active [`Receiver`] for the associated channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{TrySendError, channel};
    ///
    /// let (s, r) = channel(NonZeroUsize::new(1).unwrap());
    /// let inactive = r.deactivate();
    /// assert_eq!(s.try_broadcast(10), Err(TrySendError::Inactive(10)));
    ///
    /// let mut r = inactive.activate_cloned();
    /// assert_eq!(s.try_broadcast(10), Ok(None));
    /// assert_eq!(r.try_recv(), Ok(10));
    /// ```
    pub fn activate_cloned(&self) -> Receiver<T> {
        self.channel.new_receiver()
    }

    /// The channel's capacity.
    ///
    /// See [`Receiver::capacity`] documentation for examples.
    pub fn capacity(&self) -> NonZeroUsize {
        lock(&self.channel.inner).capacity
    }

    /// Sets the channel capacity.
    ///
    /// There are times when you need to change the channel's capacity after creating it. If the
    /// `new_cap` is less than the number of messages in the channel, the oldest messages will be
    /// dropped to shrink the channel.
    ///
    /// See [`Receiver::set_capacity`] documentation for examples.
    pub fn set_capacity(&mut self, new_cap: NonZeroUsize) {
        self.channel.set_capacity(new_cap);
    }

    /// Whether overflow mode is enabled on this channel.
    ///
    /// See [`Receiver::overflow`] documentation for examples.
    pub fn overflow(&self) -> bool {
        lock(&self.channel.inner).overflow
    }

    /// Sets overflow mode on the channel.
    ///
    /// When overflow mode is set, broadcasting to the channel will succeed even if the channel is
    /// full. It achieves that by removing the oldest message from the channel.
    ///
    /// See [`Receiver::set_overflow`] documentation for examples.
    pub fn set_overflow(&mut self, overflow: bool) {
        self.channel.set_overflow(overflow);
    }

    /// Whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (_, r) = channel::<i32>(NonZeroUsize::new(5).unwrap());
    /// let r = r.deactivate();
    /// assert!(r.await_active());
    /// ```
    pub fn await_active(&self) -> bool {
        lock(&self.channel.inner).await_active
    }

    /// Sets whether senders wait for active receivers.
    ///
    /// If set to `false`, [`Send`] will resolve immediately with a [`SendError`] when there are no
    /// active receivers. Defaults to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::{SendError, channel};
    ///
    /// let (s, r) = channel::<i32>(NonZeroUsize::new(2).unwrap());
    /// s.broadcast(1).await.unwrap();
    ///
    /// let mut inactive = r.deactivate();
    /// inactive.set_await_active(false);
    /// assert_eq!(s.broadcast(2).await, Err(SendError(2)));
    /// assert!(!s.is_closed());
    /// # });
    /// ```
    pub fn set_await_active(&mut self, await_active: bool) {
        self.channel.set_await_active(await_active);
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already.
    ///
    /// The remaining messages can still be received.
    ///
    /// See [`Receiver::close`] documentation for examples.
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Whether the channel is closed.
    ///
    /// See [`Receiver::is_closed`] documentation for examples.
    pub fn is_closed(&self) -> bool {
        lock(&self.channel.inner).is_closed
    }

    /// Whether the channel is empty.
    ///
    /// See [`Receiver::is_empty`] documentation for examples.
    pub fn is_empty(&self) -> bool {
        lock(&self.channel.inner).queue.is_empty()
    }

    /// Whether the channel is full.
    ///
    /// See [`Receiver::is_full`] documentation for examples.
    pub fn is_full(&self) -> bool {
        let inner = lock(&self.channel.inner);

        inner.queue.len() == inner.capacity.get()
    }

    /// The number of messages in the channel.
    ///
    /// See [`Receiver::len`] documentation for examples.
    pub fn len(&self) -> usize {
        lock(&self.channel.inner).queue.len()
    }

    /// The number of receivers for the channel.
    ///
    /// This does not include inactive receivers. Use [`InactiveReceiver::inactive_receiver_count`]
    /// if you're interested in that.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        lock(&self.channel.inner).receiver_count
    }

    /// The number of inactive receivers for the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use zruntime::broadcast::channel;
    ///
    /// let (s, r) = channel::<()>(NonZeroUsize::new(1).unwrap());
    /// assert_eq!(s.receiver_count(), 1);
    /// let r = r.deactivate();
    /// assert_eq!(s.receiver_count(), 0);
    ///
    /// let _r2 = r.activate_cloned();
    /// assert_eq!(r.receiver_count(), 1);
    /// assert_eq!(r.inactive_receiver_count(), 1);
    /// ```
    pub fn inactive_receiver_count(&self) -> usize {
        lock(&self.channel.inner).inactive_receiver_count
    }

    /// The number of senders for the channel.
    ///
    /// See [`Receiver::sender_count`] documentation for examples.
    pub fn sender_count(&self) -> usize {
        lock(&self.channel.inner).sender_count
    }
}

impl<T> Clone for InactiveReceiver<T> {
    fn clone(&self) -> Self {
        lock(&self.channel.inner).inactive_receiver_count += 1;

        InactiveReceiver {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for InactiveReceiver<T> {
    fn drop(&mut self) {
        let closed = {
            let mut inner = lock(&self.channel.inner);

            inner.inactive_receiver_count -= 1;

            inner.close_channel()
        };

        if closed {
            self.channel.notify_closed();
        }
    }
}

/// A future returned by [`Sender::broadcast()`].
#[derive(Debug)]
#[must_use = "futures do nothing unless .awaited"]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    listener: Option<EventListener>,
    msg: Option<T>,
}

// The message is never pinned, so the future is `Unpin` whether the message is or not.
impl<T> Unpin for Send<'_, T> {}

impl<T: Clone> Future for Send<'_, T> {
    type Output = Result<Option<T>, SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let msg = this.msg.take().unwrap();
            let channel = &this.sender.channel;

            // Attempt to send a message.
            match this.sender.try_broadcast(msg) {
                Ok(msg) => {
                    // A listener this future still holds may have been notified and not polled
                    // since. Let go of it first, which passes its notification on, so it cannot
                    // swallow the one below.
                    this.listener = None;

                    let room = {
                        let inner = lock(&channel.inner);

                        inner.queue.len() < inner.capacity.get()
                    };

                    if room {
                        // Not full still, so notify the next awaiting sender.
                        channel.send_ops.notify_unfenced(1);
                    }

                    return Poll::Ready(Ok(msg));
                }
                Err(TrySendError::Closed(msg)) => {
                    // Let go of a listener that may be notified, as on success.
                    this.listener = None;

                    return Poll::Ready(Err(SendError(msg)));
                }
                Err(TrySendError::Full(m)) => this.msg = Some(m),
                Err(TrySendError::Inactive(m)) if lock(&channel.inner).await_active => {
                    this.msg = Some(m)
                }
                Err(TrySendError::Inactive(m)) => {
                    // Let go of a listener that may be notified, as on success: nothing may be
                    // there to wake the next waiting sender otherwise.
                    this.listener = None;

                    return Poll::Ready(Err(SendError(m)));
                }
            }

            // Sending failed - now start listening for notifications or wait for one.
            match &mut this.listener {
                None => {
                    // Start listening and then try sending again.
                    this.listener = Some(channel.send_ops.listen_unfenced());
                }
                Some(listener) => {
                    // Wait for a notification.
                    ready!(Pin::new(listener).poll(cx));
                    this.listener = None;
                }
            }
        }
    }
}

/// A future returned by [`Receiver::recv()`].
#[derive(Debug)]
#[must_use = "futures do nothing unless .awaited"]
pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    listener: Option<EventListener>,
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Attempt to receive a message.
            match this.receiver.try_recv() {
                Ok(msg) => return Poll::Ready(Ok(msg)),
                Err(TryRecvError::Closed) => return Poll::Ready(Err(RecvError::Closed)),
                Err(TryRecvError::Overflowed(n)) => {
                    return Poll::Ready(Err(RecvError::Overflowed(n)));
                }
                Err(TryRecvError::Empty) => {}
            }

            // Receiving failed - now start listening for notifications or wait for one.
            match &mut this.listener {
                None => {
                    // Start listening and then try receiving again.
                    this.listener = Some(this.receiver.channel.recv_ops.listen_unfenced());
                }
                Some(listener) => {
                    // Wait for a notification.
                    ready!(Pin::new(listener).poll(cx));
                    this.listener = None;
                }
            }
        }
    }
}

/// An error returned from [`Sender::broadcast()`].
///
/// Received because the channel is closed, or because no active receivers were present while
/// `await_active` was set to `false` (see [`Sender::set_await_active`] for details).
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// Unwraps the message that couldn't be sent.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> error::Error for SendError<T> {}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending into a closed channel")
    }
}

/// An error returned from [`Sender::try_broadcast()`].
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The channel is full but not closed.
    Full(T),

    /// The channel is closed.
    Closed(T),

    /// There are currently no active receivers, only inactive ones.
    Inactive(T),
}

impl<T> TrySendError<T> {
    /// Unwraps the message that couldn't be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(t) => t,
            TrySendError::Closed(t) => t,
            TrySendError::Inactive(t) => t,
        }
    }

    /// Whether the channel is full but not closed.
    pub fn is_full(&self) -> bool {
        match self {
            TrySendError::Full(_) => true,
            TrySendError::Closed(_) | TrySendError::Inactive(_) => false,
        }
    }

    /// Whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        match self {
            TrySendError::Full(_) | TrySendError::Inactive(_) => false,
            TrySendError::Closed(_) => true,
        }
    }

    /// Whether there are currently no active receivers, only inactive ones.
    pub fn is_disconnected(&self) -> bool {
        match self {
            TrySendError::Full(_) | TrySendError::Closed(_) => false,
            TrySendError::Inactive(_) => true,
        }
    }
}

impl<T> error::Error for TrySendError<T> {}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TrySendError::Full(..) => write!(f, "Full(..)"),
            TrySendError::Closed(..) => write!(f, "Closed(..)"),
            TrySendError::Inactive(..) => write!(f, "Inactive(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TrySendError::Full(..) => write!(f, "sending into a full channel"),
            TrySendError::Closed(..) => write!(f, "sending into a closed channel"),
            TrySendError::Inactive(..) => write!(f, "sending into the void (no active receivers)"),
        }
    }
}

/// An error returned from [`Receiver::recv()`].
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RecvError {
    /// Messages were dropped from the channel before this receiver got to them, in overflow mode
    /// or by a reduction of the capacity. Future recv operations will succeed, but some messages
    /// have been skipped.
    ///
    /// Contains the number of messages missed.
    Overflowed(u64),

    /// The channel is closed and has no messages left for this receiver.
    Closed,
}

impl error::Error for RecvError {}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflowed(n) => write!(f, "receiving skipped {} messages", n),
            Self::Closed => write!(f, "receiving from an empty and closed channel"),
        }
    }
}

/// An error returned from [`Receiver::try_recv()`].
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// Messages were dropped from the channel before this receiver got to them, in overflow mode
    /// or by a reduction of the capacity. Future recv operations will succeed, but some messages
    /// have been skipped.
    ///
    /// Contains the number of messages missed.
    Overflowed(u64),

    /// There is no message for this receiver yet, and the channel is not closed.
    Empty,

    /// The channel is closed and has no messages left for this receiver.
    Closed,
}

impl TryRecvError {
    /// Whether there is no message for this receiver yet, and the channel is not closed.
    pub fn is_empty(&self) -> bool {
        match self {
            TryRecvError::Empty => true,
            TryRecvError::Closed => false,
            TryRecvError::Overflowed(_) => false,
        }
    }

    /// Whether the channel is closed and has no messages left for this receiver.
    pub fn is_closed(&self) -> bool {
        match self {
            TryRecvError::Empty => false,
            TryRecvError::Closed => true,
            TryRecvError::Overflowed(_) => false,
        }
    }

    /// Whether this error indicates the receiver missed messages.
    pub fn is_overflowed(&self) -> bool {
        match self {
            TryRecvError::Empty => false,
            TryRecvError::Closed => false,
            TryRecvError::Overflowed(_) => true,
        }
    }
}

impl error::Error for TryRecvError {}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TryRecvError::Empty => write!(f, "receiving from an empty channel"),
            TryRecvError::Closed => write!(f, "receiving from an empty and closed channel"),
            TryRecvError::Overflowed(n) => {
                write!(f, "receiving operation observed {} lost messages", n)
            }
        }
    }
}

/// What the ends of a channel share: its state, behind a lock, and the two events that tell the
/// operations waiting on it that the state changed.
///
/// The events sit beside the lock and not behind it, because an operation notifies them only once
/// it has let go of the lock. A notification wakes tasks, and waking one runs its waker, which is
/// somebody else's code: it may panic, and it may come straight back to the channel. Run under the
/// lock, a waker that panics would leave the operation half done, with the poison-tolerant [`lock`]
/// hiding it, and a waker that came back would deadlock. So each operation locks, changes the
/// state and decides what to notify, lets go of the lock, makes what it is to return, notifies, and
/// only then drops any messages that left the channel.
///
/// Listening takes no lock either. [`Event`] keeps a notification from slipping in between a
/// listener being taken and its caller checking what it waits for, as long as whoever notifies has
/// changed that before it notifies, which is the order every operation here keeps. And every
/// operation that waits listens first and checks after, as [`Event`] asks.
///
/// The channel listens and notifies without the event's fences, through `Event::listen_unfenced`
/// and `Event::notify_unfenced`. Every operation checks what it waits for under this lock, and
/// every one that changes it does so under this lock and notifies after: the lock then orders a
/// check after the change, or the listener taken before the check ahead of the notification.
#[derive(Debug)]
struct Channel<T> {
    inner: Mutex<Inner<T>>,

    /// Send operations waiting while the channel is full.
    send_ops: Event,

    /// Receive operations waiting while the channel is empty and not closed.
    recv_ops: Event,
}

impl<T> Channel<T> {
    /// Makes a receiver that starts with no message available, and counts it as an active one.
    fn new_receiver(self: &Arc<Self>) -> Receiver<T> {
        let (pos, first) = {
            let mut inner = lock(&self.inner);
            inner.receiver_count += 1;

            (
                inner.head_pos + inner.queue.len() as u64,
                inner.receiver_count == 1,
            )
        };

        // Made before the notification and kept in a variable: if a waker panics in it, the
        // receiver is dropped as the panic unwinds, which counts it out again. Nothing is held
        // that its drop would need.
        let receiver = Receiver {
            channel: self.clone(),
            pos,
            listener: None,
        };

        // The channel may have had no active receiver until now, only inactive ones keeping it
        // open, and a sender may be waiting for exactly that.
        if first {
            // Notify 1 awaiting sender that there is now a receiver. If there is still room in
            // the queue, the notified operation will notify another awaiting sender.
            self.send_ops.notify_unfenced(1);
        }

        receiver
    }

    /// Closes the channel, and tells the operations waiting on it.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already.
    fn close(&self) -> bool {
        let closed = lock(&self.inner).close();

        if closed {
            self.notify_closed();
        }

        closed
    }

    /// Notifies every operation waiting on the channel that it is closed.
    fn notify_closed(&self) {
        self.send_ops.notify_unfenced(usize::MAX);
        self.recv_ops.notify_unfenced(usize::MAX);
    }

    /// Sets the channel's capacity, and tells a waiting sender if that made room.
    fn set_capacity(&self, new_cap: NonZeroUsize) {
        let (grown, dropped) = {
            let mut inner = lock(&self.inner);
            let old_cap = inner.capacity;

            (new_cap > old_cap, inner.set_capacity(new_cap))
        };

        if grown {
            // Notify 1 awaiting sender that there is now room. If there is still room in the
            // queue, the notified operation will notify another awaiting sender.
            self.send_ops.notify_unfenced(1);
        }

        // Dropped last, once the lock is let go of and the senders are told: a message's `Drop` is
        // somebody else's code.
        drop(dropped);
    }

    /// Sets overflow mode, and tells the waiting senders if it was turned on.
    fn set_overflow(&self, overflow: bool) {
        let was = mem::replace(&mut lock(&self.inner).overflow, overflow);

        if overflow && !was {
            // A full queue stays full in overflow mode, so a sender that goes through passes
            // nothing on: every waiting sender is to hear of it, and go through.
            self.send_ops.notify_unfenced(usize::MAX);
        }
    }

    /// Sets whether senders wait for an active receiver, and tells the waiting senders if they no
    /// longer do.
    fn set_await_active(&self, await_active: bool) {
        let was = mem::replace(&mut lock(&self.inner).await_active, await_active);

        if was && !await_active {
            // The senders waiting for an active receiver are to give up, each with an error.
            self.send_ops.notify_unfenced(usize::MAX);
        }
    }
}

#[derive(Debug)]
struct Inner<T> {
    queue: VecDeque<(T, usize)>,
    // We assign the same capacity to the queue but that's just specifying the minimum capacity and
    // the actual capacity could be anything. Hence the need to keep track of our own set capacity.
    capacity: NonZeroUsize,
    receiver_count: usize,
    inactive_receiver_count: usize,
    sender_count: usize,
    /// Send sequence number of the front of the queue.
    head_pos: u64,
    overflow: bool,
    await_active: bool,

    is_closed: bool,
}

impl<T> Inner<T> {
    /// Tries receiving at the given position, returning either the element or a reference to it.
    ///
    /// Result is used here instead of Cow because we don't have a Clone bound on T.
    ///
    /// An element that is returned rather than referred to was popped off the front of the queue,
    /// because this was the last receiver to want it. That made room, which it is for the caller to
    /// announce once it has let go of the lock.
    fn try_recv_at(&mut self, pos: &mut u64) -> Result<Result<T, &T>, TryRecvError> {
        let i = match pos.checked_sub(self.head_pos) {
            Some(i) => i
                .try_into()
                .expect("Head position more than usize::MAX behind a receiver"),
            None => {
                let count = self.head_pos - *pos;
                *pos = self.head_pos;
                return Err(TryRecvError::Overflowed(count));
            }
        };

        let last_waiter;
        if let Some((_elt, waiters)) = self.queue.get_mut(i) {
            *pos += 1;
            *waiters -= 1;
            last_waiter = *waiters == 0;
        } else {
            debug_assert_eq!(i, self.queue.len());
            if self.is_closed {
                return Err(TryRecvError::Closed);
            } else {
                return Err(TryRecvError::Empty);
            }
        }

        // If we read from the front of the queue and this is the last receiver reading it, we can
        // pop the queue instead of cloning the message.
        if last_waiter {
            // Only the first element of the queue should have 0 waiters.
            assert_eq!(i, 0);

            // Remove the element from the queue and adjust space.
            let elt = self.queue.pop_front().unwrap().0;
            self.head_pos += 1;

            Ok(Ok(elt))
        } else {
            Ok(Err(&self.queue[i].0))
        }
    }

    /// Closes the channel.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already, in which
    /// case the caller is to notify all waiting operations once it has let go of the lock.
    fn close(&mut self) -> bool {
        if self.is_closed {
            return false;
        }

        self.is_closed = true;

        true
    }

    /// Sets the channel capacity.
    ///
    /// There are times when you need to change the channel's capacity after creating it. If the
    /// `new_cap` is less than the number of messages in the channel, the oldest messages will be
    /// dropped to shrink the channel.
    ///
    /// Returns the messages that were dropped from the channel, for the caller to drop once it has
    /// let go of the lock: a message's `Drop` is somebody else's code, which must not run under it.
    fn set_capacity(&mut self, new_cap: NonZeroUsize) -> Vec<T> {
        self.capacity = new_cap;
        let new_cap = new_cap.get();
        if new_cap > self.queue.capacity() {
            let diff = new_cap - self.queue.capacity();
            self.queue.reserve(diff);
        }

        // Ensure queue doesn't have more than `new_cap` messages.
        if new_cap < self.queue.len() {
            let diff = self.queue.len() - new_cap;
            self.head_pos += diff as u64;
            self.queue.drain(0..diff).map(|(msg, _)| msg).collect()
        } else {
            Vec::new()
        }
    }

    /// Closes the channel if there are no receivers left, active or inactive.
    ///
    /// Returns `true` if this call has closed the channel and it was not closed already, as
    /// [`Inner::close`] does.
    fn close_channel(&mut self) -> bool {
        self.receiver_count == 0 && self.inactive_receiver_count == 0 && self.close()
    }
}

/// The value behind a lock, taken whether or not a panic poisoned it.
///
/// The only user code that an operation of the channel runs under its lock is `T::clone`, in
/// [`Receiver::try_recv`]. The rest is run once the lock is let go of: the wakers that
/// [`Event::notify`] wakes, and the `Drop` of a message that leaves the channel.
///
/// A panic in that clone leaves the state consistent. The receiver's position and the message's
/// count of receivers are updated before the clone, which changes nothing else, so the panic costs
/// this receiver the message and no more. Nothing is left to announce either: a message is cloned
/// only where it stays in the channel, and it is the one that leaves that makes room.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
