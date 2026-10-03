//! An async adapter for a blocking I/O handle, which runs each operation on the handle as blocking
//! work.

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::{self, Future},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Wake, Waker, ready},
};

use futures_core::Stream;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};

use super::{BlockingWork, unblock};

/// An async adapter for a blocking I/O handle, which runs each operation on the handle as blocking
/// work, through [`unblock()`].
///
/// The handle is anything that implements [`Read`], [`Write`], [`Seek`] or [`Iterator`]: a file,
/// the standard input or output, a pipe to a child process, the entries of a directory. The adapter
/// implements the [`AsyncRead`], [`AsyncWrite`] and [`AsyncSeek`] traits of `futures-io`, and the
/// [`Stream`] trait of `futures-core`, wherever the handle implements the blocking trait each of
/// them stands for, so that the handle can be used from async code without its calls blocking the
/// thread that polls a task. For each operation, the adapter hands the handle over to blocking
/// work, which hands it back along with the outcome.
///
/// Like [`unblock()`], the adapter needs no runtime and works under any executor. It is `Send` and
/// `Sync` wherever the handle is `Send`, and `Unpin` whatever the handle is.
///
/// # One operation at a time
///
/// The adapter runs one operation on the handle at a time, and an operation waits for the one
/// before it to be over, whichever way either of them goes: a write waits for a read in flight, and
/// a read for a write. Two tasks may wait at once, as the reading and the writing half of a split
/// adapter do, and both are woken when the operation is over, even where the waker of one of them
/// panics. A file is none the worse for that, but a handle that reads and writes two separate
/// streams, as a socket or a serial port does, can wait for good: a read that waits for the other
/// end to send something holds up a write that the other end waits for first.
///
/// # Reading
///
/// A read reads up to the capacity of the adapter from the handle, whatever the size of the buffer
/// it was given, and what that buffer does not take is kept for the reads that follow. The handle's
/// own position is thus ahead of what the reads have handed out. A seek accounts for that, one from
/// [`SeekFrom::Current`] included, but a write does not: on a handle whose reads and writes share
/// one position, as those of a file do, seek to `SeekFrom::Current(0)` before writing after a read,
/// which brings the handle's position back to where the reads got to. The bytes read ahead are kept
/// across writes, so that a handle with two separate streams loses none of those it read.
///
/// # Writing
///
/// A write takes as many of the bytes it was given as the capacity allows, and completes as soon as
/// it has handed them over to blocking work that writes every one of them to the handle. The
/// operation after it waits for that work to be over, and an error the work ran into is reported by
/// the next write or flush. A flush waits for the bytes of the write before it to be written, then
/// flushes the handle. Closing the adapter flushes it and leaves the handle open: the handle is
/// closed when it is dropped, along with the adapter or after [`into_inner`](Unblock::into_inner).
///
/// No byte a write took waits on the adapter to go to the handle. Dropping the adapter gives up the
/// wait for the write in flight, not the write itself, which runs to its end as the work of
/// [`unblock()`] does; an error it runs into then goes unreported, so flush the adapter before
/// dropping it to learn of any.
///
/// # Iterating
///
/// Items are pulled from the iterator a batch at a time, by blocking work that pulls as many as the
/// capacity, or 16 where the capacity is larger, or until the iterator ends, and the stream hands
/// them out one by one. The first item of a batch thus waits for the last, which matters for an
/// iterator whose items are slow to come, such as the lines of the standard input: a capacity of 1
/// has each of them handed out as soon as it comes. The stream ends where the iterator does, and
/// polled again after that, pulls from the iterator again.
///
/// # Using the handle directly
///
/// [`get_mut`](Unblock::get_mut), [`with_mut`](Unblock::with_mut) and
/// [`into_inner`](Unblock::into_inner) hand the handle over for use as it is, once the operation in
/// flight is over. They drop the bytes read ahead and the items pulled ahead: the handle is past
/// them, and nothing done to it directly can be accounted for.
///
/// # Panics
///
/// A panic in an operation on the handle is raised again, with the payload it had, by the poll that
/// waits for that operation, as it is for [`unblock()`]. The handle goes with the panic, and any
/// use of the adapter after it panics as well.
///
/// # Example
///
/// A [`Cursor`](std::io::Cursor) stands in for a blocking handle to read, and a `Vec` for one to
/// write. The futures are driven by `block_on` from the `futures-lite` crate, but the `block_on` of
/// any executor would do, as the adapter needs no runtime:
///
/// ```
/// use std::io::Cursor;
///
/// use futures_lite::{AsyncReadExt, AsyncWriteExt, future::block_on};
/// use zruntime::Unblock;
///
/// block_on(async {
///     let mut reader = Unblock::new(Cursor::new("hello, world"));
///     let mut text = String::new();
///     reader.read_to_string(&mut text).await?;
///     assert_eq!(text, "hello, world");
///
///     let mut writer = Unblock::new(Vec::new());
///     writer.write_all(b"goodbye").await?;
///     writer.flush().await?;
///     assert_eq!(writer.into_inner().await, b"goodbye");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// ```
pub struct Unblock<T> {
    /// Behind a mutex that is never locked: the adapter is only ever used through a mutable
    /// borrow, which reaches what the mutex holds through its `get_mut`. The mutex is there for
    /// its `Sync`, which it has wherever what it holds is `Send`, as nothing of the handle is
    /// reachable through a shared reference to the adapter.
    inner: Mutex<Inner<T>>,
}

impl<T> Unblock<T> {
    /// An adapter for `io`, with the default capacity of 8 KiB.
    ///
    /// See [`with_capacity`](Unblock::with_capacity) for what the capacity is.
    pub fn new(io: T) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, io)
    }

    /// An adapter for `io`, with a capacity of `cap`.
    ///
    /// The capacity is the most bytes that one operation on the handle reads ahead or writes, and
    /// the most items, up to 16, that one pulls from an iterator. A larger one has the handle read
    /// and written in fewer and larger calls, at the cost of memory: the adapter keeps a buffer of
    /// that size for the reads, and one for the writes, once there have been any.
    pub fn with_capacity(cap: NonZeroUsize, io: T) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: State::Idle(io),
                read_ahead: ReadAhead::default(),
                write_buf: Vec::new(),
                write_error: None,
                items: None,
                waiters: Arc::default(),
                cap,
            }),
        }
    }

    /// The handle, borrowed mutably once the operation in flight is over.
    ///
    /// By then, every byte a write took has been handed to the handle, though the handle may still
    /// hold some of them in a buffer of its own. An error the last write ran into, if no write or
    /// flush has reported it yet, is kept for the next one. The bytes read ahead and the items
    /// pulled ahead are dropped: see
    /// [using the handle directly](Unblock#using-the-handle-directly).
    ///
    /// # Example
    ///
    /// ```
    /// use std::io::Cursor;
    ///
    /// use futures_lite::{AsyncReadExt, future::block_on};
    /// use zruntime::Unblock;
    ///
    /// block_on(async {
    ///     let mut reader = Unblock::new(Cursor::new("hello"));
    ///     let mut text = String::new();
    ///     reader.read_to_string(&mut text).await?;
    ///
    ///     assert_eq!(reader.get_mut().await.position(), 5);
    ///
    ///     std::io::Result::Ok(())
    /// })
    /// .unwrap();
    /// ```
    pub async fn get_mut(&mut self) -> &mut T {
        let inner = self.inner();
        future::poll_fn(|cx| inner.poll_idle(cx)).await;
        inner.drop_ahead();

        let State::Idle(handle) = &mut inner.state else {
            unreachable!("the handle is back once no operation is in flight");
        };
        handle
    }

    /// Runs `op` on the handle as blocking work, once the operation in flight is over, and hands
    /// back what `op` returned.
    ///
    /// This is for what the async traits do not cover: the metadata of a file, say, or its length
    /// to set. As with [`get_mut`](Unblock::get_mut), every byte a write took has been handed to
    /// the handle before `op` runs, an error the last write ran into is kept for the next write or
    /// flush, and the bytes read ahead and the items pulled ahead are dropped.
    ///
    /// Once `op` has started, nothing can stop it: dropping the future then gives up the wait for
    /// `op`, which runs to its end, and the next operation waits for it and drops what it
    /// returned. A future dropped before `op` started, while it waited for the operation before,
    /// never runs it.
    ///
    /// # Panics
    ///
    /// A panic in `op` is raised again by the poll of the future that would have returned its
    /// value, and the adapter loses the handle with it, as for any operation on the handle.
    ///
    /// # Example
    ///
    /// ```
    /// use std::io::Cursor;
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::Unblock;
    ///
    /// let mut reader = Unblock::new(Cursor::new(vec![1, 2, 3]));
    ///
    /// let len = block_on(reader.with_mut(|cursor| cursor.get_ref().len()));
    ///
    /// assert_eq!(len, 3);
    /// ```
    pub async fn with_mut<R, F>(&mut self, op: F) -> R
    where
        T: Send + 'static,
        F: FnOnce(&mut T) -> R + Send + 'static,
        R: Send + 'static,
    {
        let inner = self.inner();
        future::poll_fn(|cx| inner.poll_idle(cx)).await;
        inner.drop_ahead();

        inner.start(move |handle| Outcome::Ran(Box::new(op(handle))));
        // The future holds the adapter borrowed, so the operation that ends is the one it started.
        let Some(Outcome::Ran(value)) = future::poll_fn(|cx| inner.poll_job(cx)).await else {
            unreachable!("the operation in flight is the one `with_mut` started");
        };
        match value.downcast() {
            Ok(value) => *value,
            Err(_) => unreachable!("`with_mut` hands back what its own operation returned"),
        }
    }

    /// The handle, out of the adapter once the operation in flight is over.
    ///
    /// By then, every byte a write took has been handed to the handle, though the handle may still
    /// hold some of them in a buffer of its own. An error the last write ran into, if no write or
    /// flush has reported it yet, is lost: flush the adapter first to learn of it. So are the bytes
    /// read ahead and the items pulled ahead: see
    /// [using the handle directly](Unblock#using-the-handle-directly).
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::{AsyncWriteExt, future::block_on};
    /// use zruntime::Unblock;
    ///
    /// block_on(async {
    ///     let mut writer = Unblock::new(Vec::new());
    ///     writer.write_all(b"hello").await?;
    ///
    ///     assert_eq!(writer.into_inner().await, b"hello");
    ///
    ///     std::io::Result::Ok(())
    /// })
    /// .unwrap();
    /// ```
    pub async fn into_inner(mut self) -> T {
        let inner = self.inner();
        future::poll_fn(|cx| inner.poll_idle(cx)).await;

        let inner = self
            .inner
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        let State::Idle(handle) = inner.state else {
            unreachable!("the handle is back once no operation is in flight");
        };
        handle
    }

    /// What the adapter holds.
    ///
    /// An adapter whose handle went with a panic is of no use any more, so this panics for one.
    fn inner(&mut self) -> &mut Inner<T> {
        let inner = self.inner.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let State::Lost = inner.state {
            lost();
        }
        inner
    }
}

// The handle is never pinned, so the adapter is `Unpin` whether the handle is or not.
impl<T> Unpin for Unblock<T> {}

impl<T> fmt::Debug for Unblock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Unblock").finish_non_exhaustive()
    }
}

impl<T> AsyncRead for Unblock<T>
where
    T: Read + Send + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let inner = self.get_mut().inner();
        // There is nothing to read into, which std's reads take as their cue to read nothing.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        loop {
            // What was read ahead goes first, without waiting for a write in flight: those bytes
            // came before it.
            if let Some(read) = inner.read_ahead.hand_out(buf) {
                return Poll::Ready(read);
            }

            match ready!(inner.poll_job(cx)) {
                Some(outcome) => inner.keep(outcome),
                None => {
                    let mut ahead = inner.read_ahead.take_buf();
                    // A no-op past the first read, which leaves the buffer as long as the capacity.
                    ahead.resize(inner.cap.get(), 0);
                    inner.start(move |handle| {
                        let read = read_uninterrupted(handle, &mut ahead);
                        Outcome::Read(ahead, read)
                    });
                }
            }
        }
    }
}

impl<T> AsyncWrite for Unblock<T>
where
    T: Write + Send + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let inner = self.get_mut().inner();
        ready!(inner.poll_write_ready(cx))?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let len = buf.len().min(inner.cap.get());
        let mut bytes = mem::take(&mut inner.write_buf);
        bytes.extend_from_slice(&buf[..len]);
        inner.start(move |handle| {
            let written = handle.write_all(&bytes);
            bytes.clear();
            Outcome::Written(bytes, written)
        });

        // Taken: the bytes are the job's to write, and the next operation waits for it.
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = self.get_mut().inner();

        loop {
            match ready!(inner.poll_job(cx)) {
                // Whether this flush started it or one given up on did, no write can have started
                // after it, as a write waits for the operation in flight: every byte written
                // before this flush has been flushed.
                Some(Outcome::Flushed(flushed)) => return Poll::Ready(flushed),
                Some(outcome) => inner.keep(outcome),
                None => {
                    if let Some(error) = inner.write_error.take() {
                        return Poll::Ready(Err(error));
                    }
                    inner.start(|handle| Outcome::Flushed(handle.flush()));
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The handle stays open, to be closed when it is dropped: a handle has no close of its own
        // to call, and the adapter has the handle to hand back after this.
        self.poll_flush(cx)
    }
}

impl<T> AsyncSeek for Unblock<T>
where
    T: Seek + Send + 'static,
{
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let inner = self.get_mut().inner();

        loop {
            match ready!(inner.poll_job(cx)) {
                // A seek polled again after it was pending is asked for the same position. One
                // asked for another is a new seek, after one given up on.
                Some(Outcome::Sought {
                    asked,
                    sought,
                    read_ahead,
                }) if asked == pos => {
                    inner.settle_seek(&sought, read_ahead);
                    return Poll::Ready(sought);
                }
                Some(outcome) => inner.keep(outcome),
                None => {
                    let to = match pos {
                        SeekFrom::Current(offset) => {
                            let Some(offset) = inner.read_ahead.offset_from_handle(offset) else {
                                // Nothing was sought, so what was read ahead stays where it is.
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "seek from the current position out of the range of an i64",
                                )));
                            };
                            SeekFrom::Current(offset)
                        }
                        SeekFrom::Start(_) | SeekFrom::End(_) => pos,
                    };
                    let read_ahead = mem::take(&mut inner.read_ahead);
                    inner.start(move |handle| Outcome::Sought {
                        asked: pos,
                        sought: handle.seek(to),
                        read_ahead,
                    });
                }
            }
        }
    }
}

impl<T> Stream for Unblock<T>
where
    T: Iterator + Send + 'static,
    T::Item: Send + 'static,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        let inner = self.get_mut().inner();

        loop {
            if let Some(batch) = &mut inner.items {
                let Some(batch) = batch.downcast_mut::<Batch<T::Item>>() else {
                    unreachable!("the items pulled ahead are the iterator's own");
                };
                if let Some(item) = batch.items.pop_front() {
                    return Poll::Ready(Some(item));
                }
                // Reported once: polled again, the stream pulls from the iterator again, as a
                // call of `next` after the end of an iterator does.
                if mem::take(&mut batch.ended) {
                    return Poll::Ready(None);
                }
            }

            match ready!(inner.poll_job(cx)) {
                Some(outcome) => inner.keep(outcome),
                None => {
                    // The batch before, emptied, is filled again, so as not to allocate another.
                    let mut batch = match inner.items.take().map(|batch| batch.downcast()) {
                        Some(Ok(batch)) => batch,
                        Some(Err(_)) => {
                            unreachable!("the items pulled ahead are the iterator's own")
                        }
                        None => Box::new(Batch {
                            items: VecDeque::new(),
                            ended: false,
                        }),
                    };
                    let size = inner.cap.get().min(BATCH);
                    inner.start(move |iter| {
                        batch.pull(iter, size);
                        Outcome::Pulled(batch)
                    });
                }
            }
        }
    }
}

/// What an adapter holds: the handle, or the operation that has it, and what the operations keep
/// for those that come after them.
struct Inner<T> {
    state: State<T>,
    /// The bytes read from the handle ahead of the reads of the adapter.
    read_ahead: ReadAhead,
    /// The buffer that a write copies its bytes into for the job to write, handed back by the job
    /// for the next write, emptied.
    write_buf: Vec<u8>,
    /// The error that a write or flush job ran into, found by an operation of another kind, kept
    /// for the next write or flush to report.
    write_error: Option<io::Error>,
    /// The items pulled from the iterator ahead of the stream: a `Batch<T::Item>`, in a box that
    /// does not name its type, as this one does not know that `T` is an iterator.
    items: Option<Box<dyn Any + Send>>,
    /// The tasks waiting for the job in flight.
    waiters: Arc<Waiters>,
    /// The most bytes a job reads ahead or writes.
    cap: NonZeroUsize,
}

impl<T> Inner<T> {
    /// Waits for the job in flight, if there is one, and hands back its outcome, the handle being
    /// back in place by then.
    fn poll_job(&mut self, cx: &mut Context<'_>) -> Poll<Option<Outcome>> {
        // Out of its place while it is polled, so that a panic in the job, which the poll raises
        // again, leaves the adapter without the handle that went with it.
        let mut work = match mem::replace(&mut self.state, State::Lost) {
            State::Busy(work) => work,
            state @ State::Idle(_) => {
                self.state = state;
                return Poll::Ready(None);
            }
            State::Lost => lost(),
        };

        // Counted in before the poll, so that a job that ends right after it does not wake the
        // others waiting and leave this task out.
        self.waiters.add(cx.waker());
        let waker = Waker::from(self.waiters.clone());
        let polled = panic::catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut work).poll(&mut Context::from_waker(&waker))
        }));

        match polled {
            Ok(Poll::Pending) => {
                self.state = State::Busy(work);
                Poll::Pending
            }
            Ok(Poll::Ready(Done { handle, outcome })) => {
                self.state = State::Idle(handle);
                // The job wakes the others itself, unless this poll took the outcome before it
                // could.
                if let Some(panic) = self.waiters.wake_others(cx.waker()) {
                    // Kept for the operations it is for, as a later poll would keep it, so that a
                    // waker that panics loses nothing of what the job did: the bytes it read, say.
                    self.keep(outcome);
                    panic::resume_unwind(panic);
                }
                Poll::Ready(Some(outcome))
            }
            Err(panic) => {
                // Those waiting with this task find the handle gone and panic in turn, rather than
                // wait for a job that is over. The panic of the job is the one raised: that of a
                // waker woken after it goes no further.
                if let Some(waker_panic) = self.waiters.wake_others(cx.waker()) {
                    dispose(waker_panic);
                }
                panic::resume_unwind(panic)
            }
        }
    }

    /// Waits until no job is in flight, keeping the outcome of the one there was for the
    /// operations it was for.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        while let Some(outcome) = ready!(self.poll_job(cx)) {
            self.keep(outcome);
        }
        Poll::Ready(())
    }

    /// Waits until no job is in flight, and hands back the error of a write or flush that no write
    /// or flush has reported yet.
    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_idle(cx));
        Poll::Ready(self.write_error.take().map_or(Ok(()), Err))
    }

    /// Keeps the outcome of a job for the operations it is for: what a read read for the reads
    /// after it, the error a write ran into for the next write or flush, and so on.
    fn keep(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Read(buf, read) => self.read_ahead.fill(buf, read),
            Outcome::Written(buf, written) => {
                self.write_buf = buf;
                if let Err(error) = written {
                    self.write_error = Some(error);
                }
            }
            Outcome::Flushed(flushed) => {
                if let Err(error) = flushed {
                    self.write_error = Some(error);
                }
            }
            // Given up on, the seek has moved the handle's position all the same.
            Outcome::Sought {
                sought, read_ahead, ..
            } => self.settle_seek(&sought, read_ahead),
            Outcome::Pulled(batch) => self.items = Some(batch),
            // What a `with_mut` whose future was dropped returned, which is nobody's to take.
            Outcome::Ran(_) => {}
        }
    }

    /// Puts back what was read ahead before a seek, if the seek failed, and drops it otherwise.
    fn settle_seek(&mut self, sought: &io::Result<u64>, mut read_ahead: ReadAhead) {
        // A seek that fails leaves the handle's position where it was, on most handles, and so
        // past the bytes read ahead as before: they are kept for the reads that follow, as std's
        // `BufReader` keeps its own.
        if sought.is_ok() {
            read_ahead.clear();
        }
        self.read_ahead = read_ahead;
    }

    /// Drops the bytes read ahead and the items pulled ahead, for the handle to be used directly.
    fn drop_ahead(&mut self) {
        self.read_ahead.clear();
        self.items = None;
    }

    /// Hands the handle over to a job that runs `op` on it, and hands it back along with what `op`
    /// returned.
    ///
    /// No job may be in flight.
    fn start<F>(&mut self, op: F)
    where
        T: Send + 'static,
        F: FnOnce(&mut T) -> Outcome + Send + 'static,
    {
        let State::Idle(mut handle) = mem::replace(&mut self.state, State::Lost) else {
            unreachable!("a job starts only once the one before it is over");
        };
        self.state = State::Busy(unblock(move || {
            let outcome = op(&mut handle);
            Done { handle, outcome }
        }));
    }
}

/// Where the handle of an adapter is.
enum State<T> {
    /// In the adapter, with no job in flight.
    Idle(T),
    /// With the job in flight, which hands it back when it is over.
    Busy(BlockingWork<Done<T>>),
    /// Gone with a panic in a job.
    Lost,
}

/// What a job hands back: the handle, and what it did with it.
struct Done<T> {
    handle: T,
    outcome: Outcome,
}

/// What a job did with the handle.
enum Outcome {
    /// Read into the buffer of the bytes read ahead, which comes back with the bytes.
    Read(Vec<u8>, io::Result<usize>),
    /// Wrote the bytes of the buffer, which comes back for the next write.
    Written(Vec<u8>, io::Result<()>),
    /// Flushed the handle.
    Flushed(io::Result<()>),
    /// Sought, for the position asked for, along with the bytes read ahead before it, which a seek
    /// that fails leaves valid.
    Sought {
        asked: SeekFrom,
        sought: io::Result<u64>,
        read_ahead: ReadAhead,
    },
    /// Pulled a batch of items from the iterator: a `Box<Batch<T::Item>>`.
    Pulled(Box<dyn Any + Send>),
    /// Ran the operation that `with_mut` was given, which returned this.
    Ran(Box<dyn Any + Send>),
}

/// The bytes read from the handle ahead of the reads of an adapter, and what ended the last read
/// that read none.
#[derive(Default)]
struct ReadAhead {
    /// What the reads read into, as long as the capacity once one has.
    buf: Vec<u8>,
    /// Where the bytes not handed out yet start in `buf`.
    start: usize,
    /// Where they end.
    end: usize,
    /// The end of the handle, or the error, that the last read ran into, if no read has reported
    /// it yet.
    stop: Option<io::Result<()>>,
}

impl ReadAhead {
    /// Hands out as many bytes read ahead as `buf` takes, or else what ended the last read, if
    /// there are any.
    fn hand_out(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        let ahead = &self.buf[self.start..self.end];
        if ahead.is_empty() {
            // The end of the handle is a read of nothing.
            return self.stop.take().map(|stop| stop.map(|()| 0));
        }

        let len = ahead.len().min(buf.len());
        buf[..len].copy_from_slice(&ahead[..len]);
        self.start += len;
        Some(Ok(len))
    }

    /// The buffer, for a read to read into, every byte in it having been handed out.
    fn take_buf(&mut self) -> Vec<u8> {
        self.start = 0;
        self.end = 0;
        mem::take(&mut self.buf)
    }

    /// Takes in what a read read into `buf`.
    fn fill(&mut self, buf: Vec<u8>, read: io::Result<usize>) {
        self.start = 0;
        self.end = 0;
        match read {
            Ok(0) => self.stop = Some(Ok(())),
            // A reader that claims more than the buffer holds read no more than the buffer.
            Ok(len) => self.end = len.min(buf.len()),
            Err(error) => self.stop = Some(Err(error)),
        }
        self.buf = buf;
    }

    /// The offset from the handle's position of the one `offset` away from where the reads got to:
    /// behind the reads by the bytes read ahead, if that is in the range of an `i64`.
    fn offset_from_handle(&self, offset: i64) -> Option<i64> {
        offset.checked_sub(i64::try_from(self.end - self.start).ok()?)
    }

    /// Drops the bytes read ahead and what ended the last read, keeping the buffer.
    fn clear(&mut self) {
        self.start = 0;
        self.end = 0;
        self.stop = None;
    }
}

/// Items pulled from an iterator ahead of the stream of an adapter.
struct Batch<I> {
    items: VecDeque<I>,
    /// Whether the iterator ended after these items.
    ended: bool,
}

impl<I> Batch<I> {
    /// Pulls items from `iter` until the batch holds `size` of them, or the iterator ends.
    fn pull<T>(&mut self, iter: &mut T, size: usize)
    where
        T: Iterator<Item = I>,
    {
        while self.items.len() < size {
            let Some(item) = iter.next() else {
                self.ended = true;
                return;
            };
            self.items.push_back(item);
        }
    }
}

/// The tasks waiting for the job in flight of an adapter.
///
/// A job wakes one waker, but more than one task may wait for the same job: the reading and the
/// writing half of a split adapter, say, the one polled after the other while a read is in flight.
/// The job is polled with the waker of this instead, which wakes every one of them.
#[derive(Default)]
struct Waiters(Mutex<Vec<Waker>>);

impl Waiters {
    /// Counts in the task that `waker` wakes, unless it is in already.
    fn add(&self, waker: &Waker) {
        let mut wakers = self.wakers();
        if !wakers.iter().any(|waiting| waiting.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    /// Wakes the tasks waiting, but for the one that `waker` wakes, which is running already, and
    /// hands back the panic of the first waker that panicked, if one did: see [`wake_all`].
    fn wake_others(&self, waker: &Waker) -> Option<Box<dyn Any + Send>> {
        let wakers = mem::take(&mut *self.wakers());
        // Past the lock, which a waker that polls the adapter there and then takes again.
        wake_all(
            wakers
                .into_iter()
                .filter(|waiting| !waiting.will_wake(waker)),
        )
    }

    /// The wakers of the tasks waiting, behind their lock, taken whether or not a panic poisoned
    /// it.
    fn wakers(&self) -> MutexGuard<'_, Vec<Waker>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Wake for Waiters {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let wakers = mem::take(&mut *self.wakers());
        // Past the lock, as in `wake_others`. The panic of a waker reaches the job that wakes this,
        // once every task waiting has been woken.
        if let Some(panic) = wake_all(wakers) {
            panic::resume_unwind(panic);
        }
    }
}

/// Wakes each of `wakers`, every one of them even where one before it panics, and hands back the
/// panic of the first that panicked, if one did.
///
/// A task left unwoken would wait for good for an operation that is over. The payload of any panic
/// after the first is disposed of: dropped with a panic of its own destructor caught, as that would
/// otherwise escape the loop, past the wakers still to wake.
fn wake_all<I>(wakers: I) -> Option<Box<dyn Any + Send>>
where
    I: IntoIterator<Item = Waker>,
{
    let mut first_panic = None;
    for waker in wakers {
        if let Err(panic) = panic::catch_unwind(AssertUnwindSafe(|| waker.wake())) {
            match first_panic {
                None => first_panic = Some(panic),
                Some(_) => dispose(panic),
            }
        }
    }

    first_panic
}

/// Drops the payload of a panic that is to go no further, with a panic of its destructor caught.
///
/// The payload is somebody else's value, and its `Drop` may panic in turn. The payload of such a
/// second panic is dropped too, the same way, and only one that panics a third time is leaked, so
/// as not to follow a chain of destructors that each panic. `Event` has a function like this one
/// too, but this module does not use it: the `event` feature may be left out of the build, and
/// blocking work needs none of it.
fn dispose(payload: Box<dyn Any + Send>) {
    let Err(payload) = panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) else {
        return;
    };
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        mem::forget(payload);
    }
}

/// Reads from `handle` into `buf`, again for as long as the read is interrupted, as std's own
/// loops of reads do: an interruption is no failure of the read, and nothing but trying again is
/// left to do.
fn read_uninterrupted<T>(handle: &mut T, buf: &mut [u8]) -> io::Result<usize>
where
    T: Read,
{
    loop {
        match handle.read(buf) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            read => return read,
        }
    }
}

/// Panics, for an adapter whose handle went with a panic in a job.
fn lost() -> ! {
    panic!("the handle of an `Unblock` was lost to a panic in an operation on it");
}

/// The capacity of an adapter made by [`Unblock::new`].
const DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(8 * 1024).unwrap();

/// The most items a job pulls from an iterator.
const BATCH: usize = 16;
