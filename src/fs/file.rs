//! [`File`], an open file whose reads and writes are blocking work.

use std::{
    fmt, future,
    io::{self, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle, RawHandle};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};

use super::{Metadata, Permissions};
use crate::{Unblock, lock::Mutex, unblock};

/// An open file, whose reads, writes and seeks are blocking work on the pool of threads that
/// [`unblock()`] hands its work to.
///
/// A file is opened by [`open`](File::open), [`create`](File::create) or
/// [`OpenOptions`](super::OpenOptions), or made from one of std's with [`From`]. It implements
/// `futures-io`'s [`AsyncRead`], [`AsyncWrite`] and [`AsyncSeek`] by way of an
/// [`Unblock`](crate::Unblock) over the file, and the methods that std's file has on top of those
/// are here as `async` methods. Like [`unblock()`], it needs no runtime and works under any
/// executor. It is `Send` and `Sync`, and `Unpin`.
///
/// The file is closed when it is dropped, and an error that closing it runs into is lost:
/// [`sync_all`](File::sync_all) learns of any before that.
///
/// # Reading
///
/// A read has the pool read up to 64 KiB from the file at once, whatever the size of the buffer it
/// was given, and keeps what the buffer does not take for the reads that follow. The position the
/// OS keeps for the file is thus ahead of where the reads got to. The position a seek reports and
/// seeks from, `SeekFrom::Current` included, is where the reads got to, and so is the one a write
/// goes to: the file has the OS position put back there before it writes, if it was read from since
/// it was last put right.
///
/// # Writing
///
/// A write takes up to 64 KiB of the bytes it was given, and is done as soon as it has handed them
/// to the pool, which writes every one of them to the file. The operation after it, of any kind,
/// waits for that work to be over first, so the bytes are written in the order they were given in,
/// and a file is read as it was written. An error that the work ran into is reported by the next
/// write or flush, and [`flush`](futures_io::AsyncWrite::poll_flush) waits for every write before
/// it to be over.
///
/// Once a write is done, then, the pool has its bytes, and nothing of it waits on the file. A file
/// that is dropped right after a write stays open until the pool is done with it, and every byte
/// is in the file by then, but an error goes unreported: flush the file, or call
/// [`sync_all`](File::sync_all), before dropping it to learn of any. The same goes for a file that
/// is dropped by a task that is cancelled.
///
/// The methods that look at the file or change it as a whole, [`sync_all`](File::sync_all),
/// [`sync_data`](File::sync_data), [`set_len`](File::set_len) and [`metadata`](File::metadata),
/// take `&self`, and each first waits for the writes before it to be over, and reports the error of
/// one that failed, as a flush does. Calls of them from several tasks at once take turns.
///
/// # Raw handles
///
/// The file can be given out as a raw file descriptor or handle, for a call that std or the OS has
/// and this type does not. Whatever is done with it behind the file's back is not accounted for:
/// the position of the file may be ahead of where the reads got to, as said above, and the writes
/// that are in flight are not waited for.
///
/// # Example
///
/// A file written and then read through the same handle, from a position it seeks to. The futures
/// are driven by `block_on` from the `futures` crate, but the `block_on` of any executor would do,
/// as the file needs no runtime:
///
/// ```
/// use std::io::SeekFrom;
///
/// use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, executor::block_on};
/// use zruntime::fs::OpenOptions;
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-file-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let mut file = OpenOptions::new()
///         .read(true)
///         .write(true)
///         .create(true)
///         .open(dir.join("notes.txt"))
///         .await?;
///
///     file.write_all(b"hello, world").await?;
///     // The length includes the bytes written, though no flush asked for them to be waited for.
///     assert_eq!(file.metadata().await?.len(), 12);
///
///     file.seek(SeekFrom::Start(7)).await?;
///     let mut word = String::new();
///     file.read_to_string(&mut word).await?;
///     assert_eq!(word, "world");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
///
/// A write that follows a read lands where the reads got to, not where the read ahead of them did:
///
/// ```
/// use futures::{AsyncReadExt, AsyncWriteExt, executor::block_on};
/// use zruntime::fs::{self, OpenOptions};
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-file-write-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let path = dir.join("greeting.txt");
///     fs::write(&path, "hello, world").await?;
///
///     let mut file = OpenOptions::new().read(true).write(true).open(&path).await?;
///     file.read_exact(&mut [0; 5]).await?;
///     file.write_all(b"!").await?;
///     file.flush().await?;
///
///     assert_eq!(fs::read_to_string(&path).await?, "hello! world");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub struct File {
    /// The file, for the operations that are not reads, writes or seeks, and for the raw handle.
    /// The adapter holds the same file, in an [`ArcFile`], to run its operations on.
    file: Arc<std::fs::File>,
    /// The adapter that runs the reads, writes and seeks as blocking work.
    ///
    /// The methods that take `&self` need the adapter to flush it, which takes a mutable borrow,
    /// so it sits in a mutex. The ones that take `&mut self` reach it without locking.
    unblock: Mutex<Unblock<ArcFile>>,
    /// Whether a read has been started since the position of the file was last put right, which
    /// means the adapter may have read ahead of where the reads got to.
    read_ahead: bool,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// This is [`std::fs::File::open`], run as blocking work. It fails if there is no file at
    /// `path`, and if the process may not read it. [`OpenOptions`](super::OpenOptions) opens a file
    /// in other ways.
    pub async fn open<P>(path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_owned();
        let file = unblock(move || std::fs::File::open(path)).await?;
        Ok(File::from(file))
    }

    /// Opens the file at `path` for writing, creating it if it is not there and emptying it if it
    /// is.
    ///
    /// This is [`std::fs::File::create`], run as blocking work. It fails if the directory of the
    /// file does not exist, and if the process may not write to it. The file cannot be read
    /// through the handle: [`OpenOptions`](super::OpenOptions) opens one that can.
    pub async fn create<P>(path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_owned();
        let file = unblock(move || std::fs::File::create(path)).await?;
        Ok(File::from(file))
    }

    /// Waits for the writes so far to reach the file, then has the OS write the data and the
    /// metadata of the file to the disk.
    ///
    /// This is [`std::fs::File::sync_all`], run as blocking work after a flush. It is what tells
    /// whether the file reached the disk, and what reports an error that a write or the closing of
    /// the file would otherwise lose.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::{AsyncWriteExt, executor::block_on};
    /// use zruntime::fs::File;
    ///
    /// # let pid = std::process::id();
    /// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-sync-all-{pid}"));
    /// # std::fs::create_dir_all(&dir).unwrap();
    /// block_on(async {
    ///     let mut file = File::create(dir.join("journal.txt")).await?;
    ///     file.write_all(b"entry").await?;
    ///     file.sync_all().await?;
    ///
    ///     std::io::Result::Ok(())
    /// })
    /// .unwrap();
    /// # std::fs::remove_dir_all(&dir).unwrap();
    /// ```
    pub async fn sync_all(&self) -> io::Result<()> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.sync_all()).await
    }

    /// Waits for the writes so far to reach the file, then has the OS write the data of the file
    /// to the disk, and the metadata only where it is needed to read the data back.
    ///
    /// This is [`std::fs::File::sync_data`], run as blocking work after a flush. It does less than
    /// [`sync_all`](File::sync_all) on a platform that can tell the two apart, and the same on one
    /// that cannot.
    pub async fn sync_data(&self) -> io::Result<()> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.sync_data()).await
    }

    /// Waits for the writes so far to reach the file, then cuts the file short or extends it to
    /// `size` bytes.
    ///
    /// This is [`std::fs::File::set_len`], run as blocking work after a flush. A file that is
    /// extended is filled with zeros. The position of the file stays where it is, even if that is
    /// past the new end, and the reads after this see the file as it now is, from there: what the
    /// reads before it read ahead, of the file as it was, is dropped.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        let mut adapter = self.unblock.lock().await;
        future::poll_fn(|cx| Pin::new(&mut *adapter).poll_flush(cx)).await?;
        if self.read_ahead {
            // Drops the bytes read ahead, which may be of the part that is cut off, or that is
            // extended again with zeros, and puts the position of the OS back to where the reads
            // got to, as `poll_reposition` does before a write. Its outcome is of no interest
            // there, and none here: a file that cannot be sought cannot be cut either, which the
            // `set_len` below reports.
            let current = SeekFrom::Current(0);
            let _ = future::poll_fn(|cx| Pin::new(&mut *adapter).poll_seek(cx, current)).await;
        }

        let file = self.file.clone();
        unblock(move || file.set_len(size)).await
    }

    /// Waits for the writes so far to reach the file, then reads the metadata of the file.
    ///
    /// This is [`std::fs::File::metadata`], run as blocking work after a flush, so that the length
    /// it reports includes every byte written before the call.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.metadata()).await
    }

    /// Changes the permissions of the file to `perm`.
    ///
    /// This is [`std::fs::File::set_permissions`], run as blocking work. What it changes does not
    /// depend on the bytes written, so unlike the methods above it does not wait for them.
    pub async fn set_permissions(&self, perm: Permissions) -> io::Result<()> {
        let file = self.file.clone();
        unblock(move || file.set_permissions(perm)).await
    }

    /// Waits for every write so far to be over, and for the adapter to flush the file.
    ///
    /// The flush leaves what the adapter read ahead where it is, which running the operation on
    /// the file through the adapter would not.
    async fn flush_writes(&self) -> io::Result<()> {
        let mut unblock = self.unblock.lock().await;
        future::poll_fn(|cx| Pin::new(&mut *unblock).poll_flush(cx)).await
    }

    /// Puts the position of the OS back to where the reads got to, if a read may have moved it
    /// past that.
    ///
    /// A seek by `SeekFrom::Current(0)` through the adapter does that: it accounts for the bytes
    /// read ahead. Its outcome is of no interest. A file that cannot be sought, a pipe or a
    /// socket, has no position to put right, and its reads and writes are separate streams, so the
    /// bytes read ahead are kept for the reads that follow, as the adapter keeps them when a seek
    /// fails. One that can be sought does not fail to report where it is.
    fn poll_reposition(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.read_ahead {
            let _ = ready!(Pin::new(self.unblock.get_mut()).poll_seek(cx, SeekFrom::Current(0)));
            self.read_ahead = false;
        }
        Poll::Ready(())
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.file, f)
    }
}

impl From<std::fs::File> for File {
    /// The file of std as an async one, from the position it is at.
    fn from(file: std::fs::File) -> Self {
        let file = Arc::new(file);
        Self {
            unblock: Mutex::new(Unblock::with_capacity(CAPACITY, ArcFile(file.clone()))),
            file,
            read_ahead: false,
        }
    }
}

#[cfg(unix)]
impl From<OwnedFd> for File {
    fn from(fd: OwnedFd) -> Self {
        Self::from(std::fs::File::from(fd))
    }
}

#[cfg(windows)]
impl From<OwnedHandle> for File {
    fn from(handle: OwnedHandle) -> Self {
        Self::from(std::fs::File::from(handle))
    }
}

#[cfg(unix)]
impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsHandle for File {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.file.as_handle()
    }
}

#[cfg(windows)]
impl AsRawHandle for File {
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Set before the read is polled, as one that is pending, or that is given up on, has the
        // pool read ahead all the same.
        self.read_ahead = true;
        Pin::new(self.unblock.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.poll_reposition(cx));
        Pin::new(self.unblock.get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.unblock.get_mut()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The file is closed when it is dropped, which is after the writes the flush waits for.
        Pin::new(self.unblock.get_mut()).poll_close(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        // The adapter takes the bytes read ahead into account, including for a seek from the
        // current position, so there is nothing to put right first.
        let pos = ready!(Pin::new(self.unblock.get_mut()).poll_seek(cx, pos))?;
        // A seek that worked leaves nothing read ahead. One that failed leaves it, and the flag.
        self.read_ahead = false;
        Poll::Ready(Ok(pos))
    }
}

/// The file of std behind an [`Arc`], for the adapter to read, write and seek: through the
/// implementations of std's traits for a reference to a file, which `Arc<File>` does not have.
///
/// The adapter owns the clone of the `Arc` that this holds, and so keeps the file open until its
/// last operation is over, even if the [`File`] was dropped meanwhile.
struct ArcFile(Arc<std::fs::File>);

impl Read for ArcFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self.0).read(buf)
    }
}

impl Write for ArcFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.0).flush()
    }
}

impl Seek for ArcFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&*self.0).seek(pos)
    }
}

/// The most bytes that one operation of a file reads ahead or writes.
///
/// Each operation is a hand-over to the pool, which takes tens of microseconds, so a larger
/// capacity is faster for a large file, and a smaller one is cheaper for a file that is read for a
/// few bytes only: the buffer of a read is zeroed in full when the first read of the file begins.
/// On a 64 MiB file in the page cache, reading went at 0.3 GB/s with the 8 KiB that
/// [`Unblock::new`] has, at 1.6 GB/s with 64 KiB, and at 3.5 GB/s with 256 KiB, while opening a
/// file and reading ten bytes of it took 54, 65 and 85 microseconds. This is the middle one: most
/// of the speed of the larger, and little of the cost of the smaller.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
