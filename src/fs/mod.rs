//! Async access to the filesystem, as blocking work on the pool of threads that [`unblock()`] hands
//! its work to.
//!
//! A file cannot be waited on for readiness the way a socket can. An OS either cannot watch a
//! regular file at all, as Linux cannot, or reports it ready whether or not the disk has its data
//! at hand, and the call that follows blocks the thread for as long as the disk takes. Made from
//! the thread that polls a task, such a call holds up every other task that thread has to poll, so
//! each operation of this module is blocking work instead: it is handed to the pool through
//! [`unblock()`], and the task that awaits it is woken once it is over. The module needs no
//! runtime, and works under any executor, as [`unblock()`] does.
//!
//! It has the shape of [`std::fs`]. The functions of the same names, from [`read`] to [`write()`],
//! take their paths by `AsRef<Path>` and fail with the errors of the function of std they run; the
//! types of the same names, [`OpenOptions`], [`DirBuilder`], [`DirEntry`] and [`File`], are
//! built and used alike. What a function or a type has of the platform it runs on, the permission
//! bits of a new file or the flags a file is opened with, comes with the extension traits in the
//! `unix` and `windows` modules, each present on its own platform alone, and is the same as in
//! std. Code written against `smol::fs` is ported to this module by changing the path.
//!
//! # Handing the work over
//!
//! A function converts its arguments to owned values before it first waits, so that the work does
//! not borrow from the task, and hands the work to the pool when its future is first polled. From
//! then on the work runs to its end whether or not the future is polled, and even if the future is
//! dropped: dropping a future gives up the wait for the outcome, not the work, which nothing can
//! stop from outside. A [`remove_dir_all`] that is given up on goes on removing, and a [`write()`]
//! writes. Where the work is a single call, it is done or not done; where it is many, as in a
//! removal, a crash or a full disk may leave it half done, as it would in std.
//!
//! # Files
//!
//! A [`File`] reads and writes through [`Unblock`](crate::Unblock), the adapter that runs each
//! operation on a blocking handle as blocking work, and so implements `futures-io`'s `AsyncRead`,
//! `AsyncWrite` and `AsyncSeek`. A read reads ahead of the buffer it was given, and a write is done
//! once its bytes are handed to the pool: [`File`] says what that means for the file and for the
//! bytes written when the file is dropped.
//!
//! # Example
//!
//! The paths of the example are in a directory made for it, which stands in for the directory of
//! a program. The futures are driven by `block_on` from the `futures-lite` crate, but the
//! `block_on` of any executor would do, as the module needs no runtime:
//!
//! ```
//! use futures_lite::{AsyncReadExt, AsyncWriteExt, future::block_on};
//! use zruntime::fs::{self, File};
//!
//! # let pid = std::process::id();
//! # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-module-{pid}"));
//! # std::fs::create_dir_all(&dir).unwrap();
//! block_on(async {
//!     let path = dir.join("greeting.txt");
//!
//!     let mut file = File::create(&path).await?;
//!     file.write_all(b"hello, world").await?;
//!     file.flush().await?;
//!
//!     assert_eq!(fs::read_to_string(&path).await?, "hello, world");
//!     assert_eq!(fs::metadata(&path).await?.len(), 12);
//!
//!     fs::rename(&path, dir.join("farewell.txt")).await?;
//!     assert!(fs::metadata(&path).await.is_err());
//!
//!     let mut text = String::new();
//!     File::open(dir.join("farewell.txt")).await?.read_to_string(&mut text).await?;
//!     assert_eq!(text, "hello, world");
//!
//!     std::io::Result::Ok(())
//! })
//! .unwrap();
//! # std::fs::remove_dir_all(&dir).unwrap();
//! ```
//!
//! # Difference with `async-fs`
//!
//! This module is modelled on [`async-fs`], the crate behind `smol::fs`, and differs from it in
//! these places:
//!
//! * It is built on the pool and the adapter of this crate, not on those of `blocking`. A thread of
//!   the pool is kept for ten seconds after its work, and there are at most 500 of them: see
//!   [`unblock()`].
//! * [`File::metadata`] waits for the bytes written to the file to reach it, as [`File::sync_all`]
//!   and [`File::set_len`] do, so the length it reports includes every write that was done before
//!   it. In `async-fs`, it reports the file as it is at that moment, which may be short of a write
//!   still on its way.
//! * [`File`] keeps no logical position and no flag of its own for a write that is yet to be
//!   flushed. The adapter knows where the reads got to, so the only thing the file has to do is say
//!   that a write comes after a read, and a flush is always passed on: one for a file that was not
//!   written to costs a hand-over and does nothing else.
//! * The [`ReadDir`] stream pulls up to 16 entries from the directory in one hand-over, where
//!   `async-fs` hands over once per entry.
//! * The futures of [`DirBuilder::create`] and [`OpenOptions::open`] do nothing until polled, as
//!   those of the functions do, where those of `async-fs` start the work of the first at once.
//!
//! [`async-fs`]: https://crates.io/crates/async-fs

mod dir;
mod file;
mod options;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

use std::{
    io,
    path::{Path, PathBuf},
};

pub use dir::{DirBuilder, DirEntry, ReadDir};
pub use file::File;
pub use options::OpenOptions;
#[doc(no_inline)]
pub use std::fs::{FileType, Metadata, Permissions};

use crate::unblock;

/// Resolves `path` to its canonical form: absolute, with every `.` and `..` resolved and every
/// symbolic link followed.
///
/// This is [`std::fs::canonicalize`], run as blocking work. It fails if `path`, or any directory
/// on the way to it, does not exist.
pub async fn canonicalize<P>(path: P) -> io::Result<PathBuf>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::canonicalize(path)).await
}

/// Copies the contents and the permissions of the file `src` to `dst`, and tells how many bytes
/// that was.
///
/// This is [`std::fs::copy`], run as blocking work. A `dst` that exists is overwritten, and one
/// that is the same file as `src` is likely to be truncated by that. To copy between two open
/// [`File`]s instead, use an async copy that reads and writes through them, such as
/// `futures_lite::io::copy`.
pub async fn copy<P, Q>(src: P, dst: Q) -> io::Result<u64>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::copy(src, dst)).await
}

/// Creates a new, empty directory at `path`.
///
/// This is [`std::fs::create_dir`], run as blocking work. It fails if the parent of `path` does not
/// exist, and if `path` does: [`create_dir_all`] makes the missing parents as well, and does not
/// mind a directory that is there already.
pub async fn create_dir<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::create_dir(path)).await
}

/// Creates a directory at `path`, and every one of its parents that does not exist.
///
/// This is [`std::fs::create_dir_all`], run as blocking work. It does not fail for a directory
/// that is there already, nor for one that another thread or process makes in the meantime.
pub async fn create_dir_all<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::create_dir_all(path)).await
}

/// Makes `dst` another name for the file `src`.
///
/// This is [`std::fs::hard_link`], run as blocking work. The two names are the same file, and most
/// operating systems allow that only for two names on the same filesystem.
pub async fn hard_link<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::hard_link(src, dst)).await
}

/// Reads the metadata of the file or directory at `path`, following symbolic links to what they
/// point at.
///
/// This is [`std::fs::metadata`], run as blocking work. [`symlink_metadata`] reads the link itself
/// instead.
pub async fn metadata<P>(path: P) -> io::Result<Metadata>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::metadata(path)).await
}

/// Reads the whole of the file at `path` into a vector of bytes.
///
/// This is [`std::fs::read`], run as blocking work: it sizes the vector by the length of the file
/// where it can, and does all of its reading in one piece of work. That makes it quicker than
/// opening a [`File`] and reading that, which has the pool do a piece of work for every part of the
/// file it reads. [`read_to_string`] reads text instead.
pub async fn read<P>(path: P) -> io::Result<Vec<u8>>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read(path)).await
}

/// Opens the directory at `path` for listing, and hands back the stream of its entries.
///
/// This is [`std::fs::read_dir`], run as blocking work, which fails if `path` is not a directory
/// that can be read. The entries then come out of [`ReadDir`] in no particular order, and it is
/// that stream that can fail while it is read.
///
/// # Example
///
/// ```
/// use futures_lite::{StreamExt, future::block_on};
/// use zruntime::fs;
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-read-dir-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     fs::write(dir.join("a.txt"), "first").await?;
///     fs::create_dir(dir.join("b")).await?;
///
///     let mut entries = fs::read_dir(&dir).await?;
///     let mut names = Vec::new();
///     while let Some(entry) = entries.next().await {
///         let entry = entry?;
///         names.push((entry.file_name(), entry.file_type().await?.is_dir()));
///     }
///     names.sort();
///
///     assert_eq!(names, [("a.txt".into(), false), ("b".into(), true)]);
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub async fn read_dir<P>(path: P) -> io::Result<ReadDir>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    let dir = unblock(move || std::fs::read_dir(path)).await?;
    Ok(ReadDir::new(dir))
}

/// Reads where the symbolic link at `path` points to.
///
/// This is [`std::fs::read_link`], run as blocking work. The path it finds is the one the link
/// holds, which may be relative to the directory of the link, and may lead nowhere.
pub async fn read_link<P>(path: P) -> io::Result<PathBuf>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read_link(path)).await
}

/// Reads the whole of the file at `path` into a string.
///
/// This is [`std::fs::read_to_string`], run as blocking work, which fails with
/// [`InvalidData`](io::ErrorKind::InvalidData) for a file that is not UTF-8. [`read`] reads bytes
/// instead.
pub async fn read_to_string<P>(path: P) -> io::Result<String>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read_to_string(path)).await
}

/// Removes the directory at `path`, which has to be empty.
///
/// This is [`std::fs::remove_dir`], run as blocking work. [`remove_dir_all`] removes a directory
/// together with what is in it.
pub async fn remove_dir<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_dir(path)).await
}

/// Removes the directory at `path` and everything in it.
///
/// This is [`std::fs::remove_dir_all`], run as blocking work. A directory with a lot in it takes a
/// while to remove, which is what the pool is for, and an error part of the way leaves what was
/// not removed yet where it is.
pub async fn remove_dir_all<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_dir_all(path)).await
}

/// Removes the file at `path`.
///
/// This is [`std::fs::remove_file`], run as blocking work. It removes a name, which for a symbolic
/// link is the link and not what it points at.
pub async fn remove_file<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_file(path)).await
}

/// Renames the file or directory at `src` to `dst`, replacing what is at `dst` where the platform
/// allows it.
///
/// This is [`std::fs::rename`], run as blocking work. It fails where `src` and `dst` are on
/// different filesystems.
pub async fn rename<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::rename(src, dst)).await
}

/// Changes the permissions of the file or directory at `path` to `perm`.
///
/// This is [`std::fs::set_permissions`], run as blocking work. The permissions to change to are
/// usually those from [`metadata`], changed as wanted.
pub async fn set_permissions<P>(path: P, perm: Permissions) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::set_permissions(path, perm)).await
}

/// Reads the metadata of the file, directory or symbolic link at `path`, without following a link.
///
/// This is [`std::fs::symlink_metadata`], run as blocking work. [`metadata`] reads what a link
/// points at instead.
pub async fn symlink_metadata<P>(path: P) -> io::Result<Metadata>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::symlink_metadata(path)).await
}

/// Makes `contents` the new contents of the file at `path`, creating the file if it is not there.
///
/// This is [`std::fs::write`], run as blocking work, which replaces what the file held. The bytes
/// are copied before the function first waits, as the work runs on another thread, and a slice of
/// the caller's cannot go there.
pub async fn write<P, C>(path: P, contents: C) -> io::Result<()>
where
    P: AsRef<Path>,
    C: AsRef<[u8]>,
{
    let path = path.as_ref().to_owned();
    let contents = contents.as_ref().to_owned();
    unblock(move || std::fs::write(path, contents)).await
}

/// What the `unix` and `windows` modules seal their extension traits with, out of reach of every
/// crate but this one.
///
/// The trait is public in name only, so that the extension traits can name it as their
/// supertrait, and sits in a module nobody outside can reach, so that nobody outside can implement
/// it: the types of this module that it is implemented for are the only ones the extension traits
/// are meant for.
pub(crate) mod sealed {
    /// Marks a type of this module that an extension trait is implemented for.
    pub trait Sealed {}
}
