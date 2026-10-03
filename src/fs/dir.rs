//! The directories of [`fs`](super): the stream of the entries of one, an entry, and the builder of
//! new ones.

use std::{
    ffi::OsString,
    fmt,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use futures_core::Stream;

use super::{FileType, Metadata, sealed::Sealed};
use crate::{Unblock, unblock};

/// The stream of the entries of a directory, from [`read_dir()`](super::read_dir).
///
/// It yields an [`io::Result`] of a [`DirEntry`] for each entry, and ends after the last one. The
/// directory is read as the stream is, so an error can come up in the middle of it, and the
/// entries are in no particular order. `.` and `..` are not among them.
///
/// The stream is an [`Unblock`] over the directory of std, and so pulls a batch of entries from it
/// in one piece of blocking work, up to 16, and hands them out one by one. A directory is thus read
/// a little ahead of the entries that the stream has yielded, and an entry made after it was read
/// from may or may not be in the stream. [`read_dir()`](super::read_dir) has an example.
pub struct ReadDir(Unblock<std::fs::ReadDir>);

impl ReadDir {
    /// A stream of the entries of `dir`.
    pub(super) fn new(dir: std::fs::ReadDir) -> Self {
        Self(Unblock::new(dir))
    }
}

impl Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let entry = ready!(Pin::new(&mut self.0).poll_next(cx));
        Poll::Ready(entry.map(|entry| entry.map(|entry| DirEntry(Arc::new(entry)))))
    }
}

impl fmt::Debug for ReadDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadDir").finish_non_exhaustive()
    }
}

/// An entry of a directory, as the stream of [`read_dir()`](super::read_dir) yields it.
///
/// The name of the entry and the path to it are known from the stream, and are read without
/// waiting. What more there is to know of the entry, its [metadata](DirEntry::metadata) and its
/// [type](DirEntry::file_type), is read by blocking work, as the OS may have to go to the disk for
/// it. On unix, the `DirEntryExt` trait of the `unix` module adds the inode number.
///
/// An entry is cheap to clone: the clones share the entry of std.
#[derive(Clone)]
pub struct DirEntry(Arc<std::fs::DirEntry>);

impl DirEntry {
    /// The full path of the entry: the path that was given to [`read_dir()`](super::read_dir),
    /// joined with the name of the entry.
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// The name of the entry alone, without the path of the directory it is in.
    pub fn file_name(&self) -> OsString {
        self.0.file_name()
    }

    /// Reads the metadata of the entry itself, without following a symbolic link.
    ///
    /// This is [`std::fs::DirEntry::metadata`], run as blocking work. For an entry that is a
    /// symbolic link, it describes the link, not what the link points at: the metadata of that is
    /// what [`metadata`](super::metadata) of the entry's [`path`](DirEntry::path) reads. It fails
    /// if the entry has been removed since the directory was read.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let entry = self.0.clone();
        unblock(move || entry.metadata()).await
    }

    /// Reads the type of the entry: a file, a directory or a symbolic link, without following the
    /// link.
    ///
    /// This is [`std::fs::DirEntry::file_type`], run as blocking work. Most platforms hand the type
    /// over with the entry, which makes it a short piece of work, but not all do, and so it is
    /// always run as blocking work.
    pub async fn file_type(&self) -> io::Result<FileType> {
        let entry = self.0.clone();
        unblock(move || entry.file_type()).await
    }
}

impl fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl Sealed for DirEntry {}

#[cfg(unix)]
impl super::unix::DirEntryExt for DirEntry {
    fn ino(&self) -> u64 {
        std::os::unix::fs::DirEntryExt::ino(&*self.0)
    }
}

/// A builder of directories, with the options for how they are made.
///
/// The options are set on the builder, and then [`create`](DirBuilder::create) makes a directory
/// with them, any number of times. On unix, the `DirBuilderExt` trait of the `unix` module adds the
/// permission bits a new directory gets.
///
/// # Example
///
/// ```
/// use futures_lite::future::block_on;
/// use zruntime::fs::DirBuilder;
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-dir-builder-{pid}"));
/// block_on(async {
///     let path = dir.join("one").join("two");
///     DirBuilder::new().recursive(true).create(&path).await?;
///
///     assert!(path.is_dir());
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Debug, Default)]
pub struct DirBuilder {
    /// Whether the parents that are missing are made as well, and a directory that is there
    /// already is no error.
    recursive: bool,
    /// The permission bits of the directories made, where they are not the default.
    #[cfg(unix)]
    mode: Option<u32>,
}

impl DirBuilder {
    /// A builder with the options of std's: `recursive` is off.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether the directory made has its missing parents made as well, and whether a
    /// directory that is there already is no error, as [`create_dir_all`](super::create_dir_all)
    /// has it.
    ///
    /// The parents are made with the same options as the directory itself. This is off for a new
    /// builder.
    pub fn recursive(&mut self, recursive: bool) -> &mut Self {
        self.recursive = recursive;
        self
    }

    /// Makes the directory at `path` with the options of the builder.
    ///
    /// This is [`std::fs::DirBuilder::create`], run as blocking work. The future does not borrow
    /// the builder, which can be changed or dropped as soon as this returns, and does nothing until
    /// it is polled.
    pub fn create<P>(&self, path: P) -> impl Future<Output = io::Result<()>> + use<P>
    where
        P: AsRef<Path>,
    {
        let builder = self.to_std();
        let path = path.as_ref().to_owned();
        async move { unblock(move || builder.create(path)).await }
    }

    /// The builder of std with the options of this one.
    fn to_std(&self) -> std::fs::DirBuilder {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(self.recursive);
        #[cfg(unix)]
        if let Some(mode) = self.mode {
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, mode);
        }
        builder
    }
}

impl Sealed for DirBuilder {}

#[cfg(unix)]
impl super::unix::DirBuilderExt for DirBuilder {
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = Some(mode);
        self
    }
}
