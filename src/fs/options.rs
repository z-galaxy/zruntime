//! [`OpenOptions`], the builder of how a file is opened.

use std::{future::Future, io, path::Path};

use super::{File, sealed::Sealed};
use crate::unblock;

/// A builder of how a file is opened: for reading, writing or both, and what is to be done about a
/// file that is there or is not.
///
/// The options are set on the builder, and then [`open`](OpenOptions::open) opens a file with them,
/// any number of times. They are those of [`std::fs::OpenOptions`], which this wraps, with the same
/// defaults and the same combinations that make an open fail. On unix, the `OpenOptionsExt` trait
/// of the `unix` module adds the permission bits of a new file and the flags the file is opened
/// with, and on Windows, the one of the `windows` module adds the access, the sharing and the
/// attributes.
///
/// # Example
///
/// A file opened to append to has every write go to its end, whatever its position is:
///
/// ```
/// use futures_lite::{AsyncWriteExt, future::block_on};
/// use zruntime::fs::{self, OpenOptions};
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-open-options-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let path = dir.join("log.txt");
///     fs::write(&path, "first\n").await?;
///
///     let mut log = OpenOptions::new().append(true).open(&path).await?;
///     log.write_all(b"second\n").await?;
///     log.flush().await?;
///
///     assert_eq!(fs::read_to_string(&path).await?, "first\nsecond\n");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct OpenOptions(std::fs::OpenOptions);

impl OpenOptions {
    /// A builder with every option off, which opens nothing until at least one of
    /// [`read`](OpenOptions::read), [`write`](OpenOptions::write) and
    /// [`append`](OpenOptions::append) is on.
    pub fn new() -> Self {
        Self(std::fs::OpenOptions::new())
    }

    /// Sets whether the file is opened for reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.0.read(read);
        self
    }

    /// Sets whether the file is opened for writing.
    ///
    /// A write to a file that is there overwrites what it holds, from the start of the file, and
    /// does not shorten it: [`truncate`](OpenOptions::truncate) empties it on opening.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.0.write(write);
        self
    }

    /// Sets whether the file is opened for appending, which is for writing as well, with every
    /// write going to the end of the file, wherever its position is.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.0.append(append);
        self
    }

    /// Sets whether a file that is there is emptied when it is opened.
    ///
    /// This needs the file to be opened for [writing](OpenOptions::write), and fails to open it
    /// otherwise.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.0.truncate(truncate);
        self
    }

    /// Sets whether the file is created when it is not there.
    ///
    /// This needs the file to be opened for [writing](OpenOptions::write) or
    /// [appending](OpenOptions::append), and fails to open it otherwise. A file that is there is
    /// opened as it is.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.0.create(create);
        self
    }

    /// Sets whether the file has to be created by this open, which fails with
    /// [`AlreadyExists`](io::ErrorKind::AlreadyExists) if a file is there.
    ///
    /// The check and the creation are one step, so of several that try to create the same file at
    /// once, exactly one succeeds. [`create`](OpenOptions::create) and
    /// [`truncate`](OpenOptions::truncate) have no effect when this is on, and a symbolic link that
    /// is there is not followed, which fails the open as well. Like them, it needs the file to be
    /// opened for [writing](OpenOptions::write) or [appending](OpenOptions::append).
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.0.create_new(create_new);
        self
    }

    /// Opens the file at `path` with the options of the builder.
    ///
    /// This is [`std::fs::OpenOptions::open`], run as blocking work. It fails for the reasons that
    /// the options give, such as a file that is not there with `create` off, and for the ones the
    /// OS gives, such as a lack of permission. The future does not borrow the builder, which can be
    /// changed or dropped as soon as this returns, and does nothing until it is polled.
    pub fn open<P>(&self, path: P) -> impl Future<Output = io::Result<File>> + use<P>
    where
        P: AsRef<Path>,
    {
        let options = self.0.clone();
        let path = path.as_ref().to_owned();
        async move {
            let file = unblock(move || options.open(path)).await?;
            Ok(File::from(file))
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl Sealed for OpenOptions {}

#[cfg(unix)]
impl super::unix::OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut Self {
        std::os::unix::fs::OpenOptionsExt::mode(&mut self.0, mode);
        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut self.0, flags);
        self
    }
}

#[cfg(windows)]
impl super::windows::OpenOptionsExt for OpenOptions {
    fn access_mode(&mut self, access: u32) -> &mut Self {
        std::os::windows::fs::OpenOptionsExt::access_mode(&mut self.0, access);
        self
    }

    fn share_mode(&mut self, share: u32) -> &mut Self {
        std::os::windows::fs::OpenOptionsExt::share_mode(&mut self.0, share);
        self
    }

    fn custom_flags(&mut self, flags: u32) -> &mut Self {
        std::os::windows::fs::OpenOptionsExt::custom_flags(&mut self.0, flags);
        self
    }

    fn attributes(&mut self, attributes: u32) -> &mut Self {
        std::os::windows::fs::OpenOptionsExt::attributes(&mut self.0, attributes);
        self
    }

    fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        std::os::windows::fs::OpenOptionsExt::security_qos_flags(&mut self.0, flags);
        self
    }
}
