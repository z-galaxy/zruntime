//! Windows-specific extensions to [`fs`](super).
//!
//! The extension trait adds to [`OpenOptions`](super::OpenOptions) what the trait of the same name
//! in [`std::os::windows::fs`] adds to the one of std, and is used alike: import the trait, and
//! call its methods on the builder. The trait of std that extends
//! [`Metadata`](super::Metadata), which is the one of std here as well, is re-exported.

use std::{io, path::Path};

use super::sealed::Sealed;
use crate::unblock;

#[doc(no_inline)]
pub use std::os::windows::fs::MetadataExt;

/// Makes `dst` a symbolic link to the directory `src`.
///
/// This is [`std::os::windows::fs::symlink_dir`], run as blocking work. [`symlink_file`] makes a
/// link to a file.
pub async fn symlink_dir<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::os::windows::fs::symlink_dir(src, dst)).await
}

/// Makes `dst` a symbolic link to the file `src`.
///
/// This is [`std::os::windows::fs::symlink_file`], run as blocking work. [`symlink_dir`] makes a
/// link to a directory.
pub async fn symlink_file<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::os::windows::fs::symlink_file(src, dst)).await
}

/// Windows-specific extensions to [`OpenOptions`](super::OpenOptions).
///
/// It is implemented by that type alone, and sealed. Each method sets an argument of the
/// `CreateFile` call that opens the file.
pub trait OpenOptionsExt: Sealed {
    /// Sets the `dwDesiredAccess` argument to `access`, in place of the one that
    /// [`read`](super::OpenOptions::read), [`write`](super::OpenOptions::write) and
    /// [`append`](super::OpenOptions::append) work out between them.
    ///
    /// This gives a finer grain of access than those: an access mode of `0`, for example, opens a
    /// file only to look at its metadata.
    fn access_mode(&mut self, access: u32) -> &mut Self;

    /// Sets the `dwShareMode` argument to `share`.
    ///
    /// It is `FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE` unless this sets it, which
    /// lets other processes read, write, and delete or rename the file while it is open. Leaving
    /// out one of the bits refuses them that, until the handle is closed.
    fn share_mode(&mut self, share: u32) -> &mut Self;

    /// Sets the flags of the `dwFlagsAndAttributes` argument that are neither attributes nor flags
    /// of the security quality of service, such as `FILE_FLAG_DELETE_ON_CLOSE`.
    ///
    /// The flags can set bits and not clear any that the options set, and replace the ones this
    /// set before.
    fn custom_flags(&mut self, flags: u32) -> &mut Self;

    /// Sets the attributes of the `dwFlagsAndAttributes` argument, such as
    /// `FILE_ATTRIBUTE_HIDDEN`.
    ///
    /// They are the attributes of a file that the open creates. For a file that is there already
    /// they have an effect only if the open truncates it, in which case they are added to the
    /// attributes it has.
    fn attributes(&mut self, attributes: u32) -> &mut Self;

    /// Sets the flags of the security quality of service in `dwFlagsAndAttributes`, such as
    /// `SECURITY_IDENTIFICATION`, and `SECURITY_SQOS_PRESENT` along with them.
    ///
    /// They control how far the server of a named pipe may act on behalf of the client that opens
    /// it. They are not set unless this sets them, and should be set when the path to open comes
    /// from somewhere that is not trusted, as it may lead to a named pipe.
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self;
}
