//! Unix-specific extensions to [`fs`](super).
//!
//! The extension traits add to the builders and the entries of the parent module what the traits of
//! the same names in [`std::os::unix::fs`] add to those of std, and are used alike: import the
//! trait, and call its methods on the builder or the entry. The traits of std that extend
//! [`Metadata`](super::Metadata), [`Permissions`](super::Permissions) and
//! [`FileType`](super::FileType), which are those of std here as well, are re-exported.

use std::{io, path::Path};

use super::sealed::Sealed;
use crate::unblock;

#[doc(no_inline)]
pub use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

/// Makes `dst` a symbolic link to `src`.
///
/// This is [`std::os::unix::fs::symlink`], run as blocking work. The link holds the path `src` as
/// it was given, which is read relative to the directory of the link and not to the directory of
/// the process, and `src` need not exist.
///
/// # Example
///
/// ```
/// use futures::executor::block_on;
/// use zruntime::fs::{self, unix};
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-symlink-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let target = dir.join("target.txt");
///     let link = dir.join("link.txt");
///     fs::write(&target, "pointed at").await?;
///
///     unix::symlink(&target, &link).await?;
///
///     assert_eq!(fs::read_link(&link).await?, target);
///     assert_eq!(fs::read_to_string(&link).await?, "pointed at");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub async fn symlink<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::os::unix::fs::symlink(src, dst)).await
}

/// Unix-specific extensions to [`DirBuilder`](super::DirBuilder).
///
/// It is implemented by that type alone, and sealed.
pub trait DirBuilderExt: Sealed {
    /// Sets the permission bits that the directories made are created with.
    ///
    /// The OS clears the bits that the umask of the process has set, so the bits of a directory are
    /// usually fewer than these. They are `0o777` unless this sets them, and apply to the parents
    /// made by a [recursive](super::DirBuilder::recursive) builder as well.
    fn mode(&mut self, mode: u32) -> &mut Self;
}

/// Unix-specific extensions to [`DirEntry`](super::DirEntry).
///
/// It is implemented by that type alone, and sealed.
pub trait DirEntryExt: Sealed {
    /// The inode number of the entry, as the directory holds it: the `d_ino` of the entry that the
    /// OS handed over, which is known without asking the disk for more.
    fn ino(&self) -> u64;
}

/// Unix-specific extensions to [`OpenOptions`](super::OpenOptions).
///
/// It is implemented by that type alone, and sealed.
pub trait OpenOptionsExt: Sealed {
    /// Sets the permission bits that a file is created with, if the open creates it.
    ///
    /// The OS clears the bits that the umask of the process has set, so the bits of a file are
    /// usually fewer than these. They are `0o666` unless this sets them, and have no effect on a
    /// file that is there already.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::executor::block_on;
    /// use zruntime::fs::{self, OpenOptions, unix::{OpenOptionsExt, PermissionsExt}};
    ///
    /// # let pid = std::process::id();
    /// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-open-mode-{pid}"));
    /// # std::fs::create_dir_all(&dir).unwrap();
    /// block_on(async {
    ///     let path = dir.join("secret.txt");
    ///     OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path).await?;
    ///
    ///     let permissions = fs::metadata(&path).await?.permissions();
    ///     assert_eq!(permissions.mode() & 0o777, 0o600);
    ///
    ///     std::io::Result::Ok(())
    /// })
    /// .unwrap();
    /// # std::fs::remove_dir_all(&dir).unwrap();
    /// ```
    fn mode(&mut self, mode: u32) -> &mut Self;

    /// Passes `flags` to the call that opens the file, along with the ones that the other options
    /// work out.
    ///
    /// The bits of the access mode are cleared from `flags`, so that they cannot disagree with
    /// [`read`](super::OpenOptions::read), [`write`](super::OpenOptions::write) and
    /// [`append`](super::OpenOptions::append). The flags can set bits and not clear any that the
    /// options set, and replace the ones this set before.
    fn custom_flags(&mut self, flags: i32) -> &mut Self;
}
