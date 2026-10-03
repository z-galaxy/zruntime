//! Tests of `zruntime::fs`, async access to the filesystem as blocking work.
//!
//! Each test works in a directory of its own under the temporary directory of the OS, which goes
//! with everything in it when the test ends, and drives the futures with the `block_on` of
//! `futures-lite`. They come in the order of what they pin down: that each function does what the
//! function of std it runs does, and takes paths and bytes of any type that converts; that the
//! stream of a directory lists every entry, and that an entry knows what it is; that the builders
//! of directories and of files set what they are told to, that their futures do not borrow them and
//! do nothing before they are polled; that a file hands back what it was given, wherever it is
//! sought to, and puts its position right where a write follows a read; that what looks at a file
//! as a whole sees every byte written before it, and that a file dropped right after a write loses
//! none; that a file gives out its raw handle and prints; and, where the platform has them, what
//! its extension traits add.

use std::{
    env,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

use futures_lite::{
    AsyncReadExt, AsyncSeekExt, AsyncWriteExt, StreamExt,
    future::{block_on, poll_once},
};
use ntest::timeout;

use crate::fs::{self, DirBuilder, DirEntry, File, OpenOptions, ReadDir};

#[cfg(unix)]
use crate::fs::unix::{self, DirBuilderExt, DirEntryExt, OpenOptionsExt, PermissionsExt};
#[cfg(windows)]
use std::os::windows::io::{AsHandle, AsRawHandle};
#[cfg(unix)]
use std::os::{
    fd::{AsFd, AsRawFd, OwnedFd},
    unix::{fs::MetadataExt, net::UnixStream},
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::{
    future::Future,
    pin::{Pin, pin},
    task::{Context, Waker},
};

/// A path comes out of `canonicalize` with its dots resolved, as std resolves them.
#[test]
#[timeout(15000)]
fn canonicalize_resolves_dots() {
    let dir = TempDir::new();
    std::fs::create_dir(dir.join("sub")).unwrap();

    let canonical = block_on(fs::canonicalize(dir.join("sub").join("..").join("sub"))).unwrap();

    assert_eq!(canonical, std::fs::canonicalize(dir.join("sub")).unwrap());
}

/// A path that is not there cannot be canonicalized.
#[test]
#[timeout(15000)]
fn canonicalize_fails_for_a_missing_path() {
    let dir = TempDir::new();

    let error = block_on(fs::canonicalize(dir.join("missing"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// `copy` makes the destination hold what the source does, and says how many bytes that is.
#[test]
#[timeout(15000)]
fn copy_copies_the_contents_and_counts_the_bytes() {
    let dir = TempDir::new();
    std::fs::write(dir.join("from"), "some text").unwrap();

    let copied = block_on(fs::copy(dir.join("from"), dir.join("to"))).unwrap();

    assert_eq!(copied, 9);
    assert_eq!(
        std::fs::read_to_string(dir.join("to")).unwrap(),
        "some text"
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("from")).unwrap(),
        "some text"
    );
}

/// `copy` of a file that is not there fails.
#[test]
#[timeout(15000)]
fn copy_fails_for_a_missing_source() {
    let dir = TempDir::new();

    let error = block_on(fs::copy(dir.join("missing"), dir.join("to"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(!dir.join("to").exists());
}

/// `create_dir` makes a directory in one that is there, and refuses one that is there itself.
#[test]
#[timeout(15000)]
fn create_dir_makes_one_directory() {
    let dir = TempDir::new();

    block_on(fs::create_dir(dir.join("new"))).unwrap();
    let error = block_on(fs::create_dir(dir.join("new"))).unwrap_err();

    assert!(dir.join("new").is_dir());
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
}

/// `create_dir` does not make the parents that are missing.
#[test]
#[timeout(15000)]
fn create_dir_fails_without_a_parent() {
    let dir = TempDir::new();

    let error = block_on(fs::create_dir(dir.join("missing").join("new"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// `create_dir_all` makes every parent that is missing, and takes a directory that is there as
/// done.
#[test]
#[timeout(15000)]
fn create_dir_all_makes_every_missing_parent() {
    let dir = TempDir::new();
    let deep = dir.join("one").join("two").join("three");

    block_on(fs::create_dir_all(&deep)).unwrap();
    block_on(fs::create_dir_all(&deep)).unwrap();

    assert!(deep.is_dir());
}

/// The second name that `hard_link` makes is the same file as the first.
#[test]
#[timeout(15000)]
fn hard_link_makes_a_second_name_for_the_same_file() {
    let dir = TempDir::new();
    std::fs::write(dir.join("first"), "before").unwrap();

    block_on(fs::hard_link(dir.join("first"), dir.join("second"))).unwrap();
    std::fs::write(dir.join("second"), "after").unwrap();

    assert_eq!(std::fs::read_to_string(dir.join("first")).unwrap(), "after");
}

/// `metadata` tells a file from a directory, and gives the length of a file, as `symlink_metadata`
/// does for what is not a link.
#[test]
#[timeout(15000)]
fn metadata_tells_the_length_and_the_kind() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "four").unwrap();

    let file: fs::Metadata = block_on(fs::metadata(dir.join("file"))).unwrap();
    let directory = block_on(fs::metadata(dir.path())).unwrap();
    let not_a_link = block_on(fs::symlink_metadata(dir.join("file"))).unwrap();

    assert!(file.is_file());
    assert_eq!(file.len(), 4);
    assert!(directory.is_dir());
    assert!(not_a_link.is_file());
    assert_eq!(not_a_link.len(), 4);
}

/// `metadata` of a path that is not there fails.
#[test]
#[timeout(15000)]
fn metadata_fails_for_a_missing_path() {
    let dir = TempDir::new();

    let error = block_on(fs::metadata(dir.join("missing"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// `read` hands back every byte of a file, those that are not text included.
#[test]
#[timeout(15000)]
fn read_returns_the_bytes_of_a_file() {
    let dir = TempDir::new();
    let data = bytes(100_000);
    std::fs::write(dir.join("file"), &data).unwrap();

    assert_eq!(block_on(fs::read(dir.join("file"))).unwrap(), data);
}

/// `read_to_string` hands back the text of a file.
#[test]
#[timeout(15000)]
fn read_to_string_returns_the_text_of_a_file() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "héllo").unwrap();

    assert_eq!(
        block_on(fs::read_to_string(dir.join("file"))).unwrap(),
        "héllo"
    );
}

/// `read_to_string` refuses bytes that are not UTF-8.
#[test]
#[timeout(15000)]
fn read_to_string_fails_for_bytes_that_are_not_utf8() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), [0xff, 0xfe]).unwrap();

    let error = block_on(fs::read_to_string(dir.join("file"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

/// `remove_dir` removes an empty directory, and not one with something in it.
#[test]
#[timeout(15000)]
fn remove_dir_removes_an_empty_directory_only() {
    let dir = TempDir::new();
    std::fs::create_dir(dir.join("empty")).unwrap();
    std::fs::create_dir(dir.join("full")).unwrap();
    std::fs::write(dir.join("full").join("file"), "").unwrap();

    block_on(fs::remove_dir(dir.join("empty"))).unwrap();
    let full = block_on(fs::remove_dir(dir.join("full")));

    assert!(!dir.join("empty").exists());
    assert!(full.is_err());
    assert!(dir.join("full").join("file").exists());
}

/// `remove_dir_all` removes a directory with everything in it, down the tree.
#[test]
#[timeout(15000)]
fn remove_dir_all_removes_a_tree() {
    let dir = TempDir::new();
    let tree = dir.join("tree");
    std::fs::create_dir_all(tree.join("a").join("b")).unwrap();
    std::fs::write(tree.join("a").join("b").join("file"), "deep").unwrap();
    std::fs::write(tree.join("file"), "shallow").unwrap();

    block_on(fs::remove_dir_all(&tree)).unwrap();

    assert!(!tree.exists());
}

/// `remove_file` removes a file, and fails for one that is not there.
#[test]
#[timeout(15000)]
fn remove_file_removes_a_file() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    block_on(fs::remove_file(dir.join("file"))).unwrap();
    let error = block_on(fs::remove_file(dir.join("file"))).unwrap_err();

    assert!(!dir.join("file").exists());
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// `rename` moves a file to a new name, in place of the one that was at that name.
#[test]
#[timeout(15000)]
fn rename_moves_a_file_over_another() {
    let dir = TempDir::new();
    std::fs::write(dir.join("old"), "moved").unwrap();
    std::fs::write(dir.join("new"), "replaced").unwrap();

    block_on(fs::rename(dir.join("old"), dir.join("new"))).unwrap();

    assert!(!dir.join("old").exists());
    assert_eq!(std::fs::read_to_string(dir.join("new")).unwrap(), "moved");
}

/// `set_permissions` makes a file read-only, and writable again.
#[test]
#[timeout(15000)]
fn set_permissions_changes_the_permissions_of_a_file() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();
    let writable: fs::Permissions = std::fs::metadata(dir.join("file")).unwrap().permissions();
    let mut read_only = writable.clone();
    read_only.set_readonly(true);

    block_on(fs::set_permissions(dir.join("file"), read_only)).unwrap();
    let after = std::fs::metadata(dir.join("file"))
        .unwrap()
        .permissions()
        .readonly();
    // Windows does not remove a file that is read-only, so the directory could not go otherwise.
    block_on(fs::set_permissions(dir.join("file"), writable)).unwrap();

    assert!(after);
    assert!(
        !std::fs::metadata(dir.join("file"))
            .unwrap()
            .permissions()
            .readonly()
    );
}

/// `write` makes a file with the bytes it is given, and replaces what a file held.
#[test]
#[timeout(15000)]
fn write_creates_a_file_and_replaces_its_contents() {
    let dir = TempDir::new();

    block_on(fs::write(dir.join("file"), "a longer text")).unwrap();
    block_on(fs::write(dir.join("file"), "short")).unwrap();

    assert_eq!(std::fs::read_to_string(dir.join("file")).unwrap(), "short");
}

/// The paths and the bytes may be of any type that converts to a path or to bytes, owned or
/// borrowed.
#[test]
#[timeout(15000)]
fn paths_and_bytes_may_be_of_any_type_that_converts() {
    let dir = TempDir::new();
    let path = dir.join("file");
    let as_string = path.to_str().unwrap().to_owned();

    block_on(async {
        fs::write(&as_string, b"array").await.unwrap();
        fs::write(as_string.as_str(), vec![b'v']).await.unwrap();
        fs::write(path.clone(), String::from("string"))
            .await
            .unwrap();
        fs::write(path.as_path(), &b"slice"[..]).await.unwrap();
        fs::write(path.as_os_str(), "os str").await.unwrap();
        assert_eq!(fs::read_to_string(as_string).await.unwrap(), "os str");
        assert_eq!(fs::read(path).await.unwrap(), b"os str");
    });
}

/// The futures of the functions are `Send`, and `'static` where their arguments are, as a task
/// that awaits them needs. They are not polled, so nothing is done.
#[test]
#[timeout(15000)]
fn the_futures_are_send() {
    fn send<T>(_: T)
    where
        T: Send,
    {
    }

    fn send_and_static<T>(_: T)
    where
        T: Send + 'static,
    {
    }

    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();
    let file = block_on(File::open(dir.join("file"))).unwrap();
    let entry = block_on(block_on(fs::read_dir(dir.path())).unwrap().next())
        .unwrap()
        .unwrap();

    send_and_static(fs::read(String::new()));
    send_and_static(fs::write(PathBuf::new(), Vec::<u8>::new()));
    send_and_static(fs::read_dir(PathBuf::new()));
    send_and_static(fs::copy(String::new(), String::new()));
    send_and_static(File::open(String::new()));
    send_and_static(DirBuilder::new().create(String::new()));
    send_and_static(OpenOptions::new().open(String::new()));
    send(entry.metadata());
    send(entry.file_type());
    send(file.sync_all());
    send(file.sync_data());
    send(file.set_len(0));
    send(file.metadata());
    send(file.set_permissions(std::fs::metadata(dir.join("file")).unwrap().permissions()));
}

/// The types are `Send` and `Sync`, and the stream and the file are `Unpin`, so that a task that
/// holds one can move between threads and a combinator can poll it.
#[test]
#[timeout(15000)]
fn the_types_are_send_and_sync() {
    fn send_and_sync<T>()
    where
        T: Send + Sync,
    {
    }

    fn unpinned<T>()
    where
        T: Unpin,
    {
    }

    send_and_sync::<File>();
    send_and_sync::<ReadDir>();
    send_and_sync::<DirEntry>();
    send_and_sync::<DirBuilder>();
    send_and_sync::<OpenOptions>();
    unpinned::<File>();
    unpinned::<ReadDir>();
}

/// `read_link` fails for a path that is not a symbolic link, and for one that is not there.
#[test]
#[timeout(15000)]
fn read_link_fails_for_a_path_that_is_no_link() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    assert!(block_on(fs::read_link(dir.join("file"))).is_err());
    let error = block_on(fs::read_link(dir.join("missing"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// A symbolic link is read for where it points to, and `symlink_metadata` looks at the link and
/// `metadata` at what it points to.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn symlinks_are_made_read_and_not_followed_by_symlink_metadata() {
    let dir = TempDir::new();
    std::fs::write(dir.join("target"), "pointed at").unwrap();

    block_on(unix::symlink(dir.join("target"), dir.join("link"))).unwrap();

    assert_eq!(
        block_on(fs::read_link(dir.join("link"))).unwrap(),
        dir.join("target")
    );
    let link = block_on(fs::symlink_metadata(dir.join("link"))).unwrap();
    assert!(link.file_type().is_symlink());
    let followed = block_on(fs::metadata(dir.join("link"))).unwrap();
    assert!(followed.file_type().is_file());
    assert_eq!(
        block_on(fs::read_to_string(dir.join("link"))).unwrap(),
        "pointed at"
    );
}

/// A stream of a directory lists every entry of it, more of them than one batch pulled from the
/// directory at a time holds, and then ends.
#[test]
#[timeout(15000)]
fn read_dir_lists_every_entry() {
    let dir = TempDir::new();
    let mut expected = Vec::new();
    for index in 0..40 {
        std::fs::write(dir.join(format!("file-{index:02}")), "").unwrap();
        expected.push(format!("file-{index:02}"));
    }
    for name in ["dir-a", "dir-b", "dir-c"] {
        std::fs::create_dir(dir.join(name)).unwrap();
        expected.push(name.to_owned());
    }
    expected.sort();

    let mut entries = block_on(fs::read_dir(dir.path())).unwrap();
    let mut listed = Vec::new();
    while let Some(entry) = block_on(entries.next()) {
        listed.push(entry.unwrap().file_name().into_string().unwrap());
    }
    listed.sort();

    assert_eq!(listed, expected);
    assert!(block_on(entries.next()).is_none());
}

/// The stream of an empty directory ends at once.
#[test]
#[timeout(15000)]
fn read_dir_of_an_empty_directory_yields_nothing() {
    let dir = TempDir::new();

    let mut entries = block_on(fs::read_dir(dir.path())).unwrap();

    assert!(block_on(entries.next()).is_none());
}

/// A directory that is not there, and a path that is not a directory, cannot be read.
#[test]
#[timeout(15000)]
fn read_dir_fails_for_a_path_that_is_no_directory() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    let missing = block_on(fs::read_dir(dir.join("missing"))).unwrap_err();
    let file = block_on(fs::read_dir(dir.join("file")));

    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    assert!(file.is_err());
}

/// An entry gives its name and its path, and, by blocking work, its type and its metadata. A clone
/// of it is the same entry.
#[test]
#[timeout(15000)]
fn entries_know_their_path_name_type_and_metadata() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "12345").unwrap();
    std::fs::create_dir(dir.join("sub")).unwrap();

    let mut entries = block_on(fs::read_dir(dir.path())).unwrap();
    let mut seen = Vec::new();
    while let Some(entry) = block_on(entries.next()) {
        let entry = entry.unwrap();
        let clone = entry.clone();
        assert_eq!(clone.path(), entry.path());
        assert!(format!("{entry:?}").contains(entry.file_name().to_str().unwrap()));
        let file_type = block_on(entry.file_type()).unwrap();
        let metadata = block_on(clone.metadata()).unwrap();
        seen.push((entry.path(), file_type.is_dir(), metadata.len()));
    }
    seen.sort();

    assert_eq!(seen[0].0, dir.join("file"));
    assert!(!seen[0].1);
    assert_eq!(seen[0].2, 5);
    assert_eq!(seen[1].0, dir.join("sub"));
    assert!(seen[1].1);
    assert_eq!(seen.len(), 2);
}

/// The stream of a directory prints as a struct, without anything of the directory.
#[test]
#[timeout(15000)]
fn a_stream_of_a_directory_prints_as_a_struct() {
    let dir = TempDir::new();

    let entries = block_on(fs::read_dir(dir.path())).unwrap();

    assert_eq!(format!("{entries:?}"), "ReadDir { .. }");
}

/// An entry gives the inode number the directory holds for it, which is the one of the file.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn entries_know_their_inode_number() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    let mut entries = block_on(fs::read_dir(dir.path())).unwrap();
    let entry = block_on(entries.next()).unwrap().unwrap();

    let metadata = std::fs::metadata(dir.join("file")).unwrap();
    assert_eq!(entry.ino(), metadata.ino());
}

/// A builder makes the directory it is given the path of, in one that is there, and neither makes
/// the parents that are missing nor takes a directory that is there as done.
#[test]
#[timeout(15000)]
fn a_dir_builder_makes_one_directory_by_default() {
    let dir = TempDir::new();
    let builder = DirBuilder::new();

    block_on(builder.create(dir.join("new"))).unwrap();
    let again = block_on(builder.create(dir.join("new"))).unwrap_err();
    let orphan = block_on(builder.create(dir.join("missing").join("new"))).unwrap_err();

    assert!(dir.join("new").is_dir());
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(orphan.kind(), io::ErrorKind::NotFound);
}

/// A recursive builder makes the parents that are missing, and takes a directory that is there as
/// done, and a builder that is turned back to not recursive stops doing both.
#[test]
#[timeout(15000)]
fn a_recursive_dir_builder_makes_every_missing_parent() {
    let dir = TempDir::new();
    let deep = dir.join("one").join("two");
    let mut builder = DirBuilder::default();
    builder.recursive(true);

    block_on(builder.create(&deep)).unwrap();
    block_on(builder.create(&deep)).unwrap();
    builder.recursive(false);
    let again = block_on(builder.create(&deep)).unwrap_err();

    assert!(deep.is_dir());
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists);
}

/// The future of a builder does not borrow it: the builder is gone at the end of the statement
/// that made the future, and the future is polled after that.
#[test]
#[timeout(15000)]
fn the_futures_of_the_builders_do_not_borrow_them() {
    let dir = TempDir::new();
    let path = dir.join("file");
    let new_dir = dir.join("new");

    let make_dir = DirBuilder::new().recursive(true).create(&new_dir);
    let open_file = OpenOptions::new().write(true).create(true).open(&path);

    assert!(block_on(make_dir).is_ok());
    assert!(block_on(open_file).is_ok());
    assert!(new_dir.is_dir());
    assert!(path.is_file());
}

/// The future of a builder does nothing before it is polled, as the future of a function does not.
#[test]
#[timeout(15000)]
fn the_futures_of_the_builders_do_nothing_before_they_are_polled() {
    let dir = TempDir::new();
    let make_dir = DirBuilder::new().create(dir.join("new"));
    let open_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(dir.join("file"));
    let write = fs::write(dir.join("written"), "text");

    thread::sleep(Duration::from_millis(50));

    assert!(!dir.join("new").exists());
    assert!(!dir.join("file").exists());
    assert!(!dir.join("written").exists());
    block_on(make_dir).unwrap();
    block_on(open_file).unwrap();
    block_on(write).unwrap();
    assert!(dir.join("new").exists());
    assert!(dir.join("file").exists());
    assert!(dir.join("written").exists());
}

/// A directory is made with the mode a unix builder is given, and so are its missing parents.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn a_dir_builder_sets_the_mode_of_new_directories() {
    let dir = TempDir::new();

    block_on(DirBuilder::new().mode(0o700).create(dir.join("one"))).unwrap();
    block_on(
        DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(dir.join("a").join("b")),
    )
    .unwrap();

    assert_eq!(mode_of(dir.join("one")), 0o700);
    assert_eq!(mode_of(dir.join("a")), 0o750);
    assert_eq!(mode_of(dir.join("a").join("b")), 0o750);
}

/// A file is not opened if it is not there, unless the options say that it is to be created.
#[test]
#[timeout(15000)]
fn open_fails_for_a_missing_file_unless_it_creates_it() {
    let dir = TempDir::new();

    let missing = block_on(OpenOptions::new().write(true).open(dir.join("file"))).unwrap_err();
    let created = block_on(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(dir.join("file")),
    );

    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    assert!(created.is_ok());
    assert!(dir.join("file").is_file());
}

/// `create_new` opens a file only if it is not there already.
#[test]
#[timeout(15000)]
fn create_new_fails_for_a_file_that_is_there() {
    let dir = TempDir::new();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);

    block_on(options.open(dir.join("file"))).unwrap();
    let error = block_on(options.open(dir.join("file"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
}

/// A file opened to append to has every write go to its end, wherever it was sought to.
#[test]
#[timeout(15000)]
fn append_adds_to_the_end_of_a_file() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "abc").unwrap();

    block_on(async {
        let mut file = OpenOptions::new()
            .append(true)
            .open(dir.join("file"))
            .await
            .unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        file.write_all(b"def").await.unwrap();
        file.flush().await.unwrap();
    });

    assert_eq!(std::fs::read_to_string(dir.join("file")).unwrap(), "abcdef");
}

/// A file opened to truncate is empty, and what is written to it is all it holds.
#[test]
#[timeout(15000)]
fn truncate_empties_a_file_that_is_there() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "a long text").unwrap();

    block_on(async {
        let opened = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(dir.join("file"))
            .await;
        let mut file = opened.unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 0);
        file.write_all(b"new").await.unwrap();
        file.flush().await.unwrap();
    });

    assert_eq!(std::fs::read_to_string(dir.join("file")).unwrap(), "new");
}

/// A file opened to write without truncating is overwritten from its start, and not shortened.
#[test]
#[timeout(15000)]
fn write_overwrites_a_file_without_shortening_it() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "abcdef").unwrap();

    block_on(async {
        let mut file = OpenOptions::new()
            .write(true)
            .open(dir.join("file"))
            .await
            .unwrap();
        file.write_all(b"XY").await.unwrap();
        file.flush().await.unwrap();
    });

    assert_eq!(std::fs::read_to_string(dir.join("file")).unwrap(), "XYcdef");
}

/// A write to a file opened for reading is handed to the pool all the same, and the error comes
/// from the flush.
#[test]
#[timeout(15000)]
fn a_write_to_a_read_only_file_fails_at_the_flush() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "text").unwrap();

    block_on(async {
        let mut file = File::open(dir.join("file")).await.unwrap();
        file.write_all(b"x").await.unwrap();
        assert!(file.flush().await.is_err());
    });

    assert_eq!(std::fs::read_to_string(dir.join("file")).unwrap(), "text");
}

/// Options that open nothing, or that need to write and do not, fail to open a file.
#[test]
#[timeout(15000)]
fn open_fails_for_options_that_make_no_sense() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    let nothing = block_on(OpenOptions::new().open(dir.join("file"))).unwrap_err();
    let truncating = block_on(
        OpenOptions::new()
            .read(true)
            .truncate(true)
            .open(dir.join("file")),
    );

    assert_eq!(nothing.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(truncating.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

/// The options of a default builder are those of a new one, and a clone of a builder opens as the
/// builder does.
#[test]
#[timeout(15000)]
fn default_options_are_new_options_and_clones_open_alike() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "text").unwrap();
    let mut options = OpenOptions::default();
    options.read(true);

    let clone = options.clone();

    assert_eq!(
        format!("{:?}", OpenOptions::default()),
        format!("{:?}", OpenOptions::new())
    );
    assert_eq!(format!("{options:?}"), format!("{clone:?}"));
    assert!(block_on(clone.open(dir.join("file"))).is_ok());
}

/// A file is created with the mode a unix builder is given, and the mode has no effect on one that
/// is there.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn open_options_set_the_mode_of_new_files() {
    let dir = TempDir::new();
    std::fs::write(dir.join("old"), "").unwrap();
    let old_mode = mode_of(dir.join("old"));
    let mut options = OpenOptions::new();
    options.write(true).create(true).mode(0o600);

    block_on(options.open(dir.join("new"))).unwrap();
    block_on(options.open(dir.join("old"))).unwrap();

    assert_eq!(mode_of(dir.join("new")), 0o600);
    assert_eq!(mode_of(dir.join("old")), old_mode);
}

/// The flags a unix builder is given reach the call that opens the file: one that refuses to
/// follow a symbolic link fails to open one.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn open_options_pass_custom_flags() {
    let Some(no_follow) = O_NOFOLLOW else {
        return;
    };
    let dir = TempDir::new();
    std::fs::write(dir.join("target"), "").unwrap();
    std::os::unix::fs::symlink(dir.join("target"), dir.join("link")).unwrap();

    let followed = block_on(OpenOptions::new().read(true).open(dir.join("link")));
    let refused = block_on(
        OpenOptions::new()
            .read(true)
            .custom_flags(no_follow)
            .open(dir.join("link")),
    );

    assert!(followed.is_ok());
    assert!(refused.is_err());
}

/// What a file is written, it is read back as, however it is flushed to be there.
#[test]
#[timeout(15000)]
fn a_file_written_is_read_back() {
    let dir = TempDir::new();

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(b"hello, world").await.unwrap();
        file.flush().await.unwrap();

        let mut text = String::new();
        File::open(dir.join("file"))
            .await
            .unwrap()
            .read_to_string(&mut text)
            .await
            .unwrap();
        assert_eq!(text, "hello, world");
    });
}

/// A file of many times what one operation reads or writes round-trips: written by one large
/// buffer and by small ones, and read through large buffers and small ones.
#[test]
#[timeout(15000)]
fn a_file_larger_than_one_operation_round_trips() {
    let dir = TempDir::new();
    let data = bytes(200_000);

    block_on(async {
        let mut file = File::create(dir.join("large")).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        let mut file = File::create(dir.join("small")).await.unwrap();
        for chunk in data.chunks(777) {
            file.write_all(chunk).await.unwrap();
        }
        file.flush().await.unwrap();

        let mut large = Vec::new();
        File::open(dir.join("large"))
            .await
            .unwrap()
            .read_to_end(&mut large)
            .await
            .unwrap();
        assert_eq!(large, data);

        let mut file = File::open(dir.join("small")).await.unwrap();
        let mut small = Vec::new();
        let mut buf = [0; 100];
        loop {
            let len = file.read(&mut buf).await.unwrap();
            if len == 0 {
                break;
            }
            small.extend_from_slice(&buf[..len]);
        }
        assert_eq!(small, data);
    });
}

/// A file with nothing in it reads as the end at once.
#[test]
#[timeout(15000)]
fn an_empty_file_reads_as_the_end() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    block_on(async {
        let mut file = File::open(dir.join("file")).await.unwrap();
        assert_eq!(file.read(&mut [0; 8]).await.unwrap(), 0);
    });
}

/// A seek lands where it is asked to, from the start, the end and the current position, and the
/// next read reads from there.
#[test]
#[timeout(15000)]
fn seeks_land_where_asked() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "0123456789").unwrap();

    block_on(async {
        let mut file = File::open(dir.join("file")).await.unwrap();
        let mut two = [0; 2];

        assert_eq!(file.seek(SeekFrom::Start(3)).await.unwrap(), 3);
        file.read_exact(&mut two).await.unwrap();
        assert_eq!(&two, b"34");

        assert_eq!(file.seek(SeekFrom::Current(2)).await.unwrap(), 7);
        file.read_exact(&mut two).await.unwrap();
        assert_eq!(&two, b"78");

        assert_eq!(file.seek(SeekFrom::End(-4)).await.unwrap(), 6);
        file.read_exact(&mut two).await.unwrap();
        assert_eq!(&two, b"67");

        assert_eq!(file.seek(SeekFrom::Current(-3)).await.unwrap(), 5);
        file.read_exact(&mut two).await.unwrap();
        assert_eq!(&two, b"56");
    });
}

/// A seek from the current position counts the bytes read, and not the ones read ahead of them.
#[test]
#[timeout(15000)]
fn the_current_position_is_where_the_reads_got_to() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "0123456789").unwrap();

    block_on(async {
        let mut file = File::open(dir.join("file")).await.unwrap();
        assert_eq!(position(&mut file).await, 0);

        file.read_exact(&mut [0; 3]).await.unwrap();

        assert_eq!(position(&mut file).await, 3);
        let mut one = [0; 1];
        file.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"3");
    });
}

/// After a write, the position is at the end of the bytes written, whether or not the write is
/// over.
#[test]
#[timeout(15000)]
fn the_position_after_a_write_is_the_end_of_the_bytes_written() {
    let dir = TempDir::new();

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(b"hello").await.unwrap();

        assert_eq!(position(&mut file).await, 5);
    });
}

/// A write that follows a read goes where the reads got to, and not to where the read ahead of
/// them left the position of the file: three bytes read, two bytes written, and the two are at
/// offsets 3 and 4.
#[test]
#[timeout(15000)]
fn a_write_after_a_read_lands_where_the_reads_got_to() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "hello world").unwrap();

    block_on(async {
        let mut file = read_write(dir.join("file")).await;
        let mut three = [0; 3];
        file.read_exact(&mut three).await.unwrap();
        assert_eq!(&three, b"hel");

        file.write_all(b"XY").await.unwrap();
        file.flush().await.unwrap();
    });

    assert_eq!(std::fs::read(dir.join("file")).unwrap(), b"helXY world");
}

/// A read after that write goes on from the end of the bytes written.
#[test]
#[timeout(15000)]
fn a_read_after_that_write_goes_on_from_the_bytes_written() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "abcdefgh").unwrap();

    block_on(async {
        let mut file = read_write(dir.join("file")).await;
        file.read_exact(&mut [0; 3]).await.unwrap();
        file.write_all(b"XY").await.unwrap();

        let mut rest = String::new();
        file.read_to_string(&mut rest).await.unwrap();

        assert_eq!(rest, "fgh");
    });
}

/// Reads and writes may alternate any number of times: each read gets the byte after the last
/// write, and each write goes after the last read.
#[test]
#[timeout(15000)]
fn reads_and_writes_alternate_without_losing_the_position() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "0123456789").unwrap();

    block_on(async {
        let mut file = read_write(dir.join("file")).await;
        let mut read = Vec::new();
        for _ in 0..5 {
            let mut one = [0; 1];
            file.read_exact(&mut one).await.unwrap();
            read.push(one[0]);
            file.write_all(b"a").await.unwrap();
        }
        file.flush().await.unwrap();

        assert_eq!(read, b"02468");
    });

    assert_eq!(std::fs::read(dir.join("file")).unwrap(), b"0a2a4a6a8a");
}

/// A write also follows a read that was given up on before it ended, whether the pool had begun to
/// read ahead or not.
#[test]
#[timeout(15000)]
fn a_write_after_a_read_given_up_on_lands_where_the_reads_got_to() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "abcdefgh").unwrap();

    let read = block_on(async {
        let mut file = read_write(dir.join("file")).await;
        let mut buf = [0; 3];
        // Polled once: the read is pending, or the pool was quick and it is over.
        let read = match poll_once(file.read(&mut buf)).await {
            Some(read) => read.unwrap(),
            None => 0,
        };
        file.write_all(b"XY").await.unwrap();
        file.flush().await.unwrap();
        read
    });

    let mut expected = b"abcdefgh".to_vec();
    expected[read..read + 2].copy_from_slice(b"XY");
    assert_eq!(std::fs::read(dir.join("file")).unwrap(), expected);
}

/// A file that cannot be sought, as a socket cannot, is written after a read all the same, and
/// what was read ahead of the write is not lost to it.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn a_write_after_a_read_works_on_a_file_that_cannot_seek() {
    let (mut peer, ours) = UnixStream::pair().unwrap();
    let mut file = File::from(OwnedFd::from(ours));

    block_on(async {
        for round in 0..3u8 {
            let first = [b'a' + round; 4];
            let second = [b'm' + round; 4];
            // Sent together, so that the read of the first half reads the second half ahead.
            io::Write::write_all(&mut peer, &[first, second].concat()).unwrap();
            let mut buf = [0; 4];
            file.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, first);

            file.write_all(&[b'A' + round; 4]).await.unwrap();
            file.flush().await.unwrap();
            let mut reply = [0; 4];
            io::Read::read_exact(&mut peer, &mut reply).unwrap();
            assert_eq!(reply, [b'A' + round; 4]);

            file.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, second);
        }
    });
}

/// `sync_all` waits for the writes before it, so the file holds all of them once it is over.
#[test]
#[timeout(15000)]
fn sync_all_sees_every_byte_written() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.sync_all().await.unwrap();

        assert_eq!(
            std::fs::metadata(dir.join("file")).unwrap().len(),
            data.len() as u64
        );
    });
}

/// `sync_data` waits for the writes before it, as `sync_all` does.
#[test]
#[timeout(15000)]
fn sync_data_sees_every_byte_written() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.sync_data().await.unwrap();

        assert_eq!(
            std::fs::metadata(dir.join("file")).unwrap().len(),
            data.len() as u64
        );
    });
}

/// `metadata` waits for the writes before it, so the length it reports includes every one.
#[test]
#[timeout(15000)]
fn metadata_sees_every_byte_written() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), data.len() as u64);
    });
}

/// `set_len` waits for the writes before it: none of them is still to come to lengthen a file that
/// it cut short.
#[test]
#[timeout(15000)]
fn set_len_comes_after_every_byte_written() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.set_len(10).await.unwrap();
        file.flush().await.unwrap();

        assert_eq!(std::fs::read(dir.join("file")).unwrap(), &data[..10]);
    });
}

/// `set_len` extends a file with zeros, and leaves the position where it was: the next write goes
/// after the ones before it, and not to the end of the file.
#[test]
#[timeout(15000)]
fn set_len_extends_a_file_and_keeps_the_position() {
    let dir = TempDir::new();

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(b"ab").await.unwrap();
        file.set_len(6).await.unwrap();
        file.write_all(b"cd").await.unwrap();
        file.flush().await.unwrap();
    });

    assert_eq!(std::fs::read(dir.join("file")).unwrap(), b"abcd\0\0");
}

/// What `set_len`, `sync_all` and `metadata` do between reads leaves the reads where they were.
#[test]
#[timeout(15000)]
fn looking_at_a_file_does_not_disturb_the_reads() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "0123456789").unwrap();

    block_on(async {
        let mut file = read_write(dir.join("file")).await;
        file.read_exact(&mut [0; 3]).await.unwrap();

        file.sync_all().await.unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 10);
        file.sync_data().await.unwrap();

        let mut rest = String::new();
        file.read_to_string(&mut rest).await.unwrap();
        assert_eq!(rest, "3456789");
    });
}

/// `sync_all` does not end while a write before it is still going on.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn sync_all_waits_for_a_write_in_flight() {
    let (file, mut peer) = file_with_a_blocked_write();
    let mut sync = pin!(file.sync_all());

    assert_pending_while_the_write_is_blocked(sync.as_mut());
    release_the_write(&mut peer);

    // The socket cannot be synced to a disk, which is of no interest: the order is.
    let _ = block_on(sync);
}

/// `sync_data` does not end while a write before it is still going on.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn sync_data_waits_for_a_write_in_flight() {
    let (file, mut peer) = file_with_a_blocked_write();
    let mut sync = pin!(file.sync_data());

    assert_pending_while_the_write_is_blocked(sync.as_mut());
    release_the_write(&mut peer);

    let _ = block_on(sync);
}

/// `set_len` does not start while a write before it is still going on.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn set_len_waits_for_a_write_in_flight() {
    let (file, mut peer) = file_with_a_blocked_write();
    let mut set_len = pin!(file.set_len(0));

    assert_pending_while_the_write_is_blocked(set_len.as_mut());
    release_the_write(&mut peer);

    // The length of a socket cannot be set, which is of no interest: the order is.
    let _ = block_on(set_len);
}

/// `metadata` does not start while a write before it is still going on.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn metadata_waits_for_a_write_in_flight() {
    let (file, mut peer) = file_with_a_blocked_write();
    let mut metadata = pin!(file.metadata());

    assert_pending_while_the_write_is_blocked(metadata.as_mut());
    release_the_write(&mut peer);

    assert!(block_on(metadata).is_ok());
}

/// A flush does not end while a write before it is still going on, and ends once it is over.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn flush_waits_for_a_write_in_flight() {
    let (mut file, mut peer) = file_with_a_blocked_write();
    let mut flush = pin!(file.flush());

    assert_pending_while_the_write_is_blocked(flush.as_mut());
    release_the_write(&mut peer);

    block_on(flush).unwrap();
}

/// A file dropped with a write in flight stays open until the write is over: the bytes reach
/// the other end of a socket that was full when the file was dropped.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
#[timeout(15000)]
fn a_file_dropped_with_a_write_in_flight_finishes_the_write() {
    let (file, mut peer) = file_with_a_blocked_write();

    drop(file);

    release_the_write(&mut peer);
}

/// A file dropped right after a write, with no flush, still gets every byte once the pool is done.
#[test]
#[timeout(15000)]
fn a_file_dropped_after_a_write_still_gets_every_byte() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();
        drop(file);
    });

    // The writes go in order, so the file is as long as the bytes once the last one is written.
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::metadata(dir.join("file")).unwrap().len() < data.len() as u64 {
        assert!(
            Instant::now() < deadline,
            "the bytes written never all reached the file"
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(std::fs::read(dir.join("file")).unwrap(), data);
}

/// Closing a file flushes it, so the file has the bytes written by the time the close is over.
#[test]
#[timeout(15000)]
fn closing_a_file_flushes_it() {
    let dir = TempDir::new();
    let data = bytes(1 << 20);

    block_on(async {
        let mut file = File::create(dir.join("file")).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.close().await.unwrap();
    });

    assert_eq!(std::fs::read(dir.join("file")).unwrap(), data);
}

/// A file changes its permissions, which do not wait for the writes before them.
#[test]
#[timeout(15000)]
fn a_file_changes_its_own_permissions() {
    let dir = TempDir::new();

    block_on(async {
        let file = File::create(dir.join("file")).await.unwrap();
        let writable = file.metadata().await.unwrap().permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);

        file.set_permissions(read_only).await.unwrap();
        let after = file.metadata().await.unwrap().permissions().readonly();
        // Windows does not remove a file that is read-only, so the directory could not go
        // otherwise.
        file.set_permissions(writable).await.unwrap();

        assert!(after);
    });
}

/// A file made from one of std's reads from where that was, and so does one made from its
/// descriptor or its handle.
#[test]
#[timeout(15000)]
fn a_file_made_from_one_of_stds_reads_from_where_that_was() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "0123456789").unwrap();
    let mut from_std = std::fs::File::open(dir.join("file")).unwrap();
    io::Seek::seek(&mut from_std, SeekFrom::Start(4)).unwrap();
    let from_owned = std::fs::File::open(dir.join("file")).unwrap();

    block_on(async {
        let mut text = String::new();
        File::from(from_std)
            .read_to_string(&mut text)
            .await
            .unwrap();
        assert_eq!(text, "456789");

        #[cfg(unix)]
        let mut owned = File::from(OwnedFd::from(from_owned));
        #[cfg(windows)]
        let mut owned = File::from(std::os::windows::io::OwnedHandle::from(from_owned));
        let mut text = String::new();
        owned.read_to_string(&mut text).await.unwrap();
        assert_eq!(text, "0123456789");
    });
}

/// A file gives out the descriptor, or the handle, of the file of std behind it, which is the same
/// whichever way it is asked for.
#[test]
#[timeout(15000)]
fn a_file_gives_out_its_raw_handle() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    let file = block_on(File::open(dir.join("file"))).unwrap();

    #[cfg(unix)]
    {
        assert_eq!(file.as_raw_fd(), file.as_fd().as_raw_fd());
        assert!(file.as_raw_fd() >= 0);
    }
    #[cfg(windows)]
    {
        assert_eq!(file.as_raw_handle(), file.as_handle().as_raw_handle());
        assert!(!file.as_raw_handle().is_null());
    }
}

/// A file prints as the file of std behind it does, and a file that is not there is not opened.
#[test]
#[timeout(15000)]
fn a_file_prints_as_a_file_of_std_does() {
    let dir = TempDir::new();
    std::fs::write(dir.join("file"), "").unwrap();

    let file = block_on(File::open(dir.join("file"))).unwrap();
    let missing = block_on(File::open(dir.join("missing")));

    assert!(format!("{file:?}").starts_with("File {"));
    assert_eq!(missing.unwrap_err().kind(), io::ErrorKind::NotFound);
}

/// A file is not created in a directory that is not there.
#[test]
#[timeout(15000)]
fn create_fails_without_the_directory_of_the_file() {
    let dir = TempDir::new();

    let error = block_on(File::create(dir.join("missing").join("file"))).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// A scratch directory under the temporary directory of the OS, which goes with everything in it
/// when the value is dropped.
struct TempDir(PathBuf);

impl TempDir {
    /// A new directory, named by the process and by how many there have been before it.
    fn new() -> Self {
        static COUNT: AtomicUsize = AtomicUsize::new(0);

        let count = COUNT.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("zruntime-fs-test-{}-{count}", process::id()));
        // A directory left by a run that crashed, with the same process id, is cleared away.
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    /// The directory.
    fn path(&self) -> &Path {
        &self.0
    }

    /// The path of `name` in the directory.
    fn join<P>(&self, name: P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A file opened to be read and written, without being created or emptied.
async fn read_write<P>(path: P) -> File
where
    P: AsRef<Path>,
{
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .await
        .unwrap()
}

/// Where the reads and the writes of `file` have got to, as a seek from the current position tells.
async fn position(file: &mut File) -> u64 {
    file.seek(SeekFrom::Current(0)).await.unwrap()
}

/// A file on a socket with a write in flight that cannot end, and the other end of the socket.
///
/// The buffer of the socket is made too small for the bytes of the write, and nobody reads from the
/// other end, so the write blocks on the pool until the test reads.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn file_with_a_blocked_write() -> (File, UnixStream) {
    let (ours, peer) = UnixStream::pair().unwrap();
    socket2::SockRef::from(&ours)
        .set_send_buffer_size(1)
        .unwrap();
    let mut file = File::from(OwnedFd::from(ours));

    // Done as soon as the pool has the bytes, which it then cannot write all of.
    block_on(file.write_all(&[0; 8192])).unwrap();

    (file, peer)
}

/// Reads from the other end of the socket what [`file_with_a_blocked_write`] wrote, which lets the
/// write end.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn release_the_write(peer: &mut UnixStream) {
    io::Read::read_exact(peer, &mut [0; 8192]).unwrap();
}

/// Polls `future` twice, with a pause between, and fails the test if either poll finishes it.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn assert_pending_while_the_write_is_blocked<F>(mut future: Pin<&mut F>)
where
    F: Future,
{
    let mut cx = Context::from_waker(Waker::noop());

    assert!(future.as_mut().poll(&mut cx).is_pending());
    // Long enough for the work of a future that did not wait to be done by the pool.
    thread::sleep(Duration::from_millis(100));
    assert!(future.as_mut().poll(&mut cx).is_pending());
}

/// `len` bytes that are not all alike, so that bytes in the wrong place are noticed.
fn bytes(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index * 31 % 251) as u8).collect()
}

/// The permission bits of the file or directory at `path`, those of the access modes alone.
#[cfg(unix)]
fn mode_of<P>(path: P) -> u32
where
    P: AsRef<Path>,
{
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// The flag that has an open fail for a symbolic link, whose value differs among platforms, if it
/// is one that this knows.
#[cfg(unix)]
const O_NOFOLLOW: Option<i32> = if cfg!(all(
    any(target_os = "linux", target_os = "android"),
    any(target_arch = "x86", target_arch = "x86_64")
)) {
    Some(0o400000)
} else if cfg!(all(
    any(target_os = "linux", target_os = "android"),
    any(target_arch = "arm", target_arch = "aarch64")
)) {
    Some(0o100000)
} else if cfg!(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
)) {
    Some(0x100)
} else {
    None
};
