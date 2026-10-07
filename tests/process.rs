//! A test of the child processes that fills the pool of threads that all blocking work shares, and
//! so is a binary of its own.
//!
//! The pool is the one `zruntime::unblock` hands work to, which `Unblock`, `fs` and the pipes of a
//! child on Windows use as well. The test holds every thread of it for as long as it needs, and any
//! other test in the same process that had blocking work to run would wait for it to let them go.
//! The unit tests of the crate are one binary, whose tests run side by side, so none of them can do
//! that. A file under `tests/` is a binary of its own, and Cargo runs the binaries one after
//! another, never side by side. This one holds the single test: a second would run beside it, and
//! wait behind the pool that it fills.

#![cfg(all(feature = "process", unix))]

use std::{
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant},
};

use rustix::{
    io::Errno,
    process::{Pid, test_kill_process},
};
use zruntime::{LocalRuntime, process::Command};

/// A child that is dropped while its process runs is reaped, although every thread of the pool of
/// blocking work is held.
///
/// The reaps have a pool of their own, so the one that a dropped child needs does not wait for a
/// thread of `unblock()`'s, which the test holds until it has seen the child collected.
#[test]
#[ntest::timeout(30000)]
fn a_dropped_child_is_reaped_while_blocking_work_fills_the_pool() {
    // The test's own thread counts in both barriers: in the first, so as to know that every thread
    // of the pool holds a job, and in the second, to let the jobs go once it is done.
    let started = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    for _ in 0..THREADS {
        let started = started.clone();
        let release = release.clone();
        drop(zruntime::unblock(move || {
            started.wait();
            release.wait();
        }));
    }
    started.wait();

    // The pool is full, as the test needs it to be: one job more has no thread to run on.
    let (report, reported) = mpsc::channel();
    drop(zruntime::unblock(move || {
        let _ = report.send(());
    }));
    assert!(
        reported.recv_timeout(Duration::from_millis(200)).is_err(),
        "the pool has a thread to spare",
    );

    let runtime = LocalRuntime::new().unwrap();
    let child = Command::new("sh")
        .args(["-c", "sleep 0.3"])
        .spawn(&runtime)
        .unwrap();
    let id = child.id();

    drop(child);
    let reaped = disappears(id);

    // The pool is let go of before the test judges, so that a failure does not leave it held.
    release.wait();
    assert!(reaped, "the child was never collected");
}

/// Waits, for a few seconds at most, for the process with the ID `id` to be gone from the process
/// table, and tells whether it went.
///
/// A process that has exited stays there, as a zombie, until its status is collected, so this is
/// also how the test sees that somebody collected it.
fn disappears(id: u32) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);

    while exists(id) {
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }

    true
}

/// Whether a process with the ID `id` is there, a zombie included.
fn exists(id: u32) -> bool {
    let pid = Pid::from_raw(id as i32).expect("a child's process ID is not zero");

    test_kill_process(pid) != Err(Errno::SRCH)
}

/// The most threads that the pool of `zruntime::unblock` has at once, as the documentation of that
/// function gives it.
const THREADS: usize = 500;
