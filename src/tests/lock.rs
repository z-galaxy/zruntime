//! Tests of the locks of [`crate::lock`]: [`Mutex`] and [`RwLock`].
//!
//! Most of these drive the futures of a lock by hand, polling each with a waker that goes nowhere,
//! so that every test says exactly who a release lets in and who still waits. The mutex comes
//! first, with who it admits and who it keeps out, what giving up a wait leaves behind, and what
//! `try_lock` does. The readers-writer lock follows, with who it admits and who it keeps out, what
//! a waiting writer does to the readers behind it, what giving up a wait leaves behind, and what
//! the calls that never wait do and do not look at. Then come the tests that cover both locks:
//! that a panic while a guard is held releases it, the smaller conveniences (borrowing the value,
//! taking it back, making a lock, printing it) and that a lock may be a trait object.
//!
//! The last six drive one lock from many threads at once. The first two check that it never lets a
//! holder in beside one it should keep out and that nothing deadlocks on the way, the next three
//! that a release racing the start of a wait is not missed, and the last that a mutex whose
//! waiters' wakers come back into it from their wake still admits one holder at a time and
//! deadlocks nothing. They are shrunk under Miri, which runs them far more slowly.

use std::{
    fmt,
    future::Future,
    hint,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    sync::{Arc, Barrier},
    task::{Context, Poll, Wake, Waker},
    thread,
};

use futures_lite::future::block_on;
use ntest::timeout;

use crate::lock::{Mutex, RwLock};

/// A second taker waits while the first holds the mutex, and is let in once the first lets go.
#[test]
fn a_mutex_admits_one_holder_at_a_time() {
    let mutex = Mutex::new(0);
    let guard = ready(mutex.lock());
    let mut second = Box::pin(mutex.lock());
    assert!(poll_once(&mut second).is_pending());
    drop(guard);
    assert!(poll_once(&mut second).is_ready());
}

/// A newcomer takes a mutex that is free ahead of a taker that has been waiting for it: the
/// release woke that taker, which had not run yet when the newcomer came. The waiter finds the
/// mutex taken again and goes back to waiting, and is let in only by the newcomer's release.
#[test]
fn a_newcomer_takes_a_free_mutex_ahead_of_a_waiter() {
    let mutex = Mutex::new(());
    let guard = ready(mutex.lock());
    let mut waiter = Box::pin(mutex.lock());
    assert!(poll_once(&mut waiter).is_pending());
    drop(guard);

    let mut newcomer = Box::pin(mutex.lock());
    let Poll::Ready(newcomer) = poll_once(&mut newcomer) else {
        panic!("a free mutex is taken at once, whoever waits for it");
    };

    assert!(poll_once(&mut waiter).is_pending());
    drop(newcomer);
    assert!(poll_once(&mut waiter).is_ready());
}

/// A `lock` future dropped after the release woke it, before it took the mutex, passes the wake-up
/// on, so the taker behind it is let in and not left waiting for a release that never comes.
#[test]
fn a_dropped_lock_future_does_not_strand_the_next_waiter() {
    let mutex = Mutex::new(());
    let guard = ready(mutex.lock());
    let mut first = Box::pin(mutex.lock());
    assert!(poll_once(&mut first).is_pending());
    let mut second = Box::pin(mutex.lock());
    assert!(poll_once(&mut second).is_pending());
    // The release notifies `first`, which is then dropped without ever polling that
    // notification: it has to reach `second` instead.
    drop(guard);
    drop(first);
    assert!(poll_once(&mut second).is_ready());
}

/// `try_lock` takes a mutex that is free and fails, without waiting, on one that is held, whether
/// a `try_lock` or a `lock` holds it; it takes the mutex again once that guard is gone.
#[test]
fn try_lock_takes_a_free_mutex_and_fails_on_a_held_one() {
    let mutex = Mutex::new(1);

    let mut guard = mutex.try_lock().expect("a new mutex is free");
    *guard = 2;
    assert!(mutex.try_lock().is_none());
    drop(guard);

    let guard = ready(mutex.lock());
    assert!(mutex.try_lock().is_none());
    drop(guard);

    assert_eq!(*mutex.try_lock().expect("a released mutex is free"), 2);
}

/// Readers share the lock and keep a writer waiting until the last of them is gone; a writer in
/// turn keeps readers out until it lets go, and the readers then see what it wrote.
#[test]
fn readers_share_and_a_writer_excludes() {
    let lock = RwLock::new(1);
    let first = ready(lock.read());
    let second = ready(lock.read());
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(&mut writer).is_pending());
    drop(first);
    drop(second);
    let Poll::Ready(mut guard) = poll_once(&mut writer) else {
        panic!("the last reader lets the writer in");
    };
    *guard = 2;
    let mut reader = Box::pin(lock.read());
    assert!(poll_once(&mut reader).is_pending());
    drop(guard);
    let Poll::Ready(guard) = poll_once(&mut reader) else {
        panic!("the writer's release lets readers in");
    };
    assert_eq!(*guard, 2);
}

/// A writer's release lets in every reader that waits for it, not just the first: readers share the
/// lock, so once the writer is gone nothing keeps any of them out.
#[test]
fn a_writer_release_lets_every_waiting_reader_in() {
    let lock = RwLock::new(());
    let writer = lock.try_write().expect("a new lock is free to write");
    let mut first = Box::pin(lock.read());
    let mut second = Box::pin(lock.read());
    assert!(poll_once(&mut first).is_pending());
    assert!(poll_once(&mut second).is_pending());

    drop(writer);

    assert!(poll_once(&mut first).is_ready());
    assert!(poll_once(&mut second).is_ready());
}

/// A writer waiting for readers to leave holds new readers back, so that readers coming one after
/// the other cannot keep it out for good. It is also why a task holding a read guard must not ask
/// for another: the second `read` waits behind the writer, which waits for the first guard.
#[test]
fn a_waiting_writer_holds_new_readers_back() {
    let lock = RwLock::new(());
    let reader = ready(lock.read());
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(&mut writer).is_pending());
    let mut late_reader = Box::pin(lock.read());
    assert!(poll_once(&mut late_reader).is_pending());
    drop(reader);
    assert!(poll_once(&mut writer).is_ready());
}

/// A `write` future dropped after the release woke it, before it took the lock, passes the wake-up
/// on, so the writer behind it is let in and not left waiting for a release that never comes.
#[test]
fn a_dropped_write_future_does_not_strand_the_next_writer() {
    let lock = RwLock::new(());
    let guard = ready(lock.write());
    let mut first = Box::pin(lock.write());
    assert!(poll_once(&mut first).is_pending());
    let mut second = Box::pin(lock.write());
    assert!(poll_once(&mut second).is_pending());
    // The release notifies `first`, which is then dropped without ever polling that
    // notification: it has to reach `second` instead.
    drop(guard);
    drop(first);
    assert!(poll_once(&mut second).is_ready());
}

/// A writer given up stops holding readers back: one that asks afterwards is let in.
#[test]
fn a_cancelled_writer_lets_readers_in_again() {
    let lock = RwLock::new(());
    let reader = ready(lock.read());
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(&mut writer).is_pending());
    drop(writer);
    assert!(poll_once(&mut Box::pin(lock.read())).is_ready());
    drop(reader);
}

/// A writer given up while readers already wait behind it wakes every one of them, not just the
/// first: they wait only for that writer, and the readers holding the lock would wake a writer, not
/// them, when they go.
#[test]
fn a_cancelled_writer_wakes_every_reader_it_held_back() {
    let lock = RwLock::new(());
    let reader = ready(lock.read());
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(&mut writer).is_pending());
    let mut first = Box::pin(lock.read());
    let mut second = Box::pin(lock.read());
    assert!(poll_once(&mut first).is_pending());
    assert!(poll_once(&mut second).is_pending());

    drop(writer);

    assert!(poll_once(&mut first).is_ready());
    assert!(poll_once(&mut second).is_ready());
    drop(reader);
}

/// `try_read` and `try_write` each take a lock that is free. Readers let other readers in and keep
/// every writer out, and a writer keeps everyone out; none of them waits, and the lock is free for
/// both again once the guards are gone.
#[test]
fn try_read_and_try_write_take_a_free_lock_and_fail_on_a_held_one() {
    let lock = RwLock::new(1);

    drop(lock.try_read().expect("a new lock is free to read"));
    drop(lock.try_write().expect("a new lock is free to write"));

    let first = lock.try_read().expect("a free lock is read");
    let second = lock.try_read().expect("readers share the lock");
    assert!(lock.try_write().is_none());
    drop(first);
    assert!(lock.try_write().is_none());
    drop(second);

    let mut writer = lock.try_write().expect("the readers are gone");
    *writer = 2;
    assert!(lock.try_read().is_none());
    assert!(lock.try_write().is_none());
    drop(writer);

    assert_eq!(*lock.try_read().expect("the writer is gone"), 2);
}

/// `try_read` fails while a writer waits for the lock, whether readers hold it or nobody does yet:
/// a waiting writer holds readers back from `try_read` as it does from `read`, until it has had its
/// turn.
#[test]
fn try_read_fails_while_a_writer_waits() {
    let lock = RwLock::new(());
    let reader = lock.try_read().expect("a new lock is free to read");
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(&mut writer).is_pending());

    assert!(lock.try_read().is_none());
    drop(reader);
    // Free now, but the writer it woke has not taken the lock yet.
    assert!(lock.try_read().is_none());
    let Poll::Ready(guard) = poll_once(&mut writer) else {
        panic!("the last reader lets the writer in");
    };
    drop(guard);

    assert!(lock.try_read().is_some());
}

/// `try_write` looks at who holds the lock and not at who waits for it: where the lock is free and
/// a writer waits, it takes the lock ahead of that writer. The writer finds the lock taken and goes
/// back to waiting, and gets it once the guard `try_write` gave is dropped.
#[test]
fn try_write_takes_a_free_lock_while_a_writer_waits() {
    let lock = RwLock::new(1);
    let holder = ready(lock.write());
    let mut waiting = Box::pin(lock.write());
    assert!(poll_once(&mut waiting).is_pending());
    // The release wakes the waiting writer, which has not run yet when `try_write` comes in.
    drop(holder);

    let mut barging = lock
        .try_write()
        .expect("a free lock is taken, whoever waits for it");
    *barging = 2;

    assert!(poll_once(&mut waiting).is_pending());
    drop(barging);
    let Poll::Ready(guard) = poll_once(&mut waiting) else {
        panic!("the release of the barging guard lets the waiting writer in");
    };
    assert_eq!(*guard, 2);
}

/// A panic while a guard of a mutex is held drops the guard on the way out, which releases the
/// mutex: nothing poisons it, and the next holder finds the value as the panicking code left it.
#[test]
fn a_panic_while_a_mutex_guard_is_held_releases_the_mutex() {
    let mutex = Mutex::new(1);

    // The guard is taken outside the closure, which then owns it: a `ready` that panicked inside
    // would be caught along with the panic this test is after.
    let mut guard = ready(mutex.lock());
    let panicked = catch_unwind(AssertUnwindSafe(move || {
        *guard = 2;
        panic!("a panic while the guard is held");
    }));

    assert!(panicked.is_err());
    assert_eq!(*mutex.try_lock().expect("the panic released the mutex"), 2);
}

/// A panic while a write guard or a read guard is held drops the guard on the way out, which
/// releases the lock: nothing poisons it, the next holder finds the value as the panicking code
/// left it, and the reader that panicked is not left counted among the readers keeping writers out.
#[test]
fn a_panic_while_an_rwlock_guard_is_held_releases_the_lock() {
    let lock = RwLock::new(1);

    // Each guard is taken outside its closure, which then owns it, as in the mutex's test above.
    let mut guard = ready(lock.write());
    let panicked = catch_unwind(AssertUnwindSafe(move || {
        *guard = 2;
        panic!("a panic while the write guard is held");
    }));
    assert!(panicked.is_err());
    assert_eq!(*lock.try_read().expect("the panic released the lock"), 2);

    let guard = ready(lock.read());
    let panicked = catch_unwind(AssertUnwindSafe(move || {
        let _guard = guard;
        panic!("a panic while a read guard is held");
    }));
    assert!(panicked.is_err());
    *lock.try_write().expect("the panic released the lock") = 3;
    assert_eq!(*lock.try_read().expect("the writer is gone"), 3);
}

/// A lock borrowed mutably has no guard to wait for, so `get_mut` reaches the value without a wait,
/// and what it changes is what the next guard sees.
#[test]
fn get_mut_reaches_the_value_without_a_guard() {
    let mut mutex = Mutex::new(vec![1]);
    mutex.get_mut().push(2);
    assert_eq!(*ready(mutex.lock()), [1, 2]);

    let mut lock = RwLock::new(vec![1]);
    lock.get_mut().push(2);
    assert_eq!(*ready(lock.read()), [1, 2]);
}

/// `into_inner` hands back the value as the guards left it, with no wait: the lock is moved, so
/// none is left to hold it.
#[test]
fn into_inner_hands_back_the_value() {
    let mutex = Mutex::new(vec![1]);
    ready(mutex.lock()).push(2);
    assert_eq!(mutex.into_inner(), [1, 2]);

    let lock = RwLock::new(vec![1]);
    ready(lock.write()).push(2);
    assert_eq!(lock.into_inner(), [1, 2]);
}

/// A lock made by `default` holds the default of its value type, and one made by `from` holds the
/// value it is given; a lock made either way is free.
#[test]
fn a_lock_is_made_from_a_default_or_from_a_value() {
    let mutex = Mutex::<Vec<u8>>::default();
    assert!(mutex.try_lock().expect("a new mutex is free").is_empty());
    let mutex = Mutex::from(7);
    assert_eq!(*mutex.try_lock().expect("a new mutex is free"), 7);

    let lock = RwLock::<Vec<u8>>::default();
    assert!(lock.try_write().expect("a new lock is free").is_empty());
    let lock = RwLock::from(7);
    assert_eq!(*lock.try_write().expect("a new lock is free"), 7);
}

/// A lock prints its value while that can be read at once, and `<locked>` while it cannot: for a
/// mutex, while it is held; for an `RwLock`, while a writer holds it and also while a writer only
/// waits for it, which holds the printing back as it holds any reader back. Readers holding the
/// `RwLock` do not keep it from printing its value.
#[test]
fn a_lock_prints_its_value_while_free_and_a_placeholder_while_held() {
    let mutex = Mutex::new(1);
    assert_eq!(format!("{mutex:?}"), "Mutex { value: 1 }");
    let guard = ready(mutex.lock());
    assert_eq!(format!("{mutex:?}"), "Mutex { value: <locked> }");
    drop(guard);
    assert_eq!(format!("{mutex:?}"), "Mutex { value: 1 }");

    let lock = RwLock::new(1);
    assert_eq!(format!("{lock:?}"), "RwLock { value: 1 }");
    let writer = ready(lock.write());
    assert_eq!(format!("{lock:?}"), "RwLock { value: <locked> }");
    drop(writer);
    let reader = ready(lock.read());
    assert_eq!(format!("{lock:?}"), "RwLock { value: 1 }");
    let mut waiting = Box::pin(lock.write());
    assert!(poll_once(&mut waiting).is_pending());
    assert_eq!(format!("{lock:?}"), "RwLock { value: <locked> }");
    drop(waiting);
    assert_eq!(format!("{lock:?}"), "RwLock { value: 1 }");
    drop(reader);
}

/// Each guard prints as the value it stands for, with the options of the format it is printed
/// with passed on to the value: a width, or the pretty form.
#[test]
fn a_guard_prints_as_its_value() {
    let mutex = Mutex::new(1);
    let guard = ready(mutex.lock());
    assert_eq!(format!("{guard:?}"), "1");
    assert_eq!(format!("{guard:>3?}"), "  1");
    drop(guard);

    let lock = RwLock::new(vec![1, 2]);
    let reader = ready(lock.read());
    assert_eq!(format!("{reader:?}"), "[1, 2]");
    assert_eq!(format!("{reader:#?}"), "[\n    1,\n    2,\n]");
    drop(reader);
    let writer = ready(lock.write());
    assert_eq!(format!("{writer:?}"), "[1, 2]");
    assert_eq!(format!("{writer:#?}"), "[\n    1,\n    2,\n]");
}

/// A mutex behind an `Arc` coerces to one of a trait object, which is what the value being the
/// last field of the lock lets it do.
#[test]
fn a_mutex_coerces_to_an_unsized_value() {
    let mutex: Arc<Mutex<dyn fmt::Debug + Send>> = Arc::new(Mutex::new(5u8));
    let guard = ready(mutex.lock());
    assert_eq!(format!("{:?}", &*guard), "5");
}

/// A readers-writer lock behind an `Arc` coerces to one of a trait object, as a mutex does.
#[test]
fn a_rwlock_coerces_to_an_unsized_value() {
    let lock: Arc<RwLock<dyn fmt::Debug + Send + Sync>> = Arc::new(RwLock::new(5u8));
    let guard = ready(lock.read());
    assert_eq!(format!("{:?}", &*guard), "5");
}

/// Threads that take a mutex over and over, each adding one to a count that it reads and writes
/// back as two steps: a second holder let in at the same time would lose a step of the count, and a
/// release that no waiting thread heard of would leave a thread waiting for good, which the timeout
/// would catch.
#[test]
#[timeout(15000)]
fn a_mutex_admits_one_holder_at_a_time_across_threads() {
    let (threads, rounds) = if cfg!(miri) { (3, 20) } else { (8, 1000) };
    let mutex = Arc::new(Mutex::new(0usize));
    let start = Arc::new(Barrier::new(threads));

    let takers: Vec<_> = (0..threads)
        .map(|_| {
            let mutex = mutex.clone();
            let start = start.clone();
            thread::spawn(move || {
                // Lets the threads in together, so that they contend from their first round rather
                // than each running its rounds before the next is spawned.
                start.wait();
                for _ in 0..rounds {
                    let mut count = block_on(mutex.lock());
                    let seen = *count;
                    // Gives a second holder, were one let in, a moment to come in between.
                    linger();
                    *count = seen + 1;
                }
            })
        })
        .collect();
    for taker in takers {
        taker.join().unwrap();
    }

    assert_eq!(*block_on(mutex.lock()), threads * rounds);
}

/// Writers that update a pair in two steps, and readers that check the two halves agree, all on
/// threads of their own: a reader let in while a writer is halfway through would see them differ,
/// and two writers let in together would lose a step. Readers held back by a waiting writer, and
/// writers woken by the last reader to leave, must each get through, which the timeout would catch.
#[test]
#[timeout(15000)]
fn readers_never_see_a_pair_half_updated_by_a_writer() {
    let (writers, readers, rounds) = if cfg!(miri) { (2, 2, 10) } else { (4, 4, 500) };
    let lock = Arc::new(RwLock::new((0usize, 0usize)));
    let start = Arc::new(Barrier::new(writers + readers));

    let writer_threads: Vec<_> = (0..writers)
        .map(|_| {
            let lock = lock.clone();
            let start = start.clone();
            thread::spawn(move || {
                // Lets the threads in together, so that they contend from their first round rather
                // than each running its rounds before the next is spawned.
                start.wait();
                for _ in 0..rounds {
                    let mut guard = block_on(lock.write());
                    guard.0 += 1;
                    // Gives a reader, were one let in, a moment to look in between.
                    linger();
                    guard.1 += 1;
                }
            })
        })
        .collect();
    let reader_threads: Vec<_> = (0..readers)
        .map(|_| {
            let lock = lock.clone();
            let start = start.clone();
            thread::spawn(move || {
                // Lets the threads in together, as the writers do.
                start.wait();
                for _ in 0..rounds {
                    let guard = block_on(lock.read());
                    assert_eq!(guard.0, guard.1);
                }
            })
        })
        .collect();
    for thread in writer_threads.into_iter().chain(reader_threads) {
        thread.join().unwrap();
    }

    assert_eq!(*block_on(lock.read()), (writers * rounds, writers * rounds));
}

/// A `lock` that starts to wait just as the holder lets go is let in, wherever the release falls
/// among its steps: before its first try, between that try and its listener, or after both. Only
/// that one release can let the waiter in, so a release it misses leaves it waiting for good, which
/// the timeout catches.
#[test]
#[timeout(15000)]
fn a_release_racing_a_lock_is_not_missed() {
    let rounds = if cfg!(miri) { 20 } else { 20_000 };
    handoff(
        Arc::new(Mutex::new(())),
        rounds,
        |mutex, start_waiting| {
            let _guard = mutex
                .try_lock()
                .expect("nothing holds the mutex between rounds");
            start_waiting();
        },
        |mutex| drop(block_on(mutex.lock())),
    );
}

/// A `read` that starts to wait just as the writer lets go is let in, wherever the release falls
/// among its steps: before its first try, between that try and its listener, or after both. Only
/// that one release can let the reader in, so a release it misses leaves it waiting for good, which
/// the timeout catches.
#[test]
#[timeout(15000)]
fn a_write_release_racing_a_read_is_not_missed() {
    let rounds = if cfg!(miri) { 20 } else { 20_000 };
    handoff(
        Arc::new(RwLock::new(())),
        rounds,
        |lock, start_waiting| {
            let _guard = lock
                .try_write()
                .expect("nothing holds the lock between rounds");
            start_waiting();
        },
        |lock| drop(block_on(lock.read())),
    );
}

/// A `write` that starts to wait just as the reader lets go is let in, wherever the release falls
/// among its steps: before its first try, between that try and its listener, or after both. Only
/// that one release can let the writer in, so a release it misses leaves it waiting for good, which
/// the timeout catches.
#[test]
#[timeout(15000)]
fn a_read_release_racing_a_write_is_not_missed() {
    let rounds = if cfg!(miri) { 20 } else { 20_000 };
    handoff(
        Arc::new(RwLock::new(())),
        rounds,
        |lock, start_waiting| {
            let _guard = lock
                .try_read()
                .expect("nothing holds the lock between rounds");
            start_waiting();
        },
        |lock| drop(block_on(lock.write())),
    );
}

/// Wakers that come back into the lock from their wake, each taking it and letting go at once or
/// listening for it and giving up, while threads take and release it: nothing deadlocks, and the
/// lock still admits one holder at a time.
///
/// A release notifies the event that waiting threads listen to, and the notification runs their
/// wakers, which come back into that very event from the thread that is notifying it: were the
/// event to run them with its own state still held, that thread would wait for itself for good,
/// which the timeout would catch. The count is read and written back as two steps, so a second
/// holder let in at the same time would lose a step of it.
#[test]
#[timeout(15000)]
fn wakers_coming_back_into_the_lock_from_many_threads_do_not_deadlock() {
    let (threads, rounds) = if cfg!(miri) { (3, 10) } else { (4, 250) };
    let mutex = Arc::new(Mutex::new(0usize));
    let start = Arc::new(Barrier::new(threads));

    let takers: Vec<_> = (0..threads)
        .map(|_| {
            let mutex = mutex.clone();
            let start = start.clone();
            thread::spawn(move || {
                // Lets the threads in together, so that they contend from their first round rather
                // than each running its rounds before the next is spawned.
                start.wait();
                for _ in 0..rounds {
                    let mut count = block_on(ComingBack {
                        mutex: mutex.clone(),
                        future: pin!(mutex.lock()),
                    });
                    let seen = *count;
                    // Gives a second holder, were one let in, a moment to come in between.
                    linger();
                    *count = seen + 1;
                }
            })
        })
        .collect();
    for taker in takers {
        taker.join().unwrap();
    }

    assert_eq!(*block_on(mutex.lock()), threads * rounds);
}

/// Polls `future` once, with a waker that goes nowhere.
fn poll_once<F>(future: &mut F) -> Poll<F::Output>
where
    F: Future + Unpin,
{
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

/// Polls `future` once, with a waker that goes nowhere, and hands back what it completed with.
///
/// Panics if the future is still pending: a test that takes a lock it expects to be free fails
/// here, rather than waits for a release that nothing in it will ever make.
fn ready<F>(future: F) -> F::Output
where
    F: Future,
{
    let Poll::Ready(output) = pin!(future).poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("the future is pending, where it was expected to complete at once");
    };

    output
}

/// Lets a moment pass while a lock is held.
///
/// A spin rather than a yield: on a busy machine, a yield hands the thread away with the lock
/// held, keeping every other thread waiting for it until the thread gets its turn again.
fn linger() {
    for _ in 0..64 {
        hint::spin_loop();
    }
}

/// Runs `rounds` handoffs of `lock` between this thread and another. Each has this thread take the
/// lock in `hold`, which calls the function it is given to let the other thread start to `wait`
/// for the lock, and then returns, which releases the lock. The release comes a little later each
/// round, so that it is spread across the steps of the wait. Nothing else ever releases the lock,
/// so a release the waiter misses leaves it waiting for good, which the timeout of a test catches.
fn handoff<L, H, W>(lock: Arc<L>, rounds: usize, hold: H, wait: W)
where
    L: Send + Sync + 'static,
    H: Fn(&L, &dyn Fn()),
    W: Fn(&L) + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(2));
    let waiter = thread::spawn({
        let lock = lock.clone();
        let barrier = barrier.clone();
        move || {
            for _ in 0..rounds {
                // Meets the holder as it is about to let go, and starts to wait.
                barrier.wait();
                wait(&lock);
                // The wait is over, so the next round may take the lock again.
                barrier.wait();
            }
        }
    });
    for round in 0..rounds {
        // The function passed to `hold` is where the waiter is met, and the lock is released as
        // `hold` returns, after a spin that is longer each round. Left to the timing of the two
        // threads alone, the release would seldom fall between the steps of the wait.
        hold(&lock, &|| {
            barrier.wait();
            for _ in 0..round % 256 {
                hint::spin_loop();
            }
        });
        // Waits for the waiter to have had its turn, so that the next round finds the lock free.
        barrier.wait();
    }
    waiter.join().unwrap();
}

/// A future that polls `future` through a waker that comes back into `mutex` from its wake, before
/// it wakes the task that polled this future.
struct ComingBack<F> {
    mutex: Arc<Mutex<usize>>,
    future: F,
}

impl<F> Future for ComingBack<F>
where
    F: Future + Unpin,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let waker = Waker::from(Arc::new(ComeBackThenWake {
            mutex: this.mutex.clone(),
            task: cx.waker().clone(),
        }));

        Pin::new(&mut this.future).poll(&mut Context::from_waker(&waker))
    }
}

/// A waker that comes back into a mutex from its wake, before it wakes the task it stands for.
///
/// The mutex keeps its event to itself, so the waker comes back into it through the mutex's own
/// calls: it polls a fresh `lock` once, with a waker that goes nowhere, and lets go of what that
/// gave. A guard, where the mutex was free, releases the mutex and notifies its event as it goes;
/// a wait still pending gives up the listener it took. The waker is run by a notification of the
/// mutex's event, so either happens inside that notification.
struct ComeBackThenWake {
    mutex: Arc<Mutex<usize>>,
    task: Waker,
}

impl Wake for ComeBackThenWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // `drop` lets go of the poll's result, and the future it polled goes at the end of this
        // statement, so whatever the poll took is let go of before the task is woken.
        drop(pin!(self.mutex.lock()).poll(&mut Context::from_waker(Waker::noop())));
        self.task.wake_by_ref();
    }
}
