//! What `Event`'s listen and notify paths cost: taking a listener, alone and dropped at once,
//! sending a notification to nobody, waking one listener and a hundred taken beforehand, and
//! `lock::Mutex`, built on an event, taken by one thread alone and under four contending threads.
//! `Event` needs no runtime, so no id here goes through `zruntime::block_on`: the single-threaded
//! ones poll listeners by hand or take the mutex with `try_lock`, and `mutex/4-threads` blocks on
//! each `lock` with futures-lite's `block_on`.
//!
//! `listen` times taking a listener, alone, on an event whose shared state is already allocated:
//! what every wait starts with. The listeners are taken a batch at a time and dropped once the
//! batch has been timed, so the event's queue holds a batch of them at most.
//!
//! `listen-drop` times taking a listener and dropping it at once, on an event whose shared state
//! is already allocated: the common case of a listener whose wait never has to happen because the
//! condition it was taken for already held.
//!
//! `notify-none` times `notify(1)` on an initialised event with nobody listening: the path a
//! lock's release takes when nobody is waiting for it, and so what `notify` costs even where
//! there is nothing to wake.
//!
//! `mutex/uncontended` times taking and releasing zruntime's own `lock::Mutex` on one thread, with
//! nobody else after it: the commonest use of a lock, whose release notifies the mutex's event with
//! nobody listening.
//!
//! `wake-one` times notifying one listener and polling it once to `Ready`: the end of a wait,
//! from being told it is over to finding so, without its start, which `listen` times. The listeners
//! are taken a batch at a time before the batch is timed, and each notification reaches the oldest
//! of them, which is the one polled next.
//!
//! `wake-all/100` times waking a hundred listeners, taken before the timing starts, with one
//! `notify(usize::MAX)` and polling each to `Ready`: a broadcast to every listener an event has,
//! such as a connection announcing that it has closed.
//!
//! `mutex/4-threads` times zruntime's own `lock::Mutex` — built on an `Event`, which a release
//! notifies once, to wake one waiter — taken and released 1000 times by each of four threads at
//! once, a contended lock under real cross-thread wakes. The four threads are spawned once,
//! before the group starts, and kept alive for every sample: a pair of barriers starts a round of
//! 1000 lock/unlock each and waits for it to end, and only the time between the two is measured,
//! so spawning and joining the threads is never counted as the lock's cost. Each thread is pinned
//! to a CPU of its own, and they start each round together, so that every process measures the
//! same placement of the threads, and all four contend from the first lock of a round on (see
//! `common/crew.rs`).

use std::{
    future::Future,
    hint::black_box,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use crew::{Crew, Round};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use zruntime::{Event, EventListener, lock::Mutex};

#[path = "common/cpus.rs"]
mod cpus;
#[path = "common/crew.rs"]
mod crew;

/// Times `listen`, `notify` and the poll that resolves a listener, all on a single thread.
fn event_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("event");

    let listen = Event::new();
    // Allocates the event's shared state up front, so the timed batches below never pay for that.
    listen.notify(0);
    group.bench_function("listen", |b| {
        // The listeners each batch takes are dropped once it has been timed.
        b.iter_batched(
            || (),
            |()| listen.listen(),
            BatchSize::NumIterations(LISTENERS_PER_BATCH),
        );
    });

    let listen_drop = Event::new();
    // Allocates the event's shared state up front, so the timed loop below never pays for that.
    listen_drop.notify(0);
    group.bench_function("listen-drop", |b| {
        b.iter(|| drop(black_box(listen_drop.listen())));
    });

    let notify_none = Event::new();
    notify_none.notify(0);
    group.bench_function("notify-none", |b| {
        b.iter(|| black_box(notify_none.notify(1)));
    });

    let uncontended = Mutex::new(());
    group.bench_function("mutex/uncontended", |b| {
        // The guard is dropped at once: the mutex is taken and released.
        b.iter(|| drop(black_box(uncontended.try_lock())));
    });

    let wake_one = Event::new();
    group.bench_function("wake-one", |b| {
        // A batch's listeners are all taken before it is timed, and each notification reaches the
        // oldest of them still waiting: the one the same iteration polls. Each is handed back to be
        // dropped once the batch has been timed.
        b.iter_batched(
            || wake_one.listen(),
            |mut listener| {
                assert_eq!(wake_one.notify(1), 1);
                assert!(poll_once(&mut listener).is_ready());

                listener
            },
            BatchSize::NumIterations(LISTENERS_PER_BATCH),
        );
    });

    group.throughput(Throughput::Elements(100));
    let wake_all = Event::new();
    group.bench_function("wake-all/100", |b| {
        b.iter_batched(
            || (0..100).map(|_| wake_all.listen()).collect::<Vec<_>>(),
            |mut listeners| {
                assert_eq!(wake_all.notify(usize::MAX), 100);
                for listener in &mut listeners {
                    assert!(poll_once(listener).is_ready());
                }

                listeners
            },
            // One iteration a batch: a notification to every listener there is would reach the
            // listeners taken for the next iterations as well.
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// Polls `listener` once, with a waker that goes nowhere, and hands back what the poll found.
fn poll_once(listener: &mut EventListener) -> Poll<()> {
    Pin::new(listener).poll(&mut Context::from_waker(Waker::noop()))
}

/// How many listeners `listen` and `wake-one` take for each timed batch: few enough to keep the
/// event's queue short, and enough that reading the clock once a batch costs little next to them.
const LISTENERS_PER_BATCH: u64 = 64;

/// How many threads contend for the lock in `mutex/4-threads`.
const MUTEX_THREADS: usize = 4;
/// How many lock/unlock rounds each thread runs per timed sample of `mutex/4-threads`.
const MUTEX_ROUNDS: usize = 1000;

/// Times `lock::Mutex`, which is built on an event, taken and released [`MUTEX_ROUNDS`] times by
/// each of [`MUTEX_THREADS`] threads at once.
fn mutex_bench(c: &mut Criterion) {
    let lock = Arc::new(Mutex::new(()));
    let crew = Crew::spawn((0..MUTEX_THREADS).map(|_| {
        let lock = lock.clone();
        Box::new(move || {
            for _ in 0..MUTEX_ROUNDS {
                // The guard is dropped at once: the mutex is taken and released.
                drop(futures_lite::future::block_on(lock.lock()));
            }
        }) as Round
    }));

    let mut group = c.benchmark_group("event");
    group.sample_size(10);
    group.throughput(Throughput::Elements((MUTEX_THREADS * MUTEX_ROUNDS) as u64));
    group.bench_function("mutex/4-threads", |b| {
        b.iter_custom(|iters| crew.time(iters))
    });
    group.finish();

    crew.join();
}

criterion_group!(benches, event_benches, mutex_bench);
criterion_main!(benches);
