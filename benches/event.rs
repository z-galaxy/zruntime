//! What `Event`'s listen and notify paths cost: taking a listener, sending a notification to
//! nobody, waking one listener, waking a broadcast of a hundred, and a lock built on an event
//! under four contending threads. `Event` needs no runtime, so every id here polls listeners by
//! hand rather than driving them through `zruntime::block_on`.
//!
//! `listen-drop` times taking a listener and dropping it at once, on an event whose shared state
//! is already allocated: the common case of a listener whose wait never has to happen because the
//! condition it was taken for already held.
//!
//! `notify-none` times `notify(1)` on an initialised event with nobody listening: the path a
//! lock's release takes when nobody is waiting for it, and so what `notify` costs even where
//! there is nothing to wake.
//!
//! `notify-one` times taking a listener, notifying it, and polling it once to `Ready`: a waiter's
//! whole life, from registering to being told the wait is over.
//!
//! `notify-all/100` times waking a hundred listeners with one `notify(usize::MAX)` and polling
//! each to `Ready`: a broadcast to every listener an event has, such as a connection announcing
//! that it has closed.
//!
//! `mutex/4-threads` times a lock built the way zbus builds its own — an `AtomicBool` guarding
//! the critical section and an `Event` that a release notifies once — taken and released 1000
//! times by each of four threads at once, a contended lock under real cross-thread wakes. The
//! four threads are spawned once, before the group starts, and kept alive for every sample: a
//! pair of barriers starts a round of 1000 lock/unlock each and waits for it to end, and only the
//! time between the two is measured, so spawning and joining the threads is never counted as the
//! lock's cost.

use std::{
    future::Future,
    hint::black_box,
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use zruntime::{Event, EventListener};

/// Times `listen`, `notify` and the poll that resolves a listener, all on a single thread.
fn event_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("event");

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

    let notify_one = Event::new();
    group.bench_function("notify-one", |b| {
        b.iter(|| {
            let mut listener = notify_one.listen();
            notify_one.notify(1);

            assert!(poll_once(&mut listener).is_ready());
        });
    });

    group.throughput(Throughput::Elements(100));
    let notify_all = Event::new();
    group.bench_function("notify-all/100", |b| {
        b.iter(|| {
            let mut listeners: Vec<_> = (0..100).map(|_| notify_all.listen()).collect();
            assert_eq!(notify_all.notify(usize::MAX), 100);
            for listener in &mut listeners {
                assert!(poll_once(listener).is_ready());
            }
        });
    });

    group.finish();
}

/// Polls `listener` once, with a waker that goes nowhere, and hands back what the poll found.
fn poll_once(listener: &mut EventListener) -> Poll<()> {
    Pin::new(listener).poll(&mut Context::from_waker(Waker::noop()))
}

/// How many threads contend for the lock in `mutex/4-threads`.
const MUTEX_THREADS: usize = 4;
/// How many lock/unlock rounds each thread runs per timed sample of `mutex/4-threads`.
const MUTEX_ROUNDS: usize = 1000;

/// Times a lock built on an event, taken and released [`MUTEX_ROUNDS`] times by each of
/// [`MUTEX_THREADS`] threads at once.
///
/// The threads are spawned once, before the benchmark starts, and stay for every sample: each
/// waits on `start`, runs its rounds, and waits on `end`, so a sample is timed from the main
/// thread releasing `start` to every thread having reached `end`, with nothing of a thread's own
/// startup or shutdown inside that span.
fn mutex_bench(c: &mut Criterion) {
    let lock = Arc::new(Mutex::default());
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(MUTEX_THREADS + 1));
    let end = Arc::new(Barrier::new(MUTEX_THREADS + 1));
    let threads: Vec<_> = (0..MUTEX_THREADS)
        .map(|_| {
            let lock = lock.clone();
            let stop = stop.clone();
            let start = start.clone();
            let end = end.clone();
            thread::spawn(move || {
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    for _ in 0..MUTEX_ROUNDS {
                        futures_lite::future::block_on(lock.lock());
                        lock.unlock();
                    }
                    end.wait();
                }
            })
        })
        .collect();

    let mut group = c.benchmark_group("event");
    group.sample_size(10);
    group.throughput(Throughput::Elements((MUTEX_THREADS * MUTEX_ROUNDS) as u64));
    group.bench_function("mutex/4-threads", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let started = Instant::now();
                start.wait();
                end.wait();
                total += started.elapsed();
            }

            total
        });
    });
    group.finish();

    stop.store(true, Ordering::Release);
    start.wait();
    for thread in threads {
        thread.join().unwrap();
    }
}

/// A lock built the way zbus builds its own: a flag taken with a compare-exchange, and an event
/// that a release notifies once.
#[derive(Default)]
struct Mutex {
    locked: AtomicBool,
    released: Event,
}

impl Mutex {
    /// Takes the lock, waiting for its release where it is already held.
    async fn lock(&self) {
        loop {
            if self.try_lock() {
                return;
            }
            // Taken before the second try, so that a release in between is one it hears of.
            let listener = self.released.listen();
            if self.try_lock() {
                return;
            }
            listener.await;
        }
    }

    /// Lets go of the lock, and wakes a thread waiting for it.
    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
        self.released.notify(1);
    }

    fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }
}

criterion_group!(benches, event_benches, mutex_bench);
criterion_main!(benches);
