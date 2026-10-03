//! What an `mpmc` channel and a `lock::Mutex` cost between threads that wait for each other: the
//! ids of the `mpmc` and `event` groups that run on several threads. They are kept apart from
//! those groups' other ids, in `mpmc.rs` and `event.rs`, because only the clock can measure them:
//! their threads wait for each other in the kernel, which CodSpeed's CPU simulation leaves out,
//! and they time their rounds themselves, which the simulation's harness does not run at all.
//!
//! Every thread runs its whole round inside one futures-lite `block_on`, not one per operation.
//! The threads are spawned once per id and kept alive for every sample: a pair of barriers starts a
//! round and waits for it to end, and only the time between the two is measured, so spawning and
//! joining the threads is never counted. Each thread is pinned to a CPU of its own, and they start
//! each round together, so that every process measures the same placement of the threads and the
//! same contention from the start of a round on (see `common/crew.rs`).
//!
//! The channel ids are the thread rows of the table in the "Performance" section of the `mpmc`
//! module's documentation, as `mpmc.rs` tells. In a round each sender sends
//! [`channel::MESSAGES`] messages, and the receivers between them receive exactly as many as the
//! senders send, each an equal share, so a round leaves the channel empty and the next starts as
//! this one did. The throughput is in messages per round.
//!
//! - `spsc-cap16` and `spsc-cap1024`: one sender thread and one receiver thread, on a channel with
//!   room for 16 messages and for 1024.
//! - `mpsc-4to1-cap16`: four sender threads and one receiver thread, with room for 16.
//! - `mpmc-4to4-cap16` and `mpmc-4to4-unbounded`: four sender threads and four receiver threads,
//!   with room for 16 and without a limit.
//!
//! `spsc-cap16` and `mpsc-4to1-cap16` mostly measure how long the operating system takes to wake a
//! parked thread. With room for 16 messages the channel is small enough that the side that is ahead
//! fills or drains it and waits for the other, and each such `Pending` costs tens of microseconds
//! of wake latency, in which the channel's own cost is lost. A change in either points at a change
//! in how often a side has to wait, not necessarily at a change in how much a send or a receive
//! costs.
//!
//! `mutex/4-threads` times zruntime's own `lock::Mutex` — built on an `Event`, which a release
//! notifies once, to wake one waiter — taken and released 1000 times by each of four threads in a
//! round, a contended lock under real cross-thread wakes.

use std::sync::Arc;

use crew::{Crew, Round};
use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main, measurement::WallTime,
};
use futures_lite::future::block_on;
use zruntime::lock::Mutex;

#[path = "common/channel.rs"]
mod channel;
#[path = "common/cpus.rs"]
mod cpus;
#[path = "common/crew.rs"]
mod crew;

/// Times every channel id, one after the other, in one group.
fn channel_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");
    // A round takes milliseconds, and that of `mpsc-4to1-cap16` about half a second on four
    // cores, so these take fewer samples than criterion's default.
    group.sample_size(10);

    let crews = vec![
        bench_threads(&mut group, "spsc-cap16", 1, 1, Some(16)),
        bench_threads(&mut group, "spsc-cap1024", 1, 1, Some(1024)),
        bench_threads(&mut group, "mpsc-4to1-cap16", 4, 1, Some(16)),
        bench_threads(&mut group, "mpmc-4to4-cap16", 4, 4, Some(16)),
        bench_threads(&mut group, "mpmc-4to4-unbounded", 4, 4, None),
    ];

    group.finish();

    for crew in crews {
        crew.join();
    }
}

/// Times rounds of `senders` threads sending [`channel::MESSAGES`] messages each to `receivers`
/// threads receiving them, on a channel of capacity `cap` (unbounded if `None`), one `block_on` per
/// thread per round. Returns the threads, still parked, for the caller to join once it has timed
/// whatever else it has.
fn bench_threads(
    group: &mut BenchmarkGroup<'_, WallTime>,
    id: &str,
    senders: usize,
    receivers: usize,
    cap: Option<usize>,
) -> Crew {
    let share = channel::receiver_share(senders, receivers);
    group.throughput(Throughput::Elements(senders as u64 * channel::MESSAGES));

    let (s, r) = channel::new(cap);
    let sender_rounds = (0..senders).map(|_| {
        let s = s.clone();
        Box::new(move || block_on(channel::send_n(&s, channel::MESSAGES))) as Round
    });
    let receiver_rounds = (0..receivers).map(|_| {
        let r = r.clone();
        Box::new(move || block_on(channel::recv_n(&r, share))) as Round
    });
    let crew = Crew::spawn(sender_rounds.chain(receiver_rounds));

    group.bench_function(id, |b| b.iter_custom(|iters| crew.time(iters)));

    crew
}

/// Times `lock::Mutex`, which is built on an event, taken and released [`MUTEX_ROUNDS`] times by
/// each of [`MUTEX_THREADS`] threads at once.
fn mutex_bench(c: &mut Criterion) {
    let lock = Arc::new(Mutex::new(()));
    let crew = Crew::spawn((0..MUTEX_THREADS).map(|_| {
        let lock = lock.clone();
        Box::new(move || {
            for _ in 0..MUTEX_ROUNDS {
                // The guard is dropped at once: the mutex is taken and released.
                drop(block_on(lock.lock()));
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

/// How many threads contend for the lock in `mutex/4-threads`.
const MUTEX_THREADS: usize = 4;
/// How many lock/unlock rounds each thread runs per timed sample of `mutex/4-threads`.
const MUTEX_ROUNDS: usize = 1000;

criterion_group!(benches, channel_benches, mutex_bench);
criterion_main!(benches);
