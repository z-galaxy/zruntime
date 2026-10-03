//! What an `mpmc` channel costs: on a single thread, between threads, and between the tasks of a
//! runtime, every id a different shape of use of one channel of `u64`s. They are the shapes the
//! "Performance" section of the `mpmc` module's documentation times against async-channel and
//! tokio's `mpsc`, kept here as regression checks of this channel alone: the comparison needs
//! those two crates, which this one does not depend on. The single-thread and the thread ids are
//! the rows of that table of the same shape; the task ids are not, as the comparison ran its tasks
//! on a tokio runtime.
//!
//! `mpmc` needs no runtime, so the single-thread and the thread ids drive their futures with
//! futures-lite's `block_on`, not `zruntime::block_on`. Only the two `tasks-*` ids use the runtime,
//! to run tasks on. Every id sends one message and receives it before it is timed, so that what
//! the channel allocates lazily on its first use is allocated by then.
//!
//! Single thread:
//!
//! - `ping-bounded1` times one `send(..).await` and one `recv().await` on a channel with room for
//!   one message, with one `block_on` around the pair: the uncontended path, and so the cost of a
//!   channel whose receivers keep up with its senders. The message is received before the next is
//!   sent, so no send finds the channel full, no receive finds it empty, and nothing waits. Every
//!   notification reaches nobody: the two that a send makes to the receivers waiting for a message,
//!   and the one that a receive makes to the senders waiting for room. Each finds that nobody
//!   listens with one atomic look at its event, and takes no lock.
//! - `ping-unbounded` times the same pair on an unbounded channel. It differs from `ping-bounded1`
//!   in one notification: an unbounded channel has no sender waiting for room, so a receive from it
//!   makes none, where a receive from a bounded one looks for a sender to tell.
//! - `burst-bounded1024` and `burst-unbounded` time 1024 `try_send`s and then as many `try_recv`s,
//!   with no `block_on` and no future: the channel's queue and the bookkeeping around it, with
//!   nothing to wait for. The bounded channel has room for all 1024. The throughput is in messages.
//!
//! Threads, each running its whole round inside one `block_on`, not one per operation. The
//! threads are spawned once per id and kept alive for every sample: a pair of barriers starts a
//! round and waits for it to end, and only the time between the two is measured, so spawning and
//! joining the threads is never counted as the channel's cost. In a round each sender sends
//! [`MESSAGES`] messages, and the receivers between them receive exactly as many as the senders
//! send, each an equal share, so a round leaves the channel empty and the next starts as this one
//! did. The throughput is in messages per round.
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
//! Tasks, rather than threads, on one `zruntime::LocalRuntime` that is made once, before the first
//! of the two ids, where the comparison used a tokio runtime of four workers. Each iteration spawns
//! the sender and receiver tasks with `runtime.spawn`, in one `block_on` of the runtime, and awaits
//! every one of them. The channel has room for 16 messages, and the senders and receivers are those
//! of the thread ids of the same shape. A local runtime is single-threaded, so these measure the
//! channel and the runtime's task wakes with no thread parking at all, which makes them the
//! steadiest of the ids with more than one side.
//!
//! - `tasks-mpsc-4to1-cap16`: four sender tasks and one receiver task.
//! - `tasks-mpmc-4to4-cap16`: four sender tasks and four receiver tasks.

use std::{
    hint::black_box,
    num::NonZeroUsize,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main, measurement::WallTime,
};
use futures_lite::future::block_on;
use zruntime::{
    LocalRuntime,
    mpmc::{Receiver, Sender, bounded, unbounded},
};

/// Times every id, one after the other, in one group.
fn channel_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");

    ping(&mut group, "ping-bounded1", Some(1));
    ping(&mut group, "ping-unbounded", None);
    burst(&mut group, "burst-bounded1024", Some(BURST as usize));
    burst(&mut group, "burst-unbounded", None);

    // A round of the ids below takes milliseconds, and that of `mpsc-4to1-cap16` about half a
    // second on four cores, so they take fewer samples than the ids above.
    group.sample_size(10);

    let workers = vec![
        bench_threads(&mut group, "spsc-cap16", 1, 1, Some(16)),
        bench_threads(&mut group, "spsc-cap1024", 1, 1, Some(1024)),
        bench_threads(&mut group, "mpsc-4to1-cap16", 4, 1, Some(16)),
        bench_threads(&mut group, "mpmc-4to4-cap16", 4, 4, Some(16)),
        bench_threads(&mut group, "mpmc-4to4-unbounded", 4, 4, None),
    ];

    let runtime = LocalRuntime::new().unwrap();
    bench_tasks(&mut group, &runtime, "tasks-mpsc-4to1-cap16", 4, 1, 16);
    bench_tasks(&mut group, &runtime, "tasks-mpmc-4to4-cap16", 4, 4, 16);

    group.finish();

    for workers in workers {
        workers.join();
    }
}

/// Times sending one message and receiving it, on a channel of capacity `cap` (unbounded if
/// `None`), all on a single thread.
fn ping(group: &mut BenchmarkGroup<'_, WallTime>, id: &str, cap: Option<usize>) {
    group.throughput(Throughput::Elements(1));

    let (s, r) = channel(cap);
    warm_up(&s, &r);
    let mut n = 0u64;
    group.bench_function(id, |b| {
        b.iter(|| {
            block_on(async {
                s.send(n).await.unwrap();
                assert_eq!(r.recv().await.unwrap(), n);
            });
            n += 1;
        })
    });
}

/// Times sending [`BURST`] messages with `try_send` and then receiving them with `try_recv`, on a
/// channel of capacity `cap` (unbounded if `None`), all on a single thread.
fn burst(group: &mut BenchmarkGroup<'_, WallTime>, id: &str, cap: Option<usize>) {
    group.throughput(Throughput::Elements(BURST));

    let (s, r) = channel(cap);
    warm_up(&s, &r);
    group.bench_function(id, |b| {
        b.iter(|| {
            for i in 0..BURST {
                s.try_send(i).unwrap();
            }
            for _ in 0..BURST {
                black_box(r.try_recv().unwrap());
            }
        })
    });
}

/// Times rounds of `senders` threads sending [`MESSAGES`] messages each to `receivers` threads
/// receiving them, on a channel of capacity `cap` (unbounded if `None`), one `block_on` per thread
/// per round. Returns the threads, still parked, for the caller to join once it has timed
/// whatever else it has.
///
/// The threads are spawned once, before the first sample, and stay for every sample: each waits
/// on `start`, runs its round, and waits on `end`, so a sample is timed from the main thread
/// releasing `start` to every thread having reached `end`, with nothing of a thread's own startup
/// or shutdown inside that span.
fn bench_threads(
    group: &mut BenchmarkGroup<'_, WallTime>,
    id: &str,
    senders: usize,
    receivers: usize,
    cap: Option<usize>,
) -> Workers {
    let share = receiver_share(senders, receivers);
    group.throughput(Throughput::Elements(senders as u64 * MESSAGES));

    let (s, r) = channel(cap);
    warm_up(&s, &r);

    let stop = Arc::new(AtomicBool::new(false));
    // The senders and the receivers, and the main thread that times them.
    let start = Arc::new(Barrier::new(senders + receivers + 1));
    let end = Arc::new(Barrier::new(senders + receivers + 1));

    let mut threads = Vec::with_capacity(senders + receivers);
    for _ in 0..senders {
        let s = s.clone();
        threads.push(spawn_worker(&stop, &start, &end, move || {
            block_on(send_n(&s, MESSAGES));
        }));
    }
    for _ in 0..receivers {
        let r = r.clone();
        threads.push(spawn_worker(&stop, &start, &end, move || {
            block_on(recv_n(&r, share));
        }));
    }

    group.bench_function(id, |b| {
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

    Workers {
        stop,
        start,
        threads,
    }
}

/// Times rounds of `senders` tasks sending [`MESSAGES`] messages each to `receivers` tasks
/// receiving them, on a channel with room for `cap` messages, spawned on `runtime` by a
/// `block_on` of it that awaits them all.
///
/// The endpoints are moved into the tasks and handed back by them, so that the channel is never
/// closed between rounds and no round clones an endpoint.
fn bench_tasks(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &LocalRuntime,
    id: &str,
    senders: usize,
    receivers: usize,
    cap: usize,
) {
    let share = receiver_share(senders, receivers);
    group.throughput(Throughput::Elements(senders as u64 * MESSAGES));

    let (s, r) = channel(Some(cap));
    warm_up(&s, &r);
    let mut idle_senders: Vec<_> = (0..senders).map(|_| s.clone()).collect();
    let mut idle_receivers: Vec<_> = (0..receivers).map(|_| r.clone()).collect();

    group.bench_function(id, |b| {
        b.iter(|| {
            runtime.block_on(async {
                let sender_tasks: Vec<_> = idle_senders
                    .drain(..)
                    .map(|s| {
                        runtime.spawn("sender", async move {
                            send_n(&s, MESSAGES).await;
                            s
                        })
                    })
                    .collect();
                let receiver_tasks: Vec<_> = idle_receivers
                    .drain(..)
                    .map(|r| {
                        runtime.spawn("receiver", async move {
                            recv_n(&r, share).await;
                            r
                        })
                    })
                    .collect();

                for task in sender_tasks {
                    idle_senders.push(task.await.unwrap());
                }
                for task in receiver_tasks {
                    idle_receivers.push(task.await.unwrap());
                }
            })
        })
    });
}

/// How many messages each of `receivers` receives in a round of `senders` senders, which is the
/// same for each, so that they receive exactly the messages that are sent and no receiver is left
/// waiting for one that never comes.
fn receiver_share(senders: usize, receivers: usize) -> u64 {
    let total = senders as u64 * MESSAGES;
    let share = total / receivers as u64;
    assert_eq!(
        share * receivers as u64,
        total,
        "the receivers must share the messages equally"
    );

    share
}

/// Sends `n` messages, waiting for room whenever the channel is full.
async fn send_n(s: &Sender<u64>, n: u64) {
    for i in 0..n {
        s.send(i).await.unwrap();
    }
}

/// Receives `n` messages, waiting whenever the channel is empty.
async fn recv_n(r: &Receiver<u64>, n: u64) {
    for _ in 0..n {
        black_box(r.recv().await.unwrap());
    }
}

/// A channel with room for `cap` messages, or an unbounded one if `cap` is `None`.
fn channel(cap: Option<usize>) -> (Sender<u64>, Receiver<u64>) {
    match cap {
        Some(cap) => bounded(NonZeroUsize::new(cap).unwrap()),
        None => unbounded(),
    }
}

/// Sends one message and receives it, so that whatever the channel allocates lazily on its first
/// use is allocated before the measurement starts.
fn warm_up(s: &Sender<u64>, r: &Receiver<u64>) {
    block_on(async {
        s.send(0).await.unwrap();
        r.recv().await.unwrap();
    });
}

/// The threads of one id of `bench_threads`, waiting on `start` for a round that never comes.
struct Workers {
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    threads: Vec<JoinHandle<()>>,
}

impl Workers {
    /// Releases the threads with `stop` set, so that they exit, and joins them.
    fn join(self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for thread in self.threads {
            thread.join().unwrap();
        }
    }
}

/// Spawns a thread that waits on `start`, runs `round`, and waits on `end`, over and over, until
/// `stop` is set by the time `start` releases it, when it exits instead of running a round.
fn spawn_worker<F>(
    stop: &Arc<AtomicBool>,
    start: &Arc<Barrier>,
    end: &Arc<Barrier>,
    mut round: F,
) -> JoinHandle<()>
where
    F: FnMut() + Send + 'static,
{
    let stop = stop.clone();
    let start = start.clone();
    let end = end.clone();

    thread::spawn(move || {
        loop {
            start.wait();
            if stop.load(Ordering::Acquire) {
                return;
            }
            round();
            end.wait();
        }
    })
}

/// How many messages each sender of a threaded or a task id sends in a round.
const MESSAGES: u64 = 10_000;
/// How many messages the `burst-*` ids send, and then receive.
const BURST: u64 = 1024;

criterion_group!(benches, channel_benches);
criterion_main!(benches);
