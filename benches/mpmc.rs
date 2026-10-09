//! What an `mpmc` channel costs: on a single thread, and between the tasks of a runtime, every id
//! a different shape of use of one channel of `u64`s. They are the shapes the "Performance" section
//! of the `mpmc` module's documentation times against async-channel and tokio's `mpsc`, kept here
//! as regression checks of this channel alone: the comparison needs those two crates, which this
//! one does not depend on. The single-thread ids are the rows of that table of the same shape; the
//! task ids are not, as the comparison ran its tasks on a tokio runtime. The rows of threads are
//! timed in `contention.rs`, as only the clock can measure them.
//!
//! `mpmc` needs no runtime, so the single-thread ids drive their futures with the `block_on` of
//! `futures`, not `zruntime::block_on`. Only the two `tasks-*` ids use the runtime, to run tasks
//! on. Every id sends one message and receives it before it is timed, so that what the channel
//! allocates lazily on its first use is allocated by then.
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
//! Tasks, rather than threads, on one `zruntime::LocalRuntime` that is made once, before the first
//! of the two ids, where the comparison used a tokio runtime of four workers. Each iteration spawns
//! the sender and receiver tasks with `runtime.spawn`, in one `block_on` of the runtime, and awaits
//! every one of them. The channel has room for 16 messages, and the senders and receivers are those
//! of the thread ids of the same shape in `contention.rs`. A local runtime is single-threaded, so
//! these measure the channel and the runtime's task wakes with no thread parking at all. In a round
//! each sender sends [`channel::MESSAGES`] messages, and the receivers between them receive exactly
//! as many as the senders send, each an equal share. The throughput is in messages per round.
//!
//! - `tasks-mpsc-4to1-cap16`: four sender tasks and one receiver task.
//! - `tasks-mpmc-4to4-cap16`: four sender tasks and four receiver tasks.

use std::hint::black_box;

use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main, measurement::WallTime,
};
use futures::executor::block_on;
use zruntime::LocalRuntime;

#[path = "common/channel.rs"]
mod channel;

/// Times every id, one after the other, in one group.
fn channel_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");

    ping(&mut group, "ping-bounded1", Some(1));
    ping(&mut group, "ping-unbounded", None);
    burst(&mut group, "burst-bounded1024", Some(BURST as usize));
    burst(&mut group, "burst-unbounded", None);

    // A round of the ids below takes milliseconds, so they take fewer samples than the ids above.
    group.sample_size(10);

    let runtime = LocalRuntime::new().unwrap();
    bench_tasks(&mut group, &runtime, "tasks-mpsc-4to1-cap16", 4, 1, 16);
    bench_tasks(&mut group, &runtime, "tasks-mpmc-4to4-cap16", 4, 4, 16);

    group.finish();
}

/// Times sending one message and receiving it, on a channel of capacity `cap` (unbounded if
/// `None`), all on a single thread.
fn ping(group: &mut BenchmarkGroup<'_, WallTime>, id: &str, cap: Option<usize>) {
    group.throughput(Throughput::Elements(1));

    let (s, r) = channel::new(cap);
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

    let (s, r) = channel::new(cap);
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

/// Times rounds of `senders` tasks sending [`channel::MESSAGES`] messages each to `receivers` tasks
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
    let share = channel::receiver_share(senders, receivers);
    group.throughput(Throughput::Elements(senders as u64 * channel::MESSAGES));

    let (s, r) = channel::new(Some(cap));
    let mut idle_senders: Vec<_> = (0..senders).map(|_| s.clone()).collect();
    let mut idle_receivers: Vec<_> = (0..receivers).map(|_| r.clone()).collect();

    group.bench_function(id, |b| {
        b.iter(|| {
            runtime.block_on(async {
                let sender_tasks: Vec<_> = idle_senders
                    .drain(..)
                    .map(|s| {
                        runtime.spawn("sender", async move {
                            channel::send_n(&s, channel::MESSAGES).await;
                            s
                        })
                    })
                    .collect();
                let receiver_tasks: Vec<_> = idle_receivers
                    .drain(..)
                    .map(|r| {
                        runtime.spawn("receiver", async move {
                            channel::recv_n(&r, share).await;
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

/// How many messages the `burst-*` ids send, and then receive.
const BURST: u64 = 1024;

criterion_group!(benches, channel_benches);
criterion_main!(benches);
