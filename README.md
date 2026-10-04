# zruntime

[![CI Pipeline Status](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml/badge.svg)](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml)
[![](https://docs.rs/zruntime/badge.svg)](https://docs.rs/zruntime/)
[![](https://img.shields.io/crates/v/zruntime)](https://crates.io/crates/zruntime)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/z-galaxy/zruntime?utm_source=badge)

A simple, single-threaded Rust async runtime. A [`Runtime`] is an owned value made of:

* a scheduler that holds tasks and hands them out to be polled,
* a `poll(2)`/`select` reactor that watches registered I/O sources and keeps timers, and
* [`Runtime::block_on`] to drive a future to completion on the calling thread.

It comes in two flavours. [`LocalRuntime`] is the default one: it stays on the thread it was made on, runs any
`'static` future, and holds its state in `Rc` and `RefCell` rather than behind atomics and locks.
[`SharedRuntime`] may be reached from, and driven on, any thread, at the cost of `Send` futures
and state kept in `Arc` and `Mutex`.

## Example

A local runtime, used by the thread that made it:

```rust
use std::{cell::RefCell, rc::Rc, time::Duration};

use zruntime::LocalRuntime;

let runtime = LocalRuntime::new().expect("a runtime for this thread");
let doubled = runtime.block_on(async {
    let count = Rc::new(RefCell::new(0));
    let sleeper = runtime.clone();
    let task = runtime.spawn("double", {
        let count = count.clone();
        async move {
            sleeper.sleep(Duration::from_millis(1)).await;
            *count.borrow_mut() += 21;
            *count.borrow() * 2
        }
    });

    task.await.expect("the task did not panic")
});

assert_eq!(doubled, 42);
```

A shared runtime, reached from another thread while the main thread drives it:

```rust
use std::{sync::mpsc, thread};

use zruntime::SharedRuntime;

let runtime = SharedRuntime::new().expect("a runtime for this thread");
let (sender, receiver) = mpsc::channel();
let spawner = thread::spawn({
    let runtime = runtime.clone();
    move || {
        let task = runtime.spawn("double", async { 21 * 2 });
        sender.send(task).expect("the main thread is still waiting for it");
    }
});

let task = receiver.recv().expect("the other thread sent the task");
let doubled = runtime.block_on(task).expect("the task did not panic");
spawner.join().expect("the other thread did not panic");

assert_eq!(doubled, 42);
```

`Runtime::new()` alone cannot tell which flavour to build, so reach for one of [`LocalRuntime`]
or [`SharedRuntime`] instead of naming [`Runtime`] directly.

A future built from several tasks, timers and registered sockets runs the same way: `block_on`
drives all of it on the calling thread.

## Timers

[`Runtime::sleep`] waits for a length of time and [`Runtime::sleep_until`] for a moment on the
clock, which keeps a loop to a schedule without drift, and a [`Sleep`] can be pushed back in place
with [`Sleep::reset`], as a keep-alive or an idle timeout is on every message. [`Runtime::timeout`]
gives any future a time limit, and [`Runtime::interval`] ticks once every period, as a stream:

```rust
use std::{future, time::Duration};

use zruntime::LocalRuntime;

let runtime = LocalRuntime::new().expect("a runtime for this thread");
runtime.block_on(async {
    let mut interval = runtime.interval(Duration::from_millis(1));
    for _ in 0..3 {
        interval.tick().await;
    }

    let never = runtime.timeout(Duration::from_millis(2), future::pending::<()>());
    assert!(never.await.is_err());
});
```

## Spawning

[`Runtime::spawn`] puts a task on the runtime it is called on, under a name that the message logged
if the task panics goes by. Code that has no runtime to call it on, because it was never handed
one, spawns with the free [`spawn`] or [`spawn_local`] instead: the task goes on the runtime that is
running the calling code, whichever it is, and is named by where it was spawned.

```rust
use zruntime::{LocalRuntime, Task};

// Handed no runtime, and needing none: it spawns on whichever one is running its caller.
fn double(number: u32) -> Task<u32> {
    zruntime::spawn_local(async move { number * 2 })
}

let runtime = LocalRuntime::new().expect("a runtime for this thread");
let doubled = runtime.block_on(async { double(21).await.expect("the task did not panic") });

assert_eq!(doubled, 42);
```

[`spawn_local`] takes any `'static` future and needs the calling thread to be inside a
[`LocalRuntime::block_on`], or in one of its tasks. [`spawn`] takes a `Send` future for the
[`SharedRuntime`] the calling thread drives; where it drives none, it panics, unless the `helper`
feature, described below, is there to give it a runtime to spawn on.

The [`Task`] either hands back is what joins the task, by being awaited. Dropping it cancels the
task, and [`Task::cancel`] cancels it and waits until it has stopped, handing back its output where
it had finished already. [`Task::detach`] lets it run on unobserved, and [`Task::is_finished`] tells
whether it has ended without polling it or taking its output.

## The `helper` feature

The examples above each drive their runtime with one `block_on` call. A library whose
futures may be polled from another executor, or that calls `block_on` once per operation rather
than once for the whole program — [zbus], say — needs a runtime that keeps running in between
those calls instead of standing still until the next one.

The non-default `helper` feature adds that: [`SharedRuntime::current`] hands out a runtime for
the calling thread (its own where the thread is inside a `block_on`, one shared by the whole
process otherwise), and the free [`block_on`] drives it. Work that outlives every `block_on`
call is picked up by a helper thread, started the moment such work is found with nobody driving
the runtime, and put down once nothing is left to run, watch or time. A `block_on` call that
arrives while the helper holds the runtime is handed it straight away, so a program calling
`block_on` once per operation still runs each of them on its own thread rather than behind a
thread of the runtime's own. With it, [`spawn`] has a runtime to go on from any thread: where the
calling thread drives none, the task goes on the one [`SharedRuntime::current`] hands out.

## Sockets

The non-default `tcp`, `udp` and `unix` features add ready-made async sockets, as smol has in
`smol::net`: the [`net`] module's `TcpListener`, `TcpStream` and `UdpSocket`, and on unix its
`unix` module's `UnixListener`, `UnixStream` and `UnixDatagram`, all of which run on a runtime of
either flavour. A stream implements the `AsyncRead` and `AsyncWrite` traits of `futures-io`, and
connecting it never blocks the thread. The TCP and UDP sockets take socket addresses rather than
host names: a name is the caller's to look up, which the `unblock` feature can do off the thread.

## Events

An [`Event`] is a notification that tasks can wait for. A task takes an [`EventListener`] from it
and awaits that, and whoever changes what the task is waiting for — releases a lock, fills a
queue, closes a connection — notifies the event, which wakes as many of the tasks listening as it
is asked to, oldest first. It is what the waiting part of a lock, a channel or a connection is
built on.

An event needs no runtime. A listener is a plain future, woken through the waker of whatever
polled it last, so it works under any executor, and an event may be notified from any thread. It
is behind the `event` feature, which builds without the runtime: see [Features](#features).

## Features

* `runtime` (default): [`Runtime`], [`LocalRuntime`] and [`SharedRuntime`], with the tasks,
  timers and I/O registrations built on them; it brings the `futures-core` crate.
* `event` (default): [`Event`] and [`EventListener`], which need no runtime.
* `tracing` (default): the runtime logs through [`tracing`]; a build without it emits no log
  events.
* `helper`: the layer [described above](#the-helper-feature); it implies `runtime`.
* `broadcast`: the [`broadcast`] module, an async multi-producer multi-consumer broadcast channel
  built on [`Event`]; it implies `event`, and needs no runtime either.
* `mpmc`: the [`mpmc`] module, an async multi-producer multi-consumer channel, each of whose
  messages one receiver gets, built on [`Event`]; it implies `event`, and needs no runtime
  either.
* `lock`: the [`lock`] module, an async `Mutex` and `RwLock`, whose guards a task may hold across
  an await, built on [`Event`]; it implies `event`, and needs no runtime either.
* `unblock`: [`unblock`], which runs a piece of blocking work on a pool of threads kept for it
  and hands back a future of its outcome, and [`Unblock`], an adapter that gives a blocking I/O
  handle, such as a file or the standard input, the async I/O traits of `futures-io` that way; it
  needs no runtime either, and brings the `futures-io` and `futures-core` crates.
* `fs`: the [`fs`] module, async access to the filesystem in the shape of `std::fs` and of smol's
  `smol::fs`, with each operation run as blocking work on [`unblock`]'s pool; it implies `unblock`
  and `lock`, and needs no runtime either.
* `tcp`: the [`net`] module's TCP sockets, `TcpListener` and `TcpStream`; it implies `runtime`,
  and brings the `socket2`, `futures-io` and `futures-core` crates.
* `udp`: the [`net`] module's `UdpSocket`; it implies `runtime`.
* `unix`: the [`net`] module's `unix` module, with unix-domain sockets, on unix only; it implies
  `runtime`, and brings the `socket2`, `futures-io` and `futures-core` crates and `rustix`'s
  `net` feature.

`runtime` and `event` each build without the other. A crate that wants only the `Event` builds
zruntime with `default-features = false, features = ["event"]`, which builds none of the runtime,
nor the `rustix` and `windows-sys` crates the runtime polls with; one that wants only the runtime
leaves `event` out.

## Why?

The project grew out of the need for a single-threaded runtime in [zbus] that it would use by
default. It was split into a separate project so non-zbus users can use it too.

## License

[MIT]

[`Runtime`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html
[`Runtime::block_on`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.block_on
[`LocalRuntime`]: https://docs.rs/zruntime/latest/zruntime/type.LocalRuntime.html
[`SharedRuntime`]: https://docs.rs/zruntime/latest/zruntime/type.SharedRuntime.html
[`SharedRuntime::current`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.current
[`block_on`]: https://docs.rs/zruntime/latest/zruntime/fn.block_on.html
[`Runtime::spawn`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.spawn
[`LocalRuntime::block_on`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.block_on
[`spawn`]: https://docs.rs/zruntime/latest/zruntime/fn.spawn.html
[`spawn_local`]: https://docs.rs/zruntime/latest/zruntime/fn.spawn_local.html
[`Task`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html
[`Task::cancel`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.cancel
[`Task::detach`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.detach
[`Task::is_finished`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.is_finished
[`Runtime::sleep`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.sleep
[`Runtime::sleep_until`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.sleep_until
[`Sleep`]: https://docs.rs/zruntime/latest/zruntime/struct.Sleep.html
[`Sleep::reset`]: https://docs.rs/zruntime/latest/zruntime/struct.Sleep.html#method.reset
[`Runtime::timeout`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.timeout
[`Runtime::interval`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.interval
[`Event`]: https://docs.rs/zruntime/latest/zruntime/struct.Event.html
[`EventListener`]: https://docs.rs/zruntime/latest/zruntime/struct.EventListener.html
[`broadcast`]: https://docs.rs/zruntime/latest/zruntime/broadcast/index.html
[`mpmc`]: https://docs.rs/zruntime/latest/zruntime/mpmc/index.html
[`lock`]: https://docs.rs/zruntime/latest/zruntime/lock/index.html
[`unblock`]: https://docs.rs/zruntime/latest/zruntime/fn.unblock.html
[`Unblock`]: https://docs.rs/zruntime/latest/zruntime/struct.Unblock.html
[`fs`]: https://docs.rs/zruntime/latest/zruntime/fs/index.html
[`net`]: https://docs.rs/zruntime/latest/zruntime/net/index.html
[`tracing`]: https://docs.rs/tracing
[zbus]: https://github.com/z-galaxy/zbus
[MIT]: (LICENSE)
