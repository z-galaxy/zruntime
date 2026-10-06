# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For contribution conventions — commit-message format, atomic commits, code layout, and
more — follow the guidelines in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Project Overview

zruntime is a simple, single-threaded Rust async runtime: an owned `Runtime<M: Mode = Local>`, a
scheduler that holds tasks and hands them out to be polled, a reactor that watches registered I/O
sources (on epoll, kqueue, `select` or `poll(2)`, whichever the platform waits best on) and keeps
timers, and `Runtime::block_on` to drive a future to
completion on the calling thread. It comes in two flavours: `Local` (the default, aliased
`LocalRuntime`), which stays on the thread it was made on and holds its state in `Rc`/`RefCell`,
and `Shared` (aliased `SharedRuntime`), which may be reached from and driven on any thread and
holds its state in `Arc`/`Mutex`. A non-default `helper` cargo feature layers per-thread shared
runtimes, a seat and a helper thread on top of `Runtime<Shared>`, for callers (such as zbus) that
drive their work with one `block_on` call per operation rather than one for the whole program. It
is a standalone crate with no dependency on any particular application; it was extracted from
zbus's built-in runtime, which now depends on it.

Two default cargo features, each usable without the other, split the crate. `runtime` is the
runtime above — `Runtime`, `LocalRuntime`, `SharedRuntime`, tasks, timers, I/O registrations
and `Async`, the async handle of any source the reactor can watch — and is what needs `rustix`
on unix and `windows-sys` on Windows, and `futures-core` and `futures-io` everywhere, for the
`Stream` impl of its interval timer and the `AsyncRead` and `AsyncWrite` impls of `Async`.
`event` is `Event` and `EventListener`, a notification that tasks wait for, which needs
no runtime and works under any executor. A crate that only notifies builds zruntime with
`default-features = false, features = ["event"]` and gets none of the runtime (zbus, whatever
runtime it runs on, adds `broadcast` and `lock` to that); one that only runs tasks leaves `event`
out. `helper` implies `runtime`, and `tracing`, also a default feature, makes the runtime log
through the `tracing` crate.

A non-default `broadcast` feature, which implies `event` and adds a `futures-core` dependency,
gives `zruntime::broadcast`: an async multi-producer multi-consumer broadcast channel, moved here
from async-broadcast. It is built on `Event`, needs no runtime and works under any executor.

A non-default `mpmc` feature, which implies `event` and adds a `futures-core` dependency, gives
`zruntime::mpmc`: an async multi-producer multi-consumer channel, bounded or unbounded, each of
whose messages one receiver gets. It is built on `Event`, needs no runtime and works under any
executor.

A non-default `lock` feature, which implies `event` and adds no dependency, gives `zruntime::lock`:
an async `Mutex` and `RwLock` whose guards may be held across an await, moved here from zbus. They
are built on `Event`, need no runtime and work under any executor. Each hands out guards that borrow
it and, through `lock_arc`, `read_arc` and `write_arc` on an `Arc` of it, guards that hold a clone
of that `Arc` instead, to be kept in a struct or moved into a spawned task. The module also gives a
`Semaphore`, a lock that up to a set number of tasks may hold at once, built the same way, with
`acquire` and `acquire_arc`, a `Barrier` that tasks wait at for each other, and a `OnceCell` that is
set once, by an initialiser that may await, and that tasks can wait for the value of.

A non-default `unblock` feature, which adds `futures-io` and `futures-core` dependencies, gives
`zruntime::unblock`: a piece of blocking work run on a pool of threads kept for it, out of the way
of the async tasks, and a future of its outcome, moved here from zbus. It also gives
`zruntime::Unblock`, as smol has `smol::Unblock`: an adapter that gives a blocking I/O handle (a
file, the standard input, an iterator) `futures-io`'s `AsyncRead`, `AsyncWrite` and `AsyncSeek`
and `futures-core`'s `Stream` by running each operation on it as such work. Both need no runtime
and work under any executor.

A non-default `fs` feature, which implies `unblock` and `lock` and adds no dependency of its own,
gives `zruntime::fs`: async access to the filesystem in the shape of `std::fs`, as smol has in
`smol::fs` (the async-fs crate), with each operation run as blocking work through `unblock`, and a
`File` built on `Unblock`. It needs no runtime and works under any executor.

A non-default `tcp` feature, which implies `runtime` and adds the `socket2` dependency, gives the
`zruntime::net` module's TCP sockets, `TcpListener` and `TcpStream`, as smol has in `smol::net`.
They run on a `Runtime` of either flavour, connect without blocking the thread, take socket
addresses rather than host names, and implement `futures-io`'s `AsyncRead` and `AsyncWrite`. Their
non-blocking connect is carried over from zbus, whose own socket layer stays there: it drives its
sockets through whichever runtime a connection runs on. A non-default `udp` feature, which implies
`runtime` and adds no dependency, gives the module's `UdpSocket`, likewise. A non-default `unix`
feature, which implies `runtime` and adds the `socket2` dependency and `rustix`'s `net` feature,
gives the `zruntime::net::unix` module's `UnixListener`, `UnixStream` and `UnixDatagram`, on unix
only: on Windows it builds nothing.

It is a single crate at the repository root — not a workspace.

## Common Development Commands

### Building and Testing
```bash
# Full test suite, every feature on (the features all but only add code: what they take out is
# the no-op `error!` in `log.rs` and what `spawn` does without `helper`, which the next command
# tests)
cargo test --all-features

# The runtime's tests without `event` and `helper`, the one of `spawn` without `helper` among them
cargo test --no-default-features --features runtime

# Run a single test
cargo test --all-features some_test_name
```

### Code Quality
```bash
# Format code (requires nightly)
cargo +nightly fmt --all

# Lint with clippy
cargo clippy --all-targets --all-features -- -D warnings

# Check the runtime, Event, the two channels, the locks, unblock, fs and each family of socket
# built alone: `--all-features` cannot show that each builds without the others, and leaves out
# the no-op `error!` in `log.rs` that replaces `tracing`'s
cargo check --no-default-features --features runtime
cargo check --no-default-features --features event
cargo check --no-default-features --features broadcast
cargo check --no-default-features --features mpmc
cargo check --no-default-features --features lock
cargo check --no-default-features --features unblock
cargo check --no-default-features --features fs
cargo check --no-default-features --features tcp
cargo check --no-default-features --features udp
cargo check --no-default-features --features unix

# Run what needs no runtime (Event, the locks, the two channels, unblock) under Miri, as CI does;
# the runtime's poller on Linux keeps a timerfd, which Miri does not run, and fs reaches the
# filesystem, which Miri's isolation keeps it from
cargo +nightly miri test --no-default-features --features lock,broadcast,mpmc,unblock

# Run the locks' waiting paths on `wasm32-unknown-unknown`, which has no clock, in Node, as CI
# does
cargo build --release --target wasm32-unknown-unknown --manifest-path ci/wasm/Cargo.toml
node ci/wasm/run.mjs ci/wasm/target/wasm32-unknown-unknown/release/zruntime_wasm_check.wasm

# Check cross-platform compatibility
cargo check --all-features --target x86_64-pc-windows-gnu
cargo check --all-features --target x86_64-apple-darwin
cargo check --all-features --target x86_64-unknown-freebsd
cargo check --all-features --target x86_64-unknown-netbsd
cargo check --all-features --target aarch64-linux-android
```

### Documentation
```bash
cargo doc --all-features
```

### Benchmarks
```bash
# Run benchmarks (the runtime, spawn and connection ones need the helper feature: they measure the
# block_on-per-operation case; the event and connection ones need the lock feature, which implies
# Event)
cargo bench --features helper,lock

# The broadcast channel's benchmark needs the broadcast feature and no runtime
cargo bench --features broadcast --bench broadcast

# The MPMC channel's benchmark needs the mpmc feature and the default runtime one: its task ids
# run on a LocalRuntime
cargo bench --features mpmc --bench mpmc

# The channel and the mutex between contending threads need the mpmc and lock features
cargo bench --features mpmc,lock --bench contention

# The benchmark of handing work to `unblock` needs the unblock feature and no runtime
cargo bench --features unblock --bench unblock
```

On CodSpeed, `.github/workflows/bench.yml` measures each bench target in one of two jobs: by CPU
simulation, on a GitHub-hosted runner, the targets that run on one thread and wait on no timer or
socket; by the clock, on a CodSpeed macro runner, those that wait on timers, sockets or other
threads. A target holds benchmarks of one kind only, and a new one goes in the list of its job.

## Architecture Overview

```
src/
├── lib.rs        # Public API: Runtime, LocalRuntime, SharedRuntime, Registration, Readiness,
│                 # Interest, Async, Source, Task, Sleep, Timeout, TimedOut, Interval,
│                 # MissedTickBehavior, and the free spawn and spawn_local (runtime feature),
│                 # Event, EventListener (event feature), and the free block_on (helper feature)
├── async_io.rs   # [runtime feature] Async<T, M>: the async handle of any source the reactor
│                 # can watch
├── event.rs      # [event feature] Event/EventListener: a notification tasks wait for, under
│                 # any executor, with no use of the runtime
├── event-only.md # The crate's documentation in a build with `event` but not `runtime`, whose
│                 # README examples it cannot run
├── broadcast.rs  # [broadcast feature] The async multi-producer multi-consumer broadcast channel,
│                 # built on Event, with no use of the runtime
├── mpmc.rs       # [mpmc feature] The async multi-producer multi-consumer channel, each
│                 # message to one receiver, built on Event, with no use of the runtime
├── fs/           # [fs feature] Async filesystem access, built on unblock and Unblock, with no
│                 # use of the runtime
├── lock/         # [lock feature] Mutex, RwLock, Semaphore, Barrier and OnceCell, built on Event,
│                 # with no use of the runtime
├── log.rs        # [runtime feature] Logging through `tracing`, or nothing without it
├── mode.rs       # [runtime feature] The sealed `Mode` trait: what Local/Shared build state from
├── net/          # [tcp, udp, unix features] Async sockets, each built on `Async<T, M>`:
│                 # connect.rs, the non-blocking connect; tcp.rs; udp.rs; unix.rs, the
│                 # `net::unix` module (unix only)
├── runtime.rs    # [runtime feature] Core<M>: scheduler + reactor + driving state of a Runtime<M>
├── scheduler.rs  # [runtime feature] Holds spawned tasks and hands them out to be polled
├── reactor.rs    # [runtime feature] Watches registered I/O sources and keeps timers
├── time.rs       # [runtime feature] The timers a runtime hands out: Sleep, Timeout and Interval
├── poll/         # [runtime feature] The OS pollers: epoll.rs (Linux, Android), kqueue.rs (the
│                 # BSDs), select.rs (Apple), generic.rs (poll(2), any other unix), windows.rs
│                 # (Winsock's select); list.rs, the list the last three keep; pipe.rs, the
│                 # channel that breaks a unix wait
├── driver.rs     # [helper feature] the seat/helper-thread machinery, per-thread registries
├── unblock/      # [unblock feature] Blocking work on a pool of threads (pool.rs), and the
│                 # Unblock adapter of a blocking I/O handle (io.rs), with no use of the runtime
└── tests/        # core.rs: Local + Shared, runtime feature; event.rs: Event, event feature;
                  # broadcast.rs: the broadcast channel, broadcast feature; mpmc.rs: the MPMC
                  # channel, mpmc feature; lock/: the locks, lock feature; helper.rs: helper
                  # feature; unblock/: unblock, its pool and Unblock, unblock feature; fs.rs: fs,
                  # fs feature; net/: the sockets, each family under its own feature
```

### Key Design Patterns

**Two flavours over a sealed `Mode`**: `Runtime<M: Mode = Local>` is generic over how it shares
its state. `Local` builds it from `Rc`/`RefCell`/`Cell`, stays on its thread, and runs any
`'static` future; `Shared` builds it from `Arc`/`Mutex`, is `Send + Sync` by auto traits alone (the
runtime has no `unsafe impl`: the crate's only ones are the `lock` module's `Send` and `Sync`
impls), and runs `Send` futures. The sealed trait in `mode.rs` is the single place that names which:
everything else in `scheduler.rs`, `reactor.rs` and `runtime.rs` is written once, generic over `M`.

**The wake path is the only cross-thread part of a `Local` runtime**: a `Waker` must be
`Send + Sync` even for a `Local` task, whose future is not, so a task's waker never holds the
future — it holds an id into the scheduler's task map plus an `Arc<Remote>` (an atomic-backed
ready queue and the poller's notify half). This is the one place a `Local` runtime pays for
atomics; everywhere else it is a plain `Rc`/`RefCell`/`Cell` structure. On a `Shared` runtime the
waker lives inside the task's join state instead, saving an allocation per task.

**Single-threaded, seat-based driving (the `helper` feature)**: on top of `Runtime<Shared>`,
`driver.rs` gives a runtime made through the per-thread registries (`SharedRuntime::current()`,
the free `block_on`) a seat: only one thread at a time runs its scheduler and reactor. A thread
calling `block_on` takes the seat for as long as it is inside the call; when nothing is inside a
`block_on`, a helper thread takes the seat instead, so that spawned tasks, timers and registered
I/O still make progress. The helper parks as soon as a `block_on` arrives to take the seat back,
and puts itself down once it finds nothing left to run, watch or time. A `Runtime::new()` runtime
never has a seat or a helper: it runs only while some thread is inside `block_on` on it.

**Parallelism through several runtimes**: a runtime's scheduler and reactor are only ever run by
one thread at a time, so all of its tasks share one core, and that stays so: several threads
taking tasks from one runtime's scheduler, as smol's `Executor::run` does, was decided against.
Work that needs more cores runs on several runtimes, one per thread, spread over them through an
`mpmc` channel whose receiver each thread holds a clone of. The README's "Running on several
threads" section and the `mpmc` module's "Spreading work over threads" example document this.

**I/O integration**: `Runtime::register` erases the source into the mode's `SourcePtr` (`Rc<dyn
AsFd>` / `Arc<dyn AsFd + Send + Sync>` on unix, `AsSocket` on Windows) and returns a
`Registration` whose `poll_io` drives an arbitrary operation against
`Interest::Readable`/`Writable` readiness, retrying on `WouldBlock`, and whose `ready` hands out a
`Readiness` future that waits for readiness alone. The source's descriptor is read once, at
registration, and kept beside it in the map: the poller watches that and calls no `as_fd` or
`as_socket`, so it runs no code of a source's. A runtime watches a descriptor through one
registration at a time, and turns a second away with `AlreadyExists`. Each direction of a source
keeps one waker for `poll_io`, which the next operation to wait takes the place of, and a map of
`Readiness` waits under ids of their own, any number of which may wait at once: readiness wakes
them all and takes them out of the map, which is how a `Readiness` tells readiness from a poll
anything else caused.

**Telling the poller what changes**: the reactor tells the poller (`src/poll/`) of each change to
what a source is watched for, under the map's lock, rather than handing it every source on each
wait: `add` as a source is registered, `modify` as the directions it is watched in change, and
`delete` as its registration goes. A direction is armed as soon as a waiter is stored in it, and
disarmed lazily: a source a wait found ready, or a `Readiness` gave up on, is listed in
`Sources::stale` and looked at before the next wait, so that a task that stores its waker again
before then costs the poller nothing. `Poller::LIVE` says whether a change reaches a wait under
way (a poller that keeps what it watches in the kernel) or only the next one (one that keeps a
`poll/list.rs` list and copies it as a wait starts). For the latter, arming breaks the wait under
way, and a source whose registration goes while a wait runs is kept in `Sources::retired`, so
that its descriptor stays open, until that wait returns.

**`Async`, the handle of a source**: `Async<T, M>` is a `Registration` and the mode's `Ptr<T>`
(`Rc`/`Arc`) of the source, of which the reactor holds a clone through the sealed
`IntoSource::source_ptr`; its I/O runs on the `Ptr<T>`. The constructors are written once, generic
over `M`, with `T: Source<M>`: a sealed public bound, whose per-flavour blanket impls go through
the private supertrait `IntoSource<M>` (`AsFd`/`AsSocket` and `'static`, and `Send + Sync` for
`Shared`). A `new` in an `impl` of each flavour instead would make `Async::new` ambiguous (E0034),
as the compiler looks the item up before it has chosen the flavour. No `&mut T` is handed out, the
reactor sharing the pointer, so the I/O traits are there only where `&T` implements
`Read`/`Write`, for `Async` and `&Async` both. `readable`, `writable`, `read_with` and `write_with`
wait through `Readiness`, any number of tasks at once; `poll_read_with`, `poll_write_with` and the
traits keep one waiting task per direction, through `poll_io`. `into_inner` ends the watch and
takes the source out of the pointer: on `Local` nothing else holds it by then, and on `Shared` it
yields until a wait under way on another thread returns and the reactor lets go of the source it
kept for that wait, which it does clear of every lock and before it wakes anyone.

**Cooperative cancellation**: dropping a `Task` cancels it; `Task::detach` lets it run to
completion unobserved; `Task::is_finished` tells, without polling it, whether it has ended.
`Task::cancel` cancels it and hands back a future that resolves once the task's future is gone:
to its output, where the task had finished first. A cancelling handle stops the task as a drop
does, but leaves the outcome for the guard in the task's wrapper to settle, which it does only
once the future has been dropped.

**Spawning without a runtime handle**: the free `spawn` and `spawn_local` find their runtime in a
per-thread record of the runtime of their flavour that the thread drives (`set_driven` and
`driven` of the sealed trait, a `Weak` in a thread-local of each impl), which `set_driving` in
`runtime.rs` writes together with the `NonNull<Remote>` marker that wakes read, for `block_on` on
a runtime with no seat and for the seat of one with a seat alike. Like the marker, the record has
no destructor (its `Weak` is in a `ManuallyDrop`, and every writer clears it as the thread stops
driving), so that a `block_on` run from a thread-local's destructor finds it. `spawn` falls back,
with `helper`, on what `SharedRuntime::current` resolves to, which differs from the record for a
`Runtime::new` runtime being driven: `driver::driven` leaves a runtime with no seat out. A runtime
that fallback brings into being inside the free `block_on` is held by that call from then on
(`driver::current_to_spawn_on`), as nothing else would hold it, in a thread-local of its own
(`HELD`) that, like the records, has no destructor. Both name the task by `Location::caller()`
through `scheduler::Name`, which never allocates.

**Sockets (the `net` features)**: every socket is an `Async<T, M>`: the std socket, non-blocking,
in the mode's `Ptr` (`Rc`/`Arc`), of which the reactor holds a clone through the sealed
`Mode::source_ptr`, and the `Registration` its I/O waits on, made by the crate-private
`Async::from_nonblocking`. The async methods (`accept`, `peek`, `recv`, `send` and the like) wait
through `read_with` and `write_with`, which wait on `Registration::ready`, so any number of tasks
wait in them at once. The poll-based traits and the `Incoming` streams go through `poll_read_with`
and `poll_write_with` instead, and so through `Registration::poll_io`, which keeps one waker per
direction: a socket serves one waiting reader and one waiting writer at a time through them, and
neither kind of wait takes the place of the other. A stream implements `futures-io`'s traits for
`&Stream` as well, which is how a reader and a writer share it. No socket is `Clone`.

## Development Guidelines

- **MSRV**: 1.88.0
- **Commit style**: Emoji prefix only, no package prefix — this is a single crate (e.g.,
  "🐛 Fix timer rounding").
- **Changelog**: `CHANGELOG.md` is managed by [release-plz] — do **not** hand-edit it. Write a
  good commit message (conventional-commits-ish) and release-plz will generate the entry at
  release time.
- **Features**: `runtime` and `event` each build without the other. Nothing in `event.rs` may reach
  into the runtime, and a test of the runtime that uses an `Event` is gated with
  `#[cfg(feature = "event")]` (`cargo test --no-default-features --features runtime` builds the
  tests without it). `broadcast`, `mpmc` and `lock` imply `event` and, like it, must not reach
  into the runtime; nor may `unblock`, which needs neither, or `fs`, which implies `unblock` and
  `lock`. `tcp`, `udp` and `unix` imply `runtime`, and `unix` builds nothing on Windows. CI builds
  and tests everything with every feature on, tests `runtime` alone (for what `spawn` does without
  `helper`), and checks `runtime`, `event`, `broadcast`, `mpmc`, `lock`, `unblock`, `fs`, `tcp`,
  `udp` and `unix` each alone.
- **Testing**: The test suite needs no external services (no D-Bus, no network beyond loopback).
- **Cross-platform**: Validate changes work on Linux, Windows, macOS (and ideally the BSDs and
  Android, which CI also checks).
- **Dependencies**: Keep this crate's own dependency footprint small; it is meant to be pulled in
  by other crates (such as zbus) that want an async runtime with no external deps of their own.

[release-plz]: https://release-plz.ieni.dev/

## Key Files for Understanding

- `src/lib.rs`: Public API and the `Runtime` handle
- `src/mode.rs`: The sealed `Mode` trait behind `Local`/`Shared`
- `src/runtime.rs`: `Core<M>`, the scheduler + reactor + driving state `Runtime<M>` owns
- `src/driver.rs`: [helper feature] the free `block_on` and the seat/helper-thread machinery
- `src/reactor.rs`: I/O readiness and timers
- `src/async_io.rs`: `Async<T, M>`, the async handle of any source the reactor can watch, built on
  a `Registration` and the `Ptr<T>` of the source it shares with the reactor
- `src/scheduler.rs`: Task storage and polling
- `src/time.rs`: The timers a runtime hands out, each built on a deadline its reactor keeps
- `src/event.rs`: `Event`/`EventListener`, a queue of listeners in a slab behind one mutex, and an
  atomic word beside it that lets a notification that would reach nobody skip the mutex
- `src/lock/`: [lock feature] `Mutex`, `RwLock` and `Semaphore`, the async locks built on `Event`,
  with guards that borrow the lock and guards that hold an `Arc` of it, and `Barrier` and
  `OnceCell`, built on `Event` too
- `src/unblock/`: [unblock feature] `unblock`, blocking work on a pool of threads (`pool.rs`), and
  `Unblock`, the async adapter of a blocking I/O handle built on it (`io.rs`)
- `src/fs/`: [fs feature] Async filesystem access: the free functions (`mod.rs`), `File` on
  `Unblock` (`file.rs`), `ReadDir`/`DirEntry`/`DirBuilder` (`dir.rs`), `OpenOptions`
  (`options.rs`), and the platform extension traits (`unix.rs`, `windows.rs`)
- `src/net/connect.rs`: [tcp, unix features] The non-blocking connect, and Winsock's check of one
