# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For contribution conventions — commit-message format, atomic commits, code layout, and
more — follow the guidelines in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Project Overview

zruntime is a simple, single-threaded Rust async runtime: an owned `Runtime<M: Mode = Local>`, a
scheduler that holds tasks and hands them out to be polled, a `poll(2)`/`select` reactor that
watches registered I/O sources and keeps timers, and `Runtime::block_on` to drive a future to
completion on the calling thread. It comes in two flavours: `Local` (the default, aliased
`LocalRuntime`), which stays on the thread it was made on and holds its state in `Rc`/`RefCell`,
and `Shared` (aliased `SharedRuntime`), which may be reached from and driven on any thread and
holds its state in `Arc`/`Mutex`. A non-default `helper` cargo feature layers per-thread shared
runtimes, a seat and a helper thread on top of `Runtime<Shared>`, for callers (such as zbus) that
drive their work with one `block_on` call per operation rather than one for the whole program. It
is a standalone crate with no dependency on any particular application; it was extracted from
zbus's built-in runtime, which now depends on it.

Two default cargo features, each usable without the other, split the crate. `runtime` is the
runtime above — `Runtime`, `LocalRuntime`, `SharedRuntime`, tasks, timers and I/O registrations —
and is what needs `rustix` on unix and `windows-sys` on Windows. `event` is `Event` and
`EventListener`, a notification that tasks wait for, which needs no runtime and works under any
executor. A crate that only notifies builds zruntime with
`default-features = false, features = ["event"]` and gets none of the runtime (zbus, whatever
runtime it runs on, adds `broadcast` and `lock` to that); one that only runs tasks leaves `event`
out. `helper` implies `runtime`, and `tracing`, also a default feature, makes the runtime log
through the `tracing` crate.

A non-default `broadcast` feature, which implies `event` and adds a `futures-core` dependency,
gives `zruntime::broadcast`: an async multi-producer multi-consumer broadcast channel, moved here
from async-broadcast. It is built on `Event`, needs no runtime and works under any executor.

A non-default `lock` feature, which implies `event` and adds no dependency, gives `zruntime::lock`:
an async `Mutex` and `RwLock` whose guards may be held across an await, moved here from zbus. They
are built on `Event`, need no runtime and work under any executor.

A non-default `unblock` feature, which adds no dependency, gives `zruntime::unblock`: a piece of
blocking work run on a thread of its own, out of the way of the async tasks, and a future of its
outcome, moved here from zbus. It needs no runtime and works under any executor.

A non-default `tcp` feature, which implies `runtime` and adds `socket2`, `futures-io` and
`futures-core` dependencies, gives the `zruntime::net` module's TCP sockets, `TcpListener` and
`TcpStream`, as smol has in `smol::net`. They run on a `Runtime` of either flavour, connect
without blocking the thread, take socket addresses rather than host names, and implement
`futures-io`'s `AsyncRead` and `AsyncWrite`. Their non-blocking connect is carried over from zbus,
whose own socket layer stays there: it drives its sockets through whichever runtime a connection
runs on. A non-default `udp` feature, which implies `runtime` and adds no dependency, gives the
module's `UdpSocket`, likewise. A non-default `unix` feature, which implies `runtime` and adds the
same dependencies as `tcp` and `rustix`'s `net` feature, gives the `zruntime::net::unix` module's
`UnixListener`, `UnixStream` and `UnixDatagram`, on unix only: on Windows it builds nothing.

It is a single crate at the repository root — not a workspace.

## Common Development Commands

### Building and Testing
```bash
# Full test suite, every feature on (the features only add code, so this runs all of it)
cargo test --all-features

# Run a single test
cargo test --all-features some_test_name
```

### Code Quality
```bash
# Format code (requires nightly)
cargo +nightly fmt --all

# Lint with clippy
cargo clippy --all-targets --all-features -- -D warnings

# Check the runtime, Event, broadcast, the locks, unblock and each family of socket built alone:
# `--all-features` cannot show that each builds without the others, and leaves out the no-op
# `error!` in `log.rs` that replaces `tracing`'s
cargo check --no-default-features --features runtime
cargo check --no-default-features --features event
cargo check --no-default-features --features broadcast
cargo check --no-default-features --features lock
cargo check --no-default-features --features unblock
cargo check --no-default-features --features tcp
cargo check --no-default-features --features udp
cargo check --no-default-features --features unix

# Run what needs no runtime (Event, the locks, the broadcast channel, unblock) under Miri, as CI
# does; the runtime polls with `ppoll`, which Miri cannot run
cargo +nightly miri test --no-default-features --features lock,broadcast,unblock

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
# Run benchmarks (the runtime and connection ones need the helper feature: they measure the
# block_on-per-operation case; the event and connection ones need the lock feature, which implies
# Event)
cargo bench --features helper,lock

# The broadcast channel's benchmark needs the broadcast feature and no runtime
cargo bench --features broadcast --bench broadcast
```

## Architecture Overview

```
src/
├── lib.rs        # Public API: Runtime, LocalRuntime, SharedRuntime, Registration, Interest,
│                 # Task, Sleep (runtime feature), Event, EventListener (event feature), and
│                 # the free block_on (helper feature)
├── event.rs      # [event feature] Event/EventListener: a notification tasks wait for, under
│                 # any executor, with no use of the runtime
├── event-only.md # The crate's documentation in a build with `event` but not `runtime`, whose
│                 # README examples it cannot run
├── broadcast.rs  # [broadcast feature] The async multi-producer multi-consumer broadcast channel,
│                 # built on Event, with no use of the runtime
├── lock/         # [lock feature] Mutex and RwLock, built on Event, with no use of the runtime
├── log.rs        # [runtime feature] Logging through `tracing`, or nothing without it
├── mode.rs       # [runtime feature] The sealed `Mode` trait: what Local/Shared build state from
├── net/          # [tcp, udp, unix features] Async sockets: io.rs, `Io<T, M>`, a socket and its
│                 # registration, which every socket is built on; connect.rs, the non-blocking
│                 # connect; tcp.rs; udp.rs; unix.rs, the `net::unix` module (unix only)
├── runtime.rs    # [runtime feature] Core<M>: scheduler + reactor + driving state of a Runtime<M>
├── scheduler.rs  # [runtime feature] Holds spawned tasks and hands them out to be polled
├── reactor.rs    # [runtime feature] Watches registered I/O sources and keeps timers
├── poll/         # [runtime feature] The OS polling primitive (poll(2) on unix, select on Windows)
├── driver.rs     # [helper feature] the seat/helper-thread machinery, per-thread registries
├── unblock.rs    # [unblock feature] Blocking work on a thread of its own, with no use of the
│                 # runtime
└── tests/        # core.rs: Local + Shared, runtime feature; event.rs: Event, event feature;
                  # broadcast.rs: the broadcast channel, broadcast feature; lock.rs: the locks,
                  # lock feature; helper.rs: helper feature; unblock.rs: unblock, unblock
                  # feature; net/: the sockets, each family under its own feature
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

**I/O integration**: `Runtime::register` erases the source into the mode's `SourcePtr` (`Rc<dyn
AsFd>` / `Arc<dyn AsFd + Send + Sync>` on unix, `AsSocket` on Windows) and returns a
`Registration` whose `poll_io` drives an arbitrary operation against
`Interest::Readable`/`Writable` readiness, retrying on `WouldBlock`.

**Cooperative cancellation**: dropping a `Task` cancels it; `Task::detach` lets it run to
completion unobserved.

**Sockets (the `net` features)**: every socket is an `Io<T, M>`: the std socket, non-blocking, in
the mode's `Ptr` (`Rc`/`Arc`), of which the reactor holds a clone through the sealed
`Mode::source_ptr`, and the `Registration` its I/O waits on. A registration keeps one waker per
direction, so a socket serves one waiting reader and one waiting writer at a time; a stream
implements `futures-io`'s traits for `&Stream` as well, which is how a reader and a writer share
it. No socket is `Clone`.

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
  tests without it). `broadcast` and `lock` imply `event` and, like it, must not reach into the
  runtime; nor may `unblock`, which needs neither. `tcp`, `udp` and `unix` imply `runtime`, and
  `unix` builds nothing on Windows. CI builds and tests everything with every feature on, and
  checks `runtime`, `event`, `broadcast`, `lock`, `unblock`, `tcp`, `udp` and `unix` each alone.
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
- `src/scheduler.rs`: Task storage and polling
- `src/event.rs`: `Event`/`EventListener`, a queue of listeners in a slab behind one mutex, and an
  atomic word beside it that lets a notification that would reach nobody skip the mutex
- `src/lock/`: [lock feature] `Mutex` and `RwLock`, the async locks built on `Event`
- `src/unblock.rs`: [unblock feature] `unblock`, blocking work on a thread of its own
- `src/net/io.rs`: [tcp, udp, unix features] `Io<T, M>`, the socket and registration every
  socket is built on
- `src/net/connect.rs`: [tcp, unix features] The non-blocking connect, and Winsock's check of one
