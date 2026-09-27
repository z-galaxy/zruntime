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

It is a single crate at the repository root — not a workspace.

## Common Development Commands

### Building and Testing
```bash
# Full test suite, default features only (Local runtime, tracing)
cargo test

# Full test suite, every feature (adds the helper-layer tests)
cargo test --all-features

# Test with default features off (no tracing)
cargo test --no-default-features

# Test the helper feature alone
cargo test --no-default-features --features helper

# Run a single test
cargo test some_test_name
```

### Code Quality
```bash
# Format code (requires nightly)
cargo +nightly fmt --all

# Lint with clippy
cargo clippy -- -D warnings
cargo clippy --all-features -- -D warnings
cargo clippy --no-default-features --features helper -- -D warnings

# Check cross-platform compatibility
cargo check --target x86_64-pc-windows-gnu
cargo check --target x86_64-apple-darwin
cargo check --target x86_64-unknown-freebsd
cargo check --target x86_64-unknown-netbsd
cargo check --target aarch64-linux-android
```

### Documentation
```bash
cargo doc --all-features
cargo doc --no-default-features
```

### Benchmarks
```bash
# Run benchmarks (need the helper feature: they measure the block_on-per-operation case)
cargo bench --features helper
```

## Architecture Overview

```
src/
├── lib.rs        # Public API: Runtime, LocalRuntime, SharedRuntime, Registration, Interest,
│                 # Task, Sleep, and (helper feature) the free block_on
├── mode.rs       # The sealed `Mode` trait: what Local/Shared build their shared state from
├── runtime.rs    # Core<M>: scheduler + reactor + driving state, pointed to by a Runtime<M>
├── scheduler.rs  # Holds spawned tasks and hands them out to be polled
├── reactor.rs    # Watches registered I/O sources and keeps timers
├── poll/         # The OS polling primitive (poll(2) on unix, select on Windows)
├── driver.rs     # [helper feature] the seat/helper-thread machinery, per-thread registries
└── tests/        # core.rs: Local + Shared, always compiled; helper.rs: the helper feature
```

### Key Design Patterns

**Two flavours over a sealed `Mode`**: `Runtime<M: Mode = Local>` is generic over how it shares
its state. `Local` builds it from `Rc`/`RefCell`/`Cell`, stays on its thread, and runs any
`'static` future; `Shared` builds it from `Arc`/`Mutex`, is `Send + Sync` by auto traits alone (no
`unsafe impl` anywhere in the crate), and runs `Send` futures. The sealed trait in `mode.rs` is
the single place that names which: everything else in `scheduler.rs`, `reactor.rs` and
`runtime.rs` is written once, generic over `M`.

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

## Development Guidelines

- **MSRV**: 1.87.0
- **Commit style**: Emoji prefix only, no package prefix — this is a single crate (e.g.,
  "🐛 Fix timer rounding").
- **Changelog**: `CHANGELOG.md` is managed by [release-plz] — do **not** hand-edit it. Write a
  good commit message (conventional-commits-ish) and release-plz will generate the entry at
  release time.
- **Testing**: The test suite needs no external services (no D-Bus, no network).
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
