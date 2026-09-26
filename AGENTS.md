# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For contribution conventions — commit-message format, atomic commits, code layout, and
more — follow the guidelines in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Project Overview

zruntime is a simple, single-threaded Rust async runtime: a scheduler that holds tasks and hands
them out to be polled, a `poll(2)`/`select` reactor that watches registered I/O sources and keeps
timers, and `block_on` to drive a future to completion on the calling thread. It is a standalone
crate with no dependency on any particular application; it was extracted from zbus's built-in
runtime, which now depends on it.

It is a single crate at the repository root — not a workspace.

## Common Development Commands

### Building and Testing
```bash
# Full test suite
cargo test --all-features

# Test with default features off (no tracing)
cargo test --no-default-features

# Run a single test
cargo test some_test_name
```

### Code Quality
```bash
# Format code (requires nightly)
cargo +nightly fmt --all

# Lint with clippy
cargo clippy -- -D warnings

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
```

## Architecture Overview

```
src/
├── lib.rs        # Public API: block_on, Runtime, Source, Registration, Interest, Task, Sleep
├── scheduler.rs  # Holds spawned tasks and hands them out to be polled
├── reactor.rs    # Watches registered I/O sources and keeps timers
├── poll/         # The OS polling primitive (poll(2) on unix, select on Windows)
└── driver.rs     # block_on and the seat/helper-thread machinery that drives the other two
```

### Key Design Patterns

**Single-threaded, seat-based driving**: only one thread at a time is "in the seat" running the
scheduler and reactor. A thread calling `block_on` takes the seat for as long as it is inside the
call; when nothing is inside a `block_on`, a helper thread takes the seat instead, so that spawned
tasks, timers and registered I/O still make progress. The helper parks as soon as a `block_on`
arrives to take the seat back, and puts itself down once it finds nothing left to run, watch or
time.

**Runtime handle**: `Runtime::current()` returns a cheap, cloneable handle to whatever runtime the
calling thread's seat belongs to (creating one if none is alive yet). All of `spawn`, `sleep` and
`register` go through this handle.

**I/O integration**: types that can be polled implement `Source` (a blanket impl over `AsFd` on
unix / `AsSocket` on Windows); `Runtime::register` returns a `Registration` whose `poll_io` drives
an arbitrary operation against `Interest::Readable`/`Writable` readiness.

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
- `src/driver.rs`: `block_on` and the seat/helper-thread machinery
- `src/reactor.rs`: I/O readiness and timers
- `src/scheduler.rs`: Task storage and polling
