# zruntime

With the `event` feature, zruntime provides notifications that work with any executor.
Call [`Event::listen`] to create an [`EventListener`], then await the listener for a
notification. This build does not include the runtime APIs.

Whoever changes what a task is waiting for — releases a lock, fills a queue, closes a connection —
notifies the event with [`Event::notify`], which wakes as many of the tasks listening as it is
asked to, oldest first. A listener is a plain future, woken through the waker of whatever polled
it last, and an event may be notified from any thread.

## Features

* `event`, on in this build: [`Event`] and [`EventListener`].
* `runtime`, off in this build: the runtime itself, `Runtime`, `LocalRuntime` and `SharedRuntime`,
  with the tasks, timers and I/O registrations built on them, and `Async`, the async handle of any
  source they can watch, the `rustix` and `windows-sys` crates it polls with, and the
  `futures-core` and `futures-io` crates.
* `tracing`: the runtime logs through the `tracing` crate. Without the runtime, it has nothing to
  log.
* `helper`: per-thread shared runtimes, and the helper thread that keeps their work moving in
  between `block_on` calls. It implies `runtime`.
* `broadcast`: the `broadcast` module, an async multi-producer multi-consumer broadcast channel
  built on [`Event`]; it implies `event`.
* `mpmc`: the `mpmc` module, an async multi-producer multi-consumer channel, each of whose
  messages one receiver gets, built on [`Event`]; it implies `event`.
* `lock`: the `lock` module, an async `Mutex`, `RwLock` and `Semaphore`, whose guards a task may
  hold across an await, a `Barrier` that tasks wait at for each other, and a `OnceCell` that is set
  once, by an initialiser that may await, built on [`Event`]; it implies `event`.
* `unblock`: `unblock`, which runs a piece of blocking work on a pool of threads kept for it and
  hands back a future of its outcome, and `Unblock`, an adapter that gives a blocking I/O handle
  the async I/O traits of `futures-io` that way; it needs no runtime, and brings the `futures-io`
  and `futures-core` crates.
* `fs`: the `fs` module, async access to the filesystem, with each operation run as blocking work
  on `unblock`'s pool; it implies `unblock` and `lock`, and needs no runtime.
* `tcp`: the `net` module's TCP sockets, `TcpListener` and `TcpStream`; it implies `runtime`,
  and brings the `socket2` crate.
* `udp`: the `net` module's `UdpSocket`; it implies `runtime`.
* `unix`: the `net` module's `unix` module, with unix-domain sockets, on unix only; it implies
  `runtime`, and brings the `socket2` crate and `rustix`'s `net` feature.

`event`, `runtime` and `tracing` are on by default, and `event` and `runtime` each build without
the other: a crate that only notifies depends on zruntime with
`default-features = false, features = ["event"]`. [The crate's documentation on
docs.rs](https://docs.rs/zruntime) is built with every feature on, and covers the runtime as well.
