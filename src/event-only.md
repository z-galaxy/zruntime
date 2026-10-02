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
  with the tasks, timers and I/O registrations built on them, and the `rustix` and `windows-sys`
  crates it polls with.
* `tracing`: the runtime logs through the `tracing` crate. Without the runtime, it has nothing to
  log.
* `helper`: per-thread shared runtimes, and the helper thread that keeps their work moving in
  between `block_on` calls. It implies `runtime`.
* `broadcast`: the `broadcast` module, an async multi-producer multi-consumer broadcast channel
  built on [`Event`]; it implies `event`.
* `lock`: the `lock` module, an async `Mutex` and `RwLock`, whose guards a task may hold across an
  await, built on [`Event`]; it implies `event`.
* `unblock`: `unblock`, which runs a piece of blocking work on a thread of its own and hands back
  a future of its outcome; it needs no runtime.
* `tcp`: the `net` module's TCP sockets, `TcpListener` and `TcpStream`; it implies `runtime`,
  and brings the `socket2`, `futures-io` and `futures-core` crates.
* `udp`: the `net` module's `UdpSocket`; it implies `runtime`.

`event`, `runtime` and `tracing` are on by default, and `event` and `runtime` each build without
the other: a crate that only notifies depends on zruntime with
`default-features = false, features = ["event"]`. [The crate's documentation on
docs.rs](https://docs.rs/zruntime) is built with every feature on, and covers the runtime as well.
