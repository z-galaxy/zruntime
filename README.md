# zruntime

[![CI Pipeline Status](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml/badge.svg)](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml)
[![](https://docs.rs/zruntime/badge.svg)](https://docs.rs/zruntime/)
[![](https://img.shields.io/crates/v/zruntime)](https://crates.io/crates/zruntime)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/z-galaxy/zruntime?utm_source=badge)

A simple, single-threaded Rust async runtime:

* a scheduler that holds tasks and hands them out to be polled.
* a `poll(2)`/`select` reactor that watches registered I/O sources and keeps timers, and
* [`block_on`] to drive a future to completion on the calling thread.

## Example

```rust
use std::time::Duration;

use zruntime::Runtime;

let runtime = Runtime::current().expect("a runtime for this thread");
let doubled = zruntime::block_on(async {
    let sleeper = runtime.clone();
    let task = runtime.spawn("double", async move {
        sleeper.sleep(Duration::from_millis(1)).await;
        21 * 2
    });

    task.await.expect("the task did not panic")
});

assert_eq!(doubled, 42);
```

A future built from several tasks, timers and registered sockets runs the same way: [`block_on`]
drives all of it on the calling thread, starting a helper thread only for work still alive once
the call returns.

zruntime logs through [`tracing`], behind the default `tracing` feature; a `default-features =
false` build emits no log events.

## Why?

The project grew out of the need for a single-threaded runtime in [zbus] that it would use by default.
It was split into a separate project so non-zbus users can use it too.

## License

[MIT]

[`block_on`]: https://docs.rs/zruntime/latest/zruntime/fn.block_on.html
[`tracing`]: https://docs.rs/tracing
[zbus]: https://github.com/z-galaxy/zbus
[MIT]: (LICENSE)
