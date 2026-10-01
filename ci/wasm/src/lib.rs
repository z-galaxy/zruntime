//! Runs zruntime's locks on `wasm32-unknown-unknown`, where the standard library has no clock
//! and `Instant::now` panics, which traps the module.
//!
//! Each exported function drives the futures of a lock by hand, with a waker that goes nowhere,
//! through the paths that start a wait, and returns 0 where every step went as expected, or the
//! number of the first step that did not. `run.mjs` runs them all in Node.

use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use zruntime::lock::{Mutex, RwLock};

/// A `lock` that has to wait for a mutex waits, and gets it once the holder lets go.
#[unsafe(no_mangle)]
pub extern "C" fn contended_mutex() -> u32 {
    let mutex = Mutex::new(());
    let holder = mutex.try_lock().expect("a new mutex is free");
    let mut waiter = pin!(mutex.lock());
    if !poll(waiter.as_mut()).is_pending() {
        return 1;
    }
    drop(holder);
    if !poll(waiter.as_mut()).is_ready() {
        return 2;
    }

    0
}

/// A `read` that has to wait for a writer waits, and enters once the writer lets go.
#[unsafe(no_mangle)]
pub extern "C" fn contended_read() -> u32 {
    let lock = RwLock::new(());
    let writer = lock.try_write().expect("a new lock is free");
    let mut reader = pin!(lock.read());
    if !poll(reader.as_mut()).is_pending() {
        return 1;
    }
    drop(writer);
    if !poll(reader.as_mut()).is_ready() {
        return 2;
    }

    0
}

/// A `write` that has to wait for a reader waits, and gets the lock once the reader lets go.
#[unsafe(no_mangle)]
pub extern "C" fn contended_write() -> u32 {
    let lock = RwLock::new(());
    let reader = lock.try_read().expect("a new lock is free");
    let mut writer = pin!(lock.write());
    if !poll(writer.as_mut()).is_pending() {
        return 1;
    }
    drop(reader);
    if !poll(writer.as_mut()).is_ready() {
        return 2;
    }

    0
}

/// A `lock` that waited and then found the mutex taken holds newcomers back: with no clock to tell
/// how long it waited, the first such wake is enough.
#[unsafe(no_mangle)]
pub extern "C" fn starved_lock() -> u32 {
    let mutex = Mutex::new(());
    let holder = mutex.try_lock().expect("a new mutex is free");
    let mut waiter = pin!(mutex.lock());
    if !poll(waiter.as_mut()).is_pending() {
        return 1;
    }
    drop(holder);
    let Some(barging) = mutex.try_lock() else {
        return 2;
    };
    if !poll(waiter.as_mut()).is_pending() {
        return 3;
    }
    drop(barging);
    if mutex.try_lock().is_some() {
        return 4;
    }
    let Poll::Ready(guard) = poll(waiter.as_mut()) else {
        return 5;
    };
    drop(guard);
    if mutex.try_lock().is_none() {
        return 6;
    }

    0
}

/// Polls `future` once, with a waker that goes nowhere.
fn poll<F>(future: std::pin::Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    future.poll(&mut Context::from_waker(Waker::noop()))
}
