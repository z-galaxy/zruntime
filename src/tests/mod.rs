//! Tests of a runtime as a whole, of the event that tasks wait for, of the broadcast channel and
//! the locks built on that event, of blocking work run on a thread of its own, and of the sockets
//! a runtime watches.
//!
//! What a runtime does on its own — spawn, join, cancel, time, watch and drive — is tested in
//! the `core` module, in both flavours wherever the test means the same in each. An event and its
//! listeners, which need no runtime, are tested in the `event` module. What the type system is to
//! rule out — a local runtime's handles leaving their thread, a shared runtime taking a future
//! that could not follow it to another, a lock handing its value to a thread that could not own
//! it, the future of blocking work doing the same with the value it resolves to — can only be
//! shown by code that does not compile, so it is shown here, by the doc tests of the items below.
//! The broadcast channel and the locks, built on the event, are tested in the `broadcast` and
//! `lock` modules. Blocking work, which needs neither the runtime nor the event, is tested in the
//! `unblock` module. The sockets of the `net` module are tested in the `net` module, one family of
//! them to a module there, in both flavours wherever the test means the same in each.
//!
//! The runtime and the event are features of their own, and a build may have either without the
//! other. A test of the runtime that uses an event to know that something happened is built only
//! where both are. The broadcast channel and the locks are features of their own as well, each
//! built on the event and with no use of the runtime.

#[cfg(all(test, feature = "broadcast"))]
mod broadcast;
#[cfg(all(test, feature = "runtime"))]
mod core;
#[cfg(all(test, feature = "event"))]
mod event;
#[cfg(all(test, feature = "helper"))]
mod helper;
#[cfg(all(test, feature = "lock"))]
mod lock;
#[cfg(all(test, feature = "tcp"))]
mod net;
#[cfg(all(test, feature = "unblock"))]
mod unblock;

/// A local runtime's handle stays on its thread: it is neither `Send`...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::LocalRuntime>();
/// ```
///
/// ...nor `Sync`...
///
/// ```compile_fail
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::LocalRuntime>();
/// ```
///
/// ...while a shared runtime's handle is both, which is what says the two checks above fail for
/// the reason they were written for and no other.
///
/// ```
/// fn sent_and_shared<T>()
/// where
///     T: Send + Sync,
/// {
/// }
///
/// sent_and_shared::<zruntime::SharedRuntime>();
/// ```
#[cfg(all(doctest, feature = "runtime"))]
struct LocalRuntimeStaysOnItsThread;

/// What is built on a local runtime stays on its thread as well: a task handle...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::Task<(), zruntime::Local>>();
/// ```
///
/// ...a timer...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::Sleep<zruntime::Local>>();
/// ```
///
/// ...and a registration...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::Registration<zruntime::Local>>();
/// ```
///
/// ...while the same three built on a shared runtime may go anywhere.
///
/// ```
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::Task<(), zruntime::Shared>>();
/// sent::<zruntime::Sleep<zruntime::Shared>>();
/// sent::<zruntime::Registration<zruntime::Shared>>();
/// ```
#[cfg(all(doctest, feature = "runtime"))]
struct LocalHandlesStayOnTheirThread;

/// A shared runtime's tasks may be polled on any thread, so it turns a future away that could not
/// follow it there...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// let runtime = zruntime::SharedRuntime::new().unwrap();
/// let local = Rc::new(7);
/// let task = runtime.spawn("a task holding an `Rc`", async move { *local });
///
/// assert_eq!(runtime.block_on(task).unwrap(), 7);
/// ```
///
/// ...which a local runtime takes as it is.
///
/// ```
/// use std::rc::Rc;
///
/// let runtime = zruntime::LocalRuntime::new().unwrap();
/// let local = Rc::new(7);
/// let task = runtime.spawn("a task holding an `Rc`", async move { *local });
///
/// assert_eq!(runtime.block_on(task).unwrap(), 7);
/// ```
#[cfg(all(doctest, feature = "runtime"))]
struct SharedRuntimeTakesSendFuturesOnly;

/// A lock hands its value to whichever thread holds a guard, so a lock may go to another thread, or
/// be shared with one, only where its value may: a mutex of an `Rc`, which cannot be sent, is not
/// `Send`...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::lock::Mutex<Rc<u8>>>();
/// ```
///
/// ...nor `Sync`, since sharing it lets one thread after another take the `Rc`...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::lock::Mutex<Rc<u8>>>();
/// ```
///
/// ...and a readers-writer lock of an `Rc` is not `Send` either...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::lock::RwLock<Rc<u8>>>();
/// ```
///
/// ...while a readers-writer lock of a `Cell`, which may be sent, is not `Sync`, unlike a mutex of
/// one: its readers would share the cell, which a `Cell` does not allow.
///
/// ```compile_fail
/// use std::cell::Cell;
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::lock::RwLock<Cell<u8>>>();
/// ```
///
/// A guard is as `Send` and `Sync` as the reference it stands for: a `&mut T` for the guard of a
/// mutex or a write guard, and a `&T` for a read guard. A mutex guard of an `Rc` is not `Send`, for
/// it would hand the `Rc` to the thread it goes to...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::lock::MutexGuard<'static, Rc<u8>>>();
/// ```
///
/// ...and one of a `Cell` is not `Sync`, for sharing the guard shares the `&Cell` it dereferences
/// to...
///
/// ```compile_fail
/// use std::cell::Cell;
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::lock::MutexGuard<'static, Cell<u8>>>();
/// ```
///
/// ...a read guard yields a `&T` and nothing more, so it is `Send` only where `T` is `Sync`, which
/// a `Cell` is not...
///
/// ```compile_fail
/// use std::cell::Cell;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::lock::RwLockReadGuard<'static, Cell<u8>>>();
/// ```
///
/// ...and it is `Sync` only where `T` is, for sharing the guard shares that `&T`...
///
/// ```compile_fail
/// use std::cell::Cell;
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::lock::RwLockReadGuard<'static, Cell<u8>>>();
/// ```
///
/// ...a write guard of an `Rc` is not `Send`, for it would hand the `Rc` to the thread it goes to,
/// as a mutex guard of one does...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::lock::RwLockWriteGuard<'static, Rc<u8>>>();
/// ```
///
/// ...and a write guard of a `Cell`, which may be sent, is not `Sync`, for the same reason as a
/// mutex guard of one.
///
/// ```compile_fail
/// use std::cell::Cell;
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::lock::RwLockWriteGuard<'static, Cell<u8>>>();
/// ```
///
/// Where the value's own bounds hold, the locks and their guards are `Send` and `Sync` as far as
/// the value allows, and the futures that wait for a lock are `Send`: which says the checks above
/// fail for the reason they were written for and no other. A value that is `Sync` without being
/// `Send`, as a `std::sync::MutexGuard` is, tells a guard that is `Sync` where `T` is `Sync` apart
/// from one that would ask for `T` to be `Send` as well.
///
/// ```
/// use std::cell::Cell;
///
/// use zruntime::lock::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// fn sent_and_shared<T>()
/// where
///     T: Send + Sync,
/// {
/// }
///
/// fn sent_future<T>(_: T)
/// where
///     T: Send,
/// {
/// }
///
/// sent_and_shared::<Mutex<Cell<u8>>>();
/// sent::<RwLock<Cell<u8>>>();
/// sent_and_shared::<RwLock<u8>>();
///
/// sent::<MutexGuard<'static, Cell<u8>>>();
/// sent_and_shared::<MutexGuard<'static, u8>>();
/// sent_and_shared::<RwLockReadGuard<'static, u8>>();
/// sent::<RwLockWriteGuard<'static, Cell<u8>>>();
/// sent_and_shared::<RwLockWriteGuard<'static, u8>>();
///
/// shared::<MutexGuard<'static, std::sync::MutexGuard<'static, u8>>>();
/// sent_and_shared::<RwLockReadGuard<'static, std::sync::MutexGuard<'static, u8>>>();
/// shared::<RwLockWriteGuard<'static, std::sync::MutexGuard<'static, u8>>>();
///
/// let mutex = Mutex::new(Cell::new(0u8));
/// let rwlock = RwLock::new(0u8);
/// sent_future(mutex.lock());
/// sent_future(rwlock.read());
/// sent_future(rwlock.write());
/// ```
#[cfg(all(doctest, feature = "lock"))]
struct LocksAreAsSendAndSyncAsTheirValues;

/// The future of blocking work hands the value the work returned from the thread it ran on to
/// whichever thread polls it, so it may be sent to another thread only where that value may be: a
/// future that resolves to an `Rc`, which cannot be sent, is not `Send`...
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::Unblock<Rc<()>>>();
/// ```
///
/// ...while one that resolves to a value that may be sent is `Send` and `Sync` both, even where
/// that value is not `Sync` itself, which says the check above fails for the reason it was
/// written for and no other.
///
/// ```
/// use std::cell::Cell;
///
/// fn sent_and_shared<T>()
/// where
///     T: Send + Sync,
/// {
/// }
///
/// sent_and_shared::<zruntime::Unblock<u32>>();
/// sent_and_shared::<zruntime::Unblock<Cell<u32>>>();
/// ```
#[cfg(all(doctest, feature = "unblock"))]
struct UnblockIsSendAndSyncWhereItsOutcomeIsSend;

/// A local runtime's sockets stay on its thread, as everything built on it does: a stream is not
/// `Send`...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::net::TcpStream<zruntime::Local>>();
/// ```
///
/// ...nor `Sync`...
///
/// ```compile_fail
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::net::TcpStream<zruntime::Local>>();
/// ```
///
/// ...and a listener is not `Send` either...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::net::TcpListener<zruntime::Local>>();
/// ```
///
/// ...nor `Sync`...
///
/// ```compile_fail
/// fn shared<T>()
/// where
///     T: Sync,
/// {
/// }
///
/// shared::<zruntime::net::TcpListener<zruntime::Local>>();
/// ```
///
/// ...and the stream of the connections a listener accepts borrows the listener, and a reference
/// is `Send` only where what it refers to is `Sync`, so that stream is not `Send` either...
///
/// ```compile_fail
/// fn sent<T>()
/// where
///     T: Send,
/// {
/// }
///
/// sent::<zruntime::net::Incoming<'static, zruntime::Local>>();
/// ```
///
/// ...while the same sockets built on a shared runtime may go anywhere, and so may the stream of
/// the connections a listener accepts: which is what says the checks above fail for the reason
/// they were written for and no other.
///
/// ```
/// fn sent_and_shared<T>()
/// where
///     T: Send + Sync,
/// {
/// }
///
/// sent_and_shared::<zruntime::net::TcpStream<zruntime::Shared>>();
/// sent_and_shared::<zruntime::net::TcpListener<zruntime::Shared>>();
/// sent_and_shared::<zruntime::net::Incoming<'static, zruntime::Shared>>();
/// ```
#[cfg(all(doctest, feature = "tcp"))]
struct LocalSocketsStayOnTheirThread;
