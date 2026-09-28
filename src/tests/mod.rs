//! Tests of a runtime as a whole, and of the event that tasks wait for.
//!
//! What a runtime does on its own — spawn, join, cancel, time, watch and drive — is tested in the
//! `core` module, in both flavours wherever the test means the same in each. An event and its
//! listeners, which need no runtime, are tested in the `event` module. What the type system is to
//! rule out — a local runtime's handles leaving their thread, a shared runtime taking a future that
//! could not follow it to another — can only be shown by code that does not compile, so it is shown
//! here, by the doc tests of the items below.

#[cfg(test)]
mod core;
#[cfg(test)]
mod event;
#[cfg(test)]
mod helper;

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
#[cfg(doctest)]
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
#[cfg(doctest)]
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
#[cfg(doctest)]
struct SharedRuntimeTakesSendFuturesOnly;
