//! A cell that is set once, to a value that tasks can wait for.
//!
//! [`OnceCell`] holds a value that is set once, by an initialiser that may be asynchronous, and
//! handed out by reference from then on. What the primitives of this module do and do not promise
//! is said once, in the [module documentation](super).

use std::{
    convert::Infallible,
    fmt,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::OnceLock,
    task::Poll,
};

use super::{Mutex, MutexGuard};
use crate::Event;

/// A cell that is set once, by an initialiser that may be asynchronous, and that tasks can wait
/// for the value of.
///
/// A cell starts out empty. The first value that reaches it stays for as long as the cell lives,
/// and every task is handed a reference to that one value. It is [`std::sync::OnceLock`] for tasks:
/// [`get_or_init`](OnceCell::get_or_init) and [`get_or_try_init`](OnceCell::get_or_try_init) take
/// an initialiser that makes a future, which may await, and [`wait`](OnceCell::wait) suspends the
/// task, rather than block the thread, until another task sets the value.
///
/// # Initialisation
///
/// A task that finds the cell empty initialises it, and the tasks that find it empty meanwhile wait
/// for that to end: one initialiser runs at a time, and the others take their turns in the order an
/// async [`Mutex`] serves its waiters (see [fairness](crate::lock#fairness)). When the initialiser
/// is done, the value is set and every task that waits for it is woken: those that
/// [`wait`](OnceCell::wait) for it, and those that wait for their turn to initialise the cell or to
/// [`set`](OnceCell::set) it. The latter find the value set, and so run no initialiser and set no
/// value, each as soon as it is polled, without waiting for the tasks ahead of it in line.
///
/// An initialiser that fails, by returning an error from
/// [`get_or_try_init`](OnceCell::get_or_try_init), or panics, or is given up by dropping the
/// future that runs it, leaves the cell empty. The tasks waiting to
/// initialise it do not see that as the answer: the next of them runs its own initialiser. A task
/// that only [`wait`](OnceCell::wait)s for the value is not an initialiser, and goes on waiting.
///
/// # Re-entrancy
///
/// The future an initialiser makes must not call [`get_or_init`](OnceCell::get_or_init),
/// [`get_or_try_init`](OnceCell::get_or_try_init) or [`set`](OnceCell::set) on its own cell, nor
/// [`wait`](OnceCell::wait) for its value: it would wait for the initialiser that is running it,
/// which is itself, and the two would wait for each other for good, as the initialisation of a
/// [`std::sync::OnceLock`] from inside its own initialiser does.
///
/// # No poisoning, and `Send` and `Sync`
///
/// A panic in an initialiser does not poison the cell: it leaves the cell empty, and the next
/// initialiser runs, as the [module documentation](crate::lock#no-poisoning) says.
///
/// The cell is built on [`Event`] and needs no runtime: it works under any executor, and may be
/// initialised and waited for from any thread. It is `Send` where `T` is `Send`, and `Sync` where
/// `T` is `Send` and `Sync`. The futures of its methods may move between threads wherever the cell
/// may be shared, which takes a `T` that is `Send` and `Sync`, and wherever the initialiser and the
/// future it makes may move too.
///
/// # Example
///
/// A value that is made when it is first asked for, with the work of making it awaited, and that
/// every later task is handed without any more work:
///
/// ```
/// use futures_lite::future::{block_on, yield_now};
/// use zruntime::lock::OnceCell;
///
/// let cell = OnceCell::new();
///
/// block_on(async {
///     let first = cell
///         .get_or_init(|| async {
///             // Where a value that takes a while to make is made.
///             yield_now().await;
///             String::from("made")
///         })
///         .await;
///     // The cell holds a value already, so this initialiser has no say.
///     let second = cell.get_or_init(|| async { String::from("again") }).await;
///
///     assert_eq!(first, "made");
///     assert!(std::ptr::eq(first, second));
/// });
/// ```
pub struct OnceCell<T> {
    /// The value, once set. Only the holder of `initializing` sets it, so the cell is never found
    /// full by a task that has just checked it empty under that lock.
    value: OnceLock<T>,
    /// Held by the one task that initialises the cell, for as long as its initialiser runs, so
    /// that the initialisers wait for each other. It is let go of as the initialiser's future is
    /// dropped, and so by one that finishes, fails, panics or is given up alike.
    initializing: Mutex<()>,
    /// Where the tasks that wait for the value listen, those that wait for their turn to
    /// initialise the cell or to set it among them, notified once it is set. It is listened to and
    /// notified with the fences of `Event::listen` and `Event::notify`: the value is read with an
    /// `Acquire` load, which does not order it against the event the way a lock would.
    ready: Event,
}

impl<T> OnceCell<T> {
    /// A new cell with no value.
    ///
    /// This is a `const fn`, so a cell can be a `static`.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// static NAME: OnceCell<&str> = OnceCell::new();
    ///
    /// assert_eq!(NAME.get(), None);
    /// assert_eq!(block_on(NAME.get_or_init(|| async { "zruntime" })), &"zruntime");
    /// ```
    pub const fn new() -> Self {
        Self {
            value: OnceLock::new(),
            initializing: Mutex::new(()),
            ready: Event::new(),
        }
    }

    /// The value, if the cell has one.
    ///
    /// This never waits: it is `None` while no value has been set, including while an initialiser
    /// is running.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// let cell = OnceCell::new();
    /// assert_eq!(cell.get(), None);
    ///
    /// block_on(cell.set(1)).expect("nobody set a new cell");
    /// assert_eq!(cell.get(), Some(&1));
    /// ```
    pub fn get(&self) -> Option<&T> {
        self.value.get()
    }

    /// The value, if the cell has one, borrowed mutably.
    ///
    /// No initialiser can be running while the cell is borrowed mutably, so this reaches the value
    /// without a wait.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::OnceCell;
    ///
    /// let mut cell = OnceCell::from(1);
    /// *cell.get_mut().expect("the cell was made with a value") += 1;
    ///
    /// assert_eq!(cell.get(), Some(&2));
    /// ```
    pub fn get_mut(&mut self) -> Option<&mut T> {
        self.value.get_mut()
    }

    /// Takes the value out of the cell, leaving it empty.
    ///
    /// No initialiser can be running while the cell is borrowed mutably, so this takes the value
    /// without a wait. The cell can be set again afterwards.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::OnceCell;
    ///
    /// let mut cell = OnceCell::from(String::from("taken"));
    ///
    /// assert_eq!(cell.take().as_deref(), Some("taken"));
    /// assert_eq!(cell.take(), None);
    /// ```
    pub fn take(&mut self) -> Option<T> {
        self.value.take()
    }

    /// Consumes the cell, handing back its value if it has one.
    ///
    /// The cell is moved, so no initialiser can be left running.
    ///
    /// # Example
    ///
    /// ```
    /// use zruntime::lock::OnceCell;
    ///
    /// assert_eq!(OnceCell::from(1).into_inner(), Some(1));
    /// assert_eq!(OnceCell::<u8>::new().into_inner(), None);
    /// ```
    pub fn into_inner(self) -> Option<T> {
        self.value.into_inner()
    }

    /// Waits for the cell to have a value, and hands out a reference to it.
    ///
    /// This never initialises the cell: it waits for another task to do so, or to
    /// [`set`](OnceCell::set) the value, however many initialisers fail or are given up on the way.
    /// It completes at once where the cell has a value already.
    ///
    /// Dropping the future before it completes gives up the wait, and affects no other task.
    ///
    /// # Example
    ///
    /// A thread that waits for a value that this thread sets:
    ///
    /// ```
    /// use std::{sync::Arc, thread};
    ///
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// let cell = Arc::new(OnceCell::new());
    /// let waiter = thread::spawn({
    ///     let cell = cell.clone();
    ///     move || *block_on(cell.wait())
    /// });
    ///
    /// block_on(cell.set(5)).expect("nobody set a new cell");
    ///
    /// assert_eq!(waiter.join().expect("the other thread did not panic"), 5);
    /// ```
    pub async fn wait(&self) -> &T {
        if let Some(value) = self.value.get() {
            return value;
        }
        loop {
            // Listen before re-checking so a value set between the check and the wait is seen.
            let listener = self.ready.listen();
            if let Some(value) = self.value.get() {
                return value;
            }
            listener.await;
        }
    }

    /// The value, set by `init` where the cell has none, waiting for an initialiser that is running
    /// already.
    ///
    /// Where the cell has a value, it is handed out at once and `init` is not called. Where it has
    /// not, and no other initialiser is running, `init` is called and the future it makes is
    /// awaited, and what that future resolves to becomes the value. Where another initialiser is
    /// running, this call waits for it, and, if it sets the value, hands that out without calling
    /// `init`; if it fails or is given up, this call initialises the cell itself, or waits for the
    /// next initialiser in line to.
    ///
    /// Dropping the future before it completes gives up the call. Where it was running `init`'s
    /// future, that is dropped with it and the cell stays empty. See the
    /// [type documentation](OnceCell#initialisation) for what that means for the other tasks, and
    /// for what not to do from inside `init`'s future.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// let cell = OnceCell::new();
    ///
    /// assert_eq!(block_on(cell.get_or_init(|| async { 1 })), &1);
    /// // The cell has its value now, and this initialiser is not called.
    /// assert_eq!(block_on(cell.get_or_init(|| async { 2 })), &1);
    /// ```
    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        // The error type has no values, so the `Ok` is the only result there can be.
        let Ok(value) = self
            .get_or_try_init(|| async move { Ok::<T, Infallible>(init().await) })
            .await;

        value
    }

    /// The value, set by `init` where the cell has none, or the error `init`'s future failed with.
    ///
    /// As [`get_or_init`](OnceCell::get_or_init), except that the future `init` makes may fail.
    /// Where it does, the error is returned, to this call alone, and the cell stays empty: the
    /// next initialiser in line runs its own, rather than take the failure as the answer.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// let cell = OnceCell::<u8>::new();
    ///
    /// let failed = block_on(cell.get_or_try_init(|| async { Err("not available") }));
    /// assert_eq!(failed, Err("not available"));
    /// assert_eq!(cell.get(), None);
    ///
    /// let made = block_on(cell.get_or_try_init(|| async { Ok::<_, &str>(1) }));
    /// assert_eq!(made, Ok(&1));
    /// ```
    pub async fn get_or_try_init<F, Fut, E>(&self, init: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        // Held until this call ends, whichever way it does: the guard is dropped with the future
        // that holds it, which lets the next initialiser in, whether this one set the value,
        // failed, panicked or was given up.
        let _initializing = match self.lock_or_value().await {
            Ok(guard) => guard,
            Err(value) => return Ok(value),
        };
        // The turn may have come together with the value, as the initialiser that set it let go of
        // the turn, which the wait prefers: look again before running an initialiser.
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        let value = init().await?;

        Ok(self.store(value))
    }

    /// Sets the value of the cell if it has none, waiting for an initialiser that is running.
    ///
    /// Hands back a reference to the value that is set, or, where the cell has a value already,
    /// `value` itself in the `Err`. An initialiser that is running has its turn first, which is
    /// why this waits: if it sets a value, this call fails; if it fails or is given up, this call
    /// sets its value, unless another initialiser that gets in first sets one, which makes this
    /// call fail.
    ///
    /// Dropping the future before it completes gives up the call: `value` is dropped with it, and
    /// the cell is left as it was.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use zruntime::lock::OnceCell;
    ///
    /// let cell = OnceCell::new();
    ///
    /// assert_eq!(block_on(cell.set(1)), Ok(&1));
    /// // The cell has its value, which stays: this one comes back.
    /// assert_eq!(block_on(cell.set(2)), Err(2));
    /// assert_eq!(cell.get(), Some(&1));
    /// ```
    pub async fn set(&self, value: T) -> Result<&T, T> {
        if self.value.get().is_some() {
            return Err(value);
        }
        let Ok(_initializing) = self.lock_or_value().await else {
            return Err(value);
        };
        // The turn may have come together with the value, as the initialiser that set it let go of
        // the turn, which the wait prefers: look again before setting a value.
        if self.value.get().is_some() {
            return Err(value);
        }

        Ok(self.store(value))
    }

    /// Waits for this call's turn to initialise the cell, which is the lock on `initializing`, or
    /// for the cell to have a value, whichever comes first, and hands out the guard of the lock or
    /// the value.
    ///
    /// The value wakes every task that waits for it, those that wait for their turn among them:
    /// the lock alone would let them out one at a time, each behind the one before it, and one
    /// that was woken and is never polled would hold back all of those behind it. Where the turn
    /// and the value come together the turn wins, and the caller looks at the value again.
    async fn lock_or_value(&self) -> Result<MutexGuard<'_, ()>, &T> {
        // Polled until it completes, and never after: the loop below ends as soon as it does.
        let mut lock = pin!(self.initializing.lock());
        loop {
            // Listen before checking so a value set in between is heard of.
            let mut set = self.ready.listen();
            if let Some(value) = self.value.get() {
                return Err(value);
            }
            let guard = poll_fn(|cx| {
                if let Poll::Ready(guard) = lock.as_mut().poll(cx) {
                    return Poll::Ready(Some(guard));
                }

                Pin::new(&mut set).poll(cx).map(|()| None)
            })
            .await;
            if let Some(guard) = guard {
                // Dropped before the guard is returned: a drop of the listener that panics then
                // unwinds with the guard still a local, which lets the lock go, where a returned
                // guard would be leaked, with the lock held for good.
                drop(set);

                return Ok(guard);
            }
        }
    }

    /// Puts `value` in the cell and wakes the tasks that wait for it, handing back a reference to
    /// it.
    ///
    /// For the holder of `initializing`, which has seen the cell empty: nobody else sets the value,
    /// so this one is the one that stays.
    fn store(&self, value: T) -> &T {
        let stored = self.value.get_or_init(|| value);
        self.ready.notify(usize::MAX);

        stored
    }
}

impl<T> Default for OnceCell<T> {
    /// A new cell with no value, whatever `T` is.
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<T> for OnceCell<T> {
    /// A new cell that holds `value` already.
    fn from(value: T) -> Self {
        Self {
            value: OnceLock::from(value),
            initializing: Mutex::new(()),
            ready: Event::new(),
        }
    }
}

impl<T> fmt::Debug for OnceCell<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_tuple("OnceCell");
        match self.value.get() {
            Some(value) => s.field(value),
            None => s.field(&format_args!("<uninit>")),
        };

        s.finish()
    }
}
