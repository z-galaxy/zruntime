//! The tasks of one runtime, and the polling of those that are ready.
//!
//! A scheduler holds every task spawned on it and hands them out one at a time to be polled, on
//! whichever thread calls [`Scheduler::run_one`]. It has no thread of its own: the thread driving
//! the runtime calls that method for as long as there is anything to run.
//!
//! Every task lives in a slot of the scheduler's own, under an id that is never handed out
//! again, and holds its future there while it waits. Waking a task, from any thread, puts its id
//! on the runtime's ready queue and breaks the wait of the thread driving the runtime, which is
//! how a thread waiting with nothing to do learns that it has a task to poll. The waker holds the
//! id rather than the task: a waker has to be `Send` and `Sync`, and the task's future need be
//! neither. A wake for a task that is already queued does nothing, so a task woken three times is
//! polled once; the task is marked unqueued right before it is polled, so a wake that arrives
//! during the poll queues it again. An id whose task has gone by the time it comes up is skipped.
//!
//! [`spawn`] hands back a handle that resolves to what the task produced. The task's future is
//! wrapped before it is stored, and the wrapper is what hands the outcome over: the future's
//! output, or an error where it panicked, or — through a guard it holds, as it is dropped
//! unfinished — an error where the runtime went away with it. Dropping the handle cancels the
//! task: the future is dropped there and then if nobody is polling it, and by the poll under way
//! otherwise. A task whose outcome is of no further interest is detached instead, and runs until
//! it ends.
//!
//! A task that panics takes nothing with it. The panic is caught where the task is polled, the
//! task ends there, its handle reports the failure, and the thread that polled it carries on
//! with the next task.
//!
//! No lock is held while a future is polled or dropped, nor while a waker is woken or dropped,
//! because all of these run code that may spawn a task, cancel one or wake another on this very
//! scheduler — the destructor of a future is where its last piece of work is handed off — and
//! that code takes the locks that would be held. On a local runtime the locks are `RefCell`s, and
//! the same rule is what keeps a borrow from ever finding another one out. The table of tasks and
//! the outcome a handle waits on each have a lock of their own, and neither is ever taken while
//! the other is held.

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    future::{Future, poll_fn},
    hash::{BuildHasherDefault, Hasher},
    io, mem,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
};

use crate::{
    Mode,
    log::error,
    mode::sealed::Lock,
    runtime::{Core, Remote},
};

/// The tasks of one runtime: what has been spawned on it and has not ended yet.
pub(crate) struct Scheduler<M>
where
    M: Mode,
{
    tasks: M::Lock<Tasks<M>>,
    /// Where the ids of the tasks ready to be polled are queued.
    remote: Arc<Remote>,
}

impl<M> Scheduler<M>
where
    M: Mode,
{
    /// An empty scheduler, which queues its tasks on `remote`.
    pub(crate) fn new(remote: Arc<Remote>) -> Self {
        Self {
            tasks: Lock::new(Tasks {
                slots: HashMap::default(),
                next_id: 0,
            }),
            remote,
        }
    }

    /// Polls one ready task. `false` when the queue was empty.
    ///
    /// An id whose task has gone since it was queued counts as one polled: it took its place in
    /// the queue, and the batch it was taken in is that much closer to its end.
    pub(crate) fn run_one(&self) -> bool {
        let Some(id) = self.remote.next_ready() else {
            return false;
        };
        let taken = {
            let mut tasks = self.tasks.lock();
            tasks.slots.get_mut(&id).and_then(|slot| {
                match mem::replace(&mut slot.state, SlotState::Running { cancelled: false }) {
                    SlotState::Idle(future) => Some((future, slot.waker.clone())),
                    // Nothing to poll: the future is already out of its slot. Only a poll takes
                    // it out, and nothing polls from inside a poll, but an entry like that is
                    // put back rather than trusted to be impossible.
                    running => {
                        slot.state = running;

                        None
                    }
                }
            })
        };
        // A task that is gone — finished, or cancelled while it waited — leaves its id behind in
        // the queue.
        let Some((mut future, task_waker)) = taken else {
            return true;
        };

        // The queue entry popped above is spent by this: a wake that arrives from here on, the
        // poll below included, queues the task afresh. A swap rather than a store, so that the
        // poll sees what the wakes turned away since the entry was queued were for.
        task_waker.queued.swap(false, Ordering::AcqRel);
        let waker = Waker::from(task_waker);
        // The wrapper catches the panics of the task's own future, so what is caught here is what
        // lies outside that: the wake of whoever joins the task, say.
        let polled = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut future).poll(&mut Context::from_waker(&waker))
        }));
        let ended = match polled {
            Ok(Poll::Ready(())) => true,
            Ok(Poll::Pending) => false,
            Err(payload) => {
                error!("A task panicked outside its own future");
                // Nothing carries the panic any further, so its payload ends here. The payload's
                // destructor is the task's code as much as the future is, and is contained the
                // same way.
                dispose(payload);

                true
            }
        };

        // A future that is finished with is taken out of its slot here and dropped below, with
        // no lock held.
        let finished = {
            let mut tasks = self.tasks.lock();
            let Some(slot) = tasks.slots.get_mut(&id) else {
                unreachable!("a task being polled keeps its slot");
            };
            let SlotState::Running { cancelled } = slot.state else {
                unreachable!("only this call takes a slot out of `Running`");
            };
            if ended || cancelled {
                tasks.slots.remove(&id).map(|slot| (slot, future))
            } else {
                slot.state = SlotState::Idle(future);

                None
            }
        };
        if let Some((slot, future)) = finished {
            // A destructor is free to panic and free to spawn, so it runs here: caught, and clear
            // of every lock a spawn of its own would take.
            dispose(future);
            drop(slot);
        }

        true
    }

    /// How many spawned futures have neither finished nor been cancelled.
    #[cfg(test)]
    pub(crate) fn live_tasks(&self) -> usize {
        self.tasks.lock().slots.len()
    }

    /// The waker of the task `id`.
    #[cfg(test)]
    pub(crate) fn waker(&self, id: u64) -> Waker {
        Waker::from(self.tasks.lock().slots[&id].waker.clone())
    }
}

impl<M> Drop for Scheduler<M>
where
    M: Mode,
{
    fn drop(&mut self) {
        // Nothing can reach the scheduler any more, so its table is emptied without a lock, and
        // each future is dropped with no borrow of it out. Dropping a future unfinished is what
        // fails its handle: the guard the task's wrapper holds sees to it as it goes.
        let slots: Vec<Slot<M>> = self
            .tasks
            .get_mut()
            .slots
            .drain()
            .map(|(_, slot)| slot)
            .collect();
        for slot in slots {
            dispose(slot);
        }
    }
}

/// Wraps `future` as a task named `name`: the future to store, and what its handle and it share.
///
/// The wrapper polls `future` with its panics caught, drops it once it is done with, and hands
/// its outcome over to the handle: the output, or an error where it panicked. It holds a guard
/// that hands an error over instead, should the wrapper itself be dropped unfinished.
///
/// The future is left unerased, so that the caller, which knows the concrete type and so whether
/// it is `Send`, boxes it as its runtime's flavour takes it.
pub(crate) fn task<M, F>(
    name: Cow<'static, str>,
    future: F,
) -> (
    M::Ptr<Join<M, F::Output>>,
    impl Future<Output = ()> + 'static,
)
where
    M: Mode,
    F: Future + 'static,
    F::Output: 'static,
{
    let join = M::new_ptr(Join {
        name,
        state: Lock::new(JoinState {
            output: None,
            done: false,
            join_waker: None,
        }),
    });
    let guard = CompleteOnDrop::<M, F::Output>(join.clone());
    let wrapper = async move {
        // Moved in first, so that it is dropped last: a task cancelled while it waits drops its
        // future before the guard hears of it, as a task that ends does.
        let guard = guard;
        let mut future = pin!(Some(future));
        let polled = catch_unwind_polls(future.as_mut()).await;
        // Done with, and dropped here, before the handle hears of the outcome: whoever joins the
        // task finds whatever the future held already gone. The value is moved out of `polled`
        // ahead of the drop, so a future whose destructor panics still hands its value back.
        let dropped = catch_unwind(AssertUnwindSafe(|| future.set(None)));
        let output = match polled {
            Ok(value) => Ok(value),
            Err(payload) => {
                error!("The task `{}` panicked", guard.0.name);
                // Nothing carries the panic any further, so its payload ends here. That
                // payload's destructor is the task's code as much as the future is, and is
                // contained the same way.
                dispose(payload);

                Err(io::Error::other("the task panicked"))
            }
        };
        if let Err(payload) = dropped {
            dispose(payload);
        }

        guard.complete(output);
    };

    (join, wrapper)
}

/// Queues `future`, wrapped by [`task`] around `join`, on `core`, and hands back the handle that
/// joins or cancels it.
pub(crate) fn spawn<M, T>(
    core: &M::Ptr<Core<M>>,
    join: M::Ptr<Join<M, T>>,
    future: M::BoxFuture,
) -> JoinHandle<T, M>
where
    M: Mode,
{
    let id = {
        let mut tasks = core.scheduler.tasks.lock();
        let id = tasks.next_id;
        tasks.next_id += 1;
        let waker = Arc::new(TaskWaker {
            id,
            // Queued directly below rather than through a wake: the task is reachable by nobody
            // else yet, so it is safe to know without asking that it is not already queued.
            queued: AtomicBool::new(true),
            remote: core.remote.clone(),
        });
        tasks.slots.insert(
            id,
            Slot {
                waker,
                state: SlotState::Idle(future),
            },
        );

        id
    };
    core.remote.schedule(id);

    JoinHandle {
        id,
        core: M::downgrade(core),
        join,
        detached: false,
    }
}

/// Joins a spawned task, and cancels it when dropped unless [`JoinHandle::detach`] was called.
pub(crate) struct JoinHandle<T, M>
where
    M: Mode,
{
    id: u64,
    /// Weakly, so that a handle nobody polls keeps nothing of a runtime alive but the outcome it
    /// is waiting on.
    core: M::Weak<Core<M>>,
    join: M::Ptr<Join<M, T>>,
    detached: bool,
}

impl<T, M> JoinHandle<T, M>
where
    M: Mode,
{
    /// Lets the task run to completion on its own.
    pub(crate) fn detach(mut self) {
        self.detached = true;
    }

    /// The task's outcome, once it has one; a wake of `cx`'s waker once it does, otherwise.
    pub(crate) fn poll_join(&self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        // Cloned before the lock is taken: a waker's clone is somebody else's code, which is not
        // to run with a lock of this runtime's held.
        let waker = cx.waker().clone();
        let replaced = {
            let mut state = self.join.state.lock();
            if let Some(output) = state.output.take() {
                return Poll::Ready(output);
            }

            state.join_waker.replace(waker)
        };
        // Clear of the lock, for the same reason.
        drop(replaced);

        Poll::Pending
    }
}

impl<T, M> Drop for JoinHandle<T, M>
where
    M: Mode,
{
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        // Marked done first, so that the guard in the wrapper finds nobody left to tell when the
        // future goes, here or after the poll under way.
        let join_waker = {
            let mut state = self.join.state.lock();
            // Finished, or failed as its runtime went: nothing to take back.
            if state.done {
                return;
            }
            state.done = true;

            state.join_waker.take()
        };
        let Some(core) = M::upgrade(&self.core) else {
            // The runtime is on its way out, and its future with it.
            if let Some(waker) = join_waker {
                waker.wake();
            }

            return;
        };
        let removed = {
            let mut tasks = core.scheduler.tasks.lock();
            let running = match tasks.slots.get_mut(&self.id) {
                // Whoever is polling the task drops the future once that poll returns.
                Some(Slot {
                    state: SlotState::Running { cancelled },
                    ..
                }) => {
                    *cancelled = true;

                    true
                }
                Some(_) => false,
                // Gone already: it finished while the handle was marked done above.
                None => true,
            };

            (!running).then(|| tasks.slots.remove(&self.id)).flatten()
        };
        // On the thread that let the handle go, with no lock held: a destructor that panics
        // here panics where the handle was dropped, and one that spawns is served as any other
        // caller is.
        let dropped = removed.map(|slot| catch_unwind(AssertUnwindSafe(move || drop(slot))));
        if let Some(waker) = join_waker {
            // Outside the lock: whoever registered it, polling this very handle earlier and
            // getting `Pending`, may be polled to completion right here.
            waker.wake();
        }
        // Even where nothing was queued: a thread waiting with this as its last task learns
        // from it that it has nothing left to wait for.
        core.remote.notify();
        // Carried on from here, so that the panic is still the caller's to see while the task it
        // belonged to is closed off either way.
        if let Some(Err(payload)) = dropped {
            resume_unwind(payload);
        }
    }
}

impl<T, M> fmt::Debug for JoinHandle<T, M>
where
    M: Mode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("task", &self.join.name)
            .field("detached", &self.detached)
            .finish_non_exhaustive()
    }
}

/// What a task and its handle share: the task's name, and the outcome the handle waits on.
pub(crate) struct Join<M, T>
where
    M: Mode,
{
    /// What the task is there for. It is for diagnostics alone: the message a panicking task
    /// logs, and the handle's [`Debug`](fmt::Debug).
    name: Cow<'static, str>,
    state: M::Lock<JoinState<T>>,
}

/// The outcome of a task, and the waker of whoever is joining it.
struct JoinState<T> {
    /// The task's output, until the handle that joins it takes it.
    output: Option<io::Result<T>>,
    /// Whether the task's outcome is settled: an output handed over, or the handle gone and so
    /// nobody left to hand one to.
    done: bool,
    /// Registered by a poll of the [`JoinHandle`] that found no output waiting yet.
    join_waker: Option<Waker>,
}

/// Hands the outcome of a task over to its handle: the one it is given, or, where it is dropped
/// with none given, the error of a task whose runtime went away with it.
///
/// Dropped unfinished, a task's wrapper is either cancelled, and its handle gone and marked done
/// already, or dropped with the runtime it was on, which is what the error says.
struct CompleteOnDrop<M, T>(M::Ptr<Join<M, T>>)
where
    M: Mode;

impl<M, T> CompleteOnDrop<M, T>
where
    M: Mode,
{
    /// Hands `output` over, unless the handle is gone and has nobody to hand it to.
    fn complete(&self, output: io::Result<T>) {
        let (unwanted, join_waker) = {
            let mut state = self.0.state.lock();
            if state.done {
                (Some(output), None)
            } else {
                state.done = true;
                state.output = Some(output);

                (None, state.join_waker.take())
            }
        };
        // Clear of the lock: an output's destructor is the task's code.
        drop(unwanted);
        if let Some(waker) = join_waker {
            // Outside the lock: the handle may be polled to completion on this very thread.
            waker.wake();
        }
    }
}

impl<M, T> Drop for CompleteOnDrop<M, T>
where
    M: Mode,
{
    fn drop(&mut self) {
        let join_waker = {
            let mut state = self.0.state.lock();
            if state.done {
                return;
            }
            state.done = true;
            state.output = Some(Err(io::Error::other("the task's runtime is gone")));

            state.join_waker.take()
        };
        if let Some(waker) = join_waker {
            waker.wake();
        }
    }
}

/// Polls the future in `future` until it is done, with every panic a poll of it raises caught
/// and handed back in place of its output.
///
/// The future sits in an `Option` so that the caller can drop it once this is done, in place and
/// with its own panics caught, before anybody hears of the outcome.
fn catch_unwind_polls<F>(
    mut future: Pin<&mut Option<F>>,
) -> impl Future<Output = thread::Result<F::Output>>
where
    F: Future,
{
    poll_fn(move |cx| {
        let Some(future) = future.as_mut().as_pin_mut() else {
            unreachable!("the future is polled until it is done, and dropped only then");
        };
        match catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
}

/// Drops `value` with every panic its destructor raises contained.
///
/// A task's panic is contained per task: the destructor of its payload, and of that payload's
/// own payload should it have one, is part of that same task rather than of whoever polls it.
fn dispose<T>(value: T) {
    let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) else {
        return;
    };
    // A payload whose own destructor panics is leaked rather than dropped a third time: the
    // panic is contained either way, and a chain of destructors that each panic with the next
    // is not one the scheduler should follow to its end.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        mem::forget(payload);
    }
}

/// The tasks spawned on a scheduler that have not ended yet, under their ids.
struct Tasks<M>
where
    M: Mode,
{
    /// Keyed, rather than a list to scan: a server handing each request a task of its own would
    /// make a scan per completion quadratic under a burst of them.
    slots: HashMap<u64, Slot<M>, BuildHasherDefault<IdHasher>>,
    /// The id the next task gets. Never handed out twice, so that a stale id in the ready queue
    /// can only ever name a task that is gone, never a newer one in its place.
    next_id: u64,
}

/// Hashes a task id as itself.
///
/// Ids come from a counter, so they are spread as evenly as a hash could spread them, and the
/// default hasher's defence against chosen keys buys nothing for keys no caller chooses.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 << 8) | u64::from(*byte);
        }
    }

    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }
}

/// One task: its waker, made once as it was spawned and cloned into every poll of it, and what
/// it is up to.
struct Slot<M>
where
    M: Mode,
{
    waker: Arc<TaskWaker>,
    state: SlotState<M>,
}

/// What a task is up to.
enum SlotState<M>
where
    M: Mode,
{
    /// Waiting to be polled, with its future in hand.
    Idle(M::BoxFuture),
    /// Out of its slot, being polled.
    Running {
        /// Whether the handle was dropped while the task was being polled, so the future is to
        /// be dropped once that poll returns.
        cancelled: bool,
    },
}

/// The waker of one task: its id, and the queue it goes on when woken.
///
/// Holds nothing of the task itself, which may not leave the thread its runtime is on, so that
/// it can be woken from any thread whatever the runtime's flavour.
struct TaskWaker {
    id: u64,
    /// Whether the task's id sits in the ready queue, so that a wake which finds it there leaves
    /// it at that. Cleared right before the task is polled.
    queued: AtomicBool,
    remote: Arc<Remote>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // A previous `true` means the task is queued already, and the poll that entry leads to
        // sees what this wake was for: it acquires this very swap as it clears the flag.
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        self.remote.schedule(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Duration,
    };

    use event_listener::Event;
    use futures_lite::future::{block_on, yield_now};
    use ntest::timeout;

    use super::*;
    use crate::{LocalRuntime, Task};

    #[test]
    #[timeout(15000)]
    fn a_spawned_task_runs_and_hands_its_output_back() {
        let runtime = runtime();
        let handle = runtime.spawn("an answer", async { 42 });

        drive(&runtime);

        assert_eq!(block_on(handle).unwrap(), 42);
        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn a_wake_from_another_thread_notifies_the_driver() {
        let runtime = runtime();
        let event = Arc::new(Event::new());
        let handle = {
            let event = event.clone();
            runtime.spawn("a waiting task", async move {
                event.listen().await;
                "woken"
            })
        };

        // Polled here, so that it is waiting for the event rather than for its first poll.
        drive(&runtime);
        assert!(!runtime.core.remote.has_ready());
        take_wake_up(&runtime);

        thread::spawn(move || {
            event.notify(1);
        })
        .join()
        .unwrap();

        assert!(runtime.core.remote.wake_pending());
        assert!(runtime.core.remote.has_ready());
        drive(&runtime);
        assert_eq!(block_on(handle).unwrap(), "woken");
    }

    #[test]
    #[timeout(15000)]
    fn a_task_woken_during_its_own_poll_runs_again() {
        struct WakeOnce(bool);

        impl Future for WakeOnce {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 {
                    return Poll::Ready(());
                }
                self.0 = true;
                cx.waker().wake_by_ref();

                Poll::Pending
            }
        }

        let runtime = runtime();
        let scheduler = &runtime.core.scheduler;
        let handle = runtime.spawn("a self-waking task", WakeOnce(false));

        assert!(scheduler.run_one());
        assert!(runtime.core.remote.has_ready());
        assert!(scheduler.run_one());
        assert!(!scheduler.run_one());
        assert!(block_on(handle).is_ok());
    }

    #[test]
    #[timeout(15000)]
    fn dropping_the_handle_cancels_and_drops_the_future() {
        let runtime = runtime();
        let dropped = Arc::new(AtomicBool::new(false));
        let handle = {
            let marker = SetOnDrop(dropped.clone());
            runtime.spawn("a task that never finishes", async move {
                let _marker = marker;
                std::future::pending::<()>().await;
            })
        };

        // Polled here, so that the cancellation finds it idle rather than merely queued.
        drive(&runtime);
        assert!(!dropped.load(Ordering::Acquire));

        drop(handle);

        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    /// A task cancelled while it waits in the queue leaves its id behind, which is skipped.
    ///
    /// The id counts as one polled, so that a batch taking it moves on, and nothing is polled for
    /// it: the task it named is gone, and ids are never handed out again, so no newer task can
    /// have taken its place.
    #[test]
    #[timeout(15000)]
    fn a_task_cancelled_while_queued_leaves_an_id_that_is_skipped() {
        let runtime = runtime();
        let polled = Rc::new(Cell::new(false));
        let handle = {
            let polled = polled.clone();
            runtime.spawn("a task that is never polled", async move {
                polled.set(true);
            })
        };

        drop(handle);

        assert_eq!(runtime.core.remote.ready_len(), 1);
        assert!(runtime.core.scheduler.run_one());
        assert!(!runtime.core.scheduler.run_one());
        assert!(!polled.get());
    }

    #[test]
    #[timeout(15000)]
    fn cancelling_notifies_the_driver() {
        let runtime = runtime();
        let handle = runtime.spawn("a task that never finishes", std::future::pending::<()>());

        drive(&runtime);
        take_wake_up(&runtime);

        drop(handle);

        assert!(runtime.core.remote.wake_pending());
    }

    #[test]
    #[timeout(15000)]
    fn a_detached_task_runs_to_completion() {
        let runtime = runtime();
        let ran = Arc::new(AtomicBool::new(false));
        {
            let ran = ran.clone();
            runtime
                .spawn("a detached task", async move {
                    yield_now().await;
                    ran.store(true, Ordering::Release);
                })
                .detach();
        }

        drive(&runtime);

        assert!(ran.load(Ordering::Acquire));
        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn a_panicking_task_fails_its_handle_and_is_forgotten() {
        let runtime = runtime();
        let handle = runtime.spawn("a task that panics", async {
            panic!("the task panicked on purpose");
        });

        drive(&runtime);

        let error = block_on(handle).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    /// A panic whose payload panics as it goes takes its task down with it all the same.
    ///
    /// The payload is the task's to dispose of, and a destructor that panics is one more panic
    /// of that task rather than one of the thread polling it. The task is finished with either
    /// way: nobody will poll it again, and whoever waits on its handle is told so.
    #[test]
    #[timeout(15000)]
    fn a_panic_payload_that_panics_as_it_goes_still_ends_its_task() {
        struct PanickingPayload;

        impl Drop for PanickingPayload {
            fn drop(&mut self) {
                panic!("the panic payload's drop panicked on purpose");
            }
        }

        let runtime = runtime();
        let scheduler = &runtime.core.scheduler;
        let handle = runtime.spawn("a task that panics with a payload of its own", async {
            std::panic::panic_any(PanickingPayload);
        });

        assert!(scheduler.run_one());

        assert_eq!(scheduler.live_tasks(), 0);
        assert_eq!(block_on(handle).unwrap_err().kind(), io::ErrorKind::Other);
        assert!(!scheduler.run_one());
    }

    /// Cancelling a task from inside its own poll, whose future's drop panics with a payload
    /// whose own drop panics too, is contained at every level: the scheduler runs the next task
    /// instead of unwinding past the one that was cancelled.
    #[test]
    #[timeout(15000)]
    fn cancelling_a_task_whose_drop_panics_with_a_panicking_payload_is_contained() {
        struct PanickingPayload;

        impl Drop for PanickingPayload {
            fn drop(&mut self) {
                panic!("the payload's drop panicked on purpose");
            }
        }

        struct PanicsWithAPayloadWhenDropped;

        impl Drop for PanicsWithAPayloadWhenDropped {
            fn drop(&mut self) {
                std::panic::panic_any(PanickingPayload);
            }
        }

        let runtime = runtime();
        let scheduler = &runtime.core.scheduler;
        let own_handle: Rc<RefCell<Option<Task<()>>>> = Rc::new(RefCell::new(None));
        let dropped_from_inside = own_handle.clone();
        let handle = runtime.spawn(
            "a task that cancels itself from within its own poll",
            async move {
                let _marker = PanicsWithAPayloadWhenDropped;
                dropped_from_inside.borrow_mut().take();
                std::future::pending::<()>().await;
            },
        );
        *own_handle.borrow_mut() = Some(handle);

        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let other = runtime.spawn("an unrelated task", async move {
            flag.store(true, Ordering::Release);
        });

        assert!(scheduler.run_one());
        assert!(scheduler.run_one());

        assert!(ran.load(Ordering::Acquire));
        assert!(block_on(other).is_ok());
        assert_eq!(scheduler.live_tasks(), 0);
    }

    /// A panic payload whose own drop panics with a further payload, whose drop panics too, is
    /// contained at every level: the task is failed and the next one still runs.
    #[test]
    #[timeout(15000)]
    fn a_panic_payload_whose_drop_panics_with_another_payload_is_contained() {
        struct Inner;

        impl Drop for Inner {
            fn drop(&mut self) {
                panic!("the inner payload's drop panicked on purpose");
            }
        }

        struct Outer;

        impl Drop for Outer {
            fn drop(&mut self) {
                std::panic::panic_any(Inner);
            }
        }

        let runtime = runtime();
        let scheduler = &runtime.core.scheduler;
        let handle = runtime.spawn("a task whose panic payload panics twice over", async {
            std::panic::panic_any(Outer);
        });

        assert!(scheduler.run_one());

        assert_eq!(scheduler.live_tasks(), 0);
        assert_eq!(block_on(handle).unwrap_err().kind(), io::ErrorKind::Other);

        let next = runtime.spawn("the task after it", async { 7 });
        drive(&runtime);
        assert_eq!(block_on(next).unwrap(), 7);
    }

    #[test]
    #[timeout(15000)]
    fn a_panic_in_a_futures_drop_is_contained() {
        struct PanicOnDrop;

        impl Future for PanicOnDrop {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("the future's drop panicked on purpose");
            }
        }

        let runtime = runtime();
        let handle = runtime.spawn("a task that panics as it goes", PanicOnDrop);

        assert!(runtime.core.scheduler.run_one());
        assert!(block_on(handle).is_ok());

        let next = runtime.spawn("the task after it", async { 7 });
        drive(&runtime);
        assert_eq!(block_on(next).unwrap(), 7);
    }

    /// A cancellation whose future panics as it goes notifies all the same.
    ///
    /// The notification is what tells a thread waiting with this as its last task that it has
    /// nothing left to wait for, and a destructor of somebody else's that panicked is no reason
    /// to leave that thread waiting.
    #[test]
    #[timeout(15000)]
    fn a_panic_in_a_cancelled_futures_drop_still_notifies() {
        struct PanicOnDrop;

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("the future's drop panicked on purpose");
            }
        }

        let runtime = runtime();
        let handle = {
            let marker = PanicOnDrop;
            runtime.spawn("a task that never finishes", async move {
                let _marker = marker;
                std::future::pending::<()>().await;
            })
        };
        // Polled here, so that the cancellation finds the future idle and the drop of it is this
        // thread's to make.
        drive(&runtime);
        take_wake_up(&runtime);

        let cancelled = catch_unwind(AssertUnwindSafe(|| drop(handle)));

        // The panic is the caller's to see, where the handle was let go of.
        assert!(cancelled.is_err());
        assert!(runtime.core.remote.wake_pending());
        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn a_ready_task_is_queued_once_however_often_it_is_woken() {
        let runtime = runtime();
        let scheduler = &runtime.core.scheduler;
        let handle = runtime.spawn("a task that never finishes", std::future::pending::<()>());
        let waker = scheduler.waker(handle.0.id);

        for _ in 0..3 {
            waker.wake_by_ref();
        }

        assert_eq!(runtime.core.remote.ready_len(), 1);
        assert!(scheduler.run_one());
        assert!(!scheduler.run_one());
    }

    #[test]
    #[timeout(15000)]
    fn a_cancelled_futures_drop_may_spawn_on_the_scheduler() {
        struct SpawnOnDrop {
            runtime: LocalRuntime,
            ran: Rc<Cell<bool>>,
        }

        impl Drop for SpawnOnDrop {
            fn drop(&mut self) {
                let ran = self.ran.clone();
                self.runtime
                    .spawn("a task spawned from a drop", async move {
                        ran.set(true);
                    })
                    .detach();
            }
        }

        let runtime = runtime();
        let ran = Rc::new(Cell::new(false));
        let handle = {
            let spawner = SpawnOnDrop {
                runtime: runtime.clone(),
                ran: ran.clone(),
            };
            runtime.spawn("a task that never finishes", async move {
                let _spawner = spawner;
                std::future::pending::<()>().await;
            })
        };

        drive(&runtime);
        drop(handle);

        assert!(!ran.get());
        drive(&runtime);
        assert!(ran.get());
    }

    #[test]
    #[timeout(15000)]
    fn live_tasks_counts_unfinished_futures_only() {
        let runtime = runtime();
        let done = runtime.spawn("a task that finishes at once", async {});
        let pending = runtime.spawn("a task that never finishes", std::future::pending::<()>());
        assert_eq!(runtime.core.scheduler.live_tasks(), 2);

        drive(&runtime);

        assert_eq!(runtime.core.scheduler.live_tasks(), 1);
        assert!(block_on(done).is_ok());

        drop(pending);

        assert_eq!(runtime.core.scheduler.live_tasks(), 0);
    }

    #[test]
    #[timeout(15000)]
    fn dropping_the_runtime_fails_pending_joins() {
        let runtime = runtime();
        let handle = runtime.spawn("a task that never finishes", std::future::pending::<()>());

        drive(&runtime);
        drop(runtime);

        assert!(block_on(handle).is_err());
    }

    /// Polls ready tasks until none is left, the way the thread driving the runtime does.
    fn drive(runtime: &LocalRuntime) {
        while runtime.core.scheduler.run_one() {}
    }

    /// Takes the wake-up the runtime has written, if any, out of its channel, so that the next
    /// one a test looks for is one written after this.
    fn take_wake_up(runtime: &LocalRuntime) {
        runtime.core.reactor.wait(Some(Duration::ZERO)).unwrap();
        assert!(!runtime.core.remote.wake_pending());
    }

    /// A runtime of the test's own, driven by nobody but the test.
    fn runtime() -> LocalRuntime {
        LocalRuntime::new().unwrap()
    }

    /// Sets the flag it holds when it is dropped.
    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
}
