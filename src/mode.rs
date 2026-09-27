//! The two flavours a runtime comes in, and what each builds its shared state from.
//!
//! A runtime's state is reached from several places at once: every handle on it, every task
//! handle, timer and registration, and the thread driving it. What those places share it through
//! is the only thing that tells the two flavours apart. A [`Local`] runtime shares it through
//! [`Rc`] and [`RefCell`], and so never leaves the thread it was made on; a [`Shared`] one shares
//! it through [`Arc`] and [`Mutex`], and may be handed to, and driven from, any thread. Everything
//! else — the scheduler, the reactor, the loop that drives them — is written once, over [`Mode`],
//! and reaches that state through the associated types of the sealed trait behind it.
//!
//! Neither flavour is `Send` or `Sync` by assertion: a `Local` runtime is neither because an `Rc`
//! is neither, and a `Shared` one is both because every piece of it is.

use std::{
    cell::{RefCell, RefMut},
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::{self, Rc},
    sync::{self, Arc, Mutex, MutexGuard, PoisonError},
};

#[cfg(unix)]
use std::os::fd::{AsFd as AsSource, BorrowedFd as BorrowedSource};
#[cfg(windows)]
use std::os::windows::io::{AsSocket as AsSource, BorrowedSocket as BorrowedSource};

/// The flavour of a [`Runtime`](crate::Runtime): [`Local`] or [`Shared`].
///
/// Implemented by exactly those two, and by nothing outside this crate.
pub trait Mode: sealed::Sealed {}

/// The flavour of a runtime that stays on the thread it was made on.
///
/// A local runtime runs any `'static` future, `Send` or not, and holds its state in [`Rc`] and
/// [`RefCell`] rather than behind atomics and locks. Its handles, and the task handles, timers
/// and registrations built on it, cannot be sent to another thread.
///
/// This is a marker type: it has no values, and is only ever named as a type parameter.
#[derive(Debug)]
pub enum Local {}

impl Mode for Local {}

/// The flavour of a runtime that may be reached from, and driven on, any thread.
///
/// A shared runtime runs `Send` futures only, and holds its state in [`Arc`] and [`Mutex`]. Its
/// handles, and the task handles, timers and registrations built on it, are `Send` and `Sync`.
///
/// This is a marker type: it has no values, and is only ever named as a type parameter.
#[derive(Debug)]
pub enum Shared {}

impl Mode for Shared {}

/// What [`Mode`] carries, out of reach of every crate but this one.
///
/// The trait is public in name only, so that [`Mode`] can name it as its supertrait, and sits in a
/// module nobody outside can reach, so that nobody outside can implement it: the two
/// implementations below are the only two there are.
pub(crate) mod sealed {
    use super::*;

    /// What a runtime of one flavour builds its shared state from.
    pub trait Sealed: Sized + 'static {
        /// A pointer that shares a value: [`Rc`] or [`Arc`].
        ///
        /// `Unpin` whatever it points to, as both are, so that a future holding one can be
        /// polled through a plain `&mut` whatever the runtime's flavour.
        type Ptr<T>: Clone + Deref<Target = T> + Unpin;

        /// A pointer that shares a value without keeping it alive: [`rc::Weak`] or
        /// [`sync::Weak`].
        type Weak<T>: Unpin;

        /// A lock around a value: [`RefCell`] or [`Mutex`].
        type Lock<T>: Lock<T>;

        /// A task's future once its type is erased: boxed, and `Send` where the runtime is shared.
        type BoxFuture: Future<Output = ()> + Unpin + 'static;

        /// A registered source once its type is erased: shared, so that a wait can hold on to it
        /// while the registration goes, and `Send + Sync` where the runtime is shared.
        type SourcePtr: Clone + 'static;

        /// Shares `value`.
        fn new_ptr<T>(value: T) -> Self::Ptr<T>;

        /// A pointer to what `ptr` points to, which does not keep it alive.
        fn downgrade<T>(ptr: &Self::Ptr<T>) -> Self::Weak<T>;

        /// What `weak` points to, if it is still alive.
        fn upgrade<T>(weak: &Self::Weak<T>) -> Option<Self::Ptr<T>>;

        /// The descriptor, or the socket on Windows, of the source `source` points to.
        ///
        /// A function rather than a bound on [`Sealed::SourcePtr`]: Windows implements
        /// `AsSocket` for an `Rc` or an `Arc` of a sized type only, and a source pointer is one
        /// of a trait object.
        fn as_source(source: &Self::SourcePtr) -> BorrowedSource<'_>;
    }

    /// A lock around a value, taken with no way to fail.
    ///
    /// A [`RefCell`] fails a borrow while another is out, and the runtime's lock discipline —
    /// never hold a lock across a poll, a future's drop, a waker's wake or drop, or anything else
    /// that can come back into the runtime — is what keeps that from ever happening. A [`Mutex`]
    /// fails a lock once a panic has poisoned it, and the value behind it is taken all the same:
    /// every panic the runtime can see is contained where it happens, before it could leave a
    /// value half-changed.
    pub trait Lock<T> {
        /// What holds the lock, and derefs to the value behind it.
        type Guard<'a>: DerefMut<Target = T>
        where
            Self: 'a;

        /// A lock around `value`.
        fn new(value: T) -> Self;

        /// Takes the lock.
        fn lock(&self) -> Self::Guard<'_>;

        /// The value behind the lock, reached through a unique reference with no locking at all.
        fn get_mut(&mut self) -> &mut T;
    }

    impl Sealed for Local {
        type Ptr<T> = Rc<T>;
        type Weak<T> = rc::Weak<T>;
        type Lock<T> = RefCell<T>;
        type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;
        type SourcePtr = Rc<dyn AsSource>;

        fn new_ptr<T>(value: T) -> Rc<T> {
            Rc::new(value)
        }

        fn downgrade<T>(ptr: &Rc<T>) -> rc::Weak<T> {
            Rc::downgrade(ptr)
        }

        fn upgrade<T>(weak: &rc::Weak<T>) -> Option<Rc<T>> {
            weak.upgrade()
        }

        fn as_source(source: &Self::SourcePtr) -> BorrowedSource<'_> {
            borrow_source(&**source)
        }
    }

    impl Sealed for Shared {
        type Ptr<T> = Arc<T>;
        type Weak<T> = sync::Weak<T>;
        type Lock<T> = Mutex<T>;
        type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
        type SourcePtr = Arc<dyn AsSource + Send + Sync>;

        fn new_ptr<T>(value: T) -> Arc<T> {
            Arc::new(value)
        }

        fn downgrade<T>(ptr: &Arc<T>) -> sync::Weak<T> {
            Arc::downgrade(ptr)
        }

        fn upgrade<T>(weak: &sync::Weak<T>) -> Option<Arc<T>> {
            weak.upgrade()
        }

        fn as_source(source: &Self::SourcePtr) -> BorrowedSource<'_> {
            borrow_source(&**source)
        }
    }

    /// The descriptor of `source`.
    #[cfg(unix)]
    fn borrow_source<S>(source: &S) -> BorrowedSource<'_>
    where
        S: AsSource + ?Sized,
    {
        source.as_fd()
    }

    /// The socket of `source`.
    #[cfg(windows)]
    fn borrow_source<S>(source: &S) -> BorrowedSource<'_>
    where
        S: AsSource + ?Sized,
    {
        source.as_socket()
    }

    impl<T> Lock<T> for RefCell<T> {
        type Guard<'a>
            = RefMut<'a, T>
        where
            T: 'a;

        fn new(value: T) -> Self {
            RefCell::new(value)
        }

        fn lock(&self) -> RefMut<'_, T> {
            self.borrow_mut()
        }

        fn get_mut(&mut self) -> &mut T {
            RefCell::get_mut(self)
        }
    }

    impl<T> Lock<T> for Mutex<T> {
        type Guard<'a>
            = MutexGuard<'a, T>
        where
            T: 'a;

        fn new(value: T) -> Self {
            Mutex::new(value)
        }

        fn lock(&self) -> MutexGuard<'_, T> {
            Mutex::lock(self).unwrap_or_else(PoisonError::into_inner)
        }

        fn get_mut(&mut self) -> &mut T {
            Mutex::get_mut(self).unwrap_or_else(PoisonError::into_inner)
        }
    }
}
