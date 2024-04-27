use crate::{
    Exclusive, Guarded, Optimistic, OptimisticLockError, SeqLockMode, SeqLockModeImpl, SeqLockWrappable, Wrapper,
};
use std::cell::UnsafeCell;
use std::mem::forget;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

unsafe impl<T: Send> Send for SeqLock<T> {}
unsafe impl<T: Send + Sync> Sync for SeqLock<T> {}

pub struct LockState {
    pub(crate) version: AtomicU64,
}

pub struct SeqLock<T> {
    lock: LockState,
    data: UnsafeCell<T>,
}

struct NoDrop;

impl Drop for NoDrop {
    fn drop(&mut self) {
        panic!();
    }
}

impl<T: SeqLockWrappable> SeqLock<T> {
    pub fn new(x: T) -> Self {
        SeqLock { lock: LockState { version: Default::default() }, data: UnsafeCell::new(x) }
    }

    pub fn lock<M: SeqLockMode>(&self) -> Guard<M, T> {
        let guard_data = M::acquire(&self.lock);
        Guard {
            lock: &self.lock,
            guard_data,
            access: unsafe { Guarded::wrap_unchecked(self.data.get()) },
            _no_drop: NoDrop,
        }
    }
}

pub struct Guard<'a, M: SeqLockMode, T: SeqLockWrappable> {
    lock: &'a LockState,
    guard_data: M::GuardData,
    access: T::Wrapper<Guarded<'a, M, T>>,
    _no_drop: NoDrop,
}

impl<'a, T: SeqLockWrappable> Guard<'a, Optimistic, T> {
    pub fn release(self) -> Result<(), OptimisticLockError> {
        Optimistic::release(self.lock, self.guard_data).map(|_| ())
    }

    pub fn check_or_release(self)->Result<Self,OptimisticLockError>{
        Optimistic::release(self.lock, self.guard_data).map(|_| self)
    }

    pub fn upgrade(self) -> Result<Guard<'a, Exclusive, T>, OptimisticLockError> {
        forget(self._no_drop);
        if self.lock.version.compare_exchange(self.guard_data, self.guard_data + 1, Acquire, Relaxed).is_ok() {
            Ok(Guard {
                lock: self.lock,
                guard_data: (),
                access: unsafe { Guarded::wrap_unchecked(Optimistic::as_ptr(&self.access.get().p)) },
                _no_drop: NoDrop,
            })
        } else {
            Err(OptimisticLockError(()))
        }
    }
}

impl<'a, T: SeqLockWrappable> Clone for Guard<'a, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            lock: self.lock,
            guard_data: self.guard_data,
            access: unsafe { Guarded::wrap_unchecked(Optimistic::as_ptr(&self.access.get().p)) },
            _no_drop: NoDrop,
        }
    }
}

impl<'a, T: SeqLockWrappable> Guard<'a, Exclusive, T> {
    pub fn release(self) {
        Exclusive::release(self.lock, ()).unwrap();
        forget(self);
    }

    pub fn downgrade(self) -> Guard<'a, Optimistic, T> {
        let guard_data = Exclusive::release(self.lock, ()).unwrap();
        let lock = self.lock;
        let ptr = Exclusive::as_ptr(&self.access.get().p);
        forget(self);
        Guard { lock, guard_data, access: unsafe { Guarded::wrap_unchecked(ptr) }, _no_drop: NoDrop }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable> Deref for Guard<'a, M, T> {
    type Target = T::Wrapper<Guarded<'a, M, T>>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable> DerefMut for Guard<'a, M, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
