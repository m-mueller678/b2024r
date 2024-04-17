use crate::{
    optimistic_release, wrap_unchecked, Exclusive, Optimistic, OptimisticLockError, SeqLockGuarded,
    SeqLockMode, SeqLockModeBase, SeqLockSafe,
};
use std::cell::UnsafeCell;
use std::mem::forget;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread::yield_now;

pub struct LockState {
    version: AtomicU64,
}

unsafe impl SeqLockModeBase for Exclusive {
    type GuardData = ();
    type ReleaseErrorType = !;
    type ReleaseData = u64;

    fn acquire(lock: &LockState) -> Self::GuardData {
        loop {
            let x = lock.version.load(Relaxed);
            if x % 2 == 0 {
                if lock
                    .version
                    .compare_exchange(x, x + 1, Acquire, Relaxed)
                    .is_ok()
                {
                    return;
                }
            } else {
                yield_now();
            }
        }
    }

    fn release(lock: &LockState, (): ()) -> Result<Self::ReleaseData, Self::ReleaseErrorType> {
        let prev = lock.version.fetch_add(1, Release);
        Ok(prev + 1)
    }
}

unsafe impl SeqLockModeBase for Optimistic {
    type GuardData = u64;
    type ReleaseErrorType = OptimisticLockError;
    type ReleaseData = ();

    fn acquire(lock: &LockState) -> Self::GuardData {
        loop {
            let x = lock.version.load(Acquire);
            if x % 2 == 0 {
                return x;
            } else {
                yield_now();
            }
        }
    }

    fn release(
        lock: &LockState,
        guard: Self::GuardData,
    ) -> Result<Self::ReleaseData, Self::ReleaseErrorType> {
        optimistic_release(&lock.version, guard)
    }
}

pub struct SeqLock<T> {
    s: LockState,
    data: UnsafeCell<T>,
}

struct NoDrop;

impl Drop for NoDrop {
    fn drop(&mut self) {
        panic!();
    }
}

impl<T: SeqLockSafe> SeqLock<T> {
    pub fn lock<M: SeqLockMode>(&self) -> Guard<M, T> {
        let guard_data = M::acquire(&self.s);
        Guard {
            l: &self.s,
            guard_data,
            access: unsafe { wrap_unchecked::<M, T>(self.data.get()) },
            _no_drop: NoDrop,
        }
    }
}

pub struct Guard<'a, M: SeqLockMode, T: SeqLockSafe> {
    l: &'a LockState,
    guard_data: M::GuardData,
    access: T::Wrapped<SeqLockGuarded<'a, M, T>>,
    _no_drop: NoDrop,
}

impl<'a, T: SeqLockSafe> Guard<'a, Optimistic, T> {
    pub fn release(self) -> Result<(), OptimisticLockError> {
        Optimistic::release(self.l, self.guard_data).map(|_| ())
    }

    pub fn upgrade(self) -> Result<Guard<'a, Exclusive, T>, OptimisticLockError> {
        forget(self._no_drop);
        if self
            .l
            .version
            .compare_exchange(self.guard_data, self.guard_data + 1, Acquire, Relaxed)
            .is_ok()
        {
            Ok(Guard {
                l: self.l,
                guard_data: (),
                access: unsafe {
                    wrap_unchecked(Optimistic::as_ptr(&T::unwrap_ref(&self.access).0))
                },
                _no_drop: NoDrop,
            })
        } else {
            Err(OptimisticLockError(()))
        }
    }
}

impl<'a, T: SeqLockSafe> Clone for Guard<'a, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            l: self.l,
            guard_data: self.guard_data,
            access: unsafe { wrap_unchecked(Optimistic::as_ptr(&T::unwrap_ref(&self.access).0)) },
            _no_drop: NoDrop,
        }
    }
}

impl<'a, T: SeqLockSafe> Guard<'a, Exclusive, T> {
    pub fn release(self) {
        Exclusive::release(self.l, ()).unwrap();
        forget(self);
    }

    pub fn downgrade(self) -> Guard<'a, Optimistic, T> {
        let v = Exclusive::release(self.l, ()).unwrap();
        let l = self.l;
        let ptr = Exclusive::as_ptr(&T::unwrap_ref(&self.access).0);
        forget(self);
        Guard {
            l,
            guard_data: v,
            access: unsafe { wrap_unchecked(ptr) },
            _no_drop: NoDrop,
        }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockSafe> Deref for Guard<'a, M, T> {
    type Target = T::Wrapped<SeqLockGuarded<'a, M, T>>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'a, M: SeqLockMode, T: SeqLockSafe> DerefMut for Guard<'a, M, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
