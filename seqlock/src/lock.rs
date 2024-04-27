use crate::{Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockModeImpl, SeqLockWrappable, Wrapper};
use std::cell::UnsafeCell;
use std::mem::{forget, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::thread::panicking;

unsafe impl<T: Send> Send for SeqLock<T> {}
unsafe impl<T: Send + Sync> Sync for SeqLock<T> {}

pub struct LockState {
    pub(crate) version: AtomicU64,
}

pub struct SeqLock<T> {
    lock: LockState,
    data: UnsafeCell<T>,
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
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(self.data.get()) }),
        }
    }
}

pub struct Guard<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> {
    lock: &'a LockState,
    guard_data: M::GuardData,
    access: ManuallyDrop<T::Wrapper<Guarded<'a, M, T>>>,
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Drop for Guard<'a, M, T> {
    fn drop(&mut self) {
        if panicking() {
            todo!()
        } else {
            M::release(self.lock, self.guard_data);
        }
    }
}

impl<'a, T: SeqLockWrappable> Guard<'a, Optimistic, T> {
    pub fn release_unchecked(self) {
        forget(self);
    }

    pub fn check(&self) {
        drop(self.clone())
    }

    pub fn upgrade(self) -> Guard<'a, Exclusive, T> {
        if self.lock.version.compare_exchange(self.guard_data, self.guard_data + 1, Acquire, Relaxed).is_ok() {
            Guard {
                lock: self.lock,
                guard_data: (),
                access: ManuallyDrop::new(unsafe {
                    Guarded::wrap_unchecked(Optimistic::as_ptr(&(*self.access).get().p))
                }),
            }
        } else {
            Optimistic::release_error()
        }
    }
}

impl<'a, T: SeqLockWrappable> Clone for Guard<'a, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            lock: self.lock,
            guard_data: self.guard_data,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(Optimistic::as_ptr(&(*self.access).get().p)) }),
        }
    }
}

impl<'a, T: SeqLockWrappable> Guard<'a, Exclusive, T> {
    pub fn downgrade(self) -> Guard<'a, Optimistic, T> {
        let guard_data = Exclusive::release(self.lock, ());
        let lock = self.lock;
        let ptr = Exclusive::as_ptr(&(*self.access).get().p);
        forget(self);
        Guard { lock, guard_data, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(ptr) }) }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable> Guard<'a, M, T> {
    pub fn map<U: SeqLockWrappable + ?Sized + 'static>(
        mut self,
        f: impl FnOnce(T::Wrapper<Guarded<'a, M, T>>) -> U::Wrapper<Guarded<'a, M, U>>,
    ) -> Guard<'a, M, U> {
        unsafe {
            let Guard { lock, guard_data, ref mut access } = self;
            let access_taken = ManuallyDrop::take(access);
            forget(self);
            Guard { lock, guard_data, access: ManuallyDrop::new(f(access_taken)) }
        }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Deref for Guard<'a, M, T> {
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
