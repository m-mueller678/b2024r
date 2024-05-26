use crate::{Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockModeImpl, SeqLockWrappable, Wrapper};
use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
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
            #[cfg(debug_assertions)]
            written: false,
            lock: &self.lock,
            guard_data,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(self.data.get()) }),
        }
    }
}

pub struct Guard<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> {
    #[cfg(debug_assertions)]
    written: bool,
    lock: &'a LockState,
    guard_data: M::GuardData,
    access: ManuallyDrop<T::Wrapper<Guarded<'a, M, T>>>,
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Debug for Guard<'a, M, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("lock_addr", &(self.lock as *const _))
            .field("lock_val", &self.lock.version.load(Relaxed))
            .field("data", &self.guard_data)
            .field("ptr", &M::as_ptr(&(*self.access).get().p))
            .finish()
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Drop for Guard<'a, M, T> {
    fn drop(&mut self) {
        if panicking() {
            if M::EXCLUSIVE {
                if self.written {
                    panic!("unwinding out of written exclusive lock")
                }
                M::release(self.lock, self.guard_data);
            }
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
            let x = Guard {
                #[cfg(debug_assertions)]
                written: false,
                lock: self.lock,
                guard_data: (),
                access: ManuallyDrop::new(unsafe {
                    Guarded::wrap_unchecked(Optimistic::as_ptr(&(*self.access).get().p))
                }),
            };
            self.release_unchecked();
            x
        } else {
            forget(self);
            Optimistic::release_error()
        }
    }
}

impl<'a, T: SeqLockWrappable> Clone for Guard<'a, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            #[cfg(debug_assertions)]
            written: false,
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
        Guard {
            #[cfg(debug_assertions)]
            written: false,
            lock,
            guard_data,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(ptr) }),
        }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable> Guard<'a, M, T> {
    pub fn map<U: SeqLockWrappable + ?Sized + 'static>(
        mut self,
        f: impl FnOnce(T::Wrapper<Guarded<'a, M, T>>) -> U::Wrapper<Guarded<'a, M, U>>,
    ) -> Guard<'a, M, U> {
        unsafe {
            let Guard {
                #[cfg(debug_assertions)]
                written,
                lock,
                guard_data,
                ref mut access,
            } = self;
            let access_taken = ManuallyDrop::take(access);
            forget(self);
            Guard {
                #[cfg(debug_assertions)]
                written,
                lock,
                guard_data,
                access: ManuallyDrop::new(f(access_taken)),
            }
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
        #[cfg(debug_assertions)]
        {
            self.written = true;
        }
        &mut self.access
    }
}
