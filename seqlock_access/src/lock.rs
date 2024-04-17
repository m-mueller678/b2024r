use crate::{optimistic_release, Exclusive, OptimisticLockError, SeqLockSafe, Optimistic, SeqLockGuarded, SeqLockMode, SeqLockModeBase};
use as_base::{AsBase, AsBaseRefExt};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread::yield_now;
use bytemuck::Pod;

pub struct LockState {
    version: AtomicU64,
}

unsafe impl SeqLockModeBase for Exclusive{
    type GuardData = ();
    type ReleaseErrorType = !;


    fn acquire(lock:&LockState) {
        loop {
            let x = lock.version.load(Relaxed);
            if x % 2 == 0 {
                if lock.version.compare_exchange(x, x + 1, Acquire, Relaxed).is_ok(){
                    return;
                }
            } else {
                yield_now();
            }
        }
    }

    fn release(lock:&LockState) {
        lock.version.fetch_add(1, Release);
    }
}

impl SeqLockModeBase for Optimistic{
    type GuardData = ();
    type ReleaseErrorType = ();

    fn acquire(_: &LockState) {
        todo!()
    }

    fn release(_: &LockState) -> Result<(), Self::ErrorType> {
        todo!()
    }
}

impl LockState {


    fn acquire_optimistic(&self) -> u64 {
        loop {
            let x = self.version.load(Acquire);
            if x % 2 == 0 {
                return x;
            } else {
                yield_now();
            }
        }
    }

    fn release_optimistic(&self, observed: u64) -> Result<(), OptimisticLockError> {
        optimistic_release(&self.version, observed)
    }
}

pub struct SeqLock<T>{
    s:LockState,
    data:UnsafeCell<T>,
}

struct NoDrop;

impl Drop for NoDrop{
    fn drop(&mut self) {
        panic!();
    }
}

impl<T> SeqLock<T>{
    pub fn exclusive(&self)->ExclusiveGuard<T>{
        self.s.acquire_exclusive();
        ExclusiveGuard{l:self,_no_drop:NoDrop}
    }
}

struct ExclusiveGuard<'a,T:SeqLockSafe>{
    l:&'a LockState,
    access:T::Wrapped<SeqLockGuarded<'a,Exclusive,T>>,
    _no_drop:NoDrop,
}

impl<'a,M:SeqLockMode,T:SeqLockSafe> Deref for ExclusiveGuard<'a,T>{
    type Target = T::Wrapped<SeqLockGuarded<'a,Exclusive,T>>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'a,M:SeqLockMode,T:SeqLockSafe> DerefMut for ExclusiveGuard<'a,T>{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
