use crate::{Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockModeImpl, SeqLockWrappable, Wrapper};
use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
use std::mem::{forget, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread::yield_now;

#[derive(Default)]
pub struct LockState {
    pub(crate) version: AtomicU64,
}

impl LockState {
    pub fn acquire_optimistic(&self) -> u64 {
        loop {
            let x = self.version.load(Acquire);
            if x % 2 == 0 {
                return x;
            } else {
                yield_now();
            }
        }
    }

    pub fn acquire_exclusive(&self) {
        loop {
            let x = self.version.load(Relaxed);
            if x % 2 == 0 {
                if self.version.compare_exchange(x, x + 1, Acquire, Relaxed).is_ok() {
                    return;
                }
            } else {
                yield_now();
            }
        }
    }

    pub fn release_exclusive(&self) -> u64 {
        let prev = self.version.fetch_add(1, Release);
        prev + 1
    }

    pub fn upgrade_lock(&self, expected: u64) {
        if self.version.compare_exchange(expected, expected + 1, Acquire, Relaxed).is_err() {
            Optimistic::release_error()
        }
    }
}

pub struct Guard<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable + ?Sized> {
    bm: BM,
    guard_data: M::GuardData,
    access: ManuallyDrop<T::Wrapper<Guarded<'bm, M, T>>>,
}

impl<'bm, BM: BufferManager<'bm>, T: SeqLockWrappable + ?Sized> Guard<'bm, BM, Exclusive, T> {
    pub fn reset_written(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.guard_data = false;
        }
    }
}

impl<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Debug for Guard<'bm, BM, M, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("data", &self.guard_data)
            .field("ptr", &M::as_ptr(&(*self.access).get().p))
            .finish()
    }
}

impl<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Drop for Guard<'bm, BM, M, T> {
    /// dropping an exclusive lock that has been used for writing during unwinding will abort the process on debug builds
    fn drop(&mut self) {
        M::release(self.bm, self.page_address(), self.guard_data);
    }
}

impl<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Guard<'bm, BM, M, T> {
    pub fn ptr(&self) -> *mut T {
        M::as_ptr(&(*self.access).get().p)
    }

    pub fn page_address(&self) -> usize {
        self.bm.page_address_from_contained_address(self.ptr().addr())
    }
}

impl<'bm, BM: BufferManager<'bm>, T: SeqLockWrappable> Guard<'bm, BM, Optimistic, T> {
    pub fn release_unchecked(self) {
        forget(self);
    }

    pub fn check(&self) {
        drop(self.clone())
    }

    pub fn upgrade(self) -> Guard<'bm, BM, Exclusive, T> {
        let ptr = Optimistic::as_ptr(&(*self.access).get().p);
        self.bm.upgrade_lock(self.bm.page_address_from_contained_address(ptr as usize), self.guard_data);
        let x = Guard {
            bm: self.bm,
            #[cfg(debug_assertions)]
            guard_data: false,
            #[cfg(not(debug_assertions))]
            guard_data: true,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(ptr) }),
        };
        self.release_unchecked();
        x
    }
}

impl<'bm, BM: BufferManager<'bm>, T: SeqLockWrappable> Clone for Guard<'bm, BM, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            bm: self.bm,
            guard_data: self.guard_data,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(Optimistic::as_ptr(&(*self.access).get().p)) }),
        }
    }
}

impl<'bm, BM: BufferManager<'bm>, T: SeqLockWrappable> Guard<'bm, BM, Exclusive, T> {
    pub fn downgrade(self) -> Guard<'bm, BM, Optimistic, T> {
        let ptr = self.ptr();
        let guard_data = Exclusive::release(self.bm, self.page_address(), self.guard_data);
        let bm = self.bm;
        forget(self);
        Guard { bm, guard_data, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(ptr) }) }
    }

    pub fn free(self) {
        self.bm.free(self.page_address());
        forget(self);
    }
}

impl<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable> Guard<'bm, BM, M, T> {
    /// unlike drop, calling this on an exclusive lock during unwinding is ok.
    pub fn release(self) {
        M::release(self.bm, self.page_address(), self.guard_data);
        forget(self)
    }

    /// # Safety
    /// mapped object must lie within source object
    pub unsafe fn map<U: SeqLockWrappable + ?Sized + 'bm>(
        mut self,
        f: impl FnOnce(T::Wrapper<Guarded<'bm, M, T>>) -> U::Wrapper<Guarded<'bm, M, U>>,
    ) -> Guard<'bm, BM, M, U> {
        unsafe {
            let Guard { bm, guard_data, ref mut access } = self;
            let access_taken = ManuallyDrop::take(access);
            forget(self);
            Guard { bm, guard_data, access: ManuallyDrop::new(f(access_taken)) }
        }
    }
}

impl<'bm, BM: BufferManager<'bm>, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Deref for Guard<'bm, BM, M, T> {
    type Target = T::Wrapper<Guarded<'bm, M, T>>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'bm, BM: BufferManager<'bm>, T: SeqLockWrappable> DerefMut for Guard<'bm, BM, Exclusive, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        #[cfg(debug_assertions)]
        {
            self.guard_data = true;
        }
        &mut self.access
    }
}

pub unsafe trait BufferManager<'bm>: 'bm+Copy +Send+Sync+ Sized {
    type Page: Sized + SeqLockWrappable;

    /// Returned page is exclusively locked
    fn alloc(self) -> (u64, &'bm UnsafeCell<Self::Page>);
    /// Page must be exclusively locked.
    /// The lock is automatically released.
    fn free(self, page_address: usize);
    fn release_exclusive(self, page_address: usize) -> u64;
    fn acquire_exclusive(self, page_id: u64) -> &'bm UnsafeCell<Self::Page>;
    fn acquire_optimistic(self, page_id: u64) -> (&'bm UnsafeCell<Self::Page>, u64);
    fn release_optimistic(self, page_address: usize, version: u64);

    /// page must be locked.
    /// For optimistic locks, wrong id may be returned if the lock has become invalid.
    fn page_id(self,page_address:usize)->u64;
    fn upgrade_lock(self, page_address: usize, version: u64);

    /// Accepts any address within a page and returns a value that can be passed as `page_address` to the other methods to refer to the containing page.
    /// This needs not be the address of the page.
    fn page_address_from_contained_address(self, address: usize) -> usize;
}

pub trait BmExt<'bm>: BufferManager<'bm> {
    fn lock_optimistic(self, page_id: u64) -> Guard<'bm, Self, Optimistic, Self::Page> {
        let (page, guard_data) = self.acquire_optimistic(page_id);
        Guard { bm: self, guard_data, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(page.get()) }) }
    }

    fn lock_exclusive(self, page_id: u64) -> Guard<'bm, Self, Exclusive, Self::Page> {
        let page = self.acquire_exclusive(page_id);
        Guard { bm: self, guard_data: false, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(page.get()) }) }
    }

    fn lock_new(self) -> (u64, Guard<'bm, Self, Exclusive, Self::Page>) {
        let (id, page) = self.alloc();
        (
            id,
            Guard {
                bm: self,
                guard_data: false,
                access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(page.get()) }),
            },
        )
    }
}

impl<'bm, BM: BufferManager<'bm>> BmExt<'bm> for BM {}
