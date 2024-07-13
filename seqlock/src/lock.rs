use crate::{Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockModeImpl, SeqLockWrappable, Wrapper};
use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
use std::mem::{forget, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::thread::panicking;

pub struct LockState {
    pub(crate) version: AtomicU64,
}

pub struct Guard<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable + ?Sized> {
    bm: BM,
    guard_data: M::GuardData,
    access: ManuallyDrop<T::Wrapper<Guarded<'static, M, T>>>,
}

impl<BM: BufferManager, T: SeqLockWrappable + ?Sized> Guard<BM, Exclusive, T> {
    pub fn reset_written(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.guard_data = false;
        }
    }
}

impl<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Debug for Guard<BM, M, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("data", &self.guard_data)
            .field("ptr", &M::as_ptr(&(*self.access).get().p))
            .finish()
    }
}

impl<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Drop for Guard<BM, M, T> {
    /// dropping an exclusive lock that has been used for writing during unwinding will abort the process on debug builds
    fn drop(&mut self) {
        M::release(self.bm, self.page_address(), self.guard_data);
    }
}

impl<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Guard<BM, M, T> {
    pub fn ptr(&self) -> *mut T {
        M::as_ptr(&(*self.access).get().p)
    }

    pub fn page_address(&self) -> usize {
        self.bm.from_contained_address(self.ptr().addr())
    }
}

impl<BM: BufferManager, T: SeqLockWrappable> Guard<BM, Optimistic, T> {
    pub fn release_unchecked(self) {
        forget(self);
    }

    pub fn check(&self) {
        drop(self.clone())
    }

    pub fn upgrade(self) -> Guard<BM, Exclusive, T> {
        let ptr = Optimistic::as_ptr(&(*self.access).get().p);
        self.bm.upgrade_lock(self.bm.from_contained_address(ptr as usize), self.guard_data);
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

impl<BM: BufferManager, T: SeqLockWrappable> Clone for Guard<BM, Optimistic, T> {
    fn clone(&self) -> Self {
        Guard {
            bm: self.bm,
            guard_data: self.guard_data,
            access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(Optimistic::as_ptr(&(*self.access).get().p)) }),
        }
    }
}

impl<BM: BufferManager, T: SeqLockWrappable> Guard<BM, Exclusive, T> {
    pub fn downgrade(self) -> Guard<BM, Optimistic, T> {
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

impl<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable> Guard<BM, M, T> {
    /// unlike drop, calling this on an exclusive lock during unwinding is ok.
    pub fn release(self) {
        M::release(self.bm, self.page_address(), self.guard_data);
        forget(self)
    }

    /// mapped object must lie within source object
    pub unsafe fn map<U: SeqLockWrappable + ?Sized + 'static>(
        mut self,
        f: impl FnOnce(T::Wrapper<Guarded<'static, M, T>>) -> U::Wrapper<Guarded<'static, M, U>>,
    ) -> Guard<BM, M, U> {
        unsafe {
            let Guard { bm, guard_data, ref mut access } = self;
            let access_taken = ManuallyDrop::take(access);
            forget(self);
            Guard { bm, guard_data, access: ManuallyDrop::new(f(access_taken)) }
        }
    }
}

impl<BM: BufferManager, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Deref for Guard<BM, M, T> {
    type Target = T::Wrapper<Guarded<'static, M, T>>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<BM: BufferManager, T: SeqLockWrappable> DerefMut for Guard<BM, Exclusive, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        #[cfg(debug_assertions)]
        {
            self.guard_data = true;
        }
        &mut self.access
    }
}

pub unsafe trait BufferManager: Copy + Sized {
    type Page: Sized + SeqLockWrappable;

    /// Returned page is exclusively locked
    fn alloc(&self) -> (u64, &'static UnsafeCell<Self::Page>);
    /// Page must be exclusively locked.
    /// The lock is automatically released.
    fn free(&self, page_address: usize);
    fn release_exclusive(self, page_address: usize) -> u64;
    fn acquire_exclusive(self, page_id: u64) -> (&'static UnsafeCell<Self::Page>);
    fn acquire_optimistic(self, page_id: u64) -> (&'static UnsafeCell<Self::Page>, u64);
    fn release_optimistic(self, page_address: usize, version: u64);
    fn upgrade_lock(self, page_address: usize, version: u64);

    /// Accepts any address within a page and returns a value that can be passed as `page_address` to the other methods to refer to the containing page.
    /// This needs not be the address of the page.
    fn from_contained_address(self, address: usize) -> usize;
}

pub trait BmExt: BufferManager {
    fn lock_optimistic(self, page_id: u64) -> Guard<Self, Optimistic, Self::Page> {
        let (page, guard_data) = self.acquire_optimistic(page_id);
        Guard { bm: self, guard_data, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(page.get()) }) }
    }

    fn lock_exclusive(self, page_id: u64) -> Guard<Self, Exclusive, Self::Page> {
        let page = self.acquire_exclusive(page_id);
        Guard { bm: self, guard_data: false, access: ManuallyDrop::new(unsafe { Guarded::wrap_unchecked(page.get()) }) }
    }

    fn lock_new(self) -> (u64, Guard<Self, Exclusive, Self::Page>) {
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
