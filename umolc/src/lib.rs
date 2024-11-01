#![feature(slice_index_methods)]
#![feature(array_ptr_get)]

use bytemuck::Zeroable;
use std::ops::{Deref, DerefMut};

mod o_ptr;
mod unwind;

use self::unwind::{OlcErrorHandler, OptimisticError};
pub use o_ptr::OPtr;

#[derive(Eq, PartialEq)]
pub struct OlcVersion {
    v: u64,
}

#[derive(Debug, Zeroable, Copy, Clone)]
pub struct PageId(pub u64);

pub trait BufferManager<'bm>: 'bm + Copy + Send + Sync + Sized + OlcErrorHandler {
    type Page;
    type GuardO: BufferManagerGuard<'bm, Self>
        + OptimisticGuard<Self::Page, Self>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardS>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardS: BufferManagerGuard<'bm, Self>
        + Deref<Target = Self::Page>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardX: BufferManagerGuard<'bm, Self> + Deref<Target = Self::Page> + DerefMut;
    fn alloc(self) -> (Self::GuardX, PageId);
    fn free(self, g: Self::GuardX);
}

pub trait BufferManagerExt<'bm>: BufferManager<'bm> {
    fn repeat<R>(mut f: impl FnMut() -> R) -> R {
        loop {
            if let Ok(x) = Self::catch(&mut f) {
                return x;
            }
        }
    }

    fn lock_optimistic(self, pid: PageId) -> Self::GuardO {
        Self::GuardO::acquire_wait(self, pid)
    }
    fn lock_shared(self, pid: PageId) -> Self::GuardS {
        Self::GuardS::acquire_wait(self, pid)
    }
    fn lock_exclusive(self, pid: PageId) -> Self::GuardX {
        Self::GuardX::acquire_wait(self, pid)
    }
}

impl<'bm, BM: BufferManager<'bm>> BufferManagerExt<'bm> for BM {}

pub trait BufferManagerGuard<'bm, B: BufferManager<'bm>>: Sized {
    fn acquire_wait(bm: B, page_id: PageId) -> Self;
    fn acquire_wait_version(bm: B, page_id: PageId, v: OlcVersion) -> Option<Self>;
    fn release(self, bm: B) -> OlcVersion;
}

pub trait OptimisticGuard<T, O: OlcErrorHandler> {
    fn o_ptr(&self) -> OPtr<'_, T, O>;
}

pub trait BufferManageGuardUpgrade<'bm, B: BufferManager<'bm>, Target>: Sized {
    fn upgrade_wait(self) -> Target;
}
