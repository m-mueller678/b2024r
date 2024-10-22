#![feature(slice_index_methods)]
#![feature(array_ptr_get)]

use std::ops::{Deref, DerefMut};

mod o_ptr;

pub use o_ptr::OPtr;

#[derive(Eq, PartialEq)]
pub struct OlcVersion {
    v: u64,
}

pub struct PageId(pub u64);

pub unsafe trait BufferManager<'bm>: 'bm + Copy + Send + Sync + Sized + OlcErrorHandler {
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

pub trait OlcErrorHandler {
    fn optimistic_fail() -> !;
}

pub trait BufferManagerGuard<'bm, B: BufferManager<'bm>>: Sized {
    fn acquire_wait(bm: B, page_id: PageId) -> Self;
    fn acquire_wait_version(bm: B, page_id: PageId, v: OlcVersion) -> Option<Self>;
    fn release(self, bm: B) -> OlcVersion;
}

pub trait OptimisticGuard<T, O: OlcErrorHandler> {
    fn o_ptr(&self) -> OPtr<'_, T, O>;
}

pub trait BufferManageGuardUpgrade<'bm, B: BufferManager<'bm>, Target>: Sized {
    fn try_upgrade(self) -> Result<Target, Self>;
}
