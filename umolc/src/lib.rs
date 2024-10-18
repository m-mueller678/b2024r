use radium::marker::Atomic;
use radium::Radium;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::Relaxed;

impl<T: Atomic> !Send for OlcAtomic<T> {}
impl<T: Atomic> !Sync for OlcAtomic<T> {}

struct OlcAtomic<T: Atomic> {
    x: T::Atom,
}

impl<T: Atomic> OlcAtomic<T> {
    fn r(&self) {
        self.x.load(Relaxed)
    }

    fn new(x: T) -> Self {
        OlcAtomic { x }
    }

    fn get_mut(&mut self) -> &mut Self {
        self.x.get_mut()
    }
}

unsafe trait OlcPage: Sized {}

struct OlcVersion {
    v: u64,
}

pub unsafe trait BufferManager<'bm>: 'bm + Copy + Send + Sync + Sized {
    type Page: OlcPage;
    type GuardO<'a>: BufferManagerGuard<'bm, Self>
        + Deref<Target = Self::Page>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardS>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardS<'a>: BufferManagerGuard<'bm, Self>
        + Deref<Target = Self::Page>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardX<'a>: BufferManagerGuard<'bm, Self> + Deref<Target = Self::Page> + DerefMut;
    fn alloc(self) -> Self::GuardX;
    fn free(self, g: Self::GuardX);
}

trait BufferManagerGuard<'bm, B: BufferManager<'bm>> {
    fn acquire_wait(bm: B, page_id: u64) -> Self;
    fn acquire_wait_version(bm: B, page_id: u64, v: OlcVersion) -> Option<Self> {
        None
    }
    fn release(self, bm: B) -> OlcVersion;
}

trait BufferManageGuardUpgrade<'bm, B: BufferManager<'bm>, Target> {
    fn try_upgrade(self) -> Result<Target, Self>;
}
