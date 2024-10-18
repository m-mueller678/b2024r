use std::marker::PhantomData;
use std::mem::{MaybeUninit, transmute};
use radium::marker::Atomic;
use radium::{Atom, Radium};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::Relaxed;
use static_assertions::{assert_eq_align, assert_eq_size};

pub struct OlcAtomic<T: Atomic> {
    _not_send_sync:PhantomData<*mut u8>,
    x: T::Atom,
}

impl<T: Atomic> OlcAtomic<T> {
    fn r(&self)->T {
        self.x.load(Relaxed)
    }

    fn new(x: T) -> Self {
        OlcAtomic { x:T::Atom::new(x),_not_send_sync:PhantomData }
    }

    fn get_mut(&mut self) -> &mut T {
        self.x.get_mut()
    }
}

pub unsafe trait OlcSafe: Sized {}

fn assert_size_align<Src,Dst>(){
    assert!(size_of::<Src>() == size_of::<Dst>() && align_of::<Src>() >= align_of::<Dst>())
}

pub fn cast_slice<A:OlcSafe,B:OlcSafe>(a:&[A])->&[B]{
    assert_size_align::<A,B>();

}

pub struct OlcVersion {
    v: u64,
}

pub unsafe trait BufferManager<'bm>: 'bm + Copy + Send + Sync + Sized {
    type Page: OlcSafe;
    type GuardO: BufferManagerGuard<'bm, Self>
        + Deref<Target = Self::Page>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardS>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardS: BufferManagerGuard<'bm, Self>
        + Deref<Target = Self::Page>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardX: BufferManagerGuard<'bm, Self> + Deref<Target = Self::Page> + DerefMut;
    fn alloc(self) -> Self::GuardX;
    fn free(self, g: Self::GuardX);
}

trait BufferManagerGuard<'bm, B: BufferManager<'bm>>:Sized {
    fn acquire_wait(bm: B, page_id: u64) -> Self;
    fn acquire_wait_version(bm: B, page_id: u64, v: OlcVersion) -> Option<Self> {
        None
    }
    fn release(self, bm: B) -> OlcVersion;
}

trait BufferManageGuardUpgrade<'bm, B: BufferManager<'bm>, Target>:Sized {
    fn try_upgrade(self) -> Result<Target, Self>;
}
