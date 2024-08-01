#![allow(unused_variables)]

use crate::lock::LockState;
use crate::{Exclusive, Optimistic, SeqLockMode, SeqLockModeImpl};
use bytemuck::Pod;
use radium::marker::Atomic;
use radium::{Atom, Radium};
use std::cmp::Ordering;
use std::ffi::c_void;
use std::mem::{align_of, size_of, MaybeUninit};
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

impl LockState {
    pub fn release_optimistic(&self, expected: u64) {
        fence(Acquire);
        if self.version.load(Relaxed) != expected {
            if !std::thread::panicking() {
            Optimistic::release_error()}
        }
    }
}

unsafe impl SeqLockModeImpl for Optimistic {
    type Pointer<'a, T: ?Sized + 'a> = *mut T;
    unsafe fn from_pointer<'a, T: ?Sized + 'a>(x: *mut T) -> Self::Pointer<'a, T> {
        x
    }

    fn as_ptr<'a, T: 'a + ?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        *x
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        let p: *mut T = *p;
        let mut dst = MaybeUninit::<T>::uninit();
        #[inline(always)]
        unsafe fn atomic_load<T, A: Atomic + PartialEq>(src: *mut T, dst: *mut T) {
            let src = src as *mut Atom<A>;
            let dst = dst as *mut A;
            for i in 0..(size_of::<T>() / align_of::<T>()) {
                let src = src.add(i);
                *dst.add(i) = (*src).load(Relaxed);
            }
        }
        if align_of::<T>() >= 8 {
            atomic_load::<T, u64>(p, dst.as_mut_ptr());
        } else if align_of::<T>() >= 4 {
            atomic_load::<T, u32>(p, dst.as_mut_ptr());
        } else if align_of::<T>() >= 2 {
            atomic_load::<T, u16>(p, dst.as_mut_ptr());
        } else {
            atomic_load::<T, u8>(p, dst.as_mut_ptr());
        }
        dst.assume_init()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        std::ptr::copy_nonoverlapping::<T>((*p).cast::<T>(), dst.as_mut_ptr() as *mut T, p.len());
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        let cmp_len = p.len().min(other.len()) * size_of::<T>();
        let r = libc::memcmp((*p).cast::<c_void>(), other.as_ptr() as *const c_void, cmp_len);
        r.cmp(&0).then(p.len().cmp(&other.len()))
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        std::ptr::copy_nonoverlapping::<T>((*p).cast::<T>(), dst.as_mut_ptr(), p.len());
    }
}
