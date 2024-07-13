#![allow(unused_variables)]

use crate::{Exclusive, Optimistic, SeqLockMode, SeqLockModeImpl};
use bytemuck::Pod;
use radium::Radium;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::mem::{size_of, MaybeUninit};
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{compiler_fence, AtomicU64};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) {
    compiler_fence(Acquire);
    if lock.load(Relaxed) != expected {
        Optimistic::release_error()
    }
}

unsafe impl SeqLockModeImpl for Optimistic {
    type Pointer<'a, T: ?Sized + 'a> = &'a T;
    unsafe fn from_pointer<'a, T: ?Sized + 'a>(x: *mut T) -> Self::Pointer<'a, T> {
        &*x
    }

    fn as_ptr<'a, T: 'a + ?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        (*x) as *const T as *mut T
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        **p
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        assert_eq!(p.len(), dst.len());
        for i in 0..p.len() {
            dst[i].write(p[i]);
        }
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        let cmp_len = p.len().min(other.len()) * size_of::<T>();
        let r = libc::memcmp(p.as_ptr().cast::<c_void>(), other.as_ptr() as *const c_void, cmp_len);
        r.cmp(&0).then(p.len().cmp(&other.len()))
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        dst.copy_from_slice(p)
    }
}
