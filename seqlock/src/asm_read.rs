#![allow(unused_variables)]

use std::borrow::Borrow;
use crate::{Exclusive, Optimistic, SeqLockMode, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared};
use bytemuck::Pod;
use std::cmp::Ordering;
use std::mem::{size_of, size_of_val, MaybeUninit};
use std::ops::{Deref, Range};
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{compiler_fence, AtomicU64, AtomicU8};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) {
    if lock.load(Relaxed) != expected {
        Optimistic::release_error()
    }
}

trait CommonImpl {
    type Pointer<'a,T:?Sized+'a>:Borrow<T>+Sized+'a;
    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T>;
    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T;

}

unsafe fn atomic_memcpy<const REVERSE: bool>(src: *const u8, dst: *mut u8, len: usize) {
    let src = from_raw_parts(src as *const AtomicU8, len);
    let dst = from_raw_parts(dst as *const AtomicU8, len);
    if REVERSE {
        for i in (0..len).rev() {
            dst[i].store(src[i].load(Relaxed), Relaxed)
        }
    } else {
        for i in 0..len {
            dst[i].store(src[i].load(Relaxed), Relaxed)
        }
    }
}

unsafe impl<C: CommonImpl> SeqLockModeImpl for C {
    type Pointer<'a, T: ?Sized+'a> = C::Pointer<'a,T>;
    unsafe fn from_pointer<'a, T: ?Sized+'a>(x: *mut T) -> Self::Pointer<'a, T> {
        <Self as CommonImpl>::from_pointer(x)
    }

    fn as_ptr<'a,T: 'a+?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        <Self as CommonImpl>::as_pointer(x)
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        *p.as_ref()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        std::ptr::copy_nonoverlapping(p.as_ref().as_ptr(),dst.as_mut_ptr(),p.len());
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        p.as_ref().cast_slice::<u8>().cmp(other.cast_slice::<u8>())
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {

        todo!()
    }
}


impl CommonImpl for Shared {
    type Pointer<'a, T: ?Sized+'a> = &'a T;

    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T> { &*x }

    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T {x as *const _ as * mut _}
}

impl CommonImpl for Exclusive {
    type Pointer<'a, T:?Sized+'a> = &'a mut T;

    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T> { &mut *x }

    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T {x as * mut _}
}


unsafe impl SeqLockModeImpl for Optimistic {
    type Pointer<'a, T: ?Sized+'a> = *mut T;
    unsafe fn from_pointer<'a, T: ?Sized+'a>(x: *mut T) -> Self::Pointer<'a, T> {
        x
    }

    fn as_ptr<'a,T: 'a+?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        *x
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        todo!()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        todo!()
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        todo!()
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        todo!()
    }
}

unsafe impl SeqLockModeExclusiveImpl for Exclusive {
    unsafe fn store<T>(p: &mut Self::Pointer<'_, T>, x: T) {
        **p=x;
    }

    unsafe fn store_slice<T>(p: &mut Self::Pointer<'_, [T]>, x: &[T]) {
        p.copy_from_slice(x);
    }

    unsafe fn move_within_slice_to<T, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        src_range: Range<usize>,
        dst: usize,
    ) {
        todo!()
    }
}
