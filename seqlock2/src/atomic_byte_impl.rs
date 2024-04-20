#![allow(unused_variables)]
use crate::{
    Exclusive, Optimistic, OptimisticLockError, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared,
};
use bytemuck::Pod;
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{compiler_fence, AtomicU64};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), OptimisticLockError> {
    compiler_fence(Acquire);
    if lock.load(Relaxed) == expected {
        Ok(())
    } else {
        Err(OptimisticLockError(()))
    }
}

unsafe impl SeqLockModeImpl for Exclusive {
    type Pointer<'a, T: ?Sized> = ();

    unsafe fn from_pointer<'a, T>(x: *mut T) -> Self::Pointer<'a, T> {
        todo!()
    }

    fn as_pointer<T>(x: &Self::Pointer<'_, T>) -> *mut T {
        todo!()
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
}

unsafe impl SeqLockModeImpl for Shared {
    type Pointer<'a, T: ?Sized> = ();

    unsafe fn from_pointer<'a, T>(x: *mut T) -> Self::Pointer<'a, T> {
        todo!()
    }

    fn as_pointer<T>(x: &Self::Pointer<'_, T>) -> *mut T {
        todo!()
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
}

unsafe impl SeqLockModeImpl for Optimistic {
    type Pointer<'a, T: ?Sized> = ();

    unsafe fn from_pointer<'a, T>(x: *mut T) -> Self::Pointer<'a, T> {
        todo!()
    }

    fn as_pointer<T>(x: &Self::Pointer<'_, T>) -> *mut T {
        todo!()
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
}

unsafe impl SeqLockModeExclusiveImpl for Exclusive {
    unsafe fn store<T>(p: &mut Self::Pointer<'_, T>, x: T) {
        todo!()
    }

    unsafe fn store_slice<T>(p: &mut Self::Pointer<'_, T>, x: T) {
        todo!()
    }

    unsafe fn move_within_slice<T, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        distance: usize,
    ) {
        todo!()
    }
}
