use crate::{Exclusive, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared};
use bytemuck::Pod;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::ops::Range;

pub trait CommonImpl {
    type Pointer<'a, T: ?Sized + 'a>: Borrow<T> + Sized + 'a;
    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T>;
    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T;
}

unsafe impl<C: CommonImpl> SeqLockModeImpl for C {
    type Pointer<'a, T: ?Sized + 'a> = C::Pointer<'a, T>;
    unsafe fn from_pointer<'a, T: ?Sized + 'a>(x: *mut T) -> Self::Pointer<'a, T> {
        <Self as CommonImpl>::from_pointer(x)
    }

    fn as_ptr<'a, T: 'a + ?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        <Self as CommonImpl>::as_pointer(x)
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        *p.borrow()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        std::ptr::copy_nonoverlapping(p.borrow().as_ptr(), dst.as_mut_ptr() as *mut T, p.borrow().len());
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        bytemuck::cast_slice::<T, u8>(p.borrow()).cmp(bytemuck::cast_slice::<T, u8>(other))
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        Exclusive::store_slice(dst, p.borrow())
    }
}

impl CommonImpl for Shared {
    type Pointer<'a, T: ?Sized + 'a> = &'a T;

    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T> {
        &*x
    }

    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T {
        *x as *const T as *mut T
    }
}

impl CommonImpl for Exclusive {
    type Pointer<'a, T: ?Sized + 'a> = &'a mut T;

    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T> {
        &mut *x
    }

    fn as_pointer<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T {
        *x as *const T as *mut T
    }
}

unsafe impl SeqLockModeExclusiveImpl for Exclusive {
    unsafe fn store<T: Pod>(p: &mut Self::Pointer<'_, T>, x: T) {
        **p = x;
    }

    unsafe fn store_slice<T: Pod>(p: &mut Self::Pointer<'_, [T]>, x: &[T]) {
        p.copy_from_slice(x);
    }

    unsafe fn move_within_slice_to<T: Pod, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        src_range: Range<usize>,
        dst: usize,
    ) {
        if MOVE_UP {
            debug_assert!(src_range.start < dst);
        } else {
            debug_assert!(src_range.start > dst);
        }
        p.copy_within(src_range, dst);
    }
}
