#![allow(unused_variables)]
use crate::{Exclusive, Optimistic, SeqLockMode, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared};
use bytemuck::Pod;
use std::cmp::Ordering;
use std::mem::{size_of, size_of_val, MaybeUninit};
use std::ops::Range;
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{compiler_fence, AtomicU64, AtomicU8};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) {
    compiler_fence(Acquire);
    if lock.load(Relaxed) != expected {
        Optimistic::release_error()
    }
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

unsafe fn half_atomic_memcpy<const REVERSE: bool>(src: *const u8, dst: *mut u8, len: usize) {
    let src = from_raw_parts(src, len);
    let dst = from_raw_parts(dst as *const AtomicU8, len);
    if REVERSE {
        for i in (0..len).rev() {
            dst[i].store(src[i], Relaxed)
        }
    } else {
        for i in 0..len {
            dst[i].store(src[i], Relaxed)
        }
    }
}

trait CommonImpl {}
impl CommonImpl for Exclusive {}
impl CommonImpl for Optimistic {}
impl CommonImpl for Shared {}

unsafe impl<M: CommonImpl> SeqLockModeImpl for M {
    type Pointer<'a, T: ?Sized + 'a> = *mut T;
    unsafe fn from_pointer<'a, T: ?Sized + 'a>(x: *mut T) -> Self::Pointer<'a, T> {
        x
    }

    fn as_ptr<'a, T: ?Sized + 'a>(x: &Self::Pointer<'a, T>) -> *mut T {
        *x
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        let mut buffer = MaybeUninit::<T>::uninit();
        atomic_memcpy::<false>(*p as *const u8, buffer.as_mut_ptr() as *mut u8, size_of::<T>());
        buffer.assume_init()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        atomic_memcpy::<false>(*p as *const u8, dst.as_mut_ptr() as *mut u8, size_of_val(dst))
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        let other_bytes = bytemuck::cast_slice::<T, u8>(other);
        let this_bytes_len = p.len() * size_of::<T>();
        let mut this = (0..this_bytes_len).map(|i| unsafe { (*(*p as *mut AtomicU8).add(i)).load(Relaxed) });
        let mut other = other_bytes.iter().copied();
        for _ in 0..std::cmp::min(this_bytes_len, other_bytes.len()) {
            let c = this.next().cmp(&other.next());
            if !c.is_eq() {
                return c;
            }
        }
        this.next().cmp(&other.next())
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        atomic_memcpy::<false>(*p as *mut u8, *dst as *mut u8, p.len() * size_of::<T>());
    }
}

unsafe impl SeqLockModeExclusiveImpl for Exclusive {
    unsafe fn store<T: Pod>(p: &mut Self::Pointer<'_, T>, x: T) {
        half_atomic_memcpy::<false>(&x as *const T as *const u8, *p as *mut u8, size_of::<T>())
    }

    unsafe fn store_slice<'a, T: Pod>(p: &mut Self::Pointer<'a, [T]>, x: &[T]) {
        half_atomic_memcpy::<false>(x.as_ptr() as *const u8, *p as *mut T as *mut u8, size_of_val(x));
    }

    unsafe fn move_within_slice_to<T: Pod, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        src_range: Range<usize>,
        dst: usize,
    ) {
        let len = (src_range.len()) * size_of::<T>();
        if len == 0 {
            return;
        }
        let p = *p as *mut T;
        atomic_memcpy::<MOVE_UP>(p.add(src_range.start).cast(), p.add(dst).cast(), len)
    }
}
