#![allow(unused_variables)]
use crate::{
    Exclusive, Optimistic, OptimisticLockError, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared,
};
use bytemuck::Pod;
use std::cmp::Ordering;
use std::mem::size_of;
use std::mem::{size_of_val, MaybeUninit};
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{compiler_fence, AtomicU64, AtomicU8};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), OptimisticLockError> {
    compiler_fence(Acquire);
    if lock.load(Relaxed) == expected {
        Ok(())
    } else {
        Err(OptimisticLockError(()))
    }
}

unsafe fn atomic_memcpy<const FORWARD: bool>(src: *const u8, dst: *mut u8, len: usize) {
    let src = from_raw_parts(src as *const AtomicU8, len);
    let dst = from_raw_parts(dst as *const AtomicU8, len);
    if FORWARD {
        for i in 0..len {
            dst[i].store(src[i].load(Relaxed), Relaxed)
        }
    } else {
        for i in (0..len).rev() {
            dst[i].store(src[i].load(Relaxed), Relaxed)
        }
    }
}

trait CommonImpl {}
impl CommonImpl for Exclusive {}
impl CommonImpl for Optimistic {}
impl CommonImpl for Shared {}

unsafe impl<M: CommonImpl> SeqLockModeImpl for M {
    type Pointer<'a, T: ?Sized> = *mut T;
    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T> {
        x
    }

    fn as_ptr<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T {
        *x
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        let mut buffer = MaybeUninit::<T>::uninit();
        atomic_memcpy::<true>(
            *p as *const u8,
            buffer.as_mut_ptr() as *mut u8,
            size_of::<T>(),
        );
        buffer.assume_init()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        atomic_memcpy::<true>(
            *p as *const u8,
            dst.as_mut_ptr() as *mut u8,
            size_of_val(dst),
        )
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        let other_bytes = bytemuck::cast_slice::<T, u8>(other);
        let mut this = (0..other_bytes.len())
            .map(|i| unsafe { (*(*p as *mut AtomicU8).add(i)).load(Relaxed) });
        let mut other = other_bytes.iter().copied();
        for _ in 0..other_bytes.len() {
            let c = this.next().cmp(&other.next());
            if !c.is_eq() {
                return c;
            }
        }
        this.next().cmp(&other.next())
    }
}

unsafe impl SeqLockModeExclusiveImpl for Exclusive {
    unsafe fn store<T>(p: &mut Self::Pointer<'_, T>, x: T) {
        atomic_memcpy::<true>(&x as *const T as *const u8, *p as *mut u8, size_of::<T>())
    }

    unsafe fn store_slice<T>(p: &mut Self::Pointer<'_, [T]>, x: &[T]) {
        atomic_memcpy::<true>(
            x.as_ptr() as *const u8,
            *p as *mut T as *mut u8,
            size_of_val(x),
        );
    }

    unsafe fn move_within_slice<T, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        distance: usize,
    ) {
        let len = (p.len() - distance) * size_of::<T>();
        let offset = distance * size_of::<T>();
        let p = *p as *mut T as *mut u8;
        if MOVE_UP {
            atomic_memcpy::<false>(p, p.add(offset), len);
        } else {
            atomic_memcpy::<true>(p.add(offset), p, len);
        }
    }
}
