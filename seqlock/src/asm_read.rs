#![allow(unused_variables)]

use crate::{Exclusive, Optimistic, SeqLockMode, SeqLockModeExclusiveImpl, SeqLockModeImpl, Shared};
use bytemuck::Pod;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem::{align_of, size_of, transmute, MaybeUninit};
use std::ops::Range;
use std::ptr::slice_from_raw_parts_mut;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub fn optimistic_release(lock: &AtomicU64, expected: u64) {
    if lock.load(Relaxed) != expected {
        Optimistic::release_error()
    }
}

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

unsafe impl SeqLockModeImpl for Optimistic {
    type Pointer<'a, T: ?Sized + 'a> = *mut T;
    unsafe fn from_pointer<'a, T: ?Sized + 'a>(x: *mut T) -> Self::Pointer<'a, T> {
        x
    }

    fn as_ptr<'a, T: 'a + ?Sized>(x: &Self::Pointer<'a, T>) -> *mut T {
        *x
    }

    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T {
        if size_of::<T>() == align_of::<T>() {
            let addr: *const T = *p;
            macro_rules! load_case {
            ($size:expr,$reg_class:ident,$reg_format:literal,$Via:ty) => {
                if size_of::<T>()==$size{
                    let dst:$Via;
                 core::arch::asm!(
                        concat!("mov {dst",$reg_format,"}, [{addr:r}]"),
                        addr = in(reg) addr,
                        dst = lateout($reg_class) dst,
                        options(readonly,preserves_flags,nostack)
                    );
                    return (&dst as *const $Via as *const T).read();
                }
            };
        }
            load_case!(1, reg_byte, "", u8);
            load_case!(2, reg, ":x", u16);
            load_case!(4, reg, ":e", u32);
            load_case!(8, reg, ":r", u64);
        }
        let mut dst = [MaybeUninit::<T>::uninit()];
        Self::load_slice(&slice_from_raw_parts_mut(*p, 1), &mut dst);
        dst[0].assume_init()
    }

    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]) {
        asm_memcpy::<false, T>((*p) as *const T as *const u8, dst.as_mut_ptr() as *mut u8, p.len())
    }

    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering {
        let cmp_len = p.len().min(other.len()) * size_of::<T>();
        let result: i8;
        unsafe {
            core::arch::asm!(
            "cmp eax, eax", // clear flags in case len==0
            "repe cmpsb",
            "sete {result}",
            "setb {neg}",
            "xor {result}, 1",
            "shl {neg}, 1",
            "sub {result}, {neg}",
            in("si") *p as *const T,
            in("di") other.as_ptr(),
            in("cx") cmp_len,
            neg = lateout(reg_byte) _,
            result = lateout(reg_byte) result,
            options(readonly,nostack)
            );
            let result = transmute::<i8, Ordering>(result);
            result.then(p.len().cmp(&other.len()))
        }
    }

    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    ) {
        asm_memcpy::<false, T>((*p) as *const T as *const u8, dst.as_mut_ptr() as *mut u8, p.len())
    }
}

unsafe fn asm_memcpy<const REVERSE: bool, T>(src: *const u8, dst: *mut u8, count: usize) {
    let align = align_of::<T>();
    let word_size = if align % 8 == 0 {
        8
    } else if align % 4 == 0 {
        4
    } else if align % 2 == 0 {
        2
    } else {
        1
    };
    macro_rules! memcpy_case {
        ($size:expr,$inst:literal,$set_df:literal,$clear_df:literal,$offset:expr) => {
                core::arch::asm!(
                    $set_df,
                std::concat!("rep ",$inst),
                    $clear_df,
                in("si") (src as isize) + $offset,
                in("di") (dst as isize) + $offset,
                in("cx") count * (size_of::<T>()/word_size),
                options(nostack,preserves_flags),
            );
        };
        ($size:expr,$inst:literal)=>{
            if $size == word_size{
                if REVERSE{
                    memcpy_case!($size,$inst,"std","cld",count as isize *size_of::<T>() as isize-word_size as isize);
                }else{
                    memcpy_case!($size,$inst,"","",0);
                }
                return;
            }
        }
    }
    memcpy_case!(1, "movsb");
    memcpy_case!(2, "movsw");
    memcpy_case!(4, "movsd");
    memcpy_case!(8, "movsq");
    panic!("bad size align");
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
        let p = p.as_mut_ptr();
        asm_memcpy::<MOVE_UP, T>(p.add(src_range.start).cast(), p.add(dst).cast(), src_range.len())
    }
}
