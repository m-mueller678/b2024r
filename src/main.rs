#![feature(slice_ptr_len)]

use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of_mut, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicU64, compiler_fence, Ordering};
use std::sync::atomic::Ordering::Relaxed;

struct MyStruct {
    a: u32,
    b: i64,
}

unsafe impl SeqLockSafe for MyStruct {}

fn main() {
    dbg!();
}

unsafe trait SeqLockSafe {}

unsafe impl SeqLockSafe for [u8] {}


#[cfg(target_arch = "x86_64")]
pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), ()> {
    compiler_fence(Ordering::Acquire);
    if lock.load(Relaxed) == expected { Ok(()) } else { Err(()) }
}


#[repr(transparent)]
pub struct SeqLockGuardedExclusive<'a, T: SeqLockSafe + ?Sized> {
    p: &'a mut T,
}


#[repr(transparent)]
pub struct SeqLockGuardedOptimistic<'a, T: ?Sized> {
    p: *const T,
    _p: PhantomData<&'a T>,
}

impl<'a> SeqLockGuardedOptimistic<'a, [u8]> {
    fn cmp(&self, other: &[u8]) -> Ordering {
        let cmp_len = self.p.len().min(other.len());
        if cmp_len == 0 || {
            unsafe {
                core::arch::asm!(
                "repe cmpsb",
                in("si") self.p as *mut u8,
                in("di") other.as_ptr(),
                in("cx") cmp_len,
                );
            }
        } {}

        todo!()
    }
}

impl<'a, T: SeqLockSafe> SeqLockGuardedExclusive<'a, T> {
    pub unsafe fn new_unchecked(p: *mut T) -> Self {
        Self { p: &mut *p }
    }

    pub fn as_bytes_mut(&mut self) -> SeqLockGuardedExclusive<'_, [u8]> {
        unsafe {
            SeqLockGuardedExclusive { p: from_raw_parts_mut(self.p as *mut T as *mut u8, size_of::<T>()) }
        }
    }
}

impl<'a, T: SeqLockSafe> SeqLockGuardedOptimistic<'a, T> {
    pub unsafe fn new_unchecked(p: *const T) -> Self {
        Self { p, _p: PhantomData }
    }

    pub fn as_bytes(&self) -> SeqLockGuardedOptimistic<'_, [u8]> {
        SeqLockGuardedOptimistic { p: slice_from_raw_parts(self.p as *const u8, size_of::<T>()), _p: PhantomData }
    }
}

macro_rules! seqlock_accessors {
    (struct $This:ty: $($vis:vis $name:ident : $T:ty),*) => {
        impl<'a> SeqLockGuardedExclusive<'a,$This>{
            $($vis fn $name<'b>(&'b mut self)->SeqLockGuardedExclusive<'b,$T>{
                unsafe{
                    SeqLockGuardedExclusive::new_unchecked(&mut self.p.$name)
                }
            })*
        }

        impl<'a> SeqLockGuardedOptimistic<'a,$This>{
            $($vis fn $name<'b>(&'b self)->SeqLockGuardedOptimistic<'b,$T>{
                unsafe{
                    SeqLockGuardedOptimistic::new_unchecked(std::ptr::addr_of!((*self.p).$name))
                }
            })*
        }
    };
}

macro_rules! seqlock_primitive {
    ($(($T:ty) reg=$reg:ident),*) => {
        $(
        impl SeqLockGuardedOptimistic<'_,$T>{
            pub fn load(&self)->$T{
                let dst;
                unsafe{
                    #[cfg(target_arch = "x86_64")]
                    core::arch::asm!(
                        "mov ({addr}),{dst}",
                        addr = in(reg) self.p,
                        dst = lateout($reg) dst
                    );
                }
                dst
            }
        }

        unsafe impl SeqLockSafe for $T{}

        impl SeqLockGuardedExclusive<'_,$T>{
            pub fn store(&mut self,v:$T){
                unsafe{*self.p=v;}
            }
            pub fn load(&self)->$T{
                unsafe{*self.p}
            }
        }
        )*
    };
}

seqlock_primitive!(
    (u8) reg=reg_byte,
    (u16) reg=reg,
    (u32) reg=reg,
    (u64) reg=reg,
    (i8) reg=reg_byte,
    (i16) reg=reg,
    (i32) reg=reg,
    (i64) reg=reg
);

seqlock_accessors!(struct MyStruct: a:u32,b:i64);