#![feature(slice_ptr_len)]

use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of_mut, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicU64, compiler_fence, Ordering};
use std::sync::atomic::Ordering::Relaxed;

unsafe trait SeqLockGuardedInner:Sized{
    type T:SeqLockSafe+?Sized;
    unsafe fn new_unchecked(p: *mut Self::T) -> Self;
    fn to_ptr(&self)->*mut Self::T;
    unsafe fn wrap_unsafe(p:*mut Self::T)-><Self::T as SeqLockSafe>::Wrapped<Self>{
        <Self::T as SeqLockSafe>::wrap(Self::new_unchecked(p))
    }
}

pub struct SeqLockGuarded<G:SeqLockGuardedInner+?Sized>{
    g:G,
}


struct MyStruct {
    a: u32,
    b: i64,
}

fn main() {
    unsafe{
        let x=&mut MyStruct{a:1,b:2};
        let mut x= SeqLockGuardedExclusive::wrap_unsafe(x);
        dbg!(x.a().load());
        dbg!(x.b().load());
    }
}

unsafe trait SeqLockSafe {
    type Wrapped<T>;
    fn wrap<T>(x:T)->Self::Wrapped<T>;
}

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

impl<'a> SeqLockGuarded<SeqLockGuardedOptimistic<'a,[u8]>>{
    fn cmp(&self, other: &[u8]) -> Ordering {
        // let cmp_len = self.g.p.len().min(other.len());
        // if cmp_len == 0 || {
        //     unsafe {
        //         core::arch::asm!(
        //         "repe cmpsb",
        //         in("si") self.g.p as *mut u8,
        //         in("di") other.as_ptr(),
        //         in("cx") cmp_len,
        //         );
        //     }
        // } {}

        todo!()
    }
}

unsafe impl<'a, T: SeqLockSafe+?Sized> SeqLockGuardedInner for SeqLockGuardedExclusive<'a, T> {
    type T=T;
    unsafe fn new_unchecked(p: *mut T) -> Self {
        Self { p: &mut *p }
    }

    fn to_ptr(&self) -> *mut Self::T {
        self.p as *const Self::T as *mut Self::T
    }
}


unsafe impl<'a, T: SeqLockSafe+?Sized> SeqLockGuardedInner for SeqLockGuardedOptimistic<'a, T> {
    type T = T;

    unsafe fn new_unchecked(p: *mut T) -> Self {
        Self { p, _p: PhantomData }
    }

    fn to_ptr(&self) -> *mut Self::T {
        self.p as *mut T
    }
}

macro_rules! seqlock_accessors {
    (struct $This:ty as $ThisWrapper:ident: $($vis:vis $name:ident : $T:ty),*) => {
        struct $ThisWrapper<T>(pub T);

        impl<T> Deref for $ThisWrapper<T>{
            type Target = T;

        fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

         impl<T> DerefMut for $ThisWrapper<T>{

        fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        unsafe impl SeqLockSafe for $This{type Wrapped<T> = $ThisWrapper<T>;

fn wrap<T>(x: T) -> Self::Wrapped<T> {
        $ThisWrapper(x)
    }

        }

        impl<'a> SeqLockGuardedExclusive<'a,$This>{
            $($vis fn $name<'b>(&'b mut self)-><$T as SeqLockSafe>::Wrapped<SeqLockGuardedExclusive<'b,$T>>{
                unsafe{
                    <$T as SeqLockSafe>::wrap(SeqLockGuardedExclusive::new_unchecked(&mut self.p.$name))
                }
            })*
        }

        impl<'a> SeqLockGuardedOptimistic<'a,$This>{
            $($vis fn $name<'b>(&'b self)-><$T as SeqLockSafe>::Wrapped<SeqLockGuardedOptimistic<'b,$T>>{
                unsafe{
                    <$T as SeqLockSafe>::wrap(SeqLockGuardedOptimistic::new_unchecked(std::ptr::addr_of!((*self.p).$name) as *mut $T))
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

        unsafe impl SeqLockSafe for $T{
            type Wrapped<T> = T;

fn wrap<T>(x: T) -> Self::Wrapped<T> {
        x
    }

        }

        impl SeqLockGuardedExclusive<'_,$T>{
            pub fn store(&mut self,v:$T){
                *self.p=v;
            }
            pub fn load(&self)->$T{
                *self.p
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

macro_rules! seqlock_safe_no_wrap {
    ($($T:ty),*) => {
        $(unsafe impl SeqLockSafe for $T{
            type Wrapped<T> = T;

fn wrap<T>(x: T) -> Self::Wrapped<T> {
        x
    }

        })*
    };
}

seqlock_safe_no_wrap!([u8]);

seqlock_accessors!(struct MyStruct as MyStructWrapper: a:u32,b:i64);