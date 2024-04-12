#![feature(slice_ptr_len)]

use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of_mut, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::from_raw_parts_mut;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};

unsafe trait SeqLockMode{
    type Access<'a,T:'a>;

    unsafe fn new_unchecked<'a,T:'a>(p: *mut T) -> Self::Access<'a,T>;
    fn to_ptr<'a,T:'a>(a:&Self::Access<'a,T>) -> *mut T;
}

unsafe fn wrap_unchecked<'a,M:SeqLockMode,T:SeqLockSafe+'a>(p: *mut T) -> T::Wrapped<SeqLockGuarded<'a,M,T>> {
    T::wrap(SeqLockGuarded(M::new_unchecked(p)))
}

pub struct SeqLockGuarded<'a,M:SeqLockMode,T:'a> (M::Access<'a,T>);

impl <'a,M:SeqLockMode,T:'a> SeqLockGuarded<'a,M,T>{
    fn to_ptr(&self) -> *mut T{
        M::to_ptr(&self.0)
    }
}

struct MyStruct {
    a: u32,
    b: i64,
}

fn main() {
    unsafe {
        let x = &mut MyStruct { a: 1, b: 2 };
        let mut x = wrap_unchecked::<Exclusive,MyStruct>(x);
        let a=x.a();
        dbg!(x.a().load());
        dbg!(x.b().load());
    }
}

unsafe trait SeqLockSafe {
    type Wrapped<T>;
    fn wrap<T>(x: T) -> Self::Wrapped<T>;
}

#[cfg(target_arch = "x86_64")]
pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), ()> {
    compiler_fence(Ordering::Acquire);
    if lock.load(Relaxed) == expected {
        Ok(())
    } else {
        Err(())
    }
}

pub struct Exclusive;
pub struct Optimistic;

// impl<'a> SeqLockGuarded<SeqLockGuardedOptimistic<'a, [u8]>> {
//     fn cmp(&self, other: &[u8]) -> Ordering {
//         // let cmp_len = self.g.p.len().min(other.len());
//         // if cmp_len == 0 || {
//         //     unsafe {
//         //         core::arch::asm!(
//         //         "repe cmpsb",
//         //         in("si") self.g.p as *mut u8,
//         //         in("di") other.as_ptr(),
//         //         in("cx") cmp_len,
//         //         );
//         //     }
//         // } {}
//
//         todo!()
//     }
// }

unsafe impl SeqLockMode for Optimistic {
    type Access<'a, T:'a> = *const T;

    unsafe fn new_unchecked<'a, T:'a>(p: *mut T) -> Self::Access<'a, T> {
        p
    }

    fn to_ptr<'a,T:'a>(a: &Self::Access<'a,T>) -> *mut T {
        *a as *mut T
    }
}

unsafe impl SeqLockMode for Exclusive {
    type Access<'a,T:'a> = &'a mut T;

    unsafe fn new_unchecked<'a, T>(p: *mut T) -> Self::Access<'a, T> {
        &mut *p
    }

    fn to_ptr<'a,T:'a>(a: &Self::Access<'a,T>) -> *mut T {
        *a as *const T as *mut T
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

        unsafe impl SeqLockSafe for $This{
            type Wrapped<T> = $ThisWrapper<T>;

            fn wrap<T>(x: T) -> Self::Wrapped<T> {
                    $ThisWrapper(x)
                }
        }

        impl<'a,M:SeqLockMode> $ThisWrapper<SeqLockGuarded<'a,M,$This>>{
            $($vis fn $name<'b>(&'b mut self)-><$T as SeqLockSafe>::Wrapped<SeqLockGuarded<'b,M,$T>>{
                unsafe{wrap_unchecked::<M,$T>(addr_of_mut!((*self.0.to_ptr()).$name))}
            })*
        }
    };
}

macro_rules! seqlock_safe_no_wrap {
    ($($T:ty),*) => {
        $(unsafe impl SeqLockSafe for $T{
            type Wrapped<T> = T;
            fn wrap<T>(x: T) -> Self::Wrapped<T> { x }
        })*
    };
}

macro_rules! seqlock_primitive {
    ($(($T:ty) reg=$reg:ident),*) => {
        $(
        seqlock_safe_no_wrap!($T);

        impl SeqLockGuarded<'_,Optimistic,$T>{
            pub fn load(&self)->$T{
                let dst;
                unsafe{
                    #[cfg(target_arch = "x86_64")]
                    core::arch::asm!(
                        "mov ({addr}),{dst}",
                        addr = in(reg) self.0,
                        dst = lateout($reg) dst
                    );
                }
                dst
            }
        }

        impl SeqLockGuarded<'_,Exclusive,$T>{
            pub fn store(&mut self,v:$T){
                *self.0=v;
            }
            pub fn load(&self)->$T{
                *self.0
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

seqlock_safe_no_wrap!([u8]);

seqlock_accessors!(struct MyStruct as MyStructWrapper: a:u32,b:i64);
