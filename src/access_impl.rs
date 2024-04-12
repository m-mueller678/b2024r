use super::*;
use std::cmp::Ordering;
use std::sync::atomic::{compiler_fence, AtomicU64, Ordering::*};

#[cfg(target_arch = "x86_64")]
pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), ()> {
    compiler_fence(Acquire);
    if lock.load(Relaxed) == expected {
        Ok(())
    } else {
        Err(())
    }
}

pub struct Exclusive;

pub struct Optimistic;

unsafe impl SeqLockMode for Optimistic {
    type Access<'a, T: 'a + ?Sized> = *const T;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T> {
        p
    }

    fn to_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T {
        *a as *mut T
    }
}

unsafe impl SeqLockMode for Exclusive {
    type Access<'a, T: 'a + ?Sized> = &'a mut T;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T> {
        &mut *p
    }

    fn to_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T {
        *a as *const T as *mut T
    }
}

macro_rules! seqlock_primitive {
    ($(($T:ty) reg=$reg:ident reg_f=$reg_f:literal),*) => {
        $(

        impl SeqLockGuarded<'_,Optimistic,$T>{
            pub fn load(&self)->$T{
                let dst;
                unsafe{
                    #[cfg(target_arch = "x86_64")]
                    core::arch::asm!(
                        concat!("mov ({addr:r}),{dst",$reg_f,"}"),
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
    (u8) reg=reg_byte reg_f="",
    (u16) reg=reg reg_f=":x",
    (u32) reg=reg reg_f=":e",
    (u64) reg=reg reg_f=":r",
    (i8) reg=reg_byte reg_f="",
    (i16) reg=reg reg_f=":x",
    (i32) reg=reg reg_f=":e",
    (i64) reg=reg reg_f=":r"
);

impl<'a> SeqLockGuarded<'a, Optimistic, [u8]> {
    pub fn cmp(&self, other: &[u8]) -> Ordering {
        let cmp_len = self.to_ptr().len().min(other.len());
        if cmp_len == 0 {
            self.to_ptr().len().cmp(&other.len())
        } else {
            let result: i8;
            unsafe {
                core::arch::asm!(
                "repe cmpsb",
                "sete {result}",
                "setb {neg}",
                "xor {result}, 1",
                "shl {neg}, 1",
                "sub {result}, {neg}",
                in("si") self.to_ptr() as *mut u8,
                in("di") other.as_ptr(),
                in("cx") cmp_len,
                neg = lateout(reg_byte) _,
                result = lateout(reg_byte) result,
                );
                let result = std::mem::transmute::<i8, Ordering>(result);
                result.then(self.to_ptr().len().cmp(&other.len()))
            }
        }
    }
}
