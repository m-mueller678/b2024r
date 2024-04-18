use super::*;
use std::cmp::Ordering;
use std::mem::{size_of, transmute};
use std::sync::atomic::{fence, AtomicU64, Ordering::*};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), OptimisticLockError> {
    fence(Acquire);
    if lock.load(Relaxed) == expected {
        Ok(())
    } else {
        Err(OptimisticLockError(()))
    }
}

unsafe impl SeqLockModeImpl for Optimistic {
    type Access<'a, T: 'a + ?Sized> = *const T;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T> {
        p
    }

    fn as_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T {
        *a as *mut T
    }

    unsafe fn load_primitive<P: SeqLockPrimitive>(p: *const P) -> P {
        P::asm_load(p)
    }

    unsafe fn cmp_bytes(this: *const [u8], other: &[u8]) -> Ordering {
        let cmp_len = this.len().min(other.len());
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
            in("si") this.as_ptr(),
            in("di") other.as_ptr(),
            in("cx") cmp_len,
            neg = lateout(reg_byte) _,
            result = lateout(reg_byte) result,
            options(readonly,nostack)
            );
            let result = transmute::<i8, Ordering>(result);
            result.then(this.len().cmp(&other.len()))
        }
    }
}

unsafe impl SeqLockModeImpl for Exclusive {
    type Access<'a, T: 'a + ?Sized> = &'a mut T;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T> {
        &mut *p
    }

    fn as_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T {
        *a as *const T as *mut T
    }

    unsafe fn load_primitive<P: SeqLockPrimitive>(p: *const P) -> P {
        unsafe { *p }
    }

    unsafe fn cmp_bytes(this: *const [u8], other: &[u8]) -> Ordering {
        Optimistic::cmp_bytes(this,other)
    }
}

macro_rules! seqlock_primitive{
    ($(($T:ident) reg=$reg:ident reg_f=$reg_f:literal),*) =>{
        $(impl SeqLockPrimitive for $T{
            fn asm_load(addr:*const $T)->$T{
                let dst;
                unsafe{
                    core::arch::asm!(
                        concat!("mov {dst",$reg_f,"}, [{addr:r}]"),
                        addr = in(reg) addr,
                        dst = lateout($reg) dst,
                        options(readonly,preserves_flags,nostack)
                    );
                }
                dst
            }
        }

        impl SeqLockGuarded<'_,Exclusive,$T>{
            pub fn store(&mut self,v:$T){
                *self.0=v;
            }
        })*
    }
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
    pub fn load(&self, dest: &mut [u8]) {
        assert_eq!(self.as_ptr().len(), dest.len());
        unsafe {
            core::arch::asm!(
            "rep movsb",
            in("si") self.as_ptr().as_mut_ptr(),
            in("di") dest.as_ptr(),
            in("cx") dest.len(),
            options(nostack,preserves_flags)
            );
        }
    }
}

impl<'a> SeqLockGuarded<'a, Exclusive, [u8]> {
    pub fn store(&mut self, src: &[u8]) {
        self.0.copy_from_slice(src);
    }
}

pub trait SeqLockPrimitive: Copy {
    #[doc(hidden)]
    fn asm_load(p: *const Self) -> Self;
}
