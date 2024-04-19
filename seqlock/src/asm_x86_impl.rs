use super::*;
use std::cmp::Ordering;
use std::mem::{align_of, MaybeUninit, size_of, transmute};
use std::sync::atomic::{fence, AtomicU64, Ordering::*};
use bytemuck::Pod;

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

macro_rules! asm_memcpy{
    ($align_var:expr,$len:expr,$src:expr,$dst:expr;$($align:expr,$inst:literal;)*)=>{
        $(
        if $align >= $align_var{
             core::arch::asm!(
                std::concat!("rep ",$inst),
                in("si") $src as *const u8,
                in("di") $dst as *const u8,
                in("cx") $len/$align,
                options(nostack,preserves_flags),
            );
            return;
        }
        )*
    }
}

unsafe fn asm_memcpy<T>(src:*const [T],dst:*const [T]){
    assert_eq!(src.len(),dst.len());
    let len=std::mem::size_of::<T>()*src.len();
    let align= std::mem::align_of::<T>();
    asm_memcpy!(align,len,src,dst;8,"movsb";4,"movsb";2,"movsb";1,"movsb";);
}


impl<'a,M:SeqLockMode,T:Pod> SeqLockGuarded<'a, M, [T]> {
    pub fn load_slice(&self, dst: &mut [T]) {
        unsafe{
            asm_memcpy(self.as_ptr(),dst);
        }
    }
}

impl<T:SeqLockPrimitive,const N:usize> SeqLockPrimitive for [T;N]{
    fn asm_load(p: *const Self) -> Self {
        unsafe{
            let mut dst=MaybeUninit::<[T;N]>::uninit();
            asm_memcpy(p,dst.as_ptr());
            dst.assume_init()
        }
    }
}

impl<'a> SeqLockGuarded<'a, Exclusive, [u8]> {
    pub fn store_slice(&mut self, src: &[u8]) {
        unsafe{
            asm_memcpy(src,self.as_ptr());
        }
    }
}

pub trait SeqLockPrimitive: Pod {
    #[doc(hidden)]
    fn asm_load(p: *const Self) -> Self;
}
