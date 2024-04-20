use crate::{Exclusive, Optimistic, OptimisticLockError, SeqLockGuarded, SeqLockModeImpl};
use core::mem::{size_of, MaybeUninit};
use std::cmp::Ordering;
use std::sync::atomic::{compiler_fence, AtomicU64, AtomicU8, Ordering::*};

pub fn optimistic_release(lock: &AtomicU64, expected: u64) -> Result<(), OptimisticLockError> {
    compiler_fence(Acquire);
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

    unsafe fn load_primitive<P: crate::SeqLockPrimitive>(p: *const P) -> P {
        load_primitive(p)
    }
}

unsafe impl SeqLockModeImpl for Exclusive {
    type Access<'a, T: 'a + ?Sized> = *const T;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T> {
        p
    }

    fn as_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T {
        *a as *mut T
    }

    unsafe fn load_primitive<P: crate::SeqLockPrimitive>(p: *const P) -> P {
        load_primitive(p)
    }
}

unsafe fn load_primitive<P: crate::SeqLockPrimitive>(p: *const P) -> P {
    unsafe {
        let mut ret = MaybeUninit::<P>::uninit();
        for i in 0..size_of::<P>() {
            (ret.as_mut_ptr() as *mut u8)
                .add(i)
                .write((*(p as *const AtomicU8).add(i)).load(Relaxed))
        }
        ret.assume_init()
    }
}

macro_rules! seqlock_primitive {
    ($(($T:ty) reg=$reg:ident reg_f=$reg_f:literal),*) => {
        $(

        impl SeqLockGuarded<'_,Exclusive,$T>{
            pub fn store(&mut self,v:$T){
                unsafe{
                    for (i,x) in v.to_ne_bytes().iter().enumerate(){
                    (*(self.0 as *const AtomicU8).add(i)).store(*x,Relaxed);
                }
                }
            }
        }

        impl SeqLockPrimitive for $T{}
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
        let mut this = (0..self.0.len())
            .map(|i| unsafe { (*(self.0 as *const AtomicU8).add(i)).load(Relaxed) });
        let mut other = other.iter().copied();
        for _ in 0..self.0.len() {
            let c = this.next().cmp(&other.next());
            if !c.is_eq() {
                return c;
            }
        }
        this.next().cmp(&other.next())
    }

    pub fn load(&self, dest: &mut [u8]) {
        for i in 0..self.0.len() {
            dest[i] = unsafe { (*(self.0 as *const AtomicU8).add(i)).load(Relaxed) };
        }
    }
}

impl<'a> SeqLockGuarded<'a, Exclusive, [u8]> {
    pub fn store(&mut self, src: &[u8]) {
        for i in 0..self.0.len() {
            unsafe { (*(self.0 as *const AtomicU8).add(i)).store(src[i], Relaxed) };
        }
    }
}

pub trait SeqLockPrimitive: Copy {}
