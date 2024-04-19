#![feature(slice_ptr_len)]
#![feature(slice_ptr_get)]
#![feature(never_type)]
#![allow(clippy::missing_safety_doc)]

use std::cmp::Ordering;
use std::collections::Bound;
use std::ops::RangeBounds;
use std::ptr::slice_from_raw_parts_mut;

#[cfg(all(feature = "asm_read", target_arch = "x86_64", not(miri)))]
#[path = "asm_x86_impl.rs"]
mod access_impl;
#[cfg(any(miri, feature = "atomic_byte"))]
#[path = "atomic_byte_impl.rs"]
mod access_impl;

mod lock;

pub use lock::{Guard, SeqLock};

use crate::lock::LockState;
pub use access_impl::{optimistic_release, SeqLockPrimitive};
pub use seqlock_macros::SeqlockAccessors;

pub struct Exclusive;

pub struct OptimisticLockError(());

pub struct Optimistic;
pub unsafe trait SeqLockModeBase {
    type GuardData;
    type ReleaseError;
    type ReleaseData;

    fn release_error() -> Self::ReleaseError;
    fn acquire(s: &LockState) -> Self::GuardData;
    fn release(s: &LockState, d: Self::GuardData) -> Result<Self::ReleaseData, Self::ReleaseError>;
}

#[allow(private_bounds)]
pub unsafe trait SeqLockModeImpl {
    type Access<'a, T: 'a + ?Sized>;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T>;
    fn as_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T;

    unsafe fn load_primitive<P: SeqLockPrimitive>(p: *const P) -> P;
    unsafe fn cmp_bytes(this: *const [u8], other: &[u8]) -> Ordering;
}

#[allow(private_bounds)]
pub trait SeqLockMode: SeqLockModeBase + SeqLockModeImpl {}
impl SeqLockMode for Exclusive {}
impl SeqLockMode for Optimistic {}

pub unsafe fn wrap_unchecked<'a, M: SeqLockMode, T: SeqLockSafe + 'a + ?Sized>(
    p: *mut T,
) -> T::Wrapped<SeqLockGuarded<'a, M, T>> {
    T::wrap(SeqLockGuarded(M::new_unchecked(p)))
}

pub struct SeqLockGuarded<'a, M: SeqLockMode, T: 'a + ?Sized>(M::Access<'a, T>);

impl<'a, M: SeqLockMode, T: 'a + ?Sized> SeqLockGuarded<'a, M, T> {
    pub fn as_ptr(&self) -> *mut T {
        M::as_ptr(&self.0)
    }
}

impl<'a, T: 'a + ?Sized + SeqLockSafe> SeqLockGuarded<'a, Exclusive, T> {
    pub fn optimistic(&self) -> T::Wrapped<SeqLockGuarded<'a, Optimistic, T>> {
        unsafe { wrap_unchecked(self.as_ptr()) }
    }
}

pub unsafe trait SeqLockSafe {
    type Wrapped<T>;
    fn wrap<T>(x: T) -> Self::Wrapped<T>;
    fn unwrap_ref<T>(x: &Self::Wrapped<T>) -> &T;
    fn unwrap_mut<T>(x: &mut Self::Wrapped<T>) -> &mut T;
}

#[macro_export]
macro_rules! seqlock_wrapper {
    ($v:vis $T:ident) => {
        $v struct $T<T>($v T);

        impl<T> core::ops::Deref for $T<T>{
            type Target = T;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl<T> core::ops::DerefMut for $T<T>{
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

macro_rules! seqlock_safe_no_wrap {
    ($($T:ty),*) => {
        $(unsafe impl SeqLockSafe for $T{
            type Wrapped<T> = T;
            fn wrap<T>(x: T) -> Self::Wrapped<T> { x }
            fn unwrap_ref<T>(x: &Self::Wrapped<T>) -> &T { x }
            fn unwrap_mut<T>(x: &mut Self::Wrapped<T>) -> &mut T { x }
        })*
    };
}

unsafe impl<X> SeqLockSafe for [X] {
    type Wrapped<T> = T;
    fn wrap<T>(x: T) -> Self::Wrapped<T> {
        x
    }

    fn unwrap_ref<T>(x: &Self::Wrapped<T>) -> &T {
        x
    }
    fn unwrap_mut<T>(x: &mut Self::Wrapped<T>) -> &mut T {
        x
    }
}

seqlock_safe_no_wrap!(u8, u16, u32, u64, i8, i16, i32, i64);

impl<'a, T: SeqLockSafe, M: SeqLockMode> SeqLockGuarded<'a, M, [T]> {
    #[inline]
    pub fn slice(&mut self, i: impl RangeBounds<usize>) -> SeqLockGuarded<'a, M, [T]> {
        let array = self.as_ptr();
        let mut ptr = array.as_mut_ptr();
        let mut len = array.len();
        match i.end_bound() {
            Bound::Included(&x) => {
                assert!(x < len);
                len = x + 1;
            }
            Bound::Excluded(&x) => {
                assert!(x <= len);
                len = x;
            }
            Bound::Unbounded => {}
        }
        unsafe {
            match i.start_bound() {
                Bound::Included(&x) => {
                    assert!(x <= len);
                    ptr = ptr.add(x);
                    len -= x;
                }
                Bound::Excluded(&x) => {
                    assert!(x < len);
                    ptr = ptr.add(x + 1);
                    len -= x + 1;
                }
                Bound::Unbounded => {}
            }
            SeqLockGuarded(M::new_unchecked(slice_from_raw_parts_mut(ptr, len)))
        }
    }

    pub fn index(&mut self, i: usize) -> T::Wrapped<SeqLockGuarded<'a, M, T>> {
        assert!(i < self.as_ptr().len());
        unsafe { wrap_unchecked(self.as_ptr().as_mut_ptr().add(i)) }
    }

    pub fn len(&self) -> usize {
        self.as_ptr().len()
    }
}

unsafe impl<E, const N: usize> SeqLockSafe for [E; N] {
    type Wrapped<T> = T;

    fn wrap<T>(x: T) -> Self::Wrapped<T> {
        x
    }

    fn unwrap_ref<T>(x: &Self::Wrapped<T>) -> &T {
        x
    }
    fn unwrap_mut<T>(x: &mut Self::Wrapped<T>) -> &mut T {
        x
    }
}

impl<'a, M: SeqLockMode, T: SeqLockPrimitive> SeqLockGuarded<'a, M, T> {
    pub fn load(&self) -> T {
        unsafe { M::load_primitive(self.as_ptr()) }
    }
}

impl<'a, M: SeqLockMode> SeqLockGuarded<'a, M, [u8]> {
    pub fn cmp_bytes(&self, other: &[u8]) -> Ordering {
        unsafe { M::cmp_bytes(self.as_ptr(), other) }
    }
}

seqlock_safe_no_wrap!(());
