#![feature(slice_ptr_len)]
#![feature(slice_ptr_get)]

use std::collections::Bound;
use std::ops::RangeBounds;
use std::ptr::slice_from_raw_parts_mut;

mod access_impl;

pub use access_impl::{optimistic_release, Exclusive, Optimistic};

trait Sealed {}

#[allow(private_bounds)]
pub unsafe trait SeqLockMode: Sealed {
    type Access<'a, T: 'a + ?Sized>;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T>;
    fn as_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T;
}

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
}

#[macro_export]
macro_rules! seqlock_accessors {
    (struct $This:ty as $WrapVis:vis $ThisWrapper:ident: $($vis:vis $name:ident : $T:ty),*) => {
        $WrapVis struct $ThisWrapper<T>($WrapVis T);

        impl<T> core::ops::Deref for $ThisWrapper<T>{
            type Target = T;

        fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

         impl<T> core::ops::DerefMut for $ThisWrapper<T>{

        fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        unsafe impl $crate::SeqLockSafe for $This{
            type Wrapped<T> = $ThisWrapper<T>;

            fn wrap<T>(x: T) -> Self::Wrapped<T> {
                    $ThisWrapper(x)
                }
        }

        impl<'a,M:$crate::SeqLockMode> $ThisWrapper<$crate::SeqLockGuarded<'a,M,$This>>{
            $($vis fn $name<'b>(&'b mut self)-><$T as $crate::SeqLockSafe>::Wrapped<$crate::SeqLockGuarded<'b,M,$T>>{
                unsafe{$crate::wrap_unchecked::<M,$T>(core::ptr::addr_of_mut!((*self.0.as_ptr()).$name))}
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

unsafe impl<X> SeqLockSafe for [X] {
    type Wrapped<T> = T;
    fn wrap<T>(x: T) -> Self::Wrapped<T> {
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
}
