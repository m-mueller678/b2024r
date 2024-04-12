#![feature(slice_ptr_len)]

use access_impl::{Exclusive, Optimistic};
use std::ops::{Deref, DerefMut};
use std::ptr::addr_of_mut;

mod access_impl;

unsafe trait SeqLockMode {
    type Access<'a, T: 'a + ?Sized>;

    unsafe fn new_unchecked<'a, T: 'a + ?Sized>(p: *mut T) -> Self::Access<'a, T>;
    fn to_ptr<'a, T: 'a + ?Sized>(a: &Self::Access<'a, T>) -> *mut T;
}

unsafe fn wrap_unchecked<'a, M: SeqLockMode, T: SeqLockSafe + 'a + ?Sized>(
    p: *mut T,
) -> T::Wrapped<SeqLockGuarded<'a, M, T>> {
    T::wrap(SeqLockGuarded(M::new_unchecked(p)))
}

pub struct SeqLockGuarded<'a, M: SeqLockMode, T: 'a + ?Sized>(M::Access<'a, T>);

impl<'a, M: SeqLockMode, T: 'a + ?Sized> SeqLockGuarded<'a, M, T> {
    fn to_ptr(&self) -> *mut T {
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
        let mut x = wrap_unchecked::<Exclusive, MyStruct>(x);
        let a = x.a();
        dbg!(x.a().load());
        dbg!(x.b().load());
    }
}

unsafe trait SeqLockSafe {
    type Wrapped<T>;
    fn wrap<T>(x: T) -> Self::Wrapped<T>;
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

seqlock_safe_no_wrap!([u8], u8, u16, u32, u64, i8, i16, i32, i64);

seqlock_accessors!(struct MyStruct as MyStructWrapper: a:u32,b:i64);

#[test]
fn test_memcmp() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    for a in &samples {
        for b in &samples {
            let std = a.cmp(b);
            let optimistic = unsafe {
                wrap_unchecked::<Optimistic, [u8]>(a.as_bytes() as *const [u8] as *mut [u8])
                    .cmp(b.as_bytes())
            };
            assert_eq!(std, optimistic);
        }
    }
}
