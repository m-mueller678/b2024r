pub trait SeqLockWrappable {
    type Wrapper<T>;
    fn wrap<T>(x: T) -> Self::Wrapper<T>;
    fn get<T>(x: &Self::Wrapper<T>) -> &T;
    fn get_mut<T>(x: &mut Self::Wrapper<T>) -> &mut T;
}

pub trait SeqLockWrappableIdentity {}
impl<X: SeqLockWrappableIdentity + ?Sized> SeqLockWrappable for X {
    type Wrapper<T> = T;
    fn wrap<T>(x: T) -> Self::Wrapper<T> {
        x
    }
    fn get<T>(x: &Self::Wrapper<T>) -> &T {
        x
    }
    fn get_mut<T>(x: &mut Self::Wrapper<T>) -> &mut T {
        x
    }
}

macro_rules! trivial_safe {
    ($($T:ty),*) => {
        $(impl SeqLockWrappableIdentity for $T{})*
    };
}

trivial_safe!(
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    ()
);

impl<X: SeqLockWrappable, const N: usize> SeqLockWrappableIdentity for [X; N] {}
impl<X: SeqLockWrappable> SeqLockWrappableIdentity for [X] {}

#[macro_export]
macro_rules! seqlock_wrapper {
    ($v:vis $T:ident) => {
        #[derive(Clone,Copy)]
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
