pub trait Wrapper<T: Sized>: Sized {
    fn wrap(x: T) -> Self;
    fn rewrap(x: impl Wrapper<T>) -> Self {
        Self::wrap(x.dewrap())
    }
    fn get(&self) -> &T;
    fn dewrap(self) -> T;
    fn get_mut(&mut self) -> &mut T;
}

pub trait SeqLockWrappable {
    type Wrapper<T>: Wrapper<T>;
}

impl<T> Wrapper<T> for T {
    fn wrap(x: T) -> Self {
        x
    }
    fn get(&self) -> &T {
        self
    }
    fn dewrap(self) -> T {
        self
    }
    fn get_mut(&mut self) -> &mut T {
        self
    }
}

pub trait SeqLockWrappableIdentity {}
impl<X: SeqLockWrappableIdentity + ?Sized> SeqLockWrappable for X {
    type Wrapper<T> = T;
}

macro_rules! trivial_safe {
    ($($T:ty),*) => {
        $(impl SeqLockWrappableIdentity for $T{})*
    };
}

trivial_safe!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, ());

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
        impl<T> $crate::Wrapper<T> for $T<T>{
            fn wrap(x: T) -> Self {$T(x)}
            fn get(&self) -> &T { &self.0 }
            fn dewrap(self) -> T { self.0 }
            fn get_mut(&mut self) -> &mut T { &mut self.0 }
        }
    };
}
