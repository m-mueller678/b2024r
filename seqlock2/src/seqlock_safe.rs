use bytemuck::Pod;

pub trait SeqLockSafe: Pod {
    type Wrapper<T>;
    fn wrap<T>(x: T) -> Self::Wrapper<T>;
    fn get<T>(x: &Self::Wrapper<T>) -> &T;
    fn get_mut<T>(x: &mut Self::Wrapper<T>) -> &mut T;
}

macro_rules! trivial_safe {
    ()=>{
        type Wrapper<T> = T;
        fn wrap<T>(x: T) -> Self::Wrapper<T> {x}
        fn get<T>(x: &Self::Wrapper<T>) -> &T {x}
        fn get_mut<T>(x: &mut Self::Wrapper<T>) -> &mut T {x}
    };
    ($($T:ty),*) => {
        $(impl SeqLockSafe for $T{
            trivial_safe!();
        })*
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

impl<X: SeqLockSafe, const N: usize> SeqLockSafe for [X; N] {
    trivial_safe!();
}
