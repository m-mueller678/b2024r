use std::marker::PhantomData;

pub struct OPtr<'a, T> {
    p: *const T,
    _p: PhantomData<&'a T>,
}
