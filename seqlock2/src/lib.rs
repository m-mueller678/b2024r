use bytemuck::Pod;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

mod seqlock_safe;
pub use access_impl::optimistic_release;
pub use seqlock_safe::SeqLockSafe;
#[path = "atomic_byte_impl.rs"]
mod access_impl;

#[allow(private_bounds)]
pub trait SeqLockMode: SeqLockModeImpl {}
#[allow(private_bounds)]
pub trait SeqLockModeExclusive: SeqLockMode + SeqLockModeExclusiveImpl {}

pub struct Optimistic;
pub struct Exclusive;
pub struct Shared;

impl SeqLockMode for Optimistic {}
impl SeqLockMode for Exclusive {}
impl SeqLockModeExclusive for Exclusive {}
impl SeqLockMode for Shared {}

impl<'a, T> Copy for Guarded<'a, Optimistic, T> {}
impl<'a, T> Copy for Guarded<'a, Shared, T> {}

impl<'a, T, M: SeqLockMode> Clone for Guarded<'a, M, T>
where
    Self: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

unsafe trait SeqLockModeImpl {
    type Pointer<'a, T: ?Sized>;
    unsafe fn from_pointer<'a, T>(x: *mut T) -> Self::Pointer<'a, T>;
    fn as_pointer<T>(x: &Self::Pointer<'_, T>) -> *mut T;
    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T;
    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]);
    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering;
}

impl<'a, M: SeqLockMode, T: SeqLockSafe> Guarded<'a, M, T> {
    pub unsafe fn map_ptr_mut<'b, U: SeqLockSafe>(
        &'b mut self,
        f: impl FnOnce(*mut T) -> *mut U,
    ) -> U::Wrapper<Guarded<'b, M, U>> {
        Guarded::wrap_unchecked(f(M::as_pointer(&self.p)))
    }

    pub unsafe fn wrap_unchecked(p: *mut T) -> T::Wrapper<Guarded<'a, M, T>> {
        T::wrap(Guarded {
            p: M::from_pointer(p),
            _p: PhantomData,
        })
    }

    pub fn as_pointer(&self) -> *mut T {
        M::as_pointer(&self.p)
    }

    pub fn load(&self) -> T {
        unsafe { M::load(&self.p) }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockSafe + Sized> crate::Guarded<'a, M, [T]> {
    pub fn load_slice_uninit(&self, dst: &mut [MaybeUninit<T>]) {
        unsafe { M::load_slice(&self.p, dst) }
    }

    pub unsafe fn bit_cmp_slice(&self, other: &[T]) -> Ordering {
        unsafe { M::bit_cmp_slice(&self.p, other) }
    }
}

unsafe trait SeqLockModeExclusiveImpl: SeqLockModeImpl {
    unsafe fn store<T>(p: &mut Self::Pointer<'_, T>, x: T);
    unsafe fn store_slice<T>(p: &mut Self::Pointer<'_, T>, x: T);
    unsafe fn move_within_slice<T, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        distance: usize,
    );
}

pub struct Guarded<'a, M: SeqLockMode, T: ?Sized> {
    p: M::Pointer<'a, T>,
    _p: PhantomData<&'a T>,
}

pub struct OptimisticLockError(());
