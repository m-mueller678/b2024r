use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use bytemuck::Pod;

mod seqlock_safe;

pub trait SeqLockMode: SeqLockModeImpl {}

pub struct Optimistic;
pub struct Exclusive;
pub struct Shared;

impl SeqLockMode for Optimistic{}
impl SeqLockMode for Exclusive{}
impl SeqLockMode for Shared{}

impl<'a,T> Copy for Guarded<'a,Optimistic,T>{}
impl<'a,T> Copy for Guarded<'a,Shared,T>{}

impl<'a,T,M:SeqLockMode> Clone for Guarded<'a,M,T> where Self:Copy{
    fn clone(&self) -> Self {
        *self
    }
}


unsafe trait SeqLockModeImpl {
    type Pointer<'a,T:?Sized>;
    unsafe fn from_pointer<'a,T>(x:*mut T)->Self::Pointer<'a,T>;
    unsafe fn as_pointer<T>(x:&Self::Pointer<'_,T>)->*mut T;
    unsafe fn load<T:Pod>(p:&Self::Pointer<'_,T>)->T;
    unsafe fn load_slice<T:Pod>(p:&Self::Pointer<'_,[T]>,dst:&mut [MaybeUninit<T>]);
    unsafe fn bit_cmp_slice<T:Pod>(p:&Self::Pointer<'_,[T]>,other:&[T])->Ordering;
}

trait SeqLockModeExclusive:SeqLockMode{
    unsafe fn store<T>(p:&mut Self::Pointer<'_,T>,x:T);
    unsafe fn store_slice<T>(p:&mut Self::Pointer<'_,T>,x:T);
    unsafe fn move_within_slice<T,const MOVE_UP:bool>(p:&mut Self::Pointer<'_,[T]>,distance:usize);
}

struct Guarded<'a,M:SeqLockMode,T>{
    p:M::Pointer<'a,T>,
    _p:PhantomData<&'a T>,
}
