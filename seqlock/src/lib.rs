#![feature(never_type)]
#![feature(pointer_is_aligned_to)]
#![feature(layout_for_ptr)]
#![feature(strict_provenance)]
extern crate core;

use bytemuck::Pod;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{align_of, align_of_val_raw, size_of, transmute, MaybeUninit};
use std::ops::{Bound, Range, RangeBounds};
use std::ptr::slice_from_raw_parts_mut;
use libc::abort;

pub mod unwind;
mod wrappable;

pub use lock::{BmExt, BufferManager, Guard, LockState};
pub use seqlock_macros::SeqlockAccessors;
pub use wrappable::{SeqLockWrappable, Wrapper};
pub use default_bm::DefaultBm;

#[derive(Debug)]
pub enum Never {}

impl From<Never> for OptimisticLockError {
    fn from(_: Never) -> Self {
        unreachable!()
    }
}

impl From<Never> for () {
    fn from(_: Never) -> Self {
        unreachable!()
    }
}

mod access_impl;

mod lock;
mod default_bm;

#[allow(private_bounds)]
pub trait SeqLockMode: SeqLockModeImpl + 'static {
    type SharedDowngrade: SeqLockMode;
    type GuardData: Copy + Debug;
    type ReleaseData;
    const PESSIMISTIC: bool;
    const EXCLUSIVE: bool;

    fn acquire<'bm, BM: BufferManager<'bm>>(bm: BM, page_id: u64) -> (&'bm UnsafeCell<BM::Page>, Self::GuardData);
    fn release<'bm, BM: BufferManager<'bm>>(
        bm: BM,
        page_address: usize,
        guard_data: Self::GuardData,
    ) -> Self::ReleaseData;

    fn release_error() -> !;
}

#[allow(private_bounds)]
pub trait SeqLockModeExclusive: SeqLockModeExclusiveImpl + SeqLockModePessimistic {}

pub struct Optimistic;

pub struct Exclusive;

pub struct Shared;

impl SeqLockMode for Optimistic {
    type GuardData = u64;
    type ReleaseData = ();
    const PESSIMISTIC: bool = false;
    const EXCLUSIVE: bool = false;

    type SharedDowngrade = Optimistic;

    fn release_error() -> ! {
        unwind::start()
    }

    fn acquire<'bm, BM: BufferManager<'bm>>(bm: BM, page_id: u64) -> (&'bm UnsafeCell<BM::Page>, Self::GuardData) {
        bm.acquire_optimistic(page_id)
    }

    fn release<'bm, BM: BufferManager<'bm>>(
        bm: BM,
        page_address: usize,
        guard_data: Self::GuardData,
    ) -> Self::ReleaseData {
        if !std::thread::panicking(){
            bm.release_optimistic(page_address, guard_data)
        }
    }
}

impl SeqLockModeExclusive for Exclusive {}

impl SeqLockModePessimistic for Exclusive {}

impl SeqLockModePessimistic for Shared {}

pub trait SeqLockModePessimistic: SeqLockMode {}

impl SeqLockMode for Exclusive {
    const PESSIMISTIC: bool = true;
    const EXCLUSIVE: bool = true;
    type SharedDowngrade = Shared;

    // track if guard has been written to
    #[cfg(debug_assertions)]
    type GuardData = bool;
    #[cfg(not(debug_assertions))]
    type GuardData = ();
    type ReleaseData = u64;

    fn release_error() -> ! {
        unreachable!()
    }

    fn acquire<'bm, BM: BufferManager<'bm>>(bm: BM, page_id: u64) -> (&'bm UnsafeCell<BM::Page>, Self::GuardData) {
        let p = bm.acquire_exclusive(page_id);
        #[cfg(debug_assertions)]
        {
            (p, false)
        }
        #[cfg(not(debug_assertions))]
        {
            (p, ())
        }
    }

    fn release<'bm, BM: BufferManager<'bm>>(
        bm: BM,
        page_address: usize,
        guard_data: Self::GuardData,
    ) -> Self::ReleaseData {
        #[cfg(debug_assertions)]
        if std::thread::panicking() && guard_data {
            eprintln!("unwinding out of written exclusive lock");
            unsafe{
                abort()
            }
        }
        bm.release_exclusive(page_address)
    }
}

impl SeqLockMode for Shared {
    const PESSIMISTIC: bool = false;
    const EXCLUSIVE: bool = false;
    type SharedDowngrade = Shared;
    type GuardData = ();
    type ReleaseData = u64;

    fn acquire<'bm, BM: BufferManager<'bm>>(_bm: BM, _page_id: u64) -> (&'bm UnsafeCell<BM::Page>, Self::GuardData) {
        unimplemented!()
    }

    fn release<'bm, BM: BufferManager<'bm>>(
        _bm: BM,
        _page_address: usize,
        _guard_data: Self::GuardData,
    ) -> Self::ReleaseData {
        unimplemented!()
    }

    fn release_error() -> ! {
        unreachable!()
    }
}

impl<'a, T: ?Sized> Copy for Guarded<'a, Optimistic, T> {}

impl<'a, T: ?Sized> Copy for Guarded<'a, Shared, T> {}

impl<'a, T: ?Sized, M: SeqLockMode> Clone for Guarded<'a, M, T>
where
    Self: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

#[allow(clippy::missing_safety_doc)]
unsafe trait SeqLockModeImpl {
    type Pointer<'a, T: ?Sized + 'a>;
    unsafe fn from_pointer<'a, T: 'a + ?Sized>(x: *mut T) -> Self::Pointer<'a, T>;
    fn as_ptr<'a, T: 'a + ?Sized>(x: &Self::Pointer<'a, T>) -> *mut T;
    unsafe fn load<T: Pod>(p: &Self::Pointer<'_, T>) -> T;
    unsafe fn load_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, dst: &mut [MaybeUninit<T>]);
    unsafe fn bit_cmp_slice<T: Pod>(p: &Self::Pointer<'_, [T]>, other: &[T]) -> Ordering;
    unsafe fn copy_slice_non_overlapping<T: Pod>(
        p: &Self::Pointer<'_, [T]>,
        dst: &mut <Exclusive as SeqLockModeImpl>::Pointer<'_, [T]>,
    );
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + ?Sized> Guarded<'a, M, T> {
    pub fn b(&mut self) -> T::Wrapper<Guarded<'_, M, T>> {
        unsafe { Guarded::wrap_unchecked(self.as_ptr()) }
    }

    pub fn s(&self) -> T::Wrapper<Guarded<'_, M::SharedDowngrade, T>> {
        unsafe { Guarded::wrap_unchecked(self.as_ptr()) }
    }

    pub fn optimistic(&self) -> T::Wrapper<Guarded<'_, Optimistic, T>> {
        unsafe { Guarded::wrap_unchecked(self.as_ptr()) }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn map_ptr<U: SeqLockWrappable + ?Sized>(
        self,
        f: impl FnOnce(*mut T) -> *mut U,
    ) -> U::Wrapper<Guarded<'a, M, U>> {
        Guarded::wrap_unchecked(f(M::as_ptr(&self.p)))
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn wrap_unchecked(p: *mut T) -> T::Wrapper<Guarded<'a, M, T>> {
        T::Wrapper::wrap(Guarded { p: M::from_pointer(p), _p: PhantomData })
    }

    pub fn wrap_mut(p: &'a mut T) -> T::Wrapper<Guarded<'a, M, T>> {
        unsafe { T::Wrapper::wrap(Guarded { p: M::from_pointer(p), _p: PhantomData }) }
    }

    pub fn as_ptr(&self) -> *mut T {
        let ptr = M::as_ptr(&self.p);
        let align = unsafe { align_of_val_raw(ptr) };
        debug_assert!(ptr.is_aligned_to(align));
        unsafe {
            std::hint::assert_unchecked(ptr.is_aligned_to(align));
        }
        ptr
    }

    pub fn load(&self) -> T
    where
        T: Pod,
    {
        unsafe { M::load(&self.p) }
    }

    pub fn cast<U: Pod + SeqLockWrappable>(self) -> U::Wrapper<Guarded<'a, M, U>>
    where
        T: Pod,
    {
        assert_eq!(size_of::<U>(), size_of::<T>());
        assert_eq!(align_of::<T>() % align_of::<U>(), 0);
        unsafe { Guarded::wrap_unchecked(self.as_ptr() as *mut U) }
    }

    pub fn store(&mut self, x: T)
    where
        T: Pod,
        M: SeqLockModeExclusive,
    {
        unsafe { M::store(&mut self.p, x) }
    }

    pub fn update(&mut self, f: impl FnOnce(T) -> T) -> T
    where
        T: Pod,
        M: SeqLockModeExclusive,
    {
        let x = f(self.load());
        self.store(x);
        x
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + Pod> Guarded<'a, M, [T]> {
    pub fn iter(self) -> impl Iterator<Item = T::Wrapper<Guarded<'a, M, T>>> {
        let p = self.as_ptr() as *mut T;
        (0..self.len()).map(move |i| unsafe { Guarded::wrap_unchecked(p.add(i)) })
    }

    pub fn copy_to(&self, dst: &mut Guarded<Exclusive, [T]>) {
        assert_eq!(dst.len(), self.len());
        unsafe { M::copy_slice_non_overlapping(&self.p, &mut dst.p) };
    }

    pub fn load_slice_uninit(&self, dst: &mut [MaybeUninit<T>]) {
        assert_eq!(self.len(), dst.len());
        unsafe { M::load_slice(&self.p, dst) }
    }

    pub fn load_slice<'dst>(&self, dst: &'dst mut [T]) {
        assert_eq!(self.len(), dst.len());
        unsafe { M::load_slice(&self.p, transmute::<&'dst mut [T], &'dst mut [MaybeUninit<T>]>(dst)) }
    }

    pub fn load_slice_into<'dst>(&self, dst: &'dst mut [T]) -> &'dst mut [T] {
        let len = self.len();
        let dst = &mut dst[..len];
        self.load_slice(dst);
        dst
    }

    pub fn load_slice_to_vec(&self) -> Vec<T> {
        let mut dst = vec![T::zeroed(); self.len()];
        self.load_slice(&mut dst);
        dst
    }

    pub fn len(&self) -> usize {
        self.as_ptr().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_ptr().is_empty()
    }

    pub fn cast_slice<U: Pod + SeqLockWrappable>(self) -> Guarded<'a, M, [U]> {
        let ptr = M::as_ptr(&self.p);
        let byte_len = ptr.len() * size_of::<T>();
        if byte_len % size_of::<U>() != 0 {
            M::release_error();
        }
        let ptr = ptr as *mut U;
        if !ptr.is_aligned() {
            M::release_error();
        }
        unsafe { Guarded::wrap_unchecked(slice_from_raw_parts_mut(ptr, byte_len / size_of::<U>())) }
    }

    pub fn mem_cmp(&self, other: &[T]) -> Ordering {
        unsafe { M::bit_cmp_slice(&self.p, other) }
    }

    pub fn store_slice(&mut self, x: &[T])
    where
        M: SeqLockModeExclusive,
    {
        assert_eq!(self.len(), x.len());
        unsafe { M::store_slice::<T>(&mut self.p, x) }
    }
    pub fn move_within_by<const MOVE_UP: bool>(&mut self, src_range: Range<usize>, distance: usize)
    where
        M: SeqLockModeExclusive,
    {
        if !MOVE_UP {
            assert!(distance <= src_range.start);
        }
        unsafe {
            M::move_within_slice_to::<T, MOVE_UP>(
                &mut self.p,
                src_range.clone(),
                if MOVE_UP { src_range.start + distance } else { src_range.start - distance },
            );
        }
    }

    pub fn move_within_to<const MOVE_UP: bool>(&mut self, src_range: Range<usize>, dst: usize)
    where
        M: SeqLockModeExclusive,
    {
        if MOVE_UP {
            assert!(dst > src_range.start);
        } else {
            assert!(dst < src_range.start);
        }
        assert!(src_range.start.max(dst) + src_range.len() <= self.as_ptr().len());
        unsafe {
            M::move_within_slice_to::<T, MOVE_UP>(&mut self.p, src_range, dst);
        }
    }

    pub fn index(self, i: usize) -> T::Wrapper<Guarded<'a, M, T>> {
        let ptr: *mut [T] = self.as_ptr();
        if i >= ptr.len() {
            M::release_error()
        }
        unsafe { Guarded::wrap_unchecked((ptr as *mut T).add(i)) }
    }

    pub fn slice(self, x: impl RangeBounds<usize>) -> Self {
        let ptr: *mut [T] = self.as_ptr();
        let mut start = ptr as *mut T;
        let mut len = ptr.len();
        let upper = match x.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(&i) => Some(i + 1),
            Bound::Excluded(&i) => Some(i),
        };
        if let Some(upper) = upper {
            if upper > len {
                M::release_error()
            }
            len = upper;
        }
        let lower = match x.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(&i) => Some(i),
            Bound::Excluded(&i) => Some(i + 1),
        };
        if let Some(lower) = lower {
            if lower > len {
                M::release_error()
            }
            len -= lower;
            start = unsafe { start.add(lower) };
        }
        unsafe { Guarded::wrap_unchecked(slice_from_raw_parts_mut(start, len)) }
    }

    pub fn as_array<const LEN: usize>(self) -> Guarded<'a, M, [T; LEN]> {
        unsafe {
            let ptr: *mut [T] = self.as_ptr();
            assert_eq!(ptr.len(), LEN);
            Guarded::wrap_unchecked(ptr as *mut [T; LEN])
        }
    }
}

impl<'a, M: SeqLockMode, T: SeqLockWrappable + Pod, const N: usize> crate::Guarded<'a, M, [T; N]> {
    pub fn as_slice(self) -> Guarded<'a, M, [T]> {
        unsafe { Guarded::wrap_unchecked(self.as_ptr() as *mut [T]) }
    }
}

#[allow(clippy::missing_safety_doc)]
unsafe trait SeqLockModeExclusiveImpl: SeqLockModeImpl {
    unsafe fn store<T: Pod>(p: &mut Self::Pointer<'_, T>, x: T);
    unsafe fn store_slice<T: Pod>(p: &mut Self::Pointer<'_, [T]>, x: &[T]);
    unsafe fn move_within_slice_to<T: Pod, const MOVE_UP: bool>(
        p: &mut Self::Pointer<'_, [T]>,
        src_range: Range<usize>,
        dst: usize,
    );
}

pub struct Guarded<'a, M: SeqLockMode, T: ?Sized> {
    p: M::Pointer<'a, T>,
    _p: PhantomData<&'a T>,
}

#[derive(Debug)]
pub struct OptimisticLockError(());

impl<T: Pod + SeqLockWrappable + Eq> Eq for Guarded<'_, Shared, [T]> {}

impl<T: Pod + SeqLockWrappable + PartialEq> PartialEq for Guarded<'_, Shared, [T]> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        };
        for i in 0..self.len() {
            if self.index(i).get().load() != other.index(i).get().load() {
                return false;
            }
        }
        true
    }
}

impl<T: Pod + SeqLockWrappable + Ord> PartialOrd for Guarded<'_, Shared, [T]> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Pod + SeqLockWrappable + Ord> Ord for Guarded<'_, Shared, [T]> {
    fn cmp(&self, other: &Self) -> Ordering {
        Iterator::cmp(self.iter().map(|x| x.get().load()), other.iter().map(|x| x.get().load()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockWrappable, Shared, Wrapper};
    use bytemuck::Pod;
    use rand::distributions::{Distribution, Standard};
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use std::fmt::Debug;
    use std::mem::size_of;

    fn fill_gen<T>(dst: &mut [T], rng: &mut impl Rng)
    where
        Standard: Distribution<T>,
    {
        for d in dst {
            *d = rng.gen();
        }
    }

    fn accessor_exclusive<T: Pod + Debug + Eq + SeqLockWrappable>()
    where
        Standard: Distribution<T>,
    {
        let rng = &mut SmallRng::seed_from_u64(42);
        let mut array = [T::zeroed(); if cfg!(miri) { 5 } else { 9 }];
        for src in 0..=array.len() {
            for dst in 0..=array.len() {
                if src == dst {
                    continue;
                }
                for len in 0..=array.len() - src.max(dst) {
                    fill_gen(&mut array, rng);
                    let mut reference = array;
                    reference.copy_within(src..src + len, dst);
                    if src < dst {
                        Guarded::<Exclusive, [T]>::wrap_mut(&mut array[..])
                            .get_mut()
                            .move_within_to::<true>(src..src + len, dst);
                    } else {
                        Guarded::<Exclusive, [T]>::wrap_mut(&mut array[..])
                            .get_mut()
                            .move_within_to::<false>(src..src + len, dst);
                    }
                    assert_eq!(array, reference);
                }
            }
        }
    }

    fn accessor_load<M: SeqLockMode, T: Pod + Debug + Eq + SeqLockWrappable>()
    where
        Standard: Distribution<T>,
    {
        let rng = &mut SmallRng::seed_from_u64(42);
        for len in 0..=16 {
            let mut src = [T::zeroed(); 16];
            let mut dst = src;
            let mut dst2 = src;
            let mut dst3 = src;
            fill_gen(&mut src, rng);
            let mut src2 = src;
            {
                {
                    let src2 = Guarded::<M, [T]>::wrap_mut(&mut src2[..len]);
                    let mut dst2 = Guarded::<Exclusive, [T]>::wrap_mut(&mut dst2[..len]);
                    src2.get().load_slice(&mut dst[..len]);
                    src2.get().copy_to(&mut dst2);
                    if len > 0 {
                        assert_eq!(src[0], src2.index(0).get().load());
                    }
                }
                let mut dst3 = Guarded::<Exclusive, [T]>::wrap_mut(&mut dst3[..len]);
                dst3.store_slice(&src2[..len]);
                if len > 0 {
                    let mut dst4 = T::zeroed();
                    Guarded::<Exclusive, T>::wrap_mut(&mut dst4).get_mut().store(src2[0]);
                    assert_eq!(dst4, src2[0])
                }
            }
            for dst in &[dst, dst2, dst3] {
                assert_eq!(&src[..len], &dst[..len]);
                assert!(dst[len..].iter().all(|x| *x == T::zeroed()));
            }
            assert_eq!(src2, src);
        }
    }

    fn accessor_cmp<M: SeqLockMode, T: Pod + Debug + PartialEq + SeqLockWrappable>()
    where
        Standard: Distribution<T>,
    {
        if size_of::<T>() == 0 {
            return;
        }
        let rng = &mut SmallRng::seed_from_u64(42);
        for _i in 0..if cfg!(miri) { 10 } else { 1000 } {
            for a_len in 0..=3 {
                for b_len in 0..=3 {
                    let mut array = [T::zeroed(); 6];
                    fill_gen(&mut array, rng);
                    let (a, b) = array.split_at_mut(3);
                    let a = &mut a[..a_len];
                    let b = &mut b[..b_len];
                    let c1 = unsafe { Guarded::<M, [T]>::wrap_unchecked(a).get().mem_cmp(b) };
                    assert_eq!(c1, Ord::cmp(bytemuck::cast_slice::<T, u8>(a), bytemuck::cast_slice::<T, u8>(b)));
                }
            }
        }
    }

    #[test]
    fn test_accessors() {
        macro_rules! type_iter {
            ($N:ident;$($M:ty),*;$tt:tt) => {
                $(
                    {
                        type $N=$M;
                        $tt
                    }
                )*
            };
        }
        type_iter!(T;(),u8,i8,u16,i32,u64,usize,isize,[u16;2],[i16;20],[u8;16],[();3];{
            accessor_exclusive::<T>();
            type_iter!(M;Exclusive,Shared,Optimistic;{
                accessor_cmp::<M,T>();
                accessor_load::<M,T>();
                }
            );}
        );
    }
}

// #[allow(dead_code)]
// #[derive(SeqlockAccessors)]
// #[seq_lock_wrapper(MyWrapper)]
// struct MyStructGeneric<T: Deref + SeqLockWrappable, U>
// where
//     T::Target: Deref,
// {
//     #[allow(dead_code)]
//     x: T,
//     #[seq_lock_skip_accessor]
//     #[allow(dead_code)]
//     u: U,
// }

//
// seqlock_wrapper!(MyWrapper);
//
// #[allow(dead_code)]
// struct MyStructGeneric<T: Deref + SeqLockWrappable, U> where
//     T::Target: Deref {
//     #[allow(dead_code)]
//     x: T,
//     #[allow(dead_code)]
//     u: U,
// }
// impl<T: Deref + SeqLockWrappable, U> crate::SeqLockWrappable for
// MyStructGeneric<T, U> where T::Target: Deref {
//     type Wrapper<WrappedParam> = MyWrapper<WrappedParam>;
// }
// impl<'wrapped_guard, SeqLockModeParam: crate::SeqLockMode, T: Deref +
// SeqLockWrappable, U>
// MyWrapper<crate::Guarded<'wrapped_guard, SeqLockModeParam,
//     MyStructGeneric<T, U>>> where T::Target: Deref {
//     fn x_mut<'b>(&'b mut self)
//                  ->
//                  <T as
//                  crate::SeqLockWrappable>::Wrapper<crate::Guarded<'b,
//                      SeqLockModeParam, T>> {
//         unsafe {
//             crate::Guarded::wrap_unchecked(
//                 core::ptr::addr_of_mut!((*self.0.as_ptr()).x)
//             )
//         }
//     }
//     fn x<'b>(&'b self)
//              ->
//              <T as
//              crate::SeqLockWrappable>::Wrapper<crate::Guarded<'b,
//                  SeqLockModeParam::SharedDowngrade, T>> {
//         unsafe {
//             crate::Guarded::wrap_unchecked(
//                 core::ptr::addr_of_mut!((*self.0.as_ptr()).x)
//                 )
//         }
//     }
// }
