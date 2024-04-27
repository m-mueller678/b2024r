#![feature(never_type)]
#![feature(hint_assert_unchecked)]
#![feature(pointer_is_aligned_to)]
#![feature(layout_for_ptr)]

extern crate core;

use bytemuck::Pod;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{align_of, align_of_val_raw, size_of, transmute, MaybeUninit};
use std::ops::{Bound, Range, RangeBounds};
use std::ptr::slice_from_raw_parts_mut;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread::yield_now;

pub mod unwind;
mod wrappable;

use crate::lock::LockState;
pub use access_impl::optimistic_release;
pub use lock::{Guard, SeqLock};
pub use seqlock_macros::SeqlockAccessors;
pub use wrappable::{SeqLockWrappable, Wrapper};

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

#[path = "atomic_byte_impl.rs"]
mod access_impl;
mod lock;

#[allow(private_bounds)]
pub trait SeqLockMode: SeqLockModeImpl + 'static {
    type SharedDowngrade: SeqLockMode;
    type GuardData: Copy;
    type ReleaseData;
    const PESSIMISTIC: bool;

    fn acquire(s: &LockState) -> Self::GuardData;
    fn release(s: &LockState, d: Self::GuardData) -> Self::ReleaseData;

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

    type SharedDowngrade = Optimistic;

    fn release_error() -> ! {
        unwind::start()
    }

    fn acquire(lock: &LockState) -> Self::GuardData {
        loop {
            let x = lock.version.load(Acquire);
            if x % 2 == 0 {
                return x;
            } else {
                yield_now();
            }
        }
    }

    fn release(lock: &LockState, guard: Self::GuardData) -> Self::ReleaseData {
        optimistic_release(&lock.version, guard)
    }
}

impl SeqLockModeExclusive for Exclusive {}
impl SeqLockModePessimistic for Exclusive {}
impl SeqLockModePessimistic for Shared {}

pub trait SeqLockModePessimistic: SeqLockMode {}

impl SeqLockMode for Exclusive {
    const PESSIMISTIC: bool = true;
    type SharedDowngrade = Shared;
    type GuardData = ();
    type ReleaseData = u64;

    fn release_error() -> ! {
        unreachable!()
    }

    fn acquire(lock: &LockState) -> Self::GuardData {
        loop {
            let x = lock.version.load(Relaxed);
            if x % 2 == 0 {
                if lock.version.compare_exchange(x, x + 1, Acquire, Relaxed).is_ok() {
                    return;
                }
            } else {
                yield_now();
            }
        }
    }

    fn release(lock: &LockState, (): ()) -> Self::ReleaseData {
        let prev = lock.version.fetch_add(1, Release);
        prev + 1
    }
}

impl SeqLockMode for Shared {
    const PESSIMISTIC: bool = false;
    type SharedDowngrade = Shared;
    type GuardData = ();
    type ReleaseData = u64;

    fn acquire(_s: &LockState) -> Self::GuardData {
        unimplemented!()
    }

    fn release(_s: &LockState, _d: Self::GuardData) -> Self::ReleaseData {
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
    type Pointer<'a, T: ?Sized>: Copy;
    unsafe fn from_pointer<'a, T: ?Sized>(x: *mut T) -> Self::Pointer<'a, T>;
    fn as_ptr<T: ?Sized>(x: &Self::Pointer<'_, T>) -> *mut T;
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
        assert_eq!(align_of::<T>() & align_of::<U>(), 0);
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
        unsafe { M::load_slice(&self.p, dst) }
    }

    pub fn load_slice<'dst>(&self, dst: &'dst mut [T]) {
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
    unsafe fn store<T>(p: &mut Self::Pointer<'_, T>, x: T);
    unsafe fn store_slice<T>(p: &mut Self::Pointer<'_, [T]>, x: &[T]);
    unsafe fn move_within_slice_to<T, const MOVE_UP: bool>(
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
