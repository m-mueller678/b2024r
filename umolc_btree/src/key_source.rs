use bytemuck::Pod;
use std::cmp::Ordering;
use std::collections::Bound;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::RangeBounds;

pub fn common_prefix(a: impl SourceSlice, b: impl SourceSlice) -> usize {
    a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
}

pub fn key_head(k: impl SourceSlice) -> u32 {
    let mut buffer = [0u8; 4];
    let k_len = k.len();
    if k_len >= 4 {
        // having a special case for len>=4 allows for better code gen, ~5% speedup
        k.slice_end(4).write_to(&mut buffer[..]);
    } else {
        k.write_to(&mut buffer[..k_len]);
    }
    u32::from_be_bytes(buffer)
}

/// # Safety
/// slice returned from write_to_uninit must fill entire slice and return it or diverge
pub unsafe trait SourceSlice<T: Pod = u8>: Default + Copy {
    fn index_ss(self, i: usize) -> T {
        self.slice_start(i).iter().next().unwrap()
    }
    fn join<B: SourceSlice<T>>(self, b: B) -> SourceSlicePair<T, Self, B> {
        SourceSlicePair(self, b, PhantomData)
    }
    fn to_mut_buffer<const SIZE: usize, R>(self, f: impl FnOnce(&mut [T]) -> R) -> R {
        let mut buffer: [MaybeUninit<T>; SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        f(self.write_to_uninit(&mut buffer[..self.len()]))
    }

    fn to_ref_buffer<const SIZE: usize, R>(self, f: impl FnOnce(&[T]) -> R) -> R {
        let mut buffer: [MaybeUninit<T>; SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        f(self.write_to_uninit(&mut buffer[..self.len()]))
    }

    fn to_vec(self) -> Vec<T> {
        self.iter().collect()
    }
    fn write_suffix_to_offset(self, dst: &mut [T], offset: usize) {
        self.slice(offset..).write_to(&mut dst[offset..]);
    }
    fn write_to(self, dst: &mut [T]) {
        assert_eq!(self.len(), dst.len());
        for (i, b) in self.iter().enumerate() {
            dst[i] = b;
        }
    }

    fn write_to_uninit(self, dst: &mut [MaybeUninit<T>]) -> &mut [T] {
        assert_eq!(self.len(), dst.len());
        for (i, b) in self.iter().enumerate() {
            dst[i].write(b);
        }
        unsafe { MaybeUninit::slice_assume_init_mut(dst) }
    }

    fn slice(mut self, b: impl RangeBounds<usize>) -> Self {
        let start = match b.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(&x) => Some(x),
            Bound::Excluded(&x) => Some(x + 1),
        };
        if let Some(start) = start {
            self = self.slice_start(start);
        }
        let end = match b.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(&x) => Some(x + 1),
            Bound::Excluded(&x) => Some(x),
        };
        if let Some(end) = end {
            self = self.slice_end(end);
        }
        self
    }
    fn slice_start(self, start: usize) -> Self {
        self.slice(start..)
    }
    fn slice_end(self, end: usize) -> Self {
        self.slice(..end)
    }
    fn len(self) -> usize;

    fn iter(self) -> impl Iterator<Item = T>;

    fn cmp<R: SourceSlice<T>>(self, rhs: R) -> Ordering
    where
        T: Ord,
    {
        Iterator::cmp(self.iter(), rhs.iter())
    }
}

unsafe impl<T: Pod> SourceSlice<T> for &'_ [T] {
    fn write_to(self, dst: &mut [T]) {
        dst.copy_from_slice(self)
    }

    fn slice_start(self, start: usize) -> Self {
        &self[start..]
    }

    fn slice_end(self, end: usize) -> Self {
        &self[..end]
    }

    fn len(self) -> usize {
        self.len()
    }

    fn iter(self) -> impl Iterator<Item = T> {
        self.iter().copied()
    }
}

#[derive(Copy, Clone, Default)]
pub struct SourceSlicePair<T: Pod, A: SourceSlice<T>, B: SourceSlice<T>>(A, B, PhantomData<[T]>);

unsafe impl<T: Pod + Default, A: SourceSlice<T>, B: SourceSlice<T>> SourceSlice<T> for SourceSlicePair<T, A, B> {
    fn write_to(self, dst: &mut [T]) {
        let a_len = self.0.len();
        self.0.write_to(&mut dst[..a_len]);
        self.1.write_to(&mut dst[a_len..]);
    }

    fn slice_start(mut self, start: usize) -> Self {
        let a_len = self.0.len();
        if start <= a_len {
            self.0 = self.0.slice_start(start);
        } else {
            self.0 = self.0.slice_start(a_len);
            self.1 = self.1.slice_start(start - a_len);
        }
        self
    }

    fn slice_end(mut self, len: usize) -> Self {
        let a_len = self.0.len();
        if len >= a_len {
            self.1 = self.1.slice_end(len - a_len);
        } else {
            self.0 = self.0.slice_end(len);
            self.1 = self.1.slice_end(0);
        }
        self
    }

    fn len(self) -> usize {
        self.0.len() + self.1.len()
    }

    fn iter(self) -> impl Iterator<Item = T> {
        self.0.iter().chain(self.1.iter())
    }
}

#[derive(Copy, Clone, Default)]
pub struct HeadSourceSlice {
    array: u32,
    start: usize,
    end: usize,
}

impl HeadSourceSlice {
    pub fn from_head_len(head: u32, len: usize) -> Self {
        HeadSourceSlice { array: head, start: 0, end: len.min(4) }
    }
}

unsafe impl SourceSlice<u8> for HeadSourceSlice {
    fn write_to(self, dst: &mut [u8]) {
        dst.copy_from_slice(&self.array.to_be_bytes()[self.start..self.end])
    }

    fn slice_start(mut self, start: usize) -> Self {
        self.start += start;
        self
    }

    fn slice_end(mut self, end: usize) -> Self {
        self.end = self.start + end;
        self
    }

    fn len(self) -> usize {
        self.end - self.start
    }

    fn iter(self) -> impl Iterator<Item = u8> {
        self
    }
}

impl Iterator for HeadSourceSlice {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            None
        } else {
            let v = self.array >> (8 * (3 - self.start) as u32);
            self.start += 1;
            Some(v as u8)
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ZeroKey {
    len: usize,
}

impl ZeroKey {
    pub fn new(len: usize) -> Self {
        ZeroKey { len }
    }
}

unsafe impl SourceSlice for ZeroKey {
    fn len(self) -> usize {
        self.len
    }

    fn iter(self) -> impl Iterator<Item = u8> {
        std::iter::repeat(0).take(self.len)
    }

    fn slice_start(mut self, start: usize) -> Self {
        assert!(start <= self.len);
        self.len -= start;
        self
    }

    fn slice_end(mut self, end: usize) -> Self {
        assert!(end <= self.len);
        self.len = end;
        self
    }
}
