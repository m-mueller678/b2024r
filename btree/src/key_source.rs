use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Guarded, SeqLockMode, SeqLockWrappable};
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;

pub fn common_prefix(a: impl SourceSlice, b: impl SourceSlice) -> usize {
    a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
}

pub fn key_head(k: impl SourceSlice) -> u32 {
    let mut buffer = [0u8; 4];
    let common_len = k.len().min(4);
    k.slice_end(common_len).write_to(&mut Guarded::wrap_mut(&mut buffer[..common_len]));
    u32::from_be_bytes(buffer)
}

pub trait SourceSlice<T: Pod + SeqLockWrappable = u8>: Copy {
    fn join<B: SourceSlice<T>>(self, b: B) -> impl SourceSlice<T> {
        SourceSlicePair(self, b, PhantomData)
    }
    fn to_stack_buffer<const SIZE: usize, R>(self, f: impl FnOnce(&mut [T]) -> R) -> R {
        let mut buffer = <[T; SIZE]>::zeroed();
        self.write_to(&mut Guarded::wrap_mut(&mut buffer[..self.len()]));
        f(&mut buffer[..self.len()])
    }
    fn write_suffix_to_offset(self, dst: Guarded<Exclusive, [T]>, offset: usize) {
        self.slice(offset..).write_to(&mut dst.slice(offset..));
    }
    fn write_to(self, dst: &mut Guarded<Exclusive, [T]>);
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
}

impl<M: SeqLockMode, T: Pod + SeqLockWrappable> SourceSlice<T> for Guarded<'_, M, [T]>
where
    Self: Copy,
{
    fn write_to(self, dst: &mut Guarded<Exclusive, [T]>) {
        self.copy_to(dst);
    }

    fn slice(self, b: impl RangeBounds<usize>) -> Self {
        Guarded::slice(self, b)
    }

    fn len(self) -> usize {
        Self::len(&self)
    }

    fn iter(self) -> impl Iterator<Item = T> {
        Guarded::iter(self).map(|x| seqlock::Wrapper::get(&x).load())
    }
}

impl<T: Pod + SeqLockWrappable> SourceSlice<T> for &'_ [T] {
    fn write_to(self, dst: &mut Guarded<Exclusive, [T]>) {
        dst.store_slice(self)
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

#[derive(Copy, Clone)]
pub struct SourceSlicePair<T: Pod + SeqLockWrappable, A: SourceSlice<T>, B: SourceSlice<T>>(A, B, PhantomData<[T]>);

impl<T: Pod + SeqLockWrappable, A: SourceSlice<T>, B: SourceSlice<T>> SourceSlice<T> for SourceSlicePair<T, A, B> {
    fn write_to(self, dst: &mut Guarded<Exclusive, [T]>) {
        let a_len = self.0.len();
        self.0.write_to(&mut dst.b().slice(..a_len));
        self.1.write_to(&mut dst.b().slice(a_len..));
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
