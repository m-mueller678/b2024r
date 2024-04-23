use seqlock::{Exclusive, Guarded, SeqLockMode};
use std::collections::Bound;
use std::ops::RangeBounds;

pub fn common_prefix(a: impl KeySource, b: impl KeySource) -> usize {
    a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
}

pub trait KeySource: Copy {
    fn write_suffix_to_offset(self, dst: Guarded<Exclusive, [u8]>, offset: usize) {
        self.slice(offset..).write_to(&mut dst.slice(offset..));
    }
    fn write_to(self, dst: &mut Guarded<Exclusive, [u8]>);
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

    fn iter(self) -> impl Iterator<Item = u8>;
}

impl<M: SeqLockMode> KeySource for Guarded<'_, M, [u8]>
where
    Self: Copy,
{
    fn write_to(self, dst: &mut Guarded<Exclusive, [u8]>) {
        self.copy_to(dst);
    }

    fn slice(self, b: impl RangeBounds<usize>) -> Self {
        self.try_slice(b).unwrap()
    }

    fn len(self) -> usize {
        Self::len(&self)
    }

    fn iter(self) -> impl Iterator<Item = u8> {
        Self::iter(self).map(|x| x.load())
    }
}

impl KeySource for &'_ [u8] {
    fn write_to(self, dst: &mut Guarded<Exclusive, [u8]>) {
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

    fn iter(self) -> impl Iterator<Item = u8> {
        self.iter().copied()
    }
}

impl<A: KeySource, B: KeySource> KeySource for (A, B) {
    fn write_to(self, dst: &mut Guarded<Exclusive, [u8]>) {
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

    fn iter(self) -> impl Iterator<Item = u8> {
        self.0.iter().chain(self.1.iter())
    }
}
