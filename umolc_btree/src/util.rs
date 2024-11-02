use bytemuck::{Pod, Zeroable};
use std::fmt::{Debug, Formatter};

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct PodPad<const L: usize>([u8; L]);

impl<const L: usize> Debug for PodPad<L> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PodPad")
    }
}

unsafe impl<const L: usize> Zeroable for PodPad<L> {}
unsafe impl<const L: usize> Pod for PodPad<L> {}
