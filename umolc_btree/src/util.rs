use bytemuck::{Pod, Zeroable};

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct PodPad<const L: usize>([u8; L]);

unsafe impl<const L: usize> Zeroable for Pad<L> {}
unsafe impl<const L: usize> Pod for Pad<L> {}
