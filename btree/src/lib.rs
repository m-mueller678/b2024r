use seqlock::{SeqLockGuarded, SeqLockMode};
use std::mem::size_of;

const PAGE_SIZE: usize = 1 << 12;
const PAGE_HEAD_SIZE: usize = 8;

pub struct CommonNodeHead {
    prefix_len: u16,
    count: u16,
    lower_fence_len: u16,
    upper_fence_len: u16,
}

struct PageId([u16; 3]);

pub struct BasicNode<V: BasicNodeVariant> {
    common: CommonNodeHead,
    heap_bump: u16,
    heap_freed: u16,
    hints: [u32; 16],
    upper: V::Upper,
    _pad: u16,
    _data: [u32; PAGE_SIZE - PAGE_HEAD_SIZE - size_of::<CommonNodeHead>() - 6 * 2 - 16 * 4],
}

trait BasicNodeVariant {
    type Upper;
}

impl<V: BasicNodeVariant> BasicNode<V> {
    fn find<M: SeqLockMode>(this: SeqLockGuarded<M, Self>, key: &[u8]) -> Result<usize, usize> {
        todo!()
    }
}
