use crate::basic_node::{BasicNode, BasicNodeInner, BasicNodeLeaf};
use crate::node::{CommonNodeHead, PAGE_HEAD_SIZE, PAGE_SIZE};
use bytemuck::{Pod, Zeroable};
use seqlock::SeqLock;
use std::ptr;

struct MetadataPage {
    root: u64,
    _pad: [u64; PAGE_TAIL_SIZE / 8 - 1],
}

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;

#[derive(Zeroable, Pod, Clone, Copy)]
#[repr(C)]
struct PageTail {
    _pad: [u64; PAGE_TAIL_SIZE / 8],
}

pub struct Page {
    lock: SeqLock<PageTail>,
}
