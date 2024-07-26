use crate::node::{CommonNodeHead, NODE_TAIL_SIZE, PAGE_SIZE};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{DefaultBm, Guarded, SeqLockMode, SeqlockAccessors};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Mutex, OnceLock};

pub type PageId = u64;
pub fn page_id_to_3x16(p: u64) -> [u16; 3] {
    #[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
    compile_error!("only little endian 64-bit is supported");
    debug_assert!(p < (1 << 48));
    let a = bytemuck::cast::<[u8; 8], [u16; 4]>(p.to_ne_bytes());
    [a[0], a[1], a[2]]
}

pub fn page_id_from_3x16(x: [u16; 3]) -> u64 {
    let a = bytemuck::cast::<[u16; 4], [u8; 8]>([x[0], x[1], x[2], 0]);
    u64::from_ne_bytes(a)
}

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE;

#[derive(Zeroable, Pod, Clone, Copy, SeqlockAccessors)]
#[seq_lock_wrapper(crate::W)]
#[repr(C, align(8))]
#[seq_lock_accessor(pub tag: u8 = common.tag)]
pub struct PageTail {
    pub common: CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; NODE_TAIL_SIZE],
}

#[repr(align(1024), C)]
pub struct Page {
    page: UnsafeCell<PageTail>,
}

const _: () = {
    assert!(PAGE_SIZE == align_of::<Page>());
    assert!(PAGE_SIZE == size_of::<Page>());
};
