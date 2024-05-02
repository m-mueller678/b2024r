use std::{eprint, eprintln};
use std::fmt::Formatter;
use crate::basic_node::BasicNodeData;
use crate::page::{PageId, PageTail};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Guard, Guarded, SeqLockMode, SeqLockWrappable, SeqlockAccessors, Shared, Wrapper};
use std::mem::size_of;
use bstr::BString;

pub mod node_tag {
    pub const BASIC_INNER: u8 = 250;
    pub const BASIC_LEAF: u8 = 251;
}

pub const PAGE_SIZE: usize = 1 << 10;
pub const PAGE_HEAD_SIZE: usize = 8;

#[derive(Pod, Copy, Clone, Zeroable, SeqlockAccessors)]
#[repr(C)]
#[seq_lock_wrapper(W)]
pub struct CommonNodeHead {
    pub tag: u8,
    _pad: u8,
    pub prefix_len: u16,
    pub count: u16,
    pub lower_fence_len: u16,
    pub upper_fence_len: u16,
}

pub unsafe trait Node: SeqLockWrappable + Pod {
    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split(
        this: &mut W<Guarded<Exclusive, Self>>,
        parent_insert: impl FnOnce(usize, Guarded<'_, Shared, [u8]>) -> Result<Guard<'static, Exclusive, PageTail>, ()>,
    ) -> Result<(), ()>;
    fn format(this:&W<Guarded<Exclusive,Self>>,f:&mut Formatter)->std::fmt::Result
        where
            Self: Copy,
    ;
}

impl<'a, N: Node, M: SeqLockMode> W<Guarded<'a, M, N>> {
    pub fn common_head(self) -> W<Guarded<'a, M, CommonNodeHead>> {
        unsafe { self.0.map_ptr(|x| x as *mut CommonNodeHead) }
    }

    pub fn slice<T: SeqLockWrappable + Pod>(self, offset: usize, count: usize) -> Guarded<'a, M, [T]> {
        self.as_bytes().slice(offset..offset + count * size_of::<T>()).cast_slice::<T>()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn as_bytes(self) -> Guarded<'a, M, [u8]> {
        const SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;
        self.0.cast::<[u8; SIZE]>().as_slice()
    }

    pub fn lower_fence(self) -> Guarded<'a, M, [u8]> {
        let lf = W::rewrap(self.s()).common_head().lower_fence_len().load() as usize;
        self.slice(size_of::<BasicNodeData>() - lf, lf)
    }

    pub fn prefix(self) -> Guarded<'a, M, [u8]> {
        let lf = W::rewrap(self.s()).common_head().lower_fence_len().load() as usize;
        let pf = W::rewrap(self.s()).common_head().prefix_len().load() as usize;
        self.slice(size_of::<BasicNodeData>() - lf, pf)
    }

    pub fn upper_fence(self) -> Guarded<'a, M, [u8]> {
        let uf = W::rewrap(self.s()).common_head().upper_fence_len().load() as usize;
        let lf = W::rewrap(self.s()).common_head().lower_fence_len().load() as usize;
        self.slice(size_of::<BasicNodeData>() - lf - uf, uf)
    }
}
