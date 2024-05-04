use crate::basic_node::{BasicInner, BasicLeaf, BasicNodeData};
use crate::page::{Page, PageTail};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{
    Exclusive, Guard, Guarded, Optimistic, SeqLockMode, SeqLockWrappable, SeqlockAccessors, Shared, Wrapper,
};
use std::fmt::{Debug, Formatter};
use std::mem::size_of;

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

#[no_mangle]
pub unsafe fn print_node(p: *const Page) {
    let tail = p.byte_offset(PAGE_HEAD_SIZE as isize).cast::<PageTail>();
    println!("{:#?}", Guarded::<Shared, PageTail>::wrap_unchecked(tail as *mut PageTail));
}

impl<M: SeqLockMode> Debug for W<Guarded<'_, M, PageTail>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.optimistic().node_cast::<BasicLeaf>().tag().load() {
            node_tag::BASIC_LEAF => Node::format(&self.optimistic().node_cast::<BasicLeaf>(), f),
            node_tag::BASIC_INNER => Node::format(&self.optimistic().node_cast::<BasicInner>(), f),
            x => write!(f, "UnknownNode{{tag:0x{x:x}}}"),
        }
    }
}

pub unsafe trait Node: SeqLockWrappable + Pod {
    const TAG: u8;

    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split(
        this: &mut W<Guarded<Exclusive, Self>>,
        parent_insert: impl FnOnce(usize, Guarded<'_, Shared, [u8]>) -> Result<Guard<'static, Exclusive, PageTail>, ()>,
        ref_key: &[u8],
    ) -> Result<(), ()>;

    fn merge(this: &mut W<Guarded<Exclusive, Self>>, right: &mut W<Guarded<Exclusive, Self>>, ref_key: &[u8]);
    fn format(this: &W<Guarded<Optimistic, Self>>, f: &mut Formatter) -> std::fmt::Result
    where
        Self: Copy;
    fn validate(this: W<Guarded<'_, Shared, Self>>);
}

impl<'a, M: SeqLockMode> W<Guarded<'a, M, PageTail>> {
    pub fn node_cast<N: Node>(self) -> N::Wrapper<Guarded<'a, M, N>> {
        if M::EXCLUSIVE {
            debug_assert_eq!(self.s().0.cast::<BasicLeaf>().tag().load(), N::TAG);
        }
        self.0.cast::<N>()
    }
}

impl<'a, M: SeqLockMode, N: Node> W<Guarded<'a, M, N>> {
    pub fn upcast(self) -> W<Guarded<'a, M, PageTail>> {
        self.0.cast::<PageTail>()
    }
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
