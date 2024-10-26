use crate::key_source::{common_prefix, SourceSlice, SourceSlicePair};
use crate::MAX_KEY_SIZE;
use bytemuck::{Pod, Zeroable};
use static_assertions::{assert_impl_all, const_assert_eq};
use std::fmt::Debug;
use std::mem::transmute;
use umolc::{BufferManager, OPtr, OlcErrorHandler, PageId};

pub mod node_tag {
    pub const METADATA_MARKER: u8 = 43;
    pub const BASIC_INNER: u8 = 250;
    pub const BASIC_LEAF: u8 = 251;
}

#[cfg(feature = "page_1k")]
pub const PAGE_SIZE: usize = 1024;

#[cfg(feature = "page_4k")]
pub const PAGE_SIZE: usize = 4096;

const NODE_TAIL_SIZE: usize = PAGE_SIZE - size_of::<CommonNodeHead>();

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct CommonNodeHead {
    pub tag: u8,
    _pad: u8,
    pub prefix_len: u16,
    pub count: u16,
    pub lower_fence_len: u16,
    pub upper_fence_len: u16,
}

#[no_mangle]
pub unsafe fn print_page(p: *const Page) {
    let p: Page = p.read();
    if p.common.tag == node_tag::METADATA_MARKER {
        println!("MetadataPage");
        return;
    }
    todo!();
}

#[derive(Eq, PartialEq, Debug)]
pub struct DebugNode<V> {
    pub prefix_len: usize,
    pub lf: Vec<u8>,
    pub uf: Vec<u8>,
    pub keys: Vec<Vec<u8>>,
    pub values: Vec<V>,
}

#[macro_export]
macro_rules! impl_to_from_page {
    ($t:ty) => {
        static_assertions::assert_eq_size!($t, $crate::node::Page);
        static_assertions::assert_eq_align!($t, crate::node::Page);
        static_assertions::assert_impl_all!($t: bytemuck::Pod);
        unsafe impl crate::node::ToFromPage for $t {}
    };
}

pub fn page_cast<A: ToFromPage, B: ToFromPage>(a: &A) -> &B {
    unsafe { transmute::<&A, &B>(a) }
}
pub fn page_cast_mut<A: ToFromPage, B: ToFromPage>(a: &mut A) -> &mut B {
    unsafe { transmute::<&mut A, &mut B>(a) }
}

pub unsafe trait ToFromPage {}

pub trait ToFromPageExt: ToFromPage + Pod {
    fn as_page(&self) -> &Page {
        page_cast::<Self, Page>(self)
    }

    fn as_page_mut(&mut self) -> &mut Page {
        page_cast_mut::<Self, Page>(self)
    }

    fn cast_slice_mut<T: Pod>(&mut self) -> &mut [T] {
        bytemuck::cast_slice_mut(std::slice::from_mut(self))
    }
    fn cast_slice<T: Pod>(&self) -> &[T] {
        bytemuck::cast_slice(std::slice::from_ref(self))
    }

    fn slice(&self, offset: usize, len: usize) -> &[u8] {
        &self.cast_slice::<u8>()[offset..][..len]
    }

    fn slice_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        &mut self.cast_slice_mut::<u8>()[offset..][..len]
    }

    fn lower_fence(&self) -> &[u8] {
        &self.cast_slice::<u8>()[size_of::<Self>() - self.as_page().common.lower_fence_len as usize..]
    }

    fn prefix(&self) -> &[u8] {
        &self.cast_slice::<u8>()[size_of::<Self>() - self.as_page().common.lower_fence_len as usize..]
            [..self.as_page().common.prefix_len as usize]
    }

    fn upper_fence_tail(&self) -> &[u8] {
        &self.cast_slice::<u8>()[size_of::<Self>()
            - self.as_page().common.lower_fence_len as usize
            - self.as_page().common.upper_fence_len as usize..][..self.as_page().common.upper_fence_len as usize]
    }

    fn upper_fence_combined(&self) -> SourceSlicePair<u8, &[u8], &[u8]> {
        self.prefix().join(self.upper_fence_tail())
    }

    fn fences_start(&self) -> usize {
        let head = self.as_page().common;
        size_of::<Self>() - head.lower_fence_len as usize - head.upper_fence_len as usize
    }
}

impl<T: ToFromPage + Pod> ToFromPageExt for T {}

pub trait NodeStatic<'bm, BM: BufferManager<'bm, Page = Page>>: NodeDynamic<'bm, BM> {
    const TAG: u8;
}

pub trait NodeDynamic<'bm, BM: BufferManager<'bm, Page = Page>>: ToFromPage {
    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split<'g>(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()>;
    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>);
    fn merge(&mut self, right: &mut Page);
    fn validate(&self);
    fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()>;
    fn validate_inter_node_fences<'b>(
        &self,
        bm: BM,
        lb: &mut &'b mut [u8; MAX_KEY_SIZE],
        hb: &mut &'b mut [u8; MAX_KEY_SIZE],
        ll: usize,
        hl: usize,
    );
}

pub const PAGE_ID_LEN: usize = 5;

pub fn page_id_to_bytes(p: PageId) -> [u8; PAGE_ID_LEN] {
    let b = p.0.to_le_bytes();
    debug_assert!(p.0 < (1 << (8 * PAGE_ID_LEN)));
    b[..PAGE_ID_LEN].try_into().unwrap()
}

pub fn page_id_from_bytes(x: &[u8; PAGE_ID_LEN]) -> PageId {
    let mut b = [0; 8];
    b[..PAGE_ID_LEN].copy_from_slice(&x[..]);
    PageId(u64::from_ne_bytes(b))
}

pub fn page_id_from_olc_bytes<O: OlcErrorHandler>(x: OPtr<[u8; PAGE_ID_LEN], O>) -> PageId {
    let mut b = [0; 8];
    unsafe {
        std::ptr::copy(x.to_raw() as *const u8, b.as_mut_ptr(), PAGE_ID_LEN);
    }
    PageId(u64::from_ne_bytes(b))
}

#[derive(Clone, Copy)]
#[repr(C, align(16))]
pub struct Page {
    pub common: CommonNodeHead,
    _pad: [u8; NODE_TAIL_SIZE],
}

assert_impl_all!(CommonNodeHead:Pod,Zeroable);
const_assert_eq!(size_of::<Page>(), PAGE_SIZE);
unsafe impl Zeroable for Page {}
unsafe impl Pod for Page {}
unsafe impl ToFromPage for Page {}

pub trait NodeKind: Pod {
    const IS_LEAF: bool;
}

impl NodeKind for KindInner {
    const IS_LEAF: bool = false;
}

impl NodeKind for KindLeaf {
    const IS_LEAF: bool = true;
}

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct KindInner;
#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct KindLeaf;

pub fn insert_upper_sibling<'bm, BM: BufferManager<'bm, Page = Page>>(
    parent: &mut dyn NodeDynamic<'bm, BM>,
    bm: BM,
    separator: impl SourceSlice,
) -> Result<BM::GuardX, ()> {
    let (new_guard, new_page) = bm.alloc();
    separator.to_stack_buffer::<MAX_KEY_SIZE, _>(|sep| {
        if let Ok(()) = parent.insert_inner(sep, new_page) {
            Ok(new_guard)
        } else {
            bm.free(new_guard);
            Err(())
        }
    })
}

impl Page {
    pub fn common_init(&mut self, tag: u8, lf: impl SourceSlice, uf: impl SourceSlice) {
        self.common.tag = tag;
        self.common.count = 0;
        let pl = common_prefix(lf, uf);
        let ll = lf.len();
        let ul = uf.len() - pl;
        self.common.prefix_len = pl as u16;
        self.common.lower_fence_len = ll as u16;
        self.common.upper_fence_len = ul as u16;
        lf.write_to(&mut self.cast_slice_mut()[size_of::<Self>() - ll..]);
        uf.slice_start(self.common.prefix_len as usize).write_to(self.slice_mut(size_of::<Self>() - ll - ul, ul));
    }

    pub fn as_dyn_node<'bm, BM: BufferManager<'bm>>(&self) -> &dyn NodeDynamic<'bm, BM> {
        todo!()
    }
}
