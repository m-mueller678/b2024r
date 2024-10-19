use bytemuck::{Pod, Zeroable};
use static_assertions::{assert_impl_all, const_assert_eq};
use std::fmt::Debug;
use std::mem::transmute;
use umolc::BufferManager;

pub mod node_tag {
    pub const METADATA_MARKER: u8 = 43;
    pub const BASIC_INNER: u8 = 250;
    pub const BASIC_LEAF: u8 = 251;
}

#[cfg(feature = "page_1k")]
pub const PAGE_SIZE: usize = 1024;

#[cfg(feature = "page_4k")]
pub const PAGE_SIZE: usize = 4096;

pub const NODE_TAIL_SIZE: usize = PAGE_SIZE - size_of::<CommonNodeHead>();

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
        assert_eq_size!($t, crate::node::Page);
        assert_eq_align!($t, crate::node::Page);
        assert_impl_all!($t, bytemuck::Pod);
        unsafe impl crate::node::ToFromPage for $t {};
    };
}

pub fn page_cast<A: ToFromPage, B: ToFromPage>(a: &A) -> &B {
    unsafe { transmute::<&A, &B>(a) }
}
pub fn page_cast_mut<A: ToFromPage, B: ToFromPage>(a: &mut A) -> &mut B {
    unsafe { transmute::<&mut A, &mut B>(a) }
}

pub unsafe trait ToFromPage {}

pub trait NodeStatic<'bm, BM: BufferManager<'bm>>: NodeDynamic<'bm, BM> {
    const TAG: u8;
}

pub trait NodeDynamic<'bm, BM: BufferManager<'bm>>: ToFromPage {
    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split<'g>(&mut self, bm: BM, parent: &mut dyn NodeDynamic<BM>) -> Result<(), ()>;
    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>);
    fn merge(&mut self, right: &mut Page);
    fn validate(&self);
}

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

#[derive(Clone, Copy)]
#[cfg_attr(feature = "page_1k", repr(C, align(1024)))]
#[cfg_attr(feature = "page_4k", repr(C, align(4096)))]
pub struct Page {
    pub common: CommonNodeHead,
    _pad: [u8; NODE_TAIL_SIZE],
}

assert_impl_all!(CommonNodeHead:Pod,Zeroable);
const_assert_eq!(size_of::<Page>(), align_of::<Page>());
const_assert_eq!(size_of::<Page>(), PAGE_SIZE);
unsafe impl Zeroable for Page {}
unsafe impl Pod for Page {}
unsafe impl ToFromPage for Page {}

impl Page {
    fn cast_slice<T: Pod>(&self) -> &[T] {
        bytemuck::cast_slice(std::slice::from_ref(self))
    }

    pub fn lower_fence(&self) -> &[u8] {
        &self.cast_slice::<u8>()[size_of::<Self>() - self.common.lower_fence_len as usize..]
    }

    pub fn prefix(&self) -> &[u8] {
        &self.cast_slice::<u8>()[size_of::<Self>() - self.common.lower_fence_len as usize..]
            [..self.common.prefix_len as usize]
    }

    pub fn upper_fence_tail(&self) -> &[u8] {
        &self.cast_slice::<u8>()
            [size_of::<Self>() - self.common.lower_fence_len as usize - self.common.upper_fence_len as usize..]
            [..self.common.upper_fence_len as usize]
    }
}
