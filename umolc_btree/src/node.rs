use crate::basic_node::{BasicInner, BasicLeaf};
use crate::hash_leaf::HashLeaf;
use crate::heap_node::{ConstHeapLength, HeapLength};
use crate::key_source::{common_prefix, SourceSlice, SourceSlicePair};
use crate::tree::MetadataPage;
use crate::MAX_KEY_SIZE;
use bstr::BStr;
use bytemuck::{Pod, Zeroable};
use static_assertions::const_assert_eq;
use std::{assert, fmt};
use std::fmt::{Debug, Formatter};
use std::mem::{swap, transmute, MaybeUninit};
use std::sync::atomic::AtomicU8;
use umolc::{
    o_project, BufferManager, BufferManagerExt, BufferManagerGuard, ExclusiveGuard, OPtr, OlcErrorHandler, PageId,
};
use crate::fully_dense_leaf::FullyDenseLeaf;

pub mod node_tag {
    pub const METADATA_MARKER: u8 = 43;
    pub const BASIC_INNER: u8 = 250;
    pub const BASIC_LEAF: u8 = 251;
    pub const HASH_LEAF: u8 = 252;
    pub const FULLY_DENSE_LEAF: u8 = 253;
}

#[cfg(feature = "page_1k")]
pub const PAGE_SIZE: usize = 1024;

#[cfg(feature = "page_4k")]
pub const PAGE_SIZE: usize = 4096;

const NODE_TAIL_SIZE: usize = PAGE_SIZE - size_of::<CommonNodeHead>();

#[derive(Debug, Zeroable)]
#[repr(C)]
pub struct CommonNodeHead {
    pub tag: u8,
    pub counter: AtomicU8,
    pub prefix_len: u16,
    pub count: u16,
    pub lower_fence_len: u16,
    pub upper_fence_len: u16,
    pub scan_counter: u8,
    pub _pad: u8,
}

const NODE_UNSAFE_CELL_HEAD: usize = 2;

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
pub struct DebugNode {
    pub prefix_len: usize,
    pub lf: Vec<u8>,
    pub uf: Vec<u8>,
    pub keys: Vec<Vec<u8>>,
    pub values: Vec<Vec<u8>>,
}

#[macro_export]
macro_rules! define_node {
    (
        pub struct $struct_name:ident $(<$($generic_param:ident),+>)?{
            pub common: CommonNodeHead,
            $($v:vis $f:ident:$t:ty,)*
        }) => {
        #[derive(bytemuck::Pod,bytemuck::Zeroable,Clone,Copy)]
        #[allow(dead_code)]
        #[repr(C,align(16))]
        struct ToFromPageExtCheck{
            common:[u16;size_of::<$crate::node::CommonNodeHead>()/2],
            $($f:$t),*
        }

        #[derive(bytemuck::Zeroable)]
        #[repr(C,align(16))]
        pub struct $struct_name $(<$($generic_param),*>)?{
            common:$crate::node::CommonNodeHead,
            $(_p:std::marker::PhantomData<($($generic_param,)*)>,)?
            $($v $f:$t),*
        }

        static_assertions::assert_eq_size!(ToFromPageExtCheck, $crate::node::Page);
        static_assertions::assert_eq_align!(ToFromPageExtCheck, $crate::node::Page);
        unsafe impl$(<$($generic_param),*>)? $crate::node::ToFromPage for $struct_name$(<$($generic_param),*>)? {}
    };
}
pub enum PromoteError {
    ValueLen,
    Keys,
    Capacity,
    Node,
    Fences,
}

impl fmt::Display for PromoteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PromoteError::*;
        let msg = match self {
            Fences => "Not all fences exist.",
            Keys => "Not all keys share the same prefix.",
            ValueLen => "Not all values have the same length.",
            Capacity => "Dense capacity exceeds limit.",
            Node => "This node cannot be promoted."
        };
        write!(f, "{}", msg)
    }
}

impl Debug for PromoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for PromoteError {}

pub fn page_cast<A: ToFromPage, B: ToFromPage>(a: &A) -> &B {
    unsafe { transmute::<&A, &B>(a) }
}
pub fn page_cast_mut<A: ToFromPage, B: ToFromPage>(a: &mut A) -> &mut B {
    unsafe { transmute::<&mut A, &mut B>(a) }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe trait ToFromPage {}

pub trait ToFromPageExt: ToFromPage + Sized {
    fn as_page(&self) -> &Page {
        page_cast::<Self, Page>(self)
    }

    /// take mutable reference to prevent concurrent atomic accesses
    fn copy_page(&mut self) -> Page {
        unsafe { (self as *const Self as *const Page).read() }
    }

    fn as_page_mut(&mut self) -> &mut Page {
        page_cast_mut::<Self, Page>(self)
    }

    fn cast<T: ToFromPage>(&self) -> &T {
        page_cast(self)
    }

    fn cast_mut<T: ToFromPage>(&mut self) -> &mut T {
        page_cast_mut(self)
    }

    fn slice<T: Pod>(&self, count_offset: usize, len: usize) -> &[T] {
        assert_eq!(align_of::<Page>() % align_of::<T>(), 0);
        assert!(count_offset * size_of::<T>() >= NODE_UNSAFE_CELL_HEAD);
        assert!((count_offset + len) <= size_of::<Page>() / size_of::<T>());
        unsafe { std::slice::from_raw_parts((self as *const Self as *const T).add(count_offset), len) }
    }

    fn slice_mut<T: Pod>(&mut self, count_offset: usize, len: usize) -> &mut [T] {
        assert_eq!(align_of::<Page>() % align_of::<T>(), 0);
        assert!(count_offset * size_of::<T>() >= NODE_UNSAFE_CELL_HEAD);
        assert!((count_offset + len) <= size_of::<Page>() / size_of::<T>());
        unsafe { std::slice::from_raw_parts_mut((self as *mut Self as *mut T).add(count_offset), len) }
    }

    fn read_unaligned<T: Pod>(&self, offset: usize) -> T {
        assert!(offset <= const { PAGE_SIZE - size_of::<T>() });
        unsafe { (self as *const Self as *const T).byte_add(offset).read_unaligned() }
    }

    fn page_index<T: Pod>(&self, index: usize) -> &T {
        &self.slice(index, 1)[0]
    }

    fn page_index_mut<T: Pod>(&mut self, index: usize) -> &mut T {
        &mut self.slice_mut(index, 1)[0]
    }

    fn lower_fence(&self) -> &[u8] {
        let l = self.as_page().common.lower_fence_len as usize;
        self.slice::<u8>(size_of::<Self>() - l, l)
    }

    fn prefix(&self) -> &[u8] {
        self.slice::<u8>(
            size_of::<Self>() - self.as_page().common.lower_fence_len as usize,
            self.as_page().common.prefix_len as usize,
        )
    }

    fn upper_fence_tail(&self) -> &[u8] {
        self.slice::<u8>(
            size_of::<Self>()
                - self.as_page().common.lower_fence_len as usize
                - self.as_page().common.upper_fence_len as usize,
            self.as_page().common.upper_fence_len as usize,
        )
    }

    fn upper_fence_combined(&self) -> SourceSlicePair<u8, &[u8], &[u8]> {
        self.prefix().join(self.upper_fence_tail())
    }

    fn fences_start(&self) -> usize {
        let head = &self.as_page().common;
        size_of::<Self>() - head.lower_fence_len as usize - head.upper_fence_len as usize
    }

    fn relocate_by<const UP: bool, T: Pod>(&mut self, offset: usize, count: usize, dist: usize) {
        assert_eq!(offset % size_of::<T>(), 0);
        let offset = offset / size_of::<T>();
        if UP {
            self.slice_mut::<T>(offset, count + dist).copy_within(0..count, dist);
        } else {
            self.slice_mut::<T>(offset - dist, count + dist).copy_within(dist..dist + count, 0);
        }
    }

    fn read_unaligned_u16(&self, offset: usize) -> usize {
        assert!(offset + 2 <= PAGE_SIZE);
        unsafe { (self as *const Self as *const u16).byte_add(offset).read_unaligned() as usize }
    }

    fn store_unaligned_u64(&mut self, offset: usize, v: u64) {
        assert!(offset + 8 <= PAGE_SIZE);
        unsafe { (self as *mut Self as *mut u64).byte_add(offset).write_unaligned(v) }
    }
}

impl<T: ToFromPage> ToFromPageExt for T {}

pub trait NodeStatic<'bm, BM: BufferManager<'bm, Page = Page>>: NodeDynamic<'bm, BM> + Zeroable {
    const TAG: u8;
    const IS_INNER: bool;
    type TruncatedKey<'a>: SourceSlice + 'a
    where
        Self: 'a;
    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()>;
    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: Option<&[u8; 5]>);
    /// first returns lower with empty slice, then pairs
    /// keys are prefix truncated
    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)>;

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>>;
    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, key: &[u8], high_on_equal: bool) -> PageId;

    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>);

    fn set_scan_counter(&mut self, counter: u8);

    fn hasGoodHeads(&self) -> (bool, bool);
}

pub trait NodeDynamic<'bm, BM: BufferManager<'bm, Page = Page>>: ToFromPage + NodeDynamicAuto<'bm, BM> + Debug {
    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>, key: &[u8]) -> Result<(), ()>;
    fn merge(&mut self, right: &mut Page);
    fn validate(&self);
    fn leaf_remove(&mut self, k: &[u8]) -> Option<()>;

    fn scan_with_callback(&self, buffer: &mut [MaybeUninit<u8>; 512], start : Option<&[u8]>, callback: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> bool;

    fn get_node_tag(&self) -> u8;

    fn get_scan_counter(&self) -> u8;

    fn can_promote(&self, to: u8) -> Result<(), PromoteError>;

    fn promote(&mut self, to: u8);

    fn qualifies_for_promote(&self) -> Option<u8>;

    fn retry_later (&mut self);
}

pub trait NodeDynamicAuto<'bm, BM: BufferManager<'bm, Page = Page>> {
    fn free_children(&mut self, bm: BM);
    fn validate_inter_node_fences<'b>(
        &self,
        bm: BM,
        lb: &mut &'b mut [u8; MAX_KEY_SIZE],
        hb: &mut &'b mut [u8; MAX_KEY_SIZE],
        ll: usize,
        hl: usize,
    );
    fn to_debug(&self) -> DebugNode;
    fn is_inner(&self) -> bool;

    #[allow(clippy::result_unit_err)]
    fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()>;
}

impl<'bm, BM: BufferManager<'bm, Page = Page>, N: NodeStatic<'bm, BM>> NodeDynamicAuto<'bm, BM> for N {
    fn free_children(&mut self, bm: BM) {
        if !Self::IS_INNER {
            return;
        }
        for (_key, child) in self.iter_children() {
            let mut child = bm.lock_exclusive(child);
            child.as_dyn_node_mut().free_children(bm);
        }
    }

    fn to_debug(&self) -> DebugNode {
        let (keys, values) = self.to_debug_kv();
        let p = self.as_page();
        DebugNode {
            prefix_len: p.common.prefix_len as usize,
            lf: p.lower_fence().to_vec(),
            uf: p.upper_fence_combined().to_vec(),
            keys,
            values,
        }
    }

    fn validate_inter_node_fences<'b>(
        &self,
        bm: BM,
        lb: &mut &'b mut [u8; MAX_KEY_SIZE],
        hb: &mut &'b mut [u8; MAX_KEY_SIZE],
        mut ll: usize,
        mut hl: usize,
    ) {
        let pl = self.as_page().common.prefix_len as usize;
        let pl_limit = pl <= ll && pl <= hl;
        let pl = if pl_limit { pl } else { 0 };
        let lf = self.lower_fence() == &lb[..ll];
        let uf = self.upper_fence_tail() == &hb[pl..hl];
        let pf = self.prefix() == &hb[..pl];
        assert!(
            pl_limit && lf && uf && pf,
            "inter node validation failed.\n\
            prefix limit:{pl},\nprefix:{pf},\nlower fence:{lf},\nupper fence:{uf},\n\
            expected upper: {:?},\nexpected lower: {:?},\nnode:\n{:?}",
            BStr::new(&lb[..ll]),
            BStr::new(&hb[..hl]),
            self
        );
        if !Self::IS_INNER {
            return;
        }
        let common = &self.as_page().common;
        let prefix = common.prefix_len as usize;
        let mut children = self.iter_children();
        let mut pre_child = children.next().unwrap().1;
        for (child_lower_fence, child) in children {
            hl = prefix + child_lower_fence.len();
            child_lower_fence.write_to(&mut hb[prefix..hl]);
            bm.lock_shared(pre_child).as_dyn_node().validate_inter_node_fences(bm, lb, hb, ll, hl);
            pre_child = child;
            ll = hl;
            swap(hb, lb);
        }
        hl = prefix + self.upper_fence_tail().len();
        hb[prefix..hl].copy_from_slice(self.upper_fence_tail());
        bm.lock_shared(pre_child).as_dyn_node().validate_inter_node_fences(bm, lb, hb, ll, hl);
        ll = self.lower_fence().len();
        lb[prefix..ll].copy_from_slice(&self.lower_fence()[prefix..]);
    }

    fn is_inner(&self) -> bool {
        Self::IS_INNER
    }

    fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()> {
        let ret = self.insert(key, &page_id_to_bytes(pid));
        ret.map(|x| debug_assert!(x.is_none()||x.is_some()))
    }

    fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        self.insert(key, val)
    }
}

pub const PAGE_ID_LEN: usize = 5;

pub fn page_id_to_bytes(p: PageId) -> [u8; PAGE_ID_LEN] {
    let b = p.x.to_le_bytes();
    debug_assert!(p.x < (1 << (8 * PAGE_ID_LEN)));
    b[..PAGE_ID_LEN].try_into().unwrap()
}

pub fn page_id_from_bytes(x: &[u8; PAGE_ID_LEN]) -> PageId {
    let mut b = [0; 8];
    b[..PAGE_ID_LEN].copy_from_slice(&x[..]);
    PageId { x: u64::from_ne_bytes(b) }
}

pub fn page_id_from_olc_bytes<O: OlcErrorHandler>(x: OPtr<[u8; PAGE_ID_LEN], O>) -> PageId {
    let mut b = [0; 8];
    unsafe {
        std::ptr::copy(x.to_raw() as *const u8, b.as_mut_ptr(), PAGE_ID_LEN);
    }
    PageId { x: u64::from_ne_bytes(b) }
}




#[repr(C, align(16))]
#[derive(Zeroable)]
pub struct Page {
    pub common: CommonNodeHead,
    _pad: [u8; NODE_TAIL_SIZE],
}

const_assert_eq!(size_of::<Page>(), PAGE_SIZE);
unsafe impl ToFromPage for Page {}

pub trait NodeKind: Pod {
    const IS_LEAF: bool;
    type BasicValLength: HeapLength;
}

impl NodeKind for KindInner {
    const IS_LEAF: bool = false;
    type BasicValLength = ConstHeapLength<PAGE_ID_LEN>;
}

impl NodeKind for KindLeaf {
    const IS_LEAF: bool = true;
    type BasicValLength = u16;
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
    let new_guard = bm.alloc();
    separator.to_ref_buffer::<MAX_KEY_SIZE, _>(|sep| {
        if let Ok(()) = parent.insert_inner(sep, new_guard.page_id()) {
            Ok(new_guard)
        } else {
            new_guard.dealloc();
            Err(())
        }
    })
}

macro_rules! invoke_all_nodes {
    ($m:ident) => {
        $m!(BasicInner, BasicLeaf, HashLeaf, MetadataPage, FullyDenseLeaf)
    };
}

impl Page {
    pub fn common_init(&mut self, tag: u8, lf: impl SourceSlice, uf: impl SourceSlice) {
        self.common.tag = tag;
        self.common.count = 0;
        let ll = lf.len();
        lf.write_to(self.slice_mut(size_of::<Self>() - ll, ll));
        self.common.lower_fence_len = ll as u16;
        self.init_upper_fence(uf);
    }

    pub fn init_upper_fence(&mut self, uf: impl SourceSlice) {
        let pl = common_prefix(self.lower_fence(), uf);
        let ll = self.common.lower_fence_len as usize;
        let ul = uf.len() - pl;
        self.common.prefix_len = pl as u16;
        self.common.upper_fence_len = ul as u16;
        uf.slice_start(self.common.prefix_len as usize).write_to(self.slice_mut(size_of::<Self>() - ll - ul, ul));
    }

    pub fn as_dyn_node<'bm, BM: BufferManager<'bm, Page = Page>>(&self) -> &dyn NodeDynamic<'bm, BM> {
        let tag = self.common.tag;
        macro_rules! impl_case {
            ($($t:ty),*) => {
                $(if tag==<$t as NodeStatic<'bm,BM>>::TAG {
                    return self.cast::<$t>()
                })*
            };
        }
        invoke_all_nodes!(impl_case);
        panic!("unexpected node tag: {tag}");
    }
    pub fn as_dyn_node_mut<'bm, BM: BufferManager<'bm, Page = Page>>(&mut self) -> &mut dyn NodeDynamic<'bm, BM> {
        let tag = self.common.tag;
        macro_rules! impl_case {
            ($($t:ty),*) => {
                $(if tag==<$t as NodeStatic<'bm,BM>>::TAG {
                    return self.cast_mut::<$t>()
                })*
            };
        }
        invoke_all_nodes!(impl_case);
        panic!("unexpected node tag: {tag}");
    }


    pub fn increase_scan_counter(&mut self){
        if self.common.scan_counter == 255 {
            return;
        }
        if self.common.scan_counter < 3 {
            self.common.scan_counter += 1;
        }
    }

    pub fn decrease_scan_counter(&mut self) {
        if self.common.scan_counter == 255 {
            return;
        }
        else if self.common.scan_counter > 0 {
            self.common.scan_counter -= 1;
        }
    }
}

pub fn o_ptr_lookup_inner<'bm, BM: BufferManager<'bm, Page = Page>>(
    this: OPtr<'_, BM::Page, BM::OlcEH>,
    key: &[u8],
    high_on_equal: bool,
) -> PageId {
    let tag = o_project!(this.common.tag).r();
    macro_rules! impl_case {
            ($($t:ty),*) => {
                $(if tag==<$t as NodeStatic<'bm,BM>>::TAG {
                    return <$t as NodeStatic<'bm,BM>>::lookup_inner(this.cast(),key,high_on_equal)
                })*
            };
        }
    invoke_all_nodes!(impl_case);
    BM::OlcEH::optimistic_fail()
}

pub fn o_ptr_lookup_leaf<'a, 'bm, BM: BufferManager<'bm, Page = Page>>(
    this: OPtr<'a, BM::Page, BM::OlcEH>,
    key: &[u8],
) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
    let tag = o_project!(this.common.tag).r();
    macro_rules! impl_case {
            ($($t:ty),*) => {
                $(if tag==<$t as NodeStatic<'bm,BM>>::TAG {
                    return <$t as NodeStatic<'bm,BM>>::lookup_leaf(this.cast(),key)
                })*
            };
        }
    invoke_all_nodes!(impl_case);
    BM::OlcEH::optimistic_fail()
}

pub fn o_ptr_is_inner<'bm, BM: BufferManager<'bm, Page = Page>>(this: OPtr<'_, BM::Page, BM::OlcEH>) -> bool {
    let tag = o_project!(this.common.tag).r();
    macro_rules! impl_case {
            ($($t:ty),*) => {
                $(if tag==<$t as NodeStatic<'bm,BM>>::TAG {
                    return <$t as NodeStatic<'bm,BM>>::IS_INNER
                })*
            };
        }
    invoke_all_nodes!(impl_case);
    BM::OlcEH::optimistic_fail()
}

/// returns the number of keys in the low node and the separator (including prefix)
pub fn find_separator<'a, 'bm, BM: BufferManager<'bm, Page = Page>, N: NodeStatic<'bm, BM>>(
    node: &'a N,
    mut get_key: impl FnMut(usize) -> N::TruncatedKey<'a>,
) -> (usize, SourceSlicePair<u8, &'a [u8], N::TruncatedKey<'a>>) {
    let count = node.as_page().common.count as usize;
    if N::IS_INNER {
        let low_count = count / 2;
        let sep = node.as_page().prefix().join(get_key(low_count));
        (low_count, sep)
    } else {
        let range_start = count / 2 - count / 8;
        let range_end = count / 2 + count / 8;
        let common_prefix = common_prefix(get_key(range_start - 1), get_key(range_end));
        let best_split = (range_start..=range_end)
            .filter(|&lc| {
                get_key(lc - 1).len() == common_prefix
                    || get_key(lc - 1).index_ss(common_prefix) != get_key(lc).index_ss(common_prefix)
            })
            .min_by_key(|&lc| (lc as isize - count as isize / 2).abs())
            .unwrap();
        let sep = get_key(best_split).slice(..common_prefix + 1);
        (best_split, node.as_page().prefix().join(sep))
    }
}
