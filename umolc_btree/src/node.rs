use umolc::{BufferManager, OlcAtomic, OlcSafe};

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

#[repr(C)]
pub struct CommonNodeHead {
    pub tag: OlcAtomic<u8>,
    _pad: OlcAtomic<u8>,
    pub prefix_len: OlcAtomic<u16>,
    pub count: OlcAtomic<u16>,
    pub lower_fence_len: OlcAtomic<u16>,
    pub upper_fence_len: OlcAtomic<u16>,
}

#[no_mangle]
pub unsafe fn print_page(p: *const Page) {
    let p:Page = p.read();
    if p.common.tag.r() == node_tag::METADATA_MARKER{
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

pub unsafe trait Node<'bm,BM:BufferManager<'bm>,PI:ParentInserter<'bm,BM>>: OlcSafe {
    const TAG: u8;
    type DebugVal: Eq + Debug;

    /// fails iff parent_insert fails.
    /// if node is near empty, no split is performed and parent_insert is not called.
    fn split<'g>(
        this: &mut Self,
        parent_insert: PI,
        ref_key: &[u8],
    ) -> Result<(), ()>;

    fn find_separator<'a>(this: &'a Self, ref_key: &'a [u8]) -> (usize, impl SourceSlice + 'a);

    fn to_debug_kv(this: &Self) -> (Vec<Vec<u8>>, Vec<Self::DebugVal>);

    fn merge(this: &mut W<Guarded<Exclusive, Self>>, right: &mut W<Guarded<Exclusive, Self>>, ref_key: &[u8]);
    fn format(this: &W<Guarded<Optimistic, Self>>, f: &mut Formatter) -> std::fmt::Result
    where
        Self: Copy;
    fn validate(this: W<Guarded<'_, Shared, Self>>);
}

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct KindInner;
#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct KindLeaf;

pub trait ParentInserter<'bm, BM: BufferManager<'bm>> {
    fn insert_upper_sibling(self, separator: impl SourceSlice) -> Result<Guard<'bm, BM, Exclusive, PageTail>, ()>;
}

pub trait NodeKind: Pod {
    const IS_LEAF: bool;
    type Lower;
    type SliceType: Pod + SeqLockWrappable;
    type DebugVal: Eq + Debug;

    fn from_lower(x: Self::Lower) -> [Self::SliceType; 3];
    fn to_lower(x: [Self::SliceType; 3]) -> Self::Lower;
    fn to_debug(x: Vec<Self::SliceType>) -> Self::DebugVal;
}

impl NodeKind for KindInner {
    const IS_LEAF: bool = false;
    type Lower = PageId;
    type SliceType = u16;
    type DebugVal = PageId;

    fn from_lower(x: Self::Lower) -> [Self::SliceType; 3] {
        page_id_to_3x16(x)
    }

    fn to_lower(x: [Self::SliceType; 3]) -> Self::Lower {
        page_id_from_3x16(x)
    }

    fn to_debug(x: Vec<Self::SliceType>) -> Self::DebugVal {
        page_id_from_3x16(x.try_into().unwrap())
    }
}

impl NodeKind for KindLeaf {
    const IS_LEAF: bool = true;
    type Lower = ();
    type SliceType = u8;
    type DebugVal = Vec<u8>;

    fn from_lower(_: Self::Lower) -> [Self::SliceType; 3] {
        unimplemented!();
    }

    fn to_lower(_: [Self::SliceType; 3]) -> Self::Lower {
        unimplemented!();
    }

    fn to_debug(x: Vec<Self::SliceType>) -> Self::DebugVal {
        x
    }
}

impl<'a, M: SeqLockMode> W<Guarded<'a, M, PageTail>> {
    pub fn node_cast<N: Node>(self) -> N::Wrapper<Guarded<'a, M, N>> {
        if M::EXCLUSIVE {
            debug_assert_eq!(self.s().0.cast::<BasicLeaf>().tag().load(), N::TAG);
        }
        self.0.cast::<N>()
    }
}

pub fn node_guard_cast<'bm, N: Node, BM: BufferManager<'bm, Page = PageTail>, M: SeqLockMode>(
    guard: Guard<'bm, BM, M, PageTail>,
) -> Guard<'bm, BM, M, N> {
    unsafe { guard.map(|x| x.node_cast::<N>()) }
}

impl<'a, N: Node> W<Guarded<'a, Shared, N>> {
    pub fn to_debug(self) -> DebugNode<N::DebugVal> {
        let (keys, values) = N::to_debug_kv(self);
        let as_basic = self.cast::<BasicLeaf>();
        DebugNode {
            prefix_len: as_basic.prefix_len().load() as usize,
            lf: as_basic.lower_fence().load_slice_to_vec(),
            uf: as_basic.upper_fence().load_slice_to_vec(),
            keys,
            values,
        }
    }
}

impl<'a, N: Node, M: SeqLockMode> W<Guarded<'a, M, N>> {
    pub fn upcast(self) -> W<Guarded<'a, M, PageTail>> {
        self.0.cast::<PageTail>()
    }
    pub fn common_head(self) -> W<Guarded<'a, M, CommonNodeHead>> {
        unsafe { self.0.map_ptr(|x| x as *mut CommonNodeHead) }
    }

    pub fn slice<T: SeqLockWrappable + Pod>(self, offset: usize, count: usize) -> Guarded<'a, M, [T]> {
        self.as_bytes().slice(offset..offset + count * size_of::<T>()).cast_slice::<T>()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn as_bytes(self) -> Guarded<'a, M, [u8]> {
        const SIZE: usize = PAGE_SIZE;
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

#[repr(C, align(8))]
pub struct Page {
    pub common: CommonNodeHead,
    _pad: [OlcAtomic<u8>; NODE_TAIL_SIZE],
}

unsafe impl OlcSafe for Page{}
