use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::hash_leaf::HashLeaf;
use crate::key_source::SourceSlice;
use crate::node::{
    node_tag, o_ptr_is_inner, o_ptr_lookup_inner, o_ptr_lookup_leaf, page_cast, page_cast_mut, page_id_to_bytes,
    CommonNodeHead, DebugNode, KindLeaf, NodeDynamic, NodeDynamicAuto, NodeStatic, Page, ToFromPageExt, PAGE_SIZE,
};
use crate::util::PodPad;
use crate::{impl_to_from_page, MAX_KEY_SIZE, MAX_VAL_SIZE};
use bytemuck::{Pod, Zeroable};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use umolc::{
    o_project, BufferManageGuardUpgrade, BufferManager, BufferManagerExt, BufferManagerGuard, ExclusiveGuard, OPtr,
    OlcErrorHandler, OptimisticGuard, PageId,
};

pub struct Tree<'bm, BM: BufferManager<'bm, Page = Page>> {
    meta: PageId,
    bm: BM,
    _p: PhantomData<&'bm BM>,
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> Tree<'bm, BM> {
    pub fn new(bm: BM) -> Self {
        let mut meta_guard = bm.alloc();
        let mut root_guard = bm.alloc();
        {
            let meta = page_cast_mut::<_, MetadataPage>(&mut *meta_guard);
            meta.root = root_guard.page_id();
            meta.node_head.tag = node_tag::METADATA_MARKER;
        }
        NodeStatic::<BM>::init(root_guard.cast_mut::<HashLeaf>(), &[][..], &[][..], None);
        Tree { meta: meta_guard.page_id(), bm, _p: PhantomData }
    }

    fn validate_fences(&self) {
        if !cfg!(feature = "validate_tree") {
            return;
        }
        let mut low_buffer = [0u8; MAX_KEY_SIZE];
        let mut high_buffer = [0u8; MAX_KEY_SIZE];
        let meta = self.bm.lock_shared(self.meta);
        let root = self.bm.lock_shared(page_cast::<_, MetadataPage>(&*meta).root);
        drop(meta);
        root.as_dyn_node().validate_inter_node_fences(self.bm, &mut &mut low_buffer, &mut &mut high_buffer, 0, 0)
    }

    pub fn remove(&self, k: &[u8]) -> Option<()> {
        let mut removed = false;
        BM::repeat(|| {
            self.try_remove(k, &mut removed);
        });
        if removed {
            Some(())
        } else {
            None
        }
    }

    fn try_remove(&self, k: &[u8], removed: &mut bool) {
        let [parent, node] = self.descend(k, None);
        let mut node: BM::GuardX = node.upgrade();
        if node.as_dyn_node_mut::<BM>().leaf_remove(k).is_some() {
            *removed = true;
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = BM::repeat(|| self.try_insert(k, val));
        self.validate_fences();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [BM::GuardO; 2] {
        let mut parent = self.bm.lock_optimistic(self.meta);
        let mut node_pid = self.meta;
        let mut node = self.bm.lock_optimistic(self.meta);
        while o_ptr_is_inner::<BM>(node.o_ptr()) && Some(node_pid) != stop_at {
            node_pid = o_ptr_lookup_inner::<BM>(node.o_ptr(), k, true);
            node.check(); // check here so we do not attempt to lock wrong page id
            parent.release_unchecked();
            parent = node;
            node = self.bm.lock_optimistic(node_pid);
        }
        // ensure we return the correct node
        // we could push this responsibility on the caller, but this has proven error-prone
        parent.check();
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if node.page_id() == split_target {
                let mut node: BM::GuardX = node.upgrade();
                let mut parent: BM::GuardX = parent.upgrade();
                self.ensure_parent_not_meta(&mut parent);
                if self.split_locked_node(node.as_dyn_node_mut(), parent.as_dyn_node_mut()).is_ok() {
                    None
                } else {
                    Some(parent.page_id())
                }
            } else {
                None
            }
        };
        if let Some(p) = parent_id {
            self.split_and_insert(p, k, val)
        } else {
            self.try_insert(k, val)
        }
    }

    fn ensure_parent_not_meta(&self, parent: &mut BM::GuardX) {
        if parent.common.tag == node_tag::METADATA_MARKER {
            let meta = parent.cast_mut::<MetadataPage>();
            let mut new_root = self.bm.alloc();
            NodeStatic::<BM>::init(
                new_root.cast_mut::<BasicInner>(),
                &[][..],
                &[][..],
                Some(&page_id_to_bytes(meta.root)),
            );
            meta.root = new_root.page_id();
            *parent = new_root
        }
    }

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let mut node: BM::GuardX = node.upgrade();
        match node.as_dyn_node_mut::<BM>().insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                x
            }
            Err(()) => {
                node.reset_written();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_meta(&mut parent);
                if self.split_locked_node(node.as_dyn_node_mut(), parent.as_dyn_node_mut()).is_err() {
                    let parent_id = parent.page_id();
                    drop(parent);
                    drop(node);
                    return self.split_and_insert(parent_id, k, val);
                }
                drop(parent);
                drop(node);
                // TODO could descend from parent
                self.try_insert(k, val)
            }
        }
    }

    pub fn lookup_to_vec(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.lookup_inspect(k, |v| v.map(|v| v.load_slice_to_vec()))
    }

    pub fn lookup_to_buffer<'a>(&self, k: &[u8], b: &'a mut [MaybeUninit<u8>; MAX_VAL_SIZE]) -> Option<&'a mut [u8]> {
        let valid_len = self.lookup_inspect(k, |v| v.map(|v| v.load_bytes_uninit(&mut b[..v.len()]).len()));
        valid_len.map(|l| unsafe { MaybeUninit::slice_assume_init_mut(&mut b[..l]) })
    }

    pub fn lookup_inspect<R>(&self, k: &[u8], mut f: impl FnMut(Option<OPtr<[u8], BM::OlcEH>>) -> R) -> R {
        BM::repeat(move || if let Some((_guard, val)) = self.try_lookup(k) { f(Some(val)) } else { f(None) })
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<(BM::GuardO, OPtr<[u8], BM::OlcEH>)> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let val = o_ptr_lookup_leaf::<BM>(node.o_ptr_bm(), k)?;
        Some((node, val))
    }

    fn split_locked_node(
        &self,
        node: &mut dyn NodeDynamic<'bm, BM>,
        parent: &mut dyn NodeDynamic<'bm, BM>,
    ) -> Result<(), ()> {
        //TODO inline
        node.split(self.bm, parent)
    }

    pub fn lock_path(&self, key: &[u8]) -> Vec<BM::GuardS> {
        let mut path = Vec::new();
        let mut node = {
            let parent = self.bm.lock_shared(self.meta);
            let node_pid = parent.cast::<MetadataPage>().root;
            path.push(parent);
            self.bm.lock_shared(node_pid)
        };
        while node.as_dyn_node::<BM>().is_inner() {
            let node_pid = o_ptr_lookup_inner::<BM>(node.o_ptr(), key, true);
            path.push(node);
            node = self.bm.lock_shared(node_pid);
        }
        path.push(node);
        path
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> Drop for Tree<'bm, BM> {
    fn drop(&mut self) {
        let mut meta_lock = self.bm.lock_exclusive(self.meta);
        meta_lock.cast_mut::<MetadataPage>().free_children(self.bm);
        meta_lock.dealloc();
    }
}

#[derive(Pod, Zeroable, Copy, Clone, Debug)]
#[repr(C, align(16))]
pub struct MetadataPage {
    node_head: CommonNodeHead,
    _pad1: [u8; (8 - size_of::<CommonNodeHead>() % 8) % 8],
    root: PageId,
    _pad2: PodPad<{ PAGE_SIZE - size_of::<CommonNodeHead>() - 8 - (8 - size_of::<CommonNodeHead>() % 8) % 8 }>,
}

impl_to_from_page!(MetadataPage);

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for MetadataPage {
    const TAG: u8 = node_tag::METADATA_MARKER;
    const IS_INNER: bool = true;
    type TruncatedKey<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        unimplemented!()
    }

    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: Option<&[u8; 5]>) {
        unimplemented!()
    }

    fn iter_children(&self) -> impl Iterator<Item = (&[u8], PageId)> {
        std::iter::once((&[][..], self.root))
    }

    fn lookup_leaf<'a>(_this: OPtr<'a, Self, BM::OlcEH>, _key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        BM::OlcEH::optimistic_fail()
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, _key: &[u8], _high_on_equal: bool) -> PageId {
        PageId { x: o_project!(this.root.x).r() }
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for MetadataPage {
    fn split(&mut self, _bm: BM, _parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        unimplemented!()
    }

    fn to_debug(&self) -> DebugNode {
        DebugNode {
            prefix_len: 0,
            lf: Vec::new(),
            uf: Vec::new(),
            keys: Vec::new(),
            values: vec![page_id_to_bytes(self.root).to_vec()],
        }
    }

    fn merge(&mut self, _right: &mut Page) {
        unimplemented!()
    }

    fn validate(&self) {}

    fn leaf_remove(&mut self, _k: &[u8]) -> Option<()> {
        todo!()
    }
}
