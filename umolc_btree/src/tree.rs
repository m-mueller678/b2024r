use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::key_source::SourceSlice;
use crate::node::{
    node_tag, page_cast, page_cast_mut, CommonNodeHead, KindInner, KindLeaf, NodeDynamic, NodeStatic, Page,
    ToFromPageExt, PAGE_SIZE,
};
use crate::util::PodPad;
use crate::{impl_to_from_page, MAX_KEY_SIZE};
use bytemuck::{Pod, Zeroable};
use std::marker::PhantomData;
use std::mem::size_of;
use umolc::{BufferManageGuardUpgrade, BufferManager, BufferManagerExt, BufferManagerGuard, OPtr, PageId};

pub struct Tree<'bm, BM: BufferManager<'bm, Page = Page>> {
    meta: PageId,
    bm: BM,
    _p: PhantomData<&'bm BM>,
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> Tree<'bm, BM> {
    pub fn new(bm: BM) -> Self {
        let (mut meta_guard, meta_id) = bm.alloc();
        let (mut root_guard, root_id) = bm.lock_new();
        {
            let mut meta = page_cast_mut::<_, MetadataPage>(&mut *meta_guard);
            meta.root = root_id;
            meta.node_head.tag = node_tag::METADATA_MARKER;
        }
        page_cast_mut::<_, BasicNode<KindLeaf>>(&mut *root_guard).init(&[], &[], None);
        Tree { meta: meta_id, bm, _p: PhantomData }
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
        let mut node = node.upgrade_wait();
        if node.as_dyn_node_mut() > ().leaf_remove(k).is_some() {
            *removed = true;
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = BM::repeat(|| self.try_insert(k, val));
        self.validate_fences_exclusive();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [BM::GuardO; 2] {
        let mut parent = self.bm.lock_optimistic(self.meta);
        let mut node_pid = parent.s().cast::<MetadataPage>().root().load();
        parent.check(); // check here so we do not attempt to lock wrong page id
        let mut node = self.bm.lock_optimistic(node_pid);
        parent.check(); // check here again to ensure node is still the same child
        while node.s().common().tag().load() == node_tag::BASIC_INNER && Some(node_pid) != stop_at {
            parent.release_unchecked();
            parent = node;
            node_pid = BasicNode::<KindInner>::lookup_inner(parent.cast::<BasicNode<KindInner>>(), k, true);
            parent.check(); // check here so we do not attempt to lock wrong page id
            node = self.bm.lock_optimistic(node_pid);
            parent.check(); // check here again to ensure node is still the same child
                            // We check here instead of the loop start to ensure the returned child guard always points to the right node.
                            // Otherwise, a caller may acquire an exclusive lock on the wrong page before dropping the parent guard
        }
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if self.bm.page_id(node.page_address()) == split_target {
                let mut node = node.upgrade();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut parent);
                debug_assert!(node.common().tag().load() == node_tag::BASIC_INNER);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicInner>(),
                    parent.b().node_cast::<BasicInner>(),
                    self.bm,
                )
                .is_ok()
                {
                    None
                } else {
                    Some(self.bm.page_id(parent.page_address()))
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

    fn ensure_parent_not_root(&self, parent: &mut BM::GuardX) {
        if self.bm.page_id(parent.page_address()) == self.meta {
            let mut meta = parent.b().0.cast::<MetadataPage>();
            let (new_root_id, mut new_root_guard) = self.bm.lock_new();
            new_root_guard.b().0.cast::<BasicInner>().init(&[][..], &[][..], page_id_to_3x16(meta.root().load()));
            meta.root_mut().store(new_root_id);
            *parent = new_root_guard
        }
    }

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        match node.b().node_cast::<BasicLeaf>().insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                x
            }
            Err(()) => {
                node.reset_written();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut parent);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicLeaf>(),
                    parent.b().node_cast::<BasicInner>(),
                    self.bm,
                )
                .is_err()
                {
                    let parent_id = self.bm.page_id(parent.page_address());
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
        BM::repeat(|| self.try_lookup(k).map(|v| v.load_slice_to_vec()))
    }

    pub fn lookup_inspect<R>(&self, k: &[u8], mut f: impl FnMut(OPtr<[u8], BM>) -> R) -> R {
        BM::repeat(move || f(self.try_lookup(k)))
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<(BM::GuardO, OPtr<[u8], BM>)> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let val = BasicLeaf::lookup_leaf(node, k)?;
        Some((node, val))
    }

    fn split_locked_node(
        &self,
        k: &[u8],
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
        while node.is_inner() {
            let node_pid = BasicInner::lookup_inner(OPtr::node, key, true);
            path.push(node);
            node = self.bm.lock_exclusive(node_pid);
        }
        path.push(node);
        path
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> Drop for Tree<'bm, BM> {
    fn drop(&mut self) {
        fn free_recursive<'bm, BM: BufferManager<'bm, Page = Page>>(bm: BM, p: PageId) {
            let node = bm.lock_exclusive(p);
            if node.as_dyn_node().is_inner() {
                let node = node.optimistic().node_cast::<BasicInner>();
                for i in 0..node.common.count as usize {
                    free_recursive(bm, node.index_child(i))
                }
            }
            node.free()
        }
        let mut meta_lock = self.bm.lock_exclusive(self.meta);
        free_recursive(self.bm, meta_lock.b().0.cast::<MetadataPage>().root().load());
        meta_lock.free();
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct MetadataPage {
    node_head: CommonNodeHead,
    _pad1: [u8; (8 - size_of::<CommonNodeHead>() % 8) % 8],
    root: PageId,
    _pad2: PodPad<{ PAGE_SIZE - size_of::<CommonNodeHead>() - 8 - (8 - size_of::<CommonNodeHead>() % 8) % 8 }>,
}

impl<'bm, BM: BufferManager<'bm>> NodeStatic<'bm, BM> for MetadataPage {
    const TAG: u8 = node_tag::METADATA_MARKER;
    const IS_INNER: bool = true;

    fn iter_children(&self) -> impl Iterator<Item = (&[u8], PageId)> {
        std::iter::once((&[], self.root))
    }
}

impl<'bm, BM: BufferManager<'bm>> NodeDynamic<'bm, BM> for MetadataPage {
    fn split<'g>(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        unimplemented!()
    }

    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        todo!()
    }

    fn merge(&mut self, right: &mut Page) {
        unimplemented!()
    }

    fn validate(&self) {}

    fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()> {
        unimplemented!()
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        todo!()
    }

    fn is_inner(&self) -> bool {
        todo!()
    }
}

impl_to_from_page!(MetadataPage);

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}

impl<'g, 'bm, BM: BufferManager<'bm, Page = PageTail>> ParentInserter<'bm, BM>
    for (W<Guarded<'g, Exclusive, BasicNode<KindInner>>>, BM)
{
    fn insert_upper_sibling(self, separator: impl SourceSlice) -> Result<Guard<'bm, BM, Exclusive, PageTail>, ()> {
        let (mut guard, bm) = self;
        let (new_page, new_guard) = bm.lock_new();
        separator.to_stack_buffer::<MAX_KEY_SIZE, _>(|sep| {
            if let Ok(()) = guard.insert_inner(sep, new_page) {
                Ok(new_guard)
            } else {
                new_guard.free();
                Err(())
            }
        })
    }
}
