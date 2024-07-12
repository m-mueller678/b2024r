use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::key_source::SourceSlice;
use crate::node::{node_tag, CommonNodeHead, KindInner, Node, ParentInserter};
use crate::page::{PageId, PageTail, PAGE_TAIL_SIZE};
use crate::{MAX_KEY_SIZE, W};
use bytemuck::{Pod, Zeroable};
use itertools::Itertools;
use seqlock::{Exclusive, Guard, Guarded, Optimistic, SeqlockAccessors};
use std::mem::size_of;

pub struct Tree {
    meta: PageId,
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    pub fn new() -> Self {
        let meta = PageId::alloc();
        let root = PageId::alloc();
        {
            let mut meta = meta.lock::<Exclusive>();
            let mut meta = meta.b().0.cast::<MetadataPage>();
            meta.b().root_mut().store(root);
            meta.node_head_mut().tag_mut().store(node_tag::METADATA_MARKER);
        }
        root.lock::<Exclusive>().b().0.cast::<BasicLeaf>().init(&[][..], &[][..], [0u8; 3]);
        Tree { meta }
    }

    fn validate_fences_exclusive(&self) {
        let mut low_buffer = [0u8; MAX_KEY_SIZE];
        let mut high_buffer = [0u8; MAX_KEY_SIZE];
        let meta = self.meta.lock::<Exclusive>();
        let mut root = meta.s().cast::<MetadataPage>().root().load().lock::<Exclusive>();
        drop(meta);
        root.b().0.cast::<BasicInner>().validate_inter_node_fences(&mut &mut low_buffer, &mut &mut high_buffer, 0, 0);
    }

    pub fn remove(&self, k: &[u8]) -> Option<()> {
        let mut removed = false;
        seqlock::unwind::repeat(|| {
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
        let mut node = node.upgrade();
        if node.b().node_cast::<BasicLeaf>().remove(k).is_some() {
            *removed = true;
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = seqlock::unwind::repeat(|| self.try_insert(k, val));
        self.validate_fences_exclusive();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [Guard<'static, Optimistic, PageTail>; 2] {
        let mut parent = self.meta.lock::<Optimistic>();
        let mut node_pid = parent.s().cast::<MetadataPage>().root().load();
        let mut node = node_pid.lock::<Optimistic>();
        parent.check();
        while node.s().common().tag().load() == node_tag::BASIC_INNER && Some(node_pid) != stop_at {
            parent.release_unchecked();
            parent = node;
            node_pid = parent.node_cast::<BasicInner>().lookup_inner(k, true);
            node = node_pid.lock();
            parent.check();
        }
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if node.page_id() == split_target {
                let mut node = node.upgrade();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut node, &mut parent);
                debug_assert!(node.common().tag().load() == node_tag::BASIC_INNER);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicInner>(),
                    parent.b().node_cast::<BasicInner>(),
                )
                .is_ok()
                {
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

    fn ensure_parent_not_root(
        &self,
        node: &mut Guard<'static, Exclusive, PageTail>,
        parent: &mut Guard<'static, Exclusive, PageTail>,
    ) {
        if parent.page_id() == self.meta {
            let mut new_root = PageId::alloc().lock::<Exclusive>();
            new_root.b().0.cast::<BasicInner>().init(&[][..], &[][..], node.page_id().to_3x16());
            parent.b().0.cast::<MetadataPage>().root_mut().store(new_root.page_id());
            *parent = new_root
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
                self.ensure_parent_not_root(&mut node, &mut parent);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicLeaf>(),
                    parent.b().node_cast::<BasicInner>(),
                )
                .is_err()
                {
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
        seqlock::unwind::repeat(|| self.try_lookup(k).map(|v| v.load_slice_to_vec()))
    }

    pub fn lookup_inspect<R>(&self, k: &[u8], mut f: impl FnMut(Option<Guard<'static, Optimistic, [u8]>>) -> R) -> R {
        seqlock::unwind::repeat(move || f(self.try_lookup(k)))
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<Guard<'static, Optimistic, [u8]>> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let key: Guarded<'static, _, _> = node.node_cast::<BasicLeaf>().lookup_leaf(k)?;
        Some(node.map(|_| key))
    }

    fn split_locked_node<N: Node>(
        k: &[u8],
        leaf: &mut W<Guarded<Exclusive, N>>,
        parent: W<Guarded<Exclusive, BasicNode<KindInner>>>,
    ) -> Result<(), ()> {
        N::split(leaf, parent, k)
    }

    fn lock_path(&self, key: &[u8]) -> Vec<Guard<'static, Exclusive, PageTail>> {
        let mut path = Vec::new();
        let mut node = {
            let parent = self.meta.lock::<Exclusive>();
            let node_pid = parent.s().cast::<MetadataPage>().root().load();
            path.push(parent);
            node_pid.lock::<Exclusive>()
        };
        while node.s().common().tag().load() == node_tag::BASIC_INNER {
            let node_pid = node.s().node_cast::<BasicInner>().optimistic().lookup_inner(key, true);
            path.push(node);
            node = node_pid.lock();
        }
        path.push(node);
        path
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        fn free_recursive(p: PageId) {
            let node = p.lock::<Optimistic>();
            if node.tag().load() == BasicInner::TAG {
                let node = node.node_cast::<BasicInner>();
                for i in 0..node.count().load() as usize {
                    free_recursive(node.index_child(i))
                }
            }
            p.free();
        }
        let mut meta_lock = self.meta.lock::<Exclusive>();
        free_recursive(meta_lock.b().0.cast::<MetadataPage>().root().load());
        meta_lock.release();
        self.meta.free()
    }
}

#[derive(SeqlockAccessors, Pod, Zeroable, Copy, Clone)]
#[repr(C)]
#[seq_lock_wrapper(crate::W)]
struct MetadataPage {
    // for debugging
    node_head: CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; PAGE_TAIL_SIZE - 8 - size_of::<CommonNodeHead>()],
    root: PageId,
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}

impl ParentInserter for W<Guarded<'_, Exclusive, BasicNode<KindInner>>> {
    fn insert_upper_sibling(mut self, separator: impl SourceSlice) -> Result<Guard<'static, Exclusive, PageTail>, ()> {
        let new_node = PageId::alloc();
        separator.to_stack_buffer::<MAX_KEY_SIZE, _>(|sep| {
            if let Ok(x) = self.insert_inner(sep, new_node) {
                Ok(new_node.lock::<Exclusive>())
            } else {
                new_node.free();
                Err(())
            }
        })
    }
}
