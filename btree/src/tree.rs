use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::key_source::SourceSlice;
use crate::node::{node_tag, KindInner, Node};
use crate::page::{PageId, PageTail, PAGE_TAIL_SIZE};
use crate::{MAX_KEY_SIZE, W};
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Guard, Guarded, Optimistic, SeqlockAccessors, Shared};
use std::cell::Cell;
use std::panic::RefUnwindSafe;

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
        meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root_mut().store(root);
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
        struct RemovedFlag(Cell<bool>);
        impl RefUnwindSafe for RemovedFlag {}
        let removed = RemovedFlag(Cell::new(false));
        seqlock::unwind::repeat(|| {
            || {
                let removed = &removed;
                self.try_remove(k, &removed.0);
            }
        });
        if removed.0.get() {
            Some(())
        } else {
            None
        }
    }

    fn try_remove(&self, k: &[u8], removed: &Cell<bool>) {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        if node.b().node_cast::<BasicLeaf>().remove(k).is_some() {
            removed.set(true);
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = seqlock::unwind::repeat(|| || self.try_insert(k, val));
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
            node_pid = node.node_cast::<BasicInner>().lookup_inner(k, true);
            parent = node;
            node = node_pid.lock();
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
        seqlock::unwind::repeat(|| || self.try_lookup(k).map(|v| v.load_slice_to_vec()))
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
        mut parent: W<Guarded<Exclusive, BasicNode<KindInner>>>,
    ) -> Result<(), ()> {
        N::split(
            leaf,
            |prefix_len, truncated| {
                let new_node = PageId::alloc();
                k[..prefix_len]
                    .join(truncated)
                    .to_stack_buffer::<{ crate::MAX_KEY_SIZE }, _>(|k| parent.b().insert_inner(k, new_node))?;
                Ok(new_node.lock::<Exclusive>())
            },
            k,
        )
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
        free_recursive(self.meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root().load());
        self.meta.free()
    }
}

#[derive(SeqlockAccessors, Pod, Zeroable, Copy, Clone)]
#[repr(C)]
#[seq_lock_wrapper(crate::W)]
struct MetadataPage {
    root: PageId,
    #[seq_lock_skip_accessor]
    _pad: [u64; PAGE_TAIL_SIZE / 8 - 1],
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}
