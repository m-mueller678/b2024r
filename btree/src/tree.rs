use crate::basic_node::{BasicNode, BasicNodeInner, BasicNodeLeaf};
use crate::key_source::SourceSlice;
use crate::node::{node_tag, Node};
use crate::page::{PageId, PageTail, PAGE_TAIL_SIZE};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Guard, Guarded, Optimistic, SeqlockAccessors};

pub struct Tree {
    meta: PageId,
}

impl Tree {
    pub fn new() -> Self {
        let meta = PageId::alloc();
        let root = PageId::alloc();
        meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root_mut().store(root);
        root.lock::<Exclusive>().b().0.cast::<BasicNode<BasicNodeLeaf>>().init(&[][..], &[][..], [0u8; 3]);
        Tree { meta }
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        seqlock::unwind::repeat(|| || self.try_insert(k, val))
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [Guard<'static, Optimistic, PageTail>; 2] {
        let mut parent = self.meta.lock::<Optimistic>();
        let node_pid = parent.s().cast::<MetadataPage>().root().load();
        let mut node = node_pid.lock::<Optimistic>();
        parent.check();
        while node.s().common().tag().load() == node_tag::BASIC_INNER && Some(node_pid) != stop_at {
            let child = node.cast::<BasicNode<BasicNodeInner>>().lookup_inner(k, true).lock();
            parent.release_unchecked();
            parent = node;
            node = child;
        }
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if PageId::from_address_in_page::<PageTail>(parent.as_ptr()) == split_target {
                let mut node = node.upgrade();
                let mut parent = parent.upgrade();
                debug_assert!(node.common().tag().load() == node_tag::BASIC_INNER);
                if Self::split_locked_node(
                    k,
                    &mut node.b().0.cast::<BasicNode<BasicNodeLeaf>>(),
                    parent.b().0.cast::<BasicNode<BasicNodeInner>>(),
                )
                .is_ok()
                {
                    None
                } else {
                    Some(PageId::from_address_in_page::<PageTail>(parent.as_ptr()))
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

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        let mut leaf = node.b().0.cast::<BasicNode<BasicNodeLeaf>>();
        match leaf.insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                return x;
            }
            Err(()) => {
                let mut parent = parent.upgrade();
                if Self::split_locked_node(k, &mut leaf, parent.b().0.cast::<BasicNode<BasicNodeInner>>()).is_err() {
                    let parent_id = PageId::from_address_in_page::<PageTail>(parent.as_ptr());
                    drop(parent);
                    drop(node);
                    return self.split_and_insert(parent_id, k, val);
                }
                drop(parent);
                node.b().0.cast::<BasicNode<BasicNodeLeaf>>().insert_leaf(k, val).unwrap()
            }
        }
    }

    pub fn lookup_to_vec(&self, k: &[u8]) -> Option<Vec<u8>> {
        seqlock::unwind::repeat(|| || self.try_lookup(k).map(|v| v.load_slice_to_vec()))
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<Guard<'static, Optimistic, [u8]>> {
        let [_parent, node] = self.descend(k, None);
        let key: Guarded<'static, _, _> = node.cast::<BasicNode<BasicNodeLeaf>>().lookup_leaf(k)?;
        Some(node.map(|_| key))
    }

    fn split_locked_node<N: Node>(
        k: &[u8],
        leaf: &mut W<Guarded<Exclusive, N>>,
        mut parent: W<Guarded<Exclusive, BasicNode<BasicNodeInner>>>,
    ) -> Result<(), ()> {
        N::split(leaf, |prefix_len, truncated| {
            let new_node = PageId::alloc();
            k[..prefix_len].join(truncated).to_stack_buffer::<{ crate::MAX_KEY_SIZE }, _>(|k| {
                parent.b().0.cast::<BasicNode<BasicNodeInner>>().insert_inner(k, new_node)
            })?;
            Ok(new_node.lock::<Exclusive>())
        })
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
