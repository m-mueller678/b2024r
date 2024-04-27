use std::mem::ManuallyDrop;
use crate::basic_node::{BasicNode, BasicNodeInner, BasicNodeLeaf};
use crate::page_id::{PageId, PAGE_TAIL_SIZE};
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Optimistic, OptimisticLockError, SeqlockAccessors};
use crate::node::node_tag;

struct Tree {
    meta: PageId,
}

impl Tree {
    pub fn new() -> Self {
        let meta = PageId::alloc();
        let root = PageId::alloc();
        meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root_mut().store(root);
        root.lock::<Exclusive>().b().0.cast::<BasicNode<BasicNodeLeaf>>().init(&[][..], &[][..], [0u8; 3]).unwrap();
        Tree { meta }
    }

    fn try_insert(&self,k:&[u8],val:&[u8])->Result<Option<()>,OptimisticLockError>{
        let mut parent = ManuallyDrop::new(self.meta.lock::<Optimistic>());
        let node_pid = parent.s().cast::<MetadataPage>().root().load();
        parent.check()?;
        let mut node = ManuallyDrop::new(node_pid.lock::<Optimistic>());
        parent.check()?;
        while node.s().common().tag().load() == node_tag::BASIC_INNER{
            let child_pid = node.cast::<BasicNode<BasicNodeInner>>().lookup_inner(k,true)?;
            parent = node;
            parent.check();
            node = ManuallyDrop::new(child_pid.lock());
            parent.check();
        }
        let node = node.0.upgrade();
        node.b().cast::<BasicNode<BasicNodeLeaf>>().insert_leaf(k,val);

        parent.check();
        node.check();
        Ok(())
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
