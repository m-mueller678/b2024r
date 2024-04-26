use crate::basic_node::{BasicNode, BasicNodeLeaf};
use crate::page_id::{PageId, PAGE_TAIL_SIZE};
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, SeqlockAccessors};

struct Tree {
    meta: PageId,
}

impl Tree {
    pub fn new() -> Self {
        let meta = PageId::alloc();
        let root = PageId::alloc();
        meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root_mut().store(root.to_u64());
        root.lock::<Exclusive>().b().0.cast::<BasicNode<BasicNodeLeaf>>().init(&[][..], &[][..], [0u8; 3]).unwrap();
        Tree { meta }
    }
}

#[derive(SeqlockAccessors, Pod, Zeroable, Copy, Clone)]
#[repr(C)]
#[seq_lock_wrapper(crate::W)]
struct MetadataPage {
    root: u64,
    #[seq_lock_skip_accessor]
    _pad: [u64; PAGE_TAIL_SIZE / 8 - 1],
}
