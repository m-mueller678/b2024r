use crate::page_id::PageId;

struct Tree {
    meta: PageId,
}

impl Tree {
    pub fn new() {
        let meta = PageId::alloc();
        let root = PageId::alloc();
    }
}
