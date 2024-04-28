use crate::node::{CommonNodeHead, PAGE_HEAD_SIZE, PAGE_SIZE};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{Guard, Guarded, SeqLock, SeqLockMode, SeqlockAccessors};
use std::collections::BTreeMap;
use std::mem::{forget, size_of};
use std::ops::Deref;
use std::sync::Mutex;

#[derive(Copy, Clone, Zeroable, Pod, SeqlockAccessors, Eq, PartialEq)]
#[seq_lock_wrapper(crate::W)]
#[repr(transparent)]
pub struct PageId(#[seq_lock_skip_accessor] u64);

pub struct UncommittedPageId(PageId);

impl Drop for UncommittedPageId {
    fn drop(&mut self) {
        self.0.free()
    }
}

impl UncommittedPageId {
    pub fn commit(self) -> PageId {
        let x = self.0;
        forget(self);
        x
    }
}

impl Deref for UncommittedPageId {
    type Target = PageId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PageId {
    pub fn alloc() -> Self {
        ALLOCATOR.alloc()
    }

    pub fn alloc_uncommitted() -> UncommittedPageId {
        UncommittedPageId(Self::alloc())
    }

    pub fn free(self) {
        ALLOCATOR.free(self);
    }

    pub fn to_page(self) -> &'static Page {
        // TODO make this efficient
        ALL_PAGES.lock().unwrap()[&self.0]
    }

    pub fn from_page(p: &'static Page) -> Self {
        PageId((p as *const Page).expose_provenance() as u64)
    }

    fn from_address_in_page<T>(p: *mut T) -> Self {
        PageId((p.addr() as u64) & (u64::MAX << 12))
    }

    pub fn to_3x16(self) -> [u16; 3] {
        #[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
        compile_error!("only little endian 64-bit is supported");
        let shifted = self.0 >> 12;
        debug_assert!(shifted < (1 << 48));
        let a = bytemuck::cast::<[u8; 8], [u16; 4]>(shifted.to_ne_bytes());
        [a[0], a[1], a[2]]
    }

    pub fn from_3x16(x: [u16; 3]) -> Self {
        let a = bytemuck::cast::<[u16; 4], [u8; 8]>([x[0], x[1], x[2], 0]);
        Self(u64::from_ne_bytes(a) << 12)
    }

    pub fn lock<M: SeqLockMode>(self) -> Guard<'static, M, PageTail> {
        self.to_page().lock.lock()
    }
}

impl<M: SeqLockMode> W<Guarded<'_, M, PageTail>> {
    pub fn page_id(&self) -> PageId {
        PageId::from_address_in_page::<PageTail>(self.as_ptr())
    }
}

trait PageAllocator {
    fn alloc(&self) -> PageId;
    fn free(&self, p: PageId);
}

static ALL_PAGES: Mutex<BTreeMap<u64, &'static Page>> = Mutex::new(BTreeMap::new());
static ALLOCATOR: DefaultPageAllocator = DefaultPageAllocator { freed: Mutex::new(Vec::new()) };
struct DefaultPageAllocator {
    freed: Mutex<Vec<PageId>>,
}

impl PageAllocator for DefaultPageAllocator {
    fn alloc(&self) -> PageId {
        if let Some(pid) = self.freed.lock().unwrap().pop() {
            return pid;
        }
        let page = Box::leak(Box::new(Page { lock: SeqLock::new(PageTail::zeroed()) }));
        let page_id = PageId::from_page(page);
        ALL_PAGES.lock().unwrap().insert(page_id.0, page);
        page_id
    }

    fn free(&self, p: PageId) {
        self.freed.lock().unwrap().push(p)
    }
}

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;

#[derive(Zeroable, Pod, Clone, Copy, SeqlockAccessors)]
#[seq_lock_wrapper(crate::W)]
#[repr(C, align(8))]
pub struct PageTail {
    pub common: CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; PAGE_TAIL_SIZE - size_of::<CommonNodeHead>()],
}

#[repr(align(4096))]
pub struct Page {
    lock: SeqLock<PageTail>,
}
