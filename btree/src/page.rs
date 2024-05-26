use crate::node::{CommonNodeHead, NODE_TAIL_SIZE, PAGE_HEAD_SIZE, PAGE_SIZE};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{Guard, Guarded, SeqLock, SeqLockMode, SeqlockAccessors};
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, forget, size_of};
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Mutex, OnceLock};

#[derive(Copy, Clone, Zeroable, Pod, SeqlockAccessors, Eq, PartialEq)]
#[seq_lock_wrapper(crate::W)]
#[repr(transparent)]
pub struct PageId(#[seq_lock_skip_accessor] u64);

impl Debug for PageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        (std::ptr::without_provenance::<u8>(self.0 as usize)).fmt(f)
    }
}

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
        PageId(ALLOCATOR.alloc())
    }

    pub fn alloc_uncommitted() -> UncommittedPageId {
        UncommittedPageId(Self::alloc())
    }

    pub fn free(self) {
        ALLOCATOR.free(self.0);
    }

    pub fn to_page(self) -> &'static Page {
        ALLOCATOR.to_page(self.0)
    }

    pub fn from_page(p: &'static Page) -> Self {
        PageId(ALLOCATOR.to_pid(p))
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
    fn alloc(&self) -> u64;
    fn free(&self, p: u64);
    fn to_page(&self, pid: u64) -> &Page;
    fn to_pid(&self, page: &Page) -> u64;
}

static ALLOCATOR: DefaultPageAllocator = DefaultPageAllocator {
    any_freed: AtomicBool::new(false),
    freed: Mutex::new(Vec::new()),

    next_index: AtomicU64::new(0),
    pages: OnceLock::new(),
};
struct DefaultPageAllocator {
    any_freed: AtomicBool,
    freed: Mutex<Vec<u64>>,
    next_index: AtomicU64,
    pages: OnceLock<Box<[Page]>>,
}

const PAGE_COUNT: usize = 1 << (30 - 12);

impl PageAllocator for DefaultPageAllocator {
    fn alloc(&self) -> u64 {
        if self.any_freed.load(Relaxed) {
            let mut freed = self.freed.lock().unwrap();
            if let Some(r) = freed.pop() {
                if freed.is_empty() {
                    self.any_freed.store(false, Relaxed);
                }
                return r;
            }
        }
        let pages = self.pages.get_or_init(|| {
            (0..PAGE_COUNT)
                .map(|_| Page { lock: SeqLock::new(PageTail::zeroed()) })
                .collect::<Vec<Page>>()
                .into_boxed_slice()
        });
        let next = self.next_index.fetch_add(1, Relaxed);
        if next >= pages.len() as u64 {
            panic!("out of pages")
        }
        (&pages[next as usize] as *const Page).addr() as u64
    }

    fn free(&self, p: u64) {
        let mut freed = self.freed.lock().unwrap();
        freed.push(p);
        self.any_freed.store(true, Relaxed);
    }

    fn to_page(&self, pid: u64) -> &Page {
        let pid = pid as usize;
        let pages = self.pages.get().unwrap();
        let base = pages.as_ptr().addr();
        let end = base + size_of::<Page>() * pages.len();
        assert!(pid >= base);
        assert!(pid < end);
        assert!(pid % size_of::<Page>() == 0);
        let index = (pid - base) / size_of::<Page>();
        &pages[index]
    }

    fn to_pid(&self, p: &Page) -> u64 {
        (p as *const Page).addr() as u64
    }
}

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;

#[derive(Zeroable, Pod, Clone, Copy, SeqlockAccessors)]
#[seq_lock_wrapper(crate::W)]
#[repr(C, align(8))]
#[seq_lock_accessor(pub tag: u8 = common.tag)]
pub struct PageTail {
    pub common: CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; NODE_TAIL_SIZE],
}

#[repr(align(1024), C)]
pub struct Page {
    lock: SeqLock<PageTail>,
}

const _: () = {
    assert!(PAGE_SIZE == size_of::<SeqLock<PageTail>>());
    assert!(PAGE_SIZE == align_of::<Page>());
    assert!(PAGE_SIZE == size_of::<Page>());
};
