use crate::node::{CommonNodeHead, NODE_TAIL_SIZE, PAGE_SIZE};
use crate::W;
use bytemuck::{Pod, Zeroable};
use seqlock::{BufferManager, Guard, Guarded, LockState, SeqLockMode, SeqlockAccessors};
use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, forget, size_of};
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Mutex, OnceLock};

pub type PageId = u64;
pub fn page_id_to_3x16(p: u64) -> [u16; 3] {
    #[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
    compile_error!("only little endian 64-bit is supported");
    debug_assert!(p < (1 << 48));
    let a = bytemuck::cast::<[u8; 8], [u16; 4]>(p.to_ne_bytes());
    [a[0], a[1], a[2]]
}

pub fn page_id_from_3x16(x: [u16; 3]) -> u64 {
    let a = bytemuck::cast::<[u16; 4], [u8; 8]>([x[0], x[1], x[2], 0]);
    u64::from_ne_bytes(a)
}

impl<M: SeqLockMode> W<Guarded<'_, M, PageTail>> {
    pub fn page_id(&self) -> PageId {
        unimplemented!()
        //PageId::from_address_in_page::<PageTail>(self.as_ptr())
    }
}

unsafe impl Sync for DefaultPageAllocator {}

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
    pages: OnceLock<(Box<[Page]>, Box<[LockState]>)>,
}

struct StaticBufferManager;

impl DefaultPageAllocator {
    fn to_pid(&self, address: usize) -> u64 {
        let p = self.pages.get().unwrap();
        let first = p.0.as_ptr().addr();
        let id = (address - first) / size_of::<Page>();
        debug_assert!(id < p.0.len());
        id as u64
    }

    fn pages(&self) -> &(Box<[Page]>, Box<[LockState]>) {
        self.pages.get_or_init(|| {
            let pages = (0..PAGE_COUNT)
                .map(|_| Page { page: UnsafeCell::new(Zeroable::zeroed()) })
                .collect::<Vec<Page>>()
                .into_boxed_slice();
            let locks = (0..PAGE_COUNT).map(|_| LockState::default()).collect::<Vec<LockState>>().into_boxed_slice();
            (pages, locks)
        })
    }
}
unsafe impl<'bm> BufferManager<'bm> for &'bm DefaultPageAllocator {
    type Page = PageTail;

    fn alloc(&self) -> (u64, &'bm UnsafeCell<Self::Page>) {
        let pages = self.pages();
        let next = 'find_page: {
            if self.any_freed.load(Relaxed) {
                let mut freed = self.freed.lock().unwrap();
                if let Some(r) = freed.pop() {
                    if freed.is_empty() {
                        self.any_freed.store(false, Relaxed);
                    }
                    break 'find_page r;
                }
            }
            let next = self.next_index.fetch_add(1, Relaxed);
            if next >= pages.0.len() as u64 {
                panic!("out of pages")
            }
            next
        };
        pages.1[next as usize].acquire_exclusive();
        (next, &pages.0[next as usize].page)
    }

    fn free(&self, page_address: usize) {
        let mut freed = self.freed.lock().unwrap();
        freed.push(self.to_pid(page_address));
        self.any_freed.store(true, Relaxed);
    }

    fn release_exclusive(self, page_address: usize) -> u64 {
        self.pages().1[self.to_pid(page_address) as usize].release_exclusive()
    }

    fn acquire_exclusive(self, page_id: u64) -> &'bm UnsafeCell<Self::Page> {
        let p = self.pages();
        p.1[page_id as usize].acquire_exclusive();
        &p.0[page_id as usize].page
    }

    fn acquire_optimistic(self, page_id: u64) -> (&'bm UnsafeCell<Self::Page>, u64) {
        let p = self.pages();
        let v = p.1[page_id as usize].acquire_optimistic();
        (&p.0[page_id as usize].page, v)
    }

    fn release_optimistic(self, page_address: usize, version: u64) {
        self.pages().1[self.to_pid(page_address) as usize].release_optimistic(version)
    }

    fn upgrade_lock(self, page_address: usize, version: u64) {
        self.pages().1[self.to_pid(page_address) as usize].upgrade_lock(version)
    }

    fn page_address_from_contained_address(self, address: usize) -> usize {
        address
    }
}

const PAGE_COUNT: usize = 1 << (30 - PAGE_SIZE.trailing_zeros());

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE;

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
    page: UnsafeCell<PageTail>,
}

const _: () = {
    assert!(PAGE_SIZE == align_of::<Page>());
    assert!(PAGE_SIZE == size_of::<Page>());
};
