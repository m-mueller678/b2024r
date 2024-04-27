use std::mem::size_of;
use crate::node::{CommonNodeHead, PAGE_HEAD_SIZE, PAGE_SIZE};
use bytemuck::{Pod, Zeroable};
use seqlock::{Guard, Guarded, SeqLock, SeqLockMode, SeqLockWrappable, SeqlockAccessors};
use std::ptr;
use std::sync::Mutex;

#[derive(Copy, Clone,Zeroable,Pod,SeqlockAccessors)]
#[seq_lock_wrapper(crate::W)]
#[repr(transparent)]
pub struct PageId(#[seq_lock_skip_accessor] u64);

impl PageId {
    pub fn alloc() -> Self {
        ALLOCATOR.alloc()
    }

    pub fn free(self) {
        ALLOCATOR.free(self);
    }

    // TODO this is currently unsafe, but will be replaced by a more robust and safe implementation.
    pub fn to_page(self)->&'static Page{
        unsafe { &*ptr::with_exposed_provenance(self.0 as usize) }
    }

    pub fn from_page(p:&'static Page)->Self{
        PageId((p as *const Page).expose_provenance() as u64)
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

trait PageAllocator {
    fn alloc(&self) -> PageId;
    fn free(&self, p: PageId);
}

static ALLOCATOR: DefaultPageAllocator = DefaultPageAllocator { freed: Mutex::new(Vec::new()) };
struct DefaultPageAllocator {
    freed: Mutex<Vec<PageId>>,
}

impl PageAllocator for DefaultPageAllocator {
    fn alloc(&self) -> PageId {
        if let Some(pid) = self.freed.lock().unwrap().pop() {
            return pid;
        }
        PageId::from_page(Box::leak(Box::new(Page { lock: SeqLock::new(PageTail::zeroed()) })))
    }

    fn free(&self, p: PageId) {
        self.freed.lock().unwrap().push(p)
    }
}

pub const PAGE_TAIL_SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;

#[derive(Zeroable, Pod, Clone, Copy, SeqlockAccessors)]
#[seq_lock_wrapper(crate::W)]
#[repr(C,align(8))]
pub struct PageTail {
    pub common:CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; { PAGE_TAIL_SIZE - size_of::<CommonNodeHead>() }],
}

#[repr(align(4096))]
pub struct Page {
    lock: SeqLock<PageTail>,
}
