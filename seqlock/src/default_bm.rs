use std::cell::UnsafeCell;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::mem::size_of;
use bytemuck::Zeroable;
use crate::{BufferManager, LockState, SeqLockWrappable};

unsafe impl<P:Send+Sync+Zeroable+SeqLockWrappable> Sync for DefaultBm<P> {}

pub struct DefaultBm<P:Send+Sync+Zeroable+SeqLockWrappable> {
    any_freed: AtomicBool,
    freed: Mutex<Vec<u64>>,
    next_index: AtomicU64,
    pages: OnceLock<(Box<[UnsafeCell<P>]>, Box<[LockState]>)>,
    page_count:usize,
}


impl<P:Send+Sync+Zeroable+SeqLockWrappable> DefaultBm<P> {
    pub const fn new_lazy()->Self{
        DefaultBm {
            any_freed:AtomicBool::new(false),
            freed:Mutex::new(Vec::new()),
            next_index:AtomicU64::new(0),
            page_count:(1 << 30)/ size_of::<P>(),
            pages:OnceLock::new(),
        }
    }

    fn pages(&self)->&(Box<[UnsafeCell<P>]>, Box<[LockState]>){
        self.pages.get_or_init(|| {
            let pages = (0..self.page_count)
                .map(|_| UnsafeCell::new(Zeroable::zeroed()))
                .collect::<Vec<UnsafeCell<P>>>()
                .into_boxed_slice();
            let locks = (0..self.page_count).map(|_| LockState::default()).collect::<Vec<LockState>>().into_boxed_slice();
            (pages, locks)
        })
    }

    fn to_pid(&self, address: usize) -> u64 {
        let p = self.pages();
        let first = p.0.as_ptr().addr();
        let id = (address - first) / size_of::<P>();
        debug_assert!(id < p.0.len());
        id as u64
    }
}

unsafe impl<'bm,P:Send+Sync+Zeroable+SeqLockWrappable> BufferManager<'bm> for &'bm DefaultBm<P> {
    type Page = P;

    fn alloc(self) -> (u64, &'bm UnsafeCell<Self::Page>) {
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
        (next, &pages.0[next as usize])
    }

    fn free(self, page_address: usize) {
        let mut freed = self.freed.lock().unwrap();
        freed.push(self.to_pid(page_address));
        self.any_freed.store(true, Relaxed);
    }

    fn release_exclusive(self, page_address: usize) -> u64 {
        self.pages().1[self.to_pid(page_address) as usize].release_exclusive()
    }

    fn page_id(self,page_address:usize)->u64{
        let page_start:*const UnsafeCell<P> = self.pages().0.as_ptr();
        let id=(page_address - page_start.addr())/size_of::<P>();
        id as u64
    }

    fn acquire_exclusive(self, page_id: u64) -> &'bm UnsafeCell<Self::Page> {
        let p = self.pages();
        p.1[page_id as usize].acquire_exclusive();
        &p.0[page_id as usize]
    }

    fn acquire_optimistic(self, page_id: u64) -> (&'bm UnsafeCell<Self::Page>, u64) {
        let p = self.pages();
        let v = p.1[page_id as usize].acquire_optimistic();
        (&p.0[page_id as usize], v)
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