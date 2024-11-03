use crate::seqlock::SeqLock;
use crate::{
    BufferManageGuardUpgrade, BufferManager, BufferManagerGuard, OPtr, OlcErrorHandler, OlcVersion, OptimisticError,
    OptimisticGuard, PageId,
};
use std::cell::UnsafeCell;

pub struct SimpleBm {}

trait CommonSeqLockBM<'bm>: Copy + Sync + Send + 'bm {
    type Page;
    type OlcEH: OlcErrorHandler;
    fn pid_from_address(self, address: usize) -> PageId;
    fn alloc(self) -> PageId;
    fn dealloc(self, pid: PageId);
    fn page(self, pid: PageId) -> &'bm UnsafeCell<Self::Page>;
    fn lock(self, pid: PageId) -> &'bm SeqLock;
}

#[derive(Clone)]
pub struct SimpleGuardO<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: OPtr<'bm, BM::Page, BM::OlcEH>,
    version: u64,
}
pub struct SimpleGuardS<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: &'bm BM::Page,
}
pub struct SimpleGuardX<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: &'bm mut BM::Page,
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManager<'bm> for BM {
    type Page = <Self as CommonSeqLockBM<'bm>>::Page;
    type OlcEH = <Self as CommonSeqLockBM<'bm>>::OlcEH;
    type GuardO = SimpleGuardO<'bm, Self>;
    type GuardS = SimpleGuardS<'bm, Self>;
    type GuardX = SimpleGuardX<'bm, Self>;

    fn alloc(self) -> Self::GuardX {
        let pid = self.alloc();
        self.lock(pid).lock_exclusive(())?;
        SimpleGuardX { bm: self, ptr: &mut *self.page(pid).get() }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManageGuardUpgrade<'bm, BM, SimpleGuardS<'bm, BM>> for SimpleGuardO<'bm, BM> {
    fn upgrade(self) -> SimpleGuardS<'bm, BM> {
        let pid = self.bm.pid_from_address(self.ptr.to_raw().addr());
        BM::OlcEH::optmistic_fail_check(self.bm.lock(pid).lock_shared(self.version));
        SimpleGuardS { bm: self.bm, ptr: unsafe { &*self.bm.page(pid).get() } }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManageGuardUpgrade<'bm, BM, SimpleGuardX<'bm, BM>> for SimpleGuardO<'bm, BM> {
    fn upgrade(self) -> SimpleGuardS<'bm, BM> {
        let pid = self.bm.pid_from_address(self.ptr.to_raw().addr());
        BM::OlcEH::optmistic_fail_check(self.bm.lock(pid).lock_exclusive(self.version));
        SimpleGuardS { bm: self.bm, ptr: unsafe { &mut *self.bm.page(pid).get() } }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> OptimisticGuard<'bm, BM> for SimpleGuardO<'bm, BM> {
    fn release_unchecked(self) {
        std::mem::forget(self);
    }

    fn check(&self) -> OlcVersion {
        BM::OlcEH::optmistic_fail_check(
            self.bm.lock(self.bm.pid_from_address(self.ptr.to_raw().addr())).try_unlock_optimistic(self.version),
        );
        OlcVersion { x: self.version }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Drop for SimpleGuardO<'bm, BM> {
    fn drop(&mut self) {
        self.check();
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManagerGuard<'bm, BM> for SimpleGuardO<'bm, BM> {
    fn acquire_wait(bm: BM, page_id: PageId) -> Self {
        todo!()
    }

    fn acquire_wait_version(bm: BM, page_id: PageId, v: OlcVersion) -> Option<Self> {
        todo!()
    }

    fn release(self) -> OlcVersion {
        todo!()
    }

    fn page_id(&self) -> PageId {
        self.bm.pid_from_address(self.ptr.to_raw() as usize)
    }

    fn o_ptr(&self) -> OPtr<'bm, BM::Page, BM::OlcEH> {
        todo!()
    }
}
