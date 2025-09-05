use crate::basic_node::{BasicInner, BasicLeaf};
use crate::hash_leaf::HashLeaf;
use crate::key_source::SourceSlice;
use crate::node::{node_tag, o_ptr_is_inner, o_ptr_lookup_inner, o_ptr_lookup_leaf, page_cast, page_cast_mut, page_id_to_bytes, CommonNodeHead, NodeDynamic, NodeDynamicAuto, NodeStatic, OPtrScanCounterExt, Page, PromoteError, ToFromPageExt, PAGE_SIZE};
use crate::{define_node, MAX_KEY_SIZE, MAX_VAL_SIZE};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::sync::atomic::{AtomicU8, Ordering};
use bstr::BStr;
use umolc::{
    o_project, BufferManageGuardUpgrade, BufferManager, BufferManagerExt, BufferManagerGuard, ExclusiveGuard, OPtr,
    OlcErrorHandler, OptimisticGuard, PageId,
};

pub struct Tree<'bm, BM: BufferManager<'bm, Page = Page>> {
    meta: PageId,
    bm: BM,
    _p: PhantomData<&'bm BM>,
}



impl<'bm, BM: BufferManager<'bm, Page = Page>> Tree<'bm, BM> {
    pub fn new(bm: BM) -> Self {
        let mut meta_guard = bm.alloc();
        let mut root_guard = bm.alloc();
        {
            let meta = page_cast_mut::<_, MetadataPage>(&mut *meta_guard);
            meta.root = root_guard.page_id();
            meta.common.tag = node_tag::METADATA_MARKER;
            meta.common.scan_counter.store(3, Ordering::Relaxed);
        }
        NodeStatic::<BM>::init(root_guard.cast_mut::<BasicLeaf>(), &[][..], &[][..], None);

        Tree { meta: meta_guard.page_id(), bm, _p: PhantomData }
    }

    fn validate_fences(&self) {
        if !cfg!(feature = "validate_tree") {
            return;
        }
        let mut low_buffer = [0u8; MAX_KEY_SIZE];
        let mut high_buffer = [0u8; MAX_KEY_SIZE];
        let meta = self.bm.lock_shared(self.meta);
        let root = self.bm.lock_shared(page_cast::<_, MetadataPage>(&*meta).root);
        drop(meta);
        root.as_dyn_node().validate_inter_node_fences(self.bm, &mut &mut low_buffer, &mut &mut high_buffer, 0, 0)
    }

    pub fn remove(&self, k: &[u8]) -> Option<()> {
        let mut removed = false;
        BM::repeat(|| {
            self.try_remove(k, &mut removed);
        });
        if removed {
            Some(())
        } else {
            None
        }
    }
    pub fn scan<F>(&self, lower_bound: &[u8], mut callback: F)
    where
            for<'a> F: FnMut(&[u8], &'a [u8]) -> bool {
        let mut buffer: [MaybeUninit<u8>; 512] = unsafe { MaybeUninit::uninit().assume_init() };
;
        let mut key = lower_bound;
        loop {

            let (len, breakof) = BM::repeat(|| self.try_scan(key, &mut buffer, &mut callback));
            if breakof || len == 0 {
                return;
            }
            key = unsafe {
                std::slice::from_raw_parts_mut(
                    buffer.as_mut_ptr() as *mut u8,
                    len
                )
            };
        }

    }

    fn try_scan<F>(&self, key: &[u8], buffer: &mut [MaybeUninit<u8>; 512], mut callback: F) -> (usize, bool)
    where
            for<'a> F: FnMut(&[u8], &'a [u8]) -> bool {

        let [parent, node] = self.descend(key, None);

        parent.release_unchecked();


        let mut node = self.increase_scan_counter(node);
        let o = node.o_ptr();
        let tag = o_project!(o.common.tag).r();

        if tag == node_tag::HASH_LEAF {
            let mut node: BM::GuardX = node.upgrade();
            node.cast_mut::<HashLeaf>().sort();

            // in theory we could demote the lock here, but for hash_leafs, being sorted is relevant that we just keep the lock on
            let ret = node.as_dyn_node::<BM>().scan_with_callback(buffer, key, &mut callback);

            if ret {
                return (0, true);
            }

            let upper = node.upper_fence_combined();

            let upper_len = upper.len();

            upper.to_vec()
                .write_to_uninit(&mut buffer[..upper_len]);


            if upper_len == 0 {
                return (0, true);
            }

            (upper_len, false)
        }

        else {

            let mut node: BM::GuardX = node.upgrade();

            let ret = node.as_dyn_node::<BM>().scan_with_callback(buffer, key, &mut callback);

            if ret {
                return (0, true);
            }

            let upper = node.upper_fence_combined();

            let upper_len = upper.len();

            upper.to_vec()
                .write_to_uninit(&mut buffer[..upper_len]);


            if upper_len == 0 {
                return (0, true);
            }

            (upper_len, false)
        }


    }

    pub fn scan_node_types<F>(&self, lower_bound: &[u8], mut callback: F)
    where F: FnMut(u8, u8, u16) -> bool {
        let mut buffer: [MaybeUninit<u8>; 512] = unsafe { MaybeUninit::uninit().assume_init() };

        let mut key = lower_bound;


        loop {
            let [parent, node] = self.descend(key, None);

            let node: BM::GuardS = node.upgrade();
            parent.release_unchecked();



            let ret = callback(node.as_dyn_node::<BM>().get_node_tag(), node.as_dyn_node::<BM>().get_scan_counter().load(Ordering::Relaxed), node.as_dyn_node::<BM>().get_count());

            if ret {
                return;
            }


            let upper = node.upper_fence_combined();

            let upper_len = upper.len();
            key = {
                upper.to_vec()
                    .write_to_uninit(&mut buffer[..upper_len])
            };
            if key.is_empty() {
                return;
            }

        }

    }

    fn try_remove(&self, k: &[u8], removed: &mut bool) {
        let [parent, node] = self.descend(k, None);

        let node = self.decrease_scan_counter(node);

        let mut node: BM::GuardX = node.upgrade();


        if node.as_dyn_node_mut::<BM>().leaf_remove(k).is_some() {
            *removed = true;
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = BM::repeat(|| self.try_insert(k, val));
        self.validate_fences();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [BM::GuardO; 2] {
        let mut parent = self.bm.lock_optimistic(self.meta);
        let mut node_pid = self.meta;
        let mut node = self.bm.lock_optimistic(self.meta);
        while o_ptr_is_inner::<BM>(node.o_ptr()) && Some(node_pid) != stop_at {
            node_pid = o_ptr_lookup_inner::<BM>(node.o_ptr(), k, true);
            node.check(); // check here so we do not attempt to lock wrong page id
            parent.release_unchecked();
            parent = node;
            node = self.bm.lock_optimistic(node_pid);
        }
        // ensure we return the correct node
        // we could push this responsibility on the caller, but this has proven error-prone
        parent.check();
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if node.page_id() == split_target {
                let mut node: BM::GuardX = node.upgrade();
                let mut parent: BM::GuardX = parent.upgrade();
                self.ensure_parent_not_meta(&mut parent);
                if self.split_locked_node(&mut node, &mut parent, k).is_ok() {
                    None
                } else {
                    Some(parent.page_id())
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

    fn ensure_parent_not_meta(&self, parent: &mut BM::GuardX) {
        if parent.common.tag == node_tag::METADATA_MARKER {
            let meta = parent.cast_mut::<MetadataPage>();
            let mut new_root = self.bm.alloc();
            NodeStatic::<BM>::init(
                new_root.cast_mut::<BasicInner>(),
                &[][..],
                &[][..],
                Some(&page_id_to_bytes(meta.root)),
            );
            meta.root = new_root.page_id();
            *parent = new_root
        }
    }

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let node = self.decrease_scan_counter(node);
        let mut node: BM::GuardX = node.upgrade();


        match node.as_dyn_node_mut::<BM>().insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                x
            }
            Err(_) => {
                node.reset_written();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_meta(&mut parent);

                #[cfg(not(feature = "disallow_promotions"))]
                let can_promote = node.as_dyn_node::<BM>().can_promote(node_tag::FULLY_DENSE_LEAF).is_ok();

                #[cfg(feature = "disallow_promotions")]
                let can_promote = false;


                if can_promote {
                    node.as_dyn_node_mut::<BM>().promote(node_tag::FULLY_DENSE_LEAF);
                }

                else if self.split_locked_node(&mut node, &mut parent, k).is_err() {
                    let parent_id = parent.page_id();
                    drop(parent);
                    drop(node);
                    return self.split_and_insert(parent_id, k, val);
                }
                drop(parent);
                drop(node);
                // TODO could descend from parent
                self.try_insert(k, val)
            }
        }
    }

    pub fn lookup_to_vec(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.lookup_inspect(k, |v| v.map(|v| v.load_slice_to_vec()))
    }

    pub fn lookup_to_buffer<'a>(&self, k: &[u8], b: &'a mut [MaybeUninit<u8>; MAX_VAL_SIZE]) -> Option<&'a mut [u8]> {
        let valid_len = self.lookup_inspect(k, |v| v.map(|v| v.load_bytes_uninit(&mut b[..v.len()]).len()));
        valid_len.map(|l| unsafe {
            std::slice::from_raw_parts_mut(b[..l].as_mut_ptr() as *mut u8, l)
        })
    }

    pub fn lookup_inspect<R>(&self, k: &[u8], mut f: impl FnMut(Option<OPtr<[u8], BM::OlcEH>>) -> R) -> R {
        BM::repeat(move || if let Some((_guard, val)) = self.try_lookup(k) { f(Some(val)) } else { f(None) })
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<(BM::GuardO, OPtr<[u8], BM::OlcEH>)> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let node = self.decrease_scan_counter(node);

        let val = o_ptr_lookup_leaf::<BM>(node.o_ptr_bm(), k)?;


        Some((node, val))
    }

    fn split_locked_node(&self, node: &mut Page, parent: &mut Page, key: &[u8]) -> Result<(), ()> {
        //TODO inline
        if node.common.count as usize > 1 {
            node.as_dyn_node_mut().split(self.bm, parent.as_dyn_node_mut(), key)
        } else {
            Ok(())
        }
    }

    pub fn lock_path(&self, key: &[u8]) -> Vec<BM::GuardS> {
        let mut path = Vec::new();
        let mut node = {
            let parent = self.bm.lock_shared(self.meta);
            let node_pid = parent.cast::<MetadataPage>().root;
            path.push(parent);
            self.bm.lock_shared(node_pid)
        };
        while node.as_dyn_node::<BM>().is_inner() {
            let node_pid = o_ptr_lookup_inner::<BM>(node.o_ptr(), key, true);
            path.push(node);
            node = self.bm.lock_shared(node_pid);
        }
        path.push(node);
        path
    }

    fn decrease_scan_counter(&self, mut node: BM::GuardO) -> BM::GuardO{
        if fastrand::u8(..100) < 5 {
            node.o_ptr().decrease_scan_counter();

            let node = self.adaptive_promotion(node);
            return node;
        }
        node
    }


    fn increase_scan_counter(&self, mut node: BM::GuardO) -> BM::GuardO{
        if fastrand::u8(..100) < 15 {
            node.o_ptr().increase_scan_counter();

            let node = self.adaptive_promotion(node);
            return node;
        }
        node
    }




    fn adaptive_promotion (&self, mut node: BM::GuardO) -> BM::GuardO{
        let o: OPtr<'_, Page, BM::OlcEH> = node.o_ptr();

        let tag: u8 = o_project!(o.common.tag).r();

        let scan: u8 = unsafe {
            (&(*(o.to_raw() as *const Page)).common.scan_counter).load(Ordering::Relaxed)
        };


        #[cfg(not(feature = "disallow_promotions"))]
        if (tag == 251 && scan == 0) || (tag==252 && scan >= 3) {
            let mut node: BM::GuardX = node.upgrade();

            let to = if tag == 251 {252} else {251};

            if node.as_dyn_node::<BM>().can_promote(to).is_ok() {
                node.as_dyn_node_mut::<BM>().promote(to);
            }
            else {
                node.as_dyn_node_mut::<BM>().retry_later();
            }

            return self.downgrade_guard(node);
        }

        node
    }


    fn downgrade_guard(&self, x: BM::GuardX) -> BM::GuardO {
        let pid = x.page_id();
        let v   = x.release();                // unlock X, get new version
        BM::GuardO::acquire_wait_version(self.bm, pid, v)
            .unwrap_or_else(|| BM::GuardO::acquire_wait(self.bm, pid))
    }

}

impl<'bm, BM: BufferManager<'bm, Page = Page>> Drop for Tree<'bm, BM> {
    fn drop(&mut self) {
        let mut meta_lock = self.bm.lock_exclusive(self.meta);
        meta_lock.cast_mut::<MetadataPage>().free_children(self.bm);
        meta_lock.dealloc();
    }
}

define_node! {
pub struct MetadataPage {
    pub common: CommonNodeHead,
    _pad1: [u8; (8 - size_of::<CommonNodeHead>() % 8) % 8],
    root: PageId,
    _pad2: [u8;PAGE_SIZE - size_of::<CommonNodeHead>() - 8 - (8 - size_of::<CommonNodeHead>() % 8) % 8 ],
}
}

impl Debug for MetadataPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct(std::any::type_name::<Self>());
        s.field("root", &self.root);
        s.finish()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for MetadataPage {
    const TAG: u8 = node_tag::METADATA_MARKER;
    const IS_INNER: bool = true;
    type TruncatedKey<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn insert(&mut self, _key: &[u8], _val: &[u8]) -> Result<Option<()>, ()> {
        unimplemented!()
    }

    fn init(&mut self, _lf: impl SourceSlice, _uf: impl SourceSlice, _lower: Option<&[u8; 5]>) {
        unimplemented!()
    }

    fn iter_children(&self) -> impl Iterator<Item = (&[u8], PageId)> {
        std::iter::once((&[][..], self.root))
    }

    fn lookup_leaf<'a>(_this: OPtr<'a, Self, BM::OlcEH>, _key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        BM::OlcEH::optimistic_fail()
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, _key: &[u8], _high_on_equal: bool) -> PageId {
        PageId { x: o_project!(this.root.x).r() }
    }


    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        (Vec::new(), vec![page_id_to_bytes(self.root).to_vec()])
    }

    fn set_scan_counter(&mut self, _counter: &AtomicU8) {
        unimplemented!()
    }

    fn has_good_heads(&self) -> (bool, bool) {
        unimplemented!()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for MetadataPage {
    fn split(&mut self, _bm: BM, _parent: &mut dyn NodeDynamic<'bm, BM>, _key: &[u8]) -> Result<(), ()> {
        unimplemented!()
    }

    fn merge(&mut self, _right: &mut Page) {
        unimplemented!()
    }

    fn validate(&self) {}

    fn leaf_remove(&mut self, _k: &[u8]) -> Option<()> {
        unimplemented!()
    }

    fn scan_with_callback(&self, _buffer: &mut [MaybeUninit<u8>; 512], _start : &[u8], _callback: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> bool {
        unimplemented!()
    }

    fn get_node_tag(&self) -> u8 {
        unimplemented!()
    }

    fn get_scan_counter(&self) -> &AtomicU8 {
        unimplemented!()
    }

    fn get_count(&self) -> u16 {
        unimplemented!()
    }

    fn can_promote(&self, _to: u8) -> Result<(), PromoteError> {
        unimplemented!()
    }

    fn promote(&mut self, _to: u8) {
        unimplemented!()
    }


    fn retry_later(&mut self) {
        todo!()
    }
}
