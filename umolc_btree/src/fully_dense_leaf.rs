use crate::basic_node::{BasicLeaf, BasicNode};
use crate::fully_dense_leaf::insert_resolver::{resolve, Resolution};
use crate::heap_node::HeapNode;
use crate::key_source::{common_prefix, HeadSourceSlice, SourceSlice, SourceSlicePair, ZeroKey};
use crate::node::{
    insert_upper_sibling, node_tag, CommonNodeHead, DebugNode, NodeDynamic, NodeDynamicAuto, NodeStatic, ToFromPageExt,
    PAGE_SIZE,
};
use crate::{define_node, Page, MAX_KEY_SIZE};
use arrayvec::ArrayVec;
use bytemuck::Zeroable;
use std::cell::Cell;
use std::fmt::{Debug, Formatter};
use std::mem::{offset_of, MaybeUninit};
use std::usize;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};

define_node! {
    pub struct FullyDenseLeaf {
        pub common: CommonNodeHead,
        key_len:u16,
        capacity:u16, // if reference is close to u32::MAX or upper fence, capacity will not be lowered
        val_len:u16,
        reference: u32,
        split_mode:u8,
        _data: [u8; PAGE_SIZE-size_of::<CommonNodeHead>()-11],
    }
}

const SPLIT_MODE_HIGH: u8 = 0;
const SPLIT_MODE_HALF: u8 = 1;

impl FullyDenseLeaf {
    // may optimistic fail if key outside fence range
    // otherwise returns Err(()) if length mismatch or nnp mismatch
    // otherwise returns offset from reference, which may be out of bounds
    fn key_to_index<O: OlcErrorHandler>(this: OPtr<Self, O>, k: &[u8]) -> Result<usize, ()> {
        let prefix_len = o_project!(this.common.prefix_len).r() as usize;
        let lower_fence_start = PAGE_SIZE - o_project!(this.common.lower_fence_len).r() as usize;
        let key_len = o_project!(this.key_len).r() as usize;
        if key_len != k.len() {
            return Err(());
        }
        let numeric_start = key_len.saturating_sub(4);
        if numeric_start > prefix_len {
            let nnp = this.as_slice().i(lower_fence_start + prefix_len..lower_fence_start + numeric_start);
            if !nnp.mem_cmp(&k[prefix_len..numeric_start]).is_eq() {
                return Err(());
            }
        }
        let numeric_part = Self::extract_numeric_part(&k);
        let reference = o_project!(this.reference).r();
        if numeric_part < reference {
            O::optimistic_fail();
        } else {
            Ok((numeric_part - reference) as usize)
        }
    }

    fn extract_numeric_part(k: &[u8]) -> u32 {
        let numeric_start = k.len().saturating_sub(4);
        let numeric_part = if k.len() < 4 {
            let mut acc = 0;
            for b in &k[numeric_start..] {
                acc *= 256;
                acc += *b as u32;
            }
            acc
        } else {
            let numeric_part: &[u8; 4] = k[numeric_start..].try_into().unwrap();
            u32::from_be_bytes(*numeric_part)
        };
        numeric_part
    }

    fn bitmap_u64_count(capacity: usize) -> usize {
        capacity.div_ceil(64)
    }

    fn first_val_start(capacity: usize) -> usize {
        // This used to use an external capacity, which I don't know where it should be taken from. TODO: look at this again, otherwise use internal value.
        offset_of!(Self, _data) + Self::bitmap_u64_count(capacity) * 8
    }

    fn key_from_numeric_part(&self, np: u32) -> SourceSlicePair<u8, &[u8], HeadSourceSlice> {
        let np_len = (self.key_len as usize).min(4);
        let nnp_len = self.key_len as usize - np_len;
        let numeric_part = HeadSourceSlice::from_head_len(np, 4);
        let last_key = self.lower_fence()[..nnp_len].join(numeric_part.slice(4 - np_len..));
        last_key
    }

    fn get_bit<O: OlcErrorHandler>(this: OPtr<Self, O>, i: usize) -> bool {
        let mask = 1 << (i % 8);
        o_project!(this._data).unsize().i(i / 8).r() & mask != 0
    }

    fn set_bit<const SET: bool>(&mut self, i: usize) -> bool {
        debug_assert!(i < self.common.count as usize);
        let mask = 1 << (i % 8);
        let ret = self._data[i / 8] & mask != 0;
        if SET {
            self._data[i / 8] |= mask;
        } else {
            self._data[i / 8] &= !mask;
        }
        ret
    }

    /// returns Err(()) if there are no keys that could be inserted with given lower fence and key_len
    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, key_len: usize, val_len: usize) -> Result<(), ()> {
        self.as_page_mut().common_init(node_tag::FULLY_DENSE_LEAF, lf, uf);
        let space = PAGE_SIZE
            - (self.common.lower_fence_len as usize)
            - (self.common.upper_fence_len as usize).max(4)
            - offset_of!(Self, _data);
        let mut capacity = space * 8 / (val_len * 8 + 1);
        let is_ok = |capacity: usize| capacity.next_multiple_of(64) / 8 + capacity * val_len <= space;
        while !is_ok(capacity) {
            capacity -= 1;
        }
        debug_assert!(!is_ok(capacity + 1));
        self.capacity = capacity as u16;
        for i in 0..Self::bitmap_u64_count(capacity) {
            self.store_unaligned_u64(offset_of!(Self, _data) + i * 8, 0);
        }
        self.val_len = val_len as u16;
        self.key_len = key_len as u16;
        self.split_mode = 0;
        self.reference = if lf.len() < key_len {
            lf.join(ZeroKey::new(key_len - lf.len()))
                .to_ref_buffer::<MAX_KEY_SIZE, _>(|k| Self::extract_numeric_part(k))
        } else if lf.len() == key_len {
            lf.to_ref_buffer::<MAX_KEY_SIZE, _>(|k| Self::extract_numeric_part(k))
        } else {
            let l = lf.slice(..key_len).to_ref_buffer::<MAX_KEY_SIZE, _>(|lf| Self::extract_numeric_part(&lf));
            if l == u32::MAX {
                return Err(());
            } else {
                l + 1
            }
        };
        Ok(())
    }

    fn iter_key_indices<'a>(
        capacity: usize,
        bit_mask_loader: impl FnMut(usize) -> u64 + 'a,
    ) -> impl Iterator<Item = usize> + 'a {
        struct Iter<F: FnMut(usize) -> u64> {
            bit_mask: F,
            index: usize,
            limit: usize,
            word: u64,
        }

        impl<F: FnMut(usize) -> u64> Iterator for Iter<F> {
            type Item = usize;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    while self.word != 0 {
                        let tz = self.word.trailing_zeros();
                        self.index += tz as usize;
                        self.word >>= tz;
                        return Some(self.index);
                    }
                    self.index = self.index.next_multiple_of(64);
                    if self.index >= self.limit {
                        return None;
                    }
                    self.word = (self.bit_mask)(offset_of!(FullyDenseLeaf, _data) + self.index / 8);
                }
            }
        }

        Iter { bit_mask: bit_mask_loader, index: 0, limit: capacity, word: 0 }
    }

    fn val_mut(&mut self, i: usize) -> &mut [u8] {
        //self.slice_mut(self.first_val_start() + self.val_len as usize * i, self.val_len as usize)
        unimplemented!()
    }

    fn val(&self, i: usize) -> &[u8] {
        //self.slice(self.first_val_start() + self.val_len as usize * i, self.val_len as usize)
        unimplemented!()
    }
/*
    fn val_opt<O: OlcErrorHandler>(this: OPtr<Self, O>, i: usize) -> OPtr<[u8], O> {
        let val_len = o_project!(this.val_len).r() as usize;
        let first_val_start = Self::first_val_start(o_project!(this.capacity).r() as usize);
        this.slice(first_val_start + val_len * i, val_len);
    }
*/
    fn split_at_wrap<'bm, BM: BufferManager<'bm, Page = Page>>(
        &mut self,
        bm: BM,
        parent: &mut dyn NodeDynamic<'bm, BM>,
    ) -> Result<(), ()> {
        todo!()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for FullyDenseLeaf {
    const TAG: u8 = node_tag::FULLY_DENSE_LEAF;
    const IS_INNER: bool = false;
    type TruncatedKey<'a>
    where
        Self: 'a,
    = SourceSlicePair<u8, &'a [u8], HeadSourceSlice>;

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        let mut index = Cell::new(usize::MAX);
        let resolution = resolve(
            || {
                let new_heap_size = BasicLeaf::record_size_after_insert_map(key, val).unwrap();
                let transfer_count = self.common.count as usize; // for simplicity assume that all existing keys will be transferred (none overwritten)
                let old_heap_size = transfer_count
                    * (BasicLeaf::KEY_OFFSET + (self.key_len as usize).saturating_sub(4) + self.val_len as usize);
                let fence_size = self.common.lower_fence_len as usize + self.common.upper_fence_len as usize;
                BasicLeaf::heap_start_min(transfer_count + 1) + old_heap_size + new_heap_size + fence_size <= PAGE_SIZE
            },
            || self.common.count as usize * 4 <= self.capacity as usize,
            val.len() == self.val_len as usize && key.len() == self.key_len as usize,
            || {
                let res = Self::key_to_index::<BM::OlcEH>(unsafe { OPtr::from_ref(self) }, key);
                if let Ok(i) = res {
                    index.set(i);
                }
                res.is_ok()
            },
            || {
                let last_key = self.key_from_numeric_part(self.reference.saturating_add(self.capacity as u32 - 1));
                let first_impossible_key = last_key.join(&[0u8][..]);
                SourceSlice::cmp(key, first_impossible_key).is_lt()
            },
            || index.get() < self.capacity as usize,
        );
        let index = index.get();
        match resolution {
            Resolution::Ok => {
                let was_present = self.set_bit::<true>(index);
                self.common.count += (!was_present) as u16;
                self.val_mut(index).copy_from_slice(val);
                Ok(if was_present { Some(()) } else { None })
            }
            Resolution::Convert => {
                let mut tmp: BasicLeaf = BasicLeaf::zeroed();
                NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), self.upper_fence_combined(), None);
                assert!(self.key_len as usize <= MAX_KEY_SIZE);
                let mut key_buf = ArrayVec::<u8, { MAX_KEY_SIZE }>::new();
                let nnp_len = self.key_len.saturating_sub(4) as usize;
                key_buf.try_extend_from_slice(&self.lower_fence()[..nnp_len]).unwrap();
                let key_slice_start = 4usize.saturating_sub(self.key_len as usize);
                key_buf.try_extend_from_slice(&[0, 0, 0, 0]).unwrap();
                for (sparse_index, dense_index) in
                    Self::iter_key_indices(self.capacity as usize, |x| self.read_unaligned::<u64>(x)).enumerate()
                {
                    let numeric_part = self.reference + index as u32;
                    key_buf[nnp_len..].copy_from_slice(&numeric_part.to_be_bytes());
                    tmp.insert_pre_allocated_slot(sparse_index, &key_buf[key_slice_start..], self.val(dense_index));
                }
                tmp.update_hints(0, self.common.count as usize, 0);
                let ret = NodeStatic::<BM>::insert(&mut tmp, key, val);
                *self.as_page_mut() = tmp.copy_page();
                debug_assert!(ret.is_ok());
                ret
            }
            Resolution::SplitHalf => {
                self.split_mode = SPLIT_MODE_HALF;
                Err(())
            }
            Resolution::SplitHigh => Err(()),
        }
    }

    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: Option<&[u8; 5]>) {
        unimplemented!()
    }

    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)> {
        // needed for type inference
        #[allow(unreachable_code)]
        std::iter::once(unimplemented!())
    }

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        let i = Self::key_to_index(this, key).ok()?;
        if i >= o_project!(this.capacity).r() as usize {
            return None;
        }

        //TODO: fix pointer issues with get_bit
        /*if Self::get_bit(i) {
            Some(Self::val_opt(this, i))
        } else {
            None
        }*/
        None
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, key: &[u8], high_on_equal: bool) -> PageId {
        unimplemented!()
    }

    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let indices = || Self::iter_key_indices(self.capacity as usize, |x| self.read_unaligned::<u64>(x));
        let keys = indices().map(|i| Self::key_from_numeric_part(self, self.reference + i as u32).to_vec()).collect();
        let values = indices().map(|i| self.val(i).to_vec()).collect();
        (keys, values)
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for FullyDenseLeaf {
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        let split_at = match self.split_mode {
            SPLIT_MODE_HALF => {
                Self::iter_key_indices(self.capacity as usize, |x| self.read_unaligned::<u64>(x))
                    .skip(self.common.count as usize / 2)
                    .next()
                    .unwrap() as u32
                    + self.reference
            }
            SPLIT_MODE_HIGH => match self.reference.checked_add(self.capacity as u32) {
                Some(x) => x,
                None => return self.split_at_wrap(bm, parent),
            },
            x => panic!("bad split mode {x}"),
        };

        // TODO: check if this is actually a valid implementation of new MaybeUninit
        let mut sep_key_buffer: [MaybeUninit<u8>; 512] = unsafe { MaybeUninit::uninit().assume_init() };


        let sep_key = &*self.key_from_numeric_part(split_at).write_to_uninit(&mut sep_key_buffer);
        let mut right = insert_upper_sibling(parent, bm, sep_key)?;
        let right = right.cast_mut::<Self>();
        // sep_key has same length as key_len, so is a valid key in right
        right.init(sep_key, self.upper_fence_combined(), self.key_len as usize, self.val_len as usize).unwrap();
        self.as_page_mut().init_upper_fence(sep_key);
        debug_assert!(self.common.upper_fence_len <= 4);
        let transfer_from_index = (split_at - self.reference) as usize;
        debug_assert!(right.reference == self.reference + self.capacity as u32);
        for i in transfer_from_index..self.capacity as usize {
            if self.set_bit::<false>(i) {
                let ri = i - self.capacity as usize;
                right.set_bit::<true>(ri);
                right.val_mut(ri).copy_from_slice(self.val(i));
                self.common.count -= 1;
                right.common.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, right: &mut Page) {
        todo!()
    }

    fn validate(&self) {
        todo!()
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        //TODO: fix pointer issues
        /*
        let i = Self::key_to_index(OPtr::from_mut(self), k).ok()?;
        if i >= self.capacity as usize {
            return None;
        }
        if self.set_bit::<false>(i) {
            Some(())
        } else {
            None
        }*/
    }
}

impl Debug for FullyDenseLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

mod insert_resolver;
