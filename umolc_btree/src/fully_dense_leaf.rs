use crate::basic_node::{BasicLeaf, BasicNode};
use crate::fully_dense_leaf::insert_resolver::{resolve, Resolution};
use crate::hash_leaf::HashLeaf;
use crate::heap_node::HeapNode;
use crate::key_source::{HeadSourceSlice, SourceSlice, SourceSlicePair, ZeroKey};
use crate::node::PromoteError::Node;
use crate::node::{insert_upper_sibling, node_tag, page_cast_mut, CommonNodeHead, KindLeaf, NodeDynamic, NodeDynamicAuto, NodeKind, NodeStatic, PromoteError, ToFromPageExt, PAGE_SIZE};
use crate::{define_node, Page, MAX_KEY_SIZE};
use arrayvec::ArrayVec;
use bstr::{BStr, BString};
use bytemuck::Zeroable;
use indxvec::Printing;
use itertools::Itertools;
use std::cell::Cell;
use std::fmt::{Debug, Display, Formatter};
use std::mem::{offset_of, MaybeUninit};
use std::sync::atomic::fence;
use std::usize;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};
use crate::fully_dense_leaf::insert_resolver::Resolution::SplitHigh;

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
    pub fn into_page(self) -> Page {
        unsafe { std::mem::transmute(self) }
    }

    pub fn get_capacity_fdl(lf_len: usize, uf_len: usize, val_len: usize) -> usize {
        let header_size = size_of::<CommonNodeHead>() + 2 + 2 + 2 + 4 + 1;
        let space = PAGE_SIZE - header_size - lf_len - uf_len;
        let mut capacity = space * 8 / (val_len * 8 + 1);
        let is_ok = |capacity: usize| capacity.next_multiple_of(64) / 8 + capacity * val_len <= space;
        while !is_ok(capacity) {
            capacity -= 1;
        }
        capacity
    }

    pub fn get_reference(&self) -> u32 {
        self.reference
    }

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

    fn first_val_start(&self, capacity: usize) -> usize {
        offset_of!(Self, _data) + Self::bitmap_u64_count(capacity) * 8
    }

    pub fn force_insert<O: OlcErrorHandler>(&mut self, key: &[u8], val: &[u8]) {
        let index = Self::key_to_index::<O>(unsafe { OPtr::from_ref(self) }, key).expect("Index computation failed");

        let was_present = self.set_bit::<true>(index, false);
        self.common.count += (!was_present) as u16;
        self.val_mut(index).copy_from_slice(val);
    }

    // specific static call for usage in val_opt
    fn first_val_start_static(capacity: usize) -> usize {
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

    fn get_bit_direct(&self, i: usize) -> bool {
        let mask: u8 = 1 << (i % 8);
        let byte_index = i / 8;
        let byte_ptr = unsafe { std::slice::from_raw_parts(self._data.as_ptr() as *const u8, self._data.len() * 2) };
        byte_ptr[byte_index] & mask != 0
    }

    // boolean ignores the debug_assert which needs to be done in split and other operations
    fn set_bit<const SET: bool>(&mut self, i: usize, ignore: bool) -> bool {
        debug_assert!(i < self.capacity as usize || ignore, "{i}");
        let mask = 1 << (i % 8);
        let ret = self._data[i / 8] & mask != 0;
        if SET {
            self._data[i / 8] |= mask;
        } else {
            self._data[i / 8] &= !mask;
        }
        ret
    }

    fn set_upper_fence_tail(&mut self, key: &[u8]) {
        let prefix_len = self.as_page().common.prefix_len as usize;
        let tail = &key[prefix_len..];
        let uf_len = tail.len();
        self.as_page_mut().common.upper_fence_len = uf_len as u16;

        let offset = size_of::<Self>()
            - self.as_page().common.lower_fence_len as usize
            - uf_len;

        self.slice_mut::<u8>(offset, uf_len).copy_from_slice(tail);
    }


    pub fn init_wrapper(
        &mut self,
        lf: impl SourceSlice,
        uf: impl SourceSlice,
        key_len: usize,
        val_len: usize,
    ) -> Result<(), ()> {
        println!(
            "Called with arguments: lf: {:?}, uf: {:?}, key_len: {key_len}, val_len: {val_len}",
            lf.to_vec(),
            uf.to_vec()
        );
        self.init(lf, uf, key_len, val_len)
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
        self.slice_mut(self.first_val_start(self.capacity as usize) + self.val_len as usize * i, self.val_len as usize)
    }

    fn val(&self, i: usize) -> &[u8] {
        self.slice(self.first_val_start(self.capacity as usize) + self.val_len as usize * i, self.val_len as usize)
    }

    fn val_opt<O: OlcErrorHandler>(this: OPtr<Self, O>, i: usize) -> OPtr<[u8], O> {
        let val_len = o_project!(this.val_len).r() as usize;
        let capacity = o_project!(this.capacity).r() as usize;
        let first_val_start = Self::first_val_start_static(capacity);
        this.as_slice().i(first_val_start + val_len * i..first_val_start + val_len * (i + 1))
    }

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
                (NodeDynamic::<BM>::can_promote)(self, node_tag::BASIC_LEAF).is_ok()
            },
            || {
                let result = self.common.count as usize * 4 <= self.capacity as usize;
                let result = false;
                //println!("is_low {:?}", result);
                result
            },
            {
                let result = val.len() == self.val_len as usize && key.len() == self.key_len as usize;
                //println!("len_is_ok {:?}", result);
                //println!("val_len: {:?} vs. val.len() {:?}, key_len: {:?} vs key.len() {:?}", self.val_len, val.len(), self.key_len, key.len());
                result
            },
            || {
                let res = Self::key_to_index::<BM::OlcEH>(unsafe { OPtr::from_ref(self) }, key);
                if let Ok(i) = res {
                    index.set(i);
                }
                let result = res.is_ok();
                //println!("nnp_is_ok: {:?}", result);
                result
            },
            || {
                let res = index.get() < self.capacity as usize;
                //println!("index_is_ok: {:?}", res);
                //println!("index vs capacity: {:?} vs {:?}", index.get(), self.capacity as usize);
                res
            },
        );
        if resolution != Resolution::Ok {
            println!("Resolution: {:?}", resolution);
        }
        let index = index.get();
        match resolution {
            Resolution::Ok => {
                let was_present = self.set_bit::<true>(index, false);
                self.common.count += (!was_present) as u16;
                self.val_mut(index).copy_from_slice(val);
                Ok(if was_present { Some(()) } else { None })
            }
            Resolution::Convert => {

                NodeDynamic::<BM>::promote(self, node_tag::BASIC_LEAF);


                // this insertion should work after copying over. We need to seperate it out for the promotion logic


                let hash_leaf = page_cast_mut::<FullyDenseLeaf, HashLeaf>(self);

                let ret = NodeStatic::<BM>::insert(hash_leaf, key, val);
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

    fn init(&mut self, _lf: impl SourceSlice, _uf: impl SourceSlice, _lower: Option<&[u8; 5]>) {
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
        if Self::get_bit(this, i) {
            Some(Self::val_opt(this, i))
        } else {
            None
        }
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
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>, key: &[u8]) -> Result<(), ()> {
        if self.split_mode == SPLIT_MODE_HIGH {

            let mut right = insert_upper_sibling(parent, bm, key)?;
            let right = right.cast_mut::<Self>();
            right.init(key, self.upper_fence_combined(), (self.key_len as usize), (self.val_len as usize));
            self.set_upper_fence_tail(key);

            return Ok(())
        }

        println!("Splitting {self}");

        if self.split_mode != SPLIT_MODE_HALF {
            unimplemented!();
        }

        // This is a more barebone method to using the iterator, but this makes it less prone to mistakes
        let mut split_at = self.capacity as u32 / 2;
        let mut count = 0;
        for i in 0..self.capacity as usize {
            if self.get_bit_direct(i) {
                if count >= self.common.count as usize / 2 {
                    split_at = i as u32;
                    break;
                }
                count+=1;
            }
        }

        let key_len = self.key_len as usize;


        let mut sep_key_buf: [MaybeUninit<u8>; 512] = unsafe { MaybeUninit::uninit().assume_init() };
        let sep_key: &[u8] = {
            let initialized = self
                .key_from_numeric_part(split_at + self.reference)
                .write_to_uninit(&mut sep_key_buf[..key_len]);
            initialized
        };
        let mut right = insert_upper_sibling(parent, bm, sep_key)?;
        let right = right.cast_mut::<Self>();

        // sep_key has same length as key_len, so is a valid key in right
        right.init(sep_key, self.upper_fence_combined(), self.key_len as usize, self.val_len as usize).unwrap();
        self.as_page_mut().init_upper_fence(sep_key);

        let old_capacity = self.capacity as usize;
        let old_count = self.common.count;

        self.capacity = split_at as u16;
        right.capacity = old_capacity as u16 - split_at as u16;
        self.common.count = count as u16;


        debug_assert!(self.capacity as usize + right.capacity as usize == old_capacity, "Capacities don't add up: {:?} + {:?} != {old_capacity}", self.capacity, right.capacity);
        debug_assert!(self.common.upper_fence_len <= 4);
        debug_assert!(right.reference == self.reference + self.capacity as u32, "References do not match: {:?} + {:?} != {:?}", self.reference, self.capacity, right.reference);
        for i in split_at as usize..old_capacity as usize {
            if self.set_bit::<false>(i, true) {
                let ri = i - count as usize;
                right.set_bit::<true>(ri, false);
                right.val_mut(ri).copy_from_slice(self.val(i));
                right.common.count += 1;
            }
        }

        debug_assert!(old_count == self.common.count + right.common.count, "Counts don't add up: {:?} + {:?} != {:?}", old_count, self.common.count, right.common.count);
        self.capacity = split_at as u16;
        println!("Splitted in half. New cacpacities are {:?} and {:?}", self.capacity, right.capacity);

        println!("Self after Split: {self}");
        println!("Right after Split: {right}");
        Ok(())
    }

    fn merge(&mut self, right: &mut Page) {
        if right.common.tag == node_tag::FULLY_DENSE_LEAF {
            // check if highest value would fit into current capacity

            // otherwise check if you can demote both values
        } else {
            // demote own node
        }
        todo!()
    }

    fn validate(&self) {
        assert!(self.key_len >= 4, "Bad key length");
        assert!(self.val_len > 0, "Bad val length");

        let val_len = self.val_len as usize;
        let space = PAGE_SIZE
            - (self.common.lower_fence_len as usize)
            - (self.common.upper_fence_len as usize).max(4)
            - offset_of!(Self, _data);
        let mut capacity = space * 8 / (val_len * 8 + 1);
        let is_ok = |capacity: usize| capacity.next_multiple_of(64) / 8 + capacity * val_len <= space;
        while !is_ok(capacity) {
            capacity -= 1;
        }
        assert_eq!(capacity, self.capacity as usize, "Bad capacity");

        let mut count = 0;
        for i in 0..capacity {
            count += if self.get_bit_direct(i) { 1 } else { 0 };
        }
        assert_eq!(count, self.common.count as usize, "Bad count");

        //TODO: add more checks regarding the fences
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        let i = Self::key_to_index::<BM::OlcEH>(OPtr::from_mut(self), k).ok()?;
        if i >= self.capacity as usize {
            return None;
        }
        if self.set_bit::<false>(i, false) {
            self.common.count -= 1;
            Some(())
        } else {
            None
        }
    }

    fn can_promote(&self, to: u8) -> Result<(), PromoteError> {
        match to {
            node_tag::HASH_LEAF => {

                let data_bytes = HashLeaf::get_hash_leaf_data_size();
                let count = self.common.count as usize;


                let key_len = self.key_len as usize;
                let val_len = self.val_len as usize;

                let reserved_slots = count.next_multiple_of(8);
                let slot_bytes = reserved_slots * 2;
                let hash_bytes = count;

                // key is only the offset, so it should be like max 400, so barely more than a byte in size.
                // 2+2 are the lengths that we store in the hash leaf
                let heap_bytes = count * (2 + 2 + key_len.min(4) + val_len);


                let fence_bytes = self.upper_fence_tail().len() + self.lower_fence().len();

                let required_bytes = slot_bytes + hash_bytes + heap_bytes + fence_bytes;

                if required_bytes > data_bytes {
                    return Err(PromoteError::Capacity);
                }
                Ok(())
            },

            node_tag::BASIC_LEAF => {
                pub type BasicLeaf = BasicNode<KindLeaf>;
                let data_bytes = BasicLeaf::get_basic_node_data_size();

                let count = self.common.count as usize;

                let key_len = self.key_len as usize;
                let val_len = self.val_len as usize;

                let head_bytes = 4 * count.next_multiple_of(BasicLeaf::reserved_head_count(count));
                let slot_bytes = 2 * count;

                let hint_bytes = 64;

                let fence_bytes = self.upper_fence_tail().len() + self.lower_fence().len();

                let heap_bytes = count * (2 + 2 + key_len.min(4) + val_len);

                let required_bytes = head_bytes + slot_bytes + hint_bytes + fence_bytes + heap_bytes;

                if required_bytes > data_bytes {
                    return Err(PromoteError::Capacity);
                }

                Ok(())
            },

            _ => Err(Node),
        }
    }

    fn promote(&mut self, to: u8) {
        match to {
            node_tag::BASIC_LEAF => {
                /*
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

                    //TODO: so it compiles for now
                    let index: usize = 0;


                    let numeric_part = self.reference + index as u32;
                    key_buf[nnp_len..].copy_from_slice(&numeric_part.to_be_bytes());
                    tmp.insert_pre_allocated_slot(sparse_index, &key_buf[key_slice_start..], self.val(dense_index));
                }
                tmp.update_hints(0, self.common.count as usize, 0);
                *self.as_page_mut() = tmp.copy_page();

*/
                let mut tmp: BasicLeaf = BasicLeaf::zeroed();
                NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), self.upper_fence_combined(), None);
                for i in 0..self.capacity as usize {
                    if self.get_bit_direct(i) {
                        let val = self.val(i);
                        let key = self.key_from_numeric_part(self.reference + i as u32);

                        NodeStatic::<BM>::insert(&mut tmp, key.to_vec().as_slice(), val).unwrap();
                    }

                }
                *self.as_page_mut() = tmp.copy_page();
            },
            node_tag::HASH_LEAF => {
                //println!("Self: {:?}", self);
                let mut tmp: HashLeaf = HashLeaf::zeroed();
                NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), self.upper_fence_combined(), None);
                for i in 0..self.capacity as usize {
                    if self.get_bit_direct(i) {
                        let val = self.val(i);
                        let key = self.key_from_numeric_part(self.reference + i as u32);

                        NodeStatic::<BM>::insert(&mut tmp, key.to_vec().as_slice(), val).unwrap();
                    }

                }
                *self.as_page_mut() = tmp.copy_page();
            },
            _ => unimplemented!(),
        }
    }
}

impl Debug for FullyDenseLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct(std::any::type_name::<Self>());
        macro_rules! fields {
            ($base:expr => $($f:ident),*) => {$(s.field(std::stringify!($f),&$base.$f);)*};
        }
        fields!(self.common => count, lower_fence_len, upper_fence_len, prefix_len);
        s.field(std::stringify!(self.key_len), &self.key_len);
        s.field(std::stringify!(self.capacity), &self.capacity);
        s.field("lf", &BStr::new(self.lower_fence()));
        s.field("uf", &BString::new(self.upper_fence_combined().to_vec()));
        let mut count = 0;
        let records_fmt =
            (0..self.capacity as usize).filter(|&i| self.get_bit_direct(i)).format_with(",\n", |i, f| {
                let val: &dyn Debug = &self.val(i);
                let key = self.key_from_numeric_part(self.reference + i as u32).to_vec().to_str();
                count+=1;
                f(&mut format_args!("{:?} - index:{i:4} -> key:{:?} , val: {:?}", count-1, key, val))
            });
        s.field("records", &format_args!("\n{}", records_fmt));
        s.finish()
    }
}

impl Display for FullyDenseLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

mod insert_resolver;

mod test {
    use std::collections::HashSet;
    use bytemuck::Zeroable;
    use umolc::{BufferManager, OPtr, SimpleBm};
    use crate::basic_node::{BasicInner, BasicLeaf};
    use crate::fully_dense_leaf::FullyDenseLeaf;
    use crate::hash_leaf::HashLeaf;
    use crate::key_source::SourceSlice;
    use crate::node::{node_tag, page_id_from_bytes, page_id_to_bytes, NodeDynamic, NodeStatic, Page, ToFromPageExt, PAGE_ID_LEN};

    type BM<'a> = &'a SimpleBm<Page>;


    fn generate_key(i: u32, key_len: usize) -> Vec<u8> {
        if key_len < 4 {
            panic!("Key length must be at least 4");
        }
        let mut key= (0..).map(|i| i as u8).take(key_len-4).collect::<Vec<u8>>();
        key.extend_from_slice(&i.to_be_bytes());
        key
    }

    #[allow(clippy::unused_enumerate_index)]
    fn test_leaf<'bm, BM: BufferManager<'bm, Page = Page>>(node_tag: u8, key_len: usize, val_len: usize) {
        let mut page = Page::zeroed();
        let leaf = page.cast_mut::<FullyDenseLeaf>();

        let mut lowerfence = generate_key(0, key_len);
        let mut upperfence = generate_key(4096, key_len);


        let res = leaf.init_wrapper(lowerfence.as_slice(), upperfence.as_slice(), key_len, val_len);

        if res.is_err() {
            panic!("Error: Couldn't initialize the node. This is an error of the node logic itself, this test has no responsibility for it.");
        }

        let max = leaf.capacity as usize;

        for i in 0..leaf.capacity as usize {
            let mut key = generate_key(i as u32, key_len);
            let val: &[u8] = &(0..).map(|i| i as u8).take(val_len).collect::<Vec<u8>>();
            leaf.force_insert::<BM::OlcEH>(key.as_slice(), val);
        }

        let mut i : u32 = 0;
        loop {
            let mut key = generate_key(i, key_len);
            i+=1;

            let result = leaf.as_page_mut().as_dyn_node_mut::<BM>().leaf_remove(key.as_slice());
            if result.is_none() {
                panic!("Error: Couldn't remove the values present. This is an error of the node logic itself, this test has no responsibility for it.");
            }
            if NodeDynamic::<BM>::can_promote(leaf, node_tag).is_ok(){
                println!("Can promote at: {:?}", leaf.common.count);
                break;
            }
        }

        let promotion_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            NodeDynamic::<BM>::promote(leaf, node_tag::HASH_LEAF);
        }));

        assert!(promotion_result.is_ok(), "promotion to HASH_LEAF panicked");


        loop {
            if i>=max as u32 {
                break;
            }
            let mut key = generate_key(i, key_len);
            i+=1;

            let result = leaf.as_page_mut().as_dyn_node_mut::<BM>().leaf_remove(key.as_slice());
            if result.is_none() {
                panic!("After demoting, the value for key '{:?}' is not present.", key);
            }
        }
    }

    #[test]
    fn basic_leaf_demotion() {
        for val_len in 0..100 {
            for key_len in 1..10 {
                test_leaf::<&'static SimpleBm<Page>>(node_tag::BASIC_LEAF, key_len*4, val_len);
            }
        }
    }


    #[test]
    fn hash_leaf_demotion() {
        for val_len in 0..100 {
            for key_len in 1..10 {
                test_leaf::<&'static SimpleBm<Page>>(node_tag::HASH_LEAF, key_len*4, val_len);
            }
        }
    }

}
