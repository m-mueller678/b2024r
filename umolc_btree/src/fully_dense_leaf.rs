use crate::key_source::{HeadSourceSlice, SourceSlice, SourceSlicePair};
use crate::node::{node_tag, CommonNodeHead, DebugNode, NodeDynamic, NodeStatic, ToFromPageExt, PAGE_SIZE};
use crate::{define_node, Page};
use bytemuck::Zeroable;
use std::fmt::{Debug, Formatter};
use std::mem::offset_of;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};

define_node! {
    pub struct FullyDenseLeaf {
        pub common: CommonNodeHead,
        key_len:u16,
        capacity:u16,
        val_len:u16,
        reference: u32,
        _data: [u8; PAGE_SIZE-size_of::<CommonNodeHead>()-10],
    }
}

impl FullyDenseLeaf {
    fn key_to_index<O: OlcErrorHandler>(this: OPtr<Self, O>, k: &[u8]) -> Result<usize, ()> {
        let prefix_len = o_project!(this.common.prefix_len).r() as usize;
        let lower_fence_start = PAGE_SIZE - o_project!(this.common.lower_fence_len).r() as usize;
        let key_len = o_project!(this.key_len).r() as usize;
        if key_len != k.len() {
            O::optimistic_fail();
        }
        let numeric_start = key_len.saturating_sub(4);
        if numeric_start > prefix_len {
            let nnp = this.as_slice().i(lower_fence_start + prefix_len..lower_fence_start + numeric_start);
            if !nnp.mem_cmp(&k[prefix_len..numeric_start]).is_eq() {
                return Err(());
            }
        }
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
        let reference = o_project!(this.reference).r();
        if numeric_part < reference {
            O::optimistic_fail();
        } else {
            Ok((numeric_part - reference) as usize)
        }
    }

    fn try_insert_to_basic(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        todo!()
    }

    fn first_val_start(&self) -> usize {
        (self.capacity as usize).div_ceil(8) + offset_of!(Self, _data)
    }

    fn key_from_numeric_part(&self, np: u32) -> SourceSlicePair<u8, &[u8], HeadSourceSlice> {
        let np_len = (self.key_len as usize).min(4);
        let nnp_len = self.key_len as usize - np_len;
        let numeric_part = HeadSourceSlice::from_head_len(np, 4);
        let last_key = self.lower_fence()[..nnp_len].join(numeric_part.slice(4 - np_len..));
        last_key
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
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for FullyDenseLeaf {
    const TAG: u8 = node_tag::FULLY_DENSE_LEAF;
    const IS_INNER: bool = false;
    type TruncatedKey<'a>
    where
        Self: 'a,
    = SourceSlicePair<u8, &'a [u8], HeadSourceSlice>;

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        let known_outside_range = match Self::key_to_index::<BM::OlcEH>(OPtr::from_mut(self), key) {
            Ok(i) if self.val_len as usize == val.len() => {
                let capacity = self.capacity as usize;
                if i < capacity {
                    self.slice_mut(self.first_val_start() + self.val_len as usize * i, self.val_len as usize)
                        .copy_from_slice(val);
                    let was_present = self.set_bit::<true>(i);
                    return Ok(if was_present { Some(()) } else { None });
                } else {
                    true
                }
            }
            Ok(i) => i >= self.capacity as usize,
            Err(()) => false,
        };
        let reasonably_full = || self.common.count > self.capacity / 4;
        let outside_range = || known_outside_range || self.key_from_index().cmp(key).is_le();
        if reasonably_full() && outside_range() {
            Err(())
        } else {
            self.try_insert_to_basic(key, val)
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
        todo!()
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, key: &[u8], high_on_equal: bool) -> PageId {
        unimplemented!()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for FullyDenseLeaf {
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        let mut last_key = self.key_from_index();
        let split_numeric_part = self.reference.saturating_add(self.capacity as u32);
        let split_key = self.key_from_numeric_part(split_numeric_part);
        let mut right: Self = Self::zeroed();
        right.init(last_key, self.upper_fence_combined(), None);
    }

    fn to_debug(&self) -> DebugNode {
        todo!()
    }

    fn merge(&mut self, right: &mut Page) {
        todo!()
    }

    fn validate(&self) {
        todo!()
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        todo!()
    }
}

impl Debug for FullyDenseLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
