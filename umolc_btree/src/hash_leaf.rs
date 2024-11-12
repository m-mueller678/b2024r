use crate::heap_node::{HeapNode, HeapNodeInfo};
use crate::key_source::{key_head, SourceSlice};
use crate::node::{
    node_tag, CommonNodeHead, DebugNode, NodeDynamic, NodeStatic, ToFromPageExt, PAGE_ID_LEN, PAGE_SIZE,
};
use crate::{impl_to_from_page, Page};
use bytemuck::{Pod, Zeroable};
use std::fmt::{Debug, Formatter};
use std::mem::offset_of;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};

#[derive(Pod, Copy, Clone, Zeroable)]
#[repr(C, align(16))]
#[allow(dead_code)]
pub struct HashLeaf {
    common: CommonNodeHead,
    heap: HeapNodeInfo,
    sorted: u16,
    _data: [u16; HASH_LEAF_DATA_SIZE / 2],
}

const HASH_LEAF_DATA_SIZE: usize = PAGE_SIZE - 16;
const SLOT_RESERVATION: usize = 8;

impl_to_from_page! {HashLeaf}

impl HashLeaf {
    const SLOT_OFFSET: usize = offset_of!(Self, _data);
    const RECORD_TO_KEY_OFFSET: usize = 4;

    fn hash_offset(count: usize) -> usize {
        Self::SLOT_OFFSET + Self::slot_reservation(count) * 2
    }

    fn slot_reservation(count: usize) -> usize {
        count.next_multiple_of(SLOT_RESERVATION)
    }

    fn heap_start_min(count: usize) -> usize {
        Self::hash_offset(count) + count
    }
    fn hash(k: &[u8]) -> u8 {
        crc32fast::hash(k) as u8
    }

    fn slot(&self, index: usize) -> usize {
        debug_assert!(index < self.common.count as usize);
        self._data[index] as usize
    }

    fn find<O: OlcErrorHandler>(this: OPtr<Self, O>, key: &[u8]) -> (Option<usize>, u8) {
        let prefix_len = o_project!(this.common.prefix_len).r() as usize;
        if prefix_len > key.len() {
            O::optimistic_fail();
        }
        let hash = Self::hash(key);
        let key = &key[prefix_len..];
        let count = o_project!(this.common.count).r() as usize;
        let hash_offset = Self::hash_offset(count);

        for i in 0..count {
            if this.as_slice::<u8>().i(hash_offset + i).r() == hash {
                let offset = o_project!(this._data).unsize().i(i).r() as usize;
                let key_len = this.read_unaligned_nonatomic_u16(offset);
                let stored_key = this.as_slice::<u8>().sub(offset + Self::RECORD_TO_KEY_OFFSET, key_len);
                if stored_key.mem_cmp(key).is_eq() {
                    return (Some(i), hash);
                }
            }
        }
        (None, hash)
    }
}

impl Debug for HashLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for HashLeaf {
    const TAG: u8 = node_tag::HASH_LEAF;
    const IS_INNER: bool = false;
    type TruncatedKey<'a>
    where
        Self: 'a,
    = &'a [u8];

    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, _lower: Option<&[u8; 5]>) {
        self.as_page_mut().common_init(node_tag::HASH_LEAF, lf, uf);
        self.init_heap();
    }

    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)> {
        // needed for type inference
        let ret: std::iter::Once<_> = unimplemented!();
        ret
    }

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        let (index, _hash) = Self::find(this, key);
        let offset = o_project!(this._data).unsize().i(index?).r() as usize;
        let v_len = this.read_unaligned_nonatomic_u16(offset + 2);
        Some(this.as_slice().sub(offset - v_len, v_len))
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, key: &[u8], high_on_equal: bool) -> PageId {
        unimplemented!()
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        let (index, hash) = Self::find::<BM::OlcEH>(OPtr::from_mut(self), key);
        let count = self.common.count as usize;
        let new_heap_start = Self::heap_start_min(count + index.is_none() as usize);
        HeapNode::insert(
            self,
            new_heap_start,
            &key[self.common.prefix_len as usize..],
            val,
            index.ok_or(count),
            |this| {
                let r1 = Self::slot_reservation(count);
                let r2 = Self::slot_reservation(count + 1);
                if r1 != r2 {
                    this.relocate_by::<true, u8>(Self::SLOT_OFFSET + r1 * 2, count, SLOT_RESERVATION * 2);
                }
                this.common.count += 1;
                this.cast_slice_mut::<u8>()[Self::hash_offset(count + 1) + count] = hash;
            },
        )
        .map_err(|_| ())
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for HashLeaf {
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        todo!()
    }

    fn to_debug(&self) -> DebugNode {
        let range = 0..self.common.count as usize;
        DebugNode {
            prefix_len: self.common.prefix_len as usize,
            lf: self.lower_fence().to_vec(),
            uf: self.upper_fence_combined().to_vec(),
            keys: range.clone().map(|i| self.heap_key(i).to_vec()).collect(),
            values: range.map(|i| self.heap_val(i).to_vec()).collect(),
        }
    }

    fn merge(&mut self, right: &mut Page) {
        todo!()
    }

    fn validate(&self) {
        HeapNode::validate(self);
    }

    fn leaf_remove(&mut self, key: &[u8]) -> Option<()> {
        let (Some(index), _hash) = Self::find::<BM::OlcEH>(OPtr::from_mut(self), key) else {
            return None;
        };
        self.heap_free(index);
        let count = self.common.count as usize;
        {
            let r1 = Self::slot_reservation(count);
            let r2 = Self::slot_reservation(count - 1);
            self.relocate_by::<false, u16>(Self::SLOT_OFFSET + 2 * index + 2, count - 1 - index, 1);
            if r1 == r2 {
                self.relocate_by::<false, u8>(Self::SLOT_OFFSET + r1 * 2 + index + 1, count - 1 - index, 1);
            } else {
                self.relocate_by::<false, u8>(Self::SLOT_OFFSET + r1 * 2, index, SLOT_RESERVATION * 2);
                self.relocate_by::<false, u8>(
                    Self::SLOT_OFFSET + r1 * 2 + index + 1,
                    count - 1 - index,
                    SLOT_RESERVATION * 2 + 1,
                );
            }
        }
        self.common.count -= 1;
        HeapNode::validate(self);
        Some(())
    }
}

impl HeapNode for HashLeaf {
    type KeyLength = u16;
    type ValLength = u16;

    fn slot_offset(&self) -> usize {
        offset_of!(Self, _data)
    }

    fn heap_info_mut(&mut self) -> &mut HeapNodeInfo {
        &mut self.heap
    }

    fn heap_info(&self) -> &HeapNodeInfo {
        &self.heap
    }

    fn validate(&self) {}
}
