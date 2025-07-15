use crate::heap_node::{HeapNode, HeapNodeInfo, HeapLength, ConstHeapLength};
use crate::key_source::SourceSlice;
use crate::node::{
    find_separator, insert_upper_sibling, node_tag, page_cast_mut, DebugNode, NodeDynamic, NodeStatic, ToFromPageExt,
    PAGE_SIZE, PromoteError,
};
use crate::key_source::common_prefix;
use crate::util::Supreme;
use crate::fully_dense_leaf::FullyDenseLeaf;
use crate::{define_node, Page};
use arrayvec::ArrayVec;
use bstr::{BStr, BString};
use bytemuck::{Pod, Zeroable};
use itertools::Itertools;
use std::fmt::{Debug, Display, Formatter};
use std::fmt;
use std::vec::Vec;
use std::mem::offset_of;
use std::ops::Range;
use indxvec::Printing;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};
use crate::hash_leaf::PromoteError::{Capacity, Keys, ValueLen};

define_node! {
    pub struct HashLeaf {
    pub common: CommonNodeHead,
    pub heap: HeapNodeInfo,
    pub sorted: u16,
    pub _data: [u16; HASH_LEAF_DATA_SIZE / 2],
    }
}


const HASH_LEAF_DATA_SIZE: usize = PAGE_SIZE - 16;
const SLOT_RESERVATION: usize = 8;

impl HashLeaf {
    const SLOT_OFFSET: usize = offset_of!(Self, _data);
    const RECORD_TO_KEY_OFFSET: usize = 4;

    pub fn get_hash_leaf_data_size() -> usize {
        HASH_LEAF_DATA_SIZE
    }

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

    fn sort(&mut self) {
        let count = self.common.count as usize;
        if self.sorted as usize == count {
            return;
        }
        let mut buffer: ArrayVec<(u16, u16), { PAGE_SIZE / 4 }> = (0..count)
            .map(|i| (self.slot(i) as u16, *self.page_index::<u8>(Self::hash_offset(count) + i) as u16))
            .collect();
        buffer.sort_unstable_by_key(|s| self.heap_key_at(s.0 as usize));
        for i in 0..count {
            self.set_slot(i, buffer[i].0 as usize);
        }
        let hashes = self.slice_mut(Self::hash_offset(count), count);
        for (i, h) in buffer.iter().enumerate() {
            hashes[i] = h.1 as u8;
        }
        self.sorted = self.common.count
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

    fn validate(&self) {
        if !cfg!(feature = "validate_node") {
            return;
        }
        assert!(self.sorted <= self.common.count);
        self.validate_heap();
        let count = self.common.count as usize;
        for i in 0..count {
            assert_eq!(Self::hash(self.heap_key(i)), *self.page_index::<u8>(Self::hash_offset(count) + i))
        }
        let lower_fence =
            std::iter::once(Supreme::X(self.lower_fence().slice(self.common.prefix_len as usize..).to_vec()));
        let sorted_keys = (0..self.sorted as usize).map(|i| self.heap_key(i)).map(|k| Supreme::X(k.to_vec()));
        let upper_fence = std::iter::once(if self.common.upper_fence_len == 0 && self.common.prefix_len == 0 {
            Supreme::Sup
        } else {
            Supreme::X(self.upper_fence_tail().to_vec())
        });
        let keys_and_fences = lower_fence.chain(sorted_keys).chain(upper_fence);
        assert!(keys_and_fences.is_sorted(), "not sorted: {:?}", self);
    }

    fn copy_records(&self, dst: &mut Self, src_range: Range<usize>, dst_start: usize) {
        let dst_range = dst_start..(src_range.end + dst_start - src_range.start);
        let dpl = dst.common.prefix_len as usize;
        let spl = self.common.prefix_len as usize;
        let restore_prefix: &[u8] = if dpl < spl { &self.as_page().prefix()[dpl..] } else { &[][..] };
        let prefix_grow = dpl.saturating_sub(spl);
        for (src_i, dst_i) in src_range.clone().zip(dst_range.clone()) {
            let key = restore_prefix.join(self.heap_key(src_i).slice(prefix_grow..));
            dst.heap_write_new(key, self.heap_val(src_i), dst_i);
        }
        let dst_hashes = dst.slice_mut::<u8>(Self::hash_offset(dst.common.count as usize) + dst_start, src_range.len());
        let self_hashes =
            self.slice::<u8>(Self::hash_offset(self.common.count as usize) + src_range.start, src_range.len());
        dst_hashes.copy_from_slice(self_hashes);
    }
}

impl Debug for HashLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct(std::any::type_name::<Self>());
        macro_rules! fields {
            ($base:expr => $($f:ident),*) => {$(s.field(std::stringify!($f),&$base.$f);)*};
        }
        fields!(self.common => count, lower_fence_len, upper_fence_len, prefix_len);
        fields!(self => heap);
        s.field("lf", &BStr::new(self.lower_fence()));
        s.field("uf", &BString::new(self.upper_fence_combined().to_vec()));
        let records_fmt = (0..self.common.count as usize).format_with(",\n", |i, f| {
            let offset = self.slot(i);
            let val: &dyn Debug = &BStr::new(self.heap_val(i));
            let key = BStr::new(self.heap_key(i));
            let kl = key.len();
            let hash = *self.page_index::<u8>(Self::hash_offset(self.common.count as usize) + i);
            f(&mut format_args!("{i:4}:{offset:04x}->[0x{hash:02x}][{kl:3}] {key:?} -> {val:?}"))
        });
        s.field("records", &format_args!("\n{}", records_fmt));
        s.finish()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeStatic<'bm, BM> for HashLeaf {
    const TAG: u8 = node_tag::HASH_LEAF;
    const IS_INNER: bool = false;
    type TruncatedKey<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, _lower: Option<&[u8; 5]>) {
        self.as_page_mut().common_init(node_tag::HASH_LEAF, lf, uf);
        self.init_heap();
        self.sorted = 0;
    }

    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)> {
        // needed for type inference
        #[allow(unreachable_code)]
        std::iter::once(unimplemented!())
    }

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        let (index, _hash) = Self::find(this, key);
        let offset = o_project!(this._data).unsize().i(index?).r() as usize;
        let v_len = this.read_unaligned_nonatomic_u16(offset + 2);
        Some(this.as_slice().sub(offset - v_len, v_len))
    }

    fn lookup_inner(_this: OPtr<'_, Self, BM::OlcEH>, _key: &[u8], _high_on_equal: bool) -> PageId {
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
                *this.page_index_mut::<u8>(Self::hash_offset(count + 1) + count) = hash;
            },
        )
        .map_err(|_| ())
    }

    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let range = 0..self.common.count as usize;
        let keys = range.clone().map(|i| self.heap_key(i).to_vec()).collect();
        let values = range.map(|i| self.heap_val(i).to_vec()).collect();
        (keys, values)
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>> NodeDynamic<'bm, BM> for HashLeaf {
    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        self.sort();
        let mut left = Self::zeroed();
        let count = self.common.count as usize;
        let (low_count, sep_key) = find_separator::<BM, _>(self, |i| self.heap_key(i));
        let mut right = insert_upper_sibling(parent, bm, sep_key)?;
        let right = right.cast_mut::<Self>();
        self.validate();
        NodeStatic::<BM>::init(&mut left, self.lower_fence(), sep_key, None);
        NodeStatic::<BM>::init(right, sep_key, self.upper_fence_combined(), None);
        let (lr, rr) = (0..low_count, low_count..count);
        debug_assert!(
            SourceSlice::cmp(self.heap_key(lr.end - 1), sep_key.slice(self.common.prefix_len as usize..)).is_lt()
        );
        debug_assert!(
            SourceSlice::cmp(sep_key.slice(self.common.prefix_len as usize..), self.heap_key(rr.start)).is_le()
        );
        left.common.count = lr.len() as u16;
        left.sorted = lr.len() as u16;
        self.copy_records(&mut left, lr.clone(), 0);
        right.common.count = rr.len() as u16;
        right.sorted = rr.len() as u16;
        self.copy_records(right, rr.clone(), 0);
        left.validate();
        right.validate();
        *self = left;
        Ok(())
    }

    fn merge(&mut self, right: &mut Page) {
        debug_assert!(right.common.tag == node_tag::HASH_LEAF);
        if cfg!(feature = "validate_node") {
            self.validate();
            right.as_dyn_node::<BM>().validate();
        }
        let right = page_cast_mut::<Page, Self>(right);
        let mut tmp = Self::zeroed();
        let left_count = self.common.count as usize;
        let right_count = right.common.count as usize;
        NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), right.upper_fence_combined(), None);
        tmp.common.count = (left_count + right_count) as u16;
        self.copy_records(&mut tmp, 0..left_count, 0);
        right.copy_records(&mut tmp, 0..right_count, left_count);
        tmp.sorted = if self.common.count == self.sorted { self.sorted + right.sorted } else { self.sorted };
        tmp.validate();
        *self = tmp;
    }

    fn validate(&self) {
        self.validate()
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

    fn can_promote(&self, to: u8) -> Result<(), PromoteError> {
        match to {
            node_tag::FULLY_DENSE_LEAF => {

                println!("Trying to promote");
                let count = self.common.count as usize;
                if count == 0 {
                    //panic!("A hashleaf was empty and should have been deleted");
                    return Err(Capacity);
                }

                if self.lower_fence().is_empty() {
                    return Err(PromoteError::Fences);
                }

                let first_key = self.heap_key(0);
                let first_val = self.heap_val(0);

                let key_len = first_key.len();
                let val_len = first_val.len();

                let mut key_error: bool = false;
                let mut val_error: bool = false;

                for i in 0..count {
                    let key = self.heap_key(i);
                    let val = self.heap_val(i);

                    if key.len()!= key_len {
                        key_error = true;
                    }

                    if val.len() != val_len {
                        val_error = true;
                    }
                }

                // we don't return immediately but just here, in case there would be a hierachy of errors.
                // if there is none, we can just remove these if-cases and return on finding an error.
                if(key_error) {
                    return Result::Err(Keys);
                }
                if val_error {
                    return Result::Err(ValueLen);
                }



                let mut min_suffix = u32::MAX;
                let mut max_suffix = 0;

                for i in 0..count {
                    let suffix = self.heap_key(i);


                    let mut full_key = Vec::with_capacity(self.common.prefix_len as usize + suffix.len());
                    full_key.extend_from_slice(self.prefix());
                    full_key.extend_from_slice(suffix);

                    let full_len = full_key.len();
                    let numeric_slice = &full_key[full_len.saturating_sub(4)..];

                    let mut padded = [0u8; 4];
                    padded[..full_len.min(4)].copy_from_slice(&numeric_slice);

                    let index = u32::from_be_bytes(padded.try_into().unwrap());

                    min_suffix = min_suffix.min(index);
                    max_suffix = max_suffix.max(index);
                }
                let area = max_suffix - min_suffix + 1;


                if area as usize >
                    FullyDenseLeaf::get_capacity_fdl(self.lower_fence().len(),
                                                     self.upper_fence_tail().len(),
                                                     first_val.len()) {
                    return Err(Capacity);

                }


                Ok(())
            },
            _ => Err(PromoteError::Node),
        }
    }

    fn promote(&mut self, to: u8, _bm: BM) {
        match to {
            node_tag::FULLY_DENSE_LEAF => {

                let count = self.common.count as usize;
                let prefix_len = self.common.prefix_len as usize;

                let first_key = self.heap_key(0);
                let first_val = self.heap_val(0);
                let key_len = first_key.len();
                let val_len = first_val.len();

                let mut fdl = FullyDenseLeaf::zeroed();


                fdl.init_wrapper(self.lower_fence(), self.upper_fence_combined(), key_len+prefix_len, val_len)
                    .expect("FDL init_wrapper failed in promote()");




                for i in 0..count {
                    let suffix = self.heap_key(i);
                    let val = self.heap_val(i);

                    let mut full_key = Vec::with_capacity(self.common.prefix_len as usize + suffix.len());
                    full_key.extend_from_slice(self.prefix());
                    full_key.extend_from_slice(suffix);
                    fdl.force_insert::<BM::OlcEH>(full_key.as_slice(), val);
                }

                *self.as_page_mut() = fdl.copy_page();
            },
            _=> unreachable!()
        }
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

    fn validate(&self) {
        self.validate();
    }
}

impl Display for HashLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}