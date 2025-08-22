use crate::define_node;
use crate::heap_node::{HeapLength, HeapLengthError, HeapNode, HeapNodeInfo};
use crate::key_source::{key_head, HeadSourceSlice, SourceSlice, SourceSlicePair};
use crate::node::{find_separator, insert_upper_sibling, node_tag, page_cast_mut, page_id_from_bytes, page_id_from_olc_bytes, CommonNodeHead, KindInner, KindLeaf, NodeDynamic, NodeKind, NodeStatic, Page, PromoteError, ToFromPageExt, PAGE_ID_LEN, PAGE_SIZE};
use crate::util::Supreme;
use bstr::{BStr, BString};
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use itertools::Itertools;
use std::fmt::{Debug, Formatter};
use std::mem::{offset_of, size_of, MaybeUninit};
use std::ops::Range;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};
use crate::fully_dense_leaf::FullyDenseLeaf;
use crate::hash_leaf::HashLeaf;
use crate::node::PromoteError::{Capacity, Keys, Node, ValueLen};

const HINT_COUNT: usize = 16;
const MIN_HINT_SPACING: usize = 3;

// must align with min hint spacing, so hints are updated when min count is reached
const MIN_HINT_COUNT: usize = MIN_HINT_SPACING * (HINT_COUNT + 1);

define_node! {
    pub struct BasicNode<V> {
        pub common: CommonNodeHead,
        heap: HeapNodeInfo,
        hints: [u32; HINT_COUNT],
        _data: [u32; BASIC_NODE_DATA_SIZE],
    }
}

pub type BasicLeaf = BasicNode<KindLeaf>;
pub type BasicInner = BasicNode<KindInner>;

const BASIC_NODE_DATA_SIZE: usize = (PAGE_SIZE - size_of::<CommonNodeHead>() - 2 * 2 - 16 * 4) / 4;

impl<V: NodeKind> BasicNode<V> {
    fn lower(&self) -> &[u8; PAGE_ID_LEN] {
        self.page_id_bytes(Self::LOWER_OFFSET)
    }

    fn page_id_bytes(&self, offset: usize) -> &[u8; PAGE_ID_LEN] {
        assert!(!V::IS_LEAF);
        self.slice::<u8>(offset, PAGE_ID_LEN).try_into().unwrap()
    }

    pub fn get_basic_node_data_size () -> usize {
        BASIC_NODE_DATA_SIZE*4
    }

    pub fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(HEAD_RESERVATION)
    }
    fn slot_offset(count: usize) -> usize {
        Self::HEAD_OFFSET + 4 * Self::reserved_head_count(count)
    }

    pub fn heap_start_min(count: usize) -> usize {
        Self::slot_offset(count) + 2 * count
    }

    fn key_combined(&self, index: usize) -> SourceSlicePair<u8, HeadSourceSlice, &[u8]> {
        let head = self.heads()[index];
        let offset = self.slot(index);
        let len = self.read_unaligned_u16(offset);
        let tail_len = len.saturating_sub(4);
        let head = HeadSourceSlice::from_head_len(head, len);
        let tail = self.slice(offset + Self::RECORD_TO_KEY_OFFSET, tail_len);
        head.join(tail)
    }

    fn find<O: OlcErrorHandler>(this: OPtr<Self, O>, key: &[u8]) -> Result<usize, usize> {
        let prefix_len = o_project!(this.common.prefix_len).r() as usize;
        if prefix_len > key.len() {
            O::optimistic_fail()
        }
        let truncated = &key[prefix_len..];
        let needle_head = key_head(truncated);
        let count = o_project!(this.common.count).r() as usize;
        let slot_start_index = Self::slot_offset(count) / 2;
        let slots = this.as_slice::<u16>().i(slot_start_index..slot_start_index + count);
        let heads = this.as_slice::<u32>().i(Self::HEAD_OFFSET / 4..Self::HEAD_OFFSET / 4 + count);
        let hints = o_project!(this.hints).unsize();
        if heads.len() == 0 {
            return Err(0);
        }
        let mut head_range_start = 0;
        let mut head_range_end = heads.len();
        if count >= MIN_HINT_COUNT {
            let spacing = count / (HINT_COUNT + 1);
            let mut hint_index = 0;
            while hint_index < HINT_COUNT {
                let hint = hints.i(hint_index).r();
                if hint < needle_head {
                    head_range_start = (hint_index + 1) * spacing + 1;
                } else {
                    break;
                }
                hint_index += 1;
            }
            while hint_index < HINT_COUNT {
                let hint = hints.i(hint_index).r();
                if hint > needle_head {
                    head_range_end = (hint_index + 1) * spacing;
                    break;
                }
                hint_index += 1;
            }
        }
        const _: () = {
            assert!(MIN_HINT_SPACING >= 2);
        };

        let matching_head_range =
            (head_range_start..=head_range_end - 1).binary_all(|i| heads.i(i).r().cmp(&needle_head));
        if matching_head_range.is_empty() {
            return Err(matching_head_range.start);
        }
        let key_position = (matching_head_range.start..=matching_head_range.end - 1).binary_by(move |i| {
            let offset = slots.i(i).r() as usize;
            let len = this.read_unaligned_nonatomic_u16(offset);
            let tail = this.as_slice::<u8>().sub(offset + Self::RECORD_TO_KEY_OFFSET, len.saturating_sub(4));
            if len <= 4 || truncated.len() <= 4 {
                len.cmp(&truncated.len())
            } else {
                tail.mem_cmp(&truncated[4..])
            }
        });
        key_position
    }

    const LOWER_OFFSET: usize = offset_of!(Self, _data);
    const HEAD_OFFSET: usize = offset_of!(Self, _data) + if V::IS_LEAF { 0 } else { 8 };

    fn heads(&self) -> &[u32] {
        self.slice(Self::HEAD_OFFSET / 4, self.common.count as usize)
    }

    fn set_head(&mut self, i: usize, head: u32) {
        *self.page_index_mut(Self::HEAD_OFFSET / 4 + i) = head;
    }
    fn copy_records(&self, dst: &mut Self, src_range: Range<usize>, dst_start: usize) {
        let dst_range = dst_start..(src_range.end + dst_start - src_range.start);
        let dpl = dst.common.prefix_len as usize;
        let spl = self.common.prefix_len as usize;
        let restore_prefix: &[u8] = if dpl < spl { &self.as_page().prefix()[dpl..] } else { &[][..] };
        let prefix_grow = dpl.saturating_sub(spl);
        for (src_i, dst_i) in src_range.clone().zip(dst_range.clone()) {
            let key = restore_prefix.join(self.key_combined(src_i).slice(prefix_grow..));
            dst.insert_pre_allocated_slot(dst_i, key, self.heap_val(src_i));
        }
    }

    pub fn head_reservation() -> usize {
        HEAD_RESERVATION
    }

    pub fn insert_pre_allocated_slot(&mut self, index: usize, key: impl SourceSlice, val: &[u8]) {
        self.heap_write_new(key, val, index);
        self.set_head(index, key_head(key));
    }

    pub fn update_hints(&mut self, old_count: usize, new_count: usize, mut change_index: usize) {
        debug_assert!(old_count != new_count);
        if new_count < MIN_HINT_COUNT {
            return;
        }
        let spacing = new_count / (HINT_COUNT + 1);
        if spacing != old_count / (HINT_COUNT + 1) {
            change_index = 0;
        }
        for hint_index in 0..HINT_COUNT {
            let head_index = spacing * (hint_index + 1);
            if head_index < change_index {
                continue;
            }
            self.hints[hint_index] = self.heads()[head_index];
        }
    }

    fn validate(&self) {
        if !cfg!(feature = "validate_node") {
            return;
        }
        let count = self.common.count as usize;
        if count >= MIN_HINT_COUNT {
            let spacing = count / (HINT_COUNT + 1);
            for i in 0..HINT_COUNT {
                assert_eq!(self.hints[i], self.heads()[(i + 1) * spacing]);
            }
        }
        self.validate_heap();
        let lower_fence =
            std::iter::once(Supreme::X(self.lower_fence().slice(self.common.prefix_len as usize..).to_vec()));
        let keys = (0..self.common.count as usize).map(|i| self.key_combined(i)).map(|k| Supreme::X(k.to_vec()));
        let upper_fence = std::iter::once(if self.common.upper_fence_len == 0 && self.common.prefix_len == 0 {
            Supreme::Sup
        } else {
            Supreme::X(self.upper_fence_tail().to_vec())
        });
        let keys_and_fences = lower_fence.chain(keys).chain(upper_fence);
        assert!(keys_and_fences.is_sorted(), "not sorted: {:?}", self);
    }

    fn remove<O: OlcErrorHandler>(&mut self, key: &[u8]) -> Option<()> {
        let Ok(index) = Self::find::<O>(OPtr::from_mut(self), key) else {
            return None;
        };
        self.heap_free(index);
        let count = self.common.count as usize;
        {
            let orhc = Self::reserved_head_count(count);
            let nrhc = Self::reserved_head_count(count - 1);
            self.relocate_by::<false, u32>(Self::HEAD_OFFSET + 4 * index + 4, count - 1 - index, 1);
            if nrhc == orhc {
                self.relocate_by::<false, u16>(Self::HEAD_OFFSET + nrhc * 4 + index * 2 + 2, count - 1 - index, 1);
            } else {
                self.relocate_by::<false, u16>(Self::HEAD_OFFSET + orhc * 4, index, HEAD_RESERVATION * 2);
                self.relocate_by::<false, u16>(
                    Self::HEAD_OFFSET + orhc * 4 + index * 2 + 2,
                    count - 1 - index,
                    HEAD_RESERVATION * 2 + 1,
                );
            }
        }
        self.common.count -= 1;
        self.update_hints(count, count - 1, index);
        self.validate();
        Some(())
    }

    const TAG: u8 = if V::IS_LEAF { node_tag::BASIC_LEAF } else { node_tag::BASIC_INNER };
    const RECORD_TO_KEY_OFFSET: usize = if V::IS_LEAF { 4 } else { 2 };
}

impl<V: NodeKind> Debug for BasicNode<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct(std::any::type_name::<Self>());
        macro_rules! fields {
            ($base:expr => $($f:ident),*) => {$(s.field(std::stringify!($f),&$base.$f);)*};
        }
        fields!(self.common => count, lower_fence_len, upper_fence_len, prefix_len);
        fields!(self => heap);
        s.field("lf", &BStr::new(self.lower_fence()));
        s.field("uf", &BString::new(self.upper_fence_combined().to_vec()));
        if !V::IS_LEAF {
            s.field("lower", &page_id_from_bytes(self.lower()));
        };
        let records_fmt = (0..self.common.count as usize).format_with(",\n", |i, f| {
            let offset = self.slot(i);
            let val: &dyn Debug = if V::IS_LEAF {
                &BStr::new(self.heap_val(i))
            } else {
                &page_id_from_bytes(self.heap_val(i).try_into().unwrap())
            };
            let head = self.heads()[i];
            let kl = self.read_unaligned_u16(offset);
            let key = self.key_combined(i);
            f(&mut format_args!("{i:4}:{offset:04x}->[0x{head:08x}][{kl:3}] {:?} -> {val:?}", key.to_vec().as_slice()))
        });
        s.field("records", &format_args!("\n{}", records_fmt));
        s.finish()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>, V: NodeKind> NodeStatic<'bm, BM> for BasicNode<V> {
    const TAG: u8 = if V::IS_LEAF { 251 } else { 250 };
    const IS_INNER: bool = !V::IS_LEAF;
    type TruncatedKey<'a> = SourceSlicePair<u8, HeadSourceSlice, &'a [u8]>;

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {

        let index = Self::find::<BM::OlcEH>(OPtr::from_mut(self), key);
        let count = self.common.count as usize;
        let new_heap_start = Self::heap_start_min(count + index.is_err() as usize);
        let key = &key[self.common.prefix_len as usize..];
        HeapNode::insert(self, new_heap_start, key, val, index, |this| {
            let insert_at = index.unwrap_err();
            let orhc = Self::reserved_head_count(count);
            let nrhc = Self::reserved_head_count(count + 1);
            if nrhc == orhc {
                this.relocate_by::<true, u16>(Self::HEAD_OFFSET + nrhc * 4 + insert_at * 2, count - insert_at, 1);
            } else {
                this.relocate_by::<true, u16>(
                    Self::HEAD_OFFSET + orhc * 4 + insert_at * 2,
                    count - insert_at,
                    HEAD_RESERVATION * 2 + 1,
                );
                this.relocate_by::<true, u16>(Self::HEAD_OFFSET + orhc * 4, insert_at, HEAD_RESERVATION * 2);
            }
            this.relocate_by::<true, u32>(Self::HEAD_OFFSET + 4 * insert_at, count - insert_at, 1);
            if index.is_err() {
                this.common.count += 1;
            }
            this.set_head(insert_at, key_head(key));
            this.update_hints(count, count + 1, insert_at);
        })
        .map_err(|_| ())
    }

    fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: Option<&[u8; 5]>) {
        if V::IS_LEAF {
            assert!(lower.is_none());
            self.common.scan_counter = 3;
        } else {
            self.slice_mut(Self::LOWER_OFFSET, 5).copy_from_slice(lower.unwrap());
        }
        self.as_page_mut().common_init(if V::IS_LEAF { node_tag::BASIC_LEAF } else { node_tag::BASIC_INNER }, lf, uf);
        self.init_heap();
    }

    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)> {
        assert!(<Self as NodeStatic<'bm, BM>>::IS_INNER);
        let lower = std::iter::once((Default::default(), Self::LOWER_OFFSET));
        let rest = (0..self.common.count as usize).map(|i| (self.key_combined(i), self.slot(i) - self.heap_val_len(i)));
        lower.chain(rest).map(|(k, o)| (k, page_id_from_bytes(self.page_id_bytes(o))))
    }

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        assert!(V::IS_LEAF);
        let index = Self::find(this, key).ok()?;
        let slot_offset = Self::slot_offset(o_project!(this.common.count).r() as usize);
        let offset = this.as_slice::<u16>().i(slot_offset / 2 + index).r() as usize;
        let v_len = this.read_unaligned_nonatomic_u16(offset + 2);
        Some(this.as_slice().sub(offset - v_len, v_len))
    }

    fn lookup_inner(this: OPtr<'_, Self, BM::OlcEH>, key: &[u8], high_on_equal: bool) -> PageId {
        assert!(!V::IS_LEAF);
        let index = match Self::find(this, key) {
            Err(i) => i,
            Ok(i) => i + high_on_equal as usize,
        };
        let lower_offset = if index == 0 {
            Self::LOWER_OFFSET
        } else {
            let slot_offset = Self::slot_offset(o_project!(this.common.count).r() as usize);
            let offset = this.as_slice::<u16>().i(slot_offset / 2 + index - 1).r() as usize;
            offset - PAGE_ID_LEN
        };
        page_id_from_olc_bytes(this.array_slice(lower_offset))
    }


    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let range = 0..self.common.count as usize;
        let keys = range.clone().map(|i| self.key_combined(i).to_vec()).collect();
        let values = (0..1)
            .filter(|_| !V::IS_LEAF)
            .map(|_| self.lower().to_vec())
            .chain(range.map(|i| self.heap_val(i).to_vec()))
            .collect();
        (keys, values)
    }

    fn set_scan_counter(&mut self, counter: u8) {
        self.common.scan_counter = counter;
    }

    fn has_good_heads(&self) -> (bool, bool) {
        let treshold = self.common.count as usize / 16;
        let mut collision_count = 0;

        let mut first = true;
        let mut second = true;

        for i in 1..(self.common.count / 2) as usize {
            let head1 = self.heads()[i - 1];
            let head2 = self.heads()[i];
            if head1 == head2 {
                collision_count += 1;
            }
            if collision_count > treshold {
                first = false;
                break;
            }
        }
        for i in (self.common.count as usize / 2)..self.common.count as usize {
            let head1 = self.heads()[i - 1];
            let head2 = self.heads()[i];
            if head1 == head2 {
                collision_count += 1;
            }
            if collision_count > treshold {
                second = false;
                break;
            }
        }
        (first, second)
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>, V: NodeKind> NodeDynamic<'bm, BM> for BasicNode<V> {
    fn validate(&self) {
        self.validate();
    }

    fn merge(&mut self, right: &mut Page) {
        debug_assert!(right.common.tag == Self::TAG);
        if cfg!(feature = "validate_node") {
            self.validate();
            right.as_dyn_node::<BM>().validate();
        }
        let right = page_cast_mut::<Page, Self>(right);
        let mut tmp = BasicNode::<V>::zeroed();
        let left_count = self.common.count as usize;
        let right_count = right.common.count as usize;
        if V::IS_LEAF {
            NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), right.upper_fence_combined(), None);
            tmp.common.count = (left_count + right_count) as u16;
            self.copy_records(&mut tmp, 0..left_count, 0);
            right.copy_records(&mut tmp, 0..right_count, left_count);
        } else {
            NodeStatic::<BM>::init(&mut tmp, self.lower_fence(), right.upper_fence_combined(), Some(self.lower()));
            tmp.common.count = (left_count + right_count + 1) as u16;
            self.copy_records(&mut tmp, 0..left_count, 0);
            right.copy_records(&mut tmp, 0..right_count, left_count + 1);
            tmp.heap_write_new(
                self.as_page().upper_fence_combined().slice_start(tmp.common.prefix_len as usize),
                right.lower().as_slice(),
                left_count,
            );
        }
        tmp.update_hints(0, tmp.common.count as usize, 0);
        tmp.validate();
        *self = tmp;
    }

    fn split(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>, _key: &[u8]) -> Result<(), ()> {

        let (lft, rght) = NodeStatic::<BM>::has_good_heads(self);

        let mut left = BasicNode::<V>::zeroed();

        let scan_counter = self.common.scan_counter;

        let count = self.common.count as usize;
        let (low_count, sep_key) = find_separator::<BM, _>(self, |i| self.key_combined(i));
        let mut right = insert_upper_sibling(parent, bm, sep_key)?;
        let right = page_cast_mut::<_, BasicNode<V>>(&mut *right);
        self.validate();
        let (lr, rr) = if V::IS_LEAF {
            NodeStatic::<BM>::init(&mut left, self.lower_fence(), sep_key, None);
            NodeStatic::<BM>::init(right, sep_key, self.upper_fence_combined(), None);
            (0..low_count, low_count..count)
        } else {
            NodeStatic::<BM>::init(&mut left, self.as_page().lower_fence(), sep_key, Some(self.lower()));
            let mid_child = self.heap_val(low_count).try_into().unwrap();
            NodeStatic::<BM>::init(right, sep_key, self.as_page().upper_fence_combined(), Some(mid_child));
            (0..low_count, low_count + 1..count)
        };
        debug_assert!(self.key_combined(lr.end - 1).cmp(sep_key.slice(self.common.prefix_len as usize..)).is_lt());
        debug_assert!(sep_key.slice(self.common.prefix_len as usize..).cmp(self.key_combined(rr.start)).is_le());
        left.common.count = lr.len() as u16;
        self.copy_records(&mut left, lr.clone(), 0);
        left.update_hints(0, lr.count(), 0);
        right.common.count = rr.len() as u16;
        self.copy_records(right, rr.clone(), 0);
        right.update_hints(0, rr.count(), 0);

        left.common.scan_counter = if lft {255} else if scan_counter == 255 {3} else {scan_counter};

        right.common.scan_counter = if rght {255} else if scan_counter == 255 {3} else {scan_counter};

        left.validate();
        right.validate();
        *self = left;
        Ok(())
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        self.remove::<BM::OlcEH>(k)
    }

    fn can_promote(&self, to: u8) -> Result<(), PromoteError> {
        match to {
            node_tag::FULLY_DENSE_LEAF => {

                let count = self.common.count as usize;
                if count == 0 {
                    return Err(Capacity);
                }

                if self.lower_fence().is_empty() {
                    return Err(PromoteError::Fences);
                }

                let first_key = self.key_combined(0);
                let first_val = self.heap_val(0);

                let key_len = first_key.len();
                let val_len = first_val.len();

                let mut key_error: bool = false;
                let mut val_error: bool = false;


                if key_len > 4 {
                    return Err(Keys);
                }


                for i in 0..count {
                    let key = self.key_combined(i);
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
                if key_error {
                    return Err(Keys);
                }
                if val_error {
                    return Err(ValueLen);
                }



                let mut min_suffix = u32::MAX;
                let mut max_suffix = 0;

                for i in 0..count {
                    let mut suffix = self.key_combined(i).to_vec();

                    let mut full_key = Vec::with_capacity(self.common.prefix_len as usize + suffix.len());
                    full_key.extend_from_slice(self.prefix());
                    full_key.append(&mut suffix);

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
            node_tag::HASH_LEAF => {

                let count = self.common.count;
                let bump = self.heap_info().bump;

                // approximated bump of hash_leaf (the keys are 4 byte longer because of the heads)
                // well, more like up to 4 bytes, because some keys might be less than 4 bytes long.
                // but I dont think this slight difference is worth the effort,
                // as they will just be promoted to FDLs then, probably.
                let new_bump = bump-(4*count);

                let slots = HashLeaf::slot_reservation(count as usize) * 2;
                let hashes = count;

                // the two is the "sorted" u16.
                let metadata = size_of::<CommonNodeHead>() + size_of::<HeapNodeInfo>() + 2;

                let total_req = metadata + slots + hashes as usize;

                if total_req >= new_bump as usize {
                    return Err(Capacity);
                }

                Ok(())
            },
            _ => Err(Node)
        }
    }


    fn promote(&mut self, to: u8) {
        match to {
            node_tag::FULLY_DENSE_LEAF => {
                let count = self.common.count as usize;
                let prefix_len = self.common.prefix_len as usize;

                let first_key = self.key_combined(0);
                let first_val = self.heap_val(0);
                let key_len = first_key.len();
                let val_len = first_val.len();

                let scan_counter = self.common.scan_counter;



                let mut fdl = FullyDenseLeaf::zeroed();
                fdl.init(self.lower_fence(), self.upper_fence_combined(), key_len+prefix_len, val_len)
                    .expect("FDL init_wrapper failed in promote()");



                for i in 0..count {
                    let suffix = self.key_combined(i);
                    let val = self.heap_val(i);

                    let mut full_key = Vec::with_capacity(self.common.prefix_len as usize + suffix.len());

                    full_key.extend_from_slice(self.prefix());
                    full_key.append(&mut suffix.to_vec());
                    fdl.force_insert::<BM::OlcEH>(full_key.as_slice(), val);
                }

                NodeStatic::<BM>::set_scan_counter(&mut fdl, scan_counter);
                *self.as_page_mut() = fdl.copy_page();
            },
            node_tag::HASH_LEAF => {
                let count = self.common.count as usize;

                let scan_counter = self.common.scan_counter;

                let mut hash_leaf = HashLeaf::zeroed();
                NodeStatic::<BM>::init(&mut hash_leaf, self.lower_fence(), self.upper_fence_combined(), None);


                for i in 0..count {
                    let suffix = self.key_combined(i);
                    let val = self.heap_val(i);

                    let mut full_key = Vec::with_capacity(self.common.prefix_len as usize + suffix.len());

                    full_key.extend_from_slice(self.prefix());
                    full_key.append(&mut suffix.to_vec());
                    let result = NodeStatic::<BM>::insert(&mut hash_leaf, full_key.as_slice(), val);
                    if result.is_err() {
                        panic!("promote: insert failed");
                    }
                }
                NodeStatic::<BM>::set_scan_counter(&mut hash_leaf, scan_counter);

                *self.as_page_mut() = hash_leaf.copy_page();
            }
            _=> unreachable!()
        }

    }

    fn scan_with_callback(
        &self,
        buffer: &mut [MaybeUninit<u8>; 512],
        start: Option<&[u8]>,
        callback: &mut dyn FnMut(&[u8], &[u8]) -> bool
    ) -> bool {

        let mut lf : usize = 0;

        match start {
            None => {},
            Some(key) => {
                
                let index = Self::find::<BM::OlcEH>(unsafe { OPtr::from_ref(self) }, key);
                lf = index.unwrap_or(0);
            }
        }

        let prefix = self.prefix();
        let prefix_len = prefix.len();
        prefix.write_to_uninit(&mut buffer[..prefix_len]);
        for i in lf..self.common.count as usize {
            let val = self.heap_val(i as usize);

            let suffix = self.key_combined(i as usize);

            let total_len = prefix_len + suffix.len();
            suffix.write_to_uninit(&mut buffer[prefix_len..total_len]);
            let full_key : &mut [u8] = unsafe {
                std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, total_len)
            };


            if callback(&full_key, val) {
                return true;
            }
        }

        false
    }

    fn qualifies_for_promote(&self) -> Option<u8> {
        debug_assert!(!<Self as NodeStatic<'bm, BM>>::IS_INNER);
        if self.common.scan_counter == 0 {
            Some(node_tag::HASH_LEAF)
        }
        else {
            None
        }
    }

    fn retry_later(&mut self) {
        self.common.scan_counter += 1;
    }

    fn get_node_tag(&self) -> u8 {
        self.common.tag
    }

    fn get_scan_counter(&self) -> u8 {
        self.common.scan_counter
    }
}

const HEAD_RESERVATION: usize = 16;

#[cfg(test)]
mod tests {
    use crate::basic_node::{BasicInner, BasicLeaf};
    use crate::hash_leaf::HashLeaf;
    use crate::key_source::SourceSlice;
    use crate::node::{
        page_id_from_bytes, page_id_to_bytes, NodeDynamic, NodeStatic, Page, ToFromPageExt, PAGE_ID_LEN,
    };
    use bytemuck::Zeroable;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use std::collections::HashSet;
    use umolc::{BufferManager, BufferManagerExt, BufferManagerGuard, OPtr, PageId, SimpleBm};
    use crate::fully_dense_leaf::FullyDenseLeaf;

    type BM<'a> = &'a SimpleBm<Page>;

    #[allow(clippy::unused_enumerate_index)]
    fn test_leaf<'bm, BM: BufferManager<'bm, Page = Page>, N: NodeStatic<'bm, BM>>() {
        let rng = &mut SmallRng::seed_from_u64(42);
        let keys = dev_utils::ascii_bin_generator(10..51);
        let mut keys: Vec<Vec<u8>> = (0..50).map(|i| keys(rng, i)).collect();
        keys.sort();
        keys.dedup();
        let mut page = Page::zeroed();
        let leaf = page.cast_mut::<N>();
        for (_k, keys) in dev_utils::subslices(&keys, 5).enumerate() {
            let kc = keys.len();
            leaf.init(keys[0].as_slice(), keys[kc - 1].as_slice(), None);
            // skip last key, equal to upper fence
            let insert_range = 0..kc - 1;
            let mut to_insert: Vec<&[u8]> = keys[insert_range.clone()].iter().map(|x| x.as_slice()).collect();
            let mut inserted = HashSet::new();
            for insert_phase in [true, false, true, true, false] {
                to_insert.shuffle(rng);
                // insert/remove
                for (_i, &k) in to_insert.iter().enumerate() {
                    if insert_phase {
                        match leaf.insert_leaf(k, k) {
                            Ok(None) => assert!(inserted.insert(k)),
                            Ok(Some(())) => assert!(!inserted.insert(k)),
                            Err(()) => (),
                        };
                    } else {
                        let in_leaf = leaf.leaf_remove(k).is_some();
                        if inserted.remove(k) {
                            assert!(in_leaf);
                        } else {
                            assert!(!in_leaf);
                        }
                    }
                }
                // lookup
                for (_i, k) in keys.iter().enumerate() {
                    let expected = Some(k).filter(|_| inserted.contains(k.as_slice()));
                    let actual = N::lookup_leaf(OPtr::from_mut(leaf), &k[..]).map(|v| v.load_slice_to_vec());
                    assert_eq!(expected, actual.as_ref());
                }
            }
        }
    }

    #[test]
    fn test_basic_leaf() {
        test_leaf::<&'static SimpleBm<Page>, BasicLeaf>()
    }

    #[test]
    fn test_fdl() {
        test_leaf::<& 'static SimpleBm<Page>, BasicLeaf>()
    }

    #[test]
    fn test_hash_leaf() {
        test_leaf::<&'static SimpleBm<Page>, HashLeaf>()
    }

    #[test]
    fn basic_inner_iter_debug() {
        inner_iter_debug::<&'static SimpleBm<Page>, BasicInner>();
    }

    fn inner_iter_debug<'bm, BM: BufferManager<'bm, Page = Page>, N: NodeStatic<'bm, BM>>() {
        let rng = &mut SmallRng::seed_from_u64(700);
        let mut page = Page::zeroed();
        let node = &mut page.cast_mut::<N>();
        let keys = dev_utils::alpha_generator(10..20);
        let mut keys: Vec<Vec<u8>> = (0..30).map(|i| keys(rng, i)).collect();
        keys.sort();
        keys.dedup();
        for keys in dev_utils::subslices(&keys, 5) {
            node.init(&*keys[0], &*keys[keys.len() - 1], Some(&[1; 5]));
            for (i, k) in keys[1..keys.len() - 1].iter().enumerate() {
                if node.insert_inner(k, PageId { x: i as u64 }).is_err() {
                    break;
                }
            }
            let (mut keys, vals): (Vec<_>, Vec<_>) =
                node.iter_children().map(|(k, v)| (k.to_vec(), page_id_to_bytes(v).to_vec())).unzip();
            keys.remove(0);
            let debug = node.to_debug();
            assert_eq!(keys, debug.keys);
            assert_eq!(vals, debug.values);
        }
    }

    fn split_merge<N>(ufb: u8, lower: Option<&[u8; 5]>, mut val: impl FnMut(u64) -> Vec<u8>)
    where
        for<'a> N: NodeStatic<'a, &'a SimpleBm<Page>>,
    {
        let bm: BM = &SimpleBm::new(3);
        let mut g1 = bm.alloc();
        let mut g2 = bm.alloc();
        <BasicInner as NodeStatic<BM>>::init(g2.cast_mut(), &[][..], &[][..], Some(&page_id_to_bytes(g1.page_id())));
        let n1 = g1.cast_mut::<N>();
        n1.init(&[0][..], &[ufb, 1][..], lower);
        for i in 0u64.. {
            if n1.insert(&i.to_be_bytes()[..], &val(i)).is_err() {
                break;
            }
        }
        let s1 = n1.to_debug();

        // the basic node does not use this logic, so it is fine to do this. The Key value was implemented here as part of the splitting mechanism for FDLs
        n1.split(bm, g2.as_dyn_node_mut(), (0 as usize).to_le_bytes().as_slice()).unwrap();
        g2.as_dyn_node_mut::<BM>().validate();
        let g2_debug = g2.as_dyn_node_mut::<BM>().to_debug();
        assert_eq!(g2_debug.keys.len(), 1);
        assert_eq!(g2_debug.values.len(), 2);
        let mut g3 = bm.lock_exclusive(page_id_from_bytes(&g2_debug.values[1][..].try_into().unwrap()));
        let n3 = g3.cast::<N>();
        assert_eq!(n1.upper_fence_combined().to_vec(), g2_debug.keys[0]);
        assert_eq!(g3.lower_fence().to_vec(), g2_debug.keys[0]);
        assert_eq!([n1.to_debug().values, n3.to_debug().values].concat(), s1.values);
        NodeDynamic::<BM>::merge(n1, &mut g3);
        let s2 = n1.to_debug();
        assert_eq!(s1, s2);
    }

    fn split_merge_leaf_val(i: u64) -> Vec<u8> {
        let mut v = i.to_be_bytes().to_vec();
        if i % 2 == 0 {
            v.push(42);
        }
        v
    }

    #[test]
    fn split_merge_hash_leaf() {
        split_merge::<HashLeaf>(1, None, split_merge_leaf_val);
        split_merge::<HashLeaf>(0, None, split_merge_leaf_val);
    }

    #[test]
    fn split_merge_basic_leaf() {
        split_merge::<BasicLeaf>(1, None, split_merge_leaf_val);
        split_merge::<BasicLeaf>(0, None, split_merge_leaf_val);
    }

    #[test]
    fn split_merge_inner() {
        let fake_pid = |i| page_id_to_bytes(PageId { x: i + 1024 }).to_vec();
        split_merge::<BasicInner>(1, Some(&[0; PAGE_ID_LEN]), fake_pid);
        split_merge::<BasicInner>(0, Some(&[0; PAGE_ID_LEN]), fake_pid);
    }
}

impl<V: NodeKind> HeapNode for BasicNode<V> {
    type KeyLength = BasicNodeKeyHeapLength;
    type ValLength = V::BasicValLength;

    fn slot_offset(&self) -> usize {
        Self::slot_offset(self.common.count as usize)
    }

    fn heap_info_mut(&mut self) -> &mut HeapNodeInfo {
        &mut self.heap
    }

    fn heap_info(&self) -> &HeapNodeInfo {
        &self.heap
    }

    fn validate(&self) {
        self.validate()
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(transparent)]
pub struct BasicNodeKeyHeapLength(u16);

impl HeapLength for BasicNodeKeyHeapLength {
    fn to_usize(self) -> usize {
        (self.0 as usize).saturating_sub(4)
    }

    fn from_slice(x: impl SourceSlice) -> Result<Self, HeapLengthError> {
        Ok(Self(x.len() as u16))
    }

    fn map_insert_slice<S: SourceSlice>(x: S) -> S {
        let head_len = x.len().min(4);
        x.slice_start(head_len)
    }
}
