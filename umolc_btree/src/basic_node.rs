use crate::impl_to_from_page;
use crate::key_source::{common_prefix, key_head, HeadSourceSlice, SourceSlice, SourceSlicePair};
use crate::node::{
    insert_upper_sibling, node_tag, page_cast_mut, page_id_from_bytes, page_id_from_olc_bytes, page_id_to_bytes,
    CommonNodeHead, KindInner, KindLeaf, NodeDynamic, NodeKind, NodeStatic, Page, ToFromPage, ToFromPageExt,
    PAGE_ID_LEN, PAGE_SIZE,
};
use crate::util::Supreme;
use bstr::{BStr, BString};
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use itertools::Itertools;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::{offset_of, size_of};
use std::ops::Range;
use umolc::{o_project, BufferManager, OPtr, OlcErrorHandler, PageId};

const HINT_COUNT: usize = 16;
const MIN_HINT_SPACING: usize = 3;

// must align with min hint spacing, so hints are updated when min count is reached
const MIN_HINT_COUNT: usize = MIN_HINT_SPACING * (HINT_COUNT + 1);

// Pod cannot be automatically implemented for generic structs, so we define a second non-generic version to get automatic checking
macro_rules! def_basic_node {
    {$($n:ident:$t:ty,)*}=>{
        #[derive(Copy, Clone, Zeroable)]
        #[repr(C, align(16))]
        pub struct BasicNode<V>{
            $($n:$t,)*
            _p:PhantomData<V>,
        }

        unsafe impl<V:Copy+Zeroable+'static> Pod for BasicNode<V>{}
        unsafe impl<V> ToFromPage for BasicNode<V>{}

        #[derive(Pod,Copy,Clone,Zeroable)]
        #[repr(C, align(16))]
        #[allow(dead_code)]
        pub struct AssertBasicNodePod{
            $($n:$t,)*
        }
        impl_to_from_page!{AssertBasicNodePod}
    }
}

def_basic_node! {
    common: CommonNodeHead,
    heap_bump: u16,
    heap_freed: u16,
    _pad: u16,
    hints: [u32; HINT_COUNT],
    _data: [u32; BASIC_NODE_DATA_SIZE],
}

pub type BasicLeaf = BasicNode<KindLeaf>;
pub type BasicInner = BasicNode<KindInner>;

const BASIC_NODE_DATA_SIZE: usize = (PAGE_SIZE - size_of::<CommonNodeHead>() - 2 * 2 - 16 * 4) / 4;

impl<V: NodeKind> BasicNode<V> {
    fn u16(&self, offset: usize) -> usize {
        assert!(offset + 2 <= size_of::<Self>());
        unsafe { (self as *const Self as *const u8).add(offset).cast::<u16>().read_unaligned() as usize }
    }

    fn store_u16(&mut self, offset: usize, x: usize) {
        assert!(offset + 2 <= size_of::<Self>());
        unsafe { (self as *mut Self as *mut u8).add(offset).cast::<u16>().write(x as u16) }
    }

    fn lower(&self) -> &[u8; PAGE_ID_LEN] {
        self.page_id_bytes(Self::LOWER_OFFSET)
    }

    fn page_id_bytes(&self, offset: usize) -> &[u8; PAGE_ID_LEN] {
        assert!(!V::IS_LEAF);
        self.cast_slice::<u8>()[offset..][..PAGE_ID_LEN].try_into().unwrap()
    }

    fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(HEAD_RESERVATION)
    }
    fn slot_offset(count: usize) -> usize {
        Self::HEAD_OFFSET + 4 * Self::reserved_head_count(count)
    }

    fn slot_end(count: usize) -> usize {
        Self::slot_offset(count) + 2 * count
    }

    fn set_slot(&mut self, index: usize, offset: usize) {
        debug_assert!(index < self.common.count as usize);
        self.cast_slice_mut::<u16>()[index] = offset as u16;
    }

    fn slots(&self) -> &[u16] {
        &self.cast_slice::<u16>()[Self::slot_offset(self.common.count as usize) / 2..][..self.common.count as usize]
    }

    fn key_combined(&self, index: usize) -> SourceSlicePair<u8, HeadSourceSlice, &[u8]> {
        let head = self.heads()[index];
        let offset = self.slots()[index] as usize;
        let len = self.u16(offset);
        let tail_len = len.saturating_sub(4);
        let head = HeadSourceSlice::from_head_len(head, len);
        let tail = self.slice(offset + Self::RECORD_TO_KEY_OFFSET, tail_len);
        head.join(tail)
    }

    fn record_val_len(&self, offset: usize) -> usize {
        if V::IS_LEAF {
            self.u16(offset + 2)
        } else {
            PAGE_ID_LEN
        }
    }

    fn stored_record_size(&self, offset: usize) -> usize {
        self.u16(offset).saturating_sub(4) + self.record_val_len(offset)
    }

    // fn key_tail_len(self, index: usize) -> (Guarded<'a, M, [u8]>, usize)
    // where
    //     Self: Copy,
    // {
    //     self.key_tail_len_with_slots(index, self.slots())
    // }

    fn key_tail(&self, index: usize) -> &[u8] {
        let offset = self.slots()[index] as usize;
        self.slice(offset + Self::RECORD_TO_KEY_OFFSET, self.u16(offset))
    }

    fn val(&self, index: usize) -> &[u8] {
        let offset = self.slots()[index] as usize;
        self.slice(offset + Self::RECORD_TO_KEY_OFFSET + self.u16(offset), self.record_val_len(offset))
    }

    fn find<O: OlcErrorHandler>(this: OPtr<Self, O>, key: &[u8]) -> Result<usize, usize>
    where
        Self: Copy,
    {
        let prefix_len = o_project!(this.common.prefix_len).r() as usize;
        if prefix_len > key.len() {
            O::optimistic_fail()
        }
        let truncated = &key[prefix_len..];
        Self::find_truncated::<O>(this, truncated)
    }

    fn find_truncated<O: OlcErrorHandler>(this: OPtr<Self, O>, truncated: &[u8]) -> Result<usize, usize>
    where
        Self: Copy,
    {
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
        &bytemuck::cast_slice(std::slice::from_ref(self))[Self::HEAD_OFFSET / 4..][..self.common.count as usize]
    }

    fn set_head(&mut self, i: usize, head: u32) {
        bytemuck::cast_slice_mut(std::slice::from_mut(self))[Self::HEAD_OFFSET / 4 + i] = head
    }
    fn copy_records(&self, dst: &mut Self, src_range: Range<usize>, dst_start: usize) {
        let dst_range = dst_start..(src_range.end + dst_start - src_range.start);
        let dpl = dst.common.prefix_len as usize;
        let spl = self.common.prefix_len as usize;
        let restore_prefix: &[u8] = if dpl < spl { &self.as_page().prefix()[dpl..] } else { &[][..] };
        let prefix_grow = dpl.saturating_sub(spl);
        for (src_i, dst_i) in src_range.clone().zip(dst_range.clone()) {
            let key = restore_prefix.join(self.key_combined(src_i).slice(prefix_grow..));
            dst.heap_write_new(key, self.val(src_i), dst_i);
            dst.set_head(dst_i, key_head(key));
        }
    }

    /// returns the number of keys in the low node and the separator (including prefix)
    fn find_separator(&self) -> (usize, impl SourceSlice<u8> + '_) {
        let count = self.common.count as usize;
        if V::IS_LEAF {
            let range_start = count / 2 - count / 8;
            let range_end = count / 2 + count / 8;
            let common_prefix = common_prefix(self.key_combined(range_start - 1), self.key_combined(range_end));
            let best_split = (range_start..=range_end)
                .filter(|&lc| {
                    self.key_combined(lc - 1).len() == common_prefix
                        || self.key_combined(lc - 1).index_ss(common_prefix)
                            != self.key_combined(lc).index_ss(common_prefix)
                })
                .min_by_key(|&lc| (lc as isize - count as isize / 2).abs())
                .unwrap();
            let sep = self.key_combined(best_split).slice(..common_prefix + 1);
            (best_split, self.as_page().prefix().join(sep))
        } else {
            let low_count = count / 2;
            let sep = self.as_page().prefix().join(self.key_combined(low_count));
            (low_count, sep)
        }
    }

    fn relocate_by<const UP: bool, T: Pod>(&mut self, offset: usize, count: usize, dist: usize) {
        assert_eq!(offset % size_of::<T>(), 0);
        let offset = offset / size_of::<T>();
        if UP {
            self.cast_slice_mut::<T>().copy_within(offset..offset + count, offset + dist);
        } else {
            self.cast_slice_mut::<T>().copy_within(offset..offset + count, offset - dist);
        }
    }
    fn heap_write_new(&mut self, key: impl SourceSlice, val: &[u8], write_slot: usize) {
        let key_len = key.len();
        let tail_offset = key.len().min(4);
        let key_tail = key.slice_start(tail_offset);
        let tail_len = key_len - tail_offset;
        let size = Self::record_size(tail_len, val.len());
        let offset = self.heap_bump as usize - size;
        self.store_u16(offset, key_len);
        if V::IS_LEAF {
            self.store_u16(offset + 2, val.len());
        }
        let key_offset = offset + Self::RECORD_TO_KEY_OFFSET;
        key_tail.write_to(self.slice_mut(key_offset, tail_len));
        self.slice_mut(key_offset + tail_len, val.len()).copy_from_slice(val);
        self.heap_bump = offset as u16;
        self.set_slot(write_slot, offset);
    }

    fn update_hints(&mut self, old_count: usize, new_count: usize, mut change_index: usize) {
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

    fn record_size(key_tail: usize, val: usize) -> usize {
        Self::RECORD_TO_KEY_OFFSET + key_tail + val
    }

    pub fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: Option<&[u8; 5]>) {
        if V::IS_LEAF {
            assert!(lower.is_none());
        } else {
            self.slice_mut(Self::LOWER_OFFSET, 5).copy_from_slice(lower.unwrap());
        }
        self.as_page_mut().common_init(if V::IS_LEAF { node_tag::BASIC_LEAF } else { node_tag::BASIC_INNER }, lf, uf);
        self.heap_freed = 0;
        self.heap_bump = size_of::<Self>() as u16 - self.common.lower_fence_len - self.common.upper_fence_len;
    }

    fn compactify(&mut self) {
        let buffer = &mut [0u8; PAGE_SIZE];
        let heap_end = self.fences_start();
        let dst_offset = heap_end;
        for i in 0..self.common.count as usize {
            let offset = self.slots()[i] as usize;
            let val_len = if V::IS_LEAF { self.u16(offset + 2) } else { PAGE_ID_LEN };
            let record_len = Self::RECORD_TO_KEY_OFFSET + self.u16(offset) + val_len;
            buffer[dst_offset..][..record_len].copy_from_slice(self.slice(offset, record_len));
            self.set_slot(i, dst_offset);
        }
        self.slice_mut(dst_offset, heap_end - dst_offset).copy_from_slice(&buffer[dst_offset..heap_end]);
        debug_assert_eq!(self.heap_bump as usize + self.heap_freed as usize, dst_offset);
        self.heap_freed = 0;
        self.heap_bump = dst_offset as u16;
        self.validate();
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
        let record_size_sum: usize = self
            .slots()
            .iter()
            .copied()
            .map(|offset| {
                let offset = offset as usize;
                self.u16(offset).saturating_sub(4) + self.record_val_len(offset) + Self::RECORD_TO_KEY_OFFSET
            })
            .sum();
        let calculated = (size_of::<Self>() - self.common.lower_fence_len as usize
            + self.common.upper_fence_len as usize)
            - record_size_sum;
        let tracked = self.heap_bump as usize + self.heap_freed as usize;
        assert_eq!(calculated, tracked);
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
        let Ok(index) = Self::find_truncated::<O>(OPtr::from_mut(self), key) else {
            return None;
        };
        self.heap_freed += self.stored_record_size(self.slots()[index] as usize) as u16;
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

    #[allow(clippy::result_unit_err)]
    fn insert<O: OlcErrorHandler>(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        self.validate();
        if !V::IS_LEAF {
            assert_eq!(val.len(), PAGE_ID_LEN);
        }
        let key = &key[self.common.count as usize..];
        let index = Self::find_truncated::<O>(OPtr::from_mut(self), key);
        let count = self.common.count as usize;
        let record_size = Self::record_size(key.len().saturating_sub(4), val.len());
        loop {
            let new_heap_start;
            match index {
                Ok(existing) => {
                    new_heap_start = Self::slot_end(count);
                    if record_size <= (self.heap_bump as usize - new_heap_start) {
                        self.heap_freed += self.stored_record_size(self.slots()[existing] as usize) as u16;
                        self.heap_write_new(key, val, existing);
                        self.validate();
                    }
                }
                Err(insert_at) => {
                    new_heap_start = Self::slot_end(count + 1);
                    if new_heap_start + record_size <= self.heap_bump as usize {
                        let orhc = Self::reserved_head_count(count);
                        let nrhc = Self::reserved_head_count(count + 1);
                        if nrhc == orhc {
                            self.relocate_by::<true, u16>(
                                Self::HEAD_OFFSET + nrhc * 4 + insert_at * 2,
                                count - insert_at,
                                1,
                            );
                        } else {
                            self.relocate_by::<true, u16>(
                                Self::HEAD_OFFSET + orhc * 4 + insert_at * 2,
                                count - insert_at,
                                HEAD_RESERVATION * 2 + 1,
                            );
                            self.relocate_by::<true, u16>(
                                Self::HEAD_OFFSET + orhc * 4,
                                insert_at,
                                HEAD_RESERVATION * 2,
                            );
                        }
                        self.relocate_by::<true, u32>(Self::HEAD_OFFSET + 4 * insert_at, count - insert_at, 1);
                        self.common.count += 1;
                        self.set_head(insert_at, key_head(key));
                        self.update_hints(count, count + 1, insert_at);
                        self.heap_write_new(key, val, insert_at);
                        self.validate();
                        return Ok(None);
                    }
                }
            }
            if self.heap_bump as usize + (self.heap_freed as usize) < new_heap_start + record_size {
                self.validate();
                return Err(());
            }
            self.compactify();
        }
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
        fields!(self => heap_bump, heap_freed);
        s.field("lf", &BStr::new(self.lower_fence()));
        s.field("uf", &BString::new(self.upper_fence_combined().to_vec()));
        let records_fmt = (0..self.common.count as usize).format_with(",\n", |i, f| {
            let offset = self.slots()[i] as usize;
            let val: &dyn Debug =
                if V::IS_LEAF { &BStr::new(self.val(i)) } else { &page_id_from_bytes(self.val(i).try_into().unwrap()) };
            let head = self.heads()[i];
            let kl = self.u16(offset);
            let key = BStr::new(self.key_tail(i));
            f(&mut format_args!("{i:4}:{offset:04x}->[0x{head:08x}][{kl:3}] {key:?} -> {val:?}"))
        });
        s.field("records", &format_args!("\n{}", records_fmt));
        s.finish()
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>, V: NodeKind> NodeStatic<'bm, BM> for BasicNode<V> {
    const TAG: u8 = if V::IS_LEAF { 251 } else { 250 };
    const IS_INNER: bool = !V::IS_LEAF;
    type TruncatedKey<'a> = SourceSlicePair<u8, HeadSourceSlice, &'a [u8]>;

    fn iter_children(&self) -> impl Iterator<Item = (Self::TruncatedKey<'_>, PageId)> {
        assert!(<Self as NodeStatic<'bm, BM>>::IS_INNER);
        let lower = std::iter::once((Default::default(), Self::LOWER_OFFSET));
        let rest = (0..self.common.count as usize).map(|i| (self.key_combined(i), self.slots()[i] as usize));
        lower.chain(rest).map(|(k, o)| (k, page_id_from_bytes(self.page_id_bytes(o))))
    }

    fn lookup_leaf<'a>(this: OPtr<'a, Self, BM::OlcEH>, key: &[u8]) -> Option<OPtr<'a, [u8], BM::OlcEH>> {
        assert!(V::IS_LEAF);
        let index = Self::find(this, key).ok()?;
        let slot_offset = Self::slot_offset(o_project!(this.common.count).r() as usize);
        let offset = this.as_slice::<u16>().i(slot_offset / 2 + index - 1).r() as usize;
        let k_len = this.read_unaligned_nonatomic_u16(offset);
        let v_len = this.read_unaligned_nonatomic_u16(offset + 2);
        Some(this.as_slice().sub(offset + Self::RECORD_TO_KEY_OFFSET + k_len, v_len))
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
            this.read_unaligned_nonatomic_u16(offset) + Self::RECORD_TO_KEY_OFFSET
        };
        page_id_from_olc_bytes(this.array_slice(lower_offset))
    }
}

impl<'bm, BM: BufferManager<'bm, Page = Page>, V: NodeKind> NodeDynamic<'bm, BM> for BasicNode<V> {
    fn validate(&self) {
        self.validate();
    }
    #[allow(clippy::result_unit_err)]
    fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()> {
        let x = self.insert::<BM::OlcEH>(key, &page_id_to_bytes(pid));
        self.validate();
        x.map(|x| debug_assert!(x.is_none()))
    }

    fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        let ret = self.insert::<BM::OlcEH>(key, val);
        self.validate();
        ret
    }

    fn merge(&mut self, right: &mut Page) {
        debug_assert!(right.common.tag == Self::TAG);
        let right = page_cast_mut::<Page, Self>(right);
        let tmp = &mut BasicNode::<V>::zeroed();
        let left_count = self.common.count as usize;
        let right_count = right.common.count as usize;
        if V::IS_LEAF {
            tmp.init(self.lower_fence(), right.upper_fence_combined(), None);
            tmp.common.count = (left_count + right_count) as u16;
            self.copy_records(tmp, 0..left_count, 0);
            right.copy_records(tmp, 0..right_count, left_count);
        } else {
            tmp.init(self.lower_fence(), self.upper_fence_combined(), Some(self.lower()));
            tmp.common.count = (left_count + right_count + 1) as u16;
            self.copy_records(tmp, 0..left_count, 0);
            right.copy_records(tmp, 0..right_count, left_count + 1);
            tmp.heap_write_new(
                self.as_page().upper_fence_combined().slice_start(tmp.common.prefix_len as usize),
                right.lower().as_slice(),
                left_count,
            );
        }
        tmp.update_hints(0, tmp.common.count as usize, 0);
        *self = *tmp;
    }

    fn split<'g>(&mut self, bm: BM, parent: &mut dyn NodeDynamic<'bm, BM>) -> Result<(), ()> {
        let left = &mut BasicNode::<V>::zeroed();
        let count = self.common.count as usize;
        let (low_count, sep_key) = self.find_separator();
        let mut right = insert_upper_sibling(parent, bm, sep_key)?;
        let right = page_cast_mut::<_, BasicNode<V>>(&mut *right);
        self.validate();
        let (lr, rr) = if V::IS_LEAF {
            left.init(self.lower_fence(), sep_key, None);
            right.init(sep_key, self.upper_fence_combined(), None);
            (0..low_count, low_count..count)
        } else {
            left.init(self.as_page().lower_fence(), sep_key, Some(self.lower()));
            let mid_child = self.val(low_count).try_into().unwrap();
            right.init(sep_key, self.as_page().upper_fence_combined(), Some(mid_child));
            (0..low_count, low_count + 1..count)
        };
        debug_assert!(self.key_combined(lr.end - 1).cmp(sep_key.slice(self.common.prefix_len as usize..)).is_lt());
        debug_assert!(sep_key.slice(self.common.prefix_len as usize..).cmp(self.key_combined(rr.start)).is_le());
        left.common.count = lr.len() as u16;
        self.copy_records(left, lr.clone(), 0);
        left.update_hints(0, lr.count(), 0);
        right.common.count = rr.len() as u16;
        self.copy_records(right, rr.clone(), 0);
        right.update_hints(0, rr.count(), 0);
        left.validate();
        right.validate();
        *self = *left;
        Ok(())
    }

    fn to_debug_kv(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let range = 0..self.common.count as usize;
        let keys = range.clone().map(|i| self.key_combined(i).to_vec()).collect();
        let vals = (0..1)
            .filter(|_| !V::IS_LEAF)
            .map(|_| self.lower().to_vec())
            .chain(range.map(|i| self.val(i).to_vec()))
            .collect();
        (keys, vals)
    }

    fn leaf_remove(&mut self, k: &[u8]) -> Option<()> {
        self.remove::<BM::OlcEH>(k)
    }
}

const HEAD_RESERVATION: usize = 16;

#[cfg(test)]
mod tests {
    use crate::basic_node::{BasicNode, NodeKind};
    use crate::node::{page_id_to_bytes, KindInner, KindLeaf, NodeStatic, Page, PAGE_ID_LEN};
    use bytemuck::Zeroable;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use std::collections::HashSet;
    use umolc::{OPtr, PageId, PanicOlcEh, SimpleBm};
    type BM<'a> = &'a SimpleBm<Page>;

    #[test]
    #[allow(clippy::unused_enumerate_index)]
    fn leaf() {
        let bm: BM = &SimpleBm::new(1);
        let rng = &mut SmallRng::seed_from_u64(42);
        let keys = dev_utils::ascii_bin_generator(10..51);
        let mut keys: Vec<Vec<u8>> = (0..50).map(|i| keys(rng, i)).collect();
        keys.sort();
        keys.dedup();
        let leaf = &mut BasicNode::<KindLeaf>::zeroed();
        for (_k, keys) in dev_utils::subslices(&keys, 5).enumerate() {
            let kc = keys.len();
            leaf.init(keys[1].as_slice(), keys[kc - 2].as_slice(), None);
            let insert_range = 2..kc - 2;
            let mut to_insert: Vec<&[u8]> = keys[insert_range.clone()].iter().map(|x| x.as_slice()).collect();
            let mut inserted = HashSet::new();
            for p in 0..=3 {
                to_insert.shuffle(rng);
                for (_i, &k) in to_insert.iter().enumerate() {
                    if p != 2 {
                        if leaf.insert::<PanicOlcEh>(k, k).is_ok() {
                            inserted.insert(k);
                        }
                    } else {
                        let in_leaf = leaf.remove::<PanicOlcEh>(k).is_some();
                        let expected = inserted.remove(k);
                        assert_eq!(in_leaf, expected);
                    }
                }
                for (_i, k) in keys.iter().enumerate() {
                    let expected = Some(k).filter(|_| inserted.contains(k.as_slice()));
                    let actual = <BasicNode<KindLeaf> as NodeStatic<BM>>::lookup_leaf(OPtr::from_mut(leaf), &k[..])
                        .map(|v| v.load_slice_to_vec());
                    assert_eq!(expected, actual.as_ref());
                }
            }
        }
    }

    fn split_merge<V: NodeKind>(_ufb: u8, _lower: [u8; PAGE_ID_LEN], _val: impl FnMut(u64) -> Vec<u8>) {
        todo!()
    }

    #[test]
    fn split_merge_leaf() {
        let val = |i: u64| i.to_be_bytes().to_vec();
        split_merge::<KindLeaf>(1, [0; PAGE_ID_LEN], val);
        split_merge::<KindLeaf>(0, [0; PAGE_ID_LEN], val);
    }

    #[test]
    fn split_merge_inner() {
        let fake_pid = |i| page_id_to_bytes(PageId { x: i + 1024 }).to_vec();
        split_merge::<KindInner>(1, [0; PAGE_ID_LEN], fake_pid);
        split_merge::<KindInner>(0, [0; PAGE_ID_LEN], fake_pid);
    }
}
