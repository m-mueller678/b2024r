use crate::key_source::{common_prefix, key_head, SourceSlice};
use crate::node::{node_tag, CommonNodeHead, KindInner, KindLeaf, Node, NodeKind, ParentInserter};
use crate::page::{page_id_from_3x16, page_id_to_3x16, PageId, PageTail, PAGE_TAIL_SIZE};
use crate::tree::Supreme;
use crate::{MAX_KEY_SIZE, W};
use bstr::{BStr, BString};
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use itertools::Itertools;
use seqlock::{
    BmExt, BufferManager, Exclusive, Guarded, Optimistic, SeqLockMode, SeqLockWrappable, SeqlockAccessors, Shared,
    Wrapper,
};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::{align_of, offset_of, size_of, swap};
use std::ops::Range;
use std::ptr::addr_of_mut;

pub type BasicLeaf = BasicNode<KindLeaf>;
pub type BasicInner = BasicNode<KindInner>;

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(align(8))]
#[repr(C)]
pub struct BasicNodeData {
    common: CommonNodeHead,
    heap_bump: u16,
    heap_freed: u16,
    _pad: u16,
    hints: [u32; 16],
    _data: [u32; BASIC_NODE_DATA_SIZE],
}

const BASIC_NODE_DATA_SIZE: usize = (PAGE_TAIL_SIZE - size_of::<CommonNodeHead>() - 2 * 2 - 16 * 4) / 4;

#[repr(transparent)]
#[derive(Clone, Copy, Zeroable, SeqlockAccessors)]
#[seq_lock_wrapper(W)]
#[seq_lock_accessor(pub tag: u8 = 0.common.tag)]
#[seq_lock_accessor(pub prefix_len: u16 = 0.common.prefix_len)]
#[seq_lock_accessor(pub count: u16 = 0.common.count)]
#[seq_lock_accessor(lower_fence_len: u16 = 0.common.lower_fence_len)]
#[seq_lock_accessor(upper_fence_len: u16 = 0.common.upper_fence_len)]
#[seq_lock_accessor(heap_bump: u16 = 0.heap_bump)]
#[seq_lock_accessor(heap_freed: u16 = 0.heap_freed)]
pub struct BasicNode<V: NodeKind>(
    #[seq_lock_skip_accessor] BasicNodeData,
    #[seq_lock_skip_accessor] PhantomData<V::SliceType>,
);

unsafe impl<V: NodeKind> Pod for BasicNode<V> {}

fn copy_records<V: NodeKind>(
    src: W<Guarded<Shared, BasicNode<V>>>,
    dst: &mut W<Guarded<Exclusive, BasicNode<V>>>,
    src_range: Range<usize>,
    dst_start: usize,
    ref_key: &[u8],
) {
    let dst_range = dst_start..(src_range.end + dst_start - src_range.start);
    let dpl = dst.prefix_len().load() as usize;
    let spl = src.prefix_len().load() as usize;
    let restore_prefix = if dpl < spl { &ref_key[dpl..spl] } else { &[][..] };
    let prefix_grow = if dpl > spl { dpl - spl } else { 0 };
    for (src_i, dst_i) in src_range.clone().zip(dst_range.clone()) {
        let key = restore_prefix.join(src.key(src_i).slice(prefix_grow..));
        dst.heap_write_new(key, src.val(src_i), dst_i);
    }
    if dpl == spl {
        src.heads().slice(src_range).copy_to(&mut dst.b().heads().slice(dst_range));
    } else {
        for dst_i in dst_range {
            let head = key_head(dst.s().key(dst_i));
            dst.b().heads().index(dst_i).store(head);
        }
    }
}

unsafe impl<V: NodeKind> Node for BasicNode<V> {
    fn validate(this: W<Guarded<'_, Shared, Self>>) {
        if !cfg!(feature = "validate_node") {
            return;
        }
        let record_size_sum: usize = this.s().slots().iter().map(|x| this.stored_record_size(x.load() as usize)).sum();
        let calculated = this.heap_end() - record_size_sum;
        let tracked = this.heap_bump().load() as usize + this.heap_freed().load() as usize;
        assert_eq!(calculated, tracked);
        let keys_and_fences = std::iter::once(this.lower_fence().slice(this.prefix_len().load() as usize..))
            .chain((0..this.count().load() as usize).map(|i| this.key(i)))
            .map(|k| Supreme::X(k.load_slice_to_vec()))
            .chain(std::iter::once(if this.upper_fence_len().load() == 0 {
                Supreme::Sup
            } else {
                Supreme::X(this.upper_fence().slice(this.prefix_len().load() as usize..).load_slice_to_vec())
            }));
        assert!(keys_and_fences.is_sorted(), "not sorted: {:?}", this.upcast());
    }
    fn format(this: &W<Guarded<Optimistic, Self>>, f: &mut Formatter) -> std::fmt::Result {
        let mut s = f.debug_struct(std::any::type_name::<Self>());
        macro_rules! field {
            ($($f:ident,)*) => {$(s.field(std::stringify!($f),&this.$f().load());)*};
        }
        field!(count, lower_fence_len, upper_fence_len, prefix_len, heap_bump, heap_freed,);
        s.field("lf", &BString::new(this.s().lower_fence().cast_slice::<u8>().load_slice_to_vec()));
        s.field("uf", &BString::new(this.s().upper_fence().cast_slice::<u8>().load_slice_to_vec()));
        let records_fmt = (0..this.count().load() as usize).format_with(",\n", |i, f| {
            let offset = this.s().slots().index(i).load() as usize;
            let val: &dyn Debug = if V::IS_LEAF {
                &BString::new(this.s().val(i).cast_slice::<u8>().load_slice_to_vec())
            } else {
                &page_id_from_3x16(this.s().val(i).as_array::<3>().cast::<[u16; 3]>().load())
            };
            let head = this.s().heads().index(i).load();
            let kl = this.s().u16(offset).load();
            let key = BString::new(this.s().key(i).load_slice_to_vec());
            f(&mut format_args!("{i:4}:{offset:04x}->[0x{head:08x}][{kl:3}] {key:?} -> {val:?}"))
        });
        s.field("records", &format_args!("\n{}", records_fmt));
        s.finish()
    }
    fn merge(
        this: &mut W<Guarded<Exclusive, BasicNode<V>>>,
        right: &mut W<Guarded<Exclusive, BasicNode<V>>>,
        ref_key: &[u8],
    ) {
        let tmp = &mut BasicNode::<V>::zeroed();
        let mut tmp = Guarded::<Exclusive, _>::wrap_mut(tmp);
        let left_count = this.count().load() as usize;
        let right_count = right.count().load() as usize;
        if V::IS_LEAF {
            tmp.init(this.s().lower_fence(), right.s().upper_fence(), Zeroable::zeroed());
            tmp.count_mut().store((left_count + right_count) as u16);
            copy_records(this.s(), &mut tmp, 0..left_count, 0, ref_key);
            copy_records(right.s(), &mut tmp, 0..right_count, left_count, ref_key);
        } else {
            tmp.init(this.s().lower_fence(), right.s().upper_fence(), this.s().lower().get().load());
            tmp.count_mut().store((left_count + right_count + 1) as u16);
            copy_records(this.s(), &mut tmp, 0..left_count, 0, ref_key);
            copy_records(right.s(), &mut tmp, 0..right_count, left_count + 1, ref_key);
            tmp.heap_write_new(
                this.s().upper_fence().slice(tmp.prefix_len().load() as usize..),
                right.s().lower().as_slice(),
                left_count,
            );
        }
        this.store(tmp.load()); //TODO optimize copy
    }

    /// returns the number of keys in the low node and the separator
    fn find_separator<'a>(this: &'a W<Guarded<'a, Shared, Self>>, ref_key: &'a [u8]) -> (usize, impl SourceSlice + 'a) {
        let prefix_len = this.prefix_len().load() as usize;
        let count = this.count().load() as usize;
        if V::IS_LEAF {
            let range_start = count / 2 - count / 8;
            let range_end = count / 2 + count / 8;
            let common_prefix = common_prefix(this.key(range_start - 1), this.key(range_end));
            let best_split = (range_start..=range_end)
                .filter(|&lc| {
                    this.key(lc - 1).len() == common_prefix
                        || this.key(lc - 1).index(common_prefix).load() != this.key(lc).index(common_prefix).load()
                })
                .min_by_key(|&lc| (lc as isize - count as isize / 2).abs())
                .unwrap();
            let sep = this.key(best_split).slice(..common_prefix + 1);
            (best_split, ref_key[..prefix_len].join(sep))
        } else {
            let low_count = count / 2;
            let sep = ref_key[..prefix_len].join(this.s().key(low_count));
            (low_count, sep)
        }
    }

    fn split<'g, 'bm, BM: BufferManager<'bm>>(
        this: &mut W<Guarded<'g, Exclusive, Self>>,
        parent_insert: impl ParentInserter<'bm, BM>,
        ref_key: &[u8],
    ) -> Result<(), ()> {
        let left = &mut BasicNode::<V>::zeroed();
        let mut left = Guarded::<Exclusive, _>::wrap_mut(left);
        let count = this.count().load() as usize;
        let this_mut = this;
        let this = this_mut.s();
        let (low_count, sep_key) = Self::find_separator(&this, ref_key);
        let mut right = parent_insert.insert_upper_sibling(sep_key)?;
        let right = &mut right.b().0.cast::<BasicNode<V>>();
        Node::validate(this.s());
        let (lr, rr) = if V::IS_LEAF {
            left.init(this.s().lower_fence(), sep_key, Zeroable::zeroed());
            right.init(sep_key, this.s().upper_fence(), Zeroable::zeroed());
            (0..low_count, low_count..count)
        } else {
            left.init(this.lower_fence(), sep_key, this.s().lower().get().load());
            let mid_child = this
                .s()
                .slice::<[V::SliceType; 3]>(this.s().slots().index(low_count).load() as usize + 2, 1)
                .index(0)
                .get()
                .load();
            right.init(sep_key, this.upper_fence(), mid_child);
            (0..low_count, low_count + 1..count)
        };
        debug_assert!(this.key(lr.end - 1).cmp(sep_key.slice(this.prefix_len().load() as usize..)).is_lt());
        debug_assert!(sep_key.slice(this.prefix_len().load() as usize..).cmp(this.key(rr.start)).is_le());
        left.count_mut().store(lr.len() as u16);
        copy_records(this.s(), &mut left, lr, 0, ref_key);
        right.count_mut().store(rr.len() as u16);
        copy_records(this.s(), right, rr, 0, ref_key);
        Node::validate(left.s());
        Node::validate(right.s());
        this_mut.store(left.load()); //TODO optimize copy
        Ok(())
    }

    const TAG: u8 = if V::IS_LEAF { 251 } else { 250 };

    type DebugVal = V::DebugVal;

    fn to_debug_kv(this: W<Guarded<Shared, Self>>) -> (Vec<Vec<u8>>, Vec<Self::DebugVal>) {
        let range = 0..this.count().load() as usize;
        let keys = range.clone().map(|i| this.key(i).load_slice_to_vec()).collect();
        let vals = (0..1)
            .filter(|_| !V::IS_LEAF)
            .map(|_| this.lower().as_slice().load_slice_to_vec())
            .chain(range.map(|i| this.val(i).load_slice_to_vec()))
            .map(V::to_debug)
            .collect();
        (keys, vals)
    }
}

const HEAD_RESERVATION: usize = 16;

const fn lower_head_slots<V: NodeKind>() -> usize {
    if V::IS_LEAF {
        0
    } else {
        2
    }
}

fn record_to_key_offset<V: NodeKind>() -> usize {
    if V::IS_LEAF {
        4
    } else {
        8
    }
}

impl<'a, V: NodeKind> W<Guarded<'a, Exclusive, BasicNode<V>>> {
    /// offset is in units of bytes, others in units of T
    fn relocate_by<const UP: bool, T: SeqLockWrappable + Pod>(&mut self, offset: usize, count: usize, dist: usize) {
        assert_eq!(offset % size_of::<T>(), 0);
        let offset = offset / size_of::<T>();
        self.b().as_bytes().cast_slice::<T>().move_within_by::<UP>(offset..offset + count, dist);
    }
    fn heap_write_new(&mut self, key: impl SourceSlice, val: impl SourceSlice<V::SliceType>, write_slot: usize) {
        let size = Self::record_size(key.len(), val.len());
        let offset = self.heap_bump().load() as usize - size;
        self.heap_write_record(key, val, offset);
        self.heap_bump_mut().store(offset as u16);
        self.b().slots().index(write_slot).store(offset as u16);
    }

    pub fn heap_write_record(&mut self, key: impl SourceSlice, val: impl SourceSlice<V::SliceType>, offset: usize) {
        let len = key.len();
        self.b().u16(offset).store(len as u16);
        if V::IS_LEAF {
            self.b().u16(offset + 2).store(val.len() as u16);
        }
        if !V::IS_LEAF {
            val.write_to(&mut self.b().slice(offset + 2, 3));
        }
        key.write_to(&mut self.b().slice(offset + record_to_key_offset::<V>(), len));
        if V::IS_LEAF {
            val.write_to(&mut self.b().slice(offset + record_to_key_offset::<V>() + len, val.len()));
        }
    }

    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], val: &[V::SliceType]) -> Result<Option<()>, ()> {
        Node::validate(self.s());
        if !V::IS_LEAF {
            assert_eq!(val.len(), 3);
        }
        let key = &key[self.prefix_len().load() as usize..];
        let index = self.s().find_truncated(key);
        let count = self.count().load() as usize;
        loop {
            let new_heap_start;
            match index {
                Ok(existing) => {
                    new_heap_start = Self::HEAD_OFFSET + Self::reserved_head_count(count) * 4 + count * 2;
                    if Self::record_size(key.len(), val.len()) <= (self.heap_bump().load() as usize - new_heap_start) {
                        let old_size = self.s().stored_record_size(self.s().slots().index(existing).load() as usize);
                        self.heap_freed_mut().update(|x| x + old_size as u16);
                        self.heap_write_new(key, val, existing);
                        Node::validate(self.s());
                        return Ok(Some(()));
                    }
                }
                Err(insert_at) => {
                    new_heap_start = Self::HEAD_OFFSET + Self::reserved_head_count(count + 1) * 4 + (count + 1) * 2;
                    if new_heap_start + Self::record_size(key.len(), val.len()) <= self.heap_bump().load() as usize {
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
                        self.count_mut().store(count as u16 + 1);
                        self.b().heads().index(insert_at).store(key_head(key));
                        self.heap_write_new(key, val, insert_at);
                        Node::validate(self.s());
                        return Ok(None);
                    }
                }
            }
            if self.heap_bump().load() as usize + (self.s().heap_freed().load() as usize)
                < new_heap_start + Self::record_size(key.len(), val.len())
            {
                Node::validate(self.s());
                return Err(());
            }
            self.compactify();
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        let Ok(index) = self.s().find(key) else {
            return None;
        };
        let offset = self.s().slots().index(index).load() as usize;
        let record_size = self.s().stored_record_size(offset);
        self.heap_freed_mut().update(|x| x + record_size as u16);
        let count = self.count().load() as usize;
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
        self.count_mut().store((count - 1) as u16);
        Node::validate(self.s());
        Some(())
    }

    fn record_size(key: usize, val: usize) -> usize {
        if V::IS_LEAF {
            4 + Self::round_up(key + val)
        } else {
            8 + Self::round_up(key)
        }
    }

    pub fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: [V::SliceType; 3]) {
        self.b().common_head().tag_mut().store(if V::IS_LEAF { node_tag::BASIC_LEAF } else { node_tag::BASIC_INNER });
        assert_eq!(size_of::<BasicNode<V>>(), PAGE_TAIL_SIZE);
        self.count_mut().store(0);
        self.prefix_len_mut().store(common_prefix(lf, uf) as u16);
        self.heap_freed_mut().store(0);
        self.lower_fence_len_mut().store(lf.len() as u16);
        self.upper_fence_len_mut().store(uf.len() as u16);
        let heap_end = self.s().heap_end() as u16;
        self.heap_bump_mut().store(heap_end);
        if !V::IS_LEAF {
            self.b().lower().get_mut().store(lower);
        }
        lf.write_to(&mut self.b().lower_fence());
        uf.write_to(&mut self.b().upper_fence());
    }

    fn compactify(&mut self) {
        let buffer = &mut [0u8; size_of::<BasicNodeData>()];
        let heap_end = self.s().heap_end();
        let mut dst_offset = heap_end;
        for i in 0..self.count().load() as usize {
            let offset = self.s().slots().index(i).load() as usize;
            let lens = self.s().slice::<u16>(offset, 2);
            let record_len = record_to_key_offset::<V>()
                + Self::round_up(
                    lens.index(0).load() as usize + if V::IS_LEAF { lens.index(1).load() as usize } else { 0 },
                );
            dst_offset -= record_len;
            self.s().slice(offset, record_len).load_slice(&mut buffer[dst_offset..][..record_len]);
            self.b().slots().index(i).store(dst_offset as u16);
        }
        self.b().slice::<u8>(dst_offset, heap_end - dst_offset).store_slice(&buffer[dst_offset..heap_end]);
        debug_assert_eq!(self.heap_bump().load() + self.heap_freed().load(), dst_offset as u16);
        self.heap_freed_mut().store(0);
        self.heap_bump_mut().store(dst_offset as u16);
        Node::validate(self.s());
    }
}

impl<'a, V: NodeKind, M: SeqLockMode> W<Guarded<'a, M, BasicNode<V>>> {
    fn stored_record_size(self, offset: usize) -> usize
    where
        Self: Copy,
    {
        Self::round_up(
            self.u16(offset).load() as usize
                + if V::IS_LEAF { 4 + self.s().u16(offset + 2).load() as usize } else { 8 },
        )
    }

    fn heap_end(self) -> usize
    where
        Self: Copy,
    {
        size_of::<BasicNodeData>()
            - Self::round_up(self.lower_fence_len().load() as usize + self.upper_fence_len().load() as usize)
    }
    fn u16(self, offset: usize) -> Guarded<'a, M, u16> {
        self.slice::<u16>(offset, 1).index(0)
    }

    fn lower(self) -> Guarded<'a, M, [V::SliceType; 3]> {
        assert!(!V::IS_LEAF);
        assert_eq!(4 % align_of::<[V::SliceType; 3]>(), 0);
        unsafe { self.0.map_ptr(|x| addr_of_mut!((*x).0._data) as *mut [V::SliceType; 3]) }
    }
    fn round_up(x: usize) -> usize {
        x + (x % 2)
    }
    const HEAD_OFFSET: usize = offset_of!(BasicNodeData, _data) + lower_head_slots::<V>() * 4;

    fn heads(self) -> Guarded<'a, M, [u32]> {
        let count = self.count().load() as usize;
        self.slice(Self::HEAD_OFFSET, count)
    }

    fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(HEAD_RESERVATION)
    }
    fn slots(self) -> Guarded<'a, M, [u16]> {
        let count = self.count().load() as usize;
        let i = Self::reserved_head_count(count);
        self.slice(Self::HEAD_OFFSET + 4 * i, count)
    }

    fn key(self, index: usize) -> Guarded<'a, M, [u8]> {
        let offset = self.s().slots().index(index).load() as usize;
        let len = self.s().u16(offset).load();
        self.slice(offset + record_to_key_offset::<V>(), len as usize)
    }

    fn val(self, index: usize) -> Guarded<'a, M, [V::SliceType]> {
        let offset = self.s().slots().index(index).load() as usize;
        if V::IS_LEAF {
            let key_len = self.s().u16(offset).load() as usize;
            let val_len = self.s().u16(offset + 2).load() as usize;
            self.slice(offset + record_to_key_offset::<V>() + key_len, val_len)
        } else {
            self.slice(offset + 2, 3)
        }
    }

    fn find(self, key: &[u8]) -> Result<usize, usize>
    where
        Self: Copy,
    {
        let prefix_len = self.prefix_len().load() as usize;
        if prefix_len > key.len() {
            M::release_error();
        }
        let truncated = &key[prefix_len..];
        self.find_truncated(truncated)
    }

    fn find_truncated(self, truncated: &[u8]) -> Result<usize, usize>
    where
        Self: Copy,
    {
        let needle_head = key_head(truncated);
        let heads = self.heads();
        if heads.is_empty() {
            return Err(0);
        }
        let matching_head_range = (0..=heads.len() - 1).binary_all(|i| heads.s().index(i).load().cmp(&needle_head));
        if matching_head_range.is_empty() {
            return Err(matching_head_range.start);
        }
        let slots = self.slots();
        if slots.len() != heads.len() {
            M::release_error()
        }
        let key_position = (matching_head_range.start..=matching_head_range.end - 1).binary_by(|i| {
            let key = self.key(i);
            key.mem_cmp(truncated)
        });
        key_position
    }
}

impl<'a, M: SeqLockMode> W<Guarded<'a, M, BasicNode<KindLeaf>>> {
    pub fn lookup_leaf(self, key: &[u8]) -> Option<Guarded<'a, M, [u8]>>
    where
        Self: Copy,
    {
        if let Ok(i) = self.find(key) {
            let offset = self.slots().index(i).load() as usize;
            let val_start = self.u16(offset).load() as usize + offset + record_to_key_offset::<KindLeaf>();
            let val_len = self.u16(offset + 2).load() as usize;
            Some(self.slice(val_start, val_len))
        } else {
            None
        }
    }
}

impl<'a> W<Guarded<'a, Exclusive, BasicNode<KindLeaf>>> {
    #[allow(clippy::result_unit_err)]
    pub fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        Node::validate(self.s());
        let x = self.insert(key, val);
        Node::validate(self.s());
        x
    }
}

impl<'a> W<Guarded<'a, Optimistic, BasicNode<KindInner>>> {
    pub fn lookup_inner(&self, key: &[u8], high_on_equal: bool) -> u64 {
        let index = match self.find(key) {
            Err(i) => i,
            Ok(i) => i + high_on_equal as usize,
        };
        self.index_child(index)
    }

    pub fn index_child(&self, index: usize) -> u64 {
        if index == 0 {
            page_id_from_3x16(self.lower().load())
        } else {
            page_id_from_3x16(self.val(index - 1).as_array().load())
        }
    }
}

impl<'a> W<Guarded<'a, Exclusive, BasicNode<KindInner>>> {
    #[allow(clippy::result_unit_err)]
    pub fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()> {
        let x = self.insert(key, &page_id_to_3x16(pid));
        Node::validate(self.s());
        x.map(|x| debug_assert!(x.is_none()))
    }

    pub fn validate_inter_node_fences<'bm, 'b>(
        self,
        bm: impl BufferManager<'bm, Page = PageTail>,
        lb: &mut &'b mut [u8; MAX_KEY_SIZE],
        hb: &mut &'b mut [u8; MAX_KEY_SIZE],
        mut ll: usize,
        mut hl: usize,
    ) {
        if !cfg!(feature = "validate_tree") {
            return;
        }
        assert!(
            self.s().lower_fence().mem_cmp(&lb[..ll]).is_eq(),
            "wrong lf {:?}\n{:?}",
            BStr::new(&lb[..ll]),
            self.upcast()
        );
        assert!(
            self.s().upper_fence().mem_cmp(&hb[..hl]).is_eq(),
            "wrong uf {:?}\n{:?}",
            BStr::new(&hb[..hl]),
            self.upcast()
        );
        if self.s().tag().load() != node_tag::BASIC_INNER {
            return;
        }
        let prefix = self.prefix_len().load() as usize;
        let count = self.count().load() as usize;
        for (i, k) in (0..count)
            .map(|i| self.s().key(i))
            .chain(std::iter::once(self.s().upper_fence().slice(prefix..)))
            .enumerate()
        {
            k.write_to(&mut Guarded::wrap_mut(&mut hb[prefix..][..k.len()]));
            hl = k.len() + prefix;
            let htmp = hb[..hl].to_vec();
            bm.lock_exclusive(self.optimistic().index_child(i))
                .b()
                .0
                .cast::<BasicInner>()
                .validate_inter_node_fences(bm, lb, hb, ll, hl);
            assert_eq!(&hb[..hl], htmp);
            swap(hb, lb);
            ll = hl;
        }
        swap(hb, lb);
    }
}

#[cfg(test)]
mod tests {
    use crate::basic_node::{BasicNode, NodeKind};
    use crate::key_source::SourceSlice;
    use crate::node::{KindInner, KindLeaf, Node, ParentInserter};
    use crate::page::{page_id_to_3x16, PageTail};
    use bytemuck::Zeroable;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use seqlock::{BmExt, DefaultBm, Exclusive, Guard, Guarded};
    use std::collections::HashSet;

    #[test]
    #[allow(clippy::unused_enumerate_index)]
    fn leaf() {
        let rng = &mut SmallRng::seed_from_u64(42);
        let keys = dev_utils::ascii_bin_generator(10..=50);
        let mut keys: Vec<Vec<u8>> = (0..50).map(|i| keys(rng, i)).collect();
        keys.sort();
        keys.dedup();
        let leaf = &mut BasicNode::<KindLeaf>::zeroed();
        let mut leaf = Guarded::<Exclusive, _>::wrap_mut(leaf);
        for (_k, keys) in dev_utils::subslices(&keys, 5).enumerate() {
            let kc = keys.len();
            leaf.init(keys[1].as_slice(), keys[kc - 2].as_slice(), [0; 3]);
            let insert_range = 2..kc - 2;
            let mut to_insert: Vec<&[u8]> = keys[insert_range.clone()].iter().map(|x| x.as_slice()).collect();
            let mut inserted = HashSet::new();
            for p in 0..=3 {
                to_insert.shuffle(rng);
                for (_i, &k) in to_insert.iter().enumerate() {
                    if p != 2 {
                        if leaf.insert_leaf(k, k).is_ok() {
                            inserted.insert(k);
                        }
                    } else {
                        let in_leaf = leaf.remove(k).is_some();
                        let expected = inserted.remove(k);
                        assert_eq!(in_leaf, expected);
                    }
                }
                for (_i, k) in keys.iter().enumerate() {
                    let expected = Some(k).filter(|_| inserted.contains(k.as_slice()));
                    let actual = leaf.s().lookup_leaf(k).map(|v| v.load_slice_to_vec());
                    assert_eq!(expected, actual.as_ref());
                }
            }
        }
    }

    fn split_merge<V: NodeKind>(ufb: u8, lower: [V::SliceType; 3], mut val: impl FnMut(u64) -> Vec<V::SliceType>) {
        let bm = &DefaultBm::new_with_page_count(2);
        struct FakeParent<'bm>(Option<(Vec<u8>, u64)>, &'bm DefaultBm<PageTail>);

        #[allow(non_local_definitions)]
        impl<'bm> ParentInserter<'bm, &'bm DefaultBm<PageTail>> for &mut FakeParent<'bm> {
            fn insert_upper_sibling(
                self,
                separator: impl SourceSlice,
            ) -> Result<Guard<'bm, &'bm DefaultBm<PageTail>, Exclusive, PageTail>, ()> {
                assert!(self.0.is_none());
                let (id, g) = self.1.lock_new();
                self.0 = Some((separator.to_vec(), id));
                Ok(g)
            }
        }

        let (_, mut ne1) = bm.lock_new();
        let mut fake_parent = FakeParent(None, bm);
        let mut n1 = ne1.b().0.cast::<BasicNode<V>>();
        n1.init(&[0][..], &[ufb, 1][..], lower);
        for i in 0u64.. {
            if n1.insert(&i.to_be_bytes()[..], &val(i)).is_err() {
                break;
            }
        }
        let s1 = n1.s().to_debug();
        Node::split(&mut n1, &mut fake_parent, &[0]).unwrap();
        let p2 = fake_parent.0.as_ref().unwrap().1;
        let mut ne2 = bm.lock_exclusive(p2);
        let mut n2 = ne2.b().0.cast::<BasicNode<V>>();
        Node::validate(n1.s());
        Node::validate(n2.s());
        let sep_key = &fake_parent.0.as_ref().unwrap().0;
        assert_eq!(&n1.s().upper_fence().load_slice_to_vec(), sep_key);
        assert_eq!(&n2.s().lower_fence().load_slice_to_vec(), sep_key);
        Node::merge(&mut n1, &mut n2, &[0]);
        Node::validate(n1.s());
        assert_eq!(s1, n1.s().to_debug());
        ne1.free();
        ne2.free();
    }

    #[test]
    fn split_merge_leaf() {
        let val = |i: u64| i.to_be_bytes().to_vec();
        split_merge::<KindLeaf>(1, [0; 3], val);
        split_merge::<KindLeaf>(0, [0; 3], val);
    }

    #[test]
    fn split_merge_inner() {
        let fake_pid = |i| page_id_to_3x16(i + 1024).to_vec();
        split_merge::<KindInner>(1, [0; 3], fake_pid);
        split_merge::<KindInner>(0, [0; 3], fake_pid);
    }
}
