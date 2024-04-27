use crate::key_source::{common_prefix, key_head, SourceSlice};
use crate::node::{CommonNodeHead, Node, PAGE_HEAD_SIZE, PAGE_SIZE};
use crate::page::{PageId, PageTail};
use crate::W;
use bstr::BString;
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use seqlock::{
    Exclusive, Guard, Guarded, Never, Optimistic, SeqLockMode, SeqLockModePessimistic, SeqLockWrappable,
    SeqlockAccessors, Shared, Wrapper,
};
use std::any::type_name;
use std::marker::PhantomData;
use std::mem::{align_of, offset_of, size_of};
use std::ops::Range;
use std::ptr::addr_of_mut;

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

const BASIC_NODE_DATA_SIZE: usize = (PAGE_SIZE - PAGE_HEAD_SIZE - size_of::<CommonNodeHead>() - 2 * 2 - 16 * 4) / 4;

#[repr(transparent)]
#[derive(Clone, Copy, Zeroable, SeqlockAccessors)]
#[seq_lock_wrapper(W)]
#[seq_lock_accessor(prefix_len: u16 = 0.common.prefix_len)]
#[seq_lock_accessor(count: u16 = 0.common.count)]
#[seq_lock_accessor(lower_fence_len: u16 = 0.common.lower_fence_len)]
#[seq_lock_accessor(upper_fence_len: u16 = 0.common.upper_fence_len)]
#[seq_lock_accessor(heap_bump: u16 = 0.heap_bump)]
#[seq_lock_accessor(heap_freed: u16 = 0.heap_freed)]
pub struct BasicNode<V: BasicNodeVariant>(
    #[seq_lock_skip_accessor] BasicNodeData,
    #[seq_lock_skip_accessor] PhantomData<V::ValueSlice>,
);

unsafe impl<V: BasicNodeVariant> Pod for BasicNode<V> {}

pub trait BasicNodeVariant: 'static + Copy + Zeroable {
    const IS_LEAF: bool;
    const RECORD_TO_KEY_OFFSET: usize = if Self::IS_LEAF { 4 } else { 8 };
    const LOWER_HEAD_SLOTS: usize = if Self::IS_LEAF { 0 } else { size_of::<[Self::ValueSlice; 3]>().div_ceil(4) };
    type ValueSlice: SeqLockWrappable + Pod;
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
pub struct BasicNodeLeaf;

impl BasicNodeVariant for BasicNodeLeaf {
    const IS_LEAF: bool = true;
    type ValueSlice = u8;
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
pub struct BasicNodeInner;

impl BasicNodeVariant for BasicNodeInner {
    type ValueSlice = u16;
    const IS_LEAF: bool = false;
}

unsafe impl<V: BasicNodeVariant> Node for BasicNode<V> {
    fn split(
        this: &mut W<Guarded<Exclusive, Self>>,
        parent_insert: impl FnOnce(usize, Guarded<'_, Shared, [u8]>) -> Result<Guard<'static, Exclusive, PageTail>, ()>,
    ) -> Result<(), ()> {
        // TODO tail compression
        assert!(V::IS_LEAF);
        let left = &mut BasicNode::<V>::zeroed();
        let mut left = Guarded::<Exclusive, _>::wrap_mut(left);
        let count = this.count().load() as usize;
        let low_count = count / 2;
        let mut right = parent_insert(this.prefix_len().load() as usize, this.s().key(low_count))?;
        let right = &mut right.b().0.cast::<BasicNode<V>>();
        let sep_key = this.s().prefix().join(this.s().key(low_count));
        left.init(this.s().lower_fence(), sep_key, this.s().lower().get().load());
        let sep_record_offset = this.s().slots().index(low_count).load() as usize;
        let lower = this.s().slice::<[V::ValueSlice; 3]>(sep_record_offset, 1).index(1).get().load();
        right.init(sep_key, this.s().upper_fence(), lower);
        if V::IS_LEAF {
            left.count_mut().store(low_count as u16);
            this.copy_records(&mut left, 0..low_count, 0);
            right.count_mut().store((count - low_count) as u16);
            this.copy_records(right, low_count..count, 0);
        } else {
            left.count_mut().store(low_count as u16);
            this.copy_records(&mut left, 0..low_count, 0);
            right.count_mut().store((count - low_count - 1) as u16);
            this.copy_records(right, low_count..count, 0);
        }
        this.store(left.load()); //TODO optimize copy
        Ok(())
    }
}

const HEAD_RESERVATION: usize = 16;

impl<'a, V: BasicNodeVariant> W<Guarded<'a, Exclusive, BasicNode<V>>> {
    /// offset is in units of bytes, others in units of T
    fn relocate_by<const UP: bool, T: SeqLockWrappable + Pod>(&mut self, offset: usize, count: usize, dist: usize) {
        assert_eq!(offset % size_of::<T>(), 0);
        let offset = offset / size_of::<T>();
        self.b().as_bytes().cast_slice::<T>().move_within_by::<UP>(offset..offset + count, dist);
    }
    fn heap_write_new(&mut self, key: impl SourceSlice, val: impl SourceSlice<V::ValueSlice>, write_slot: usize) {
        let size = Self::record_size(key.len(), val.len());
        let offset = self.heap_bump().load() as usize - size;
        self.heap_write_record(key, val, offset);
        self.heap_bump_mut().store(offset as u16);
        self.b().slots().index(write_slot).store(offset as u16);
    }

    pub fn heap_write_record(&mut self, key: impl SourceSlice, val: impl SourceSlice<V::ValueSlice>, offset: usize) {
        let len = key.len();
        self.b().u16(offset).store(len as u16);
        if V::IS_LEAF {
            self.b().u16(offset + 2).store(val.len() as u16);
        }
        if !V::IS_LEAF {
            val.write_to(&mut self.b().slice(offset + 2, 3));
        }
        key.write_to(&mut self.b().slice(offset + V::RECORD_TO_KEY_OFFSET, len));
        if V::IS_LEAF {
            val.write_to(&mut self.b().slice(offset + V::RECORD_TO_KEY_OFFSET + len, val.len()));
        }
    }

    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], val: &[V::ValueSlice]) -> Result<Option<()>, ()> {
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
                        return Ok(None);
                    }
                }
            }
            if self.heap_bump().load() as usize + (self.s().heap_freed().load() as usize)
                < new_heap_start + Self::record_size(key.len(), val.len())
            {
                return Err(());
            }
            self.compactify();
        }
    }

    fn merge(&mut self, right: &mut W<Guarded<Exclusive, BasicNode<V>>>) {
        let tmp = &mut BasicNode::<V>::zeroed();
        let mut tmp = Guarded::<Exclusive, _>::wrap_mut(tmp);
        let left_count = self.count().load() as usize;
        let right_count = right.count().load() as usize;
        tmp.init(self.s().lower_fence(), right.s().upper_fence(), self.s().lower().get().load());
        if V::IS_LEAF {
            tmp.count_mut().store((left_count + right_count) as u16);
            self.copy_records(&mut tmp, 0..left_count, 0);
            right.copy_records(&mut tmp, 0..right_count, left_count);
        } else {
            tmp.count_mut().store((left_count + right_count + 1) as u16);
            self.copy_records(&mut tmp, 0..left_count, 0);
            right.copy_records(&mut tmp, 0..right_count, left_count + 1);
            tmp.heap_write_new(
                self.s().upper_fence().slice(tmp.prefix_len().load() as usize..),
                right.s().lower().as_slice(),
                left_count,
            );
        }
        self.store(tmp.load()); //TODO optimize copy
    }

    fn copy_records(&self, dst: &mut W<Guarded<Exclusive, BasicNode<V>>>, src_range: Range<usize>, dst_start: usize) {
        let dst_range = dst_start..(src_range.end + dst_start - src_range.start);
        let prefix_grow = dst.prefix_len().load() as usize - self.prefix_len().load() as usize;
        for src_i in src_range.clone() {
            dst.heap_write_new(self.s().key(src_i).slice(prefix_grow..), self.s().val(src_i), src_i - dst_start);
        }
        if prefix_grow == 0 {
            self.s().heads().slice(src_range).copy_to(&mut dst.b().heads().slice(dst_range));
        } else {
            for (src_i, dst_i) in src_range.zip(dst_range) {
                let head = key_head(self.s().key(src_i).slice(prefix_grow..));
                dst.b().heads().index(dst_i).store(head);
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<()>, Never> {
        let Ok(index) = self.s().find(key) else {
            return Ok(None);
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
        self.s().validate();
        Ok(Some(()))
    }

    fn record_size(key: usize, val: usize) -> usize {
        if V::IS_LEAF {
            4 + Self::round_up(key + val)
        } else {
            8 + Self::round_up(key)
        }
    }

    pub fn init(&mut self, lf: impl SourceSlice, uf: impl SourceSlice, lower: [V::ValueSlice; 3]) {
        assert_eq!(size_of::<BasicNode<V>>(), PAGE_SIZE - PAGE_HEAD_SIZE);
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
            let record_len = V::RECORD_TO_KEY_OFFSET
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
    }
}

impl<'a, V: BasicNodeVariant, M: SeqLockMode> W<Guarded<'a, M, BasicNode<V>>> {
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
    fn validate(self)
    where
        M: SeqLockModePessimistic,
        Self: Copy,
    {
        if !cfg!(debug_assertions) {
            return;
        }
        let record_size_sum: usize = self.s().slots().iter().map(|x| self.stored_record_size(x.load() as usize)).sum();
        let calculated = self.heap_end() - record_size_sum;
        let tracked = self.heap_bump().load() as usize + self.heap_freed().load() as usize;
        assert_eq!(calculated, tracked);
    }
    fn lower(self) -> Guarded<'a, M, [V::ValueSlice; 3]> {
        assert!(!V::IS_LEAF);
        assert_eq!(4 % align_of::<[V::ValueSlice; 3]>(), 0);
        unsafe { self.0.map_ptr(|x| addr_of_mut!((*x).0._data) as *mut [V::ValueSlice; 3]) }
    }
    fn round_up(x: usize) -> usize {
        x + (x % 2)
    }
    const HEAD_OFFSET: usize = offset_of!(BasicNodeData, _data) + V::LOWER_HEAD_SLOTS * 4;

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
        self.slice(offset + V::RECORD_TO_KEY_OFFSET, len as usize)
    }

    fn val(self, index: usize) -> Guarded<'a, M, [V::ValueSlice]> {
        let offset = self.s().slots().index(index).load() as usize;
        if V::IS_LEAF {
            let key_len = self.s().u16(offset).load() as usize;
            let val_len = self.s().u16(offset + 2).load() as usize;
            self.slice(offset + V::RECORD_TO_KEY_OFFSET + key_len, val_len)
        } else {
            self.slice(offset + V::RECORD_TO_KEY_OFFSET + 2, 3)
        }
    }

    pub fn print(self)
    where
        Self: Copy,
    {
        eprintln!("#{}", type_name::<V>());
        dbg!(
            self.count().load(),
            self.lower_fence_len().load(),
            self.upper_fence_len().load(),
            self.prefix_len().load(),
            self.heap_bump().load(),
            self.heap_freed().load()
        );
        for i in 0..self.count().load() as usize {
            let offset = self.slots().index(i).load() as usize;
            eprint!("{i:4}:{:04x}->[0x{:08x}][{}]", offset, self.heads().index(i).load(), self.u16(offset).load());
            if V::IS_LEAF {
                eprintln!("{:}", BString::new(self.key(i).load_slice_to_vec()))
            } else {
                todo!()
            }
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

impl<'a, M: SeqLockMode> W<Guarded<'a, M, BasicNode<BasicNodeLeaf>>> {
    pub fn lookup_leaf(self, key: &[u8]) -> Option<Guarded<'a, M, [u8]>>
    where
        Self: Copy,
    {
        if let Ok(i) = self.find(key) {
            let offset = self.slots().index(i).load() as usize;
            let val_start = self.u16(offset).load() as usize + offset + BasicNodeLeaf::RECORD_TO_KEY_OFFSET;
            let val_len = self.u16(offset + 2).load() as usize;
            Some(self.slice(val_start, val_len))
        } else {
            None
        }
    }
}

impl<'a> W<Guarded<'a, Exclusive, BasicNode<BasicNodeLeaf>>> {
    #[allow(clippy::result_unit_err)]
    pub fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<Option<()>, ()> {
        let x = self.insert(key, val);
        self.s().validate();
        x
    }
}

impl<'a> W<Guarded<'a, Optimistic, BasicNode<BasicNodeInner>>> {
    pub fn lookup_inner(&self, key: &[u8], high_on_equal: bool) -> PageId {
        let index = match self.find(key) {
            Err(i) => i,
            Ok(i) => i + high_on_equal as usize,
        };
        if index == 0 {
            PageId::from_3x16(self.lower().load())
        } else {
            PageId::from_3x16(self.val(index - 1).as_array().load())
        }
    }
}

impl<'a> W<Guarded<'a, Exclusive, BasicNode<BasicNodeInner>>> {
    #[allow(clippy::result_unit_err)]
    pub fn insert_inner(&mut self, key: &[u8], pid: PageId) -> Result<(), ()> {
        let x = self.insert(key, &pid.to_3x16());
        self.s().validate();
        x.map(|x| debug_assert!(x.is_none()))
    }
}

#[cfg(test)]
mod tests {
    use crate::basic_node::{BasicNode, BasicNodeLeaf};
    use crate::test_util::subslices;
    use bytemuck::Zeroable;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use seqlock::{Exclusive, Guarded};
    use std::collections::HashSet;

    #[test]
    fn leaf() {
        let rng = &mut SmallRng::seed_from_u64(42);
        let keys = crate::test_util::ascii_bin_generator(10..=50);
        let mut keys: Vec<Vec<u8>> = (0..50).map(|_| keys(rng)).collect();
        keys.sort();
        keys.dedup();
        let leaf = &mut BasicNode::<BasicNodeLeaf>::zeroed();
        let mut leaf = Guarded::<Exclusive, _>::wrap_mut(leaf);
        for (_k, keys) in subslices(&keys, 5).enumerate() {
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
                        let in_leaf = leaf.remove(k).unwrap().is_some();
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
}
