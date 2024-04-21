#![allow(clippy::missing_safety_doc)]

mod byte_slice;
#[cfg(test)]
mod test_util;

use crate::byte_slice::common_prefix;
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use seqlock::{seqlock_wrapper, Exclusive, Guarded, Never, SeqLockMode, SeqLockWrappable, SeqlockAccessors};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::{align_of, offset_of, size_of};
use std::ptr::addr_of_mut;

pub const PAGE_SIZE: usize = 1 << 10;
pub const PAGE_HEAD_SIZE: usize = 8;

seqlock_wrapper!(pub Wrapper);

#[derive(Pod, Copy, Clone, Zeroable)]
#[repr(C)]
pub struct CommonNodeHead {
    prefix_len: u16,
    count: u16,
    lower_fence_len: u16,
    upper_fence_len: u16,
}

pub type PageId = [u16; 3];

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(align(8))]
#[repr(C)]
pub struct BasicNodeData {
    common: CommonNodeHead,
    heap_bump: u16,
    heap_freed: u16,
    hints: [u32; 16],
    _data: [u32; BASIC_NODE_DATA_SIZE],
}

const BASIC_NODE_DATA_SIZE: usize = (PAGE_SIZE - PAGE_HEAD_SIZE - size_of::<CommonNodeHead>() - 2 * 2 - 16 * 4) / 4;

#[repr(transparent)]
#[derive(Clone, Copy, Zeroable, SeqlockAccessors)]
#[seq_lock_wrapper(Wrapper)]
#[seq_lock_accessor(prefix_len: u16 = 0.common.prefix_len)]
#[seq_lock_accessor(count: u16 = 0.common.count)]
#[seq_lock_accessor(lower_fence_len: u16 = 0.common.lower_fence_len)]
#[seq_lock_accessor(upper_fence_len: u16 = 0.common.upper_fence_len)]
#[seq_lock_accessor(heap_bump: u16 = 0.heap_bump)]
#[seq_lock_accessor(heap_freed: u16 = 0.heap_freed)]
pub struct BasicNode<V: BasicNodeVariant>(
    #[seq_lock_skip_accessor] BasicNodeData,
    #[seq_lock_skip_accessor] PhantomData<V::Upper>,
);

unsafe impl<V: BasicNodeVariant> Pod for BasicNode<V> {}

pub trait BasicNodeVariant: 'static + Copy + Zeroable {
    type Upper: SeqLockWrappable + Pod;
    const IS_LEAF: bool;
    const RECORD_TO_KEY_OFFSET: usize = if Self::IS_LEAF { 4 } else { 8 };
    const UPPER_HEADS: usize = size_of::<Self::Upper>().div_ceil(4);
    type ValueSlice: SeqLockWrappable + Pod;
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
pub struct BasicNodeLeaf;

impl BasicNodeVariant for BasicNodeLeaf {
    type Upper = ();

    const IS_LEAF: bool = true;
    type ValueSlice = u8;
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
pub struct BasicNodeInner;

impl BasicNodeVariant for BasicNodeInner {
    type Upper = PageId;
    type ValueSlice = u16;
    const IS_LEAF: bool = false;
}

pub unsafe trait Node: SeqLockWrappable + Pod {}

unsafe impl<V: BasicNodeVariant> Node for BasicNode<V> {}

impl<'a, N: Node, M: SeqLockMode> Wrapper<Guarded<'a, M, N>> {
    fn slice<T: SeqLockWrappable + Pod>(
        self,
        offset: usize,
        count: usize,
    ) -> Result<Guarded<'a, M, [T]>, M::ReleaseError> {
        self.as_bytes().try_slice(offset..offset + count * size_of::<T>())?.try_cast_slice::<T>()
    }

    #[allow(clippy::wrong_self_convention)]
    fn as_bytes(self) -> Guarded<'a, M, [u8]> {
        const SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;
        self.0.cast::<[u8; SIZE]>().as_slice()
    }
}

fn key_head(k: &[u8]) -> u32 {
    let mut h = 0u32;
    for i in 0..4 {
        h <<= 8;
        h |= k.get(i).copied().unwrap_or(0) as u32;
    }
    h
}

const HEAD_RESERVATION: usize = 8;

impl<'a, V: BasicNodeVariant> Wrapper<Guarded<'a, Exclusive, BasicNode<V>>> {
    /// offset is in units of bytes, others in units of T
    fn relocate_by<const UP: bool, T: SeqLockWrappable + Pod>(&mut self, offset: usize, count: usize, dist: usize) {
        assert_eq!(offset % size_of::<T>(), 0);
        let offset = offset / size_of::<T>();
        self.b().as_bytes().cast_slice::<T>().move_within_by::<UP>(offset..offset + count, dist);
    }
    fn heap_write_new(&mut self, key: &[u8], val: &[V::ValueSlice], write_slot: usize) -> Result<(), Never> {
        let size = Self::record_size(key.len(), val.len());
        let offset = self.heap_bump().load() as usize - size;
        self.heap_write_record(key, val, offset)?;
        self.heap_bump_mut().store(offset as u16);
        self.b().slots()?.index(write_slot).store(offset as u16);
        Ok(())
    }

    fn heap_write_record(&mut self, key: &[u8], val: &[V::ValueSlice], offset: usize) -> Result<(), Never> {
        self.b().u16(offset)?.store(key.len() as u16);
        if V::IS_LEAF {
            self.b().u16(offset + 2)?.store(val.len() as u16);
        }
        if !V::IS_LEAF {
            self.b().slice(offset + 2, 3)?.store_slice(val);
        }
        self.b().slice(offset + V::RECORD_TO_KEY_OFFSET, key.len()).unwrap().store_slice(key);
        if V::IS_LEAF {
            self.b().slice(offset + V::RECORD_TO_KEY_OFFSET + key.len(), val.len()).unwrap().store_slice(val);
        }
        Ok(())
    }

    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], val: &[V::ValueSlice]) -> Result<(), ()> {
        if !V::IS_LEAF {
            assert_eq!(val.len(), 3);
        }
        loop {
            let index = self.s().find(key).unwrap();
            let key = &key[self.prefix_len().load() as usize..];
            let count = self.count().load() as usize;
            let new_heap_start;
            match index {
                Ok(existing) => {
                    new_heap_start = Self::HEAD_OFFSET + Self::reserved_head_count(count) * 4 + count * 2;
                    if Self::record_size(key.len(), val.len()) <= (self.heap_bump().load() as usize - new_heap_start) {
                        self.heap_write_new(key, val, existing)?;
                        return Ok(());
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
                        self.b().heads()?.index(insert_at).store(key_head(key));
                        self.heap_write_new(key, val, insert_at)?;
                        return Ok(());
                    }
                }
            }
            let available_space =
                self.heap_bump().load() as usize - new_heap_start + (self.s().heap_freed().load() as usize);
            if available_space < Self::record_size(key.len(), val.len()) {
                return Err(());
            }
            self.compactify()?;
        }
    }

    fn record_size(key: usize, val: usize) -> usize {
        if V::IS_LEAF {
            4 + Self::round_up(key + val)
        } else {
            8 + Self::round_up(key)
        }
    }

    fn heap_end(&mut self) -> usize {
        size_of::<BasicNodeData>()
            - Self::round_up(self.lower_fence_len().load() as usize + self.upper_fence_len().load() as usize)
    }

    pub fn init(&mut self, lf: &[u8], uf: &[u8], upper: V::Upper) {
        assert_eq!(size_of::<BasicNode<V>>(), PAGE_SIZE - PAGE_HEAD_SIZE);
        self.count_mut().store(0);
        self.prefix_len_mut().store(common_prefix(lf, uf) as u16);
        self.lower_fence_len_mut().store(lf.len() as u16);
        self.upper_fence_len_mut().store(uf.len() as u16);
        let heap_end = self.heap_end() as u16;
        self.heap_bump_mut().store(heap_end);
        self.heap_freed_mut().store(0);
        V::Upper::get_mut(&mut self.b().upper()).store(upper);
    }

    fn compactify(&mut self) -> Result<(), Never> {
        let buffer = &mut [0u8; size_of::<BasicNodeData>()];
        let heap_end = self.heap_end();
        let mut dst_offset = heap_end;
        for i in 0..self.count().load() as usize {
            let offset = self.s().slots().unwrap().index(i).load() as usize;
            let lens = self.s().slice::<u16>(offset, 2).unwrap();
            let record_len = V::RECORD_TO_KEY_OFFSET
                + Self::round_up(
                    lens.index(0).load() as usize + if V::IS_LEAF { lens.index(1).load() as usize } else { 0 },
                );
            dst_offset -= record_len;
            self.s().slice(offset, record_len)?.load_slice(&mut buffer[dst_offset..][..record_len]);
            self.b().slots().unwrap().index(i).store(dst_offset as u16);
        }
        self.b().slice::<u8>(dst_offset, heap_end - dst_offset)?.store_slice(&buffer[dst_offset..heap_end]);
        debug_assert_eq!(self.heap_bump().load() + self.heap_freed().load(), dst_offset as u16);
        self.heap_freed_mut().store(0);
        self.heap_bump_mut().store(dst_offset as u16);
        Ok(())
    }
}

impl<'a, V: BasicNodeVariant, M: SeqLockMode> Wrapper<Guarded<'a, M, BasicNode<V>>> {
    fn u16(self, offset: usize) -> Result<Guarded<'a, M, u16>, M::ReleaseError> {
        self.slice::<u16>(offset, 1)?.try_index(0)
    }
    fn upper(self) -> <V::Upper as SeqLockWrappable>::Wrapper<Guarded<'a, M, V::Upper>> {
        assert_eq!(4 % align_of::<V::Upper>(), 0);
        unsafe { self.0.map_ptr(|x| addr_of_mut!((*x).0._data) as *mut V::Upper) }
    }
    fn round_up(x: usize) -> usize {
        x + (x % 2)
    }
    const HEAD_OFFSET: usize = offset_of!(BasicNodeData, _data) + V::UPPER_HEADS;

    fn heads(self) -> Result<Guarded<'a, M, [u32]>, M::ReleaseError> {
        let count = self.count().load() as usize;
        self.slice(Self::HEAD_OFFSET, count)
    }

    fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(HEAD_RESERVATION)
    }
    fn slots(self) -> Result<Guarded<'a, M, [u16]>, M::ReleaseError> {
        let count = self.count().load() as usize;
        let i = Self::reserved_head_count(count);
        self.slice(Self::HEAD_OFFSET + 4 * i, count)
    }

    fn key(self, index: usize) -> Result<Guarded<'a, M, [u8]>, M::ReleaseError> {
        let offset = self.s().slots().unwrap().try_index(index)?.load() as usize;
        let len = self.s().u16(offset)?.load();
        self.slice(offset + V::RECORD_TO_KEY_OFFSET, len as usize)
    }

    fn find(self, key: &[u8]) -> Result<Result<usize, usize>, M::ReleaseError>
    where
        Self: Copy,
    {
        let prefix_len = self.prefix_len().load() as usize;
        if prefix_len > key.len() {
            return Err(M::release_error());
        }
        let truncated = &key[prefix_len..];
        let needle_head = key_head(truncated);
        let heads = self.heads()?;
        if heads.is_empty() {
            return Ok(Err(0));
        }
        let matching_head_range = (0..=heads.len() - 1)
            .binary_all(|i| heads.s().try_index(i).map(|x| x.load()).unwrap_or(0).cmp(&needle_head));
        if matching_head_range.is_empty() {
            return Ok(Err(matching_head_range.start));
        }
        let slots = self.slots()?;
        if slots.len() != heads.len() {
            return Err(M::release_error());
        }
        let key_position = (matching_head_range.start..=matching_head_range.end - 1).binary_by(|i| {
            let Ok(key) = self.key(i) else {
                return Ordering::Equal;
            };
            key.mem_cmp(truncated)
        });
        Ok(key_position)
    }
}

impl<'a, M: SeqLockMode> Wrapper<Guarded<'a, M, BasicNode<BasicNodeLeaf>>> {
    pub fn lookup_leaf(self, key: &[u8]) -> Result<Option<Guarded<'a, M, [u8]>>, M::ReleaseError>
    where
        Self: Copy,
    {
        if let Ok(i) = self.find(key)? {
            let offset = self.slots()?.try_index(i)?.load() as usize;
            let val_start = self.u16(offset)?.load() as usize + offset + BasicNodeLeaf::RECORD_TO_KEY_OFFSET;
            let val_len = self.u16(offset + 2)?.load() as usize;
            Ok(Some(self.slice(val_start, val_len)?))
        } else {
            Ok(None)
        }
    }
}

impl<'a> Wrapper<Guarded<'a, Exclusive, BasicNode<BasicNodeLeaf>>> {
    pub fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        self.insert(key, val)
    }
}

impl<'a> Wrapper<Guarded<'a, Exclusive, BasicNode<BasicNodeInner>>> {
    pub fn insert_inner(&mut self, key: &[u8], pid: u64) {
        self.insert(key, &bytemuck::cast::<u64, [u16; 4]>(pid)[..3]).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::{bin_key_generator, subslices};
    use crate::{BasicNode, BasicNodeLeaf};
    use bytemuck::Zeroable;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use seqlock::{Exclusive, Guarded};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn leaf() {
        let rng = &mut SmallRng::seed_from_u64(42);
        let keys = bin_key_generator(10..=50);
        let mut keys: Vec<Vec<u8>> = (0..50).map(|_| keys(rng)).collect();
        keys.sort();
        keys.dedup();
        let leaf = &mut BasicNode::<BasicNodeLeaf>::zeroed();
        let mut leaf = Guarded::<Exclusive, _>::wrap_mut(leaf);
        for (_k, keys) in subslices(&keys, 5).enumerate() {
            let kc = keys.len();
            leaf.init(&keys[1], &keys[kc - 2], ());
            let insert_range = 2..kc - 2;
            let mut to_insert: Vec<&[u8]> = keys[insert_range.clone()].iter().map(|x| x.as_slice()).collect();
            let mut inserted = HashSet::new();
            for _p in 0..2 {
                to_insert.shuffle(rng);
                for &k in to_insert.iter() {
                    if leaf.insert_leaf(k, k).is_ok() {
                        inserted.insert(k);
                    }
                }
                for (_i, k) in keys.iter().enumerate() {
                    let expected = Some(k).filter(|_| inserted.contains(k.as_slice()));
                    let actual = leaf.s().lookup_leaf(k).unwrap().map(|v| v.load_slice_to_vec());
                    assert_eq!(expected, actual.as_ref());
                }
            }
        }
    }
}
