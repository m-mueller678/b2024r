mod byte_slice;

use crate::byte_slice::common_prefix;
use bytemuck::{Pod, Zeroable};
use indxvec::Search;
use seqlock::{seqlock_wrapper, Exclusive, Guarded, Never, SeqLockMode, SeqLockWrappable, SeqlockAccessors};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::{align_of, offset_of, size_of};
use std::ptr::addr_of_mut;

pub const PAGE_SIZE: usize = 1 << 12;
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
    type Upper: SeqLockWrappable + Pod + Copy + Zeroable;
    const IS_LEAF: bool;
    const RECORD_TO_KEY_OFFSET: usize = if Self::IS_LEAF { 4 } else { 8 };
    const UPPER_HEADS: usize = size_of::<Self::Upper>().div_ceil(4);
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
struct BasicNodeLeaf;

impl BasicNodeVariant for BasicNodeLeaf {
    type Upper = ();
    const IS_LEAF: bool = true;
}

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(transparent)]
struct BasicNodeInner;

impl BasicNodeVariant for BasicNodeInner {
    type Upper = PageId;
    const IS_LEAF: bool = false;
}

unsafe trait Node: SeqLockWrappable + Pod {}

unsafe impl<V: BasicNodeVariant> Node for BasicNode<V> {}

impl<'a, N: Node, M: SeqLockMode> Wrapper<Guarded<'a, M, N>> {
    fn slice<T: SeqLockWrappable + Pod>(
        self,
        offset: usize,
        count: usize,
    ) -> Result<Guarded<'a, M, [T]>, M::ReleaseError> {
        const SIZE: usize = PAGE_SIZE - PAGE_HEAD_SIZE;
        Ok(self.0.cast::<[u8; SIZE]>().as_slice().slice(offset, count * size_of::<T>()).cast_slice::<T>())
    }
}

fn key_head(k: &[u8]) -> u32 {
    let mut h = 0u32;
    for x in k {
        h <<= 8;
        h |= *x as u32;
    }
    h
}

impl<'a, V: BasicNodeVariant> Wrapper<Guarded<'a, Exclusive, BasicNode<V>>> {
    pub fn init(&mut self, lf: &[u8], uf: &[u8], upper: V::Upper) {
        assert_eq!(size_of::<BasicNode<V>>(), PAGE_SIZE - PAGE_HEAD_SIZE);
        self.count_mut().store(0);
        self.prefix_len_mut().store(common_prefix(lf, uf) as u16);
        self.lower_fence_len_mut().store(lf.len() as u16);
        self.upper_fence_len_mut().store(uf.len() as u16);
        self.heap_bump_mut().store((size_of::<BasicNode<V>>() - lf.len() - uf.len()) as u16);
        self.heap_freed_mut().store(0);
        V::Upper::get_mut(&mut self.b().upper()).store(upper);
    }

    fn compactify(&mut self) -> Result<(), Never> {
        let buffer = &mut [0u8; size_of::<BasicNodeData>()];
        let fence_offset =
            size_of::<Self>() - self.lower_fence_len().load() as usize - self.upper_fence_len().load() as usize;
        let mut dst_offset = fence_offset;
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
        self.b().slice::<u8>(dst_offset, fence_offset - dst_offset)?.store_slice(&buffer[dst_offset..fence_offset]);
        debug_assert_eq!(self.heap_bump().load() + self.heap_freed().load(), dst_offset as u16);
        self.heap_freed_mut().store(0);
        self.heap_bump_mut().store(dst_offset as u16);
        Ok(())
    }
}

impl<'a> Wrapper<Guarded<'a, Exclusive, BasicNode<BasicNodeLeaf>>> {
    #[allow(clippy::result_unit_err)]
    pub fn insert_leaf(&mut self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        loop {
            let insert_pos = self.s().find(key).unwrap();
            let key = &key[self.prefix_len().load() as usize..];
            let record_len = Self::round_up(key.len() + val.len());
            let count = self.count().load() as usize;
            let heap_start = Self::HEAD_OFFSET + Self::reserved_head_count(count) * 4 + count * 2;
            let free_space = self.heap_bump().load() as usize - heap_start;
            match insert_pos {
                Ok(existing) => {
                    if record_len + 4 <= free_space {
                        let record_pos = self.heap_bump().load() as usize - record_len - 4;
                        self.heap_bump_mut().store(record_pos as u16);
                        self.b().slots()?.index(existing).store(record_pos as u16);
                        self.b().u16(record_pos)?.store(key.len() as u16);
                        self.b().u16(record_pos + 2)?.store(val.len() as u16);
                        self.b().slice(record_pos + 4, key.len()).unwrap().store_slice(key);
                        self.b().slice(record_pos + 4 + key.len(), val.len()).unwrap().store_slice(val);
                        return Ok(());
                    }
                    let old_offset = self.b().slots().unwrap().index(existing).load() as usize;
                    let old_record_len = 4 + Self::round_up(
                        self.s().u16(old_offset)?.load() as usize + self.s().u16(old_offset + 2)?.load() as usize,
                    );
                    if free_space + old_record_len + (self.s().heap_freed().load() as usize) < record_len {
                        return Err(());
                    }
                    self.b().u16(old_offset)?.store(0);
                    self.b().u16(old_offset + 2)?.store(0);
                    self.compactify()?;
                    continue;
                }
                Err(insert_at) => todo!(),
            }
        }
    }
}

impl<'a, V: BasicNodeVariant, M: SeqLockMode> Wrapper<Guarded<'a, M, BasicNode<V>>> {
    fn u16(self, offset: usize) -> Result<Guarded<'a, M, u16>, M::ReleaseError> {
        Ok(self.slice::<u16>(offset, 1)?.index(0))
    }
    fn upper(self) -> <V::Upper as SeqLockWrappable>::Wrapper<Guarded<'a, M, V::Upper>> {
        assert_eq!(4 % align_of::<V::Upper>(), 0);
        unsafe { self.0.map_ptr(|x| addr_of_mut!((*x).0._data) as *mut V::Upper) }
    }
    fn round_up(x: usize) -> usize {
        x + (1 - (x & 1))
    }
    const HEAD_OFFSET: usize = offset_of!(BasicNodeData, _data) + V::UPPER_HEADS;

    fn heads(self) -> Result<Guarded<'a, M, [u32]>, M::ReleaseError> {
        let count = self.count().load() as usize;
        self.slice(Self::HEAD_OFFSET, count)
    }

    fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(8)
    }
    fn slots(self) -> Result<Guarded<'a, M, [u16]>, M::ReleaseError> {
        let count = self.count().load() as usize;
        let i = Self::reserved_head_count(count);
        self.slice(Self::HEAD_OFFSET + 4 * i, count)
    }

    fn key(self, unchecked_record_offset: usize) -> Result<Guarded<'a, M, [u8]>, M::ReleaseError> {
        if unchecked_record_offset + 2 > size_of::<Self>() || unchecked_record_offset % 2 != 0 {
            return Err(M::release_error());
        }
        let len = self.s().u16(unchecked_record_offset)?.load();
        self.slice(unchecked_record_offset + V::RECORD_TO_KEY_OFFSET, len as usize)
    }

    fn find(&mut self, key: &[u8]) -> Result<Result<usize, usize>, M::ReleaseError>
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
        let matching_head_range = (0..=heads.len() - 1).binary_all(|i| heads.s().index(i).load().cmp(&needle_head));
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
