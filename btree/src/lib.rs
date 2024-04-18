use indxvec::Search;
use seqlock::{
    seqlock_wrapper, wrap_unchecked, SeqLockGuarded, SeqLockMode, SeqLockSafe, SeqlockAccessors,
};
use std::cmp::Ordering;
use std::mem::{offset_of, size_of};
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};

pub const PAGE_SIZE: usize = 1 << 12;
pub const PAGE_HEAD_SIZE: usize = 8;

seqlock_wrapper!(pub Wrapper);

#[derive(SeqlockAccessors)]
#[seq_lock_wrapper(Wrapper)]
pub struct CommonNodeHead {
    prefix_len: u16,
    count: u16,
    lower_fence_len: u16,
    upper_fence_len: u16,
}

pub struct PageId([u16; 3]);

#[derive(SeqlockAccessors)]
#[seq_lock_wrapper(Wrapper)]
pub struct BasicNode<V: BasicNodeVariant> {
    common: CommonNodeHead,
    heap_bump: u16,
    heap_freed: u16,
    hints: [u32; 16],
    upper: V::Upper,
    _pad: u16,
    _data: [u32; PAGE_SIZE - PAGE_HEAD_SIZE - size_of::<CommonNodeHead>() - 6 * 2 - 16 * 4],
}

pub trait BasicNodeVariant {
    type Upper: SeqLockSafe;
    const RECORD_TO_KEY_OFFSET: usize;
}

unsafe trait Node {}

unsafe impl<V: BasicNodeVariant> Node for BasicNode<V> {}

impl<'a, N: Node, M: SeqLockMode> Wrapper<SeqLockGuarded<'a, M, N>> {
    fn slice<'b, T: SeqLockSafe>(
        &self,
        offset: usize,
        count: usize,
    ) -> Result<SeqLockGuarded<'b, M, [T]>, M::ReleaseError> {
        if offset + count * size_of::<T>() > PAGE_SIZE - PAGE_HEAD_SIZE {
            return Err(M::release_error());
        }
        unsafe {
            Ok(wrap_unchecked(slice_from_raw_parts_mut(
                self.as_ptr().cast::<u8>().add(offset).cast::<T>(),
                count,
            )))
        }
    }
}

fn head_split(k: &[u8]) -> (u32, &[u8]) {
    let mut h = 0u32;
    for i in 0..4 {
        h <<= 8;
        h |= k[i] as u32;
    }
    (h, &k[k.len().min(4)..])
}

impl<'a, V: BasicNodeVariant, M: SeqLockMode> Wrapper<SeqLockGuarded<'a, M, BasicNode<V>>> {
    fn heads(&mut self) -> Result<SeqLockGuarded<'a, M, [u32]>, M::ReleaseError> {
        let count = self.common().count().load() as usize;
        self.slice(offset_of!(BasicNode<V>, _data), count)
    }

    fn reserved_head_count(count: usize) -> usize {
        count.next_multiple_of(8)
    }

    fn slots(&mut self) -> Result<SeqLockGuarded<'a, M, [u32]>, M::ReleaseError> {
        let count = self.common().count().load() as usize;
        self.slice(
            offset_of!(BasicNode<V>, _data) + size_of::<u32>() * Self::reserved_head_count(count),
            count,
        )
    }

    fn key_tail(
        &mut self,
        unchecked_record_offset: usize,
    ) -> Result<SeqLockGuarded<'a, M, [u8]>, M::ReleaseError> {
        if unchecked_record_offset + 2 > size_of::<Self>() || unchecked_record_offset % 2 != 0 {
            return Err(M::release_error());
        }
        let len = self
            .slice::<u16>(unchecked_record_offset, 1)?
            .index(0)
            .load();
        self.slice(
            unchecked_record_offset + V::RECORD_TO_KEY_OFFSET,
            len as usize,
        )
    }

    fn find(&mut self, key: &[u8]) -> Result<Result<usize, usize>, M::ReleaseError> {
        let prefix_len = self.common().prefix_len().load() as usize;
        if prefix_len > key.len() {
            return Err(M::release_error());
        }
        let truncated = &key[prefix_len..];
        let mut heads = self.heads()?;
        let matching_head_range = (0..=heads.len() - 1).binary_all(|i| {
            heads.index(i).cmp(head);
        });
        if matching_head_range.is_empty() {
            return Ok(Err(matching_head_range.start));
        }
        let slots = self.slots()?;
        if slots.len().len() != heads.len() {
            return Err(M::release_error());
        }
        (matching_head_range.start..=matching_head_range.end - 1).binary_by(|i| {
            let key = self.key_tail(i)?;
            todo!()
        });
        todo!()
    }
}
