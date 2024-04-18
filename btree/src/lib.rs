use seqlock::{seqlock_wrapper, SeqLockGuarded, SeqLockMode, SeqLockSafe, SeqlockAccessors};
use std::mem::{offset_of, size_of};
use std::ptr::slice_from_raw_parts;

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
}

unsafe trait Node{}

impl<N:Node,M:SeqLockMode> Wrapper<SeqLockGuarded<M,N>>{
    fn slice<T:SeqLockSafe,M:SeqLockMode>(&self,offset:usize,count:usize)->Result<Wrapper<SeqLockGuarded<M,[T]>>,M::ReleaseError>{
        if offset+count*size_of::<T>() > PAGE_SIZE-PAGE_HEAD_SIZE{
            return Err(M::release_error())
        }
        slice_from_raw_parts(self.as_ptr().cast::<u8>().add(offset).cast::<T>()  ,count)
    }
}

impl<V: BasicNodeVariant> BasicNode<V> {
    fn heads<M: SeqLockMode>(
        mut this: Wrapper<SeqLockGuarded<M, Self>>,
    ) -> Result<SeqLockGuarded<M, [u32]>, M::ReleaseError> {
        let count = this.common().count().load();
        if offset_of!(Self,data) + size_of::<u32>()*count > PAGE_SIZE{
            return Err(M::release_error())
        }

    }

    fn find<M: SeqLockMode>(
        this: Wrapper<SeqLockGuarded<M, Self>>,
        key: &[u8],
    ) -> Result<usize, usize> {
        todo!()
    }
}
