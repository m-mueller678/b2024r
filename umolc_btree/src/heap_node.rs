use crate::key_source::SourceSlice;
use crate::node::{ToFromPageExt, PAGE_SIZE};
use crate::Page;
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;

#[derive(Debug)]
pub struct HeapLengthError;

pub trait HeapLength: Pod {
    fn to_usize(self) -> usize;
    fn from_slice(x: impl SourceSlice) -> Result<Self, HeapLengthError>;
    fn map_insert_slice<S: SourceSlice>(x: S) -> S {
        x
    }
    fn load_unaligned(p: &Page, offset: usize) -> usize {
        assert!(offset <= size_of::<Page>() - size_of::<Self>());
        unsafe { (p as *const Page).cast::<u8>().add(offset).cast::<Self>().read_unaligned() }.to_usize()
    }

    fn store_unaligned(self, p: &mut Page, offset: usize) {
        assert!(offset <= size_of::<Page>() - size_of::<Self>());
        unsafe { (p as *mut Page).cast::<u8>().add(offset).cast::<Self>().write_unaligned(self) }
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(transparent)]
pub struct ConstHeapLength<const L: usize>;

impl<const L: usize> HeapLength for ConstHeapLength<L> {
    fn to_usize(self) -> usize {
        L
    }

    fn from_slice(x: impl SourceSlice) -> Result<Self, HeapLengthError> {
        if x.len() == L {
            Ok(Self)
        } else {
            Err(HeapLengthError)
        }
    }
}

impl HeapLength for u16 {
    fn to_usize(self) -> usize {
        self as usize
    }

    fn from_slice(x: impl SourceSlice) -> Result<Self, HeapLengthError> {
        Ok(x.len() as u16)
    }
}

#[derive(Pod, Zeroable, Clone, Copy, Debug)]
#[repr(C)]
pub struct HeapNodeInfo {
    pub bump: u16,
    pub freed: u16,
}

pub enum HeapInsertError {
    BadKeyLen,
    BadValLen,
    Full,
}

pub trait HeapNode: ToFromPageExt + Debug {
    type KeyLength: HeapLength;
    type ValLength: HeapLength;

    const VAL_LEN_OFFSET: usize = size_of::<Self::KeyLength>();
    const KEY_OFFSET: usize = Self::VAL_LEN_OFFSET + size_of::<Self::ValLength>();

    fn slot_offset(&self) -> usize;

    fn insert(
        &mut self,
        new_heap_start: usize,
        key: &[u8],
        val: &[u8],
        index: Result<usize, usize>,
        do_shift: impl FnOnce(&mut Self),
    ) -> Result<Option<()>, HeapInsertError> {
        self.validate();
        let record_size = Self::KEY_OFFSET
            + Self::KeyLength::from_slice(key).map_err(|_| HeapInsertError::BadKeyLen)?.to_usize()
            + Self::ValLength::from_slice(val).map_err(|_| HeapInsertError::BadValLen)?.to_usize();
        loop {
            let info = self.heap_info_mut();
            match index {
                Ok(existing) => {
                    //TODO in-place update
                    if record_size <= (info.bump as usize - new_heap_start) {
                        let existing_size = self.stored_record_size(existing) as u16;
                        self.heap_info_mut().freed += existing_size;
                        self.heap_write_new(key, val, existing);
                        self.validate();
                        return Ok(Some(()));
                    }
                }
                Err(insert_at) => {
                    if new_heap_start + record_size <= info.bump as usize {
                        do_shift(self);
                        self.heap_write_new(key, val, insert_at);
                        self.validate();
                        return Ok(None);
                    }
                }
            }
            if info.bump as usize + (info.freed as usize) < new_heap_start + record_size {
                self.validate();
                return Err(HeapInsertError::Full);
            }
            self.compactify();
        }
    }

    fn init_heap(&mut self) {
        self.heap_info_mut().freed = 0;
        self.heap_info_mut().bump =
            size_of::<Self>() as u16 - self.as_page().common.lower_fence_len - self.as_page().common.upper_fence_len;
    }

    fn compactify(&mut self) {
        self.validate();
        let buffer = &mut [0u8; PAGE_SIZE];
        let heap_end = self.as_page().fences_start();
        let mut dst_bump = heap_end;
        for i in 0..self.as_page().common.count as usize {
            let offset = self.slot(i);
            let val_len = self.heap_val_len(offset);
            let record_len = Self::KEY_OFFSET + val_len + self.heap_key_len(offset);
            dst_bump -= record_len;
            buffer[dst_bump..][..record_len].copy_from_slice(self.slice(offset - val_len, record_len));
            self.set_slot(i, dst_bump + val_len);
        }
        self.slice_mut(dst_bump, heap_end - dst_bump).copy_from_slice(&buffer[dst_bump..heap_end]);
        let h = self.heap_info_mut();
        debug_assert_eq!(h.bump as usize + h.freed as usize, dst_bump);
        h.freed = 0;
        h.bump = dst_bump as u16;
        self.validate();
    }

    fn heap_info_mut(&mut self) -> &mut HeapNodeInfo;
    fn heap_info(&self) -> &HeapNodeInfo;

    fn slot(&self, index: usize) -> usize {
        debug_assert!(index < self.as_page().common.count as usize);
        bytemuck::cast_ref::<Page, [u16; PAGE_SIZE / 2]>(self.as_page())[self.slot_offset() / 2 + index] as usize
    }

    fn set_slot(&mut self, index: usize, v: usize) {
        debug_assert!(index < self.as_page().common.count as usize);
        let so = self.slot_offset();
        bytemuck::cast_mut::<Page, [u16; PAGE_SIZE / 2]>(self.as_page_mut())[so / 2 + index] = v as u16;
    }

    fn stored_record_size(&self, slot_index: usize) -> usize {
        let offset = self.slot(slot_index);
        Self::KEY_OFFSET + self.heap_key_len(offset) + self.heap_val_len(offset)
    }

    fn heap_key_len(&self, record_offset: usize) -> usize {
        Self::KeyLength::load_unaligned(self.as_page(), record_offset)
    }

    fn heap_val_len(&self, record_offset: usize) -> usize {
        let len_offset = record_offset + Self::VAL_LEN_OFFSET;
        Self::ValLength::load_unaligned(self.as_page(), len_offset)
    }

    fn heap_val(&self, index: usize) -> &[u8] {
        let offset = self.slot(index);
        let len = self.heap_val_len(offset);
        self.slice(offset - len, len)
    }

    fn heap_key(&self, index: usize) -> &[u8] {
        self.heap_key_at(self.slot(index))
    }

    fn heap_key_at(&self, offset: usize) -> &[u8] {
        let len = self.heap_key_len(offset);
        self.slice(offset + Self::KEY_OFFSET, len)
    }

    fn validate(&self);

    fn heap_write_new(&mut self, key: impl SourceSlice, val: &[u8], write_slot: usize) {
        let kl = Self::KeyLength::from_slice(key).unwrap();
        let vl = Self::ValLength::from_slice(val).unwrap();
        let key = Self::KeyLength::map_insert_slice(key);
        let val = Self::ValLength::map_insert_slice(val);
        debug_assert_eq!(kl.to_usize(), key.len());
        debug_assert_eq!(vl.to_usize(), val.len());
        let size = Self::KEY_OFFSET + val.len() + key.len();
        let new_bump = self.heap_info_mut().bump as usize - size;
        let offset = new_bump + val.len();
        kl.store_unaligned(self.as_page_mut(), offset);
        vl.store_unaligned(self.as_page_mut(), offset + Self::VAL_LEN_OFFSET);
        let key_offset = offset + Self::KEY_OFFSET;
        key.write_to(self.slice_mut(key_offset, kl.to_usize()));
        self.slice_mut(new_bump, val.len()).copy_from_slice(val);
        self.heap_info_mut().bump = new_bump as u16;
        self.set_slot(write_slot, offset);
    }

    fn validate_heap(&self) {
        let record_size_sum: usize =
            (0..self.as_page().common.count as usize).map(|i| self.stored_record_size(i)).sum();
        let calculated = (size_of::<Self>() - self.as_page().common.lower_fence_len as usize
            + self.as_page().common.upper_fence_len as usize)
            - record_size_sum;
        let tracked = self.heap_info().bump as usize + self.heap_info().freed as usize;
        assert_eq!(calculated, tracked);
    }

    fn heap_free(&mut self, index: usize) {
        self.heap_info_mut().freed += self.stored_record_size(index) as u16;
    }
}
