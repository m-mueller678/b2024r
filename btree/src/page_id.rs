use crate::tree::Page;
use std::ptr;

pub struct PageId(u64);

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("only little endian 64-bit is supported");

impl PageId {
    pub fn to_3x16(self) -> [u16; 3] {
        let shifted = self.0 >> 12;
        debug_assert!(shifted < (1 << 48));
        let a = bytemuck::cast::<[u8; 8], [u16; 4]>(shifted.to_ne_bytes());
        [a[0], a[1], a[2]]
    }

    pub fn from_3x16(x: [u16; 3]) -> Self {
        let a = bytemuck::cast::<[u16; 4], [u8; 8]>([x[0], x[1], x[2], 0]);
        Self(u64::from_ne_bytes(a) << 12)
    }

    pub fn to_ptr(self) -> *mut Page {
        ptr::with_exposed_provenance_mut(self.0 as usize)
    }

    pub fn from_ptr(p: *mut Page) -> Self {
        Self(p.expose_provenance() as u64)
    }
}
