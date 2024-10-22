use std::marker::PhantomData;

#[derive(Clone, Copy)]
pub struct OPtr<'a, T: ?Sized> {
    p: *const T,
    _p: PhantomData<&'a T>,
}

impl OPtr<'_, [u8]> {
    fn load_bytes(self, dst: &mut [u8]) {
        assert_eq!(self.p.len(), dst.len());
        unsafe { std::ptr::copy(self.p as *const u8, dst.as_mut_ptr(), self.p.len()) }
    }
}
