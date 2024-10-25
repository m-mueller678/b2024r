use crate::OlcErrorHandler;
use bytemuck::Pod;
use radium::marker::Atomic;
use radium::Radium;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::transmute;
use std::ptr::slice_from_raw_parts;
use std::slice::SliceIndex;
use std::sync::atomic::Ordering::Relaxed;

impl<T: ?Sized, O: OlcErrorHandler> Copy for OPtr<'_, T, O> {}
impl<T: ?Sized, O: OlcErrorHandler> Clone for OPtr<'_, T, O> {
    fn clone(&self) -> Self {
        *self
    }
}

pub struct OPtr<'a, T: ?Sized, O: OlcErrorHandler> {
    p: *const T,
    _p: PhantomData<&'a T>,
    _bm: PhantomData<O>,
}

impl<'a, T: Pod, O: OlcErrorHandler> OPtr<'a, T, O> {
    pub fn from_mut(x: &'a mut T) -> Self {
        OPtr { p: x as *const T, _p: PhantomData, _bm: PhantomData }
    }

    pub fn r(self) -> T
    where
        T: Atomic,
    {
        unsafe { (*(self.p as *const T::Atom)).load(Relaxed) }
    }

    pub fn read_unaligned_nonatomic_u16(self, offset: usize) -> u16 {
        if offset + 2 <= size_of::<Self>() {
            unsafe { ((self.p as *const u8).add(offset) as *const u16).read_unaligned() }
        } else {
            O::optimistic_fail()
        }
    }

    pub fn as_slice<U: Pod>(self) -> OPtr<'a, [U], O> {
        assert_eq!(size_of::<T>() % size_of::<U>(), 0);
        assert!(align_of::<T>() >= align_of::<U>());
        unsafe {
            OPtr {
                p: slice_from_raw_parts(self.p as *const U, size_of::<T>() / size_of::<U>()),
                _p: PhantomData,
                _bm: PhantomData,
            }
        }
    }

    pub unsafe fn project<R>(self, f: impl FnOnce(*const T) -> *const R) -> OPtr<'a, R, O> {
        OPtr { p: f(self.p), _p: PhantomData, _bm: PhantomData }
    }
}

impl<'a, T: Pod, O: OlcErrorHandler> OPtr<'a, [T], O> {
    pub fn i<I: Clone + SliceIndex<[T]> + SliceIndex<[UnsafeCell<T>]>>(
        self,
        i: I,
    ) -> OPtr<'a, <I as SliceIndex<[T]>>::Output, O> {
        unsafe {
            let p = slice_from_raw_parts(self.p as *const UnsafeCell<T>, self.p.len());
            if (*p).get(i.clone()).is_none() {
                // bounds check
                O::optimistic_fail()
            };
            OPtr { p: i.get_unchecked(self.p), _p: PhantomData, _bm: PhantomData }
        }
    }

    pub fn sub(self, offset: usize, len: usize) -> OPtr<'a, [T], O> {
        if offset + len > self.p.len() {
            O::optimistic_fail()
        }
        Self { p: unsafe { slice_from_raw_parts((self.p as *const T).add(offset), len) }, ..self }
    }

    pub fn len(self) -> usize {
        self.p.len()
    }
}

impl<'a, T: Pod, O: OlcErrorHandler, const N: usize> OPtr<'a, [T; N], O> {
    pub fn unsize(self) -> OPtr<'a, [T], O> {
        OPtr { p: self.p.as_slice(), _p: PhantomData, _bm: PhantomData }
    }
}

impl<O: OlcErrorHandler> OPtr<'_, [u8], O> {
    pub fn load_bytes(self, dst: &mut [u8]) {
        assert_eq!(self.p.len(), dst.len());
        unsafe { std::ptr::copy(self.p as *const u8, dst.as_mut_ptr(), self.p.len()) }
    }

    pub fn mem_cmp(self, other: &[u8]) -> Ordering {
        unsafe {
            let cmp_len = self.len().min(other.len());
            let r = libc::memcmp(self.p as *const u8 as *const c_void, other.as_ptr() as *const c_void, cmp_len);
            r.cmp(&0).then(self.len().cmp(&other.len()))
        }
    }
}

#[macro_export]
macro_rules! o_project {
    ($this:ident$(.$member:ident)+) => {
        {
            let ptr:OPtr<_,_> = $this;
            ptr.project(|p|{
                &raw const (*p)$(.$member)+
            })
        }
    };
}
