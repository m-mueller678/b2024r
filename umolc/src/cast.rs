use crate::{OlcAtomic, OlcSafe};
use radium::marker::Atomic;
use std::mem::transmute;

pub fn cast_slice<A: OlcSafe, B: OlcSafe>(a: &[A]) -> &[B] {
    let b_ptr = a.as_ptr() as *const B;
    assert!(b_ptr.is_aligned());
    assert!(size_of::<A>() * a.len() % size_of::<B>() == 0);
    unsafe { std::slice::from_raw_parts(b_ptr, size_of::<A>() * a.len() / size_of::<B>()) }
}

pub fn cast_slice_mut<A: OlcSafe, B: OlcSafe>(a: &mut [A]) -> &mut [B] {
    let b_ptr = a.as_mut_ptr() as *mut B;
    assert!(b_ptr.is_aligned());
    assert!(size_of::<A>() * a.len() % size_of::<B>() == 0);
    unsafe { std::slice::from_raw_parts_mut(b_ptr, size_of::<A>() * a.len() / size_of::<B>()) }
}

pub fn cast_slice_non_atomic<T: Atomic>(a: &mut [OlcAtomic<T>]) -> &mut [T] {
    unsafe { transmute(a) }
}

pub fn cast_ref<A: OlcSafe, B: OlcSafe>(a: &A) -> &B {
    let b_ptr = a as *const A as *const B;
    assert!(b_ptr.is_aligned());
    assert!(size_of::<A>() == size_of::<B>());
    unsafe { &*b_ptr }
}

pub fn cast_mut<A: OlcSafe, B: OlcSafe>(a: &mut A) -> &mut B {
    let b_ptr = a as *mut A as *mut B;
    assert!(b_ptr.is_aligned());
    assert!(size_of::<A>() == size_of::<B>());
    unsafe { &mut *b_ptr }
}
