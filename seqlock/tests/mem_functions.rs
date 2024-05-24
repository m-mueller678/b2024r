use seqlock::{seqlock_wrapper, Exclusive, Guarded, Optimistic, SeqLockWrappable, SeqlockAccessors};
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::ptr::slice_from_raw_parts_mut;

#[test]
fn test_memcmp() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    for a in &samples {
        let mut a = a.as_bytes().to_vec();
        for b in &samples {
            let std = (*a).cmp(b.as_bytes());
            let optimistic =
                unsafe { Guarded::<Optimistic, [u8]>::wrap_unchecked(&mut *a as *mut [u8]).mem_cmp(b.as_bytes()) };
            assert_eq!(std, optimistic);
        }
    }
}

#[test]
fn test_memcpy() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    unsafe {
        for src in &samples {
            let mut a = src.as_bytes().to_vec();
            let mut dst = vec![0u8; a.len()];
            Guarded::<Optimistic, [u8]>::wrap_mut(&mut *a).load_slice(&mut dst);
            assert_eq!(&dst, &src.as_bytes());
        }
    }
}

#[test]
fn test_load() {
    unsafe {
        let v = 0x12345678u32;
        let a = UnsafeCell::new(v);
        let g = Guarded::<Optimistic, u32>::wrap_unchecked(a.get());
        let l = g.load();
        assert_eq!(l, v);
    }
}

seqlock_wrapper!(MyWrapper);

#[derive(SeqlockAccessors)]
#[seq_lock_wrapper(MyWrapper)]
struct MyStruct {
    a: u32,
    b: i64,
}

#[allow(dead_code)]
#[derive(SeqlockAccessors)]
#[seq_lock_wrapper(MyWrapper)]
struct MyStructGeneric<T: Deref + SeqLockWrappable, U>
where
    T::Target: Deref,
{
    #[allow(dead_code)]
    x: T,
    #[seq_lock_skip_accessor]
    #[allow(dead_code)]
    u: U,
}

#[test]
fn struct_access() {
    unsafe {
        let x = &mut MyStruct { a: 1, b: 2 };
        let mut x = Guarded::<Exclusive, MyStruct>::wrap_mut(x);
        assert_eq!(x.a_mut().load(), 1);
        assert_eq!(x.b_mut().load(), 2);
        assert_eq!(x.s().a().load(), 1);
        assert_eq!(x.optimistic().a_mut().load(), 1);
    }
}

#[test]
fn load_to_vec() {
    let src = [1, 2, 3, 4u8];
    let x = &mut src.clone();
    let mut x = Guarded::<Exclusive, [u8; 4]>::wrap_mut(x);
    let x = x.optimistic().as_slice();
    assert_eq!(x.load_slice_to_vec(), src);
}
