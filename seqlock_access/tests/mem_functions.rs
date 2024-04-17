use b2024r::{seqlock_accessors, wrap_unchecked, Exclusive, Optimistic};
use std::cell::UnsafeCell;
use std::ptr::slice_from_raw_parts_mut;
#[test]
fn test_memcmp() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    for a in &samples {
        let mut a = a.as_bytes().to_vec();
        for b in &samples {
            let std = (*a).cmp(b.as_bytes());
            let optimistic = unsafe {
                wrap_unchecked::<Optimistic, [u8]>(&mut *a as *mut [u8]).cmp(b.as_bytes())
            };
            assert_eq!(std, optimistic);
        }
    }
}

#[test]
fn test_memcpy() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    unsafe {
        for a in &samples {
            let mut a = a.as_bytes().to_vec();
            let mut dst = vec![0u8; a.len()];
            wrap_unchecked::<Optimistic, [u8]>(&mut *a as *mut [u8]).load(&mut dst);
            assert_eq!(&dst, &a);
        }
        wrap_unchecked::<Optimistic, [u8]>(slice_from_raw_parts_mut(
            core::ptr::NonNull::dangling().as_ptr(),
            0,
        ))
        .load(&mut []);
    }
}

#[test]
fn test_load() {
    unsafe {
        let v = 0x12345678u32;
        let a = UnsafeCell::new(v);
        let g = wrap_unchecked::<Optimistic, u32>(a.get());
        let l = g.load();
        eprintln!("l: {l:x}");
        assert_eq!(g.load(), v);
    }
}

struct MyStruct {
    a: u32,
    b: i64,
}

mod macro_impl {
    super::seqlock_accessors!(struct super::MyStruct as pub MyStructWrapper: pub a:u32,pub b:i64);
}

#[test]
fn struct_access() {
    unsafe {
        let x = &mut MyStruct { a: 1, b: 2 };
        let mut x = wrap_unchecked::<Exclusive, MyStruct>(x);
        assert_eq!(x.a().load(), 1);
        assert_eq!(x.b().load(), 2);
    }
}
