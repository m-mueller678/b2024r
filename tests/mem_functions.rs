use b2024r::{seqlock_accessors, wrap_unchecked, Exclusive, Optimistic};
use std::ptr::slice_from_raw_parts_mut;
#[test]
fn test_memcmp() {
    let samples = vec!["", "a", "aa", "ab", "aaa", "b", "ba", "bb", "bba"];
    for a in &samples {
        for b in &samples {
            let std = a.cmp(b);
            let optimistic = unsafe {
                wrap_unchecked::<Optimistic, [u8]>(a.as_bytes() as *const [u8] as *mut [u8])
                    .cmp(b.as_bytes())
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
            let mut dst = vec![0u8; a.len()];
            wrap_unchecked::<Optimistic, [u8]>(a.as_bytes() as *const [u8] as *mut [u8])
                .load(&mut dst);
            assert_eq!(dst, a.as_bytes());
        }
        wrap_unchecked::<Optimistic, [u8]>(slice_from_raw_parts_mut(core::ptr::NonNull::dangling().as_ptr(), 0))
            .load(&mut []);
    }
}

#[test]
fn test_load() {
    unsafe {
        let a = 0x12345678u32;
        let g = wrap_unchecked::<Optimistic, u32>(&a as *const u32 as *mut u32);
        assert_eq!(a, g.load());
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
