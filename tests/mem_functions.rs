use b2024r::{wrap_unchecked, Optimistic};
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
        wrap_unchecked::<Optimistic, [u8]>(slice_from_raw_parts_mut(ptr::null_mut(), 0))
            .load(&mut []);
    }
}
