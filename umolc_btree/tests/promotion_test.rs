use std::io::Read;
use bstr::ByteSlice;
use umolc_btree::{Page, Tree};

#[test]
fn test_fdl_promotion() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let mut insert_key = |prefix: &[u8], i: u32, insert: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());
        let value = 80085u64.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            assert_eq!(res, Some(value), "Key is not present in HashMap");
            tree.remove(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(tree.lookup_to_vec(&key), None, "KKey is still present and hasn't been removed");
        }
    };
    for i in 0..=100 {
        insert_key(b"Test", i, true);
    }
    for i in 900..=999 {
        insert_key(b"Test", i, true);
    }
    for i in 100..=300 {
        insert_key(b"Test", i, true);
    }

    for i in 600..=900 {
        insert_key(b"Test", i, true);
    }

    for i in 300..=600 {
        insert_key(b"Test", i, true);
    }

    for i in 0..999 {
        insert_key(b"Test", i, false);
    }
}

#[test]
fn test_fdl_demotion() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let mut insert_key = |prefix: &[u8], i: u32, insert: bool, valid_length: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());
        if !valid_length {
            let tmp: u32 = 0;
            key.extend_from_slice(&tmp.to_be_bytes().as_slice());
        }
        let value = 80085u64.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            assert_eq!(res, Some(value), "Key is not present in HashMap");
            tree.remove(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(tree.lookup_to_vec(&key), None, "KKey is still present and hasn't been removed");
        }
    };
    for i in 0..=100 {
        insert_key(b"Test", i, true, true);
    }
    for i in 900..=999 {
        insert_key(b"Test", i, true, true);
    }
    for i in 100..=300 {
        insert_key(b"Test", i, true, true);
    }

    for i in 600..=900 {
        insert_key(b"Test", i, true, true);
    }

    for i in 300..=600 {
        insert_key(b"Test", i, true, true);
    }


    // remove 2/3 values -> it is now sparse enough for a demotion to be possible
    for i in 0..999 {
        if i % 3 != 0 {
            insert_key(b"Test", i, false, true);
        }
    }

    for i in 0..5 {
        println!("Inserting incorrect Keys");
        insert_key(b"Test", i * 200, true, false);
    }
}