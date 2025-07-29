use bstr::ByteSlice;
use umolc::{BufferManager, CommonSeqLockBM, OPtr};
use umolc_btree::{Page, Tree};


#[test]
fn fdl_promotion() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let mut insert_key = |prefix: &[u8], i: u32, insert: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());
        let value = i.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            assert_eq!(res, Some(value), "Key is not present in HashMap");
            tree.remove(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(tree.lookup_to_vec(&key), None, "Key is still present and hasn't been removed");
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
fn fdl_demotion() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let insert_key = |prefix: &[u8], i: u32, insert: bool, valid_length: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());
        if !valid_length {
            let tmp: u32 = 0;
            key.extend_from_slice(&tmp.to_be_bytes().as_slice());
        }
        let value = i.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            assert_eq!(res, Some(value), "Key is not present in HashMap");
            tree.remove(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(tree.lookup_to_vec(&key), None, "Key is still present and hasn't been removed");
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


#[test]
fn fdl_split_high() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 1024;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let mut insert_key = |prefix: &[u8], i: u32, insert: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());
        let value = i.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(res, Some(value), "Key {key_index} is not present in HashMap");
            tree.remove(&key);
            assert_eq!(tree.lookup_to_vec(&key), None, "Key {key_index} is still present and hasn't been removed");
        }
    };

    for i in 0..=100 {
        for j in 0..=20 {
            insert_key(b"Test", i * 20 + j, true);
            insert_key(b"Test", (100*20*2) - (i * 20 + j), true);
        }
    }

    for i in 0..4001 {
        insert_key(b"Test", i, false);
    }
}

#[test]
fn fdl_split_half() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 1024;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let mut insert_key = |prefix: &[u8], i: u32, insert: bool, correct_len: bool| {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&i.to_be_bytes());

        if !correct_len {
            key.extend_from_slice("Test".as_bytes().iter().as_slice());
        }

        let value = i.to_le_bytes().to_vec();
        if(insert) {
            tree.insert(&key, &value);
        }
        else {
            let res = tree.lookup_to_vec(&key);
            assert_eq!(res, Some(value), "Key is not present in HashMap");
            tree.remove(&key);
            assert_eq!(tree.lookup_to_vec(&key), None, "Key is still present and hasn't been removed");
        }
    };

    for i in 0..=60 {
        for j in 0..=20 {
            insert_key(b"Test", i * 20 + j, true, true );
            insert_key(b"Test", (100*20*2) - (i * 20 + j), true, true);
        }
    }


    for i in 0..60*20+1 {
        insert_key(b"Test", i, true, false);
    }



    for i in 0..60*20+1 {
        insert_key(b"Test", i, false, false);
    }
}

