use dev_utils::keyset_generator::{BadHeadsKeyset, DenseKeyset, GoodHeadsKeyset, KeyGenerator};
use dev_utils::tree_utils::check_node_tag_percentage;
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};


#[test]
fn fdl_promotion() {
    use crate::Tree;
    use crate::Page;
    use umolc::SimpleBm;

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let insert_key = |prefix: &[u8], i: u32, insert: bool| {
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
            assert_eq!(tree.lookup_to_vec(&key), None, "Key is {} still present and hasn't been removed", key_index);
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
            assert_eq!(Some(value.clone()), res, "Key {:?} is not present in HashMap", value);
            tree.remove(&key);
            let index_bytes: [u8; 4] = key[(b"Test").len()..].try_into().expect("Key does not contain valid u32 suffix");
            let key_index = u32::from_be_bytes(index_bytes);
            assert_eq!(tree.lookup_to_vec(&key), None, "Key {} is still present and hasn't been removed", key_index);
        }
    };
    for i in 0..=100 {
        insert_key(b"Test", 2*i, true, true);
        insert_key(b"Test", 2*i+1, true, true);
    }
    for i in 900..=1000 {
        insert_key(b"Test", 2*i, true, true);
        insert_key(b"Test", 2*i+1, true, true);
    }
    for i in 100..=300 {
        insert_key(b"Test", 2*i, true, true);
        insert_key(b"Test", 2*i+1, true, true);
    }

    for i in 600..=900 {
        insert_key(b"Test", 2*i, true, true);
        insert_key(b"Test", 2*i+1, true, true);
    }

    for i in 300..=600 {
        insert_key(b"Test", 2*i, true, true);
        insert_key(b"Test", 2*i+1, true, true);
    }

    check_node_tag_percentage(253, 0.5, "insert", true, &tree);

    // remove 2/3 values -> it is now sparse enough for a demotion to be possible
    for i in 0..999 {
        if i % 3 != 0 {
            insert_key(b"Test", 2*i, false, true);
            insert_key(b"Test", 2*i+1, false, true);
        }
    }

    for i in 0..20 {
        println!("Inserting incorrect Keys");
        insert_key(b"Test", i * 100, true, false);
    }
    check_node_tag_percentage(251, 0.45, "insert", true, &tree);

    for i in 0..999 {
        if i % 3 == 0 {
            insert_key(b"Test", 2*i, false, true);
            insert_key(b"Test", 2*i+1, false, true);
        }
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

    let insert_key = |prefix: &[u8], i: u32, insert: bool| {
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
            assert_eq!(tree.lookup_to_vec(&key), None, "Key {} is still present and hasn't been removed", key_index);
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



fn adaptive_promotion<KG: KeyGenerator>(point_op_tag: u8, scan_tag: u8, allow_good_heads: bool, amount_keys: usize, PAGE_COUNT: usize, iterations: usize, margin: f32) {

    fastrand::seed(42);
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let keyset: Vec<(Vec<u8>, Vec<u8>)> = KG::generate_keyset(amount_keys);

    let first_key = b"\0";

    for iteration in 0..iterations {
        for i in 0..amount_keys {
            let (key, value) = keyset.get(i).unwrap();
            match iteration%3 {
                0 => {
                    tree.insert(key.as_slice(), value.as_slice());
                },
                1 => {
                    tree.lookup_to_vec(key.as_slice());
                },
                2 => {
                    tree.remove(key.as_slice());
                },
                _ => unreachable!()
            }

            if i % amount_keys/4 == 0 {
                tree.scan(key.as_slice(), |x,val| {
                    false
                });
            }
        }


        if iteration % 3 == 0 {
            let mut x = 0;
            tree.scan(first_key.as_slice(), |_,_| {
                x+=1;
                false
            });

            assert_eq!(amount_keys, x, "The scan did not find all required values.");

            for i in 0..amount_keys/5 {
                let (key, _) = keyset.get(i).unwrap();
                tree.remove(key.as_slice());
            }
        }

        let action = match iteration%3 { 0=> "insert", 1=> "lookup", 2=> "remove", _ => unreachable!() };
        check_node_tag_percentage(point_op_tag, margin, action, allow_good_heads, &tree);


        for _ in 0..100 {
            tree.scan(first_key.as_slice(), |x,val| {
                false
            });
        }


        check_node_tag_percentage(scan_tag, margin, "scan", allow_good_heads, &tree);

    }
}


#[test]
fn adaptive_promotion_bad_heads () {
    adaptive_promotion::<BadHeadsKeyset>(252, 251, false, 10000, 512, 30, 0.8);
}

#[test]
fn adaptive_promotion_good_heads () {
    adaptive_promotion::<GoodHeadsKeyset>(251, 251, true, 10000, 512, 30, 0.8);
}

#[test]
fn adaptive_promotion_dense_heads () {
    adaptive_promotion::<DenseKeyset>(253, 253, true, 100000, 4096, 6, 0.75);
}

