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
            assert_eq!(res, Some(value));
            tree.remove(&key);
            assert_eq!(tree.lookup_to_vec(&key), None);
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