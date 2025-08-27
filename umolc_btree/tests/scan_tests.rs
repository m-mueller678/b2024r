extern crate core;

use std::cmp::Ordering;
use bytemuck::from_bytes;
use dev_utils::keyset_generator::{BadHeadsKeyset, DenseKeyset, GoodHeadsKeyset, KeyGenerator};
use dev_utils::tree_utils::check_node_tag_percentage;
use umolc_btree::{Page, Tree};
use umolc::SimpleBm;

#[test]
fn basic_scan() {

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);
    let amount_inserts = 10000;



    fn generate_key(i: u32, key_len: usize) -> Vec<u8> {
        if key_len < 8 {
            panic!("Key length must be at least8");
        }
        let mut key= (0..).map(|i| i as u8).take(key_len-8).collect::<Vec<u8>>();
        key.extend_from_slice(&i.to_be_bytes());
        key.extend_from_slice(&i.to_be_bytes());
        key
    }



    for i in 0..amount_inserts {
        let key = generate_key(i, 8);
        let value = i.to_be_bytes().to_vec();
        tree.insert(key.as_slice(), value.as_slice());
    }

    for k in 0..1000{
        let mut i: u32 = k;
        let current_key = generate_key(k,8).clone();
        tree.scan(current_key.as_slice(),
                  |x, x1| {
                      assert_eq!(i.to_be_bytes().as_slice(), x1, "Values dont match on scan");
                      i += 1;
                      false
                  }
        );
        assert_eq!(amount_inserts, i, "The scan did not find all required values.");
    }
}

fn scan_on_node_type<KG: KeyGenerator>(amount: usize, node_tag: u8, margin: f32) {

    let page_count: usize = amount / 100;
    let bm = SimpleBm::<Page>::new(page_count);
    let tree = Tree::new(&bm);

    let mut keyset = KG::generate_keyset(amount);
    keyset.sort_by(|a, b| a.0.cmp(&b.0));

    for (i, (_, val)) in keyset.iter_mut().enumerate() {
        let bytes = (i as u32).to_be_bytes();
        val.clear();
        val.extend_from_slice(&bytes);
    }

    let check = keyset.clone();

    fastrand::shuffle(&mut keyset);

    println!("Prepared Keysets for scan test");

    for (key, val) in keyset.iter() {
        tree.insert(key.as_slice(), val.as_slice());
    }

    check_node_tag_percentage(node_tag, margin, "insert", true, true, &tree);

    for lower in 0..check.len() {
        let mut i = lower as u32;
        let mut first = true;
        tree.scan(check[lower].0.as_slice(),|key, val| {

            if val.len() != 4 {
                panic!("A scanned value was not long enough");
            }

            let mut buffer: [u8; 4] = [0,0,0,0];
            buffer.copy_from_slice(val);

            let index: u32 = u32::from_be_bytes(buffer);
            assert_eq!(i, index, "Scan went in wrong order. Should index {i}, but is {index}!");

            assert_eq!(check[index as usize].0, key, "Keys are in wrong order!");
            assert_eq!(check[index as usize].1, val, "Values are in wrong order!");

            let cmp = check[lower].0.cmp(&key.to_vec());

            assert!(cmp == Ordering::Less || first, "Keys are in wrong order!");
            let cmp = check[lower].1.cmp(&val.to_vec());
            assert!(cmp == Ordering::Less || first, "Values are in wrong order!");

            first = false;
            i+=1;
            false
        })
    }


    for upper in 0..check.len() {
        let mut last: Vec<u8> = Vec::new();
        let mut i = 0;
        tree.scan(check[0].0.as_slice(),|key, val| {

            if val.len() != 4 {
                panic!("A scanned value was not long enough");
            }

            let mut buffer: [u8; 4] = [0,0,0,0];
            buffer.copy_from_slice(val);

            let index: u32 = u32::from_be_bytes(buffer);
            assert_eq!(i, index, "Scan went in wrong order. Should index {i}, but is {index}!");

            assert_eq!(check[index as usize].0, key, "Keys are in wrong order!");
            assert_eq!(check[index as usize].1, val, "Values are in wrong order!");

            let cmp = check[upper].0.cmp(&key.to_vec());

            assert!(cmp == Ordering::Greater || key == check[upper].0, "Keys are in wrong order!");
            let cmp = check[upper].1.cmp(&val.to_vec());
            assert!(cmp == Ordering::Greater || key == check[upper].0, "Values are in wrong order!");

            last = key.to_vec();

            i+=1;

            key == check[upper].0
        });

        assert_eq!(check[upper].0, last, "Did not stop in time.");
    }


}

#[test]
fn test_scan_hash_leaf (){
    scan_on_node_type::<BadHeadsKeyset>(5000, 252, 0.7);
}

#[test]
fn test_scan_basic_leaf (){
    scan_on_node_type::<GoodHeadsKeyset>(5000, 251, 0.7);
}

#[test]
fn test_scan_dense_leaf (){
    scan_on_node_type::<DenseKeyset>(10000, 253, 0.15);
}