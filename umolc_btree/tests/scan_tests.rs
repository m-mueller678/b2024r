extern crate core;

use umolc::{BufferManager, CommonSeqLockBM, OPtr};
use umolc_btree::{Page, Tree};
use umolc::SimpleBm;


#[test]
fn basic_scan() {

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);



    fn generate_key(i: u32, key_len: usize) -> Vec<u8> {
        if key_len < 8 {
            panic!("Key length must be at least8");
        }
        let mut key= (0..).map(|i| i as u8).take(key_len-8).collect::<Vec<u8>>();
        key.extend_from_slice(&i.to_be_bytes());
        key.extend_from_slice(&i.to_be_bytes());
        key
    }



    for i in 0..10000 {
        let key = generate_key(i, 8);
        let value = i.to_be_bytes().to_vec();
        tree.insert(key.as_slice(), value.as_slice());
    }

    let first_key = generate_key(0,8).clone();



    tree.scan_node_types(first_key.as_slice(), |x,scan_counter| {
        println!("{:?}: {:?}", x, if scan_counter == 255 { "has good heads"} else { "does not have good heads" });

        false
    });

    for k in 0..1000{
        let mut i: u32 = k;
        let current_key = generate_key(k,8).clone();
        tree.scan(current_key.as_slice(),
                  |x, x1| {
                      assert_eq!(i.to_be_bytes().as_slice(), x1, "Values dont match on scan");
                      i += 1;
                      false
                  }
        )
    }
}

#[test]
fn adaptive_promotion () {
    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    fn generate_key(i: u32, key_len: usize) -> Vec<u8> {
        if key_len < 4 {
            panic!("Key length must be at least 4");
        }
        let mut key= (0..).map(|i| i as u8).take(key_len-4).collect::<Vec<u8>>();
        key.extend_from_slice(&i.to_be_bytes());
        key
    }   



    for r in 0..200 {
        for i in 0..100 {
            let key = generate_key(i, 4);
            let value = i.to_be_bytes().to_vec();
            tree.insert(key.as_slice(), value.as_slice());
        }
        // we just overwrite the values, doesnt matter
        for i in 0..100 {
            let key = generate_key(i, 4);
            let value = i.to_be_bytes().to_vec();
            tree.insert(key.as_slice(), value.as_slice());
        }
        for i in 0..100 {
            let key = generate_key(i, 4);
            let value = i.to_be_bytes().to_vec();
            tree.insert(key.as_slice(), value.as_slice());
        }
        for i in 0..100 {
            let key = generate_key(i, 4);
            let value = i.to_be_bytes().to_vec();
            tree.insert(key.as_slice(), value.as_slice());
        }


        tree.scan_node_types(generate_key(0,4).as_slice(), |x,scan_counter| {
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }
            assert_eq!(252, x, "Node Tag should be 252 = HashLeaf after spamming inserts");
            false
        });

        println!("Starting to spam scans");

        for i in 0..5000 {
            let key = generate_key(0, 4);
            tree.scan(key.as_slice(), |x,val| {
                true
            });
        }


        tree.scan_node_types(generate_key(0,4).as_slice(), |x,scan_counter| {
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }
            assert_eq!(251, x, "Node Tag should be 251 = Basic_Leaf after spamming scans");
            false
        });
    }
}