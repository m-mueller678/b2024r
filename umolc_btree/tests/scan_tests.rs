extern crate core;

use umolc::{BufferManager, CommonSeqLockBM, OPtr};
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
        );
        assert_eq!(amount_inserts, i, "The scan did not find all required values.");
    }
}

#[test]
fn adaptive_promotion () {
    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let amount_keys = 200;

    let generate_key = |i: u32, key_len: usize| -> Vec<u8> {
        if key_len < 4 {
            panic!("Key length must be at least 4");
        }
        let mut key = match i % 10 {
            0 => "AAAA".as_bytes().to_vec(),
            2 => "CCCC".as_bytes().to_vec(),
            3 => "DDDD".as_bytes().to_vec(),
            1 => "BBBB".as_bytes().to_vec(),
            4 => "EEEE".as_bytes().to_vec(),
            5 => "FFFF".as_bytes().to_vec(),
            6 => "GGGG".as_bytes().to_vec(),
            7 => "HHHH".as_bytes().to_vec(),
            8 => "IIII".as_bytes().to_vec(),
            9 => "JJJJ".as_bytes().to_vec(),
            _ => "KKKK".as_bytes().to_vec(),
        };
        key.extend_from_slice(&i.to_be_bytes());
        key
    };



    for iteration in 0..20 {
        for i in 0..amount_keys {
            let key = generate_key(i, 4);
            let value = i.to_be_bytes().to_vec();
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
        }


        tree.scan_node_types(generate_key(0,4).as_slice(), |x,scan_counter| {
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }

            let action = match iteration%3 { 0=> "insert", 1=> "lookup", 2=> "remove", _ => unreachable!() };

            assert_eq!(252, x, "Iteration {iteration}: Node Tag should be 252 = HashLeaf after spamming {action}. Scan counter: {scan_counter}");
            false
        });

        println!("Starting to spam scans");

        for _ in 0..5000 {
            let key = generate_key(0, 4);
            tree.scan(key.as_slice(), |x,val| {
                true
            });
        }


        tree.scan_node_types(generate_key(0,4).as_slice(), |x,scan_counter| {
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }
            assert_eq!(251, x, "Iteration {iteration}: Node Tag should be 251 = Basic_Leaf after spamming scans. Scan counter: {scan_counter}");
            false
        });
    }
}