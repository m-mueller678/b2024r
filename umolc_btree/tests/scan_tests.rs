extern crate core;

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

    fastrand::seed(42);
    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);

    let amount_keys = 10000;

    let generate_key = |i: u32| -> Vec<u8> {
        let mut key = match i % 10 {
            0 => "AA".as_bytes().to_vec(),
            2 => "CC".as_bytes().to_vec(),
            3 => "DD".as_bytes().to_vec(),
            1 => "BB".as_bytes().to_vec(),
            4 => "EE".as_bytes().to_vec(),
            5 => "FF".as_bytes().to_vec(),
            6 => "GG".as_bytes().to_vec(),
            7 => "HH".as_bytes().to_vec(),
            8 => "II".as_bytes().to_vec(),
            9 => "JJ".as_bytes().to_vec(),
            _ => "KK".as_bytes().to_vec(),
        };
        let mut key = match (i/10) % 10 {
            0 => "AA".as_bytes().to_vec(),
            2 => "CC".as_bytes().to_vec(),
            3 => "DD".as_bytes().to_vec(),
            1 => "BB".as_bytes().to_vec(),
            4 => "EE".as_bytes().to_vec(),
            5 => "FF".as_bytes().to_vec(),
            6 => "GG".as_bytes().to_vec(),
            7 => "HH".as_bytes().to_vec(),
            8 => "II".as_bytes().to_vec(),
            9 => "JJ".as_bytes().to_vec(),
            _ => "KK".as_bytes().to_vec(),
        };
        key.extend_from_slice(&i.to_be_bytes());
        key
    };



    for iteration in 0..30 {
        for i in 0..amount_keys {
            let key = generate_key(i);
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

            if i % 250 == 0 {
                // we randomly scan to trigger the sorting logic.

                tree.scan(key.as_slice(), |x,val| {
                    true
                });
            }
        }


        if iteration % 3 == 0 {
            // if the nodes are full, the hash_leaf might be unable to promote to a basic_leaf
            for i in 0..amount_keys/5 {
                let key = generate_key(i);
                tree.remove(key.as_slice());
            }
        }

        let mut total_count: f32 = 0.0;
        let mut correct : f32 = 0.0;
        let action = match iteration%3 { 0=> "insert", 1=> "lookup", 2=> "remove", _ => unreachable!() };
        tree.scan_node_types(generate_key(0).as_slice(), |x,scan_counter| {
            total_count += 1.0;
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }


            if x == 252 {
                correct += 1.0;
            }
            false
        });

        let margin = correct/total_count;

        assert!(margin > 0.8, "Not enough Nodes had the correct Tag after spamming {action}: 80 > {:?}%", margin*100.0);


        println!("Promoted correctly after spamming {action} with {:?}% correct nodes", margin*100.0);

        println!("Starting to spam scans");


        //This has a decent chance to not promote the node properly, which is why we use seeded fastrand. If this test still fails, one could think about increasing this number, it would make the test more failsafe, but would increase time
        for _ in 0..100 {
            let key = generate_key(0);
            tree.scan(key.as_slice(), |x,val| {
                false
            });
        }


        let mut total_count: f32 = 0.0;
        let mut correct : f32 = 0.0;
        tree.scan_node_types(generate_key(0).as_slice(), |x,scan_counter| {
            if scan_counter == 255 {
                panic!("Node should not have good heads by default");
            }
            if x == 251 {
                correct += 1.0;
            }
            false
        });

        let margin = correct/total_count;

        assert!(margin > 0.8, "Not enough Nodes had the correct Tag after spamming scans: 80 > {:?}%", margin*100.0);
        println!("Promoted correctly after spamming scans with {:?}% correct nodes", margin*100.0);
    }
}