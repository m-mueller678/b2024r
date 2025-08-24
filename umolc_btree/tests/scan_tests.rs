extern crate core;

use umolc_btree::{Page, Tree};
use umolc::SimpleBm;
use dev_utils::keyset_generator::{KeyGenerator, BadHeadsKeyset, GoodHeadsKeyset, DenseKeyset};
use dev_utils::tree_utils::check_node_tag_percentage;

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