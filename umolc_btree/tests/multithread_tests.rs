extern crate core;

use std::sync::Barrier;
use std::thread;
use rip_shuffle::random_bits::FairCoin;
use dev_utils::keyset_generator::{BadHeadsKeyset, KeyGenerator};
use dev_utils::tree_utils::check_node_tag_percentage;
use umolc::{BufferManager, SimpleBm};
use umolc_btree::{Page, Tree};

fn adaptive_promotion_multithreaded<KG: KeyGenerator>(amount: usize, threads: u16, iterations: u16, amount_scans : u16)
{
    let bm = SimpleBm::<Page>::new((amount * threads as usize) / 50);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize);

    let keysets = prepare_keyset::<KG>(amount, threads);

    println!("Finished setting up keysets!");

    thread::scope(|s| {
        for i in 0..threads {
            let thread_id = i;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let mut scrambled = check.clone();
                fastrand::shuffle(&mut scrambled);

                println!("Thread {thread_id} is waiting!");
                barrier_ref.wait();
                println!("Thread {thread_id} has started!");


                for iteration in 0..iterations {
                    println!("Thread: {thread_id} Iteration {iteration}");
                    for i in 0..scrambled.len() {
                        let (key, value) = scrambled.get(i).unwrap();
                        match iteration % 3 {
                            0 => {
                                tree_ref.insert(key.as_slice(), value.as_slice());
                            },
                            1 => {
                                let res = tree_ref.lookup_to_vec(key.as_slice());
                                assert!(res.is_some() || i % 5 == 0);
                            },
                            2 => {
                                let res = tree_ref.remove(key.as_slice());
                                assert!(res.is_some() || i % 5 == 0);
                            },
                            _ => unreachable!()
                        }

                        if i % scrambled.len() / 4 == 0 {
                            tree_ref.scan(key.as_slice(), |x, val| {
                                false
                            });
                        }
                    }

                    for i in 0..scrambled.len() / 5 {
                        tree_ref.remove(scrambled[i * 5 as usize].0.as_slice());
                    }

                    /*tree_ref.scan(b"".as_slice(), |key, val| {
                        assert_eq!(6, val.len(), "Lengths did not align!");
                        let id = u16::from_be_bytes(val[4..6].try_into().unwrap());
                        if thread_id == id {
                            let index = u32::from_be_bytes(val[0..4].try_into().unwrap());
                            assert_eq!(check[index as usize].0.as_slice(), key, "Keys dont match!");
                        }


                        false
                    });*/

                    for _ in 0..amount_scans {
                        /*tree_ref.scan(b"".as_slice(), |x, val| {
                            false
                        });*/
                    }
                }
                println!("Thread {thread_id} is done!");
                barrier_ref.wait();



            });
        }
    });
}


fn prepare_keyset<KG: KeyGenerator>(amount: usize, threads: u16) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {

    let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = KG::generate_keyset(amount * threads as usize);
    fastrand::shuffle(&mut keyset);

    let mut keysets: Vec<Vec<(Vec<u8>, Vec<u8>)>> = keyset
        .chunks(amount)
        .map(|set| {
            let mut set = set.to_vec();
            set.sort_by(|a, b| a.0.cmp(&b.0));
            set
        })
        .collect();


    for thread in 0..threads {
        let thread_index = thread.to_be_bytes();
        for i in 0..amount as u32 {
            let value_reference = &mut keysets[thread as usize][i as usize].1;
            value_reference.clear();
            value_reference.extend_from_slice(i.to_be_bytes().as_slice());
            value_reference.extend_from_slice(thread_index.as_slice());
        }
    }

    keysets
}


#[test]
fn bad_heads_promotion_multithreaded() {
    adaptive_promotion_multithreaded::<BadHeadsKeyset>(1000, 2, 3, 15);
}