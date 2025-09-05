extern crate core;

use std::sync::Barrier;
use std::{panic, thread};
use std::backtrace::Backtrace;
use std::thread::yield_now;
use bstr::BStr;
use dev_utils::keyset_generator::{BadHeadsKeyset, DenseKeyset, GoodHeadsKeyset, KeyGenerator, ScrambledDenseKeyset};
use dev_utils::tree_utils::check_node_tag_percentage;
use umolc::{BufferManager, SimpleBm};
use umolc_btree::{Page, Tree};



static SET_HOOK: std::sync::Once = std::sync::Once::new();

fn install_panic_hook() {
    SET_HOOK.call_once(|| {
        panic::set_hook(Box::new(|info| {
            eprintln!("\n=== PANIC ===");
            if let Some(loc) = info.location() {
                eprintln!("at {}:{}", loc.file(), loc.line());
            }
            if let Some(s) = info.payload().downcast_ref::<&str>() {
                eprintln!("msg: {s}");
            } else if let Some(s) = info.payload().downcast_ref::<String>() {
                eprintln!("msg: {s}");
            }
            eprintln!("backtrace:\n{}", Backtrace::force_capture());
        }));
    });
}

fn adaptive_promotion_multithreaded<KG: KeyGenerator>(amount: usize, threads: u16, iterations: u16, amount_scans : u16)
{
    let bm = SimpleBm::<Page>::new((amount * threads as usize)/10);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize);

    let keysets = prepare_keyset::<KG>(amount, threads);


    thread::scope(|s| {
        for i in 0..threads {
            let thread_id = i;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;
        s.spawn(move || {
                let mut scrambled = check.clone();
                fastrand::shuffle(&mut scrambled);

                barrier_ref.wait();


                for iteration in 0..iterations {
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
                    }

                    for i in 0..scrambled.len() / 5 {
                        tree_ref.remove(scrambled[i * 5 as usize].0.as_slice());
                    }

                    tree_ref.scan(b"".as_slice(), |key, val| {
                        assert_eq!(6, val.len(), "Lengths did not align!");
                        let id = u16::from_be_bytes(val[4..6].try_into().unwrap());
                        if thread_id == id {
                            let index = u32::from_be_bytes(val[0..4].try_into().unwrap());
                            assert_eq!(check[index as usize].0.as_slice(), key, "Keys dont match!");
                        }


                        false
                    });

                    for _ in 0..amount_scans {
                        tree_ref.scan(b"".as_slice(), |x, val| {
                            false
                        });
                    }
                }

            });
        }
    });
}


#[test]
fn combined_multithread_tests_bad_heads() {
    adaptive_promotion_multithreaded::<BadHeadsKeyset>(1000, 16, 15, 1);
}

#[test]
fn combined_multithread_tests_good_heads() {
    adaptive_promotion_multithreaded::<GoodHeadsKeyset>(1000, 16, 15, 1);
}
#[test]
fn combined_multithread_tests_dense_data() {
    adaptive_promotion_multithreaded::<DenseKeyset::<10000>>(1000, 16, 15, 1);
}

fn point_operations_multithreaded<KG: KeyGenerator>(amount: usize, threads: u16, iterations: u16)
{
    let bm = SimpleBm::<Page>::new((amount * threads as usize)/10);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize);

    let keysets = prepare_keyset::<KG>(amount, threads);


    thread::scope(|s| {
        for i in 0..threads {
            let thread_id = i;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {

                let mut scrambled = check.clone();
                fastrand::shuffle(&mut scrambled);

                barrier_ref.wait();


                for iteration in 0..iterations {
                    for i in 0..scrambled.len() {
                        let (key, value) = scrambled.get(i).unwrap();


                        let mut val: Vec<u8> = b"".to_vec();
                        match iteration % 3 {
                            0 => {
                                tree_ref.insert(key.as_slice(), value.as_slice());
                            },
                            1 => {
                                let res = tree_ref.lookup_to_vec(key.as_slice());
                                if res.is_none() {
                                    println!("Could not find key {:?}!", BStr::new(&key));
                                }
                                assert!(res.is_some());
                                val.extend_from_slice(res.unwrap().as_slice());

                                if 6 != val.len() {
                                    println!("Wrong value found!");
                                }
                                assert_eq!(6, val.len(), "Lengths did not align!");
                                let id = u16::from_be_bytes(val[4..6].try_into().unwrap());
                                if thread_id == id {
                                    let index = u32::from_be_bytes(val[0..4].try_into().unwrap());
                                    if check[index as usize].0.as_slice() != key.as_slice() {
                                        println!("Wrong order of values: index was {index}, but value was {:?}, not {:?}", key.as_slice(), check[index as usize].0.as_slice());
                                    }
                                    assert_eq!(check[index as usize].0.as_slice(), key.as_slice(), "Keys dont match!");
                                }

                            },
                            2 => {
                                let res = tree_ref.remove(key.as_slice());
                                assert!(res.is_some());
                            },
                            _ => unreachable!()
                        }
                    }
                }
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
fn hash_leaf_point_operations_multithreaded() {
    point_operations_multithreaded::<BadHeadsKeyset>(1000, 16, 12);
}

#[test]
fn dense_leaf_point_operations_multithreaded() {
    point_operations_multithreaded::<DenseKeyset::<10000>>(1000, 16, 12);
}

#[test]
fn basic_leaf_point_operations_multithreaded() {
    point_operations_multithreaded::<GoodHeadsKeyset>(1000, 16, 12);
}

fn scan_while_insert<KG: KeyGenerator>(amount: usize, threads: u16) {

    let bm = SimpleBm::<Page>::new((amount * threads as usize)/10);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize + 1);
    let keysets = prepare_keyset::<KG>(amount, threads);

    thread::scope(|s| {
        for i in 0..threads {
            let thread_id = i;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let mut scrambled = check.clone();
                fastrand::shuffle(&mut scrambled);

                barrier_ref.wait();


                for i in 0..scrambled.len() {

                    let (key, value) = scrambled.get(i).unwrap();
                    tree_ref.insert(key.as_slice(), value.as_slice());
                    yield_now();
                }

            });
        }
        let tree_ref = &tree;
        let barrier_ref = &barrier;

        let target = amount * threads as usize;
        s.spawn(move || {
            barrier_ref.wait();

            loop {
                let mut counter: usize = 0;
                tree_ref.scan(b"".as_slice(), |x, x1| {
                    counter += 1;
                    false
                });

                if counter >= target *95 / 100 {
                    break;
                }
            }

        });
    });
}
#[test]
fn hash_leaf_scan_while_insert() {
    scan_while_insert::<BadHeadsKeyset>(1000, 16);
}
#[test]
fn dense_leaf_scan_while_insert() {
    scan_while_insert::<DenseKeyset::<10000>>(1000, 16);
}
#[test]
fn basic_leaf_scan_while_insert() {
    scan_while_insert::<GoodHeadsKeyset>(1000, 16);
}


fn scan_while_lookup<KG: KeyGenerator>(amount: usize, threads: u16) {

    let bm = SimpleBm::<Page>::new((amount * threads as usize)/10);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize + 1);
    let keysets = prepare_keyset::<KG>(amount, threads);

    thread::scope(|s| {
        for i in 0..threads {
            let thread_id = i;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;

            for i in 0..check.len() {

                let (key, value) = check.get(i).unwrap();
                tree_ref.insert(key.as_slice(), value.as_slice());
                yield_now();
            }

            s.spawn(move || {
                let mut scrambled = check.clone();
                fastrand::shuffle(&mut scrambled);

                barrier_ref.wait();

                for _ in 0..10 {
                    for i in 0..scrambled.len() {
                        let (key, value) = scrambled.get(i).unwrap();
                        let res = tree_ref.lookup_to_vec(key.as_slice());

                        let value = value.clone();
                        assert_eq!(Some(value), res);
                        yield_now();
                    }
                }


            });
        }
        let tree_ref = &tree;
        let barrier_ref = &barrier;

        let target = amount * threads as usize;
        s.spawn(move || {
            barrier_ref.wait();

            for _ in 0..10 {
                let mut counter: usize = 0;
                tree_ref.scan(b"".as_slice(), |key, val| {
                    counter += 1;

                    assert_eq!(6, val.len());

                    false
                });

                assert_eq!(counter, target);
                yield_now();
            }

        });
    });
}

#[test]
fn hash_leaf_scan_while_lookup() {
    scan_while_lookup::<BadHeadsKeyset>(1000, 100);
}
#[test]
fn basic_leaf_scan_while_lookup() {
    scan_while_lookup::<GoodHeadsKeyset>(1000, 100);
}
#[test]
fn denses_leaf_scan_while_lookup() {
    scan_while_lookup::<DenseKeyset::<10000>>(1000, 100);
}


fn scan_while_remove<KG: KeyGenerator>(amount: usize, threads: u16) {

    install_panic_hook();

    let bm = SimpleBm::<Page>::new((amount * threads as usize)/10);
    let tree = Tree::new(&bm);


    let barrier = &Barrier::new(threads as usize + 1);
    let keysets = prepare_keyset::<KG>(amount, threads*2);

    thread::scope(|s| {
        for thread in 0..threads*2 {
            let thread_id = thread;
            let check = keysets[thread_id as usize].clone();
            let tree_ref = &tree;
            let barrier_ref = &barrier;

            for i in 0..check.len() {
                let (key, value) = check.get(i).unwrap();
                tree_ref.insert(key.as_slice(), value.as_slice());

            }

            if threads > thread {

                install_panic_hook();
                s.spawn(move || {

                    let mut scrambled = check.clone();
                    fastrand::shuffle(&mut scrambled);

                    barrier_ref.wait();

                    for i in 0..scrambled.len() {
                        let (key, value) = &scrambled[i];
                        let res = tree_ref.remove(key.as_slice());
                        yield_now();
                        assert!(res.is_some());
                    }



                });
            }
        }

        let tree_ref = &tree;
        let barrier_ref = &barrier;

        let max = amount * threads as usize;

        let scan_ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            barrier_ref.wait();

            loop {
                let mut counter = 0;
                tree_ref.scan(b"".as_slice(), |key, val| {
                    counter += 1;
                    false
                });
                if counter <= max && counter > max / 2 {
                    break;
                }
            }

        }));
        assert!(scan_ok.is_ok(), "scan panicked");

    });
}

#[test]
fn hash_leaf_scan_while_remove() {
    scan_while_remove::<BadHeadsKeyset>(1000, 10);
}
#[test]
fn basic_leaf_scan_while_remove() {
    scan_while_remove::<GoodHeadsKeyset>(1000, 10);
}
#[test]
fn dense_leaf_scan_while_remove() {
    scan_while_remove::<DenseKeyset::<10000>>(1000, 10);
}