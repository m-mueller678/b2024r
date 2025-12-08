use loom::thread;
use loom::thread::yield_now;
use loom::sync::Arc;

use dev_utils::keyset_generator::{GoodHeadsKeyset, KeyGenerator};
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};

// fn adaptive_promotion_multithreaded<KG: KeyGenerator>(amount: usize, threads: u16, amount_scans: u16) {
//     loom::model(move || {
//         let bm_static: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(amount * threads as usize)));
//         let tree = Arc::new(Tree::new(bm_static));

//         let mut handles = Vec::new();

//         let keysets = prepare_keyset::<KG>(amount, threads);

//         for i in 0..threads {
//             let thread_id = i;
//             let check = keysets[thread_id as usize].clone();
//             let tree_ref = Arc::clone(&tree);
            
//             let handle = thread::spawn(move || {            

//                 let mut scrambled = check.clone();
//                 fastrand::shuffle(&mut scrambled);

//                 let iterations = 3;
//                 for iteration in 0..iterations {
//                     for i in 0..scrambled.len() {
//                         let (key, value) = scrambled.get(i).unwrap();
//                         match iteration % 3 {
//                             0 => {
//                                 tree_ref.insert(key.as_slice(), value.as_slice());
//                             },
//                             1 => {
//                                 let res = tree_ref.lookup_to_vec(key.as_slice());
//                                 assert!(res.is_some() || i % 5 == 0);
//                             },
//                             2 => {
//                                 let res = tree_ref.remove(key.as_slice());
//                                 assert!(res.is_some() || i % 5 == 0);
//                             },
//                             _ => unreachable!()
//                         }
//                     }

//                     for i in 0..scrambled.len() / 5 {
//                         tree_ref.remove(scrambled[i * 5 as usize].0.as_slice());
//                     }

//                     tree_ref.scan(b"".as_slice(), |key, val| {
//                         assert_eq!(6, val.len(), "Lengths did not align!");
//                         let id = u16::from_be_bytes(val[4..6].try_into().unwrap());
//                         if thread_id == id {
//                             let index = u32::from_be_bytes(val[0..4].try_into().unwrap());
//                             assert_eq!(check[index as usize].0.as_slice(), key, "Keys dont match!");
//                         }


//                         false
//                     });

//                     for _ in 0..amount_scans {
//                         tree_ref.scan(b"".as_slice(), |x, val| {
//                             false
//                         });
//                     }
//                 }
//             });
//             handles.push(handle);
//         }
//         for handle in handles {
//             handle.join().unwrap();
//         }

//     });
// }

// #[test]
// fn firstTest() {
//     let amount: usize = 5;
//     let threads: u16 = 2;
//     let amount_scans = 1;
    
//     adaptive_promotion_multithreaded::<GoodHeadsKeyset>(amount, threads, amount_scans);
// }

#[test]
fn simple_insert() {
    let amount = 4;
    let threads = 2;

    loom::model(move || {          
        let bm: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(amount * threads as usize)));
        let tree = Arc::new(Tree::new(bm));

        let tree1 = Arc::clone(&tree);
        let tree2 = Arc::clone(&tree);

        let values: Arc<Vec<u8>> = Arc::new(vec![1,2,3,4,5,6,7,8]);
        let values1 = Arc::clone(&values);
        let values2 = Arc::clone(&values);

        // first thread
        let t1 = thread::spawn(move || {
            let mut key = values1[0].to_le_bytes();
            tree1.insert(&key, &key);
            key = values1[1].to_le_bytes();
            tree1.insert(&key, &key);
            key = values1[2].to_le_bytes();
            tree1.insert(&key, &key);
            key = values1[3].to_le_bytes();
            tree1.insert(&key, &key);
        });

        // second thread
        let t2 = thread::spawn(move || {
            let mut key = values2[4].to_le_bytes();
            tree2.insert(&key, &key);
            key = values2[5].to_le_bytes();
            tree2.insert(&key, &key);
            key = values2[6].to_le_bytes();
            tree2.insert(&key, &key);
            key = values2[7].to_le_bytes();
            tree2.insert(&key, &key);
        });

        t1.join().unwrap();
        t2.join().unwrap();
        for i in 0..(amount * threads) {
            let value = values[i];
            let key = value.to_le_bytes();
            let res = tree.lookup_to_vec(&key);
            assert!(res.is_some(), "stored value was none for key {}", value);        
            assert!((res.unwrap())[0] == value, "value was wrong. Should have been {}!", value);
        }
    });
}

#[test]
fn simple_lookup() {
    loom::model(move || {
        let bm: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(4)));
        let tree = Arc::new(Tree::new(bm));

        let tree1 = Arc::clone(&tree);
        let tree2 = Arc::clone(&tree);

        let values: Arc<Vec<u8>> = Arc::new(vec![1,2]);
        let values1 = Arc::clone(&values);
        let values2 = Arc::clone(&values);

        let value = values[0];
        let key = value.to_le_bytes();
        tree.insert(&key, &key);        

        // first thread that updates the value
        let t1 = thread::spawn(move || {
            let key = values1[0].to_le_bytes();
            let val = values1[1].to_le_bytes();
            tree1.insert(&key, &val);
        });

        // second thread that reads the value
        let t2 = thread::spawn(move || {
            let key = values2[0].to_le_bytes();
            return tree2.lookup_to_vec(&key);
        });

        t1.join().unwrap();
        let ret = t2.join().unwrap();

        assert!(ret.is_some(), "stored value was none for key {}", value);
        let res = ret.unwrap();
        assert!(res[0] == values[0] || res[0] == values[1], "value was wrong. It was {}", res[0]);
    });
}

// fn prepare_keyset<KG: KeyGenerator>(amount: usize, threads: u16) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {

//     let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = KG::generate_keyset(amount * threads as usize);
//     fastrand::shuffle(&mut keyset);

//     let mut keysets: Vec<Vec<(Vec<u8>, Vec<u8>)>> = keyset
//         .chunks(amount)
//         .map(|set| {
//             let mut set = set.to_vec();
//             set.sort_by(|a, b| a.0.cmp(&b.0));
//             set
//         })
//         .collect();


//     for thread in 0..threads {
//         let thread_index = thread.to_be_bytes();
//         for i in 0..amount as u32 {
//             let value_reference = &mut keysets[thread as usize][i as usize].1;
//             value_reference.clear();
//             value_reference.extend_from_slice(i.to_be_bytes().as_slice());
//             value_reference.extend_from_slice(thread_index.as_slice());
//         }
//     }

//     keysets
// }