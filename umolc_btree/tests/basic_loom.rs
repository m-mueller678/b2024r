use loom::thread;
use loom::sync::Arc;

use umolc::SimpleBm;
use umolc_btree::{Page, Tree};

#[test]
fn simple_insert1() {
    let amount = 1;
    let threads = 2;

    loom::model(move || {                  
        // println!("keyword1"); // used for counting amount of permutations
        let bm: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(amount * threads as usize)));
        let tree = Arc::new(Tree::new(bm));

        let tree1 = Arc::clone(&tree);
        let tree2 = Arc::clone(&tree);        

        let values: Arc<Vec<u8>> = Arc::new(vec![1,2,3,4]);
        let values1 = Arc::clone(&values);
        let values2 = Arc::clone(&values);        

        // first thread
        let t1 = thread::spawn(move || {
            let mut key = values1[0].to_le_bytes();
            tree1.insert(&key, &key);                 
        });

        // second thread
        let t2 = thread::spawn(move || {
            let mut key = values2[1].to_le_bytes();
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
fn simple_insert2() {
    let amount = 1;
    let threads = 3;

    loom::model(move || {                  
        // println!("keyword2"); // used for counting amount of permutations
        let bm: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(amount * threads as usize)));
        let tree = Arc::new(Tree::new(bm));

        let tree1 = Arc::clone(&tree);
        let tree2 = Arc::clone(&tree);
        let tree3 = Arc::clone(&tree);

        let values: Arc<Vec<u8>> = Arc::new(vec![1,2,3,4]);
        let values1 = Arc::clone(&values);
        let values2 = Arc::clone(&values);
        let values3 = Arc::clone(&values);

        // first thread
        let t1 = thread::spawn(move || {
            let mut key = values1[0].to_le_bytes();
            tree1.insert(&key, &key);                 
        });

        // second thread
        let t2 = thread::spawn(move || {
            let mut key = values2[1].to_le_bytes();
            tree2.insert(&key, &key);                     
        });

        // third thread
        let t3 = thread::spawn(move || {
            let mut key = values3[2].to_le_bytes();
            tree3.insert(&key, &key);                     
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
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
fn simple_insert3() {
    let amount = 2;
    let threads = 2;

    loom::model(move || {                  
        // println!("keyword3"); // used for counting amount of permutations
        let bm: &'static SimpleBm<Page> = Box::leak(Box::new(SimpleBm::<Page>::new(amount * threads as usize)));
        let tree = Arc::new(Tree::new(bm));

        let tree1 = Arc::clone(&tree);
        let tree2 = Arc::clone(&tree);        

        let values: Arc<Vec<u8>> = Arc::new(vec![1,2,3,4]);
        let values1 = Arc::clone(&values);
        let values2 = Arc::clone(&values);        

        // first thread
        let t1 = thread::spawn(move || {
            let mut key = values1[0].to_le_bytes();
            tree1.insert(&key, &key);
            let mut key = values1[2].to_le_bytes();
            tree1.insert(&key, &key);
        });

        // second thread
        let t2 = thread::spawn(move || {
            let mut key = values2[1].to_le_bytes();
            tree2.insert(&key, &key);
            let mut key = values2[3].to_le_bytes();
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
        // println!("keyword4"); // used for counting amount of permutations
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
