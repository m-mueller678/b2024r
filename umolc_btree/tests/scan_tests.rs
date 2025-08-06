use umolc::{BufferManager, CommonSeqLockBM, OPtr};
use umolc_btree::{Page, Tree};
use umolc::SimpleBm;


#[test]
fn basic_scan() {

    const PAGE_COUNT: usize = 512;
    let bm = SimpleBm::<Page>::new(PAGE_COUNT);
    let tree = Tree::new(&bm);


    let mut list: Vec<(Vec<u8>, &[u8])> = Vec::new();

    fn generate_key(i: u32, key_len: usize) -> Vec<u8> {
        if key_len < 4 {
            panic!("Key length must be at least 4");
        }
        let mut key= (0..).map(|i| i as u8).take(key_len-4).collect::<Vec<u8>>();
        key.extend_from_slice(&i.to_be_bytes());
        key
    }

    let value = generate_key(0, 4);

    for i in 0..10000 {
        let key = generate_key(i, 8);
        list.push((key.clone(), value.as_slice()));
        tree.insert(key.as_slice(), value.as_slice());
    }

    let first_key = generate_key(0,8).clone();

    let mut i = 0;
    tree.scan(first_key.as_slice(),
              |x, x1| {
                  println!("Scan {:?}: {:?}->{:?}", i, x, x1.to_vec());
                  i += 1;
                  false
              }
    )

}