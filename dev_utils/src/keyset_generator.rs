use std::mem::MaybeUninit;
use bstr::{BStr, BString};
use crate::generate_keys;

pub trait KeyGenerator {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct BadHeadsKeyset;
pub struct GoodHeadsKeyset;

pub struct DenseKeyset;

pub struct ScrambledDenseKeyset;

impl KeyGenerator for BadHeadsKeyset {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {


        // + 4 for index bytes
        let length = (amount.ilog10()/2)*4 + 4;


        if length < 8 || length % 2 != 0 {
            panic!("length must at least 8 bytes and of even length");
        }
        let mut ret :Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(amount);

        for i in 0..amount {
            let mut index = i;
            let mut key = b"".to_vec();
            let mut padding = (length -4)/2;
            while padding > 0 {

                match index % 10 {
                    0 => key.extend_from_slice(b"AA"),
                    1 => key.extend_from_slice(b"BB"),
                    2 => key.extend_from_slice(b"CC"),
                    3 => key.extend_from_slice(b"DD"),
                    4 => key.extend_from_slice(b"EE"),
                    5 => key.extend_from_slice(b"FF"),
                    6 => key.extend_from_slice(b"GG"),
                    7 => key.extend_from_slice(b"HH"),
                    8 => key.extend_from_slice(b"II"),
                    9 => key.extend_from_slice(b"JJ"),
                    _ => unreachable!(),
                }
                index/=10;
                padding -=1;
            }

            let index = (i as u32).to_le_bytes();

            key.extend_from_slice(index.as_slice());

            let value = index.to_vec();
            ret.push((key, value));
        }

        ret
    }
}


impl KeyGenerator for GoodHeadsKeyset {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        // + 4 for index bytes
        let length = (amount.ilog10()/2)*2 + 4;


        let mut ret :Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(amount);

        for i in 0..amount {
            let mut index = i;
            let mut key = b"".to_vec();
            let mut padding = (length -4)/2;
            while padding > 0 {

                match index % 10 {
                    0 => key.extend_from_slice(b"A"),
                    1 => key.extend_from_slice(b"B"),
                    2 => key.extend_from_slice(b"C"),
                    3 => key.extend_from_slice(b"D"),
                    4 => key.extend_from_slice(b"E"),
                    5 => key.extend_from_slice(b"F"),
                    6 => key.extend_from_slice(b"G"),
                    7 => key.extend_from_slice(b"H"),
                    8 => key.extend_from_slice(b"I"),
                    9 => key.extend_from_slice(b"J"),
                    _ => unreachable!(),
                }
                index/=10;
                padding -=1;
            }

            let index = (i as u32).to_le_bytes();

            key.extend_from_slice(index.as_slice());

            let value = index.to_vec();
            ret.push((key, value));
        }

        ret
    }
}


impl KeyGenerator for DenseKeyset {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {

        // we want to have a large amount of dense keys per block to create enough fdls in between
        let DENSE_LENGTH = 10000;

        let dense_fields = amount / DENSE_LENGTH;

        let length = dense_fields.ilog2()+1;

        let words = load_words();



        fn generate_keys (remaining: u32, buffer: &mut Vec<u8>, words: &Vec<String>, DENSE_LENGTH: usize, counter: &mut u32, ret: &mut Vec<(Vec<u8>, Vec<u8>)>) {

            let len = buffer.len();
            if remaining == 0 {
                for i in 0..DENSE_LENGTH {
                    let uuid = counter.to_be_bytes();
                    buffer.extend_from_slice(b"/");
                    buffer.extend_from_slice(&uuid);


                    ret.push((buffer.clone(), counter.to_be_bytes().to_vec()));

                    buffer.truncate(len);
                    *counter += 1;
                }
                return;
            }
            for i in 0..2 {

                let index = fastrand::usize(0..words.len());

                let word = words.get(index).unwrap();

                buffer.extend_from_slice(b"/");
                buffer.extend_from_slice(word.as_bytes());

                generate_keys(remaining-1, buffer, words, DENSE_LENGTH, counter, ret);

                buffer.truncate(len);
            }
        }

        let mut ret :Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(amount);

        let mut buffer = b"".to_vec();

        let mut counter = 0;
        generate_keys(length, &mut buffer, &words, DENSE_LENGTH, &mut counter, &mut ret);

        if ret.len() > amount {
            ret.truncate(amount);
        }

        ret
    }
}

impl KeyGenerator for ScrambledDenseKeyset {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ret = DenseKeyset::generate_keyset(amount);
        fastrand::shuffle(&mut ret);
        ret
    }
}


fn load_words() -> Vec<String> {
    let content = include_str!("word1000.txt");
    content.lines().map(|s| s.to_string()).collect()
}

fn check_collision_percentage <KG: KeyGenerator> (amount: usize){
    let mut keyset = KG::generate_keyset(amount);
    let mut total: f32 = 0.;
    let mut collisions:f32 = 0.;

    keyset.sort_by(|x, x1| {x.0.as_slice().cmp(x1.0.as_slice())});

    for i in 1..=keyset.len()-1 {
        let key1 = &keyset[i].0;
        let key2 = &keyset[i-1].0;

        let head1 = &key1.as_slice()[key1.len().saturating_sub(8)..key1.len().saturating_sub(4)];
        let head2 = &key2.as_slice()[key2.len().saturating_sub(8)..key2.len().saturating_sub(4)];

        println!("Head1: {:?}, Head2: {:?}", BStr::new(&head1), BStr::new(&head2));

        if head2 == head1 {
            collisions += 1.;
        }
        total += 1.;
    }

    println!("Collisions margin: {}", collisions/total);
}

#[test]
fn print_bad_heads_collisions() {
    println!("BadHeads:");
    check_collision_percentage::<BadHeadsKeyset>(5000);
}
