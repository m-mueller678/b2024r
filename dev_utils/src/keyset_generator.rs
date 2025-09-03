use std::mem::MaybeUninit;
use std::u32::MAX;
use bstr::{BStr, BString};
use crate::generate_keys;

pub trait KeyGenerator {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct BadHeadsKeyset;
pub struct GoodHeadsKeyset;
pub struct DenseKeyset<const DENSE_LENGTH: usize>;
pub struct ScrambledDenseKeyset;
pub struct BadHeadsPercentage<const PERCENTAGE: u8>;

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


impl <const DENSE_LENGTH: usize>KeyGenerator for DenseKeyset<DENSE_LENGTH> {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {


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
        let mut ret = DenseKeyset::<10000>::generate_keyset(amount);
        fastrand::shuffle(&mut ret);
        ret
    }
}


fn load_words() -> Vec<String> {
    let content = include_str!("word1000.txt");
    content.lines().map(|s| s.to_string()).collect()
}

// checks probable heads position (key[4..8] is seen as heads)
fn check_collision_percentage <KG: KeyGenerator> (amount: usize) -> f32 {
    let mut keyset = KG::generate_keyset(amount);
    let mut total: f32 = 0.;
    let mut collisions: f32 = 0.;

    keyset.sort_by(|x, x1| { x.0.as_slice().cmp(x1.0.as_slice()) });

    for i in 1..=keyset.len() - 1 {
        let key1 = &keyset[i].0;
        let key2 = &keyset[i - 1].0;

        let head1 = &key1.as_slice()[4..8];
        let head2 = &key2.as_slice()[4..8];


        if head2 == head1 {
            collisions += 1.;
        }
        total += 1.;
    }

    collisions / total
}


// this is not a real test, it just checks how bad the collisions are for keysets and can be used however you like.
// if you want to create your own keyset, just slap it in here to get the percentage of collisions
#[test]
fn print_bad_heads_collisions() {
    let res = check_collision_percentage::<BadHeadsPercentage::<0>>(5000);
    assert!(res < 0.01);
    let res = check_collision_percentage::<BadHeadsPercentage::<10>>(5000);
    assert!(res < 0.11 && res > 0.09);
    let res = check_collision_percentage::<BadHeadsPercentage::<20>>(5000);
    assert!(res < 0.21 && res > 0.19);
    let res = check_collision_percentage::<BadHeadsPercentage::<40>>(5000);
    assert!(res < 0.41 && res > 0.39);
    let res = check_collision_percentage::<BadHeadsPercentage::<60>>(5000);
    assert!(res < 0.61 && res > 0.59);
    let res = check_collision_percentage::<BadHeadsPercentage::<80>>(5000);
    assert!(res < 0.81 && res > 0.79);

}


fn expand_by(data: &mut Vec<u8>, byte: u8) {
    let expand = [byte, byte, byte, byte];
    data.extend_from_slice(&expand);
}


// rounds PERCENTAGE down by steps of 5%
impl<const PERCENTAGE: u8> KeyGenerator for BadHeadsPercentage<PERCENTAGE> {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        // just because I am lazy with handling that otherwise.
        assert!(amount >= 5000);
        assert!(PERCENTAGE <= 100);

        let mut ret : Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(amount);

        let mut counter = 0;
        for prefix in 0..u32::MAX {
            for byte in 0..u8::MAX {
                for i in 0..20 {
                    if counter >= amount {
                        return ret;
                    }

                    let mut key: Vec<u8> = Vec::with_capacity(9);
                    let mut value: Vec<u8> = Vec::with_capacity(4);
                    value.extend_from_slice((counter as u32).to_be_bytes().as_slice());

                    key.extend_from_slice((prefix as u32).to_be_bytes().as_slice());
                    expand_by(&mut key, byte);

                    if i > PERCENTAGE as usize/5 {
                        // slice last byte off, to "destroy" the colliding head
                        key.truncate(key.len() - 1);
                    }

                    key.extend_from_slice((i as u8).to_be_bytes().as_slice());

                    ret.push((key, value));

                    counter+=1;
                }
            }
        }
        unreachable!("Did you seriously just request more than 2^40 * 100 values in a keyset?");
    }
}