

pub trait KeyGenerator {
    fn generate_keyset(amount: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct BadHeadsKeyset;
pub struct GoodHeadsKeyset;

pub struct DenseKeyset;

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
        let DENSE_LENGTH = 20000;

        let dense_fields = amount / DENSE_LENGTH;

        let length = dense_fields.ilog2()+1;

        let words = load_words();

        let mut ret :Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(amount);

        for i in 0..amount {
            let mut padding = length;
            let mut index = i;
            let mut key = b"".to_vec();

            let appendix = ((index%DENSE_LENGTH)as u32).to_be_bytes();
            index /= DENSE_LENGTH;
            while padding > 0 {

                let mut word_index = (index%2) as u32 +2*(length-padding);
                word_index %= words.len() as u32;

                let word = words[word_index as usize].as_bytes();
                key.extend_from_slice(b"/");
                key.extend_from_slice(word);

                index/=2;
                padding -=1;
            }


            key.extend_from_slice(appendix.as_slice());

            let value = appendix.to_vec();
            ret.push((key, value));
        }

        fastrand::shuffle(&mut ret);

        println!("Finished Keyset generation");
        ret
    }
}

fn load_words() -> Vec<String> {
    let content = include_str!("word1000.txt");
    content.lines().map(|s| s.to_string()).collect()
}
