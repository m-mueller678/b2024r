

pub trait KeyGenerator {
    fn generate_keyset(length: usize, amount: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct BadHeadsKeyset;

impl KeyGenerator for BadHeadsKeyset {
    fn generate_keyset(length: usize, amount: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        if length < 8 || length % 8 != 0 {
            panic!("length must at least 8 bytes and of even length");
        }
        let mut ret :Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(length);

        for i in 0..amount {
            let index = i;
            let mut padding = length -4;
            while padding > 0 {
                
                padding -=2;
            }
        }

        ret
    }
}