use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use random_word::Lang;
use rayon::prelude::*;
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::RangeInclusive;

pub fn mixed_test_keys(count: usize) -> Vec<Vec<u8>> {
    const KEY_KINDS: usize = 5;
    assert!(count >= KEY_KINDS);
    let chunk_size = (1 << 10).min(count / KEY_KINDS);
    let kind_distr = Uniform::new(0, KEY_KINDS);
    let mut keys = vec![Vec::new(); count];
    keys.par_chunks_mut(chunk_size).enumerate().for_each(|(i, mut chunk)| {
        if i == 0 && chunk_size > 300 {
            for i in 0..(256) {
                if i + 1 < chunk.len() {
                    chunk[i + 1] = vec![i as u8];
                }
            }
            chunk = &mut chunk[257..]
        }

        let rng = &mut SmallRng::seed_from_u64(i as u64);
        let mut seq_pos: u64 = rng.gen();
        let mut generator: Box<dyn FnMut(&mut SmallRng) -> Vec<u8>> = match i % KEY_KINDS {
            0 => Box::new(alpha_generator(5..=20)),
            1 => Box::new(ascii_bin_generator(20..=30)),
            2 => Box::new(path_generator(30..=120)),
            3 => Box::new(|_| {
                seq_pos = seq_pos.wrapping_add(1);
                seq_pos.to_be_bytes().to_vec()
            }),
            4 => Box::new(|rng| rng.gen::<u64>().to_ne_bytes().to_vec()),
            _ => unreachable!(),
        };
        for dst in chunk {
            *dst = generator(rng);
        }
    });
    keys.par_sort();
    keys.dedup();
    let main_rng = &mut SmallRng::seed_from_u64(keys.len() as u64);
    let mut new_keys = HashSet::new();
    while keys.len() < count {
        let c = (main_rng.gen::<u64>() | u64::MAX << 48).to_be_bytes();
        if keys.binary_search(&c.to_vec()).is_err() && new_keys.insert(c) {
            keys.push(c.to_vec());
        }
    }
    keys.par_sort();
    keys
}

pub fn alpha_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    let alpha_dist = Uniform::<u8>::new(b'A', b'Z');
    move |rng| {
        let len = dist.sample(rng);
        (0..len).map(|_| alpha_dist.sample(rng)).collect()
    }
}

pub fn ascii_bin_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    move |rng| {
        let len = dist.sample(rng);
        (0..len).map(|_| if rng.gen() { b'0' } else { b'1' }).collect()
    }
}

pub fn random_key_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    move |rng| {
        let mut k = vec![0u8; dist.sample(rng)];
        rng.fill(k.as_mut_slice());
        k
    }
}

fn hash(h: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    h.hash(&mut hasher);
    hasher.finish()
}

pub fn path_generator<R: Rng>(min_len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let words = if cfg!(miri) {
        &["ab", "anti", "antibody", "antics", "the", "there", "then"]
    } else {
        random_word::all(Lang::En)
    };
    let word_dist = Uniform::new(0, words.len());
    let len_dist = Uniform::new(min_len_range.start(), min_len_range.end());
    let cardinality_dist = Uniform::new(1, 6);
    move |rng| {
        let target_len = len_dist.sample(rng);
        let mut key = Vec::new();
        let mut group_seed = 0;
        while key.len() < target_len {
            if !key.is_empty() {
                key.push(b'/')
            }
            let group_rng = &mut SmallRng::seed_from_u64(group_seed);
            let card = cardinality_dist.sample(group_rng);
            let next_word_id = rng.gen_range(0..card);
            let next_word = words[word_dist.sample(group_rng)];
            group_seed = hash((group_seed, next_word_id));
            key.extend_from_slice(next_word.as_bytes());
        }
        key
    }
}

pub fn subslices<T>(x: &[T], min_len: usize) -> impl Iterator<Item = &[T]> {
    (0..x.len() - min_len)
        .flat_map(move |start| (start + min_len..=x.len()).map(move |end| start..end))
        .map(|range| &x[range])
}
