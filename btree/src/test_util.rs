use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use std::ops::RangeInclusive;

pub fn alpha_key_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    let alpha_dist = Uniform::<u8>::new(b'A', b'Z');
    move |rng| {
        let len = dist.sample(rng);
        (0..len).map(|_| alpha_dist.sample(rng)).collect()
    }
}

pub fn bin_key_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    move |rng| {
        let len = dist.sample(rng);
        (0..len).map(|_| if rng.gen() { b'0' } else { b'1' }).collect()
    }
}

pub fn subslices<T>(x: &[T], min_len: usize) -> impl Iterator<Item = &[T]> {
    (0..x.len() - min_len)
        .flat_map(move |start| (start + min_len..=x.len()).map(move |end| start..end))
        .map(|range| &x[range])
}
