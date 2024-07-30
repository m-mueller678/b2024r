use minstant::Instant;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Once;
use std::time::Duration;
pub use {serde_json, zipf};

pub fn generate_keys(
    count: usize,
    sorted: bool,
    seed: u64,
    generator: &(impl Sync + Fn(&mut SmallRng, usize) -> Vec<u8>),
) -> Vec<Box<[u8]>> {
    let generator = |i| generator(&mut SmallRng::seed_from_u64(hash(&(seed, i))), i).into_boxed_slice();
    let mut keys = vec![Vec::new().into_boxed_slice(); count];
    keys.par_iter_mut().enumerate().for_each(|(i, dst)| {
        *dst = generator(i);
    });
    keys.par_sort();
    keys.dedup();
    let mut new_keys = HashSet::new();
    for i in count.. {
        if keys.len() + new_keys.len() >= count {
            break;
        }
        let c = generator(i);
        if keys.binary_search(&c).is_err() {
            new_keys.insert(c.clone());
        }
    }
    keys.extend(new_keys);
    assert_eq!(keys.len(), count);
    if sorted {
        keys.par_sort();
    } else {
        keys.par_shuffle(&mut SmallRng::seed_from_u64(seed));
    }
    keys
}

pub fn mixed_test_keys(count: usize, sorted: bool, seed: u64) -> Vec<Box<[u8]>> {
    generate_keys(
        count,
        sorted,
        hash(&(seed, 1)),
        &test_mix_generator(count,hash(&(seed, 2))),
    )
}

pub fn test_mix_generator(count: usize, seed: u64)->impl Sync + Fn(&mut SmallRng, usize) -> Vec<u8>{
    mixed_generator(vec![
        Box::new(alpha_generator(5..=20)),
        Box::new(ascii_bin_generator(20..=30)),
        Box::new(path_generator(30..=120)),
        Box::new(seq_u64_generator((count / 5 / 4000).max(1), &mut SmallRng::seed_from_u64(seed))),
        Box::new(random_fixed_size_generator(8)),
    ])
}

#[allow(clippy::type_complexity)]
pub fn mixed_generator<R: Rng>(
    gens: Vec<Box<dyn Sync + Fn(&mut R, usize) -> Vec<u8>>>,
) -> impl Sync + Fn(&mut R, usize) -> Vec<u8> {
    move |rng, index| gens[index % gens.len()](rng, index / gens.len())
}

pub fn seq_u64_generator<R: Rng>(ranges: usize, rng: &mut R) -> impl Fn(&mut R, usize) -> Vec<u8> {
    let starts: Vec<u64> = (0..ranges).map(|_| rng.gen()).collect();
    move |_, index| {
        let x = starts[index % starts.len()] + (index / starts.len()) as u64;
        x.to_be_bytes().to_vec()
    }
}

pub fn seq_u64_generator_0<R: Rng>() -> impl Fn(&mut R, usize) -> Vec<u8> {
    move |_, index| (index as u64).to_be_bytes().to_vec()
}

pub fn alpha_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R, usize) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    let alpha_dist = Uniform::<u8>::new(b'A', b'Z');
    move |rng, _i| {
        let len = dist.sample(rng);
        (0..len).map(|_| alpha_dist.sample(rng)).collect()
    }
}

pub fn ascii_bin_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R, usize) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    move |rng, _i| {
        let len = dist.sample(rng);
        (0..len).map(|_| if rng.gen() { b'0' } else { b'1' }).collect()
    }
}

pub fn random_uniform_size_generator<R: Rng>(len_range: RangeInclusive<usize>) -> impl Fn(&mut R, usize) -> Vec<u8> {
    let dist = Uniform::<usize>::new(*len_range.start(), *len_range.end());
    move |rng, _i| {
        let mut k = vec![0u8; dist.sample(rng)];
        rng.fill(k.as_mut_slice());
        k
    }
}

pub fn random_fixed_size_generator<R: Rng>(len: usize) -> impl Fn(&mut R, usize) -> Vec<u8> {
    move |rng, _i| {
        let mut k = vec![0u8; len];
        rng.fill(k.as_mut_slice());
        k
    }
}

fn hash(h: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    h.hash(&mut hasher);
    hasher.finish()
}

pub fn path_generator<R: Rng>(min_len_range: RangeInclusive<usize>) -> impl Fn(&mut R, usize) -> Vec<u8> {
    let words = if cfg!(miri) {
        vec!["ab", "anti", "antibody", "antics", "the", "there", "then"]
    } else {
        include_str!("word1000.txt").split('\n').collect()
    };
    let word_dist = Uniform::new(0, words.len());
    let len_dist = Uniform::new(min_len_range.start(), min_len_range.end());
    let cardinality_dist = Uniform::new(1, 6);
    move |rng, _i| {
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
            group_seed = hash(&(group_seed, next_word_id));
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

pub use perf_event;
use pfm::{PerfEvent, Perfmon};
use rip_shuffle::RipShuffleParallel;

pub struct PerfCounters {
    counters: Vec<(String, PerfEvent)>,
    time: Result<Duration, Instant>,
}

impl Default for PerfCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl PerfCounters {
    pub fn new() -> Self {
        Self::with_counters(["instructions", "cycles", "branch-misses"])
    }
    pub fn with_counters<'a>(counters: impl IntoIterator<Item = &'a str>) -> Self {
        static INIT_PFM: Once = Once::new();
        INIT_PFM.call_once(|| {
            // initializing pfm multiple times is apparently safe, but does nothing.
            // See `man pfm_initialize`
            Perfmon::default().initialize().unwrap();
        });
        PerfCounters {
            counters: counters
                .into_iter()
                .map(|name| {
                    let mut event = PerfEvent::new(name, true).unwrap();
                    event.open(0, -1).unwrap();
                    (name.to_string(), event)
                })
                .collect(),
            time: Ok(Duration::ZERO),
        }
    }

    pub fn enable(&mut self) {
        for x in &mut self.counters {
            x.1.enable().unwrap();
        }
        let Ok(duration) = self.time else { panic!("perf already enabled") };
        self.time = Err(Instant::now() - duration);
    }
    pub fn disable(&mut self) {
        for x in &mut self.counters {
            x.1.disable().unwrap();
        }
        let Err(start) = self.time else { panic!("perf already disabled") };
        self.time = Ok(Instant::now() - start);
    }
    pub fn reset(&mut self) {
        assert!(self.time.is_ok(), "perf reset while enabled");
        for x in &mut self.counters {
            x.1.reset().unwrap();
        }
        self.time = Ok(Duration::ZERO)
    }

    pub fn read_to_json(&mut self, scale: f64) -> Map<String, Value> {
        assert!(self.time.is_ok(), "perf read while enabled");
        let mut multiplexed = false;
        let perf_counters = self.counters.iter().map(|(n, c)| {
            let v = c.read().unwrap();
            multiplexed |= v.time_enabled != v.time_running;
            (n.as_str(), v.value as f64 * v.time_enabled as f64 / v.time_running as f64)
        });
        let time = std::iter::once(("time", self.time.unwrap().as_secs_f64()));
        let mut out: Map<_, _> =
            perf_counters.chain(time).map(|(n, x)| (n.to_string(), Value::from(x / scale))).collect();
        out.insert("scale".to_string(), Value::from(scale));
        out.insert("multiplexed".to_string(), Value::from(multiplexed));
        out
    }
}
