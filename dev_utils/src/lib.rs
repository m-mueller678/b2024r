use minstant::Instant;
use perf_event::events::Event;
use perf_event::{Builder, Counter};
use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use random_word::Lang;
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::atomic::AtomicBool;
use std::sync::{Barrier, Once};
use std::thread::JoinHandle;
use std::time::Duration;
pub use {serde_json, zipf};

pub fn mixed_test_keys(count: usize) -> Vec<Vec<u8>> {
    const KEY_KINDS: usize = 5;
    assert!(count >= KEY_KINDS);
    let chunk_size = (1 << 10).min(count / KEY_KINDS);
    let mut keys = vec![Vec::new(); count];
    keys.par_chunks_mut(chunk_size).enumerate().for_each(|(i, mut chunk)| {
        if i == 0 && chunk_size > 300 {
            for i in 0..256 {
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

pub use perf_event;
use pfm::{PerfEvent, Perfmon};

pub struct PerfCounters {
    counters: Vec<(String, PerfEvent)>,
    time: Result<Duration, Instant>,
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
                    let mut event = PerfEvent::new(name, false).unwrap();
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
