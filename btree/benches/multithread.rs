#![feature(thread_sleep_until)]

use btree::Tree;
use dev_utils::serde_json::{Map, Value};
use dev_utils::zipf::ZipfDistribution;
use dev_utils::{generate_keys, random_fixed_size_generator, seq_u64_generator, test_mix_generator, PerfCounters};
use rand::prelude::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rip_shuffle::RipShuffleParallel;
use seqlock::DefaultBm;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Barrier;
use std::time::{Duration, Instant};

struct Args {
    run_tag: String,
    key_set_name: String,
    key_count: usize,
    payload_size: usize,
    pre_insert_ratio: f64,
    lookup_duration: f64,
    scan_duration: f64,
    zipf: f64,
    threads: usize,
    counters: Option<Vec<String>>,
}

impl Args {
    fn keys(&self) -> Vec<Box<[u8]>> {
        let generator: Box<dyn Sync + for<'a> Fn(&'a mut _, _) -> _> = match self.key_set_name.as_str() {
            "test-mix" => Box::new(test_mix_generator(self.key_count, 42)),
            "dense64" => Box::new(seq_u64_generator(self.key_count, &mut SmallRng::seed_from_u64(42))),
            "sparse64" => Box::new(random_fixed_size_generator(8)),
            "sparse32" => Box::new(random_fixed_size_generator(4)),
            a => panic!("unknown key set: {a:?}"),
        };
        generate_keys(self.key_count, false, 43, &generator)
    }
}

fn get_arg<T: FromStr>(name: &str, default: T) -> T
where
    T::Err: Display,
{
    if let Ok(val) = std::env::var(name) {
        match T::from_str(&val) {
            Ok(x) => x,
            Err(e) => panic!("failed to parse {name}: {e}"),
        }
    } else {
        default
    }
}

fn get_args() -> Args {
    Args {
        run_tag: get_arg("RUN_TAG", String::new()),
        key_set_name: get_arg("KEY_NAME", "test-mix".to_string()),
        key_count: get_arg("KEY_COUNT", 1e5) as usize,
        payload_size: get_arg("VAL_SIZE", 8.0) as usize,
        pre_insert_ratio: get_arg("PRE_INSERT", 0.8),
        lookup_duration: get_arg("LOOKUP_DURATION", 0.5),
        scan_duration: get_arg("SCAN_DURATION", 0.5),
        zipf: get_arg("ZIPF", 1.0),
        threads: get_arg("THREADS", 1),
        counters: if let Ok(c) = std::env::var("PERF") {
            Some(c.split(',').map(|x| x.to_string()).collect())
        } else {
            None
        },
    }
}

fn thread_subrange(source: Range<usize>, count: usize, id: usize) -> Range<usize> {
    let start = |id: usize| source.start + if id == count { source.len() } else { source.len() / count * id };
    start(id)..start(id + 1)
}

fn run_jobs<J, F>(perf: &mut PerfCounters, count: usize, work_duration: f64, f: F) -> Map<String, Value>
where
    J: FnOnce(&AtomicBool) -> usize,
    F: Sync + Fn(usize) -> J,
{
    let barrier = &Barrier::new(count + 1);
    let keep_working = &AtomicBool::new(true);
    let ops_performed = &AtomicUsize::new(0);
    let f = &f;
    rayon::in_place_scope(|s| {
        for tid in 0..count {
            s.spawn(move |_| {
                let local_f = f(tid);
                // TODO cpu affinity
                barrier.wait();
                barrier.wait();
                let local_ops = local_f(keep_working);
                barrier.wait();
                ops_performed.fetch_add(local_ops, Relaxed);
                barrier.wait();
            });
        }
        perf.reset();
        barrier.wait();
        let sleep_until = Instant::now() + Duration::from_secs_f64(work_duration);
        perf.enable();
        barrier.wait();
        std::thread::sleep_until(sleep_until);
        keep_working.store(false, Relaxed);
        barrier.wait();
        perf.disable();
        barrier.wait();
    });
    perf.read_to_json(ops_performed.load(Relaxed) as f64)
}

fn main() {
    if cfg!(any(feature = "validate_node", feature = "validate_node", debug_assertions)) {
        eprintln!("warning: debug assertions or validation enabled");
    }
    let args = get_args();
    let mut perf = if let Some(c) = &args.counters {
        PerfCounters::with_counters(c.iter().map(|x| x.as_str()))
    } else {
        PerfCounters::new()
    };
    // threads must be spawned after setting up counters
    rayon::ThreadPoolBuilder::default().build_global().unwrap();
    let bm = &DefaultBm::new_lazy();
    let tree = &Tree::new(bm);
    let mut keys = args.keys();
    let pre_insert_count = (keys.len() as f64 * args.pre_insert_ratio) as usize;
    keys.par_shuffle(&mut SmallRng::seed_from_u64(0x42));
    let keys = &keys;
    let value = &vec![42u8; args.payload_size];
    dbg!();
    let mut run_insert_jobs = |range: Range<usize>| {
        run_jobs(&mut perf, args.threads, 0.0, |tid| {
            let range = range.clone();
            move |_| {
                let t_range = thread_subrange(range.clone(), args.threads, tid);
                for ki in t_range.clone() {
                    tree.insert(&keys[ki], value);
                }
                t_range.len()
            }
        })
    };
    let pre_insert = run_insert_jobs(0..pre_insert_count);
    dbg!(pre_insert);
    let insert = run_insert_jobs(pre_insert_count..keys.len());
    dbg!(insert);
    let zipf = ZipfDistribution::new(keys.len(), args.zipf).unwrap();
    let lookup = run_jobs(&mut perf, args.threads, args.lookup_duration, |tid| {
        let mut local_ops = 0;
        let mut rng = SmallRng::seed_from_u64(987 + tid as u64);
        move |keep_working| {
            let rng = &mut rng;
            while keep_working.load(Relaxed) {
                let index = zipf.sample(rng) - 1;
                let mut len = None;
                tree.lookup_inspect(&keys[index], |val| {
                    len = val.map(|x| x.len());
                });
                assert_eq!(len, Some(args.payload_size));
                local_ops += 1;
            }
            local_ops
        }
    });
    dbg!(lookup);
}
