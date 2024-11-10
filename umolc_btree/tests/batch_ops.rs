#![feature(inline_const_pat)]
#![feature(maybe_uninit_uninit_array)]

use dev_utils::mixed_test_keys;
use rand::distributions::{Distribution, Uniform, WeightedIndex};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8};
use std::sync::Barrier;
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};

#[derive(Default)]
struct KeyState {
    old_write_batch: AtomicU32,
    old_write_thread: AtomicU32,
    old_present: AtomicBool,
    insert_count: AtomicU8,
    removed_count: AtomicU8,
    principal_thread: AtomicU32,
}

fn inc_state_12(x: &AtomicU8) {
    x.fetch_update(Relaxed, Relaxed, |x| match x {
        0 => Some(1),
        1 => Some(2),
        _ => None,
    })
    .ok();
}

type BM<'bm> = &'bm SimpleBm<Page>;

fn run_many(threads: u32, batches: u32, f: &(impl Fn(&mut SmallRng, &Barrier, u32, u32) + Sync + Send)) {
    let barrier = &Barrier::new(threads as usize);
    std::thread::scope(|scope| {
        let mut join_handles: Vec<_> = (1..=threads)
            .map(|tid| {
                std::thread::Builder::new()
                    .name(format!("batch-ops-{tid}"))
                    .spawn_scoped(scope, move || {
                        let thread_rng = &mut SmallRng::seed_from_u64(tid as u64);
                        for batch_id in 1..=batches {
                            barrier.wait();
                            f(thread_rng, &barrier, tid, batch_id);
                            barrier.wait();
                        }
                    })
                    .unwrap()
            })
            .collect();
        while !join_handles.is_empty() {
            join_handles = join_handles
                .into_iter()
                .filter_map(|x| {
                    if x.is_finished() {
                        x.join().unwrap();
                        None
                    } else {
                        Some(x)
                    }
                })
                .collect();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    })
}

fn batch_ops(threads: u32, batches: u32, key_count: usize, op_weights: (impl Fn(u32, u32) -> [u32; 3] + Sync)) {
    let bm: BM = &SimpleBm::new(1 << 18);
    #[repr(u32)]
    #[derive(Debug)]
    enum Op {
        Lookup,
        Insert,
        Remove,
    }
    let keys = &mixed_test_keys(key_count, true, 1234);
    let key_dist = &Uniform::new(0, keys.len());
    let key_states: &Vec<KeyState> = &(0..keys.len()).map(|_| Default::default()).collect();
    // value is batch as ne bytes
    let tree = &Tree::new(bm);
    let op_weights = &op_weights;

    let thread_priority = |tid, key_index| {
        if tid == 0 {
            0
        } else {
            1 + (tid + key_index as u32) % threads
        }
    };

    run_many(threads, batches, &|rng, barrier, tid, bid| {
        let weights = op_weights(tid, bid);
        let op_dist = &WeightedIndex::new(weights).unwrap();
        let batch_rng_seed = SmallRng::from_rng(rng).unwrap();
        let ops = || {
            let mut brng = batch_rng_seed.clone();
            let total_ops = weights.iter().sum::<u32>();
            (0..total_ops)
                .map(move |_| {
                    let op = match op_dist.sample(&mut brng) {
                        const { Op::Lookup as usize } => Op::Lookup,
                        const { Op::Insert as usize } => Op::Insert,
                        const { Op::Remove as usize } => Op::Remove,
                        _ => unreachable!(),
                    };
                    let index = key_dist.sample(&mut brng);
                    (op, index, &key_states[index], &*keys[index])
                })
                .enumerate()
        };

        // announce ops
        for (_k_op, (op, index, ks, _key)) in ops() {
            //eprintln!("announce {tid} {op:?} {index}");
            let counter = match op {
                Op::Insert => &ks.insert_count,
                Op::Remove => &ks.insert_count,
                Op::Lookup => continue,
            };
            inc_state_12(counter);
            ks.principal_thread
                .fetch_update(Relaxed, Relaxed, |contender| {
                    if thread_priority(tid, index) > thread_priority(contender, index) {
                        Some(tid)
                    } else {
                        None
                    }
                })
                .ok();
        }
        barrier.wait();

        //run
        for (_k_op, (op, _index, ks, key)) in ops() {
            //eprintln!("run {tid} {op:?} {_index}");
            match op {
                Op::Lookup => {
                    let buffer = &mut MaybeUninit::uninit_array();
                    let val = tree.lookup_to_buffer(key, buffer);
                    match val {
                        Some(val) => {
                            let old_match =
                                ks.old_present.load(Relaxed) && &ks.old_write_batch.load(Relaxed).to_le_bytes() == val;
                            let new_match = ks.insert_count.load(Relaxed) > 0 && &bid.to_le_bytes() == val;
                            assert!(old_match || new_match);
                        }
                        None => {
                            assert!(!ks.old_present.load(Relaxed) || ks.removed_count.load(Relaxed) > 0);
                        }
                    }
                }
                Op::Insert => {
                    if tree.insert(key, &bid.to_le_bytes()).is_some() {
                        assert!(ks.old_present.load(Relaxed) || ks.insert_count.load(Relaxed) > 1);
                    } else {
                        assert!(!ks.old_present.load(Relaxed) || ks.removed_count.load(Relaxed) > 0);
                    }
                }
                Op::Remove => {
                    if tree.remove(key).is_some() {
                        assert!(ks.old_present.load(Relaxed) || ks.insert_count.load(Relaxed) > 0);
                    } else {
                        assert!(!ks.old_present.load(Relaxed) || ks.removed_count.load(Relaxed) > 0);
                    }
                }
            }
        }
        barrier.wait();

        // fix-up conflicting writes
        for (_k_op, (op, _index, ks, key)) in ops() {
            //eprintln!("fix {tid} {op:?} {_index}");
            if ks.principal_thread.load(Relaxed) != tid
                || ks.removed_count.load(Relaxed) + ks.insert_count.load(Relaxed) <= 1
            {
                continue;
            }
            let was_present = match op {
                Op::Lookup => continue,
                Op::Insert => tree.insert(key, &bid.to_le_bytes()),
                Op::Remove => tree.remove(key),
            }
            .is_some();
            if was_present {
                assert!(
                    ks.old_present.load(Relaxed) && ks.removed_count.load(Relaxed) == 0
                        || ks.insert_count.load(Relaxed) > 0
                );
            } else {
                assert!(
                    !ks.old_present.load(Relaxed) && ks.insert_count.load(Relaxed) == 0
                        || ks.removed_count.load(Relaxed) > 0
                );
            }
        }
        barrier.wait();

        // update_state
        for (_k_op, (op, _index, ks, _key)) in ops() {
            //eprintln!("update {tid} {op:?} {_index}");
            if ks.principal_thread.load(Relaxed) == tid {
                let is_present = match op {
                    Op::Lookup => continue,
                    Op::Insert => true,
                    Op::Remove => false,
                };
                ks.old_present.store(is_present, Relaxed);
                ks.old_write_thread.store(tid, Relaxed);
            }
        }
        barrier.wait();

        // reset announcements
        for (_k_op, (op, _index, ks, _key)) in ops() {
            if let Op::Remove | Op::Insert = op {
                ks.insert_count.store(0, Relaxed);
                ks.removed_count.store(0, Relaxed);
                ks.principal_thread.store(0, Relaxed);
            }
        }
    });
}

#[test]
fn single_insert_lookup_tiny() {
    batch_ops(1, 3, 500, |_, _| [400, 200, 0]);
}

#[cfg_attr(not(miri), test)]
fn single_insert_lookup() {
    batch_ops(1, 3, 2_500, |_, _| [1_500, 500, 0]);
}

#[cfg_attr(not(miri), test)]
fn single() {
    batch_ops(1, 10, 2_500, |_, _| [500, 500, 500]);
}

#[cfg_attr(not(miri), test)]
fn single_large() {
    batch_ops(1, 30, 50_000, |_, _| [4_000, 4_000, 4_000]);
}

#[cfg_attr(not(miri), test)]
fn multi() {
    batch_ops(4, 10, 2_500, |_, _| [500, 500, 500]);
}
