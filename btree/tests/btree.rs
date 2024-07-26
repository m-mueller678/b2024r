#![feature(inline_const_pat)]

use btree::{PageTail, Tree};
use dev_utils::mixed_test_keys;
use rand::distributions::{Distribution, Uniform, WeightedIndex};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use seqlock::DefaultBm;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8};
use std::sync::Barrier;

#[derive(Default)]
struct KeyState {
    old_write_batch: AtomicU32,
    old_write_thread: AtomicU32,
    old_present: AtomicBool,
    inserted: AtomicU8,
    removed: AtomicU8,
    max_write_thread: AtomicU32,
    max_write_is_insert: AtomicBool,
}

fn inc_state_12(x: &AtomicU8) {
    x.fetch_update(Relaxed, Relaxed, |x| match x {
        0 => Some(1),
        1 => Some(2),
        _ => None,
    })
    .ok();
}

fn batch_ops(
    threads: usize,
    batches: u32,
    key_count: usize,
    op_weights: (impl Fn(usize, u32) -> [u32; 3] + Sync),
    mut after_batch: (impl for<'bm> FnMut(u32, &Tree<&'bm DefaultBm<PageTail>>) + Send),
) {
    let bm = DefaultBm::new_lazy();
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
    let barrier = &Barrier::new(threads);
    // value is batch as ne bytes
    let tree = &Tree::new(&bm);
    let op_weights = &op_weights;
    let mut after_batch = Some(&mut after_batch);
    std::thread::scope(|scope| {
        let mut join_handles: Vec<_> = (0..threads)
            .map(|tid| {
                let mut after_batch = after_batch.take();
                scope.spawn(move || {
                    let mut thread_rng = SmallRng::seed_from_u64(tid as u64);
                    for batch in 1..=batches {
                        let weights = op_weights(tid, batch);
                        let op_dist = &WeightedIndex::new(weights).unwrap();
                        let batch_rng = SmallRng::from_rng(&mut thread_rng).unwrap();
                        let ops = |mut brng: SmallRng| {
                            (0..weights.iter().sum::<u32>()).map(move |_| {
                                (
                                    match op_dist.sample(&mut brng) {
                                        const { Op::Lookup as usize } => Op::Lookup,
                                        const { Op::Insert as usize } => Op::Insert,
                                        const { Op::Remove as usize } => Op::Remove,
                                        _ => unreachable!(),
                                    },
                                    key_dist.sample(&mut brng),
                                )
                            })
                        };
                        for phase in 0..4 {
                            const PHASE_ANNOUNCE: usize = 0;
                            const PHASE_RUN: usize = 1;
                            const PHASE_REWRITE: usize = 2;
                            const PHASE_CLEAN: usize = 3;
                            #[allow(clippy::unused_enumerate_index)]
                            for (_op_index, (op, index)) in ops(batch_rng.clone()).enumerate() {
                                let ks = &key_states[index];
                                if phase == PHASE_ANNOUNCE {
                                    inc_state_12(match op {
                                        Op::Insert => &ks.inserted,
                                        Op::Remove => &ks.removed,
                                        Op::Lookup => continue,
                                    });
                                    key_states[index].max_write_thread.fetch_max(tid as u32, Relaxed);
                                } else if phase == PHASE_CLEAN {
                                    match op {
                                        Op::Lookup => continue,
                                        Op::Insert => (),
                                        Op::Remove => (),
                                    }
                                    if ks
                                        .max_write_thread
                                        .fetch_update(
                                            Relaxed,
                                            Relaxed,
                                            |x| if x == tid as u32 { Some(0) } else { None },
                                        )
                                        .is_err()
                                    {
                                        continue;
                                    }
                                    ks.old_write_batch.store(batch, Relaxed);
                                    ks.old_write_thread.store(tid as u32, Relaxed);
                                    ks.old_present.store(ks.max_write_is_insert.load(Relaxed), Relaxed);
                                    // assert_eq!(tree.lookup_inspect(&keys[index],|x|x.map(|_|())).is_some(),ks.max_write_is_insert.load(Relaxed));
                                    ks.removed.store(0, Relaxed);
                                    ks.inserted.store(0, Relaxed);
                                } else {
                                    let write = || {
                                        phase == PHASE_RUN
                                            || (phase == PHASE_REWRITE
                                                && ks.inserted.load(Relaxed) + ks.removed.load(Relaxed) > 1
                                                && tid as u32 == ks.max_write_thread.load(Relaxed))
                                    };
                                    match op {
                                        Op::Lookup => {
                                            if phase == PHASE_RUN {
                                                let mut batch_matches = false;
                                                let mut new_batch_matches = false;
                                                let mut was_present = false;
                                                let is_ok = tree.lookup_inspect(&keys[index], |v| {
                                                    if let Some(v) = v {
                                                        was_present = true;
                                                        batch_matches = v
                                                            .mem_cmp(&ks.old_write_batch.load(Relaxed).to_ne_bytes())
                                                            .is_eq();
                                                        new_batch_matches = v.mem_cmp(&batch.to_ne_bytes()).is_eq();
                                                        batch_matches && ks.old_present.load(Relaxed)
                                                            || new_batch_matches && ks.inserted.load(Relaxed) != 0
                                                    } else {
                                                        was_present = false;
                                                        !ks.old_present.load(Relaxed) || ks.removed.load(Relaxed) != 0
                                                    }
                                                });
                                                assert!(is_ok);
                                            }
                                        }
                                        Op::Insert => {
                                            if write() {
                                                if phase == PHASE_RUN && ks.max_write_thread.load(Relaxed) == tid as u32
                                                {
                                                    ks.max_write_is_insert.store(true, Relaxed);
                                                }
                                                if tree.insert(&keys[index], &batch.to_ne_bytes()).is_some() {
                                                    assert!(
                                                        ks.old_present.load(Relaxed)
                                                            || ks.inserted.load(Relaxed) == 2
                                                            || phase == PHASE_REWRITE
                                                    )
                                                } else {
                                                    assert!(
                                                        !ks.old_present.load(Relaxed) || ks.removed.load(Relaxed) != 0
                                                    )
                                                }
                                            }
                                        }
                                        Op::Remove => {
                                            if write() {
                                                if phase == PHASE_RUN && ks.max_write_thread.load(Relaxed) == tid as u32
                                                {
                                                    ks.max_write_is_insert.store(false, Relaxed);
                                                }
                                                if tree.remove(&keys[index]).is_some() {
                                                    assert!(
                                                        ks.old_present.load(Relaxed) || ks.inserted.load(Relaxed) != 0
                                                    )
                                                } else {
                                                    assert!(
                                                        !ks.old_present.load(Relaxed)
                                                            || ks.removed.load(Relaxed) == 2
                                                            || phase == PHASE_REWRITE
                                                    )
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            barrier.wait();
                        }
                        if let Some(ab) = &mut after_batch {
                            ab(batch, tree);
                        }
                    }
                })
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

#[test]
fn single_insert_lookup_tiny() {
    batch_ops(1, 3, 500, |_, _| [400, 200, 0], |_, _| {});
}

#[cfg_attr(not(miri), test)]
fn single_insert_lookup() {
    batch_ops(1, 3, 2_500, |_, _| [1_500, 500, 0], |_, _| {});
}

#[cfg_attr(not(miri), test)]
fn single() {
    batch_ops(1, 10, 2_500, |_, _| [500, 500, 500], |_, _| {});
}

#[cfg_attr(not(miri), test)]
fn single_large() {
    batch_ops(1, 30, 50_000, |_, _| [4_000, 4_000, 4_000], |_, _| {});
}

#[cfg_attr(not(miri), test)]
fn multi() {
    batch_ops(4, 10, 2_500, |_, _| [500, 500, 500], |_, _| {});
}
