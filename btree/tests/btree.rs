use btree::Tree;
use dev_utils::mixed_test_keys;
use rand::distributions::{Distribution, Uniform, WeightedIndex};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Barrier;

fn batch_ops(
    threads: usize,
    batches: u32,
    key_count: usize,
    op_weights: (impl Fn(usize, u32) -> [u32; 3] + Sync),
    mut after_batch: (impl FnMut(u32, &Tree) + Send),
) {
    let keys = &mixed_test_keys(key_count);
    let key_dist = &Uniform::new(0, keys.len());
    let key_states: &Vec<_> = &(0..keys.len()).map(|_| [AtomicU32::new(0), AtomicU32::new(0)]).collect();
    let barrier = &Barrier::new(threads);
    let tree = &Tree::new();
    let op_weights = &op_weights;
    let mut after_batch = Some(&mut after_batch);
    std::thread::scope(|scope| {
        let mut join_handles: Vec<_> = (0..threads)
            .map(|tid| {
                let mut after_batch = after_batch.take();
                scope.spawn(move || {
                    let mut thread_rng = SmallRng::seed_from_u64(tid as u64);
                    for batch in 1..=batches {
                        dbg!(batch);
                        let weights = op_weights(tid, batch);
                        let op_dist = &WeightedIndex::new(weights).unwrap();
                        let batch_rng = SmallRng::from_rng(&mut thread_rng).unwrap();
                        let ops = |mut brng: SmallRng| {
                            (0..weights.iter().sum::<u32>())
                                .map(move |_| (op_dist.sample(&mut brng), key_dist.sample(&mut brng)))
                        };
                        for (op, index) in ops(batch_rng.clone()) {
                            if op != 0 {
                                key_states[index][0].fetch_max(tid as u32, Relaxed);
                            }
                        }
                        barrier.wait();
                        for (op, index) in ops(batch_rng.clone()) {
                            if op != 0 && key_states[index][0].load(Relaxed) == tid as u32 {
                                key_states[index][1].fetch_or(1 << (29 + op), Relaxed);
                            }
                        }
                        barrier.wait();
                        for (_op_index, (op, index)) in ops(batch_rng.clone()).enumerate() {
                            let state = key_states[index][1].load(Relaxed);
                            let old_batch: u32 = state & u32::MAX >> 2;
                            let is_inserted = (state >> (29 + 1) & 1) != 0;
                            let is_removed = (state >> (29 + 2) & 1) != 0;
                            match op {
                                0 => {
                                    let is_ok = tree.lookup_inspect(&keys[index], |v| {
                                        if let Some(v) = v {
                                            v.mem_cmp(&old_batch.to_ne_bytes()).is_eq()
                                                || v.mem_cmp(&batch.to_ne_bytes()).is_eq() && is_inserted
                                        } else {
                                            old_batch == 0 || is_removed
                                        }
                                    });
                                    assert!(is_ok);
                                }
                                1 => {
                                    if tree.insert(&keys[index], &batch.to_ne_bytes()).is_none() {
                                        assert!(old_batch == 0 || is_removed)
                                    }
                                }
                                2 => {
                                    if tree.remove(&keys[index]).is_some() {
                                        assert!(old_batch != 0 || is_inserted)
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                        barrier.wait();
                        for (op, index) in ops(batch_rng.clone()) {
                            if op != 0 && key_states[index][0].load(Relaxed) == tid as u32 {
                                key_states[index][0].store(0, Relaxed);
                                key_states[index][1].store(if op == 1 { batch } else { 0 }, Relaxed);
                            }
                        }
                        barrier.wait();
                        if let Some(f) = &mut after_batch {
                            f(batch, tree);
                        }
                        barrier.wait();
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
    batch_ops(1, 50, 250_000, |_, _| [20_000, 20_000, 20_000], |_, _| {});
}
