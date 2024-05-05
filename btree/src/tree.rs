use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::key_source::SourceSlice;
use crate::node::{node_tag, KindInner, Node};
use crate::page::{PageId, PageTail, PAGE_TAIL_SIZE};
use crate::{MAX_KEY_SIZE, W};
use bytemuck::{Pod, Zeroable};
use seqlock::{Exclusive, Guard, Guarded, Optimistic, SeqlockAccessors};

pub struct Tree {
    meta: PageId,
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    pub fn new() -> Self {
        let meta = PageId::alloc();
        let root = PageId::alloc();
        meta.lock::<Exclusive>().b().0.cast::<MetadataPage>().root_mut().store(root);
        root.lock::<Exclusive>().b().0.cast::<BasicLeaf>().init(&[][..], &[][..], [0u8; 3]);
        Tree { meta }
    }

    fn validate_fences_exclusive(&self) {
        let mut low_buffer = [0u8; MAX_KEY_SIZE];
        let mut high_buffer = [0u8; MAX_KEY_SIZE];
        let meta = self.meta.lock::<Exclusive>();
        let mut root = meta.s().cast::<MetadataPage>().root().load().lock::<Exclusive>();
        drop(meta);
        root.b().0.cast::<BasicInner>().validate_inter_node_fences(&mut &mut low_buffer, &mut &mut high_buffer, 0, 0);
    }

    pub fn remove(&self, _: &[u8]) -> Option<()> {
        todo!()
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = seqlock::unwind::repeat(|| || self.try_insert(k, val));
        self.validate_fences_exclusive();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [Guard<'static, Optimistic, PageTail>; 2] {
        let mut parent = self.meta.lock::<Optimistic>();
        let mut node_pid = parent.s().cast::<MetadataPage>().root().load();
        let mut node = node_pid.lock::<Optimistic>();
        parent.check();
        while node.s().common().tag().load() == node_tag::BASIC_INNER && Some(node_pid) != stop_at {
            parent.release_unchecked();
            node_pid = node.node_cast::<BasicInner>().lookup_inner(k, true);
            parent = node;
            node = node_pid.lock();
        }
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if node.page_id() == split_target {
                let mut node = node.upgrade();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut node, &mut parent);
                debug_assert!(node.common().tag().load() == node_tag::BASIC_INNER);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicInner>(),
                    parent.b().node_cast::<BasicInner>(),
                )
                .is_ok()
                {
                    None
                } else {
                    Some(parent.page_id())
                }
            } else {
                None
            }
        };
        if let Some(p) = parent_id {
            self.split_and_insert(p, k, val)
        } else {
            self.try_insert(k, val)
        }
    }

    fn ensure_parent_not_root(
        &self,
        node: &mut Guard<'static, Exclusive, PageTail>,
        parent: &mut Guard<'static, Exclusive, PageTail>,
    ) {
        if parent.page_id() == self.meta {
            let mut new_root = PageId::alloc().lock::<Exclusive>();
            new_root.b().0.cast::<BasicInner>().init(&[][..], &[][..], node.page_id().to_3x16());
            parent.b().0.cast::<MetadataPage>().root_mut().store(new_root.page_id());
            *parent = new_root
        }
    }

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        match node.b().node_cast::<BasicLeaf>().insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                x
            }
            Err(()) => {
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut node, &mut parent);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicLeaf>(),
                    parent.b().node_cast::<BasicInner>(),
                )
                .is_err()
                {
                    let parent_id = parent.page_id();
                    drop(parent);
                    drop(node);
                    return self.split_and_insert(parent_id, k, val);
                }
                drop(parent);
                drop(node);
                // TODO could descend from parent
                self.try_insert(k, val)
            }
        }
    }

    pub fn lookup_to_vec(&self, k: &[u8]) -> Option<Vec<u8>> {
        seqlock::unwind::repeat(|| || self.try_lookup(k).map(|v| v.load_slice_to_vec()))
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<Guard<'static, Optimistic, [u8]>> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let key: Guarded<'static, _, _> = node.node_cast::<BasicLeaf>().lookup_leaf(k)?;
        Some(node.map(|_| key))
    }

    fn split_locked_node<N: Node>(
        k: &[u8],
        leaf: &mut W<Guarded<Exclusive, N>>,
        mut parent: W<Guarded<Exclusive, BasicNode<KindInner>>>,
    ) -> Result<(), ()> {
        N::split(
            leaf,
            |prefix_len, truncated| {
                let new_node = PageId::alloc();
                k[..prefix_len]
                    .join(truncated)
                    .to_stack_buffer::<{ crate::MAX_KEY_SIZE }, _>(|k| parent.b().insert_inner(k, new_node))?;
                Ok(new_node.lock::<Exclusive>())
            },
            k,
        )
    }
}

#[derive(SeqlockAccessors, Pod, Zeroable, Copy, Clone)]
#[repr(C)]
#[seq_lock_wrapper(crate::W)]
struct MetadataPage {
    root: PageId,
    #[seq_lock_skip_accessor]
    _pad: [u64; PAGE_TAIL_SIZE / 8 - 1],
}

#[cfg(test)]
mod tests {
    use crate::test_util::mixed_test_keys;
    use crate::Tree;
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
                                        if let Some(v) = tree.lookup_to_vec(&keys[index]) {
                                            assert!(
                                                v == old_batch.to_ne_bytes() || v == batch.to_ne_bytes() && is_inserted
                                            );
                                        } else {
                                            assert!(old_batch == 0 || is_removed);
                                        }
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
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}
