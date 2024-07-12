use btree::Tree;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dev_utils::mixed_test_keys;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rip_shuffle::RipShuffleParallel;

criterion_group!(benches, lookup);
criterion_main!(benches);

pub fn lookup(c: &mut Criterion) {
    const SMALL: usize = 10_000;

    let mut keys = mixed_test_keys(SMALL, false, 42);
    let rng = &mut SmallRng::seed_from_u64(0x12345678);

    let tree = Tree::new();
    for k in &keys[..SMALL] {
        tree.insert(k, &[42u8; 8]);
    }
    let small_uniform = Uniform::new(0, SMALL);
    let mut group = c.benchmark_group("function");
    group.throughput(Throughput::Elements(1));
    group.bench_function("small-uniform", |b| {
        b.iter(|| assert_eq!(tree.try_lookup(&keys[small_uniform.sample(rng)]).unwrap().len(), 8))
    });
}
