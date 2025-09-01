use std::fmt::format;
use std::thread;
use std::time::{Duration, Instant};
use dev_utils::keyset_generator::{BadHeadsKeyset, KeyGenerator};
use dev_utils::PerfCounters;
use dev_utils::tree_utils::{average_leaf_count, check_node_tag_percentage, total_leaf_count};
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};

fn measure_time<F>(bench: F, name: &str)
where for<'a> F: Fn() {
    let mut perf = PerfCounters::with_counters(["cycles", "instructions", "cache-misses"]);

    perf.reset();
    perf.enable();

    println!("Starting benchmark: {name}");

    let start_instant = Instant::now();

    bench();

    let elapsed = start_instant.elapsed();
    perf.disable();

    let results = perf.read_to_json(1.);


    let cycles = results
        .get("cycles")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    println!("The benchmark {name} took {:?} CPU cycles and {:?} ms", cycles, elapsed.as_millis());

    cool_down(elapsed, 1.0, Duration::from_millis(50), Duration::from_secs(5));
}

fn cool_down(prev: Duration, factor: f32, min: Duration, max: Duration) {
    let scaled = prev.mul_f32(factor);
    let wait = scaled.clamp(min, max);
    thread::sleep(wait);
}


fn adaptive_promotion<KG: KeyGenerator>(amount_keys: usize, iterations: usize, repetitions: usize) {
    fastrand::seed(42);
    let iterations  = iterations / repetitions;

    let bm = SimpleBm::<Page>::new(amount_keys/100);
    let tree = Tree::new(&bm);

    let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = KG::generate_keyset(amount_keys);
    fastrand::shuffle(&mut keyset);

    let first_key = b"\0";

    for i in 0..amount_keys {
        let (key, value) = &keyset[i];
        tree.insert(key.as_slice(), value.as_slice());

        // a total of 4 scans to sort the hash_keys. Shouldn't matter too much.
        if i % (amount_keys/4) == 0 {
            tree.scan(key.as_slice(), |x,val| {
                false
            });
        }
    }



    // remove 20% of the values, to have the tree slightly sparse
    // (promotions can fail if the more space efficient basic leaf_is made into a hash_leaf)
    for i in 0..amount_keys {
        if i % 4 == 0 {
            tree.remove(keyset[i].0.as_slice());
        }
    }


    let amount_nodes = total_leaf_count(&tree);

    // inserting amount_nodes values will on average trigger one operation per node.

    let point_operation_count = repetitions * amount_nodes as usize *3;

    // 1/4th of operations are insert and remove, 1/2 are scan
    let max_inserts = 15*amount_nodes as usize;
    let lookups = point_operation_count - max_inserts*2;
    assert!(max_inserts < amount_keys - (amount_keys / 5));


    measure_time(|| {

        for iteration in 0..iterations {
            let mut counter = 0;
            // 3 * as many operations because of 1/3rd chance of triggering counter compared to scans
            for i in 0..max_inserts {
                let (key, value) = &keyset[i];

                tree.remove(key.as_slice());
            }
            for i in 0..max_inserts {
                let (key, value) = &keyset[i];

                tree.insert(key.as_slice(), value.as_slice());
            }

            for i in 0..lookups {

                let (key, value) = &keyset[i%amount_keys];

                tree.lookup_to_vec(key.as_slice());
            }


            let mut scan_counter = 0;
            for _ in 0..repetitions {
                tree.scan(first_key.as_slice(), |x,val| {
                    false
                });
            }


        }


    }, format!("Adaptive_promotion with {:?} repetitions", {repetitions}).as_str());

}

fn bad_heads_performance_bench() {
    for i in 10..30 {
        adaptive_promotion::<BadHeadsKeyset>(10000, 5000, i);
    }
}


fn main() {
    bad_heads_performance_bench()
}