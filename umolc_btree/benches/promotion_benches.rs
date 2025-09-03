use std::fmt::format;
use std::thread;
use std::time::{Duration, Instant};
use dev_utils::keyset_generator::{BadHeadsKeyset, BadHeadsPercentage, DenseKeyset, GoodHeadsKeyset, KeyGenerator};
use dev_utils::PerfCounters;
use dev_utils::tree_utils::{amount_values, average_leaf_count, check_node_tag_percentage, total_leaf_count};
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};
fn measure_time<F>(bench: F, name: &str)
where for<'a> F: Fn() {
    let mut perf = PerfCounters::with_counters(["cycles", "instructions", "cache-misses"]);

    perf.reset();
    perf.enable();


    let start_instant = Instant::now();

    bench();

    let elapsed = start_instant.elapsed();
    perf.disable();

    let results = perf.read_to_json(1.);


    let cycles = results
        .get("cache-misses")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    println!("The benchmark \"{name}\" caused {:.2} cache-misses and took {:?} ms", cycles, elapsed.as_millis());

    cool_down(elapsed, 1.0, Duration::from_secs(1), Duration::from_secs(10));
}

fn cool_down(prev: Duration, factor: f32, min: Duration, max: Duration) {
    let scaled = prev.mul_f32(factor);
    let wait = scaled.clamp(min, max);
    thread::sleep(wait);
}


fn most_promotions<KG: KeyGenerator>(amount_keys: usize, iterations: usize, repetitions: usize, name: &str) {
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

    // 1/4 of operations are insert, 1/4 are remove, 1/2 lookup
    let max_fill = amount_keys - (amount_keys / 5);
    let inserts = point_operation_count / 4;
    let lookups = point_operation_count - inserts;


    measure_time(|| {
        let mut index = 0;

        for _ in 0..iterations {
            for _ in 0..lookups {
                let i = (index) % max_fill;
                let (key, value) = &keyset[i];

                let res = tree.lookup_to_vec(key.as_slice());

                index += 1;
            }
            for _ in 0..lookups {
                let i = (index) % max_fill;
                let (key, value) = &keyset[i];
                tree.remove(key.as_slice());
                tree.insert(key.as_slice(), value.as_slice());

                index += 1;
            }

            let mut scan_counter = 0;
            for _ in 0..repetitions {
                tree.scan(first_key.as_slice(), |x,val| {
                    false
                });
            }


        }


    }, name);

}


fn warmup () {

    // spammed six times, as the wait afterwards goes for each individual test as long as the test did
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, 50, "Warmup");
}


fn scan_scenario(name: &str, repetitions: usize) {
    most_promotions::<BadHeadsPercentage< 0>>(10000, 5000, repetitions, "Spamming {name} promotions with a  0% Collisions in the Set");
    most_promotions::<BadHeadsPercentage< 5>>(10000, 5000, repetitions, "Spamming {name} promotions with a  5% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<10>>(10000, 5000, repetitions, "Spamming {name} promotions with a 10% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<15>>(10000, 5000, repetitions, "Spamming {name} promotions with a 15% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<20>>(10000, 5000, repetitions, "Spamming {name} promotions with a 20% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<25>>(10000, 5000, repetitions, "Spamming {name} promotions with a 25% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<30>>(10000, 5000, repetitions, "Spamming {name} promotions with a 30% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<35>>(10000, 5000, repetitions, "Spamming {name} promotions with a 35% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<40>>(10000, 5000, repetitions, "Spamming {name} promotions with a 40% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<45>>(10000, 5000, repetitions, "Spamming {name} promotions with a 45% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<50>>(10000, 5000, repetitions, "Spamming {name} promotions with a 50% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<55>>(10000, 5000, repetitions, "Spamming {name} promotions with a 55% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<60>>(10000, 5000, repetitions, "Spamming {name} promotions with a 60% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<65>>(10000, 5000, repetitions, "Spamming {name} promotions with a 65% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<70>>(10000, 5000, repetitions, "Spamming {name} promotions with a 70% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<75>>(10000, 5000, repetitions, "Spamming {name} promotions with a 75% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<80>>(10000, 5000, repetitions, "Spamming {name} promotions with a 80% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<85>>(10000, 5000, repetitions, "Spamming {name} promotions with a 85% Collisions in the Set");
    most_promotions::<BadHeadsPercentage<90>>(10000, 5000, repetitions, "Spamming {name} promotions with a 90% Collisions in the Set");
}
fn worst_case_scenario() {
    scan_scenario("worst case", 20);
    scan_scenario("bad case", 50);
    scan_scenario("better case", 20);
}

fn differing_repetition_count() {
    for i in 1..40 {
        let repetitions = i * 10;
        most_promotions::<BadHeadsPercentage<15>>(10000, 5000,   repetitions, format!("Spamming 15% Collisions with {:>3} repetitions of action", repetitions).as_str());
    }
}

fn fdl_performance() {

    let amount_keys = 1600000;
    let bm = SimpleBm::<Page>::new(amount_keys/100);
    let tree = Tree::new(&bm);

    let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = DenseKeyset::<50000>::generate_keyset(amount_keys);
    fastrand::shuffle(&mut keyset);

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.insert(key.as_slice(), val.as_slice());
        }

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.remove(key.as_slice());
        }

    }, "FDL Warmup");
    #[cfg(not(feature = "disallow_promotions"))]
    check_node_tag_percentage(253, 0.8, "insertion", true, true, &tree);

    assert_eq!(0, amount_values(&tree));

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.insert(key.as_slice(), val.as_slice());
        }

    }, "FDL Insertion");


    assert_eq!(keyset.len(), amount_values(&tree));

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.lookup_to_vec(key.as_slice());
        }

    }, "FDL Lookup");

    assert_eq!(keyset.len(), amount_values(&tree));

    measure_time(|| {

        for i in 0..20 {
            let (key, val) = &keyset[i];
            tree.scan(b"", |_, _| {false});
        }

    }, "FDL Scan");

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.remove(key.as_slice());
        }

    }, "FDL Remove");

    assert_eq!(0, amount_values(&tree));
}


fn hash_performance_dispatcher<const PERCENTAGE: u8>() {

    let amount_keys = 1600000;
    let bm = SimpleBm::<Page>::new(amount_keys/100);
    let tree = Tree::new(&bm);

    let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = BadHeadsPercentage::<PERCENTAGE>::generate_keyset(amount_keys);
    fastrand::shuffle(&mut keyset);

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.insert(key.as_slice(), val.as_slice());
        }

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.remove(key.as_slice());
        }

    }, format!("HashLeaf {:?}% collisions Warmup", PERCENTAGE).as_str());

    assert_eq!(0, amount_values(&tree));

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.insert(key.as_slice(), val.as_slice());
        }

    }, format!("HashLeaf {:?}% collisions Insert", PERCENTAGE).as_str());


    assert_eq!(keyset.len(), amount_values(&tree));

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.lookup_to_vec(key.as_slice());
        }

    }, format!("HashLeaf {:?}% collisions Lookup", PERCENTAGE).as_str());

    assert_eq!(keyset.len(), amount_values(&tree));

    measure_time(|| {

        for i in 0..keyset.len() {
            let (key, val) = &keyset[i];
            tree.remove(key.as_slice());
        }

    }, format!("HashLeaf {:?}% collisions Remove", PERCENTAGE).as_str());

    assert_eq!(0, amount_values(&tree));
}

fn hash_performance() {
    hash_performance_dispatcher::<10>();
    hash_performance_dispatcher::<20>();
    hash_performance_dispatcher::<30>();
    hash_performance_dispatcher::<40>();
    hash_performance_dispatcher::<50>();
    hash_performance_dispatcher::<60>();
    hash_performance_dispatcher::<70>();
    hash_performance_dispatcher::<80>();
    hash_performance_dispatcher::<90>();
}

fn main() {
    //warmup();
    //fdl_performance();
    hash_performance();
}