#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(warnings)]

use std::cmp::min;
use std::ops::{AddAssign, Range};
use std::thread;
use std::time::{Duration, Instant};
use bstr::BStr;
use dev_utils::keyset_generator::{BadHeadsPercentage, DenseKeyset, KeyGenerator};
use dev_utils::{average_counter, PerfCounters};
use dev_utils::tree_utils::{amount_values, average_leaf_count, check_node_tag_percentage, total_leaf_count};
use umolc::SimpleBm;
use umolc_btree::{Page, Tree};



fn measure_time<F>(bench: F, name: &str) -> (f64, f64, Duration)
where for<'a> F: Fn() {
    let mut perf = PerfCounters::with_counters(["cycles", "instructions", "cache-misses"]);

    perf.reset();
    perf.enable();


    let start_instant = Instant::now();

    bench();

    let elapsed = start_instant.elapsed();
    perf.disable();

    let results = perf.read_to_json(1.);


    let cache_misses = results
        .get("cache-misses")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let cycles = results
        .get("cycles")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    println!(
        "The benchmark {:<15} caused {:>13.2} cache-misses and took {:>5} ms",
        String::new() + "\"" + name + "\"",
        cache_misses,
        elapsed.as_millis()
    );

    cool_down(elapsed, 1.0, Duration::from_secs(1), Duration::from_secs(10));

    (cache_misses, cycles, elapsed)
}

fn cool_down(prev: Duration, factor: f32, min: Duration, max: Duration) {
    let scaled = prev.mul_f32(factor);
    let wait = scaled.clamp(min, max);
    thread::sleep(wait);
}


fn most_promotions<KG: KeyGenerator>(iterations: usize, repetitions: usize, name: &str) -> (f64, f64, Duration) {
    fastrand::seed(42);
    let amount_keys = ADAPTIVE_PROMOTION_AMOUNT;
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

            for _ in 0..repetitions {
                tree.scan(first_key.as_slice(), |x,val| {
                    false
                });
            }


        }


    }, name)

}


fn warmup () {
    // spammed six times, as the wait afterwards goes for each individual test as long as the test did
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
    most_promotions::<BadHeadsPercentage< 0>>(5000, 50, "Warmup");
}


fn scan_scenario(name: &str, repetitions: usize) {
    let mut res: Vec<(usize, (f64, f64, Duration))> = Vec::new();
    res.push(( 0, most_promotions::<BadHeadsPercentage< 0>>(5000, repetitions, format!("Spamming {:?} promotions with a  0% Collisions in the Set", name).as_str())));
    res.push(( 5, most_promotions::<BadHeadsPercentage< 5>>(5000, repetitions, format!("Spamming {:?} promotions with a  5% Collisions in the Set", name).as_str())));
    res.push((10, most_promotions::<BadHeadsPercentage<10>>(5000, repetitions, format!("Spamming {:?} promotions with a 10% Collisions in the Set", name).as_str())));
    res.push((15, most_promotions::<BadHeadsPercentage<15>>(5000, repetitions, format!("Spamming {:?} promotions with a 15% Collisions in the Set", name).as_str())));
    res.push((20, most_promotions::<BadHeadsPercentage<20>>(5000, repetitions, format!("Spamming {:?} promotions with a 20% Collisions in the Set", name).as_str())));
    res.push((25, most_promotions::<BadHeadsPercentage<25>>(5000, repetitions, format!("Spamming {:?} promotions with a 25% Collisions in the Set", name).as_str())));
    res.push((30, most_promotions::<BadHeadsPercentage<30>>(5000, repetitions, format!("Spamming {:?} promotions with a 30% Collisions in the Set", name).as_str())));
    res.push((35, most_promotions::<BadHeadsPercentage<35>>(5000, repetitions, format!("Spamming {:?} promotions with a 35% Collisions in the Set", name).as_str())));
    res.push((40, most_promotions::<BadHeadsPercentage<40>>(5000, repetitions, format!("Spamming {:?} promotions with a 40% Collisions in the Set", name).as_str())));
    res.push((45, most_promotions::<BadHeadsPercentage<45>>(5000, repetitions, format!("Spamming {:?} promotions with a 45% Collisions in the Set", name).as_str())));
    res.push((50, most_promotions::<BadHeadsPercentage<50>>(5000, repetitions, format!("Spamming {:?} promotions with a 50% Collisions in the Set", name).as_str())));
    res.push((55, most_promotions::<BadHeadsPercentage<55>>(5000, repetitions, format!("Spamming {:?} promotions with a 55% Collisions in the Set", name).as_str())));
    res.push((60, most_promotions::<BadHeadsPercentage<60>>(5000, repetitions, format!("Spamming {:?} promotions with a 60% Collisions in the Set", name).as_str())));
    res.push((65, most_promotions::<BadHeadsPercentage<65>>(5000, repetitions, format!("Spamming {:?} promotions with a 65% Collisions in the Set", name).as_str())));
    res.push((70, most_promotions::<BadHeadsPercentage<70>>(5000, repetitions, format!("Spamming {:?} promotions with a 70% Collisions in the Set", name).as_str())));
    res.push((75, most_promotions::<BadHeadsPercentage<75>>(5000, repetitions, format!("Spamming {:?} promotions with a 75% Collisions in the Set", name).as_str())));
    res.push((80, most_promotions::<BadHeadsPercentage<80>>(5000, repetitions, format!("Spamming {:?} promotions with a 80% Collisions in the Set", name).as_str())));
    res.push((85, most_promotions::<BadHeadsPercentage<85>>(5000, repetitions, format!("Spamming {:?} promotions with a 85% Collisions in the Set", name).as_str())));
    res.push((90, most_promotions::<BadHeadsPercentage<90>>(5000, repetitions, format!("Spamming {:?} promotions with a 90% Collisions in the Set", name).as_str())));

    print_results(&res, name);
}
fn worst_case_scenario() {
    scan_scenario("worst case", 20);
    scan_scenario("bad case", 50);
    scan_scenario("better case", 100);
}

fn differing_scenarios() {
    let mut res: Vec<(usize, (f64, f64, Duration))> = Vec::new();
    for i in 0..20 {
        let repetitions = (i+1) * 10;
        let measured = most_promotions::<BadHeadsPercentage<15>>(5000,   repetitions, format!("Spamming 15% Collisions with {:>3} repetitions of action", repetitions).as_str());

        res.push((repetitions, measured));
    }

    print_results(&res, "differing repetitions");
}

fn fdl_performance() {

    let amount_keys = FDL_AMOUNT / FDL_STEPS;
    let bm = SimpleBm::<Page>::new(amount_keys/100);
    let tree = Tree::new(&bm);

    let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = DenseKeyset::<50000>::generate_keyset(amount_keys);

    fastrand::shuffle(&mut keyset);

    type perf = (f64, f64, Duration);
    fn add_to_perfs(a: &mut perf, b: perf) {
        a.0 += b.0;
        a.1 += b.1;
        a.2 += b.2;
    }

    let mut inserts = (0f64, 0f64, Duration::ZERO);
    let mut lookups = (0f64, 0f64, Duration::ZERO);
    let mut scans = (0f64, 0f64, Duration::ZERO);
    let mut removes = (0f64, 0f64, Duration::ZERO);

    let mut leaf_count = 0;
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

    for _ in 0..FDL_STEPS {
        add_to_perfs(&mut inserts, measure_time(|| {
            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.insert(key.as_slice(), val.as_slice());
            }
        }, "FDL Insertion"));

        leaf_count = total_leaf_count(&tree);
        assert_eq!(keyset.len(), amount_values(&tree));

        add_to_perfs(&mut lookups, measure_time(|| {
            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.lookup_to_vec(key.as_slice());
            }
        }, "FDL Lookup"));

        assert_eq!(keyset.len(), amount_values(&tree));

        add_to_perfs(&mut scans, measure_time(|| {
            for _ in 0..20 {
                tree.scan(b"", |_, _| { false });
            }
        }, "FDL Scan"));

        add_to_perfs(&mut removes, measure_time(|| {
            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.remove(key.as_slice());
            }
        }, "FDL Remove"));

        assert_eq!(0, amount_values(&tree));

    }

    println!("Leaf Count: {leaf_count}");

    let res = [(0, inserts), (1, lookups), (2, scans), (3, removes)];
    print_results(&res, "FDL All Operations");
}


fn hash_performance_dispatcher<const PERCENTAGE: u8>() -> [(usize, (f64, f64, Duration)); 4]{

    let mut res: [(usize, (f64, f64, Duration)); 4] = [(0, (0., 0., Duration::from_secs(0))); 4];

    for _ in 0..4 {

        let amount_keys = HASH_AMOUNT;
        let bm = SimpleBm::<Page>::new(amount_keys/100);
        let tree = Tree::new(&bm);

        let mut keyset: Vec<(Vec<u8>, Vec<u8>)> = BadHeadsPercentage::<PERCENTAGE>::generate_keyset(amount_keys);
        fastrand::shuffle(&mut keyset);


        let (x,y,z) = measure_time(|| {

            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.insert(key.as_slice(), val.as_slice());
            }

            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.remove(key.as_slice());
            }

        }, format!("HashLeaf {:?}% collisions Warmup", PERCENTAGE).as_str());

        res[0].0 = PERCENTAGE as usize;
        res[0].1.0 += x;
        res[0].1.1 += y;
        res[0].1.2 += z;

        assert_eq!(0, amount_values(&tree));

        let (x,y, z) = measure_time(|| {

            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.insert(key.as_slice(), val.as_slice());
            }

        }, format!("HashLeaf {:?}% collisions Insert", PERCENTAGE).as_str());

        res[1].0 = PERCENTAGE as usize;
        res[1].1.0 += x;
        res[1].1.1 += y;
        res[1].1.2 += z;

        assert_eq!(keyset.len(), amount_values(&tree));

        let (x,y, z) = measure_time(|| {

            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.lookup_to_vec(key.as_slice());
            }

        }, format!("HashLeaf {:?}% collisions Lookup", PERCENTAGE).as_str());

        res[2].0 = PERCENTAGE as usize;
        res[2].1.0 += x;
        res[2].1.1 += y;
        res[2].1.2 += z;

        assert_eq!(keyset.len(), amount_values(&tree));

        let (x,y , z) = measure_time(|| {

            for i in 0..keyset.len() {
                let (key, val) = &keyset[i];
                tree.remove(key.as_slice());
            }

        }, format!("HashLeaf {:?}% collisions Remove", PERCENTAGE).as_str());

        res[3].0 = PERCENTAGE as usize;
        res[3].1.0 += x;
        res[3].1.1 += y;
        res[3].1.2 += z;

        assert_eq!(0, amount_values(&tree));
    }


    res
}

fn hash_performance() {
    let mut insert_res: Vec<(usize, (f64, f64, Duration))> = Vec::new();
    let mut lookup_res: Vec<(usize, (f64, f64, Duration))> = Vec::new();
    let mut remove_res: Vec<(usize, (f64, f64, Duration))> = Vec::new();
    let res = hash_performance_dispatcher::<10>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<20>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<30>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<40>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<50>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<60>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<70>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<80>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);
    let res = hash_performance_dispatcher::<90>();
    insert_res.push(res[1]);
    lookup_res.push(res[2]);
    remove_res.push(res[3]);

    print_results(&insert_res, "HashLeaf Insert Performance");
    print_results(&lookup_res, "HashLeaf Lookup Performance");
    print_results(&remove_res, "HashLeaf Remove Performance");

}

fn print_results(res: &[(usize, (f64, f64, Duration))], name: &str) {
    println!("Results for {name}");
    println!("CacheMisses:");
    for (i, (miss, _, _)) in res {
        print!("({i}, {miss}) ");
    }
    println!();
    println!("Cycles Time:");
    for (i, (_, cyc, _)) in res {
        print!("({i}, {cyc}) ");
    }
    println!();

    println!("Execution Time:");
    for (i, (_, _, dur)) in res {
        let secs = dur.as_secs_f64();
        print!("({i}, {secs:.3}) ");
    }
    println!();
}

const ADAPTIVE_PROMOTION_AMOUNT: usize = 100000;
const FDL_AMOUNT: usize = 51200000;
const FDL_STEPS: usize = 10;
const HASH_AMOUNT: usize = 3000000;

fn main() {
    warmup();
    worst_case_scenario();
    //differing_scenarios();
    //fdl_performance();
    hash_performance();
}