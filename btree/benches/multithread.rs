use dev_utils::PerfCounters;

struct BenchConfig {
    key_set_name: String,
    key_count: usize,
    duration: f64,
}

fn main() {
    let mut c = PerfCounters::new();
    let counts = c.read_to_json(1.0);
    dbg!(counts);
}
