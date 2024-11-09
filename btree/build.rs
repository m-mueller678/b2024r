use git2::{DiffOptions, Repository};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Serialize)]
struct BuildInfo {
    commit_hash: String,
    is_dirty: bool,
    diff_files: BTreeSet<PathBuf>,
    cargo_features: Vec<String>,
    cargo_cfg: Map<String, Value>,
    opt_level: String,
    rustc_version: String,
}

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Failed to get manifest directory");
    let git_dir = PathBuf::from(manifest_dir).parent().expect("failed to get git root directory").to_owned();
    let repo = Repository::open(git_dir).expect("Failed to open git repository");
    let head = repo.head().expect("Failed to get HEAD");
    let commit = head.peel_to_commit().expect("Failed to get commit");
    let commit_hash = commit.id().to_string();

    // Get the diff of the working directory against the most recent commit
    let mut diff_options = DiffOptions::new();
    let diff = repo
        .diff_tree_to_workdir_with_index(Some(&commit.tree().expect("Failed to get tree")), Some(&mut diff_options))
        .expect("Failed to diff tree to workdir");

    let diff_files: BTreeSet<PathBuf> = repo
        .diff_index_to_workdir(None, None)
        .expect("Failed to diff index to workdir")
        .deltas()
        .flat_map(|d| [d.old_file(), d.new_file()])
        .filter_map(|f| f.path())
        .map(|p| p.to_owned())
        .collect();
    let is_dirty = !diff_files.is_empty();

    // Get the list of enabled Cargo feature flags
    let cargo_features = env::vars()
        .filter_map(|(k, v)| (k.strip_prefix("CARGO_FEATURE_").map(|k| (k.to_string(), v))))
        .map(|x| {
            dbg!(x.1);
            x.0.to_string()
        })
        .collect::<Vec<_>>();
    let cargo_cfg = env::vars()
        .filter_map(|(k, v)| (k.strip_prefix("CARGO_CFG_").map(|k| (k.to_string(), v))))
        .map(|(k, v)| {
            let vs: Vec<Value> = v.split(',').filter(|vv| !vv.is_empty()).map(Value::from).collect();
            (k, Value::from(vs))
        })
        .collect();

    // Get the build profile (debug or release)
    let opt_level = env::var("OPT_LEVEL").expect("Failed to get OPT_LEVEL");

    // Get the rustc version and path
    let rustc_path = env::var("RUSTC").expect("Failed to get RUSTC path");
    let rustc_version_output =
        std::process::Command::new(&rustc_path).arg("--version").output().expect("Failed to get rustc version");
    let rustc_version = String::from_utf8(rustc_version_output.stdout).expect("Invalid UTF-8 output");

    // Create the build info struct
    let build_info = BuildInfo {
        commit_hash,
        is_dirty,
        diff_files,
        cargo_features,
        opt_level,
        cargo_cfg,
        rustc_version: rustc_version.trim().to_string(),
    };

    // Serialize the build info to JSON
    let json = serde_json::to_string_pretty(&build_info).expect("Failed to serialize build info to JSON");

    // Write the JSON to a file
    let out_dir = env::var("OUT_DIR").expect("Failed to get OUT_DIR");
    let dest_path = std::path::Path::new(&out_dir).join("build_info.json");
    let mut file = File::create(&dest_path).expect("Failed to create build_info.json");
    file.write_all(json.as_bytes()).expect("Failed to write build_info.json");
}
