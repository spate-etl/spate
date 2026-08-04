//! Turn a criterion run into the same `Report` records the rigs emit, so one
//! comparator reads both tiers.
//!
//! Usage:
//!   criterion_to_report <DIR>
//!
//! `RESULTS` appends, exactly as it does for a rig. `GIT_COMMIT` and
//! `BENCH_TRIGGER` are read by the report layer; both are required here, and
//! why is the point of the next two sections.
//!
//! ## The directory is an argument, and it must be a fresh one
//!
//! criterion's output directory is persistent and shared: `target/criterion`
//! accumulates every benchmark any target has ever run, and each keeps its
//! `new/` until something overwrites it. Converting that tree does not convert
//! *a run* — it converts everything on the disk, stamps all of it with the
//! current commit and a current timestamp, and yields a leg whose rows mostly
//! describe measurements taken days ago from another tree. Two such legs pair
//! cleanly and render as "steady", which is a comparison that looks far
//! better-founded than it is.
//!
//! So there is no default. Point criterion at a per-leg directory and convert
//! that one:
//!
//! ```sh
//! export CRITERION_HOME=$(mktemp -d)
//! cargo bench -p spate-json --locked --bench decode
//! cargo build --release -p benchmarks --locked --bin criterion_to_report
//! BENCH_TRIGGER=dispatched GIT_COMMIT=$(git rev-parse HEAD) \
//!   ./target/release/criterion_to_report "$CRITERION_HOME" >> leg.jsonl
//! ```
//!
//! Only `new/` is read; `base/` is criterion's own previous run, and comparing
//! against it is the cross-run comparison this repository does not make.
//!
//! ## `BENCH_TRIGGER` is required
//!
//! Unset, `Trigger::detect()` answers `manual` off CI — the one value that does
//! not bar publication. Every use of this tool is a comparison arm, and
//! `Trigger::Dispatched` exists to say that a comparison is not a recording. A
//! default here would mint publishable records out of a comparison, which is
//! the failure that field exists to end rather than repeat, so an unset trigger
//! is refused instead.
//!
//! ## Which estimate becomes the value
//!
//! The **mean**, with criterion's bootstrap confidence interval. Not the
//! median: this crate carries two disagreeing definitions of that
//! ([#62](https://github.com/spate-etl/spate/issues/62)), and a record whose
//! value depends on which one a reader assumes is worse than no record.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// `benchmark.json` — criterion's identity for one benchmark.
#[derive(Deserialize)]
struct Benchmark {
    group_id: String,
    function_id: Option<String>,
    /// Set for a parameterised benchmark, null otherwise.
    value_str: Option<String>,
}

#[derive(Deserialize)]
struct ConfidenceInterval {
    lower_bound: f64,
    upper_bound: f64,
}

#[derive(Deserialize)]
struct Estimate {
    confidence_interval: ConfidenceInterval,
    point_estimate: f64,
}

/// `estimates.json` — only the mean is read; see the note above.
#[derive(Deserialize)]
struct Estimates {
    mean: Estimate,
}

/// `sample.json` — one iteration count per sample.
#[derive(Deserialize)]
struct Sample {
    iters: Vec<f64>,
}

/// Every directory named `new` holding a `benchmark.json`, at any depth.
///
/// Depth-agnostic because criterion's layout varies with how a benchmark was
/// declared: `<group>/<function>` for a plain one, `<group>/<function>/<param>`
/// for `BenchmarkId::new(f, p)`, and a bare `<name>` for an ungrouped
/// `bench_function`. A walk fixed at two levels silently skipped the first and
/// third, converting a subset and reporting success — and on a tree of only
/// those, reported that nothing had run.
fn benchmark_dirs(dir: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    // Sorted so a run is reproducible in order as well as in content.
    entries.sort();
    for entry in entries {
        match entry.file_name().and_then(|n| n.to_str()) {
            // criterion's own HTML index, never a benchmark.
            Some("report") => continue,
            Some("new") if entry.join("benchmark.json").is_file() => {
                found.push(entry);
                continue;
            }
            // criterion's previous run and its diff against it.
            Some("base" | "change") => continue,
            _ => {}
        }
        benchmark_dirs(&entry, found)?;
    }
    Ok(())
}

fn read<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn main() {
    // Validates BENCH_TRIGGER before any record is built, for the reason the
    // rigs do: it is otherwise read when the first report is emitted.
    benchmarks::preflight();
    if std::env::var_os("BENCH_TRIGGER").is_none() {
        eprintln!(
            "::error::BENCH_TRIGGER is unset, so these would record as `manual` and be publishable."
        );
        eprintln!("A converted criterion run is a comparison arm: set BENCH_TRIGGER=dispatched.");
        std::process::exit(1);
    }

    let mut args = std::env::args().skip(1);
    let Some(root) = args.next() else {
        eprintln!("usage: criterion_to_report <DIR>");
        eprintln!("There is no default: target/criterion is shared across runs, and converting");
        eprintln!("it would stamp stale measurements with the current commit. Point criterion at");
        eprintln!("a per-leg CRITERION_HOME and pass that — see the module documentation.");
        std::process::exit(1);
    };
    if args.next().is_some() {
        eprintln!("::error::criterion_to_report takes exactly one directory.");
        std::process::exit(1);
    }
    let root = PathBuf::from(root);
    if !root.is_dir() {
        eprintln!("::error::no criterion output at {}", root.display());
        std::process::exit(1);
    }

    let mut dirs = Vec::new();
    benchmark_dirs(&root, &mut dirs).unwrap_or_else(|e| panic!("walk {}: {e}", root.display()));
    // An empty walk is an error, not an empty answer. A criterion step that ran
    // nothing still exits 0 — an emptied `criterion_group!` is the simplest way
    // there — and converting nothing would render as "no change".
    if dirs.is_empty() {
        eprintln!(
            "::error::{} holds no criterion results; nothing ran, or the wrong directory was given.",
            root.display()
        );
        std::process::exit(1);
    }

    // Every file is read before any record is emitted, so a malformed tree
    // fails whole rather than leaving a partly written leg.
    let mut pending = Vec::with_capacity(dirs.len());
    for dir in &dirs {
        let sample_path = dir.join("sample.json");
        // Checked rather than assumed: criterion writes the two files under one
        // block but wraps each separately, so a tree carrying estimates and no
        // sample is reachable.
        if !sample_path.is_file() {
            eprintln!("::error::{} has no sample.json.", dir.display());
            std::process::exit(1);
        }
        let bench: Benchmark = read(&dir.join("benchmark.json"));
        let estimates: Estimates = read(&dir.join("estimates.json"));
        let sample: Sample = read(&sample_path);
        pending.push((bench, estimates, sample));
    }

    for (bench, estimates, sample) in pending {
        // criterion reports times in nanoseconds per iteration.
        let mean = estimates.mean;
        // `n` is the schema's repetition count, so it is the number of samples
        // behind the interval and not the iterations inside them. Writing the
        // iteration total put seven-figure values into a column the site
        // renders as repetitions everywhere else.
        let metric = Metric::minimize(mean.point_estimate, "ns")
            .with_ci(
                mean.confidence_interval.lower_bound,
                mean.confidence_interval.upper_bound,
            )
            .with_n(sample.iters.len() as u64);

        let mut rep = Report::measurement(&bench.group_id);
        if let Some(function) = &bench.function_id {
            rep = rep.variant("function", function.clone());
        }
        if let Some(param) = &bench.value_str {
            rep = rep.variant("param", param.clone());
        }
        benchmarks::report(&rep.metric("ns_per_iter", metric));
    }

    eprintln!("converted {} criterion benchmark(s)", dirs.len());
}
