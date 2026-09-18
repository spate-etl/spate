//! # Repository automation
//!
//! Decides which CI jobs a change needs, and writes the answers to
//! `$GITHUB_OUTPUT`. The selection is defined once here and runs the same way
//! locally and in CI.
//!
//! ## Example
//!
//! ```sh
//! cargo xtask ci-changes
//! ```
//!
//! ## Mechanism
//!
//! `cargo xtask` is an alias that builds this binary and forwards the remaining
//! arguments to it. Workflows name `--manifest-path xtask/Cargo.toml` instead,
//! so a step reads as what it runs.
//!
//! Filtering is per job. A workflow skipped by `on: paths:` never reports its
//! checks, and a required check that never reports blocks a pull request
//! forever, where a skipped *job* reports success.
//!
//! ## Discovery
//!
//! `cargo xtask help`, and `cargo xtask <command> help` for one command's
//! arguments.
//!
//! ## Reference
//!
//! The [`cargo-xtask` convention](https://github.com/matklad/cargo-xtask).

// A command-line tool writes to stdout and stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod classify;
mod event;
mod graph;
mod outputs;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use event::Diff;

const HELP: &str = "\
spate repository automation

Usage:
  cargo xtask <command> [options]

Commands:
  ci-changes    Decide which CI jobs a change needs

Options for ci-changes:
  --classify-paths FILE   Classify a NUL-separated path list instead of a diff
  -h, --help              Show this message
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("ci-changes") => match ci_changes(&args[1..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("::error::xtask: {e}");
                ExitCode::FAILURE
            }
        },
        Some("-h" | "--help" | "help") | None => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("xtask: unknown command '{other}'\n\n{HELP}");
            ExitCode::FAILURE
        }
    }
}

fn ci_changes(args: &[String]) -> Result<(), String> {
    let root = repo_root()?;
    let graph = graph::Graph::load(&root)?;
    let lanes = extra_clickhouse_lanes(&root)?;
    let (ev, ctx) = event::from_environment();

    // An argument rather than an environment variable: an exported variable
    // could turn a real classification into a synthetic one.
    let (ev, diff): (classify::Event, Box<dyn Diff>) = match args.first().map(String::as_str) {
        Some("--classify-paths") => {
            let file = args.get(1).ok_or("--classify-paths needs a file")?;
            let text = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
            // The list stands in for a diff, so it classifies as one whatever
            // event the environment names.
            (
                classify::Event::PullRequest,
                Box::new(event::PathList::from_nul_separated(&text)),
            )
        }
        Some(other) => return Err(format!("unknown option '{other}'")),
        None => (ev, Box::new(event::GitDiff::new(&root, ev))),
    };

    let (ev, paths, fell_back) = event::resolve(ev, diff.as_ref());
    if fell_back {
        println!("note: no usable diff; running everything.");
    }

    let mut out = classify::classify(&paths, ev, &ctx, &graph, &lanes);

    // Push mode force-runs every other job as the last line of defence, while
    // the packaging and floors gates select on their own diff.
    if std::env::var("EVENT_NAME").as_deref() == Ok("push") {
        out.manifests = event::manifest_reach(diff.as_ref()).unwrap_or_else(|| {
            println!("note: no usable before..HEAD diff; the manifest gate fails closed.");
            true
        });
    }

    print!("{out}");
    if let Ok(path) = std::env::var("GITHUB_OUTPUT") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("{path}: {e}"))?;
        write!(f, "{out}").map_err(|e| format!("{path}: {e}"))?;
    }
    Ok(())
}

fn repo_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask/ has no parent".to_string())
}

/// The ClickHouse lanes needing a job beyond the primary one. The lane names
/// live in `ci/clickhouse/`, so adding or repointing one needs no edit here.
fn extra_clickhouse_lanes(root: &Path) -> Result<Vec<String>, String> {
    let out = std::process::Command::new("./scripts/container-image.sh")
        .args(["--extra-lanes", "clickhouse"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("container-image.sh: {e}"))?;
    if !out.status.success() {
        return Err(format!("container-image.sh exited {}", out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect())
}
