//! The two benchmark tiers: instruction counts under valgrind, and wall clock.

use std::path::Path;

use clap::Subcommand;

use crate::run::{self, Outcome, Step};

/// Cases carry their own flags, so the driver's `--package` repeats instead of
/// splitting a value.
#[derive(clap::Args)]
pub(crate) struct Selection {
    /// Narrow to one crate's targets; repeat for several
    #[arg(long, value_name = "CRATE")]
    package: Vec<String>,
    /// Narrow to cases whose name contains this
    #[arg(long, value_name = "SUBSTR")]
    filter: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct Replication {
    #[arg(long, default_value_t = 20)]
    replicates: u32,
    #[arg(long, default_value = "table")]
    format: String,
}

#[derive(Subcommand)]
pub(crate) enum Bench {
    /// Every bench target still compiles (release profile, slow)
    Check,

    /// The counted tier: run every instruction-count bench, then guard the
    /// collected regions
    Counted,

    /// The instruction-count benches alone (needs Linux, valgrind)
    Gungraun {
        /// Run the benches for these packages, or all of them when none is named
        #[arg(long, num_args = 0.., value_name = "CRATE", conflicts_with = "check")]
        run: Option<Vec<String>>,
        /// Build these packages' targets and run none
        #[arg(long, num_args = 0.., value_name = "CRATE")]
        check: Option<Vec<String>>,
        /// Cargo features to build the arm under
        #[arg(long, value_name = "FEATURES")]
        features: Option<String>,
        /// Print the benched packages as JSON
        #[arg(long)]
        pkgs_json: bool,
    },

    /// The guard rejecting a bench whose collected region measured the runtime
    Region {
        #[arg(long, value_name = "LABEL")]
        shard: Option<String>,
        #[arg(value_name = "DIR")]
        dir: Option<String>,
    },

    /// Render the counted-tier summary as a report
    Report {
        #[arg(long, value_name = "FILE")]
        regressions_out: Option<String>,
        #[arg(value_name = "ARG", trailing_var_arg = true)]
        args: Vec<String>,
    },

    /// Every wall-clock bench case, with its flags
    List {
        #[command(flatten)]
        select: Selection,
    },

    /// Compare this tree against a ref
    Ab {
        #[arg(long, default_value = "main", value_name = "REF")]
        r#ref: String,
        #[command(flatten)]
        reps: Replication,
        #[command(flatten)]
        select: Selection,
    },

    /// Compare two feature arms of this tree
    Arms {
        #[arg(long, default_value = "", value_name = "FEATURES")]
        base_features: String,
        #[arg(long, default_value = "", value_name = "FEATURES")]
        head_features: String,
        #[command(flatten)]
        reps: Replication,
        #[command(flatten)]
        select: Selection,
    },

    /// Re-render two legs that have already been measured
    Compare {
        #[arg(value_name = "BASE")]
        base: String,
        #[arg(value_name = "HEAD")]
        head: String,
        #[arg(long, default_value = "table")]
        format: String,
    },
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: Bench) -> Outcome {
    match cmd {
        Bench::Check => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                [
                    "bench",
                    "--no-run",
                    "--workspace",
                    "--all-features",
                    "--locked",
                ],
            ),
        ),
        Bench::Counted => run::steps(
            root,
            explain,
            &[
                Step::new("./scripts/gungraun-benches.sh", ["--run"]),
                Step::new("./scripts/gungraun-collected-region.sh", [] as [&str; 0]),
            ],
        ),
        Bench::Gungraun {
            run: pkgs,
            check,
            features,
            pkgs_json,
        } => {
            let mut s = Step::new("./scripts/gungraun-benches.sh", [] as [&str; 0]);
            if let Some(f) = &features {
                s = s.args(["--features", f]);
            }
            if pkgs_json {
                s = s.arg("--pkgs-json");
            }
            if let Some(pkgs) = &check {
                s = s.arg("--check").args(pkgs);
            }
            if let Some(pkgs) = &pkgs {
                s = s.arg("--run").args(pkgs);
            }
            run::run(root, explain, &s)
        }
        Bench::Region { shard, dir } => {
            let mut s = Step::new("./scripts/gungraun-collected-region.sh", [] as [&str; 0]);
            if let Some(l) = &shard {
                s = s.args(["--shard", l]);
            }
            if let Some(d) = &dir {
                s = s.arg(d);
            }
            run::run(root, explain, &s)
        }
        Bench::Report {
            regressions_out,
            args,
        } => {
            let mut s = Step::new("./scripts/gungraun-report.sh", [] as [&str; 0]);
            if let Some(f) = &regressions_out {
                s = s.args(["--regressions-out", f]);
            }
            run::run(root, explain, &s.args(&args))
        }
        Bench::List { select } => {
            let s = driver(["list", "--cases"]).args(select.flags());
            run::run(root, explain, &s)
        }
        Bench::Ab {
            r#ref,
            reps,
            select,
        } => {
            let s = driver(["ab"])
                .arg(&r#ref)
                .args(reps.flags())
                .args(select.flags());
            run::run(root, explain, &s)
        }
        Bench::Arms {
            base_features,
            head_features,
            reps,
            select,
        } => {
            let s = driver(["arms"])
                .args(["--base-features", &base_features])
                .args(["--head-features", &head_features])
                .args(reps.flags())
                .args(select.flags());
            run::run(root, explain, &s)
        }
        Bench::Compare { base, head, format } => {
            let s = driver(["compare"])
                .args([&base, &head])
                .args(["--format", &format]);
            run::run(root, explain, &s)
        }
    }
}

/// The wall-clock driver, which lives behind `spate-bench`'s `driver` feature.
fn driver<'a>(sub: impl IntoIterator<Item = &'a str>) -> Step<'a> {
    Step::new(
        "cargo",
        [
            "run",
            "-p",
            "spate-bench",
            "--features",
            "driver",
            "--locked",
            "--bin",
            "bench",
            "--",
        ],
    )
    .args(sub)
}

impl Selection {
    fn flags(&self) -> Vec<String> {
        let mut out = Vec::new();
        for p in &self.package {
            out.push("--package".to_owned());
            out.push(p.clone());
        }
        if let Some(f) = &self.filter {
            out.push("--filter".to_owned());
            out.push(f.clone());
        }
        out
    }
}

impl Replication {
    fn flags(&self) -> Vec<String> {
        vec![
            "--replicates".to_owned(),
            self.replicates.to_string(),
            "--format".to_owned(),
            self.format.clone(),
        ]
    }
}
