//! The `bench` CLI.
//!
//! Four subcommands, deliberately separable:
//!
//! - `list` asks the workspace what it can measure. No build, no git.
//! - `run` measures the tree in front of it and writes a leg. Git-agnostic:
//!   it never resolves a reference, so what it reports is the checkout it
//!   measured.
//! - `compare` reads two leg directories and renders. Run-agnostic: it does not
//!   care how they were produced, which is what lets a report be re-rendered in
//!   another format without measuring anything again.
//! - `ab` is the one that does all of it — worktree the reference, build both
//!   legs, interleave, compare.
//!
//! The separation is the point. A run that took twenty minutes can be rendered
//! as Markdown afterwards without repeating it, and a leg can be kept and
//! compared against something else.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Arg, ArgAction, ArgMatches, Command as Cli, value_parser};
use spate_bench::ab::{self, BASE_LEG, HEAD_LEG, Plan};
use spate_bench::cargo;
use spate_bench::compare::{Comparison, load_leg};
use spate_bench::render;
use spate_bench::worktree::{self, Worktree};

/// The exit code for "the thing you named does not exist".
///
/// Distinct from a general failure so a script can tell a typo from a build
/// that broke.
const EXIT_NOT_FOUND: i32 = 2;

fn main() {
    let matches = cli().get_matches();
    let code = match matches.subcommand() {
        Some(("list", args)) => report(list(args)),
        Some(("run", args)) => report(run(args)),
        Some(("compare", args)) => report(compare(args)),
        Some(("ab", args)) => report(ab_run(args)),
        _ => {
            let _ = writeln!(std::io::stderr(), "bench: no subcommand; try --help");
            1
        }
    };
    std::process::exit(code);
}

fn report(outcome: Result<(), Failure>) -> i32 {
    match outcome {
        Ok(()) => 0,
        Err(Failure { message, code }) => {
            let _ = writeln!(std::io::stderr(), "bench: {message}");
            code
        }
    }
}

/// An error, and the exit code it earns.
struct Failure {
    message: String,
    code: i32,
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self { message, code: 1 }
    }
}

fn cli() -> Cli {
    let filter = Arg::new("filter")
        .long("filter")
        .value_name("SUBSTRING")
        .help("Only cases whose id contains this");
    let replicates = Arg::new("replicates")
        .long("replicates")
        .short('n')
        .value_name("N")
        .default_value("10")
        .value_parser(value_parser!(u32).range(1..))
        .help("Measured replicates per case");
    let seed = Arg::new("seed")
        .long("seed")
        .value_name("N")
        .default_value("20260804")
        .value_parser(value_parser!(u64))
        .help("Corpus seed, identical on both legs and across replicates");
    let target_ms = Arg::new("target-ms")
        .long("target-ms")
        .value_name("MS")
        .default_value("50")
        .value_parser(value_parser!(u64).range(1..))
        .help("How long one calibrated measured region should take");
    let warmup_ms = Arg::new("warmup-ms")
        .long("warmup-ms")
        .value_name("MS")
        .default_value("50")
        .value_parser(value_parser!(u64))
        .help("Unmeasured warm-up before each region");
    let features = Arg::new("features")
        .long("features")
        .value_name("LIST")
        .help("Forwarded to cargo, identically on both legs");
    let all_features = Arg::new("all-features")
        .long("all-features")
        .action(ArgAction::SetTrue)
        .help("Forwarded to cargo, identically on both legs");
    let format = Arg::new("format")
        .long("format")
        .value_name("FORMAT")
        .default_value("table")
        .value_parser(["table", "markdown", "json"])
        .help("How to render the comparison");
    // The values are the guard names themselves, so an unrecognised one is
    // rejected by clap rather than silently waiving nothing while the report
    // header announces a waived guard.
    let allow = Arg::new("allow")
        .long("allow")
        .value_name("FIELD")
        .action(ArgAction::Append)
        .value_parser(spate_bench::compare::allowable())
        .help("Waive a comparability guard, by name");

    Cli::new("bench")
        .about("Wall-clock A/B benchmarks for the Spate workspace")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Cli::new("list")
                .about("Every wall-clock bench target, and optionally its cases")
                .arg(
                    Arg::new("cases")
                        .long("cases")
                        .action(ArgAction::SetTrue)
                        .help("Build the targets and list their cases too"),
                )
                .arg(filter.clone())
                .arg(features.clone())
                .arg(all_features.clone()),
        )
        .subcommand(
            Cli::new("run")
                .about("Measure this tree and write one leg of records")
                .arg(
                    Arg::new("out")
                        .long("out")
                        .value_name("DIR")
                        .help("Where to write the leg (default: under the bench cache)"),
                )
                .arg(
                    Arg::new("leg")
                        .long("leg")
                        .value_name("NAME")
                        .default_value(HEAD_LEG)
                        .help("The leg name stamped into every record"),
                )
                .arg(filter.clone())
                .arg(replicates.clone())
                .arg(seed.clone())
                .arg(target_ms.clone())
                .arg(warmup_ms.clone())
                .arg(features.clone())
                .arg(all_features.clone()),
        )
        .subcommand(
            Cli::new("compare")
                .about("Render two leg directories against each other")
                .arg(
                    Arg::new("base")
                        .required(true)
                        .value_name("BASE_DIR")
                        .value_parser(value_parser!(PathBuf)),
                )
                .arg(
                    Arg::new("head")
                        .required(true)
                        .value_name("HEAD_DIR")
                        .value_parser(value_parser!(PathBuf)),
                )
                .arg(format.clone())
                .arg(allow.clone()),
        )
        .subcommand(
            Cli::new("ab")
                .about("Compare this working tree against a reference")
                .arg(
                    Arg::new("ref")
                        .required(true)
                        .value_name("REF")
                        .help("The base: any commit-ish this repository knows"),
                )
                .arg(
                    Arg::new("out")
                        .long("out")
                        .value_name("DIR")
                        .help("Where to write both legs (default: under the bench cache)"),
                )
                .arg(filter)
                .arg(replicates)
                .arg(seed)
                .arg(target_ms)
                .arg(warmup_ms)
                .arg(features)
                .arg(all_features)
                .arg(format)
                .arg(allow),
        )
}

fn list(args: &ArgMatches) -> Result<(), Failure> {
    let repo = repo_root()?;
    let feature_args = feature_args(args);
    let discovery = cargo::discover(&repo, &feature_args)?;

    let mut out = std::io::stdout().lock();
    if discovery.targets.is_empty() {
        return Err(format!(
            "no wall-clock bench targets in {}. They are named \
             `crates/<pkg>/benches/<name>{}.rs`.",
            repo.display(),
            cargo::WALL_SUFFIX
        )
        .into());
    }

    if !args.get_flag("cases") {
        for target in &discovery.targets {
            let _ = writeln!(out, "{} {}", target.package, target.target);
        }
        return Ok(());
    }
    drop(out);

    // Listing cases means running the binaries, which means building them.
    let plan = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out: PathBuf::new(),
        leg: HEAD_LEG.to_owned(),
        filter: args.get_one::<String>("filter").cloned(),
        seed: 0,
        replicates: 0,
        target_ms: 1,
        warmup_ms: 0,
        feature_args,
    };
    for (target, listing) in ab::listings(&plan)? {
        for case in listing.cases {
            let mut line = format!("{} {} {}", listing.krate, target, case.id);
            if let Some(why) = case.erratic {
                line.push_str(&format!("  [erratic: {why}]"));
            }
            if let Some(iters) = case.iters_hint {
                line.push_str(&format!("  [iters pinned: {iters}]"));
            }
            let _ = writeln!(std::io::stdout().lock(), "{line}");
        }
    }
    Ok(())
}

fn run(args: &ArgMatches) -> Result<(), Failure> {
    let repo = repo_root()?;
    let leg = args
        .get_one::<String>("leg")
        .cloned()
        .unwrap_or_else(|| HEAD_LEG.to_owned());
    let out = args
        .get_one::<String>("out")
        .map_or_else(|| default_out().join(&leg), PathBuf::from);
    outside_repo(&repo, &out, "a leg")?;

    let plan = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out,
        leg,
        filter: args.get_one::<String>("filter").cloned(),
        seed: *args.get_one::<u64>("seed").expect("defaulted"),
        replicates: *args.get_one::<u32>("replicates").expect("defaulted"),
        target_ms: *args.get_one::<u64>("target-ms").expect("defaulted"),
        warmup_ms: *args.get_one::<u64>("warmup-ms").expect("defaulted"),
        feature_args: feature_args(args),
    };

    let written = ab::run(&plan)?;
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{}", written.display());
    Ok(())
}

fn compare(args: &ArgMatches) -> Result<(), Failure> {
    let base_dir = args.get_one::<PathBuf>("base").expect("required");
    let head_dir = args.get_one::<PathBuf>("head").expect("required");
    for dir in [base_dir, head_dir] {
        if !dir.is_dir() {
            return Err(Failure {
                message: format!("{} is not a directory", dir.display()),
                code: EXIT_NOT_FOUND,
            });
        }
    }
    let allow: Vec<String> = args
        .get_many::<String>("allow")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();

    let comparison =
        spate_bench::compare::compare(load_leg(base_dir)?, load_leg(head_dir)?, &allow)?;
    write_report(&comparison, args)
}

fn ab_run(args: &ArgMatches) -> Result<(), Failure> {
    let repo = repo_root()?;
    let git_ref = args.get_one::<String>("ref").expect("required");

    // Resolved here as well as inside `ab`, so a typo exits with the
    // not-found code rather than the general one.
    let commit = Worktree::resolve(&repo, git_ref).map_err(|message| Failure {
        message,
        code: EXIT_NOT_FOUND,
    })?;

    let out = args
        .get_one::<String>("out")
        .map_or_else(default_out, PathBuf::from);
    outside_repo(&repo, &out, "a leg")?;
    let base_target_dir =
        worktree::cache_root().join(format!("target-{}", &commit[..12.min(commit.len())]));
    outside_repo(&repo, &base_target_dir, "the base leg's build artifacts")?;
    let feature_args = feature_args(args);
    let common = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out: out.join(HEAD_LEG),
        leg: HEAD_LEG.to_owned(),
        filter: args.get_one::<String>("filter").cloned(),
        seed: *args.get_one::<u64>("seed").expect("defaulted"),
        replicates: *args.get_one::<u32>("replicates").expect("defaulted"),
        target_ms: *args.get_one::<u64>("target-ms").expect("defaulted"),
        warmup_ms: *args.get_one::<u64>("warmup-ms").expect("defaulted"),
        feature_args,
    };
    let base_plan = Plan {
        // `dir` is replaced by the worktree's path inside `ab`. The target dir
        // is keyed by the base commit and kept, so a second `ab` against the
        // same reference reuses the compiled *dependencies*. The workspace's
        // own crates are recompiled either way: the worktree is removed when
        // the run ends and recreated with fresh timestamps on the next one.
        target_dir: base_target_dir,
        out: out.join(BASE_LEG),
        leg: BASE_LEG.to_owned(),
        ..common.clone()
    };

    let allow: Vec<String> = args
        .get_many::<String>("allow")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();

    let outcome = ab::ab(&repo, git_ref, &base_plan, &common, &allow)?;
    write_report(&outcome.comparison, args)?;

    // Printed last, so a long run ends by saying how to render it again without
    // repeating it.
    let _ = writeln!(
        std::io::stderr(),
        "\nspate-bench: legs kept at\n  {}\n  {}\n\
         Re-render with: make bench-compare BASE={} HEAD={} FORMAT=markdown",
        outcome.base_dir.display(),
        outcome.head_dir.display(),
        outcome.base_dir.display(),
        outcome.head_dir.display(),
    );
    Ok(())
}

fn write_report(comparison: &Comparison, args: &ArgMatches) -> Result<(), Failure> {
    let rendered = match args
        .get_one::<String>("format")
        .map(String::as_str)
        .unwrap_or("table")
    {
        "markdown" => render::markdown(comparison),
        "json" => render::json(comparison)?,
        _ => render::table(comparison),
    };
    // Not discarded: `bench compare … | head` would otherwise exit 0 having
    // dropped the report on the floor.
    writeln!(std::io::stdout().lock(), "{rendered}").map_err(|e| Failure {
        message: format!("could not write the report: {e}"),
        code: 1,
    })
}

fn feature_args(args: &ArgMatches) -> Vec<String> {
    let mut out = Vec::new();
    if args.get_flag("all-features") {
        out.push("--all-features".to_owned());
    }
    if let Some(list) = args.get_one::<String>("features") {
        out.push("--features".to_owned());
        out.push(list.clone());
    }
    out
}

/// The repository the CLI is being run inside.
fn repo_root() -> Result<PathBuf, Failure> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|e| Failure {
            message: format!("could not run git: {e}"),
            code: 1,
        })?;
    if !output.status.success() {
        return Err(Failure {
            message: "not inside a git repository".to_owned(),
            code: EXIT_NOT_FOUND,
        });
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

/// The head leg builds into the repository's own target directory, which is
/// warm; the base leg gets a cache keyed by its commit.
fn head_target_dir(repo: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| repo.join("target"), PathBuf::from)
}

/// Refuses a path inside the repository, whether it came from `--out` or from
/// the cache root.
///
/// The worktree enforces this for itself; legs and the base leg's build
/// artifacts go through here, so `SPATE_BENCH_CACHE=./x` cannot put any of the
/// three in the tree.
fn outside_repo(repo: &Path, path: &Path, what: &str) -> Result<(), Failure> {
    let repo = repo.canonicalize().map_err(|e| Failure {
        message: format!("cannot resolve {}: {e}", repo.display()),
        code: 1,
    })?;
    worktree::ensure_outside(&repo, path, what).map_err(Failure::from)
}

/// Where a run writes when `--out` is not given: outside the repository, so a
/// leg is never something `git status` has an opinion about.
fn default_out() -> PathBuf {
    worktree::cache_root().join(format!("run-{}", spate_bench::record::Record::now_ms()))
}
