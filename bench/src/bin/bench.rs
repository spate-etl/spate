//! The `bench` CLI.
//!
//! Five subcommands, deliberately separable:
//!
//! - `list` asks the workspace what it can measure. No build, no git.
//! - `run` measures the tree in front of it and writes a leg. Git-agnostic:
//!   it never resolves a reference, so what it reports is the checkout it
//!   measured.
//! - `compare` reads two leg directories and renders. Run-agnostic: it does not
//!   care how they were produced, which is what lets a report be re-rendered in
//!   another format without measuring anything again.
//! - `ab` does all of it: worktree the reference, build both legs,
//!   interleave, compare.
//! - `arms` is `ab` over the other axis: one tree, two feature sets, two build
//!   directories, otherwise identical.
//!
//! The separation is the point. A run that took twenty minutes can be rendered
//! as Markdown afterwards without repeating it, and a leg can be kept and
//! compared against something else.
//!
//! # Two `run`s are not an `ab`
//!
//! `run` calibrates its own iteration count, because a lone leg has nothing to
//! inherit one from. Two `run`s therefore pin two counts for the same case, and
//! `compare` will not pair them: a mean over ninety-seven iterations and a mean
//! over a hundred and twelve are not the same estimate. Every such case is
//! demoted, which with no case left leaves a zero-row table and exit 1. Two
//! `run`s left at the default `--leg` do not get that far: both directories
//! hold the `head` leg, which is refused outright. Nor does a pair of `run`s
//! interleave, so whatever the machine did over the first leg's run lands on
//! that leg alone.
//!
//! `run` produces a leg to keep, a baseline to compare something else against
//! later, rather than half a comparison.
//!
//! `list --cases` is the exception to "no build": the case list lives in the
//! compiled target rather than in a manifest, which is what stops the list and
//! the run ever disagreeing, so asking each target what it declares means
//! building it. A bare `list` names the targets and does not.
//!
//! # Flags shared by `run`, `ab` and `arms`
//!
//! | Flag | Default | Meaning |
//! |---|---|---|
//! | `--replicates` / `-n` | `10` | Measured replicates per case |
//! | `--package` / `-p` | every package | Only the targets this package declares |
//! | `--filter` | none | Only cases whose id contains this substring |
//! | `--seed` | `20260804` | Corpus seed, identical on both legs and across replicates |
//! | `--target-ms` | `50` | How long one calibrated measured region should take |
//! | `--warmup-ms` | `50` | Unmeasured warm-up before each region |
//! | `--out` | under the bench cache | Where to write the leg or legs |
//!
//! `run` and `ab` take `--features` / `--all-features`, forwarded to cargo
//! identically on both legs. `arms` takes them per arm instead, through
//! `--base-features` / `--head-features` and the two `--*-all-features` flags,
//! because differing there is the whole of what it measures. An empty list is
//! dropped rather than forwarded, so an arm with no features asked for is the
//! default set.
//!
//! `--package` chooses which targets are built and `--filter` chooses cases
//! within them, so the two compose. Names are exact and the flag repeats. The
//! selection is checked against the working tree before anything is built, so a
//! misspelling costs no compilation.
//!
//! A selection narrows the build, never the resolved features the two legs are
//! guarded on: a bench binary links what its package depends on, so a set
//! narrowed to the selection would stop guarding packages the leg compiled.
//! A narrowed run can therefore be refused over a feature neither leg built,
//! which `--allow features` waives.
//!
//! `--filter` and the feature flags also apply to `list --cases`: a filter is a
//! filter on case ids, so it needs the case list to filter. `--package` names
//! targets rather than cases, so it applies to a bare `list` as well. `run`
//! takes `--leg NAME` (default `head`) for the name stamped into every record.
//!
//! `arms` builds each arm into its own directory under the bench cache, keyed by
//! the flags it was given and kept between runs. Neither arm uses the
//! repository's `target/`: cargo holds one build per directory, so two arms
//! sharing one would rebuild each other away, and using the warm one would
//! charge the next ordinary `cargo build` for a rebuild it did not ask for.
//!
//! A child still running thirty seconds in, or twenty times
//! `--target-ms` + `--warmup-ms`, whichever is longer, prints a `SLOW` line
//! naming its leg, its target and the call, and prints it again at every period
//! after that. Nothing is stopped: the line says which of a run's many children
//! is the slow one, and Ctrl-C is what ends it.
//!
//! `compare`, `ab` and `arms` take `--format` (`table`, `markdown` or `json`)
//! and `--allow`, whose values are the guarded field names plus `digest` and
//! `build`, which waive the two per-case guards. On the arm axis the
//! resolved feature set is the subject rather than a guard, so an `arms` run
//! needs no waiver for it and the header names both arms' sets instead of
//! announcing a bypass.
//!
//! `--format markdown`
//! produces the shape a pull-request comment carries: a header naming both
//! builds, the significant-changes table, then the informational rows and the
//! full table in collapsed sections, then anything that could not be compared,
//! then the decision rule, also collapsed, because a reader who wants it knows
//! to look and a reader who does not should see the table first. `--format json`
//! carries every row with its interval and verdict, everything that could not be
//! compared with the reason it could not, and any guarded field the two legs
//! disagree about. Its `schema` field is the report's version; its `verdict` and
//! `cause` fields are tokens a script matches on, where the two human formats
//! phrase a verdict for a reader and state a cause only as prose.
//!
//! # Exit codes, and three refusals worth knowing in advance
//!
//! Exit code 2 means the thing you named does not exist: anything the parser
//! rejects, a reference or directory that is not there, a `--package` the
//! *working tree* declares no wall-clock target in, and not being inside a git
//! repository at all. Exit code 1 means something failed while running, which
//! includes a selected package the working tree declares a target in and the
//! reference does not: the name is right and the comparison is impossible.
//!
//! A comparison in which *nothing* was comparable also exits 1, after writing
//! the report. That is "the command did no work", not a verdict: a comparison
//! that pairs cases and finds a regression exits 0, because nothing in this tier
//! gates anything. The report is written either way, since the reason nothing
//! paired is the part of it worth reading.
//!
//! Three refusals are easier to recognize than to diagnose: two packages
//! declaring a `_wall` target of the same name; a two-leg run whose legs share
//! no case at all, which a `--filter` matching nothing looks like; and
//! an `arms` in which every judged case declared the same subject on both arms,
//! which is usually a feature name spelled for the wrong package.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Arg, ArgAction, ArgMatches, Command as Cli, value_parser};
use spate_bench::ab::{self, Axis, BASE_LEG, HEAD_LEG, Plan};
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
        Some(("arms", args)) => report(arms_run(args)),
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
    let package = Arg::new("package")
        .long("package")
        .short('p')
        .value_name("NAME")
        .action(ArgAction::Append)
        .help("Only targets this package declares, repeatable");
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
    // The values are the guard names themselves, so an unrecognized one is
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
                .arg(package.clone())
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
                .arg(package.clone())
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
                .arg(package.clone())
                .arg(filter.clone())
                .arg(replicates.clone())
                .arg(seed.clone())
                .arg(target_ms.clone())
                .arg(warmup_ms.clone())
                .arg(features)
                .arg(all_features)
                .arg(format.clone())
                .arg(allow.clone()),
        )
        .subcommand(
            Cli::new("arms")
                .about("Compare two feature arms of this working tree")
                .arg(
                    Arg::new("out")
                        .long("out")
                        .value_name("DIR")
                        .help("Where to write both legs (default: under the bench cache)"),
                )
                .arg(
                    Arg::new("base-features")
                        .long("base-features")
                        .value_name("LIST")
                        .help("Features for the base arm (default: none)"),
                )
                .arg(
                    Arg::new("head-features")
                        .long("head-features")
                        .value_name("LIST")
                        .help("Features for the head arm (default: none)"),
                )
                .arg(
                    Arg::new("base-all-features")
                        .long("base-all-features")
                        .action(ArgAction::SetTrue)
                        .help("Pass --all-features to the base arm"),
                )
                .arg(
                    Arg::new("head-all-features")
                        .long("head-all-features")
                        .action(ArgAction::SetTrue)
                        .help("Pass --all-features to the head arm"),
                )
                .arg(package)
                .arg(filter)
                .arg(replicates)
                .arg(seed)
                .arg(target_ms)
                .arg(warmup_ms)
                .arg(format)
                .arg(allow),
        )
}

/// The packages a command was narrowed to, in the order given.
fn packages(args: &ArgMatches) -> Vec<String> {
    args.get_many::<String>("package")
        .map(|values| values.cloned().collect())
        .unwrap_or_default()
}

/// Refuses a `--package` this working tree declares no wall-clock target in,
/// before anything is built.
///
/// Each leg refuses the same thing, but `ab` prepares the base leg first, and
/// that leg is a worktree at the reference: a name misspelled here would
/// otherwise be reported against a tree the caller is not looking at. Only run
/// when a selection was given, since it costs a `cargo metadata`.
fn check_packages(
    repo: &Path,
    packages: &[String],
    feature_args: &[String],
) -> Result<(), Failure> {
    if packages.is_empty() {
        return Ok(());
    }
    cargo::discover(repo, feature_args)?
        .select(packages, &repo.display().to_string())
        .map_err(|message| Failure {
            message,
            code: EXIT_NOT_FOUND,
        })
}

fn list(args: &ArgMatches) -> Result<(), Failure> {
    let repo = repo_root()?;
    let feature_args = feature_args(args);
    let packages = packages(args);
    let mut discovery = cargo::discover(&repo, &feature_args)?;

    if discovery.targets.is_empty() {
        return Err(format!(
            "no wall-clock bench targets in {}. They are named \
             `crates/<pkg>/benches/<name>{}.rs`.",
            repo.display(),
            cargo::WALL_SUFFIX
        )
        .into());
    }
    // Narrowing what this command already discovered is the pre-check the other
    // subcommands make with `check_packages`, and it narrows a bare `list` too.
    discovery
        .select(&packages, &repo.display().to_string())
        .map_err(|message| Failure {
            message,
            code: EXIT_NOT_FOUND,
        })?;

    if !args.get_flag("cases") {
        let mut out = std::io::stdout().lock();
        for target in &discovery.targets {
            let _ = writeln!(out, "{} {}", target.package, target.target);
        }
        return Ok(());
    }

    // Listing cases means running the binaries, which means building them.
    let plan = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out: PathBuf::new(),
        leg: HEAD_LEG.to_owned(),
        packages,
        filter: args.get_one::<String>("filter").cloned(),
        seed: 0,
        replicates: 0,
        target_ms: 1,
        warmup_ms: 0,
        feature_args,
        axis: Axis::Commit,
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
    let feature_args = feature_args(args);
    let packages = packages(args);
    check_packages(&repo, &packages, &feature_args)?;

    let plan = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out,
        leg,
        packages,
        filter: args.get_one::<String>("filter").cloned(),
        seed: *args.get_one::<u64>("seed").expect("defaulted"),
        replicates: *args.get_one::<u32>("replicates").expect("defaulted"),
        target_ms: *args.get_one::<u64>("target-ms").expect("defaulted"),
        warmup_ms: *args.get_one::<u64>("warmup-ms").expect("defaulted"),
        feature_args,
        // A bare `run` is one leg, so it has no axis of its own. It records the
        // ordinary one, which is what makes two `run`s comparable with each
        // other and refuses to pair either with an arm leg.
        axis: Axis::Commit,
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
    let packages = packages(args);
    check_packages(&repo, &packages, &feature_args)?;
    let common = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir(&repo),
        out: out.join(HEAD_LEG),
        leg: HEAD_LEG.to_owned(),
        packages,
        filter: args.get_one::<String>("filter").cloned(),
        seed: *args.get_one::<u64>("seed").expect("defaulted"),
        replicates: *args.get_one::<u32>("replicates").expect("defaulted"),
        target_ms: *args.get_one::<u64>("target-ms").expect("defaulted"),
        warmup_ms: *args.get_one::<u64>("warmup-ms").expect("defaulted"),
        feature_args,
        axis: Axis::Commit,
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
    finish(&outcome, args)
}

fn arms_run(args: &ArgMatches) -> Result<(), Failure> {
    let repo = repo_root()?;
    let out = args
        .get_one::<String>("out")
        .map_or_else(default_out, PathBuf::from);
    outside_repo(&repo, &out, "a leg")?;

    let base_features = named_feature_args(args, "base-all-features", "base-features");
    let head_features = named_feature_args(args, "head-all-features", "head-features");
    let base_target_dir = worktree::arm_target_dir(BASE_LEG, &base_features);
    let head_target_dir = worktree::arm_target_dir(HEAD_LEG, &head_features);
    for dir in [&base_target_dir, &head_target_dir] {
        outside_repo(&repo, dir, "an arm's build artifacts")?;
    }

    // Either arm's answer would do. A bench target's existence is a property of
    // the manifest, and no feature cargo resolves changes it.
    let packages = packages(args);
    check_packages(&repo, &packages, &head_features)?;

    // Both arms are the working tree. There is no worktree and no reference:
    // what separates them is what cargo was asked to compile, so the tree they
    // are compiled from has to be the same one down to the uncommitted changes.
    let common = Plan {
        dir: repo.clone(),
        target_dir: head_target_dir,
        out: out.join(HEAD_LEG),
        leg: HEAD_LEG.to_owned(),
        packages,
        filter: args.get_one::<String>("filter").cloned(),
        seed: *args.get_one::<u64>("seed").expect("defaulted"),
        replicates: *args.get_one::<u32>("replicates").expect("defaulted"),
        target_ms: *args.get_one::<u64>("target-ms").expect("defaulted"),
        warmup_ms: *args.get_one::<u64>("warmup-ms").expect("defaulted"),
        feature_args: head_features,
        axis: Axis::Arm,
    };
    let base_plan = Plan {
        target_dir: base_target_dir,
        out: out.join(BASE_LEG),
        leg: BASE_LEG.to_owned(),
        feature_args: base_features,
        ..common.clone()
    };

    let allow: Vec<String> = args
        .get_many::<String>("allow")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();

    let outcome = ab::arms(&base_plan, &common, &allow)?;
    finish(&outcome, args)
}

/// Writes a two-leg run's report, then says where its legs were kept.
fn finish(outcome: &ab::AbOutcome, args: &ArgMatches) -> Result<(), Failure> {
    let rendered = write_report(&outcome.comparison, args);

    // Printed whatever the report said, and after it: a long run has to end by
    // saying how to render it again without repeating it, and the run that most
    // needs saying is the one that ended badly.
    let _ = writeln!(
        std::io::stderr(),
        "\nspate-bench: legs kept at\n  {}\n  {}\n\
         Re-render with: make bench-compare BASE={} HEAD={} FORMAT=markdown",
        outcome.base_dir.display(),
        outcome.head_dir.display(),
        outcome.base_dir.display(),
        outcome.head_dir.display(),
    );
    rendered
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
    })?;

    // The report is written first, then the exit code decided. The reason
    // nothing paired is in the report's *Not comparable* section, and a
    // non-zero exit that suppressed it would take the diagnosis with it.
    //
    // Classified by `Summary`, the same value both renderers print their
    // headline from, so the sentence a reader sees and the code a script
    // branches on cannot disagree.
    //
    // Matched exhaustively rather than compared against the one variant, so a
    // `Summary` gaining a case that should also exit non-zero is a compile
    // error here rather than a silent zero.
    match render::Summary::of(comparison) {
        render::Summary::NothingComparable => Err(Failure {
            message: "nothing was comparable; the report says why under 'Not comparable'"
                .to_owned(),
            code: 1,
        }),
        // A rule that judged nothing, cleared nothing, or found something are
        // all runs that did the work. Only the first is a failure to do it.
        render::Summary::NothingJudged { .. }
        | render::Summary::NoneCleared
        | render::Summary::Findings => Ok(()),
    }
}

fn feature_args(args: &ArgMatches) -> Vec<String> {
    named_feature_args(args, "all-features", "features")
}

/// The cargo feature flags one arm was given, under whatever names carry them.
///
/// `arms` takes two sets at once, so the flag names are a parameter rather than
/// fixed. An empty or blank list is dropped rather than forwarded: cargo rejects
/// `--features ''`, and `make bench-arms HEAD_FEATURES=…` with no
/// `BASE_FEATURES` produces exactly that.
fn named_feature_args(args: &ArgMatches, all: &str, list: &str) -> Vec<String> {
    let mut out = Vec::new();
    if args.get_flag(all) {
        out.push("--all-features".to_owned());
    }
    if let Some(list) = args
        .get_one::<String>(list)
        .filter(|l| !l.trim().is_empty())
    {
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

#[cfg(test)]
mod tests {
    use super::{cli, named_feature_args};

    fn arms_matches(args: &[&str]) -> clap::ArgMatches {
        let full: Vec<&str> = ["bench", "arms"].iter().chain(args).copied().collect();
        cli()
            .get_matches_from(full)
            .subcommand_matches("arms")
            .expect("arms")
            .clone()
    }

    /// `make bench-arms HEAD_FEATURES=…` passes `--base-features ""`
    /// unconditionally, so the blank path is on the common route rather than an
    /// edge. Forwarded, it becomes `--features ''`, which cargo rejects.
    #[test]
    fn a_blank_feature_list_is_dropped_rather_than_forwarded() {
        for blank in ["", "   "] {
            let matches = arms_matches(&["--base-features", blank]);
            assert!(
                named_feature_args(&matches, "base-all-features", "base-features").is_empty(),
                "a blank list reached cargo as {blank:?}"
            );
        }
    }

    /// Each arm reads its own flags and neither reads the other's, which is the
    /// whole of what makes the two plans differ.
    #[test]
    fn each_arm_reads_only_its_own_feature_flags() {
        let matches = arms_matches(&["--head-features", "spate-json/simd", "--base-all-features"]);
        assert_eq!(
            named_feature_args(&matches, "head-all-features", "head-features"),
            ["--features", "spate-json/simd"]
        );
        assert_eq!(
            named_feature_args(&matches, "base-all-features", "base-features"),
            ["--all-features"]
        );
    }

    /// The flag appends one value per occurrence. A selector taking several
    /// values at once would swallow `ab`'s required reference in
    /// `bench ab -p spate-avro main`.
    #[test]
    fn the_package_selector_repeats() {
        let matches = arms_matches(&["-p", "spate-avro", "--package", "spate-json"]);
        assert_eq!(super::packages(&matches), ["spate-avro", "spate-json"]);
    }

    /// The parser is built once and shared by five subcommands; a duplicated
    /// argument id or a clashing long name is a panic rather than a message,
    /// and only asserting it here catches it before a user does.
    #[test]
    fn the_parser_is_well_formed() {
        cli().debug_assert();
    }
}
