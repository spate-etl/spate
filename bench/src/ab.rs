//! Running a leg, and running two of them against each other.
//!
//! # Two axes, one comparison
//!
//! Two legs can differ in the tree they were built from or in the features they
//! were built with, and both are worth measuring. [`ab`] takes
//! the first, checking the reference out into a worktree; [`arms`] takes the
//! second, building one tree twice into two directories. Everything after "both
//! legs exist" is [`two_legs`], so the properties below hold on both axes.
//!
//! # The order things happen in, and why
//!
//! 1. **Resolve the reference**, on the commit axis. A mistyped one costs
//!    nothing if it is caught here, and ten minutes of building if it is not.
//! 2. **Build both legs, completely, before measuring anything.** A build
//!    interleaved with measurement would put a compile on one leg's replicate
//!    and not the other's, and the machine is never quieter than while it is
//!    not compiling.
//! 3. **List both legs and intersect.** A case that exists on one side only is
//!    reported as not comparable rather than silently skipped.
//! 4. **Calibrate once, on the base leg, and pin the count for both.** A
//!    self-calibrating head leg would answer a slowdown by running fewer
//!    iterations, which hides it, and would make the resident-set and
//!    allocation totals incomparable, since those are per-region rather than
//!    per-iteration.
//! 5. **Prime each (leg, case) once and mark the record `priming: true`.** The
//!    first execution of a freshly written binary pays for page faults and, on
//!    macOS, a first-run security scan. Unprimed, that cost lands entirely on
//!    whichever leg happens to run replicate 0 first.
//! 6. **Interleave, flipping the leg order on replicate parity.** Replicate *k*
//!    of both legs runs adjacent in time, so whatever the machine was doing at
//!    that moment is common to the pair and cancels when the pair is
//!    differenced. Flipping the order stops a systematic "second one is
//!    warmer" effect accruing to one leg.
//!
//! # Where the run puts things
//!
//! Legs, worktrees and every build directory a run of its own makes live under
//! `$TMPDIR/spate-bench`, or under `SPATE_BENCH_CACHE` when that is set, never
//! inside the repository, where cargo and git would both find them. The worktree
//! is removed when a run ends; the legs and the target directories are kept, and
//! nothing prunes either.
//!
//! A target directory is keyed by what makes its build distinct: the
//! reference's commit for an [`ab`] base, the feature flags for either
//! [`arms`] arm. A second run of the same pair reuses the compiled
//! dependencies. `ab`'s base recompiles the workspace's own crates either way,
//! since the worktree is recreated; `arms` recompiles only what its features
//! changed. They are caches in the ordinary sense: `rm -rf` costs a rebuild and
//! nothing else.
//!
//! Because a leg is a directory of self-describing records, a run that took
//! twenty minutes can be re-rendered as Markdown, or as JSON for a script,
//! without repeating it. Both leg paths are printed when a run finishes, and a
//! leg carries which axis produced it, so a re-render applies the rule the run
//! was measured under.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::cargo;
use crate::compare::{Cause, Comparison, Leg, NotComparable, guard_fingerprints, load_leg};
use crate::fingerprint::BuildFingerprint;
pub use crate::fingerprint::{Axis, BASE_LEG, HEAD_LEG};
use crate::note;
use crate::protocol::Listing;
use crate::record::{CaseId, Record};
use crate::runner::{Measurement, Runner, slow_period};
use crate::worktree::{self, Worktree};

/// What one leg is asked to measure.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Which tree to build.
    pub dir: PathBuf,
    /// Where to put its build artifacts.
    pub target_dir: PathBuf,
    /// Where to write its records.
    pub out: PathBuf,
    /// `base`, `head`, or a name a bare `run` was given.
    pub leg: String,
    /// Substring a case id must contain, if any.
    pub filter: Option<String>,
    /// Corpus seed, identical across legs and replicates.
    pub seed: u64,
    /// How many measured replicates per case.
    pub replicates: u32,
    /// How long a calibrated region should take.
    pub target_ms: u64,
    /// Unmeasured warm-up before each region.
    pub warmup_ms: u64,
    /// Feature flags, forwarded verbatim to cargo.
    ///
    /// The one field an `arms` run deliberately differs on between the two
    /// plans; every other mode passes the same value to both.
    pub feature_args: Vec<String>,
    /// What the comparison this leg belongs to varies.
    pub axis: Axis,
}

/// One leg, built and interrogated, ready to measure.
#[derive(Debug)]
struct Prepared {
    name: String,
    out: PathBuf,
    runners: BTreeMap<String, Runner>,
    cases: BTreeSet<CaseId>,
    listings: Vec<(String, Listing)>,
    fingerprint: BuildFingerprint,
}

/// Builds a leg and asks it what it can measure.
fn prepare(plan: &Plan, git_describe: Option<String>, dirty: bool) -> Result<Prepared, String> {
    let discovery = cargo::discover(&plan.dir, &plan.feature_args)?;
    if discovery.targets.is_empty() {
        return Err(format!(
            "{} declares no wall-clock bench targets. They are named \
             `crates/<pkg>/benches/<name>{}.rs` and need `harness = false`.",
            plan.dir.display(),
            cargo::WALL_SUFFIX
        ));
    }

    note(&format!(
        "building {} target(s) for the {} leg in {}",
        discovery.targets.len(),
        plan.leg,
        plan.dir.display()
    ));
    let binaries = cargo::build(
        &plan.dir,
        &plan.target_dir,
        &discovery.targets,
        &plan.feature_args,
    )?;

    let (rustc, host_triple) = cargo::toolchain(&plan.dir)?;
    let fingerprint = BuildFingerprint {
        protocol: crate::protocol::PROTOCOL_VERSION,
        leg: plan.leg.clone(),
        axis: plan.axis,
        rustc: Some(rustc),
        host_triple: Some(host_triple),
        profile: Some("bench".to_owned()),
        codegen: Some(cargo::codegen_digest(&plan.dir)),
        features: discovery.features.clone(),
        feature_args: plan.feature_args.clone(),
        git_describe,
        dirty,
    };

    let mut runners = BTreeMap::new();
    let mut cases = BTreeSet::new();
    let mut listings = Vec::new();
    // One period for every child of this leg: it is derived from the plan, and
    // the plan does not change while the leg is measured.
    let period = slow_period(plan.target_ms, plan.warmup_ms);
    for target in &discovery.targets {
        let binary = binaries
            .get(&target.target)
            .ok_or_else(|| format!("no binary for '{}'", target.target))?;
        let runner = Runner::open(binary, &plan.dir, &fingerprint, period)?;
        let mut listing = runner.list()?;
        if let Some(filter) = &plan.filter {
            listing
                .cases
                .retain(|case| case.id.contains(filter.as_str()));
        }
        for case in &listing.cases {
            cases.insert(CaseId {
                krate: listing.krate.clone(),
                target: listing.target.clone(),
                case: case.id.clone(),
            });
        }
        runners.insert(target.target.clone(), runner);
        listings.push((target.target.clone(), listing));
    }

    Ok(Prepared {
        name: plan.leg.clone(),
        out: plan.out.clone(),
        runners,
        cases,
        listings,
        fingerprint,
    })
}

/// Builds every wall-clock target in a tree and asks each what it declares.
///
/// What `bench list --cases` prints. It builds, because the case list lives in
/// the compiled target rather than in a manifest, which is what stops the list
/// and the run ever disagreeing.
///
/// # Errors
///
/// When the tree cannot be built or a binary does not speak the protocol.
pub fn listings(plan: &Plan) -> Result<Vec<(String, Listing)>, String> {
    let (describe, dirty) = worktree::describe(&plan.dir);
    Ok(prepare(plan, describe, dirty)?.listings)
}

/// Measures one tree, with no reference to compare it against.
///
/// # Errors
///
/// When the tree cannot be built, a binary does not speak the protocol, or the
/// output directory cannot be written.
pub fn run(plan: &Plan) -> Result<PathBuf, String> {
    // Installed here as well as in `ab`. A `run` has no worktree to leak, but a
    // half-written record does the same damage: `load_leg` refuses a leg whose
    // last line will not parse, and that discards every completed replicate.
    worktree::install_interrupt_handler();
    let (describe, dirty) = worktree::describe(&plan.dir);
    let leg = prepare(plan, describe, dirty)?;
    if leg.cases.is_empty() {
        return Err(no_cases(plan));
    }

    let cases: Vec<CaseId> = leg.cases.iter().cloned().collect();
    let iters = calibrate(&leg, &cases, plan)?;
    let mut writer = Writer::create(&leg.out)?;
    note(&format!("leg written to {}", leg.out.display()));

    for case in &leg.cases {
        measure_one(&leg, case, plan, iters[case], 0, true, &mut writer)?;
    }
    for replicate in 0..plan.replicates {
        note(&format!("replicate {}/{}", replicate + 1, plan.replicates));
        for case in &leg.cases {
            check_interrupt()?;
            measure_one(&leg, case, plan, iters[case], replicate, false, &mut writer)?;
        }
    }

    writer.finish()?;
    Ok(leg.out)
}

/// Everything an `ab` run produced.
#[derive(Debug)]
pub struct AbOutcome {
    /// The comparison, ready to render.
    pub comparison: Comparison,
    /// Where the base leg's records were written.
    pub base_dir: PathBuf,
    /// Where the head leg's records were written.
    pub head_dir: PathBuf,
}

/// Compares the working tree against a reference.
///
/// `base_plan.dir` is ignored, since the reference decides it, and `head_plan.dir`
/// is the working tree as it stands, dirty or not. `head_plan.replicates` is
/// what both legs run: a comparison has one replicate count by definition.
///
/// # Errors
///
/// When the reference names no commit, when either leg fails to build, or when
/// the two legs turn out not to be comparable.
pub fn ab(
    repo: &Path,
    git_ref: &str,
    base_plan: &Plan,
    head_plan: &Plan,
    allow: &[String],
) -> Result<AbOutcome, String> {
    guard_out(base_plan, head_plan)?;

    // Before anything is built: a mistyped reference must not cost a
    // compilation.
    let commit = Worktree::resolve(repo, git_ref)?;
    worktree::install_interrupt_handler();

    let checkout =
        worktree::cache_root().join(format!("worktree-{}", &commit[..12.min(commit.len())]));
    let tree = Worktree::add(repo, &commit, &checkout)?;
    note(&format!(
        "base leg: {git_ref} -> {} checked out at {}",
        &commit[..12.min(commit.len())],
        tree.path().display()
    ));

    let base_plan = Plan {
        dir: tree.path().to_owned(),
        ..base_plan.clone()
    };
    two_legs(&base_plan, head_plan, allow)
}

/// Compares two feature arms of one tree.
///
/// Both plans name the same directory and differ in `feature_args` and
/// `target_dir`. The second target directory is required, since cargo holds
/// one build per directory and two arms sharing one would rebuild each other
/// away between the legs.
///
/// Everything that makes a comparison a comparison is [`two_legs`], shared with
/// [`ab`] rather than restated: one calibration pinned across both, a priming
/// pass, and interleaving with the leg order flipped on replicate parity.
///
/// # Errors
///
/// When either arm fails to build, or when the two arms turn out not to be
/// comparable.
pub fn arms(base_plan: &Plan, head_plan: &Plan, allow: &[String]) -> Result<AbOutcome, String> {
    guard_out(base_plan, head_plan)?;
    if base_plan.target_dir == head_plan.target_dir {
        return Err(format!(
            "both arms would build into {}. Cargo keeps one build per target directory, \
             so the second arm would overwrite the first and both legs would measure it.",
            head_plan.target_dir.display()
        ));
    }
    // Guarded rather than only documented: the axis claims the two legs
    // differ in their features and in nothing else, and two different trees
    // under `axis: Arm` would carry that claim while varying the code as well,
    // with the feature guard stepped aside, so nothing downstream notices.
    if base_plan.dir != head_plan.dir {
        return Err(format!(
            "the two arms name different trees: {} and {}. An arm comparison varies the \
             features and holds the tree still; two trees is what `ab` measures.",
            base_plan.dir.display(),
            head_plan.dir.display()
        ));
    }
    worktree::install_interrupt_handler();
    two_legs(base_plan, head_plan, allow)
}

/// Refuses two legs that would be written to one directory.
fn guard_out(base_plan: &Plan, head_plan: &Plan) -> Result<(), String> {
    if base_plan.out == head_plan.out {
        return Err(format!(
            "both legs would be written to {}. Two runs' records in one directory pair \
             half of themselves away.",
            head_plan.out.display()
        ));
    }
    Ok(())
}

/// Builds two legs and measures them against each other.
///
/// What both [`ab`] and [`arms`] are, once each has decided what its two legs
/// *are*. The five properties that make the result a comparison (build both
/// before measuring, intersect, calibrate once and pin, prime, interleave) live
/// here and are therefore the same on both axes by construction.
fn two_legs(base_plan: &Plan, head_plan: &Plan, allow: &[String]) -> Result<AbOutcome, String> {
    // Both legs built before either is measured.
    let (head_describe, head_dirty) = worktree::describe(&head_plan.dir);
    if head_dirty {
        note("the working tree has uncommitted changes; the head leg records dirty: true");
    }
    // Described from the directory that was built rather than from the
    // reference it was made for, so a leg's provenance names the tree it
    // measured. It also makes an A/A run, the base and the head at the same
    // commit, produce two fingerprints that differ in nothing but `leg`, which
    // is what makes an empty significant table mean something.
    let (base_describe, base_dirty) = worktree::describe(&base_plan.dir);
    let base = prepare(base_plan, base_describe, base_dirty)?;
    let head = prepare(head_plan, head_describe, head_dirty)?;

    // The comparability guard runs here, before a single replicate, rather than
    // on the records afterwards. Both fingerprints exist the moment both legs
    // are built, and a feature-resolution difference that will refuse the
    // comparison should not first cost the whole measurement.
    guard_fingerprints(&base.fingerprint, &head.fingerprint, allow)?;

    let shared: Vec<CaseId> = base.cases.intersection(&head.cases).cloned().collect();
    if shared.is_empty() {
        return Err(format!(
            "the two legs share no case. base has {}, head has {}. \
             A filter that matches nothing, or a target added on one side only, \
             both look like this.",
            base.cases.len(),
            head.cases.len()
        ));
    }
    let only_base: Vec<&CaseId> = base.cases.difference(&head.cases).collect();
    let only_head: Vec<&CaseId> = head.cases.difference(&base.cases).collect();
    for (side, cases) in [("base", &only_base), ("head", &only_head)] {
        for case in cases {
            note(&format!(
                "'{case}' exists on the {side} leg only; it will be listed as not comparable"
            ));
        }
    }
    // Recorded now, because nothing downstream can recover them: a one-sided
    // case is never measured, so no record for it reaches either leg directory
    // and the comparator's own "present only in one leg" branch cannot fire.
    // An empty significant table has to mean "nothing moved", not "the case you
    // added was silently skipped".
    let one_sided: Vec<NotComparable> = [("base", only_base), ("head", only_head)]
        .into_iter()
        .flat_map(|(side, cases)| {
            cases.into_iter().map(move |case| NotComparable {
                what: case.to_string(),
                why: format!("present only in the {side} leg, so it was not measured on either"),
                cause: Cause::Other,
            })
        })
        .collect();

    // Calibrated on the base leg alone, pinned for both, and only over the
    // cases both legs share, since a base-only case is never measured and
    // calibrating it costs a subprocess that could fail the run.
    let iters = calibrate(&base, &shared, base_plan)?;

    let mut base_writer = Writer::create(&base.out)?;
    let mut head_writer = Writer::create(&head.out)?;

    // Announced before anything is measured, not after. Everything from here on
    // can fail: a case, a guard, a digest. Every one of those errors
    // gives advice that starts with re-reading these two directories, whose
    // names carry a timestamp the operator has no other way to learn.
    note(&format!(
        "legs are being written to\n  {}\n  {}",
        base.out.display(),
        head.out.display()
    ));

    note("priming pass (discarded)");
    for case in &shared {
        measure_one(
            &base,
            case,
            base_plan,
            iters[case],
            0,
            true,
            &mut base_writer,
        )?;
        measure_one(
            &head,
            case,
            head_plan,
            iters[case],
            0,
            true,
            &mut head_writer,
        )?;
    }

    // `head_plan.replicates` for both legs. `base_plan`'s is ignored rather
    // than reconciled: two counts would mean two runs.
    for replicate in 0..head_plan.replicates {
        note(&format!(
            "replicate {}/{}",
            replicate + 1,
            head_plan.replicates
        ));
        for case in &shared {
            check_interrupt()?;
            let legs: [(&Prepared, &Plan, &mut Writer); 2] = if base_first(replicate) {
                [
                    (&base, base_plan, &mut base_writer),
                    (&head, head_plan, &mut head_writer),
                ]
            } else {
                [
                    (&head, head_plan, &mut head_writer),
                    (&base, base_plan, &mut base_writer),
                ]
            };
            for (leg, plan, writer) in legs {
                measure_one(leg, case, plan, iters[case], replicate, false, writer)?;
            }
        }
    }

    base_writer.finish()?;
    head_writer.finish()?;

    let base_leg: Leg = load_leg(&base.out)?;
    let head_leg: Leg = load_leg(&head.out)?;
    let mut comparison = crate::compare::compare(base_leg, head_leg, allow)?;
    comparison.not_comparable.extend(one_sided);
    comparison.not_comparable.sort();
    comparison.not_comparable.dedup();

    Ok(AbOutcome {
        comparison,
        base_dir: base.out.clone(),
        head_dir: head.out.clone(),
    })
}

/// One iteration count per case, calibrated on this leg.
fn calibrate(
    leg: &Prepared,
    cases: &[CaseId],
    plan: &Plan,
) -> Result<BTreeMap<CaseId, u64>, String> {
    let mut out = BTreeMap::new();
    for case in cases {
        check_interrupt()?;
        let runner = runner_for(leg, case)?;
        let iters = runner.calibrate(&case.case, plan.seed, plan.target_ms)?;
        note(&format!("calibrated {case}: {iters} iteration(s)"));
        out.insert(case.clone(), iters);
    }
    Ok(out)
}

fn measure_one(
    leg: &Prepared,
    case: &CaseId,
    plan: &Plan,
    iters: u64,
    replicate: u32,
    priming: bool,
    writer: &mut Writer,
) -> Result<(), String> {
    let runner = runner_for(leg, case)?;
    let record = runner.measure(&Measurement {
        case: &case.case,
        seed: plan.seed,
        iters,
        replicate,
        priming,
        warmup_ms: plan.warmup_ms,
    })?;
    writer.write(&record)
}

fn runner_for<'a>(leg: &'a Prepared, case: &CaseId) -> Result<&'a Runner, String> {
    leg.runners.get(&case.target).ok_or_else(|| {
        format!(
            "the {} leg has no binary for target '{}'",
            leg.name, case.target
        )
    })
}

fn no_cases(plan: &Plan) -> String {
    match &plan.filter {
        Some(filter) => format!("no case id contains '{filter}'"),
        None => "no cases were declared by any wall-clock target".to_owned(),
    }
}

/// Whether the base leg runs first at this replicate.
///
/// Flipped on parity, so neither leg is systematically the one that ran
/// second. A machine that warms up over a pair would otherwise hand the same
/// leg the cold half every time.
const fn base_first(replicate: u32) -> bool {
    replicate.is_multiple_of(2)
}

fn check_interrupt() -> Result<(), String> {
    if worktree::interrupted() {
        // No mention of the worktree: a `run` and an `arms` reach this too and
        // neither made one, so naming it would describe cleanup that is not
        // happening. An `ab`'s worktree is removed by its own `Drop` regardless.
        return Err("interrupted".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::base_first;

    /// Neither leg may be systematically second. Asserted rather than read off
    /// the expression, because the property is about the sequence and the
    /// expression is one character from being wrong in a way nothing else
    /// notices: `replicate % 2 == 1` would swap which leg is cold, and
    /// `true` would put one leg second every time.
    #[test]
    fn the_leg_order_alternates_and_starts_with_the_base() {
        let order: Vec<bool> = (0..6).map(base_first).collect();
        assert_eq!(order, [true, false, true, false, true, false]);
        assert_eq!(
            order.iter().filter(|first| **first).count(),
            order.len() / 2,
            "one leg ran first more often than the other"
        );
    }
}

/// Appends records to one leg's JSONL file.
#[derive(Debug)]
struct Writer {
    path: PathBuf,
    file: std::fs::File,
}

impl Writer {
    fn create(dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        let path = dir.join("records.jsonl");
        let file = std::fs::File::create(&path)
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        Ok(Self { path, file })
    }

    fn write(&mut self, record: &Record) -> Result<(), String> {
        // One `write_all` of the line *and* its newline. `writeln!` issues two,
        // and an interrupt between them leaves a record without its
        // terminator, which `load_leg` reads as a corrupt leg and refuses
        // whole, taking every completed replicate with it.
        let mut line = record.to_line().map_err(|e| e.to_string())?;
        line.push('\n');
        self.file
            .write_all(line.as_bytes())
            .map_err(|e| format!("cannot write {}: {e}", self.path.display()))
    }

    /// Flushes, so a reader started immediately afterwards sees every record.
    fn finish(&mut self) -> Result<(), String> {
        self.file
            .flush()
            .map_err(|e| format!("cannot flush {}: {e}", self.path.display()))
    }
}
