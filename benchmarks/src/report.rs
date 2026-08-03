//! The versioned record every benchmark binary emits.
//!
//! One JSON object per line, appended to the file named by `RESULTS`. A
//! single shape spans every rig: the arm under test goes in [`Report::variant`]
//! (an open map, so `kafka_topology`'s `mode`, `pipeline_synthetic`'s `threads`
//! and `ch_native_format`'s `format` coexist without a union type), and each
//! measured quantity goes in [`Report::metrics`].
//!
//! [`Metric`] carries its own `unit` and `higher_is_better`. That is
//! deliberate: a consumer plotting these records cannot silently draw a
//! lower-is-better quantity as a taller bar, because the direction travels with
//! the number rather than living in the plotting code.
//!
//! Top-level struct fields serialize in declaration order; `variant` and
//! `metrics` are `BTreeMap`s and so serialize with their keys sorted.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Schema version of the emitted records. Bump on any breaking field change.
pub const SCHEMA_VERSION: u32 = 1;

/// Whether a record reports a measurement or a decision derived from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An observed quantity for one arm of a benchmark.
    Measurement,
    /// A conclusion drawn across arms (for example a go/no-go gate).
    Verdict,
}

/// What caused a run, and therefore whether its records may be published.
///
/// The site draws everything under `benchmarks/results/` without knowing which
/// machine was busy, so a contended number and a quiet one render identically.
/// Provenance already travels in [`RunMeta`] — `host`, `cpu`, `cores` — but
/// provenance describes *where*, not *whether*, and nothing here barred a record
/// from publication except nobody making a mistake.
///
/// This is the field that bars it, and [`Trigger::bars_publication`] is the
/// single authority: the lint, the note prefix and any future consumer ask it
/// rather than matching on the variants themselves, so a trigger added later
/// cannot be unpublishable in one place and publishable in another.
///
/// The variants name *what produced the record*, and each one that bars
/// publication says why in [`Trigger::publication_bar`]. There is deliberately
/// no variant without a producer: a value nothing sets is a value nobody can
/// trust, which is the failure this field exists to end rather than repeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// Run by hand, on hardware the person running it chose. The default off
    /// CI, and what every record committed before this field existed was.
    #[default]
    Manual,
    /// Produced by a job on a hosted CI runner — on a pull request or on a
    /// schedule. Never published.
    ///
    /// One variant rather than one per job, because the thing that bars these
    /// records is the hardware, not the cause: a hosted runner is contended and
    /// virtualised, so its wall-clock numbers describe the runner as much as
    /// the code. Which job produced a record is already answerable from
    /// [`RunMeta`] and the workflow that wrote it; splitting the trigger would
    /// add a distinction that changes nothing about publishability.
    Ci,
    /// Produced by a run whose purpose is one A/B comparison. Never published.
    ///
    /// Distinct from [`Trigger::Ci`] because the hardware is not the reason:
    /// these numbers can be perfectly good numbers, taken on a quiet machine.
    /// They are barred because a comparison is not a recording — it answers
    /// "did this change move it", and its arms are chosen to separate two
    /// builds rather than to describe the configuration anything ships at.
    /// Drawn beside a baseline on the site, an arm chosen to lose would read
    /// as a measurement of the framework.
    ///
    /// `make bench-ab` sets it, which is what stops its output being promoted
    /// out of `benchmarks/tuning/` into `benchmarks/results/` by hand.
    Dispatched,
}

impl Trigger {
    /// Every variant.
    ///
    /// Maintained by hand: stable Rust cannot enumerate an enum, and this
    /// deliberately does not pretend otherwise.
    /// `all_holds_distinct_variants_in_index_order` makes adding a variant a
    /// compile error and checks that this array and the match agree as far as
    /// it can — it cannot prove a new variant was added *here* as well as
    /// there. The backstop that holds that case is
    /// `scripts/check-results-publishable.sh` refusing a trigger it does not
    /// recognise, which runs on every pull request: a variant forgotten here
    /// fails the gate rather than slipping through it.
    pub const ALL: [Self; 3] = [Self::Manual, Self::Ci, Self::Dispatched];

    /// The marker a record produced under this trigger must carry, if any.
    ///
    /// The driver prefixes the record's `note` with this, so a record that must
    /// never be published says so in its own prose as well as in a typed field —
    /// a person reading one line of JSONL sees it without knowing the schema.
    #[must_use]
    pub fn publication_bar(self) -> Option<&'static str> {
        match self {
            Self::Ci => Some(
                "CI RUN: measured on a hosted runner, which is contended and virtualised — \
                 never published",
            ),
            Self::Dispatched => Some(
                "DISPATCHED RUN: one A/B comparison, never published — it answers the \
                 question it was asked, rather than recording a figure",
            ),
            Self::Manual => None,
        }
    }

    /// Whether a record produced under this trigger may reach
    /// `benchmarks/results/`.
    #[must_use]
    pub fn bars_publication(self) -> bool {
        self.publication_bar().is_some()
    }

    /// Reads `BENCH_TRIGGER`.
    ///
    /// Unset falls back to the environment: [`Trigger::Ci`] when `CI` is set,
    /// [`Trigger::Manual`] otherwise. That fallback is the load-bearing part.
    /// The only publishable trigger is `manual`, so a job that forgets to say
    /// what it is would otherwise mint publishable records from a hosted
    /// runner — the fail-open direction, and the one this field exists to
    /// close. Defaulting to the barred value where getting it wrong matters
    /// means forgetting is safe.
    ///
    /// # Panics
    ///
    /// On an unrecognised value, and on a value that is set but empty. A typo
    /// silently becoming `manual` would defeat the field entirely, and empty is
    /// what `BENCH_TRIGGER: ${{ inputs.trigger }}` evaluates to when the input
    /// is missing — the likeliest way a workflow gets this wrong, so it is a
    /// failure rather than a default.
    #[must_use]
    pub fn detect() -> Self {
        // Resolved once, at the first call. `preflight` makes that first call
        // happen before a rig does any work, so the panic below costs a
        // start-up rather than a finished sweep.
        static RESOLVED: OnceLock<Trigger> = OnceLock::new();
        *RESOLVED.get_or_init(|| {
            Self::resolve(
                std::env::var("BENCH_TRIGGER").ok().as_deref(),
                std::env::var_os("CI").is_some(),
            )
        })
    }

    /// The decision [`Trigger::detect`] makes, without reading the environment.
    ///
    /// Split out so it can be tested exhaustively: `cargo test` runs a
    /// binary's tests in one process and in parallel, so a test that set
    /// `BENCH_TRIGGER` would decide the answer for whatever else was running
    /// at the time.
    ///
    /// # Panics
    ///
    /// As [`Trigger::detect`].
    #[must_use]
    pub fn resolve(bench_trigger: Option<&str>, on_ci: bool) -> Self {
        match bench_trigger {
            None => {
                if on_ci {
                    Self::Ci
                } else {
                    Self::Manual
                }
            }
            Some("manual") => Self::Manual,
            Some("ci") => Self::Ci,
            Some("dispatched") => Self::Dispatched,
            Some("") => panic!(
                "BENCH_TRIGGER is set but empty; unset it to take the default, \
                 or name one of manual, ci, dispatched"
            ),
            Some(other) => panic!(
                "BENCH_TRIGGER={other:?} is not a trigger; expected one of \
                 manual, ci, dispatched"
            ),
        }
    }
}

/// One measured quantity, carrying its unit and its direction of goodness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The measured value, in `unit`.
    pub value: f64,
    /// Unit of `value`, e.g. `ns`, `records/s`, `bytes`, `ms`.
    pub unit: String,
    /// `true` when a larger `value` is a better result.
    pub higher_is_better: bool,
    /// 95% confidence interval `(low, high)` when the rig took repetitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci95: Option<(f64, f64)>,
    /// Sample count behind `value` (repetitions, not inner iterations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n: Option<u64>,
}

impl Metric {
    /// A metric where more is better — throughput, rows written.
    pub fn maximize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: true,
            ci95: None,
            n: None,
        }
    }

    /// A metric where less is better — latency, ns/record, bytes on the wire.
    pub fn minimize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: false,
            ci95: None,
            n: None,
        }
    }

    /// A byte throughput, recorded as `MB/s` in the SI sense — 10^6 bytes, not
    /// 2^20.
    ///
    /// Rigs had drifted apart on this: two sites divided by `1024 * 1024` and
    /// two by `1e6`, all four emitting the same `MB/s` string, so the same
    /// physical throughput read 4.86% apart with nothing in the record to tell
    /// which convention produced it. Take the rate in bytes/s and let this pick
    /// the divisor, so a new rig cannot reintroduce the split.
    pub fn bytes_per_s(bytes_per_s: f64) -> Self {
        Self::maximize(bytes_per_s / 1e6, "MB/s")
    }

    /// Attaches a 95% confidence interval.
    #[must_use]
    pub fn with_ci(mut self, low: f64, high: f64) -> Self {
        self.ci95 = Some((low, high));
        self
    }

    /// Attaches the repetition count behind the value.
    #[must_use]
    pub fn with_n(mut self, n: u64) -> Self {
        self.n = Some(n);
        self
    }
}

/// Provenance for a run: when, where, and from which commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    /// Unix epoch milliseconds at which the record was built.
    pub ts_ms: u64,
    /// Short git commit of the working tree, when discoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Label for the machine that produced the record: `BENCH_HOST`, or
    /// `local` when unset. See `detect_host`.
    pub host: String,
    /// CPU brand string, e.g. `Apple M5 Max`.
    pub cpu: String,
    /// Cores visible to the process.
    pub cores: usize,
    /// `os/arch`, e.g. `macos/aarch64`.
    pub os: String,
    /// Cargo profile the binary was built with.
    pub profile: String,
}

/// The static half of [`RunMeta`], resolved once per process.
struct StaticMeta {
    commit: Option<String>,
    host: String,
    cpu: String,
    cores: usize,
    os: String,
}

fn static_meta() -> &'static StaticMeta {
    static META: OnceLock<StaticMeta> = OnceLock::new();
    META.get_or_init(|| StaticMeta {
        commit: detect_commit(),
        host: detect_host(),
        cpu: detect_cpu(),
        cores: std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        os: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
    })
}

fn trimmed_stdout(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn detect_commit() -> Option<String> {
    if let Ok(c) = std::env::var("GIT_COMMIT")
        && !c.is_empty()
    {
        return Some(c);
    }
    trimmed_stdout("git", &["rev-parse", "--short=12", "HEAD"])
}

// Opt-in, and deliberately not `hostname`. Every record here is committed and
// published, and a machine's own name is a poor description of it as well as a
// personal one — `cpu` and `cores` already say what a reader needs to compare
// two runs. Set `BENCH_HOST` when a run's machine identity is itself part of
// the provenance, such as a named CI runner or a second box in an A/B.
fn detect_host() -> String {
    std::env::var("BENCH_HOST")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "local".to_owned())
}

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    if let Some(brand) = trimmed_stdout("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return brand;
    }
    #[cfg(target_os = "linux")]
    if let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") {
        for line in text.lines() {
            if let Some((key, value)) = line.split_once(':')
                && key.trim() == "model name"
            {
                return value.trim().to_owned();
            }
        }
    }
    "unknown".to_owned()
}

impl RunMeta {
    /// Stamps the current time onto the process-wide static provenance.
    pub fn detect() -> Self {
        let meta = static_meta();
        Self {
            ts_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            commit: meta.commit.clone(),
            host: meta.host.clone(),
            cpu: meta.cpu.clone(),
            cores: meta.cores,
            os: meta.os.clone(),
            profile: if cfg!(debug_assertions) {
                "debug".to_owned()
            } else {
                "release".to_owned()
            },
        }
    }
}

/// One emitted benchmark record.
///
/// ```no_run
/// use benchmarks::report::{Metric, Report};
///
/// Report::measurement("avro_pipeline")
///     .variant("deser", "fast_borrowed")
///     .variant("format", "native")
///     .metric("ns_per_event", Metric::minimize(54.0, "ns").with_n(15))
///     .metric("records_per_s", Metric::maximize(18_500_000.0, "records/s"))
///     .emit();
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// Schema version; always [`SCHEMA_VERSION`] on write.
    pub schema: u32,
    /// The rig that produced this record, e.g. `avro_pipeline`.
    pub bench: String,
    /// Measurement or verdict.
    pub kind: Kind,
    /// What caused the run, and so whether the record may be published.
    ///
    /// `#[serde(default)]` on the read side only: every record written since
    /// this field existed carries it, and the records that predate it were
    /// backfilled as `manual` — which is what they were. The default is
    /// tolerance for a hand-edited line, not a licence to omit it.
    #[serde(default)]
    pub trigger: Trigger,
    /// Provenance of the run.
    pub run: RunMeta,
    /// The arm under test, e.g. `{"deser": "fast_borrowed", "threads": 4}`.
    pub variant: BTreeMap<String, Value>,
    /// Measured quantities, keyed by metric name.
    pub metrics: BTreeMap<String, Metric>,
    /// Free-text caveat carried alongside the numbers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Report {
    fn new(bench: impl Into<String>, kind: Kind) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            bench: bench.into(),
            kind,
            trigger: Trigger::detect(),
            run: RunMeta::detect(),
            variant: BTreeMap::new(),
            metrics: BTreeMap::new(),
            note: None,
        }
    }

    /// An observed quantity for one arm.
    pub fn measurement(bench: impl Into<String>) -> Self {
        Self::new(bench, Kind::Measurement)
    }

    /// A conclusion drawn across arms.
    pub fn verdict(bench: impl Into<String>) -> Self {
        Self::new(bench, Kind::Verdict)
    }

    /// Adds one dimension of the arm under test.
    #[must_use]
    pub fn variant(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.variant.insert(key.into(), value.into());
        self
    }

    /// Adds one measured quantity.
    #[must_use]
    pub fn metric(mut self, key: impl Into<String>, metric: Metric) -> Self {
        self.metrics.insert(key.into(), metric);
        self
    }

    /// Attaches a caveat that travels with the numbers.
    #[must_use]
    pub fn note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Overrides what [`Trigger::detect`] read from the environment.
    ///
    /// Rarely wanted: `BENCH_TRIGGER` is how a caller says this, because it
    /// reaches every rig in a run without any of them knowing about it.
    #[must_use]
    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.trigger = trigger;
        self
    }

    /// The record as it is written out: a barred trigger's reason prefixed onto
    /// the `note`, so the prose and the typed field say the same thing.
    ///
    /// Prefixed on the way out rather than at construction because the note is
    /// usually attached after the trigger is, and a bar appended to the end of
    /// a long note is a bar nobody reads. A pure function rather than a step
    /// inside the writer so it can be tested without an environment variable or
    /// a temporary file.
    ///
    /// `pub(crate)` deliberately. It is not idempotent — what it returns still
    /// carries the trigger it keyed off, so applying it twice prefixes the bar
    /// twice — and no caller outside this crate has a reason to hold a record
    /// that has been stamped but not written.
    #[must_use]
    pub(crate) fn for_emission(&self) -> Self {
        let Some(bar) = self.trigger.publication_bar() else {
            return self.clone();
        };
        let mut barred = self.clone();
        barred.note = Some(match &self.note {
            Some(note) => format!("{bar} — {note}"),
            None => bar.to_owned(),
        });
        barred
    }

    /// Prints the record to stdout and appends it to `RESULTS` when set.
    pub fn emit(&self) {
        crate::report(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shell half of the publication bar. Read rather than duplicated: the
    /// script cannot call Rust, so the only way the two stay in step is for one
    /// of them to check the other.
    const LINT: &str = include_str!("../../scripts/check-results-publishable.sh");

    /// A record built without consulting the environment.
    ///
    /// `Report::measurement` resolves `BENCH_TRIGGER`, so a test that used it
    /// would inherit whatever the invoking shell had set — and panic on a value
    /// that has nothing to do with the test. Reproduced before this existed:
    /// `BENCH_TRIGGER=dispatch cargo test -p benchmarks --lib` failed three
    /// tests that never mention triggers.
    fn fixture(bench: &str, trigger: Trigger) -> Report {
        Report {
            schema: SCHEMA_VERSION,
            bench: bench.to_owned(),
            kind: Kind::Measurement,
            trigger,
            run: RunMeta::detect(),
            variant: BTreeMap::new(),
            metrics: BTreeMap::new(),
            note: None,
        }
    }

    /// The serde name, which is what appears in the JSON the script greps.
    fn serde_name(t: Trigger) -> String {
        serde_json::to_value(t)
            .expect("serialize trigger")
            .as_str()
            .expect("trigger serializes as a string")
            .to_owned()
    }

    /// Adding a variant makes `index` non-exhaustive, which is a compile error.
    ///
    /// What this can prove: every entry of [`Trigger::ALL`] is a distinct
    /// variant, and the indices they occupy are exactly `0..len` — so no
    /// variant appears twice and no slot is a duplicate.
    ///
    /// What it cannot prove, stated rather than implied: that a variant added
    /// to `index` was also added to `ALL`. Stable Rust cannot count an enum's
    /// variants, so `ALL`'s length is written by hand and a forgotten entry
    /// leaves this green. The check that actually catches that case is
    /// `scripts/check-results-publishable.sh` refusing a trigger it does not
    /// recognise, which runs on every pull request.
    #[test]
    fn all_holds_distinct_variants_in_index_order() {
        fn index(t: Trigger) -> usize {
            match t {
                Trigger::Manual => 0,
                Trigger::Ci => 1,
                Trigger::Dispatched => 2,
            }
        }
        let seen: Vec<usize> = Trigger::ALL.iter().copied().map(index).collect();
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            Trigger::ALL.len(),
            "ALL repeats a variant: {seen:?}"
        );
        assert_eq!(
            seen,
            (0..Trigger::ALL.len()).collect::<Vec<_>>(),
            "ALL is not in index order, so the match and the array disagree"
        );
    }

    /// The fail-safe that matters most: `manual` is the only publishable
    /// trigger, so a job that forgets to say what it is must not mint one.
    #[test]
    fn an_unset_trigger_on_ci_is_not_publishable() {
        assert_eq!(Trigger::resolve(None, true), Trigger::Ci);
        assert!(Trigger::resolve(None, true).bars_publication());
        assert_eq!(Trigger::resolve(None, false), Trigger::Manual);
        // An explicit value wins over the environment in both directions, so a
        // dispatched run on a CI-flagged box still records what it was.
        assert_eq!(
            Trigger::resolve(Some("dispatched"), true),
            Trigger::Dispatched
        );
        assert_eq!(Trigger::resolve(Some("manual"), true), Trigger::Manual);
        assert_eq!(Trigger::resolve(Some("ci"), false), Trigger::Ci);
    }

    /// `BENCH_TRIGGER: ${{ inputs.trigger }}` with a missing input evaluates to
    /// empty. Defaulting there would be indistinguishable from a job that
    /// deliberately said nothing, so it is a failure.
    #[test]
    #[should_panic(expected = "set but empty")]
    fn an_empty_trigger_is_a_failure_not_a_default() {
        let _ = Trigger::resolve(Some(""), true);
    }

    #[test]
    #[should_panic(expected = "is not a trigger")]
    fn a_misspelled_trigger_is_a_failure() {
        let _ = Trigger::resolve(Some("dispatchd"), false);
    }

    /// The unknown-trigger refusal the doc above leans on, exercised rather
    /// than asserted: a name outside the enum must not deserialize.
    #[test]
    fn a_trigger_outside_the_enum_does_not_deserialize() {
        assert!(serde_json::from_str::<Trigger>("\"speculative\"").is_err());
        assert!(serde_json::from_str::<Trigger>("null").is_err());
        assert!(serde_json::from_str::<Trigger>("false").is_err());
    }

    /// The contract `scripts/check-results-publishable.sh` documents in its
    /// header, executable. Checked in both directions on purpose: a barred
    /// trigger missing from the script is a record that could be committed,
    /// and a publishable one listed as barred would refuse the archive.
    #[test]
    fn the_lint_script_greps_exactly_the_barred_triggers() {
        // Exactly one line may assign each of these. Taking the first match
        // would otherwise read a decoy: bash uses the *last* assignment, so a
        // second `known_publishable=` further down would widen the gate while
        // this test kept reading the narrow one above it.
        let line = |prefix: &str| {
            let matches: Vec<&str> = LINT.lines().filter(|l| l.starts_with(prefix)).collect();
            assert_eq!(
                matches.len(),
                1,
                "the lint script has {} lines starting {prefix:?}; bash uses the last \
                 assignment, so more than one means this test is reading a decoy",
                matches.len()
            );
            matches[0].to_owned()
        };
        let barred_decl = line("BARRED=(");
        let publishable_decl = line("known_publishable=");

        // Both declarations quote their names, so every comparison here is on
        // the quoted form. Substring matching on a bare name would let `ci`
        // match inside an unrelated word and report agreement that is not
        // there.
        for t in Trigger::ALL {
            let name = serde_name(t);
            let quoted = format!("\"{name}\"");
            if t.bars_publication() {
                assert!(
                    barred_decl.contains(&quoted),
                    "{name} bars publication but is not in the script's BARRED: {barred_decl}"
                );
                assert!(
                    !publishable_decl.contains(&quoted),
                    "{name} bars publication but the script accepts it: {publishable_decl}"
                );
            } else {
                assert!(
                    publishable_decl.contains(&quoted),
                    "{name} is publishable but the script does not accept it: {publishable_decl}"
                );
                assert!(
                    !barred_decl.contains(&quoted),
                    "{name} is publishable but the script lists it as barred: {barred_decl}"
                );
            }
        }

        // The accept-list must be exactly the publishable set — not merely a
        // superset of it. Counting the quoted names closes the case the
        // per-variant loop above cannot see: a name in the script that no
        // longer exists in the enum.
        let accepted = publishable_decl.matches('"').count() / 2;
        let publishable = Trigger::ALL
            .iter()
            .filter(|t| !t.bars_publication())
            .count();
        assert_eq!(
            accepted, publishable,
            "the script accepts {accepted} trigger(s) but {publishable} are publishable: {publishable_decl}"
        );
    }

    #[test]
    fn a_bar_is_one_predicate_not_two() {
        for t in Trigger::ALL {
            assert_eq!(
                t.bars_publication(),
                t.publication_bar().is_some(),
                "{t:?} disagrees with itself"
            );
        }
        assert!(Trigger::Ci.bars_publication());
        assert!(Trigger::Dispatched.bars_publication());
        assert!(!Trigger::Manual.bars_publication());
    }

    /// A record that must never be published says so in its own prose, so one
    /// line of JSONL is readable without knowing the schema — and an existing
    /// note is kept rather than replaced.
    ///
    /// Exercises the write path's transform directly. Deleting the prefixing
    /// must fail a test, or the behaviour is described rather than held.
    #[test]
    fn a_barred_record_carries_the_bar_in_its_note() {
        let rep = fixture("s3_backfill", Trigger::Dispatched).note("5 reps, arms interleaved");
        let written = rep.for_emission();
        let note = written.note.as_deref().expect("a barred record has a note");
        assert!(note.starts_with("DISPATCHED RUN"), "{note}");
        assert!(note.ends_with("5 reps, arms interleaved"), "{note}");

        // A barred record with no note of its own still carries the bar.
        let bare = fixture("s3_backfill", Trigger::Ci).for_emission();
        assert!(
            bare.note
                .as_deref()
                .is_some_and(|n| n.starts_with("CI RUN")),
            "{:?}",
            bare.note
        );

        // A publishable record is passed through untouched.
        let ok = fixture("s3_backfill", Trigger::Manual).note("quiet machine");
        assert_eq!(ok.for_emission(), ok);
    }

    /// Records written before the field existed read as what they were.
    #[test]
    fn a_record_predating_the_field_reads_as_manual() {
        let line = r#"{"schema":1,"bench":"s3_backfill","kind":"measurement","run":{"ts_ms":0,"host":"local","cpu":"x","cores":1,"os":"linux/x86_64","profile":"release"},"variant":{},"metrics":{}}"#;
        let back: Report = serde_json::from_str(line).expect("deserialize");
        assert_eq!(back.trigger, Trigger::Manual);
        assert!(!back.trigger.bars_publication());
    }

    #[test]
    fn round_trips_through_json() {
        let rep = fixture("avro_pipeline", Trigger::Manual)
            .variant("deser", "fast_borrowed")
            .variant("threads", 4)
            .metric("ns_per_event", Metric::minimize(54.0, "ns").with_n(15))
            .metric(
                "records_per_s",
                Metric::maximize(18_500_000.0, "records/s").with_ci(18.0e6, 19.0e6),
            )
            .note("median of 15 reps");

        let line = serde_json::to_string(&rep).expect("serialize");
        assert!(!line.contains('\n'), "a record must be one JSON line");

        let back: Report = serde_json::from_str(&line).expect("deserialize");
        assert_eq!(back, rep);
        assert_eq!(back.schema, SCHEMA_VERSION);
        assert_eq!(back.kind, Kind::Measurement);
    }

    #[test]
    fn optional_fields_are_omitted_when_absent() {
        let mut rep = fixture("ch_native_format", Trigger::Manual);
        rep.kind = Kind::Verdict;
        let line = serde_json::to_string(&rep).expect("serialize");
        assert!(!line.contains("note"), "{line}");
        assert!(line.contains(r#""kind":"verdict""#), "{line}");
    }

    #[test]
    fn byte_rates_are_si_megabytes() {
        // The whole point of the helper: one divisor, so rigs cannot drift onto
        // 2^20 while still labelling the result "MB/s".
        let m = Metric::bytes_per_s(1_048_576.0);
        assert_eq!(m.unit, "MB/s");
        assert!(m.higher_is_better);
        assert!(
            (m.value - 1.048576).abs() < 1e-12,
            "1 MiB/s must record as 1.048576 MB/s, got {}",
            m.value
        );
    }

    #[test]
    fn direction_travels_with_the_number() {
        assert!(Metric::maximize(1.0, "records/s").higher_is_better);
        assert!(!Metric::minimize(1.0, "ns").higher_is_better);
    }
}
