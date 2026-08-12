//! Pairing two legs, and where a comparator hides its worst failure.
//!
//! The arithmetic is the easy half. What a comparator has to be built against
//! is the failure that produces a *plausible answer* rather than an error: a
//! well-formed table off a comparison that paired the wrong records. Five
//! hazards, each of which has a guard below:
//!
//! - **The builds must be the same build.** Two legs from different toolchains,
//!   targets, profiles or resolved feature sets are not a comparison, whatever
//!   the numbers look like. That is a hard error before any pairing, with an
//!   `--allow` escape hatch that is printed in the report header so a reader is
//!   never shown a bypassed guard silently.
//! - **A record with no partner is a finding, never a drop.** A case one leg
//!   added or removed goes into *Not comparable* and is named. An empty
//!   significant table has to mean "nothing moved", not "nothing paired".
//! - **Replicates pair by index, never by position.** Records are read from
//!   files in whatever order the filesystem yields, and a leg that lost one
//!   process would shift every later replicate against its partner and
//!   fabricate a difference.
//! - **The corpora must be the same corpora.** A digest mismatch on one case
//!   demotes that case; a mismatch on *every* case is systemic — a changed
//!   generator, a changed seed — and is a hard error rather than a report with
//!   nothing in it.
//! - **A metric can exist on one side only.** `peak_rss_bytes` is conditional
//!   by construction, and a newly added metric is on the head side alone. Its
//!   unit and direction come from the records rather than from a table here, so
//!   a throughput cannot be rendered as a regression because the renderer
//!   guessed.
//!
//! # Three more that produce a number rather than an error
//!
//! **The two directories are a base and a head, in that order.** The leg name is
//! not a guarded field — it differs by construction — so nothing else could tell
//! the arguments apart, and transposing them would render every difference with
//! its sign inverted. Two directories that are not a base and a head in that
//! order are refused.
//!
//! **A change from nothing has no relative size.** A metric that is zero on the
//! base leg and non-zero on the head — a path that begins allocating — goes to
//! *Not comparable* rather than into the findings table, because there is no
//! percentage to state. The entry names both values, which is the information a
//! reader actually wanted.
//!
//! **One replicate missing a metric removes that metric entirely**, rather than
//! shrinking its sample. A mean over nine pairs and a mean over ten are not the
//! same estimate, and silently mixing them would put the difference between two
//! sample sizes into a column labelled as a difference between two builds. The
//! removal is disclosed under *Not comparable*, never silent.
//!
//! A single case whose corpus digest differs is demoted like any other, and the
//! run succeeds with an empty table and the demotion beside it — which matters
//! because a `--filter` routinely puts exactly one case in scope, and one case
//! differing is not evidence about the corpora as a whole. One leg disagreeing
//! with *itself* across its own replicates is a different failure and is not
//! waivable: there is no single corpus left to compare against.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::fingerprint::{BASE_LEG, BuildFingerprint, HEAD_LEG, Host};
use crate::record::{CaseId, Record};
use crate::stats::{Analysis, analyse, seed_for};

/// The `--allow` value that waives the wholesale corpus-digest guard.
pub const ALLOW_DIGEST: &str = "digest";

/// Every value `--allow` recognises.
///
/// Validated rather than accepted, because an unrecognised one waives nothing
/// while the report header announces a waived guard — the worst combination
/// available.
#[must_use]
pub fn allowable() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = BuildFingerprint::local()
        .guarded_fields()
        .into_keys()
        .chain(
            Host {
                os: String::new(),
                cpu: String::new(),
                cores: 0,
                label: String::new(),
            }
            .guarded_fields()
            .into_keys(),
        )
        .collect();
    names.push(ALLOW_DIGEST);
    names.sort_unstable();
    names
}

/// One leg's records, and what they agree about.
#[derive(Debug, Clone)]
pub struct Leg {
    /// Where the records were read from.
    pub dir: PathBuf,
    /// The build every record in the leg came from.
    pub build: BuildFingerprint,
    /// The machine every record in the leg came from.
    pub host: Host,
    /// The records, priming passes included.
    pub records: Vec<Record>,
}

impl Leg {
    /// How many priming records the leg carries.
    #[must_use]
    pub fn priming(&self) -> usize {
        self.records.iter().filter(|r| r.priming).count()
    }
}

/// Reads every `.jsonl` file in a directory as one leg.
///
/// # Errors
///
/// When the directory cannot be read, holds no records, holds a record this
/// schema version does not recognise, or mixes records from more than one build
/// or machine — a leg assembled from two runs is not a leg.
pub fn load_leg(dir: &Path) -> Result<Leg, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    // Sorted so a leg reads the same way twice; the pairing does not depend on
    // it, but every error message and every "first record" does.
    paths.sort();

    let mut records = Vec::new();
    for path in &paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        for (n, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record: Record = serde_json::from_str(line)
                .map_err(|e| format!("{}:{}: {e}", path.display(), n + 1))?;
            if record.schema != crate::record::SCHEMA_VERSION {
                return Err(format!(
                    "{}:{}: schema {}, this driver reads {}",
                    path.display(),
                    n + 1,
                    record.schema,
                    crate::record::SCHEMA_VERSION
                ));
            }
            records.push(record);
        }
    }

    // A leg is one run. Two runs' records merged into one directory would
    // otherwise collapse silently — `group` keeps the last record per
    // (case, replicate) — and the report would state the halved replicate count
    // as fact.
    let mut seen: BTreeSet<(&CaseId, u32, bool)> = BTreeSet::new();
    for record in &records {
        if !seen.insert((&record.case, record.replicate, record.priming)) {
            return Err(format!(
                "{} holds more than one record for '{}' replicate {}{}. A leg is one run; \
                 two runs' records in one directory would pair half of themselves away.",
                dir.display(),
                record.case,
                record.replicate,
                if record.priming { " (priming)" } else { "" }
            ));
        }
    }
    drop(seen);

    let first = records
        .first()
        .ok_or_else(|| format!("{} holds no records", dir.display()))?;
    let build = first.build.clone();
    let host = first.host.clone();
    for record in &records {
        if record.build != build || record.host != host {
            return Err(format!(
                "{} mixes records from more than one build or machine — \
                 '{}' disagrees with the first record",
                dir.display(),
                record.case
            ));
        }
    }

    Ok(Leg {
        dir: dir.to_owned(),
        build,
        host,
        records,
    })
}

/// One metric of one case, compared.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The case.
    pub case: CaseId,
    /// The metric key.
    pub metric: String,
    /// The metric's unit, as both legs recorded it.
    pub unit: String,
    /// Whether more is better, as both legs recorded it.
    pub higher_is_better: bool,
    /// Whether the case declared itself noisy.
    pub erratic: bool,
    /// Why the case is noisy, if it said.
    pub erratic_reason: Option<String>,
    /// The decided comparison.
    pub analysis: Analysis,
}

/// The class of a [`NotComparable`], for a renderer that has to count them.
///
/// Typed rather than left to a substring match on the prose: the header line
/// summarising corpus digests is the most-read line of a report, and matching
/// it against a sentence this module also writes means rewording the sentence
/// silently makes the header lie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cause {
    /// The two legs measured different bytes, and the case was left out.
    DigestLeftOut,
    /// The two legs measured different bytes and were compared anyway, under
    /// [`ALLOW_DIGEST`].
    DigestCompared,
    /// Anything else: a one-sided case, mismatched iteration counts, an
    /// unpaired replicate, a metric that could not be analysed.
    Other,
}

impl Cause {
    /// The machine token, as `--format json` carries it.
    ///
    /// Stable for a report schema version — see
    /// [`crate::render::REPORT_SCHEMA_VERSION`]. Written out rather than
    /// derived from the variant names, so renaming a variant does not rewrite
    /// what a consumer matches on.
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            Self::DigestLeftOut => "digest_left_out",
            Self::DigestCompared => "digest_compared",
            Self::Other => "other",
        }
    }
}

/// Something that could not be compared, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NotComparable {
    /// What could not be compared — a case, or a case's metric.
    pub what: String,
    /// Why not, in a sentence a reader can act on.
    pub why: String,
    /// Which class of failure this is.
    pub cause: Cause,
}

/// The comparison of two legs.
#[derive(Debug, Clone)]
pub struct Comparison {
    /// The base leg.
    pub base: Leg,
    /// The head leg.
    pub head: Leg,
    /// Every comparable row, sorted by case then metric.
    pub rows: Vec<Row>,
    /// What was left out, and why.
    pub not_comparable: Vec<NotComparable>,
    /// Guards waived by `--allow`, echoed so a reader is told.
    pub allowed: Vec<String>,
}

impl Comparison {
    /// The rows that belong in the significant-changes table.
    ///
    /// An erratic case never reaches it, whatever its interval says. That is
    /// the point of the flag: the case is reported so a reader can look, and
    /// excluded so a known-noisy number cannot be the headline.
    pub fn significant(&self) -> impl Iterator<Item = &Row> {
        self.rows
            .iter()
            .filter(|row| !row.erratic && row.analysis.verdict.is_significant())
    }

    /// The rows from cases that declared themselves noisy.
    pub fn informational(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter().filter(|row| row.erratic)
    }
}

/// Compares two legs.
///
/// `allow` waives a named guard: any key of
/// [`BuildFingerprint::guarded_fields`] or [`Host::guarded_fields`], or
/// [`ALLOW_DIGEST`].
///
/// # Errors
///
/// When either leg cannot be loaded, when the builds or machines disagree on a
/// guarded field that was not waived, or when every shared case's corpus digest
/// differs — which is systemic rather than per-case.
pub fn compare(base: Leg, head: Leg, allow: &[String]) -> Result<Comparison, String> {
    let waived: BTreeSet<&str> = allow.iter().map(String::as_str).collect();
    guard_legs(&base, &head)?;
    guard(&base, &head, &waived)?;

    let base_cases = group(&base);
    let head_cases = group(&head);

    let mut rows = Vec::new();
    let mut not_comparable = Vec::new();

    let all_cases: BTreeSet<&CaseId> = base_cases
        .keys()
        .chain(head_cases.keys())
        .copied()
        .collect();
    let mut shared = 0usize;
    let mut digest_mismatches = 0usize;

    for case in all_cases {
        let (Some(base_reps), Some(head_reps)) = (base_cases.get(case), head_cases.get(case))
        else {
            let side = if base_cases.contains_key(case) {
                "the base leg"
            } else {
                "the head leg"
            };
            not_comparable.push(NotComparable {
                what: case.to_string(),
                why: format!("present only in {side}"),
                cause: Cause::Other,
            });
            continue;
        };
        shared += 1;

        // The corpus digest, before anything else: two legs that measured
        // different bytes have no comparable metric at all.
        let base_digests: BTreeSet<&str> = base_reps
            .values()
            .map(|r| r.corpus_digest.as_str())
            .collect();
        let head_digests: BTreeSet<&str> = head_reps
            .values()
            .map(|r| r.corpus_digest.as_str())
            .collect();
        if base_digests != head_digests || base_digests.len() != 1 {
            let within_one_leg = base_digests.len() != 1 || head_digests.len() != 1;
            // Only a cross-leg mismatch counts towards "the corpora changed".
            // A leg disagreeing with its own replicates is a different failure,
            // and the systemic error's advice — check the generators and the
            // seed — is not the advice for it.
            if !within_one_leg {
                digest_mismatches += 1;
            }
            let why = if within_one_leg {
                format!(
                    "corpus digest is not constant within a leg — base {:?}, head {:?}; \
                     the replicates of one leg did not all measure the same bytes",
                    base_digests.iter().copied().collect::<Vec<_>>(),
                    head_digests.iter().copied().collect::<Vec<_>>()
                )
            } else {
                format!(
                    "corpus digest differs — base {:?}, head {:?}; the two legs did not \
                     measure the same bytes",
                    base_digests.iter().copied().collect::<Vec<_>>(),
                    head_digests.iter().copied().collect::<Vec<_>>()
                )
            };
            // `--allow digest` is what turns the demotion into a disclosure. It
            // has to actually compare, or the flag whose message says "compare
            // anyway" would only convert a hard error into an empty table.
            if waived.contains(ALLOW_DIGEST) && !within_one_leg {
                not_comparable.push(NotComparable {
                    what: case.to_string(),
                    why: format!("{why} — compared anyway because --allow {ALLOW_DIGEST}"),
                    cause: Cause::DigestCompared,
                });
            } else {
                not_comparable.push(NotComparable {
                    what: case.to_string(),
                    why,
                    cause: Cause::DigestLeftOut,
                });
                continue;
            }
        }

        let base_iters: BTreeSet<u64> = base_reps.values().map(|r| r.iters).collect();
        let head_iters: BTreeSet<u64> = head_reps.values().map(|r| r.iters).collect();
        if base_iters != head_iters || base_iters.len() != 1 {
            not_comparable.push(NotComparable {
                what: case.to_string(),
                why: format!(
                    "iteration counts differ — base {base_iters:?}, head {head_iters:?}; \
                     the driver pins one count for both legs, so this is not one run"
                ),
                cause: Cause::Other,
            });
            continue;
        }

        // Pairing by replicate index. An index present on one side only is
        // disclosed rather than quietly dropped: a leg that lost a process is
        // exactly the situation in which the remaining numbers look fine.
        let paired: Vec<u32> = base_reps
            .keys()
            .filter(|k| head_reps.contains_key(*k))
            .copied()
            .collect();
        let unpaired: Vec<u32> = base_reps
            .keys()
            .chain(head_reps.keys())
            .filter(|k| !(base_reps.contains_key(*k) && head_reps.contains_key(*k)))
            .copied()
            .collect::<BTreeSet<u32>>()
            .into_iter()
            .collect();
        if !unpaired.is_empty() {
            not_comparable.push(NotComparable {
                what: case.to_string(),
                why: format!(
                    "replicate(s) {unpaired:?} exist on one leg only and were left out; \
                     the remaining {} pair(s) are compared",
                    paired.len()
                ),
                cause: Cause::Other,
            });
        }
        if paired.is_empty() {
            continue;
        }

        // Either leg declaring the case noisy is enough. A case newly marked
        // erratic on the head leg is exactly the one whose numbers should not
        // become a headline, and reading only the base leg would let it.
        let base_sample = base_reps[&paired[0]];
        let head_sample = head_reps[&paired[0]];
        let erratic = base_sample.erratic || head_sample.erratic;
        let erratic_reason = [head_sample, base_sample].into_iter().find_map(|record| {
            record
                .notes
                .iter()
                .find_map(|note| note.strip_prefix("erratic: ").map(str::to_owned))
        });

        let metrics: BTreeSet<&str> = base_reps
            .values()
            .chain(head_reps.values())
            .flat_map(|r| r.metrics.keys().map(String::as_str))
            .collect();

        for metric in metrics {
            let mut base_values = Vec::with_capacity(paired.len());
            let mut head_values = Vec::with_capacity(paired.len());
            // Seeded from *any* record carrying the metric, paired or not.
            // Taken from the paired ones alone, a metric that exists only on an
            // unpaired replicate leaves the shape unset and the whole metric
            // falls out of the report without a word — which is the one thing
            // this module promises never to do.
            let mut shape: Option<(String, bool)> = base_reps
                .values()
                .chain(head_reps.values())
                .find_map(|r| r.metrics.get(metric))
                .map(|m| (m.unit.clone(), m.higher_is_better));
            let mut missing = false;

            for replicate in &paired {
                for (side, values) in [
                    (base_reps[replicate], &mut base_values),
                    (head_reps[replicate], &mut head_values),
                ] {
                    match side.metrics.get(metric) {
                        Some(m) => {
                            // The unit and direction travel with the number, so
                            // a disagreement is a schema problem rather than
                            // something to average over.
                            if let Some((unit, higher)) = &shape {
                                if *unit != m.unit || *higher != m.higher_is_better {
                                    missing = true;
                                }
                            } else {
                                shape = Some((m.unit.clone(), m.higher_is_better));
                            }
                            values.push(m.value);
                        }
                        None => missing = true,
                    }
                }
            }

            let Some((unit, higher_is_better)) = shape else {
                continue;
            };
            if missing {
                not_comparable.push(NotComparable {
                    what: format!("{case} · {metric}"),
                    why: "absent from at least one paired replicate, or its unit and \
                          direction disagree between records"
                        .to_owned(),
                    cause: Cause::Other,
                });
                continue;
            }

            match analyse(
                metric,
                higher_is_better,
                &base_values,
                &head_values,
                seed_for(&case.to_string(), metric),
            ) {
                Ok(analysis) => rows.push(Row {
                    case: case.clone(),
                    metric: metric.to_owned(),
                    unit,
                    higher_is_better,
                    erratic,
                    erratic_reason: erratic_reason.clone(),
                    analysis,
                }),
                Err(why) => not_comparable.push(NotComparable {
                    what: format!("{case} · {metric}"),
                    why,
                    cause: Cause::Other,
                }),
            }
        }
    }

    // A digest mismatch on every shared case is not a per-case problem. It
    // means the corpora themselves changed — a generator, a seed, a framing —
    // and a report of nothing but demotions reads as "no cases" rather than as
    // the finding it is.
    // `shared > 1`, not `shared > 0`. With one case in scope — which `--filter`
    // routinely produces — "every case differs" is one case differing, and the
    // right answer for that is the demotion above rather than a claim about the
    // corpora as a whole.
    if shared > 1 && digest_mismatches == shared && !waived.contains(ALLOW_DIGEST) {
        return Err(format!(
            "every shared case ({shared}) has a different corpus digest on the two legs. \
             That is a change to the corpora rather than to the code — check the \
             generators and the seed. Pass `--allow {ALLOW_DIGEST}` to compare the cases \
             whose two legs each agreed with themselves; a leg that disagreed with its \
             own replicates has no corpus left to compare against and stays demoted."
        ));
    }

    if shared == 0 {
        return Err(format!(
            "the two legs share no case. {} holds {} case(s), {} holds {}. An empty table \
             would read as 'nothing moved' when what happened is that nothing paired.",
            base.dir.display(),
            base_cases.len(),
            head.dir.display(),
            head_cases.len()
        ));
    }

    rows.sort_by(|a, b| (&a.case, &a.metric).cmp(&(&b.case, &b.metric)));
    not_comparable.sort();
    not_comparable.dedup();

    Ok(Comparison {
        base,
        head,
        rows,
        not_comparable,
        allowed: allow.to_vec(),
    })
}

/// Non-priming records, keyed by case and then by replicate index.
fn group(leg: &Leg) -> BTreeMap<&CaseId, BTreeMap<u32, &Record>> {
    let mut out: BTreeMap<&CaseId, BTreeMap<u32, &Record>> = BTreeMap::new();
    for record in &leg.records {
        if record.priming {
            continue;
        }
        out.entry(&record.case)
            .or_default()
            .insert(record.replicate, record);
    }
    out
}

/// Refuses two directories that are not a base and a head, in that order.
///
/// The leg name is deliberately not a guarded field — it differs by
/// construction — so nothing else here distinguishes the two arguments. Without
/// this, `bench compare <head> <base>` renders a fully inverted report: a 30%
/// regression comes out as a 23% improvement, with tight intervals and a header
/// saying every guard passed. The only tell in the output is a `git describe`
/// string.
fn guard_legs(base: &Leg, head: &Leg) -> Result<(), String> {
    if base.dir == head.dir {
        return Err(format!(
            "both arguments are {} — a leg compared with itself reports no change, \
             which is true and useless",
            base.dir.display()
        ));
    }
    if base.build.leg == HEAD_LEG && head.build.leg == BASE_LEG {
        return Err(format!(
            "the arguments are the wrong way round: {} holds the head leg and {} the \
             base. Every difference would be reported with its sign inverted.",
            base.dir.display(),
            head.dir.display()
        ));
    }
    if base.build.leg == head.build.leg {
        return Err(format!(
            "both directories hold the '{}' leg, so this is not a comparison of two \
             builds",
            base.build.leg
        ));
    }
    Ok(())
}

/// The build and machine guard, over two legs' records.
fn guard(base: &Leg, head: &Leg, waived: &BTreeSet<&str>) -> Result<(), String> {
    let mut fields = base.build.guarded_fields();
    fields.extend(base.host.guarded_fields());
    let mut theirs = head.build.guarded_fields();
    theirs.extend(head.host.guarded_fields());
    guard_fields(&fields, &theirs, waived)
}

/// The same guard, over two fingerprints alone.
///
/// Exposed so an A/B run can refuse *before* it measures. The machine is not
/// compared here: one process is building both legs, so it is the same machine
/// by construction, and the record-level guard catches the case where it was
/// not.
///
/// # Errors
///
/// When a guarded field differs and was not waived.
pub fn guard_fingerprints(
    base: &BuildFingerprint,
    head: &BuildFingerprint,
    allow: &[String],
) -> Result<(), String> {
    let waived: BTreeSet<&str> = allow.iter().map(String::as_str).collect();
    guard_fields(&base.guarded_fields(), &head.guarded_fields(), &waived)
}

fn guard_fields(
    fields: &BTreeMap<&'static str, String>,
    theirs: &BTreeMap<&'static str, String>,
    waived: &BTreeSet<&str>,
) -> Result<(), String> {
    let mut differences = Vec::new();
    for (field, base_value) in fields {
        let head_value = theirs.get(field).cloned().unwrap_or_default();
        if *base_value != head_value && !waived.contains(field) {
            differences.push(format!(
                "{field}: base '{base_value}' vs head '{head_value}'"
            ));
        }
    }

    if differences.is_empty() {
        return Ok(());
    }
    Err(format!(
        "the two legs are not the same build:\n  {}\n\
         A difference here changes what was compiled, so any table drawn from it would \
         describe the toolchain rather than the change. Pass `--allow <field>` per field \
         if you know better; the report says which guards were waived.",
        differences.join("\n  ")
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{ALLOW_DIGEST, Cause, Leg, compare, load_leg};
    use crate::fingerprint::{BuildFingerprint, Host};
    use crate::record::{CaseId, Metric, Record, SCHEMA_VERSION, WALL_NS_PER_ITER};
    use crate::stats::Verdict;

    fn host() -> Host {
        Host {
            os: "macos/aarch64".to_owned(),
            cpu: "Apple M5 Max".to_owned(),
            cores: 16,
            label: "local".to_owned(),
        }
    }

    fn fingerprint(leg: &str) -> BuildFingerprint {
        BuildFingerprint {
            protocol: 1,
            leg: leg.to_owned(),
            rustc: Some("rustc 1.94.0".to_owned()),
            host_triple: Some("aarch64-apple-darwin".to_owned()),
            profile: Some("bench".to_owned()),
            codegen: Some("cafecafecafecafe".to_owned()),
            features: vec!["spate-bench/default".to_owned()],
            feature_args: Vec::new(),
            git_describe: Some(leg.to_owned()),
            dirty: false,
        }
    }

    struct Builder {
        leg: String,
        records: Vec<Record>,
    }

    impl Builder {
        fn new(leg: &str) -> Self {
            Self {
                leg: leg.to_owned(),
                records: Vec::new(),
            }
        }

        fn record(mut self, case: &str, replicate: u32, wall: f64) -> Self {
            self.records.push(Record {
                schema: SCHEMA_VERSION,
                case: CaseId {
                    krate: "spate-bench".to_owned(),
                    target: "selftest_wall".to_owned(),
                    case: case.to_owned(),
                },
                replicate,
                priming: false,
                iters: 100,
                erratic: false,
                seed: 1,
                corpus_digest: "aaaaaaaaaaaaaaaa".to_owned(),
                metrics: BTreeMap::from([(
                    WALL_NS_PER_ITER.to_owned(),
                    Metric::minimize(wall, "ns"),
                )]),
                notes: Vec::new(),
                build: fingerprint(&self.leg),
                host: host(),
                ts_ms: 0,
            });
            self
        }

        fn series(self, case: &str, values: &[f64]) -> Self {
            values.iter().enumerate().fold(self, |acc, (k, v)| {
                acc.record(case, u32::try_from(k).expect("small"), *v)
            })
        }

        fn build(self) -> Leg {
            Leg {
                dir: std::path::PathBuf::from(format!("/legs/{}", self.leg)),
                build: fingerprint(&self.leg),
                host: host(),
                records: self.records,
            }
        }
    }

    fn ten(centre: f64) -> Vec<f64> {
        (0..10)
            .map(|k| centre * (1.0 + ((k % 5) as f64 - 2.0) * 0.001))
            .collect()
    }

    /// The property the acceptance run asserts end to end: comparing a leg with
    /// itself flags nothing.
    #[test]
    fn self_comparison_flags_nothing() {
        let values = ten(1000.0);
        let base = Builder::new("base").series("a", &values).build();
        let head = Builder::new("head").series("a", &values).build();
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.significant().count(), 0);
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].analysis.verdict, Verdict::NoChange);
        assert!(out.not_comparable.is_empty(), "{:?}", out.not_comparable);
        assert_eq!(out.rows[0].analysis.replicates, 10);
    }

    #[test]
    fn a_real_shift_is_flagged() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let head = Builder::new("head")
            .series(
                "a",
                &ten(1000.0).iter().map(|v| v * 1.3).collect::<Vec<_>>(),
            )
            .build();
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.significant().count(), 1);
        assert_eq!(out.rows[0].analysis.verdict, Verdict::Regressed);
    }

    #[test]
    fn a_case_on_one_leg_only_is_named_rather_than_dropped() {
        let base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("gone", &ten(500.0))
            .build();
        let head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("new", &ten(700.0))
            .build();
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.not_comparable.len(), 2);
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.what.ends_with("gone") && n.why.contains("base"))
        );
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.what.ends_with("new") && n.why.contains("head"))
        );
    }

    /// Replicate 7 is missing from the head leg. Pairing by position would
    /// silently compare base 8 against head 7 and every later pair likewise.
    #[test]
    fn a_missing_replicate_shifts_nothing_and_is_disclosed() {
        let values = ten(1000.0);
        let base = Builder::new("base").series("a", &values).build();
        let mut head_builder = Builder::new("head");
        for (k, v) in values.iter().enumerate() {
            if k == 7 {
                continue;
            }
            head_builder = head_builder.record("a", u32::try_from(k).expect("small"), *v);
        }
        let out = compare(base, head_builder.build(), &[]).expect("compares");

        assert_eq!(out.rows[0].analysis.replicates, 9);
        assert_eq!(out.rows[0].analysis.verdict, Verdict::NoChange);
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.why.contains("[7]") && n.why.contains("one leg only")),
            "{:?}",
            out.not_comparable
        );
    }

    #[test]
    fn a_per_case_digest_mismatch_demotes_only_that_case() {
        let mut base_builder = Builder::new("base");
        base_builder = base_builder.series("a", &ten(1000.0));
        base_builder = base_builder.series("b", &ten(2000.0));
        let mut base = base_builder.build();
        for record in &mut base.records {
            if record.case.case == "b" {
                record.corpus_digest = "bbbbbbbbbbbbbbbb".to_owned();
            }
        }

        let head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].case.case, "a");
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.what.ends_with("b") && n.why.contains("corpus digest"))
        );
    }

    /// Every case differing is systemic, and a report of nothing but demotions
    /// reads as "no cases" rather than as the finding it is.
    #[test]
    fn a_wholesale_digest_mismatch_is_a_hard_error_and_waivable() {
        // Two cases, because one case differing is not evidence about the
        // corpora as a whole — see the test above.
        let mut base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut base.records {
            record.corpus_digest = "cccccccccccccccc".to_owned();
        }
        let head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(err.contains("corpus digest"), "{err}");

        // Waived means compared, not silenced: the rows exist and the reason
        // is still disclosed.
        let waived = compare(base, head, &[ALLOW_DIGEST.to_owned()]).expect("waived");
        assert_eq!(waived.rows.len(), 2);
        assert_eq!(waived.allowed, [ALLOW_DIGEST]);
        assert!(
            waived
                .not_comparable
                .iter()
                .any(|n| n.why.contains("compared anyway")),
            "{:?}",
            waived.not_comparable
        );
    }

    #[test]
    fn a_different_toolchain_is_a_hard_error_and_waivable() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.build.rustc = Some("rustc 1.95.0".to_owned());
        for record in &mut head.records {
            record.build.rustc = Some("rustc 1.95.0".to_owned());
        }

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(err.contains("rustc"), "{err}");
        assert!(compare(base, head, &["rustc".to_owned()]).is_ok());
    }

    #[test]
    fn a_different_machine_is_a_hard_error() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.host.cores = 8;
        for record in &mut head.records {
            record.host.cores = 8;
        }
        let err = compare(base, head, &[]).expect_err("refused");
        assert!(err.contains("host_cores"), "{err}");
    }

    #[test]
    fn iteration_counts_that_differ_demote_the_case() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        for record in &mut head.records {
            record.iters = 200;
        }
        let out = compare(base, head, &[]).expect("compares");
        assert!(out.rows.is_empty());
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.why.contains("iteration counts differ"))
        );
    }

    /// `peak_rss_bytes` is conditional by construction, so a metric present on
    /// some replicates only has to be named rather than averaged over whatever
    /// was there.
    #[test]
    fn a_metric_missing_from_one_replicate_is_named() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.records[3].metrics.clear();
        let out = compare(base, head, &[]).expect("compares");
        assert!(out.rows.is_empty());
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.what.contains(WALL_NS_PER_ITER) && n.why.contains("absent"))
        );
    }

    /// A priming record pairs with itself and would otherwise contribute a
    /// replicate that no other leg has.
    #[test]
    fn priming_records_are_excluded() {
        let values = ten(1000.0);
        let mut base = Builder::new("base").series("a", &values).build();
        let mut head = Builder::new("head").series("a", &values).build();
        for leg in [&mut base, &mut head] {
            let mut priming = leg.records[0].clone();
            priming.priming = true;
            priming.replicate = 99;
            priming.metrics.insert(
                WALL_NS_PER_ITER.to_owned(),
                Metric::minimize(500_000.0, "ns"),
            );
            leg.records.push(priming);
        }
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows[0].analysis.replicates, 10);
        assert_eq!(out.rows[0].analysis.verdict, Verdict::NoChange);
    }

    #[test]
    fn an_erratic_case_is_informational_however_large_the_shift() {
        let mut base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head")
            .series(
                "a",
                &ten(1000.0).iter().map(|v| v * 2.0).collect::<Vec<_>>(),
            )
            .build();
        for leg in [&mut base, &mut head] {
            for record in &mut leg.records {
                record.erratic = true;
                record
                    .notes
                    .push("erratic: the allocator decides".to_owned());
            }
        }
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.significant().count(), 0);
        assert_eq!(out.informational().count(), 1);
        assert_eq!(out.rows[0].analysis.verdict, Verdict::Regressed);
        assert_eq!(
            out.rows[0].erratic_reason.as_deref(),
            Some("the allocator decides")
        );
    }

    /// A case the head leg newly marked noisy must not be eligible for the
    /// significant table, even though the base leg's records say nothing.
    #[test]
    fn erratic_on_either_leg_is_enough() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head")
            .series(
                "a",
                &ten(1000.0).iter().map(|v| v * 2.0).collect::<Vec<_>>(),
            )
            .build();
        for record in &mut head.records {
            record.erratic = true;
            record.notes.push("erratic: newly noisy".to_owned());
        }
        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.significant().count(), 0);
        assert_eq!(out.informational().count(), 1);
        assert_eq!(out.rows[0].erratic_reason.as_deref(), Some("newly noisy"));
    }

    /// One leg disagreeing with itself is a different failure from the two legs
    /// disagreeing, and `--allow digest` does not waive it: there is no single
    /// corpus to compare against.
    #[test]
    fn a_leg_whose_own_replicates_disagree_is_named_as_such() {
        let mut base = Builder::new("base").series("a", &ten(1000.0)).build();
        base.records[4].corpus_digest = "dddddddddddddddd".to_owned();
        let head = Builder::new("head").series("a", &ten(1000.0)).build();

        for allow in [Vec::new(), vec![ALLOW_DIGEST.to_owned()]] {
            let out = compare(base.clone(), head.clone(), &allow);
            let out = match out {
                Ok(out) => out,
                Err(err) => {
                    assert!(err.contains("corpus digest"), "{err}");
                    continue;
                }
            };
            assert!(out.rows.is_empty());
            assert!(
                out.not_comparable
                    .iter()
                    .any(|n| n.why.contains("not constant within a leg")),
                "{:?}",
                out.not_comparable
            );
        }
    }

    /// `--filter` narrowing a run to one case must not turn that case's own
    /// corpus change into a claim about the corpora as a whole.
    #[test]
    fn one_shared_case_differing_is_a_demotion_rather_than_a_systemic_error() {
        let mut base = Builder::new("base").series("a", &ten(1000.0)).build();
        for record in &mut base.records {
            record.corpus_digest = "eeeeeeeeeeeeeeee".to_owned();
        }
        let head = Builder::new("head").series("a", &ten(1000.0)).build();

        let out = compare(base, head, &[]).expect("demotes rather than refusing");
        assert!(out.rows.is_empty());
        assert!(
            out.not_comparable
                .iter()
                .any(|n| n.why.contains("corpus digest differs"))
        );
    }

    /// `load_leg`'s refusals, through `load_leg`. Asserting the *setup* — that
    /// two records differ — proves nothing about the function that is supposed
    /// to notice.
    #[test]
    fn load_leg_refuses_what_it_says_it_refuses() {
        let dir = tempfile::tempdir().expect("scratch directory");
        let write = |records: &[Record]| {
            let mut text = String::new();
            for record in records {
                text.push_str(&record.to_line().expect("serialises"));
                text.push('\n');
            }
            std::fs::write(dir.path().join("records.jsonl"), text).expect("writes");
        };

        // An empty directory holds no records, which is not a leg.
        assert!(
            load_leg(dir.path())
                .expect_err("empty")
                .contains("holds no records")
        );

        // A well-formed leg loads, and files that are not `.jsonl` are ignored.
        let leg = Builder::new("base").series("a", &ten(1000.0)).build();
        write(&leg.records);
        std::fs::write(dir.path().join("notes.txt"), "ignored").expect("writes");
        let loaded = load_leg(dir.path()).expect("loads");
        assert_eq!(loaded.records.len(), 10);
        assert_eq!(loaded.build, leg.build);

        // Two builds in one directory means two runs.
        let mut mixed = leg.records.clone();
        mixed[2].build.rustc = Some("rustc 1.95.0".to_owned());
        write(&mixed);
        assert!(
            load_leg(dir.path())
                .expect_err("mixed")
                .contains("more than one build")
        );

        // A duplicate (case, replicate) would collapse silently on grouping.
        let mut duplicated = leg.records.clone();
        duplicated.push(leg.records[3].clone());
        write(&duplicated);
        assert!(
            load_leg(dir.path())
                .expect_err("duplicated")
                .contains("more than one record")
        );

        // A schema this driver does not read is refused by number.
        let mut future = leg.records.clone();
        future[0].schema = SCHEMA_VERSION + 1;
        write(&future);
        assert!(
            load_leg(dir.path())
                .expect_err("future schema")
                .contains("schema")
        );

        // And a line that is not a record names the file and the line.
        std::fs::write(dir.path().join("records.jsonl"), "{not json\n").expect("writes");
        assert!(
            load_leg(dir.path())
                .expect_err("malformed")
                .contains("records.jsonl:1")
        );
    }

    /// An empty table has to mean "nothing moved". Two legs with no case in
    /// common produce one that means "nothing paired", which is the opposite
    /// claim from the same output.
    #[test]
    fn two_legs_with_no_case_in_common_are_refused() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let head = Builder::new("head").series("b", &ten(1000.0)).build();
        let err = compare(base, head, &[]).expect_err("nothing shared");
        assert!(err.contains("share no case"), "{err}");
    }

    /// Two runners of one instance type agree on os, cpu and core count
    /// exactly. The label is the only field left that can tell them apart.
    #[test]
    fn two_differently_labelled_machines_do_not_compare() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.host.label = "runner-2".to_owned();
        for record in &mut head.records {
            record.host.label = "runner-2".to_owned();
        }
        let err = compare(base.clone(), head.clone(), &[]).expect_err("two machines");
        assert!(err.contains("host_label"), "{err}");
        assert!(compare(base, head, &["host_label".to_owned()]).is_ok());
    }

    /// Transposing the two arguments inverts every sign. Nothing else in the
    /// guard set can tell the two directories apart, because the leg name is
    /// deliberately not a guarded field.
    #[test]
    fn a_transposed_or_self_comparison_is_refused() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let head = Builder::new("head").series("a", &ten(1300.0)).build();

        assert!(compare(base.clone(), head.clone(), &[]).is_ok());

        let err = compare(head.clone(), base.clone(), &[]).expect_err("transposed");
        assert!(err.contains("wrong way round"), "{err}");

        let err = compare(base.clone(), base.clone(), &[]).expect_err("same leg");
        assert!(err.contains("both arguments are"), "{err}");

        let mut elsewhere = base.clone();
        elsewhere.dir = std::path::PathBuf::from("/legs/other");
        let err = compare(base, elsewhere, &[]).expect_err("two base legs");
        assert!(err.contains("both directories hold"), "{err}");
    }

    /// A consumer switches on these, so two classes sharing a token would make
    /// one of them unreachable.
    #[test]
    fn every_cause_has_a_distinct_token() {
        let tokens: Vec<&str> = [Cause::DigestLeftOut, Cause::DigestCompared, Cause::Other]
            .into_iter()
            .map(Cause::token)
            .collect();

        let mut unique = tokens.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), tokens.len(), "{tokens:?}");

        for token in tokens {
            assert!(
                token.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{token}"
            );
        }
    }
}
