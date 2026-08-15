//! Pairing two legs, and where a comparator hides its worst failure.
//!
//! The arithmetic is the easy half. What a comparator has to be built against
//! is the failure that produces a *plausible answer* rather than an error: a
//! well-formed table off a comparison that paired the wrong records. Six
//! hazards, each of which has a guard below:
//!
//! - **The builds must be the same build.** Two legs from different toolchains,
//!   targets, profiles or resolved feature sets are not a comparison, whatever
//!   the numbers look like. That is a hard error before any pairing, with an
//!   `--allow` escape hatch that is printed in the report header, along with
//!   what the two legs disagreed about, so a reader is never shown a bypassed
//!   guard silently.
//! - **A record with no partner is a finding, never a drop.** A case one leg
//!   added or removed goes into *Not comparable* and is named. An empty
//!   significant table has to mean "nothing moved", not "nothing paired".
//! - **Replicates pair by index, never by position.** Records are read from
//!   files in whatever order the filesystem yields, and a leg that lost one
//!   process would shift every later replicate against its partner and
//!   fabricate a difference.
//! - **The corpora must be the same corpora.** A digest mismatch on one case
//!   demotes that case; a mismatch on *every* case is systemic, from a changed
//!   generator or a changed seed, and is a hard error rather than a report with
//!   nothing in it.
//! - **The compiled subject must be what the axis says.** A case may declare
//!   what a feature arm swapped in. Two builds of one commit have to agree about
//!   it; two feature arms have to *disagree*, since agreement there means the
//!   feature never reached the case and the two columns are one measurement
//!   twice. This is the guard the `features` fingerprint cannot be: `features`
//!   records what was passed to cargo, and a feature that became a default
//!   agrees there while compiling something else. Demoted per case, systemic
//!   when every judged case answers the same way, and asserting nothing for a
//!   case that declares nothing.
//! - **A metric can exist on one side only.** `peak_rss_bytes` is conditional
//!   by construction, and a newly added metric is on the head side alone. Its
//!   unit and direction come from the records rather than from a table here, so
//!   a throughput cannot be rendered as a regression because the renderer
//!   guessed.
//!
//! # Three more that produce a number rather than an error
//!
//! **The two directories are a base and a head, in that order.** The leg name is
//! not a guarded field, since it differs by construction, so nothing else could
//! tell the arguments apart, and transposing them would render every difference with
//! its sign inverted. Two directories that are not a base and a head in that
//! order are refused.
//!
//! **A change from nothing has no relative size.** A metric that is zero on the
//! base leg and non-zero on the head, a path that begins allocating, goes to
//! *Not comparable* rather than into the findings table, because there is no
//! percentage to state. The entry names both values.
//!
//! **One replicate missing a metric removes that metric entirely**, rather than
//! shrinking its sample. A mean over nine pairs and a mean over ten are not the
//! same estimate, and silently mixing them would put the difference between two
//! sample sizes into a column labeled as a difference between two builds. The
//! removal is disclosed under *Not comparable*, never silent.
//!
//! A single case whose corpus digest differs is demoted like any other, and the
//! run succeeds with an empty table and the demotion beside it, which matters
//! because a `--filter` routinely puts one case in scope, and one case
//! differing is not evidence about the corpora as a whole. One leg disagreeing
//! with *itself* across its own replicates is a different failure and is not
//! waivable: there is no single corpus left to compare against.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::fingerprint::{Axis, BASE_LEG, BuildFingerprint, FIELD_FEATURES, HEAD_LEG, Host};
use crate::record::{CaseId, Record};
use crate::stats::{Analysis, analyse, seed_for};

/// The `--allow` value that waives the wholesale corpus-digest guard.
pub const ALLOW_DIGEST: &str = "digest";

/// The `--allow` value that waives the declared-build guard.
pub const ALLOW_BUILD: &str = "build";

/// Every value `--allow` recognizes.
///
/// Validated rather than accepted, because an unrecognized one waives nothing
/// while the report header announces a waived guard, the worst combination
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
    names.push(ALLOW_BUILD);
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
/// schema version does not recognize, or mixes records from more than one build
/// or machine. A leg assembled from two runs is not a leg.
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
    // otherwise collapse silently, because `group` keeps the last record per
    // (case, replicate), and the report would state the halved replicate count
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
    /// Whether the verdict came from the metric this one is derived from.
    ///
    /// False for a measured metric, and false for a derived one whose measured
    /// metric this comparison does not carry.
    pub inherited: bool,
    /// The decided comparison.
    pub analysis: Analysis,
}

impl Row {
    /// Whether this row belongs in the significant-changes table.
    ///
    /// Two exclusions on top of the verdict. An erratic case never reaches the
    /// table, whatever its interval says: it is reported so a reader can look,
    /// and excluded so a known-noisy number cannot be the headline. A row that
    /// took its verdict from the metric it is derived from does not reach it
    /// either, since that metric is in the table already and listing both counts
    /// one timing event twice.
    ///
    /// A derived row whose measured metric is absent keeps the verdict it was
    /// analyzed with, and is a finding on its own account. Nothing else in the
    /// report carries that difference.
    ///
    /// Asked here rather than at each caller, so the table a reader sees and the
    /// `significant` field a script reads cannot answer it differently.
    #[must_use]
    pub fn is_finding(&self) -> bool {
        !self.erratic && !self.inherited && self.analysis.verdict.is_significant()
    }
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
    /// The two legs declared different compiled subjects, or one leg
    /// disagreed with its own replicates, and the case was left out.
    BuildLeftOut,
    /// The same, compared anyway under [`ALLOW_BUILD`]. Never reached by a leg
    /// that disagreed with itself, which is not waivable.
    BuildCompared,
    /// The two *arms* declared the same compiled subject, and the case was left
    /// out.
    ///
    /// The opposite condition to [`Cause::BuildLeftOut`], and a separate token
    /// because it is the opposite: a renderer that had to tell them apart by
    /// reading the prose is the failure this type prevents.
    BuildSameLeftOut,
    /// The two arms declared the same compiled subject and were compared anyway,
    /// under [`ALLOW_BUILD`].
    BuildSameCompared,
    /// Anything else: a one-sided case, mismatched iteration counts, an
    /// unpaired replicate, a metric that could not be analyzed.
    Other,
}

impl Cause {
    /// The machine token, as `--format json` carries it.
    ///
    /// Stable for a report schema version; see
    /// [`crate::render::REPORT_SCHEMA_VERSION`]. Written out rather than
    /// derived from the variant names, so renaming a variant does not rewrite
    /// what a consumer matches on.
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            Self::DigestLeftOut => "digest_left_out",
            Self::DigestCompared => "digest_compared",
            Self::BuildLeftOut => "build_left_out",
            Self::BuildCompared => "build_compared",
            Self::BuildSameLeftOut => "build_same_left_out",
            Self::BuildSameCompared => "build_same_compared",
            Self::Other => "other",
        }
    }
}

/// A guarded field the two legs disagree about.
///
/// Produced once and read twice: the guard refuses over the ones that were not
/// waived, and the report header names the ones that were. Both render it
/// through [`Display`](std::fmt::Display), so one difference cannot be described
/// two ways.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Divergence {
    /// The field's name, as `--allow` spells it.
    pub field: &'static str,
    /// The base leg's value, empty when that leg recorded none.
    pub base: String,
    /// The head leg's value, empty when that leg recorded none.
    pub head: String,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: base '{}' vs head '{}'",
            self.field, self.base, self.head
        )
    }
}

/// Something that could not be compared, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NotComparable {
    /// What could not be compared: a case, or a case's metric.
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
    /// [`Row::is_finding`] states what qualifies.
    pub fn significant(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter().filter(|row| row.is_finding())
    }

    /// The rows from cases that declared themselves noisy.
    pub fn informational(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter().filter(|row| row.erratic)
    }

    /// The rows the rule reached no conclusion about.
    ///
    /// There is a difference to print and nothing concluded from it. Disjoint
    /// from [`Comparison::significant`]; [`crate::stats::Verdict::NoVerdict`]
    /// says when a row lands here.
    pub fn unjudged(&self) -> impl Iterator<Item = &Row> {
        self.rows
            .iter()
            .filter(|row| !row.analysis.verdict.is_judged())
    }

    /// The largest replicate count the unjudged rows ask for, and whether any of
    /// them asks for more than a count can give.
    ///
    /// The largest, since that is the count that judges every row naming one.
    /// A row past [`crate::stats::MAX_SUGGESTED_REPLICATES`] names nothing and
    /// is reported by the flag instead, so a run holding both says both.
    ///
    /// Erratic rows are left out. The rule never flags one whatever its
    /// interval, so a count that would make it judgeable buys nothing, and
    /// `selftest_wall` declares one on purpose.
    #[must_use]
    pub fn replicates_needed(&self) -> (Option<usize>, bool) {
        let rows = || self.unjudged().filter(|row| !row.erratic);
        (
            rows()
                .filter_map(|row| row.analysis.replicates_needed)
                .max(),
            rows().any(|row| row.analysis.replicates_needed.is_none()),
        )
    }

    /// Every guarded field the two legs disagree about.
    ///
    /// On the commit axis, empty for any comparison [`compare`] produced without
    /// `--allow`: a difference that was not waived is a refusal rather than a
    /// report. On the arm axis the resolved feature set differs by construction
    /// and appears here with nothing waived, which is how a report names both
    /// arms. Not filtered by what was waived, because this states what is true
    /// of the two legs rather than what the run was told to permit.
    #[must_use]
    pub fn divergences(&self) -> Vec<Divergence> {
        divergences(&guarded(&self.base), &guarded(&self.head))
    }
}

/// Compares two legs.
///
/// `allow` waives a named guard: any key of
/// [`BuildFingerprint::guarded_fields`] or [`Host::guarded_fields`], or
/// [`ALLOW_DIGEST`] or [`ALLOW_BUILD`].
///
/// # Errors
///
/// When either leg cannot be loaded, when the builds or machines disagree on a
/// guarded field that was not waived, when every shared case's corpus digest
/// differs, or when every judged case answers the declared-build question the
/// same wrong way for its axis. Each of those is systemic rather than per-case.
pub fn compare(base: Leg, head: Leg, allow: &[String]) -> Result<Comparison, String> {
    let waived: BTreeSet<&str> = allow.iter().map(String::as_str).collect();
    guard_legs(&base, &head)?;
    guard(&base, &head, &waived)?;
    // Read after `guard_legs`, which has refused two legs that disagree about
    // it, so one value describes both.
    let axis = base.build.axis;

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
    // How many cases reached the declared-build question at all, and how each
    // answered it. Counted separately from `shared` because a case demoted on
    // its corpus never reaches the block. Measured against `shared`, the
    // wholesale escalations below could not fire once any case had been dropped
    // earlier, which is the run most in need of a systemic answer.
    let mut build_judged = 0usize;
    let mut build_mismatches = 0usize;
    let mut build_same = 0usize;
    let mut build_one_sided = 0usize;

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
            // and the systemic error's advice, to check the generators and
            // the seed, is not the advice for it.
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
            // has to compare, or the flag whose message says "compare
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

        // The compiled subject, which the corpus digest deliberately says
        // nothing about. Same shape as the block above, opposite question: that
        // one asks whether the two legs read the same bytes, this one whether
        // they ran the same code through them. The answer it wants inverts
        // with the axis.
        let base_builds: BTreeSet<Option<&str>> = base_reps
            .values()
            .map(|r| r.build_digest.as_deref())
            .collect();
        let head_builds: BTreeSet<Option<&str>> = head_reps
            .values()
            .map(|r| r.build_digest.as_deref())
            .collect();
        let within_one_leg = base_builds.len() != 1 || head_builds.len() != 1;
        let agree = base_builds == head_builds;
        // A case that declared nothing states neither claim, on either axis.
        // That is every target without a feature axis, so it has to stay silent
        // rather than demote a whole run on an absent field.
        let declared_anything = base_builds
            .iter()
            .chain(head_builds.iter())
            .any(Option::is_some);
        // One leg declaring nothing while the other does is neither agreement
        // nor disagreement about a subject: the case gained or lost its
        // declaration between the two builds, which is what comparing against a
        // commit older than the field looks like. It is still not comparable,
        // because an undeclared side cannot be checked, but it is a third
        // thing, and saying "the two legs compiled different code" about it
        // would state something no run established.
        let one_sided = declared_anything
            && (base_builds == BTreeSet::from([None]) || head_builds == BTreeSet::from([None]));
        let wrong = match axis {
            _ if within_one_leg || one_sided => true,
            Axis::Commit => !agree,
            Axis::Arm => agree && declared_anything,
        };
        // Counted only where the case could have been judged, so the wholesale
        // escalations below measure against what they saw.
        if !within_one_leg {
            build_judged += 1;
        }
        if wrong {
            if !within_one_leg {
                if one_sided {
                    build_one_sided += 1;
                } else if agree {
                    build_same += 1;
                } else {
                    build_mismatches += 1;
                }
            }
            let why = if within_one_leg {
                format!(
                    "the declared build is not constant within a leg — base {}, head {}; \
                     the replicates of one leg did not all measure the same compiled code",
                    declared(&base_builds),
                    declared(&head_builds)
                )
            } else if one_sided {
                format!(
                    "only one leg declares a build — base {}, head {}; the other says \
                     nothing about what it compiled, so there is nothing to check it \
                     against. A leg built before the case declared one reads like this.",
                    declared(&base_builds),
                    declared(&head_builds)
                )
            } else if agree {
                format!(
                    "both arms declare the same build — {}; this case declares nothing that \
                     separates them, so as far as it can tell the two columns are one \
                     measurement twice",
                    declared(&base_builds)
                )
            } else {
                format!(
                    "the declared build differs — base {}, head {}; the two legs compiled \
                     different code for this case",
                    declared(&base_builds),
                    declared(&head_builds)
                )
            };
            // The cause distinguishes the two opposite conditions, so the report
            // header can count them without reading the prose above.
            let (compared, left_out) = if agree && !within_one_leg && !one_sided {
                (Cause::BuildSameCompared, Cause::BuildSameLeftOut)
            } else {
                (Cause::BuildCompared, Cause::BuildLeftOut)
            };
            if waived.contains(ALLOW_BUILD) && !within_one_leg {
                not_comparable.push(NotComparable {
                    what: case.to_string(),
                    why: format!("{why} — compared anyway because --allow {ALLOW_BUILD}"),
                    cause: compared,
                });
            } else {
                not_comparable.push(NotComparable {
                    what: case.to_string(),
                    why,
                    cause: left_out,
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
        // disclosed rather than silently dropped: a leg that lost a process is
        // the situation in which the remaining numbers look fine.
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

        // Where this case's rows start, so the derived ones can be reconciled
        // against the measured one once every metric has been analyzed. The
        // metric names are visited in sorted order, and `wall_ns_per_iter` sorts
        // after both throughputs.
        let case_start = rows.len();

        for metric in metrics {
            let mut base_values = Vec::with_capacity(paired.len());
            let mut head_values = Vec::with_capacity(paired.len());
            // Seeded from *any* record carrying the metric, paired or not.
            // Taken from the paired ones alone, a metric that exists only on an
            // unpaired replicate leaves the shape unset and the whole metric
            // falls out of the report without a word, which is the one thing
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
                    inherited: false,
                    analysis,
                }),
                Err(why) => not_comparable.push(NotComparable {
                    what: format!("{case} · {metric}"),
                    why,
                    cause: Cause::Other,
                }),
            }
        }

        inherit_derived_verdicts(&mut rows[case_start..]);
    }

    // A digest mismatch on every shared case is not a per-case problem. It
    // means the corpora themselves changed, through a generator, a seed or a
    // framing, and a report of nothing but demotions reads as "no cases"
    // rather than as the finding it is.
    // `shared > 1`, not `shared > 0`. With one case in scope, which `--filter`
    // routinely produces, "every case differs" is one case differing, and the
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

    // The same escalation, for the same reason: every shared case compiling
    // differently is not twenty per-case findings, it is one: the two legs are
    // two different builds of the subject, and the report should say so once
    // rather than demote the whole run a case at a time.
    if build_judged > 1 && !waived.contains(ALLOW_BUILD) {
        // Three wholesale answers, one per way every judged case can be wrong
        // together. Each is a statement about the run rather than a demotion
        // repeated once per case, and each has its own thing to say next.
        if build_one_sided == build_judged {
            return Err(format!(
                "every judged case ({build_judged}) is declared on one leg only. One of \
                 these legs was built before its cases declared what they compile, so \
                 nothing can be checked against it — which is what comparing against a \
                 commit older than that change looks like. Pass \
                 `--allow {ALLOW_BUILD}` to compare them without the check."
            ));
        }
        if build_mismatches == build_judged {
            return Err(format!(
                "every judged case ({build_judged}) declares a different build on the two \
                 legs. Two builds of one commit are meant to compile the same subject, so \
                 this is a feature arm rather than a change — measure it with `bench arms`, \
                 which pins one iteration count across both arms and interleaves them. Pass \
                 `--allow {ALLOW_BUILD}` to compare them anyway."
            ));
        }
        if build_same == build_judged {
            return Err(format!(
                "every judged case ({build_judged}) declares the same build on both arms. \
                 As far as these cases can tell, the two arms are one build measured twice. \
                 Either the feature never reached them — check it is spelled for the \
                 package that owns it, as in `--head-features <pkg>/<feature>` — or it \
                 changed something they do not declare, in which case \
                 `--allow {ALLOW_BUILD}` says so deliberately."
            ));
        }
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

/// Gives each derived row the verdict of the metric it comes from.
///
/// Takes one case's rows, every metric already analyzed.
/// [`crate::stats::derived_from`] states the relationship and why one verdict
/// has to cover both. The derived row keeps its own difference, interval and
/// floor; the verdict and the replicate count that goes with it are taken.
///
/// [`crate::stats::Verdict`] is a direction of goodness rather than a sign, so
/// the verdict transfers unchanged: a wall time that rose and a throughput that
/// fell are one event, and both are `Regressed`.
///
/// A case whose measured metric is absent leaves its derived rows as they were
/// analyzed, and not marked inherited. It can fail to pair, carry records
/// disagreeing on a unit, or be one [`crate::stats::analyse`] refused.
fn inherit_derived_verdicts(rows: &mut [Row]) {
    for index in 0..rows.len() {
        let Some(measured) = crate::stats::derived_from(&rows[index].metric) else {
            continue;
        };
        let Some((verdict, needed)) = rows
            .iter()
            .find(|row| row.metric == measured)
            .map(|row| (row.analysis.verdict, row.analysis.replicates_needed))
        else {
            continue;
        };
        // Both together. The count says what applying the rule to this row
        // would take, so a row taking another row's verdict takes its count
        // too. Kept apart, the two render side by side as
        // "improved (needs 8 replicates)".
        rows[index].analysis.verdict = verdict;
        rows[index].analysis.replicates_needed = needed;
        rows[index].inherited = true;
    }
}

/// How a set of declared build digests reads in a refusal.
///
/// A case that declared nothing renders as `none` rather than as an empty
/// string: "base none, head [\"a1b2…\"]" says which side declared, where two
/// bracketed lists one of which is empty says it much less clearly.
fn declared(digests: &BTreeSet<Option<&str>>) -> String {
    let rendered: Vec<&str> = digests
        .iter()
        .map(|digest| digest.unwrap_or("none"))
        .collect();
    format!("{rendered:?}")
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
/// The leg name is not a guarded field, since it differs by construction, so
/// nothing else here distinguishes the two arguments. Without
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
    // Not waivable, and not a comparability nicety: the axis decides which way
    // the declared-build guard points, so two legs disagreeing about it would
    // have one of them judged by the other's rule.
    if base.build.axis != head.build.axis {
        return Err(format!(
            "the legs disagree about what they vary: {} says {}, {} says {}. \
             One of them was produced by a different kind of run.",
            base.dir.display(),
            base.build.axis,
            head.dir.display(),
            head.build.axis
        ));
    }
    Ok(())
}

/// Every guarded field of one leg, build and machine alike, in one map.
fn guarded(leg: &Leg) -> BTreeMap<&'static str, String> {
    let mut fields = leg.build.guarded_fields();
    fields.extend(leg.host.guarded_fields());
    fields
}

/// Every guarded field the two maps disagree about, in field-name order.
fn divergences(
    fields: &BTreeMap<&'static str, String>,
    theirs: &BTreeMap<&'static str, String>,
) -> Vec<Divergence> {
    fields
        .iter()
        .filter_map(|(field, base)| {
            // The maps hold the same keys, so the fallback is unreachable. It
            // is here so a field one side stops recording reads as a difference
            // rather than panicking.
            let head = theirs.get(field).cloned().unwrap_or_default();
            (*base != head).then(|| Divergence {
                field,
                base: base.clone(),
                head,
            })
        })
        .collect()
}

/// The build and machine guard, over two legs' records.
fn guard(base: &Leg, head: &Leg, waived: &BTreeSet<&str>) -> Result<(), String> {
    guard_fields(&guarded(base), &guarded(head), waived, base.build.axis)
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
/// When a guarded field differs and was not waived. On the arm axis the
/// resolved feature set is the subject rather than a guard, so it differs
/// without erroring and without a waiver.
pub fn guard_fingerprints(
    base: &BuildFingerprint,
    head: &BuildFingerprint,
    allow: &[String],
) -> Result<(), String> {
    let waived: BTreeSet<&str> = allow.iter().map(String::as_str).collect();
    guard_fields(
        &base.guarded_fields(),
        &head.guarded_fields(),
        &waived,
        base.axis,
    )
}

fn guard_fields(
    fields: &BTreeMap<&'static str, String>,
    theirs: &BTreeMap<&'static str, String>,
    waived: &BTreeSet<&str>,
    axis: Axis,
) -> Result<(), String> {
    let differences: Vec<String> = divergences(fields, theirs)
        .iter()
        // On the arm axis the feature set is the subject, not a divergence to
        // excuse. Stepping aside here rather than making the caller pass
        // `--allow features` keeps the waiver list meaning "a guard was
        // bypassed": an arm run bypasses nothing, and the header still names
        // both arms' resolved sets because `divergences` is computed
        // separately.
        .filter(|d| !(axis == Axis::Arm && d.field == FIELD_FEATURES))
        .filter(|d| !waived.contains(d.field))
        .map(ToString::to_string)
        .collect();

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

    use super::{
        ALLOW_BUILD, ALLOW_DIGEST, Cause, FIELD_FEATURES, Leg, allowable, compare, load_leg,
    };
    use crate::fingerprint::{Axis, BuildFingerprint, Host};
    use crate::record::{
        BYTES_PER_S, CaseId, Metric, RECORDS_PER_S, Record, SCHEMA_VERSION, WALL_NS_PER_ITER,
    };
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
            axis: crate::fingerprint::Axis::Commit,
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

    /// Restamps a leg as one arm of a feature comparison.
    ///
    /// Both the leg's own fingerprint and every record's, because `load_leg`
    /// derives the first from the second and the tests build them separately.
    fn as_arm(mut leg: Leg, declared: Option<&str>, features: &str) -> Leg {
        leg.build.axis = Axis::Arm;
        leg.build.features = vec![features.to_owned()];
        for record in &mut leg.records {
            record.build.axis = Axis::Arm;
            record.build.features = vec![features.to_owned()];
            record.build_digest = declared.map(str::to_owned);
        }
        leg
    }

    struct Builder {
        leg: String,
        records: Vec<Record>,
        /// Items and bytes an iteration covers, when the case declares them.
        throughput: Option<(f64, f64)>,
    }

    impl Builder {
        fn new(leg: &str) -> Self {
            Self {
                leg: leg.to_owned(),
                records: Vec::new(),
                throughput: None,
            }
        }

        /// Declares an item and byte count, so every record carries the two
        /// derived metrics beside its wall time.
        fn throughput(mut self, items: f64, bytes: f64) -> Self {
            self.throughput = Some((items, bytes));
            self
        }

        fn record(mut self, case: &str, replicate: u32, wall: f64) -> Self {
            let mut metrics =
                BTreeMap::from([(WALL_NS_PER_ITER.to_owned(), Metric::minimize(wall, "ns"))]);
            // The arithmetic `case.rs` does: a count per iteration over the
            // seconds an iteration took. The iteration count cancels, leaving a
            // constant over the wall time, which is what makes these two
            // metrics reciprocals of it rather than measurements.
            if let Some((items, bytes)) = self.throughput {
                metrics.insert(
                    RECORDS_PER_S.to_owned(),
                    Metric::maximize(items * 1e9 / wall, "records/s"),
                );
                metrics.insert(
                    BYTES_PER_S.to_owned(),
                    Metric::maximize(bytes * 1e9 / wall, "bytes/s"),
                );
            }
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
                build_digest: None,
                metrics,
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
        assert!(out.divergences().is_empty(), "{:?}", out.divergences());
    }

    /// Every row of one case, by metric name.
    fn verdicts(out: &super::Comparison) -> BTreeMap<&str, Verdict> {
        out.rows
            .iter()
            .map(|row| (row.metric.as_str(), row.analysis.verdict))
            .collect()
    }

    /// One timing event, one verdict, at every difference either floor can
    /// separate.
    ///
    /// A wall difference `d` reaches a throughput as `-d / (1 + d)`, so a floor
    /// applied to each metric on its own disagrees across
    /// `[-5.000%, -4.762%]` and `[+5.000%, +5.263%]`. The sweep walks both
    /// bands in 0.04% steps and takes coarse points either side of them.
    #[test]
    fn a_derived_metric_never_contradicts_the_metric_it_comes_from() {
        let mut deltas: Vec<f64> = vec![-0.08, -0.06, -0.03, 0.0, 0.03, 0.06, 0.08];
        for band in [-0.054, 0.046] {
            deltas.extend((0..=25).map(|k| band + f64::from(k) * 0.0004));
        }

        let values = ten(1000.0);
        for delta in deltas {
            let shifted: Vec<f64> = values.iter().map(|v| v * (1.0 + delta)).collect();
            let base = Builder::new("base")
                .throughput(1024.0, 65536.0)
                .series("a", &values)
                .build();
            let head = Builder::new("head")
                .throughput(1024.0, 65536.0)
                .series("a", &shifted)
                .build();
            let out = compare(base, head, &[]).expect("compares");
            let by_metric = verdicts(&out);
            let wall = by_metric[WALL_NS_PER_ITER];
            assert_eq!(by_metric[RECORDS_PER_S], wall, "at delta {delta}");
            assert_eq!(by_metric[BYTES_PER_S], wall, "at delta {delta}");

            // And the derived rows are not counted again beside it.
            assert!(
                out.significant().all(|row| row.metric == WALL_NS_PER_ITER),
                "at delta {delta}: {:?}",
                out.significant().map(|r| &r.metric).collect::<Vec<_>>()
            );
        }
    }

    /// A derived row whose own interval is wider than the floor, beside the
    /// measured row whose is not.
    ///
    /// Judged apiece, the throughput declines a verdict and the wall time
    /// returns one, and the row renders as "improved (needs 8 replicates)",
    /// stating both that the rule applied and that it did not. The count
    /// describes a decision, so it travels with the verdict it belongs to.
    #[test]
    fn a_derived_row_takes_the_replicate_count_with_the_verdict() {
        // Mean zero, sample standard deviation one, over six pairs.
        let unit = [
            -1.336_306, -0.801_784, -0.267_261, 0.267_261, 0.801_784, 1.336_306,
        ];
        let base = vec![1000.0; unit.len()];
        let head: Vec<f64> = unit
            .iter()
            .map(|z| 1000.0 * (1.0 - 0.066 + 0.055 * z))
            .collect();

        let out = compare(
            Builder::new("base")
                .throughput(1024.0, 65536.0)
                .series("a", &base)
                .build(),
            Builder::new("head")
                .throughput(1024.0, 65536.0)
                .series("a", &head)
                .build(),
            &[],
        )
        .expect("compares");

        let half = |metric: &str| {
            let row = out
                .rows
                .iter()
                .find(|row| row.metric == metric)
                .expect(metric);
            (
                (row.analysis.ci_high - row.analysis.ci_low) / 2.0,
                row.analysis.floor,
            )
        };
        // The corner this test is about, asserted rather than assumed: the
        // reciprocal stretches the far tail, so the rate's interval reaches the
        // floor where the wall time's does not.
        let (wall_half, floor) = half(WALL_NS_PER_ITER);
        let (rate_half, _) = half(RECORDS_PER_S);
        assert!(wall_half < floor, "{wall_half} against {floor}");
        assert!(rate_half >= floor, "{rate_half} against {floor}");

        let by_metric = verdicts(&out);
        assert_eq!(by_metric[WALL_NS_PER_ITER], Verdict::Improved);
        assert_eq!(by_metric[RECORDS_PER_S], Verdict::Improved);
        assert_eq!(by_metric[BYTES_PER_S], Verdict::Improved);

        // The invariant the two fields carry between them, over every row: a
        // count is present exactly when the rule was not applied.
        for row in &out.rows {
            assert_eq!(
                row.analysis.replicates_needed.is_some(),
                row.analysis.verdict == Verdict::NoVerdict,
                "{} · {}: {:?} with {:?}",
                row.case,
                row.metric,
                row.analysis.verdict,
                row.analysis.replicates_needed
            );
        }
    }

    /// The mirror image: the measured row declines, so the rows derived from it
    /// decline with it and ask for the same count.
    #[test]
    fn a_derived_row_declines_when_the_measured_row_does() {
        let unit = [-1.264_911, -0.632_456, 0.0, 0.632_456, 1.264_911];
        let base = vec![1000.0; unit.len()];
        let head: Vec<f64> = unit
            .iter()
            .map(|z| 1000.0 * (1.0 + 0.0902 + 0.0834 * z))
            .collect();

        let out = compare(
            Builder::new("base")
                .throughput(1024.0, 65536.0)
                .series("a", &base)
                .build(),
            Builder::new("head")
                .throughput(1024.0, 65536.0)
                .series("a", &head)
                .build(),
            &[],
        )
        .expect("compares");

        let wall = out
            .rows
            .iter()
            .find(|row| row.metric == WALL_NS_PER_ITER)
            .expect("wall");
        assert_eq!(wall.analysis.verdict, Verdict::NoVerdict);
        let needed = wall.analysis.replicates_needed.expect("a count");

        for row in &out.rows {
            assert_eq!(row.analysis.verdict, Verdict::NoVerdict, "{}", row.metric);
            assert_eq!(
                row.analysis.replicates_needed,
                Some(needed),
                "{}",
                row.metric
            );
        }
        assert_eq!(out.significant().count(), 0);
    }

    /// A throughput whose wall time this comparison does not carry is the only
    /// row describing that difference, so it is a finding on its own account.
    ///
    /// Excluding every derived row unconditionally leaves the report saying
    /// "None. No metric cleared both halves of the rule below." beside a
    /// throughput that halved.
    #[test]
    fn a_derived_row_with_no_measured_row_is_a_finding() {
        let values = ten(1000.0);
        let halved: Vec<f64> = values.iter().map(|v| v * 2.0).collect();
        let mut base = Builder::new("base")
            .throughput(1024.0, 65536.0)
            .series("a", &values)
            .build();
        let mut head = Builder::new("head")
            .throughput(1024.0, 65536.0)
            .series("a", &halved)
            .build();
        // As a leg that lost the metric looks: `bench compare` accepts two
        // directories it did not produce, and pairs whatever they hold.
        for leg in [&mut base, &mut head] {
            for record in &mut leg.records {
                record.metrics.remove(WALL_NS_PER_ITER);
            }
        }

        let out = compare(base, head, &[]).expect("compares");
        let rate = out
            .rows
            .iter()
            .find(|row| row.metric == RECORDS_PER_S)
            .expect("rate");
        assert!(!rate.inherited);
        assert_eq!(rate.analysis.verdict, Verdict::Regressed);
        assert!(rate.is_finding());
        assert_eq!(out.significant().count(), 2);
    }

    /// The measurement from the report, in nanoseconds per iteration. Its mean
    /// paired wall difference is -4.95%, inside the band where the floor
    /// suppresses the wall rows and clears the two throughput rows. All three
    /// are the wall time's verdict, and the wall time did not clear the floor.
    #[test]
    fn the_reported_five_replicates_are_one_verdict() {
        let base = [429_500.0, 401_700.0, 414_500.0, 400_300.0, 410_800.0];
        let head = [396_600.0, 387_900.0, 398_600.0, 384_100.0, 387_100.0];
        let out = compare(
            Builder::new("base")
                .throughput(2048.0, 97_000.0)
                .series("a", &base)
                .build(),
            Builder::new("head")
                .throughput(2048.0, 97_000.0)
                .series("a", &head)
                .build(),
            &[],
        )
        .expect("compares");

        let by_metric = verdicts(&out);
        assert_eq!(by_metric[WALL_NS_PER_ITER], Verdict::NoChange);
        assert_eq!(by_metric[RECORDS_PER_S], Verdict::NoChange);
        assert_eq!(by_metric[BYTES_PER_S], Verdict::NoChange);
        assert_eq!(out.significant().count(), 0);

        // The rows still state their own numbers: the contradiction was in the
        // verdict, and a throughput row that reported the wall time's
        // difference would be a different bug.
        let wall = out
            .rows
            .iter()
            .find(|row| row.metric == WALL_NS_PER_ITER)
            .expect("wall");
        let rate = out
            .rows
            .iter()
            .find(|row| row.metric == RECORDS_PER_S)
            .expect("rate");
        assert!(wall.analysis.delta < -0.049, "{}", wall.analysis.delta);
        assert!(rate.analysis.delta > 0.051, "{}", rate.analysis.delta);
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

    /// The point of the split: a case whose corpus matches and whose compiled
    /// subject does not is demoted on the build, with the corpus guard left
    /// intact for every other case. Before the two lived in one digest, this
    /// could only be compared by waiving the guard on the bytes.
    #[test]
    fn a_per_case_build_mismatch_demotes_only_that_case() {
        let mut base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut base.records {
            if record.case.case == "b" {
                record.build_digest = Some("5e12e5e12e5e12e5".to_owned());
            }
        }

        let mut head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut head.records {
            if record.case.case == "b" {
                record.build_digest = Some("51md51md51md51md".to_owned());
            }
        }

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].case.case, "a");
        let demotion = out
            .not_comparable
            .iter()
            .find(|n| n.what.ends_with("b"))
            .expect("b was demoted");
        assert_eq!(demotion.cause, Cause::BuildLeftOut);
        assert!(
            demotion.why.contains("declared build differs"),
            "{demotion:?}"
        );
        assert!(
            !demotion.why.contains("corpus digest"),
            "the bytes matched; only the build differed: {demotion:?}"
        );
    }

    /// A case that declares nothing asserts nothing. Every existing target is
    /// this case, so the guard must be silent for them rather than demoting a
    /// whole run on an absent field.
    #[test]
    fn a_case_that_declares_no_build_is_compared_as_before() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let head = Builder::new("head").series("a", &ten(1000.0)).build();
        assert!(base.records.iter().all(|r| r.build_digest.is_none()));

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert!(out.not_comparable.is_empty(), "{:?}", out.not_comparable);
    }

    /// One leg declaring and the other not is neither agreement nor
    /// disagreement, and it is what an `ab` against a commit older than the
    /// field looks like. Still not comparable, because an undeclared side
    /// cannot be checked, but the reason must not claim the two legs compiled
    /// different code, which no run established.
    #[test]
    fn a_build_declared_on_one_leg_only_says_only_that() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        for record in &mut head.records {
            record.build_digest = Some("5e12e5e12e5e12e5".to_owned());
        }

        let out = compare(base, head, &[]).expect("compares");
        assert!(out.rows.is_empty());
        let demotion = &out.not_comparable[0];
        assert_eq!(demotion.cause, Cause::BuildLeftOut);
        assert!(
            demotion.why.contains("only one leg declares"),
            "{demotion:?}"
        );
        assert!(demotion.why.contains("none"), "{demotion:?}");
        assert!(
            !demotion.why.contains("compiled different code"),
            "the demotion claims more than it knows: {demotion:?}"
        );
    }

    /// A whole run of one-sided declarations gets its own systemic answer, and
    /// specifically not the "measure it with `bench arms`" one, which cannot
    /// help a reader comparing two commits.
    #[test]
    fn a_wholesale_one_sided_declaration_names_the_older_leg() {
        let base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        let mut head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut head.records {
            record.build_digest = Some("5e12e5e12e5e12e5".to_owned());
        }

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(err.contains("declared on one leg only"), "{err}");
        assert!(
            !err.contains("bench arms"),
            "an arm run cannot compare two commits: {err}"
        );

        let waived = compare(base, head, &[ALLOW_BUILD.to_owned()]).expect("waived");
        assert_eq!(waived.rows.len(), 2);
    }

    /// A case dropped on its corpus never reaches the declared-build question,
    /// so the wholesale answer has to be measured against the cases that did.
    /// Against `shared` it could not fire at all once anything was dropped
    /// earlier, which is the run most in need of a systemic answer.
    #[test]
    fn a_corpus_demotion_does_not_hide_the_wholesale_build_finding() {
        let mut base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .series("c", &ten(3000.0))
            .build();
        let mut head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .series("c", &ten(3000.0))
            .build();
        for record in &mut base.records {
            // `a` differs on its corpus and never reaches the build block; the
            // other two answer the build question the same wrong way.
            if record.case.case == "a" {
                record.corpus_digest = "bbbbbbbbbbbbbbbb".to_owned();
            } else {
                record.build_digest = Some("5e12e5e12e5e12e5".to_owned());
            }
        }
        for record in &mut head.records {
            if record.case.case != "a" {
                record.build_digest = Some("51md51md51md51md".to_owned());
            }
        }

        let err = compare(base, head, &[]).expect_err("refused");
        assert!(err.contains("every judged case (2)"), "{err}");
    }

    /// A leg disagreeing with its own replicates about what it compiled is not
    /// waivable, for the same reason the corpus version is not: there is no
    /// single build left to compare against.
    #[test]
    fn a_build_that_varies_within_a_leg_is_not_waivable() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.records[0].build_digest = Some("5e12e5e12e5e12e5".to_owned());

        for allow in [Vec::new(), vec![ALLOW_BUILD.to_owned()]] {
            let out = compare(base.clone(), head.clone(), &allow).expect("compares");
            assert!(out.rows.is_empty(), "{:?}", out.rows);
            assert_eq!(out.not_comparable[0].cause, Cause::BuildLeftOut);
            assert!(
                out.not_comparable[0].why.contains("within a leg"),
                "{:?}",
                out.not_comparable
            );
        }
    }

    /// Every shared case compiling differently is one finding, not twenty. It
    /// is what pointing `compare` at two feature arms looks like, so the
    /// refusal names the command that measures those properly.
    #[test]
    fn a_wholesale_build_mismatch_is_a_hard_error_and_waivable() {
        let mut base = Builder::new("base")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut base.records {
            record.build_digest = Some("5e12e5e12e5e12e5".to_owned());
        }
        let mut head = Builder::new("head")
            .series("a", &ten(1000.0))
            .series("b", &ten(2000.0))
            .build();
        for record in &mut head.records {
            record.build_digest = Some("51md51md51md51md".to_owned());
        }

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(err.contains("declares a different build"), "{err}");
        assert!(
            err.contains("bench arms"),
            "the refusal names no way out: {err}"
        );

        let waived = compare(base, head, &[ALLOW_BUILD.to_owned()]).expect("waived");
        assert_eq!(waived.rows.len(), 2);
        assert!(
            waived
                .not_comparable
                .iter()
                .all(|n| n.cause == Cause::BuildCompared),
            "{:?}",
            waived.not_comparable
        );
    }

    /// What the whole mode is for: two arms that read the same bytes through
    /// different code compare with no waiver at all. The feature sets differ,
    /// which on this axis is the subject rather than a bypassed guard.
    #[test]
    fn two_arms_declaring_different_builds_compare_without_a_waiver() {
        let base = as_arm(
            Builder::new("base").series("a", &ten(1000.0)).build(),
            Some("5e12e5e12e5e12e5"),
            "spate-json/default",
        );
        let head = as_arm(
            Builder::new("head").series("a", &ten(1200.0)).build(),
            Some("51md51md51md51md"),
            "spate-json/simd",
        );

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert!(out.not_comparable.is_empty(), "{:?}", out.not_comparable);
        assert!(out.allowed.is_empty(), "an arm run waived a guard");
        // The divergence is still reported, so a reader sees which two arms
        // produced the table even though nothing was bypassed to get it.
        assert!(
            out.divergences().iter().any(|d| d.field == FIELD_FEATURES),
            "{:?}",
            out.divergences()
        );
    }

    /// A case that declares nothing has no arm to compare, so it is measured on
    /// its corpus alone. This is what keeps a target with no feature axis usable
    /// as the A/A acceptance run for the mode itself.
    #[test]
    fn two_arms_declaring_nothing_are_compared_as_before() {
        let base = as_arm(
            Builder::new("base").series("a", &ten(1000.0)).build(),
            None,
            "spate-bench/default",
        );
        let head = as_arm(
            Builder::new("head").series("a", &ten(1000.0)).build(),
            None,
            "spate-bench/default",
        );

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert!(out.not_comparable.is_empty(), "{:?}", out.not_comparable);
    }

    /// The rule inverts with the axis, and this is the half that only exists on
    /// the arm one: two arms agreeing about what compiled means the feature
    /// never reached the case, and the numbers are one build measured twice.
    #[test]
    fn an_arm_case_that_declares_the_same_build_is_demoted() {
        let mut base = as_arm(
            Builder::new("base")
                .series("a", &ten(1000.0))
                .series("b", &ten(2000.0))
                .build(),
            Some("5e12e5e12e5e12e5"),
            "spate-json/default",
        );
        let head = as_arm(
            Builder::new("head")
                .series("a", &ten(1000.0))
                .series("b", &ten(2000.0))
                .build(),
            Some("5e12e5e12e5e12e5"),
            "spate-json/simd",
        );
        // Only `a` swapped; `b` compiled the same code on both arms.
        for record in &mut base.records {
            if record.case.case == "a" {
                record.build_digest = Some("51md51md51md51md".to_owned());
            }
        }

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].case.case, "a");
        let demotion = out
            .not_comparable
            .iter()
            .find(|n| n.what.ends_with('b'))
            .expect("b was demoted");
        assert_eq!(demotion.cause, Cause::BuildSameLeftOut);
        assert!(
            demotion.why.contains("both arms declare the same build"),
            "{demotion:?}"
        );
    }

    /// Every case agreeing is one finding about the run rather than a demotion
    /// per case. The refusal states only what is known, that these cases
    /// declare no difference, because a feature can change something they do
    /// not declare, and it names both ways out.
    #[test]
    fn an_arm_run_whose_cases_declare_no_difference_is_refused() {
        let base = as_arm(
            Builder::new("base")
                .series("a", &ten(1000.0))
                .series("b", &ten(2000.0))
                .build(),
            Some("5e12e5e12e5e12e5"),
            "spate-json/default",
        );
        let head = as_arm(
            Builder::new("head")
                .series("a", &ten(1000.0))
                .series("b", &ten(2000.0))
                .build(),
            Some("5e12e5e12e5e12e5"),
            "spate-json/default",
        );

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(
            err.contains("declares the same build on both arms"),
            "{err}"
        );
        assert!(err.contains("--head-features"), "{err}");
        assert!(
            err.contains("do not declare"),
            "the refusal claims more than it knows: {err}"
        );

        let waived = compare(base, head, &[ALLOW_BUILD.to_owned()]).expect("waived");
        assert_eq!(waived.rows.len(), 2);
    }

    /// The axis decides which way the build guard points, so two legs that
    /// disagree about it would have one judged by the other's rule. Refused
    /// alongside a transposed pair rather than waivable.
    #[test]
    fn legs_that_disagree_about_the_axis_are_refused() {
        let base = Builder::new("base").series("a", &ten(1000.0)).build();
        let head = as_arm(
            Builder::new("head").series("a", &ten(1000.0)).build(),
            None,
            "spate-bench/default",
        );

        for allow in [
            Vec::new(),
            allowable().iter().map(|a| (*a).to_owned()).collect(),
        ] {
            let err = compare(base.clone(), head.clone(), &allow).expect_err("refused");
            assert!(err.contains("disagree about what they vary"), "{err}");
        }
    }

    /// The feature set steps aside on the arm axis and nowhere else. Two
    /// commits that resolved features differently are still not one comparison.
    #[test]
    fn differing_features_are_the_subject_on_one_axis_and_a_refusal_on_the_other() {
        let mut base = Builder::new("base").series("a", &ten(1000.0)).build();
        let mut head = Builder::new("head").series("a", &ten(1000.0)).build();
        head.build.features = vec!["spate-json/simd".to_owned()];
        for record in &mut head.records {
            record.build.features = vec!["spate-json/simd".to_owned()];
        }

        let err = compare(base.clone(), head.clone(), &[]).expect_err("refused");
        assert!(err.contains("not the same build"), "{err}");

        base = as_arm(base, None, "spate-json/default");
        head = as_arm(head, None, "spate-json/simd");
        compare(base, head, &[]).expect("the arm axis compares them");
    }

    /// Every case differing is systemic, and a report of nothing but demotions
    /// reads as "no cases" rather than as the finding it is.
    #[test]
    fn a_wholesale_digest_mismatch_is_a_hard_error_and_waivable() {
        // Two cases, because one case differing is not evidence about the
        // corpora as a whole; see the test above.
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

        // One literal, both consumers. The refusal and the report header
        // describe a difference through the same `Display`, so a reader who has
        // seen one recognizes the other.
        const DIFFERENCE: &str = "rustc: base 'rustc 1.94.0' vs head 'rustc 1.95.0'";
        assert!(err.contains(DIFFERENCE), "{err}");

        let waived = compare(base, head, &["rustc".to_owned()]).expect("waived");
        let found = waived.divergences();
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].to_string(), DIFFERENCE);
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

    /// `load_leg`'s refusals, through `load_leg`. Asserting the *setup*, that
    /// two records differ, proves nothing about the function that is supposed
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

    /// Tied to real records rather than a hand-built analysis: the renderer asks
    /// this, and the replicate floor is applied where the verdict is decided.
    #[test]
    fn a_run_below_the_replicate_floor_judges_nothing() {
        let base = Builder::new("base")
            .series("a", &[100.0, 101.0, 99.0])
            .build();
        let head = Builder::new("head")
            .series("a", &[130.0, 131.0, 129.0])
            .build();

        let out = compare(base, head, &[]).expect("compares");
        assert_eq!(out.unjudged().count(), out.rows.len());
        assert_eq!(out.significant().count(), 0);
        assert!(!out.rows.is_empty());
        for row in &out.rows {
            assert_eq!(row.analysis.verdict, Verdict::NoVerdict);
        }
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
