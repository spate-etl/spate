//! Writing a [`Comparison`] out: a terminal table, Markdown, or JSON.
//!
//! Three writers over one value. Nothing is recomputed here — a marker in the
//! terminal and a row in the Markdown come from the same [`crate::stats`]
//! verdict, so the two renderings of one run cannot disagree about what moved.
//!
//! The decision rule is printed verbatim in every format. A report that states
//! how it decided can be argued with; one that shows a number and a marker
//! cannot.
//!
//! The JSON view is the one a script reads, and it is versioned by
//! [`REPORT_SCHEMA_VERSION`]. Its `verdict` and `cause` fields are tokens a
//! consumer matches on, stable for a version, where the two human formats
//! phrase a verdict for a reader and state a cause only as prose. It carries the
//! guarded fields the two legs disagree about as data, which the human formats
//! put in the header.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::Serialize;

use crate::clock::{human_bytes, human_ns, human_rate};
use crate::compare::{Cause, Comparison, Row};
use crate::record::{ALLOC_BYTES_PER_ITER, ALLOC_COUNT_PER_ITER, PEAK_RSS_BYTES};
use crate::stats::{ALLOC_FLOOR, CONFIDENCE, DEFAULT_FLOOR, MIN_REPLICATES, RSS_FLOOR, Verdict};

/// Schema version of the `--format json` report, carried as its `schema` field.
/// Bump on any breaking field change.
///
/// One of three independent versions: [`crate::record::SCHEMA_VERSION`] versions
/// the records a leg is written from, and [`crate::protocol::PROTOCOL_VERSION`]
/// versions the conversation with a bench binary. A report is rendered from
/// records rather than being one, so the two move apart.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

/// The rule, stated the same way in every rendering.
///
/// Built from the constants in [`crate::stats`] rather than restating them.
/// The numbers a report quotes and the numbers it applies have to be the same
/// numbers, and a hand-written sentence is the obvious place for them to drift
/// apart.
#[must_use]
pub fn decision_rule() -> String {
    format!(
        "A metric is flagged only when BOTH hold: the bootstrapped {confidence:.0}% confidence \
         interval on the mean per-pair relative difference excludes zero, AND the difference is \
         at least the metric's floor — {default:.0}% for wall time, CPU time and throughput, \
         {rss:.0}% for the peak resident set, {alloc:.0}% for allocation totals. Replicates are \
         interleaved and paired by index, so machine drift cancels within a pair. Fewer than \
         {min} pairs prints a difference and no verdict. A case marked erratic is reported and \
         never flagged.",
        confidence = CONFIDENCE * 100.0,
        default = DEFAULT_FLOOR * 100.0,
        rss = RSS_FLOOR * 100.0,
        alloc = ALLOC_FLOOR * 100.0,
        min = MIN_REPLICATES,
    )
}

/// What the significant-changes table has to say, decided once.
///
/// Both human formats render this rather than each deciding for itself, and the
/// CLI reads it for its exit code. The distinction it carries — the rule was
/// applied and nothing cleared it, versus the rule was never applied — is the
/// one a report gets wrong by stating the first when the second happened, so it
/// is settled in one place and phrased in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Summary {
    /// Nothing paired, so there is no table at all.
    NothingComparable,
    /// Rows exist, and not one of them was both judged and eligible to be
    /// flagged. `unjudged` fell short of the replicate floor; the rest come from
    /// cases marked erratic.
    NothingJudged {
        /// How many rows fell short of the replicate floor.
        unjudged: usize,
        /// How many rows there were in total.
        total: usize,
    },
    /// The rule was applied and no metric cleared it.
    NoneCleared,
    /// There are findings to print.
    Findings,
}

impl Summary {
    /// Classifies what a comparison has to say.
    #[must_use]
    pub fn of(comparison: &Comparison) -> Self {
        if comparison.rows.is_empty() {
            return Self::NothingComparable;
        }
        // Erratic *or* unjudged, per row: a row excluded for being noisy and a
        // row that never reached the rule are different reasons for the same
        // thing, and a run can hold both at once.
        let judged = comparison
            .rows
            .iter()
            .any(|row| !row.erratic && row.analysis.verdict.is_judged());
        if !judged {
            return Self::NothingJudged {
                unjudged: comparison.unjudged().count(),
                total: comparison.rows.len(),
            };
        }
        if comparison.significant().next().is_none() {
            return Self::NoneCleared;
        }
        Self::Findings
    }

    /// The sentence, or `None` when there is a table to print instead.
    ///
    /// Free of markup so both formats state it identically.
    fn sentence(&self) -> Option<String> {
        match *self {
            Self::NothingComparable => Some(
                "None — and no metric was comparable at all, so the rule below was never \
                 applied. See Not comparable."
                    .to_owned(),
            ),
            Self::NothingJudged { unjudged: 0, .. } => Some(
                "None — and no metric was judged at all: every metric comes from a case marked \
                 erratic, which the rule below never flags. See Informational."
                    .to_owned(),
            ),
            Self::NothingJudged { unjudged, total } if unjudged == total => Some(format!(
                "None — and no metric was judged at all: all {total} have fewer than \
                 {MIN_REPLICATES} paired replicates, so the rule below was never applied. Re-run \
                 with --replicates {MIN_REPLICATES} or more."
            )),
            Self::NothingJudged { unjudged, total } => Some(format!(
                "None — and no metric was judged at all: {unjudged} of {total} have fewer than \
                 {MIN_REPLICATES} paired replicates and the rest come from cases marked erratic, \
                 so the rule below was never applied."
            )),
            Self::NoneCleared => {
                Some("None. No metric cleared both halves of the rule below.".to_owned())
            }
            Self::Findings => None,
        }
    }
}

/// The count of rows the rule was not applied to, when that is worth saying
/// beside a table rather than instead of one.
///
/// Stated whether or not anything cleared the rule: a reader acting on a report
/// with findings in it is the one least likely to notice that part of the run
/// was never judged. Silent when the summary already says nothing was judged,
/// which would otherwise state the same shortfall twice.
fn shortfall(comparison: &Comparison, summary: &Summary) -> Option<String> {
    if !matches!(summary, Summary::NoneCleared | Summary::Findings) {
        return None;
    }
    let unjudged = comparison.unjudged().count();
    let total = comparison.rows.len();
    (unjudged > 0).then(|| {
        format!(
            "{unjudged} of the {total} metric(s) below have fewer than {MIN_REPLICATES} paired \
             replicates and were not judged; the rule was not applied to them. Re-run with \
             --replicates {MIN_REPLICATES} or more."
        )
    })
}

/// Renders the terminal view.
#[must_use]
pub fn table(comparison: &Comparison) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", header_lines(comparison).join("\n"));

    let significant: Vec<&Row> = comparison.significant().collect();
    let summary = Summary::of(comparison);
    let _ = writeln!(out, "\nSignificant changes");
    match summary.sentence() {
        Some(sentence) => {
            let _ = writeln!(out, "  {sentence}");
        }
        None => write_plain_rows(&mut out, &significant),
    }
    if let Some(note) = shortfall(comparison, &summary) {
        let _ = writeln!(out, "  note: {note}");
    }

    let informational: Vec<&Row> = comparison.informational().collect();
    if !informational.is_empty() {
        let _ = writeln!(out, "\nInformational (erratic cases, never flagged)");
        write_plain_rows(&mut out, &informational);
    }

    let all: Vec<&Row> = comparison.rows.iter().collect();
    let _ = writeln!(out, "\nAll cases ({} rows)", all.len());
    write_plain_rows(&mut out, &all);

    if !comparison.not_comparable.is_empty() {
        let _ = writeln!(out, "\nNot comparable");
        for entry in &comparison.not_comparable {
            let _ = writeln!(out, "  {} — {}", entry.what, entry.why);
        }
    }

    let _ = writeln!(out, "\n{}", decision_rule());
    out
}

/// Renders the Markdown view, for a pull-request comment.
#[must_use]
pub fn markdown(comparison: &Comparison) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Wall-clock A/B\n");
    // A bullet per header line rather than a hard line break: two trailing
    // spaces are invisible, and a tool that trims them silently reflows the
    // header into one paragraph.
    for line in header_lines(comparison) {
        let _ = writeln!(out, "- {line}");
    }

    let significant: Vec<&Row> = comparison.significant().collect();
    let summary = Summary::of(comparison);
    let _ = writeln!(out, "\n### Significant changes\n");
    match summary.sentence() {
        Some(sentence) => {
            let _ = writeln!(out, "{sentence}");
        }
        None => write_markdown_rows(&mut out, &significant),
    }
    if let Some(note) = shortfall(comparison, &summary) {
        // A blank line first: a paragraph directly under a table is part of the
        // table to a Markdown renderer.
        let _ = writeln!(out, "\n{note}");
    }

    let informational: Vec<&Row> = comparison.informational().collect();
    if !informational.is_empty() {
        let _ = writeln!(
            out,
            "\n<details>\n<summary>Informational — erratic cases, never flagged ({} rows)</summary>\n",
            informational.len()
        );
        write_markdown_rows(&mut out, &informational);
        let _ = writeln!(out, "\n</details>");
    }

    let all: Vec<&Row> = comparison.rows.iter().collect();
    let _ = writeln!(
        out,
        "\n<details>\n<summary>All cases ({} rows)</summary>\n",
        all.len()
    );
    write_markdown_rows(&mut out, &all);
    let _ = writeln!(out, "\n</details>");

    if !comparison.not_comparable.is_empty() {
        let _ = writeln!(out, "\n### Not comparable\n");
        for entry in &comparison.not_comparable {
            let _ = writeln!(out, "- `{}` — {}", entry.what, entry.why);
        }
    }

    let _ = writeln!(
        out,
        "\n<details>\n<summary>How a difference is decided</summary>\n\n{}\n\n</details>",
        decision_rule()
    );
    out
}

/// Renders the machine-readable view.
///
/// # Errors
///
/// When the report does not serialise, which for this shape means a value that
/// is not a finite number.
pub fn json(comparison: &Comparison) -> Result<String, String> {
    let counts: Vec<usize> = comparison
        .rows
        .iter()
        .map(|row| row.analysis.replicates)
        .collect();
    let report = Report {
        schema: REPORT_SCHEMA_VERSION,
        // Both ends, because a run that paired ten for one case and three for
        // another has not taken ten replicates — and a script reading one
        // number would be told it had.
        replicates_min: counts.iter().copied().min().unwrap_or(0),
        replicates_max: counts.iter().copied().max().unwrap_or(0),
        confidence: CONFIDENCE,
        min_replicates: MIN_REPLICATES,
        // Keyed by the metric names a row carries, so a consumer holding a row
        // can look its floor up. `default` is the remaining key because
        // `stats::floor_for` genuinely falls back for every other metric.
        floors: BTreeMap::from([
            ("default", DEFAULT_FLOOR),
            (PEAK_RSS_BYTES, RSS_FLOOR),
            (ALLOC_BYTES_PER_ITER, ALLOC_FLOOR),
            (ALLOC_COUNT_PER_ITER, ALLOC_FLOOR),
        ]),
        allowed: comparison.allowed.clone(),
        // The waiver alone would leave a consumer to diff the two fingerprints,
        // which over-reports: `leg`, `git_describe`, `dirty` and `feature_args`
        // are serialised and none of them is guarded.
        divergences: comparison
            .divergences()
            .into_iter()
            .map(|d| DivergenceReport {
                field: d.field,
                base: d.base,
                head: d.head,
            })
            .collect(),
        base: LegReport::of(&comparison.base),
        head: LegReport::of(&comparison.head),
        rows: comparison.rows.iter().map(RowReport::of).collect(),
        not_comparable: comparison
            .not_comparable
            .iter()
            .map(|n| NotComparableReport {
                what: n.what.clone(),
                why: n.why.clone(),
                cause: n.cause.token(),
            })
            .collect(),
        decision_rule: decision_rule(),
    };
    serde_json::to_string_pretty(&report).map_err(|e| e.to_string())
}

/// The header both human formats share.
fn header_lines(comparison: &Comparison) -> Vec<String> {
    let base = &comparison.base;
    let head = &comparison.head;
    let describe = |leg: &crate::compare::Leg| {
        let mut text = leg
            .build
            .git_describe
            .clone()
            .unwrap_or_else(|| "unknown".to_owned());
        if leg.build.dirty {
            text.push_str(" (dirty)");
        }
        text
    };

    let mut lines = vec![
        format!("base {} · {}", describe(base), base.dir.display()),
        format!("head {} · {}", describe(head), head.dir.display()),
        // The replicate span is the *paired* count over both legs, so it carries
        // no leg label — a case that lost a base-leg record would otherwise be
        // reported as the head leg running short.
        replicate_span(comparison),
        // Labelled `head` because that is the leg they describe. The guard makes
        // them true of the base leg too, right up until an `--allow` waives the
        // field that stopped being true.
        format!(
            "head {} · {} · {} · {}",
            head.build.rustc.as_deref().unwrap_or("rustc unknown"),
            head.build
                .host_triple
                .as_deref()
                .unwrap_or("triple unknown"),
            head.build.profile.as_deref().unwrap_or("profile unknown"),
            if head.build.features.is_empty() {
                "default features".to_owned()
            } else {
                head.build.features.join(",")
            },
        ),
        format!(
            "head host {} · {} · {} core(s) · label {}",
            head.host.os, head.host.cpu, head.host.cores, head.host.label
        ),
    ];

    // Classified by `Cause`, never by matching the prose the comparator also
    // writes: this is the most-read line of the report, and a reworded sentence
    // there must not silently make it say the opposite.
    let count = |cause: Cause| {
        comparison
            .not_comparable
            .iter()
            .filter(|n| n.cause == cause)
            .count()
    };
    lines.push(
        match (count(Cause::DigestCompared), count(Cause::DigestLeftOut)) {
            (0, 0) => "corpus digests: every compared case matched".to_owned(),
            (0, out) => format!("corpus digests: {out} case(s) did NOT match and were left out"),
            (waived, 0) => {
                format!("corpus digests: {waived} case(s) did NOT match and were compared anyway")
            }
            (waived, out) => format!(
                "corpus digests: {out} case(s) did NOT match and were left out, \
             {waived} more were compared anyway"
            ),
        },
    );

    if !comparison.allowed.is_empty() {
        lines.push(format!(
            "GUARDS WAIVED with --allow: {}",
            comparison.allowed.join(", ")
        ));
    }

    // Taken from the same set the guard refuses over, rather than from the
    // fields that happen to appear above: a waived `codegen` or `protocol` is
    // named here and nowhere else in the header.
    let divergences = comparison.divergences();
    if !divergences.is_empty() {
        lines.push("the two legs differ on these guarded fields:".to_owned());
        lines.extend(divergences.iter().map(ToString::to_string));
    }
    lines
}

/// "10 replicate(s)", or "3-10 replicate(s)" when the cases do not agree — a
/// single number would report the luckiest case as if it were the run.
fn replicate_span(comparison: &Comparison) -> String {
    let counts: Vec<usize> = comparison
        .rows
        .iter()
        .map(|row| row.analysis.replicates)
        .collect();
    match (counts.iter().min(), counts.iter().max()) {
        (Some(low), Some(high)) if low != high => format!("{low}-{high} replicate(s)"),
        (Some(_), Some(high)) => format!("{high} replicate(s)"),
        _ => "0 replicate(s)".to_owned(),
    }
}

fn write_plain_rows(out: &mut String, rows: &[&Row]) {
    for row in rows {
        let _ = writeln!(
            out,
            "  {:<9} {} · {}  {} -> {}  {}  CI [{}, {}]  {}",
            marker(row),
            row.case,
            row.metric,
            value(row, row.analysis.base_mean),
            value(row, row.analysis.head_mean),
            percent(row.analysis.delta),
            percent(row.analysis.ci_low),
            percent(row.analysis.ci_high),
            row.analysis.verdict.label(),
        );
    }

    // The reasons, once each, under the rows rather than repeated on every one:
    // an erratic case carries one reason and up to seven metrics.
    for why in reasons(rows) {
        let _ = writeln!(out, "  erratic: {why}");
    }
}

/// Each distinct erratic reason among these rows, in first-seen order.
fn reasons<'r>(rows: &[&'r Row]) -> Vec<&'r str> {
    let mut seen: Vec<&str> = Vec::new();
    for row in rows {
        if let Some(why) = row.erratic_reason.as_deref()
            && !seen.contains(&why)
        {
            seen.push(why);
        }
    }
    seen
}

fn write_markdown_rows(out: &mut String, rows: &[&Row]) {
    let _ = writeln!(
        out,
        "| Case | Metric | Base | Head | Δ | {:.0}% CI | Verdict |",
        CONFIDENCE * 100.0
    );
    let _ = writeln!(out, "|---|---|---:|---:|---:|:---:|---|");
    for row in rows {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} | [{}, {}] | {} |",
            row.case,
            row.metric,
            value(row, row.analysis.base_mean),
            value(row, row.analysis.head_mean),
            percent(row.analysis.delta),
            percent(row.analysis.ci_low),
            percent(row.analysis.ci_high),
            verdict_cell(row),
        );
    }

    for why in reasons(rows) {
        let _ = writeln!(out, "\nErratic: {why}");
    }
}

/// The verdict cell, marked when the case is one the rule never flags.
fn verdict_cell(row: &Row) -> String {
    if row.erratic {
        format!("{} (erratic)", row.analysis.verdict.label())
    } else {
        row.analysis.verdict.label().to_owned()
    }
}

/// The terminal marker, which also says when a row can never be flagged.
fn marker(row: &Row) -> &'static str {
    if row.erratic {
        return "[erratic]";
    }
    match row.analysis.verdict {
        Verdict::Improved => "[better]",
        Verdict::Regressed => "[worse]",
        Verdict::NoChange => "[same]",
        Verdict::NoVerdict => "[?]",
    }
}

/// A metric value in the unit it was recorded in.
///
/// Adaptive units are applied here and only here, so the stored number stays
/// the one the record carries.
fn value(row: &Row, value: f64) -> String {
    match row.unit.as_str() {
        "ns" => human_ns(value),
        "bytes" => human_bytes(value),
        "records/s" => format!("{} rec/s", human_rate(value)),
        "bytes/s" => format!("{}B/s", human_rate(value)),
        "allocations" => format!("{value:.2}"),
        other => format!("{value:.3} {other}"),
    }
}

fn percent(fraction: f64) -> String {
    format!("{:+.2}%", fraction * 100.0)
}

#[derive(Debug, Serialize)]
struct Report {
    schema: u32,
    replicates_min: usize,
    replicates_max: usize,
    confidence: f64,
    min_replicates: usize,
    floors: BTreeMap<&'static str, f64>,
    allowed: Vec<String>,
    divergences: Vec<DivergenceReport>,
    base: LegReport,
    head: LegReport,
    rows: Vec<RowReport>,
    not_comparable: Vec<NotComparableReport>,
    decision_rule: String,
}

#[derive(Debug, Serialize)]
struct LegReport {
    dir: String,
    records: usize,
    priming: usize,
    build: crate::fingerprint::BuildFingerprint,
    host: crate::fingerprint::Host,
}

impl LegReport {
    fn of(leg: &crate::compare::Leg) -> Self {
        Self {
            dir: leg.dir.display().to_string(),
            records: leg.records.len(),
            priming: leg.priming(),
            build: leg.build.clone(),
            host: leg.host.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct RowReport {
    case: crate::record::CaseId,
    metric: String,
    unit: String,
    higher_is_better: bool,
    erratic: bool,
    erratic_reason: Option<String>,
    replicates: usize,
    base_mean: f64,
    head_mean: f64,
    delta: f64,
    ci_low: f64,
    ci_high: f64,
    floor: f64,
    verdict: &'static str,
    significant: bool,
}

impl RowReport {
    fn of(row: &Row) -> Self {
        Self {
            case: row.case.clone(),
            metric: row.metric.clone(),
            unit: row.unit.clone(),
            higher_is_better: row.higher_is_better,
            erratic: row.erratic,
            erratic_reason: row.erratic_reason.clone(),
            replicates: row.analysis.replicates,
            base_mean: row.analysis.base_mean,
            head_mean: row.analysis.head_mean,
            delta: row.analysis.delta,
            ci_low: row.analysis.ci_low,
            ci_high: row.analysis.ci_high,
            floor: row.analysis.floor,
            verdict: row.analysis.verdict.token(),
            significant: !row.erratic && row.analysis.verdict.is_significant(),
        }
    }
}

#[derive(Debug, Serialize)]
struct DivergenceReport {
    field: &'static str,
    base: String,
    head: String,
}

#[derive(Debug, Serialize)]
struct NotComparableReport {
    what: String,
    why: String,
    cause: &'static str,
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use super::{Summary, decision_rule, json, markdown, table};
    use crate::compare::{Cause, Comparison, Leg, NotComparable, Row};
    use crate::fingerprint::{BuildFingerprint, Host};
    use crate::record::{CaseId, Record, WALL_NS_PER_ITER};
    use crate::stats::{Analysis, Verdict};

    fn leg(name: &str, describe: &str, dirty: bool) -> Leg {
        Leg {
            dir: PathBuf::from(format!("/legs/{name}")),
            build: BuildFingerprint {
                protocol: 1,
                leg: name.to_owned(),
                rustc: Some("rustc 1.94.0".to_owned()),
                host_triple: Some("aarch64-apple-darwin".to_owned()),
                profile: Some("bench".to_owned()),
                codegen: Some("cafecafecafecafe".to_owned()),
                features: Vec::new(),
                feature_args: Vec::new(),
                git_describe: Some(describe.to_owned()),
                dirty,
            },
            host: Host {
                os: "macos/aarch64".to_owned(),
                cpu: "Apple M5 Max".to_owned(),
                cores: 16,
                label: "local".to_owned(),
            },
            records: Vec::<Record>::new(),
        }
    }

    fn row(case: &str, verdict: Verdict, erratic: bool) -> Row {
        Row {
            case: CaseId {
                krate: "spate-bench".to_owned(),
                target: "selftest_wall".to_owned(),
                case: case.to_owned(),
            },
            metric: WALL_NS_PER_ITER.to_owned(),
            unit: "ns".to_owned(),
            higher_is_better: false,
            erratic,
            erratic_reason: erratic.then(|| "the allocator decides".to_owned()),
            analysis: Analysis {
                replicates: 10,
                base_mean: 1000.0,
                head_mean: 1300.0,
                delta: 0.3,
                ci_low: 0.25,
                ci_high: 0.35,
                floor: 0.05,
                verdict,
            },
        }
    }

    fn comparison(rows: Vec<Row>, not_comparable: Vec<NotComparable>) -> Comparison {
        Comparison {
            base: leg("base", "v0.1.0-2-gaaaaaaa", false),
            head: leg("head", "v0.1.0-3-gbbbbbbb", true),
            rows,
            not_comparable,
            allowed: Vec::new(),
        }
    }

    /// The golden Markdown. It is what a pull-request comment will carry, so a
    /// change to it should be a deliberate edit rather than a side effect.
    #[test]
    fn the_markdown_report_is_the_shape_a_comment_carries() {
        let rendered = markdown(&comparison(
            vec![row("sort_u64_16k", Verdict::Regressed, false)],
            vec![NotComparable {
                what: "spate-bench/selftest_wall/gone".to_owned(),
                why: "present only in the base leg".to_owned(),
                cause: Cause::Other,
            }],
        ));

        const GOLDEN: &str = "\
## Wall-clock A/B

- base v0.1.0-2-gaaaaaaa · /legs/base
- head v0.1.0-3-gbbbbbbb (dirty) · /legs/head
- 10 replicate(s)
- head rustc 1.94.0 · aarch64-apple-darwin · bench · default features
- head host macos/aarch64 · Apple M5 Max · 16 core(s) · label local
- corpus digests: every compared case matched

### Significant changes

| Case | Metric | Base | Head | Δ | 90% CI | Verdict |
|---|---|---:|---:|---:|:---:|---|
| `spate-bench/selftest_wall/sort_u64_16k` | wall_ns_per_iter | 1.00 µs | 1.30 µs | +30.00% | [+25.00%, +35.00%] | regressed |

<details>
<summary>All cases (1 rows)</summary>

| Case | Metric | Base | Head | Δ | 90% CI | Verdict |
|---|---|---:|---:|---:|:---:|---|
| `spate-bench/selftest_wall/sort_u64_16k` | wall_ns_per_iter | 1.00 µs | 1.30 µs | +30.00% | [+25.00%, +35.00%] | regressed |

</details>

### Not comparable

- `spate-bench/selftest_wall/gone` — present only in the base leg

<details>
<summary>How a difference is decided</summary>

";

        assert!(rendered.starts_with(GOLDEN), "got:\n{rendered}");
        assert!(rendered.contains(&decision_rule()));
        assert!(rendered.trim_end().ends_with("</details>"));
    }

    #[test]
    fn an_empty_significant_table_says_so_in_words() {
        let rendered = markdown(&comparison(
            vec![row("a", Verdict::NoChange, false)],
            Vec::new(),
        ));
        assert!(rendered.contains("None. No metric cleared both halves"));
        assert!(!rendered.contains("Not comparable"));
    }

    /// A row the rule never reached, as a run of fewer than `MIN_REPLICATES`
    /// produces one.
    fn unjudged_row(case: &str) -> Row {
        let mut row = row(case, Verdict::NoVerdict, false);
        row.analysis.replicates = 3;
        row
    }

    /// The claim this branch exists to stop: a run too short to judge said
    /// nothing cleared the rule, which is the opposite of what happened.
    #[test]
    fn a_run_too_short_to_judge_says_the_rule_was_never_applied() {
        let cmp = comparison(vec![unjudged_row("a"), unjudged_row("b")], Vec::new());

        let rendered = markdown(&cmp);
        assert!(
            rendered.contains("no metric was judged at all"),
            "{rendered}"
        );
        assert!(rendered.contains("--replicates 5"), "{rendered}");
        assert!(
            !rendered.contains("No metric cleared both halves"),
            "{rendered}"
        );

        let plain = table(&cmp);
        assert!(plain.contains("no metric was judged at all"), "{plain}");
        assert!(
            !plain.contains("note:"),
            "the whole-run case is stated once, not twice: {plain}"
        );
    }

    /// The same false claim with a different cause: an erratic row is excluded
    /// before the rule's outcome is consulted, so it never failed the rule
    /// either.
    #[test]
    fn an_all_erratic_run_says_the_rule_was_never_applied() {
        let cmp = comparison(
            vec![
                row("a", Verdict::Regressed, true),
                row("b", Verdict::NoChange, true),
            ],
            Vec::new(),
        );

        for rendered in [markdown(&cmp), table(&cmp)] {
            // Not "marked erratic": `decision_rule()` carries that phrase and
            // every format appends the rule, so it is present whatever this
            // branch does.
            assert!(
                rendered.contains("no metric was judged at all"),
                "{rendered}"
            );
            assert!(
                rendered.contains("comes from a case marked erratic"),
                "{rendered}"
            );
            assert!(
                !rendered.contains("No metric cleared both halves"),
                "{rendered}"
            );
        }
    }

    /// The two causes can split a run between them, and the claim the report
    /// must not make is the same one.
    #[test]
    fn a_run_judged_on_neither_count_says_so() {
        let cmp = comparison(
            vec![unjudged_row("a"), row("b", Verdict::Regressed, true)],
            Vec::new(),
        );

        for rendered in [markdown(&cmp), table(&cmp)] {
            assert!(
                rendered.contains("no metric was judged at all"),
                "{rendered}"
            );
            assert!(rendered.contains("1 of 2"), "{rendered}");
            assert!(
                !rendered.contains("No metric cleared both halves"),
                "a metric excluded for being erratic never failed the rule: {rendered}"
            );
        }
    }

    /// Two legs sharing no case produce no rows at all, which is a different
    /// claim again from a rule nothing cleared.
    #[test]
    fn a_comparison_with_no_rows_says_nothing_was_comparable() {
        let cmp = comparison(
            Vec::new(),
            vec![NotComparable {
                what: "spate-bench/selftest_wall/gone".to_owned(),
                why: "present only in the base leg".to_owned(),
                cause: Cause::Other,
            }],
        );

        for rendered in [markdown(&cmp), table(&cmp)] {
            assert!(
                rendered.contains("no metric was comparable at all"),
                "{rendered}"
            );
            assert!(
                !rendered.contains("No metric cleared both halves"),
                "{rendered}"
            );
        }
    }

    /// The classification the CLI exits on, asserted directly rather than
    /// through a rendered sentence. `NothingComparable` is the only one that
    /// earns a non-zero exit, and a report with findings must not: a regression
    /// is a result, not a failure, and nothing in this tier gates anything.
    #[test]
    fn only_a_comparison_with_no_rows_classifies_as_nothing_comparable() {
        assert_eq!(
            Summary::of(&comparison(Vec::new(), Vec::new())),
            Summary::NothingComparable
        );
        for rows in [
            vec![row("a", Verdict::Regressed, false)],
            vec![row("a", Verdict::NoChange, false)],
            vec![row("a", Verdict::Improved, true)],
            vec![unjudged_row("a")],
        ] {
            assert_ne!(
                Summary::of(&comparison(rows, Vec::new())),
                Summary::NothingComparable,
                "a run that paired something exited as though it had not"
            );
        }
    }

    /// A shortfall that covers part of the run is counted beside the table
    /// rather than replacing it, so it survives a report that also has findings.
    #[test]
    fn a_partial_shortfall_is_counted_beside_the_table() {
        let cmp = comparison(
            vec![
                row("a", Verdict::Regressed, false),
                row("b", Verdict::NoChange, false),
                unjudged_row("c"),
            ],
            Vec::new(),
        );

        let rendered = markdown(&cmp);
        assert!(rendered.contains("1 of the 3 metric(s)"), "{rendered}");
        assert!(
            rendered.contains("`spate-bench/selftest_wall/a`"),
            "{rendered}"
        );
        assert!(table(&cmp).contains("note: 1 of the 3 metric(s)"));
    }

    /// The shortfall line is independent of the significant table being empty,
    /// which a fourth branch would have swallowed.
    #[test]
    fn a_shortfall_is_counted_even_when_nothing_cleared_the_rule() {
        let cmp = comparison(
            vec![row("a", Verdict::NoChange, false), unjudged_row("b")],
            Vec::new(),
        );

        let rendered = markdown(&cmp);
        assert!(
            rendered.contains("No metric cleared both halves"),
            "{rendered}"
        );
        assert!(rendered.contains("1 of the 2 metric(s)"), "{rendered}");
    }

    /// An erratic case must never appear in the significant table, however
    /// large its difference.
    #[test]
    fn an_erratic_row_renders_as_informational_only() {
        let rendered = markdown(&comparison(
            vec![row("churn", Verdict::Regressed, true)],
            Vec::new(),
        ));
        let significant = rendered
            .split("### Significant changes")
            .nth(1)
            .expect("section")
            .split("<details>")
            .next()
            .expect("body");
        assert!(!significant.contains("churn"), "{significant}");
        assert!(rendered.contains("Informational — erratic cases, never flagged (1 rows)"));
    }

    /// A reader shown an informational row has to be told why it is
    /// informational, or the section is just a table with a heading.
    #[test]
    fn the_erratic_reason_reaches_every_format() {
        let cmp = comparison(vec![row("churn", Verdict::Regressed, true)], Vec::new());

        let md = markdown(&cmp);
        assert!(md.contains("Erratic: the allocator decides"), "{md}");
        assert!(md.contains("regressed (erratic)"), "{md}");

        let plain = table(&cmp);
        assert!(plain.contains("erratic: the allocator decides"), "{plain}");
        assert!(plain.contains("[erratic]"), "{plain}");

        let parsed: serde_json::Value =
            serde_json::from_str(&json(&cmp).expect("serialises")).expect("parses");
        assert_eq!(parsed["rows"][0]["erratic_reason"], "the allocator decides");
    }

    /// The rule a report quotes must be the rule it applied.
    #[test]
    fn the_stated_rule_carries_the_constants_it_is_built_from() {
        let rule = decision_rule();
        assert!(rule.contains("90% confidence"), "{rule}");
        assert!(rule.contains("5% for wall time"), "{rule}");
        assert!(rule.contains("10% for the peak resident set"), "{rule}");
        assert!(rule.contains("1% for allocation totals"), "{rule}");
        assert!(rule.contains("Fewer than 5 pairs"), "{rule}");
    }

    /// A run that paired ten replicates for one case and three for another has
    /// not taken ten replicates.
    #[test]
    fn the_header_reports_a_span_when_the_cases_disagree() {
        let mut cmp = comparison(
            vec![
                row("a", Verdict::NoChange, false),
                row("b", Verdict::NoChange, false),
            ],
            Vec::new(),
        );
        assert!(markdown(&cmp).contains("10 replicate(s)"));

        cmp.rows[1].analysis.replicates = 3;
        assert!(markdown(&cmp).contains("3-10 replicate(s)"));
    }

    #[test]
    fn waived_guards_are_announced_in_the_header() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.allowed = vec!["rustc".to_owned()];
        assert!(markdown(&cmp).contains("GUARDS WAIVED with --allow: rustc"));
        assert!(table(&cmp).contains("GUARDS WAIVED with --allow: rustc"));
        // A waiver is permission for a difference, not a difference. These legs
        // agree about `rustc`, so there is nothing to name.
        assert!(!markdown(&cmp).contains("differ on these guarded fields"));
    }

    /// `codegen` appears in no other header line, so a header that annotated the
    /// lines it already prints would not name it.
    #[test]
    fn a_waived_build_divergence_is_named_in_the_header() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.head.build.rustc = Some("rustc 1.95.0".to_owned());
        cmp.head.build.codegen = Some("deadbeefdeadbeef".to_owned());
        cmp.allowed = vec!["codegen".to_owned(), "rustc".to_owned()];

        for rendered in [markdown(&cmp), table(&cmp)] {
            assert!(
                rendered.contains("the two legs differ on these guarded fields:"),
                "{rendered}"
            );
            assert!(
                rendered.contains("codegen: base 'cafecafecafecafe' vs head 'deadbeefdeadbeef'"),
                "{rendered}"
            );
            assert!(
                rendered.contains("rustc: base 'rustc 1.94.0' vs head 'rustc 1.95.0'"),
                "{rendered}"
            );
        }
    }

    /// The host half of the merged map, which nothing else in the header would
    /// have compared.
    #[test]
    fn a_waived_host_divergence_is_named_in_the_header() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.head.host.cpu = "Apple M4".to_owned();
        cmp.allowed = vec!["host_cpu".to_owned()];

        assert!(
            markdown(&cmp).contains("host_cpu: base 'Apple M5 Max' vs head 'Apple M4'"),
            "{}",
            markdown(&cmp)
        );
    }

    /// A leg that recorded nothing for a field reads as empty rather than as
    /// absent, which is what the guard has always done with it.
    #[test]
    fn a_field_absent_from_one_leg_reads_as_empty() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.head.build.rustc = None;
        cmp.allowed = vec!["rustc".to_owned()];

        assert!(
            markdown(&cmp).contains("rustc: base 'rustc 1.94.0' vs head ''"),
            "{}",
            markdown(&cmp)
        );
    }

    #[test]
    fn the_terminal_view_states_the_same_rule_and_the_same_verdicts() {
        let cmp = comparison(
            vec![
                row("a", Verdict::Regressed, false),
                row("churn", Verdict::Improved, true),
            ],
            Vec::new(),
        );
        let rendered = table(&cmp);
        assert!(rendered.contains(&decision_rule()));
        assert!(rendered.contains("[worse]"));
        assert!(rendered.contains("Informational (erratic cases, never flagged)"));
        assert!(rendered.contains("All cases (2 rows)"));
    }

    #[test]
    fn the_json_view_carries_every_row_and_the_rule() {
        let cmp = comparison(vec![row("a", Verdict::Regressed, false)], Vec::new());
        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert_eq!(parsed["rows"].as_array().expect("rows").len(), 1);
        assert_eq!(parsed["rows"][0]["verdict"], "regressed");
        assert_eq!(parsed["rows"][0]["significant"], true);
        assert_eq!(parsed["replicates_min"], 10);
        assert_eq!(parsed["replicates_max"], 10);
        assert_eq!(parsed["decision_rule"], decision_rule());
        assert_eq!(parsed["schema"], super::REPORT_SCHEMA_VERSION);
        assert!((parsed["floors"]["peak_rss_bytes"].as_f64().expect("f64") - 0.10).abs() < 1e-12);
    }

    /// Driven from the metrics themselves rather than from the map: a metric
    /// that gains a floor in `floor_for` and is not keyed here would resolve to
    /// the wrong number for a consumer, which is the defect this map was fixed
    /// for.
    #[test]
    fn every_metric_resolves_to_the_floor_the_rule_applies() {
        use crate::record::{
            ALLOC_BYTES_PER_ITER, ALLOC_COUNT_PER_ITER, BYTES_PER_S, CPU_NS_PER_ITER,
            PEAK_RSS_BYTES, RECORDS_PER_S, WALL_NS_PER_ITER,
        };

        let cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        let floors = parsed["floors"].as_object().expect("floors");
        let default = floors["default"].as_f64().expect("f64");

        // Every metric a record can carry. A new one added to `record.rs`
        // without a decision about its floor fails to compile here.
        for metric in [
            WALL_NS_PER_ITER,
            CPU_NS_PER_ITER,
            RECORDS_PER_S,
            BYTES_PER_S,
            PEAK_RSS_BYTES,
            ALLOC_BYTES_PER_ITER,
            ALLOC_COUNT_PER_ITER,
        ] {
            let resolved = floors
                .get(metric)
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(default);
            assert!(
                (resolved - crate::stats::floor_for(metric)).abs() < 1e-12,
                "{metric} resolves to {resolved}, the rule applies {}",
                crate::stats::floor_for(metric)
            );
        }

        // And no key that is neither a metric nor the fallback: a stale one
        // resolves for nobody.
        for key in floors.keys() {
            assert!(
                key == "default"
                    || [
                        WALL_NS_PER_ITER,
                        CPU_NS_PER_ITER,
                        RECORDS_PER_S,
                        BYTES_PER_S,
                        PEAK_RSS_BYTES,
                        ALLOC_BYTES_PER_ITER,
                        ALLOC_COUNT_PER_ITER,
                    ]
                    .contains(&key.as_str()),
                "{key} is not a metric a row carries"
            );
        }
    }

    /// The key set a script pins against. A renamed or dropped field is a test
    /// failure here rather than a consumer discovering it, which is what
    /// [`REPORT_SCHEMA_VERSION`](super::REPORT_SCHEMA_VERSION) exists to make
    /// deliberate.
    #[test]
    fn the_json_report_is_the_shape_a_script_pins_against() {
        let mut cmp = comparison(
            vec![row("a", Verdict::Regressed, false)],
            vec![NotComparable {
                what: "spate-bench/selftest_wall/gone".to_owned(),
                why: "present only in the base leg".to_owned(),
                cause: Cause::Other,
            }],
        );
        cmp.head.build.rustc = Some("rustc 1.95.0".to_owned());
        cmp.allowed = vec!["rustc".to_owned()];

        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");

        let keys = |value: &serde_json::Value| -> Vec<String> {
            value
                .as_object()
                .expect("object")
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut top = keys(&parsed);
        top.sort();
        assert_eq!(
            top,
            [
                "allowed",
                "base",
                "confidence",
                "decision_rule",
                "divergences",
                "floors",
                "head",
                "min_replicates",
                "not_comparable",
                "replicates_max",
                "replicates_min",
                "rows",
                "schema",
            ]
        );

        let mut row_keys = keys(&parsed["rows"][0]);
        row_keys.sort();
        assert_eq!(
            row_keys,
            [
                "base_mean",
                "case",
                "ci_high",
                "ci_low",
                "delta",
                "erratic",
                "erratic_reason",
                "floor",
                "head_mean",
                "higher_is_better",
                "metric",
                "replicates",
                "significant",
                "unit",
                "verdict",
            ]
        );

        let mut leg_keys = keys(&parsed["base"]);
        leg_keys.sort();
        assert_eq!(leg_keys, ["build", "dir", "host", "priming", "records"]);

        let mut nc_keys = keys(&parsed["not_comparable"][0]);
        nc_keys.sort();
        assert_eq!(nc_keys, ["cause", "what", "why"]);

        let mut case_keys = keys(&parsed["rows"][0]["case"]);
        case_keys.sort();
        assert_eq!(case_keys, ["case", "crate", "target"]);
    }

    /// The machine format has to disclose a waived difference too. A consumer
    /// cannot derive it from the two fingerprints: they carry fields that differ
    /// on every run and are not guarded.
    #[test]
    fn a_waived_divergence_reaches_the_json() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.head.build.rustc = Some("rustc 1.95.0".to_owned());
        cmp.allowed = vec!["rustc".to_owned()];

        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");

        assert_eq!(parsed["divergences"].as_array().expect("array").len(), 1);
        assert_eq!(parsed["divergences"][0]["field"], "rustc");
        assert_eq!(parsed["divergences"][0]["base"], "rustc 1.94.0");
        assert_eq!(parsed["divergences"][0]["head"], "rustc 1.95.0");

        let agreeing = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        let text = json(&agreeing).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert!(
            parsed["divergences"].as_array().expect("array").is_empty(),
            "legs that agree have nothing to disclose"
        );
    }

    /// The JSON verdict is matched on rather than read, so it must not drift
    /// back to the phrasing the human formats use.
    #[test]
    fn the_json_verdict_is_a_token_rather_than_a_phrase() {
        let cmp = comparison(
            vec![
                row("a", Verdict::NoChange, false),
                row("b", Verdict::NoVerdict, false),
            ],
            Vec::new(),
        );
        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");

        assert_eq!(parsed["rows"][0]["verdict"], "no_change");
        assert_eq!(parsed["rows"][1]["verdict"], "no_verdict");
        for row in parsed["rows"].as_array().expect("rows") {
            let verdict = row["verdict"].as_str().expect("str");
            assert!(!verdict.contains(' '), "{verdict} reads as a phrase");
        }
    }

    #[test]
    fn every_cause_reaches_the_json_as_a_token() {
        let not_comparable = [Cause::DigestLeftOut, Cause::DigestCompared, Cause::Other]
            .into_iter()
            .map(|cause| NotComparable {
                what: "spate-bench/selftest_wall/a".to_owned(),
                why: "because".to_owned(),
                cause,
            })
            .collect();
        let cmp = comparison(vec![row("a", Verdict::NoChange, false)], not_comparable);
        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");

        assert_eq!(parsed["not_comparable"][0]["cause"], "digest_left_out");
        assert_eq!(parsed["not_comparable"][1]["cause"], "digest_compared");
        assert_eq!(parsed["not_comparable"][2]["cause"], "other");
    }

    #[test]
    fn digest_mismatches_are_counted_in_the_header() {
        let cmp = comparison(
            vec![row("a", Verdict::NoChange, false)],
            vec![NotComparable {
                what: "spate-bench/selftest_wall/b".to_owned(),
                why: "corpus digest differs — base [\"x\"], head [\"y\"]".to_owned(),
                cause: Cause::DigestLeftOut,
            }],
        );
        assert!(markdown(&cmp).contains("1 case(s) did NOT match"));
    }

    #[test]
    fn units_render_per_metric_rather_than_per_number() {
        let mut cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        cmp.rows[0].metric = "records_per_s".to_owned();
        cmp.rows[0].unit = "records/s".to_owned();
        cmp.rows[0].higher_is_better = true;
        cmp.rows[0].analysis.base_mean = 2_500_000.0;
        cmp.rows[0].analysis.head_mean = 2_600_000.0;
        assert!(markdown(&cmp).contains("2.50M rec/s"));

        cmp.rows[0].metric = "peak_rss_bytes".to_owned();
        cmp.rows[0].unit = "bytes".to_owned();
        cmp.rows[0].analysis.base_mean = 3.0 * 1024.0 * 1024.0;
        assert!(markdown(&cmp).contains("3.0 MiB"));
    }
}
