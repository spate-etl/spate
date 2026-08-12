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
//! [`REPORT_SCHEMA_VERSION`]. Its `verdict` and `cause` fields carry tokens
//! rather than the phrases the other two formats print, so a consumer matches on
//! a string that changes only when the version does.

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

/// Renders the terminal view.
#[must_use]
pub fn table(comparison: &Comparison) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", header_lines(comparison).join("\n"));

    let significant: Vec<&Row> = comparison.significant().collect();
    let _ = writeln!(out, "\nSignificant changes");
    if comparison.rows.is_empty() {
        let _ = writeln!(out, "  none — and nothing was comparable; see below");
    } else if significant.is_empty() {
        let _ = writeln!(out, "  none");
    } else {
        write_plain_rows(&mut out, &significant);
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
    let _ = writeln!(out, "\n### Significant changes\n");
    if comparison.rows.is_empty() {
        // Not "no metric cleared the rule": no metric was judged by it. The two
        // are the same table and opposite claims.
        let _ = writeln!(
            out,
            "None — and no metric was comparable at all, so the rule below was never \
             applied. See *Not comparable*."
        );
    } else if significant.is_empty() {
        let _ = writeln!(
            out,
            "None. No metric cleared both halves of the rule below."
        );
    } else {
        write_markdown_rows(&mut out, &significant);
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
        format!(
            "{} · {} · {} · {} · {}",
            replicate_span(comparison),
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
            "host {} · {} · {} core(s) · label {}",
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
struct NotComparableReport {
    what: String,
    why: String,
    cause: &'static str,
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use super::{decision_rule, json, markdown, table};
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
- 10 replicate(s) · rustc 1.94.0 · aarch64-apple-darwin · bench · default features
- host macos/aarch64 · Apple M5 Max · 16 core(s) · label local
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

    /// Every floor key is a metric a row can carry, apart from the fallback the
    /// rule genuinely has: a consumer holding a row's metric can resolve it.
    #[test]
    fn every_floor_is_keyed_by_a_metric_a_row_carries() {
        let cmp = comparison(vec![row("a", Verdict::NoChange, false)], Vec::new());
        let text = json(&cmp).expect("serialises");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        let floors = parsed["floors"].as_object().expect("floors");

        for metric in [
            crate::record::PEAK_RSS_BYTES,
            crate::record::ALLOC_BYTES_PER_ITER,
            crate::record::ALLOC_COUNT_PER_ITER,
        ] {
            let floor = floors[metric].as_f64().expect("f64");
            assert!(
                (floor - crate::stats::floor_for(metric)).abs() < 1e-12,
                "{metric} disagrees with the rule that applies it"
            );
        }
        let default = floors["default"].as_f64().expect("f64");
        assert!((default - crate::stats::floor_for(crate::record::WALL_NS_PER_ITER)).abs() < 1e-12);
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
