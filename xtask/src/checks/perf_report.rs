//! The counted-tier summaries rendered as a Markdown report, and the flag file
//! the performance label is synced from.
//!
//! The input is a concatenation of the `summary.json` files one or more CI runs
//! wrote under `target/gungraun` (`GUNGRAUN_SAVE_SUMMARY=json`), each a single
//! JSON object. Concatenation is the only merge operation.
//!
//! The output is one table of callgrind instruction counts and, when any
//! summary carries a DHAT profile, one of heap counts, with every other metric
//! behind a `<details>` fold. A named flag file receives the bare string `true`
//! or `false`, the two values `perf-label.yml` acts on. A metric moving is
//! still a success; the label is the only consequence.
//!
//! # Shard identity
//!
//! A summary carries neither the package nor the feature arm, so over a matrix
//! of (package, feature arm) nothing tells two rows apart. Each job stamps its
//! own summaries with one object this module reads and gungraun never writes:
//!
//! ```text
//! {"spate_shard": {"package": "spate-json",
//!                  "features": "simd",
//!                  "baseline": "main @ 0123456789ab",
//!                  "rustc": "rustc 1.98.1 (48a229cea 2026-09-01)"}}
//! ```
//!
//! `package` is the cargo package built. `features` labels the feature arm, and
//! the empty string or an absent key renders the bare package name. `baseline`
//! names what that job's old column holds; the empty string means the job
//! measured none and its rows read *no baseline*, and an absent key falls back
//! to the label the caller passes. `rustc` names the compiler both legs of that
//! job ran under, since codegen moves a count; every distinct value is named.
//!
//! Without the stamp, `package` falls back to the last segment of `package_dir`
//! and the feature arm stays blank. Two rows landing on one identity are
//! rendered with a warning naming the key they share.

use std::fmt;
use std::path::Path;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use crate::run::{Error, Outcome};

/// The summary schema the field paths below walk. Bumping the gungraun
/// dependency across a major version of the format changes those paths
/// silently, and the symptom is a report that went quietly blank, so a version
/// this report was not written against fails here.
const SCHEMA_VERSION: &str = "6";

/// Instruction counts flag on a percentage increase.
const IR_THRESHOLD_PCT: f64 = 5.0;

/// Heap blocks flag on an absolute move in either direction.
const BLOCKS_THRESHOLD_ABS: f64 = 1.0;

/// Peak heap bytes flag on a percentage increase.
const PEAK_THRESHOLD_PCT: f64 = 5.0;

/// What a row with no baseline at all reports in place of a delta.
const NO_BASELINE: &str = "*no baseline*";

/// What a bench with no comparison reports when its shard did measure a
/// baseline.
const NEW: &str = "*new*";

/// The column body an absent value renders as.
const DASH: &str = "—";

/// What an invocation this module cannot act on reports.
const USAGE: &str = "usage: cargo xtask bench report [--regressions-out FILE] <summaries> \
                     [<baseline-label>]. Needs a non-empty, readable summaries file";

/// The DHAT metrics the heap table carries, in the order it renders them.
const HEAP_METRICS: [&str; 2] = ["TotalBlocks", "AtTGmaxBytes"];

/// Renders one summaries file, writing the flag file when one is named.
///
/// `args` is the summaries path followed by the baseline label any row that
/// names no baseline of its own takes. An absent or empty label is `baseline`,
/// and anything past those two arguments is ignored.
pub(crate) fn report(explain: bool, regressions_out: Option<&str>, args: &[String]) -> Outcome {
    let summaries = args.first().map_or("", String::as_str);
    if explain {
        match regressions_out {
            Some(flag) => println!("(reads {summaries}, writes {flag})"),
            None => println!("(reads {summaries})"),
        }
        return Ok(());
    }
    if regressions_out.is_some_and(str::is_empty) {
        return Err(Error::msg("--regressions-out needs a file argument"));
    }
    // An empty label names no baseline. A row carrying none of its own would
    // then read *no baseline* off an argument.
    let base = args
        .get(1)
        .map(String::as_str)
        .filter(|label| !label.is_empty())
        .unwrap_or("baseline");
    let markdown = produce(Path::new(summaries), base, regressions_out.map(Path::new))?;
    println!("{markdown}");
    Ok(())
}

/// Renders one summaries file and writes the flag file when one is named.
///
/// The report is built whole before the flag file is touched, so a summary this
/// module cannot walk leaves no flag behind for the label to read.
fn produce(summaries: &Path, base: &str, flag: Option<&Path>) -> Result<String, Error> {
    if !readable(summaries) {
        return Err(Error::msg(USAGE));
    }
    let text = std::fs::read_to_string(summaries)
        .map_err(|e| Error::msg(format!("{}: {e}", summaries.display())))?;
    let rows = parse(&text).map_err(Error::msg)?;
    let markdown = render(&rows, base).map_err(Error::msg)?;
    if let Some(flag) = flag {
        let flagged = regressions(&rows).map_err(Error::msg)?;
        std::fs::write(flag, flag_text(flagged))
            .map_err(|e| Error::msg(format!("{}: {e}", flag.display())))?;
    }
    Ok(markdown)
}

/// Whether the path names a file this report can read and that holds anything.
fn readable(path: &Path) -> bool {
    std::fs::File::open(path).is_ok() && std::fs::metadata(path).is_ok_and(|m| m.len() > 0)
}

/// The flag file's whole contents: the bare boolean `perf-label.yml` matches on,
/// and a newline.
fn flag_text(flagged: bool) -> &'static str {
    if flagged { "true\n" } else { "false\n" }
}

// ── The summary shape ──────────────────────────────────────────────────

/// One `summary.json` object, with the fields this report walks.
#[derive(Deserialize)]
struct Summary {
    spate_shard: Option<Shard>,
    package_dir: Option<String>,
    module_path: String,
    id: Option<String>,
    profiles: Vec<Profile>,
}

/// The stamp a collecting job adds; see the module header.
#[derive(Deserialize)]
struct Shard {
    package: Option<String>,
    features: Option<String>,
    baseline: Option<String>,
    rustc: Option<String>,
}

#[derive(Deserialize)]
struct Profile {
    tool: Option<String>,
    summaries: Option<ProfileSummaries>,
}

#[derive(Deserialize)]
struct ProfileSummaries {
    total: Option<ProfileTotal>,
}

#[derive(Deserialize)]
struct ProfileTotal {
    summary: Option<ToolSummary>,
}

#[derive(Deserialize)]
struct ToolSummary {
    #[serde(rename = "Callgrind")]
    callgrind: Option<Metrics>,
    #[serde(rename = "Dhat")]
    dhat: Option<Metrics>,
}

/// One metric's two sides and the comparison between them.
#[derive(Deserialize)]
struct MetricDiff {
    metrics: Option<Sides>,
    diffs: Option<Diffs>,
}

/// Left is the new metric and Right the old one.
#[derive(Deserialize)]
struct Sides {
    #[serde(rename = "Both")]
    both: Option<Vec<Box<RawValue>>>,
    #[serde(rename = "Left")]
    left: Option<Box<RawValue>>,
    #[serde(rename = "Right")]
    right: Option<Box<RawValue>>,
}

/// Present only when both sides are. The percentage is a string so that it can
/// carry `inf`, `-inf` and `NaN`.
#[derive(Deserialize)]
struct Diffs {
    diff_pct: String,
}

/// A metric map in the order the summary carries it. The all-metrics fold
/// renders that order.
struct Metrics(Vec<(String, MetricDiff)>);

impl Metrics {
    fn get(&self, key: &str) -> Option<&MetricDiff> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

impl<'de> Deserialize<'de> for Metrics {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct Entries;

        impl<'de> Visitor<'de> for Entries {
            type Value = Metrics;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of metric name to comparison")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Metrics, A::Error> {
                let mut out: Vec<(String, MetricDiff)> = Vec::new();
                // A repeated key takes the later value at the earlier
                // position, as a JSON object parse does.
                while let Some((key, value)) = map.next_entry::<String, MetricDiff>()? {
                    match out.iter_mut().find(|(k, _)| *k == key) {
                        Some(slot) => slot.1 = value,
                        None => out.push((key, value)),
                    }
                }
                Ok(Metrics(out))
            }
        }

        de.deserialize_map(Entries)
    }
}

// ── Parsing ────────────────────────────────────────────────────────────

/// Reads every concatenated object, rejecting a schema version this report was
/// not written against.
fn parse(text: &str) -> Result<Vec<Summary>, String> {
    if let Some(drifted) = drifted_versions(text)? {
        return Err(format!(
            "gungraun summary schema is v{drifted}, this report is written against \
             v{SCHEMA_VERSION}. Update xtask/src/checks/perf_report.rs against the new \
             schema before trusting its output"
        ));
    }
    let mut out = Vec::new();
    for item in serde_json::Deserializer::from_str(text).into_iter::<Summary>() {
        out.push(item.map_err(|e| format!("cannot read a gungraun summary: {e}"))?);
    }
    Ok(out)
}

/// Every version string present that is not [`SCHEMA_VERSION`], sorted and
/// space-separated, or nothing when they all match.
fn drifted_versions(text: &str) -> Result<Option<String>, String> {
    let want = serde_json::Value::String(SCHEMA_VERSION.to_owned());
    let mut seen: Vec<String> = Vec::new();
    for item in serde_json::Deserializer::from_str(text).into_iter::<VersionOnly>() {
        let item = item.map_err(|e| format!("cannot read a gungraun summary: {e}"))?;
        let version = item.version.unwrap_or(serde_json::Value::Null);
        if version != want {
            seen.push(match version {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            });
        }
    }
    seen.sort();
    seen.dedup();
    Ok((!seen.is_empty()).then(|| seen.join(" ")))
}

#[derive(Deserialize)]
struct VersionOnly {
    version: Option<serde_json::Value>,
}

// ── Metric values ──────────────────────────────────────────────────────

/// The value a metric side carries, as the literal the summary holds.
///
/// A side is `{"Int": n}` or `{"Float": n}`; anything else is taken whole.
fn value(raw: &RawValue) -> Option<String> {
    #[derive(Deserialize)]
    struct Boxed {
        #[serde(rename = "Int")]
        int: Option<Box<RawValue>>,
        #[serde(rename = "Float")]
        float: Option<Box<RawValue>>,
    }

    let text = raw.get().trim();
    if !text.starts_with('{') {
        return (text != "null").then(|| text.to_owned());
    }
    let boxed: Boxed = serde_json::from_str(text).ok()?;
    boxed.int.or(boxed.float).map(|v| v.get().trim().to_owned())
}

/// The new side: the first of `Both`, else `Left`.
fn new_side(metric: &MetricDiff) -> Option<String> {
    let sides = metric.metrics.as_ref()?;
    match &sides.both {
        Some(both) => both.first().and_then(|v| value(v)),
        None => sides.left.as_ref().and_then(|v| value(v)),
    }
}

/// The old side: the second of `Both`, else `Right`.
fn old_side(metric: &MetricDiff) -> Option<String> {
    let sides = metric.metrics.as_ref()?;
    match &sides.both {
        Some(both) => both.get(1).and_then(|v| value(v)),
        None => sides.right.as_ref().and_then(|v| value(v)),
    }
}

/// A side as a number, for the absolute comparison.
fn as_number(side: &str) -> Result<f64, String> {
    side.parse::<f64>()
        .map_err(|_| format!("cannot read `{side}` as a number"))
}

/// A percentage string as a number.
fn to_number(pct: &str) -> Result<f64, String> {
    pct.parse::<f64>()
        .ok()
        .filter(|n| n.is_finite())
        .ok_or_else(|| format!("cannot read `{pct}` as a percentage"))
}

// ── Thresholds ─────────────────────────────────────────────────────────

/// The delta a metric reports, taking `absent` when it has no comparison.
///
/// `inf` and `-inf` render as infinity symbols and `NaN` as `n/a`, so neither
/// reaches the rounding below. Everything else rounds to two decimal places.
fn delta(metric: &MetricDiff, absent: &str) -> Result<String, String> {
    let Some(diffs) = &metric.diffs else {
        return Ok(absent.to_owned());
    };
    let pct = diffs.diff_pct.as_str();
    if pct.contains("inf") {
        return Ok(if pct.starts_with('-') {
            "-∞%"
        } else {
            "+∞%"
        }
        .to_owned());
    }
    if pct == "NaN" {
        return Ok("n/a".to_owned());
    }
    let rounded = (to_number(pct)? * 100.0).round() / 100.0;
    Ok(if rounded > 0.0 {
        format!("+{rounded}%")
    } else {
        format!("{rounded}%")
    })
}

/// Whether a metric increased by at least `threshold` percent.
///
/// `+inf` counts as flagged; `-inf` and `NaN` do not, and no comparison means
/// no baseline to have moved from.
fn flag_pct_increase(metric: &MetricDiff, threshold: f64) -> Result<bool, String> {
    let Some(diffs) = &metric.diffs else {
        return Ok(false);
    };
    let pct = diffs.diff_pct.as_str();
    if pct.contains("inf") {
        return Ok(!pct.starts_with('-'));
    }
    if pct == "NaN" {
        return Ok(false);
    }
    Ok(to_number(pct)? >= threshold)
}

/// Whether a metric moved by more than `threshold` in either direction, read
/// from the two sides. A one-block change on any baseline is the same fact.
fn flag_abs_delta(metric: &MetricDiff, threshold: f64) -> Result<bool, String> {
    let Some(old) = old_side(metric) else {
        return Ok(false);
    };
    let new = new_side(metric).ok_or_else(|| {
        format!("a metric with an old side of `{old}` carries no new side to compare")
    })?;
    Ok((as_number(&new)? - as_number(&old)?).abs() > threshold)
}

/// A delta, emboldened and annotated when it crossed its threshold.
fn marked(rendered: String, flagged: bool) -> String {
    if flagged {
        format!("**{rendered}** (over threshold)")
    } else {
        rendered
    }
}

// ── Row identity ───────────────────────────────────────────────────────

/// One summary's place in the report.
struct Row<'a> {
    summary: &'a Summary,
    shard: String,
    bench: String,
    baseline: String,
    rustc: String,
}

impl Row<'_> {
    fn key(&self) -> String {
        format!("{} — {}", self.shard, self.bench)
    }

    /// What a metric with no comparison reads as: a bench with no baseline at
    /// all where its shard produced none, and a new bench where it did.
    fn absent(&self) -> &'static str {
        if self.baseline.is_empty() {
            NO_BASELINE
        } else {
            NEW
        }
    }

    fn tool(&self, tool: Tool) -> Option<&Metrics> {
        tool_metrics(self.summary, tool)
    }
}

/// A valgrind tool, named as the summary names it and as the fold labels it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tool {
    Callgrind,
    Dhat,
}

impl Tool {
    fn name(self) -> &'static str {
        match self {
            Self::Callgrind => "Callgrind",
            Self::Dhat => "DHAT",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Callgrind => "callgrind",
            Self::Dhat => "DHAT",
        }
    }
}

/// One tool's metrics, from the first profile the summary carries for it.
///
/// Absent where the summary carries no such profile. The tables then carry an
/// explicit row for that bench and the report stays whole.
fn tool_metrics(summary: &Summary, tool: Tool) -> Option<&Metrics> {
    let profile = summary
        .profiles
        .iter()
        .find(|p| p.tool.as_deref() == Some(tool.name()))?;
    let reported = profile
        .summaries
        .as_ref()?
        .total
        .as_ref()?
        .summary
        .as_ref()?;
    match tool {
        Tool::Callgrind => reported.callgrind.as_ref(),
        Tool::Dhat => reported.dhat.as_ref(),
    }
}

fn row<'a>(summary: &'a Summary, base: &str) -> Row<'a> {
    let shard = summary.spate_shard.as_ref();
    let package = shard
        .and_then(|s| s.package.clone())
        .unwrap_or_else(|| package_from_dir(summary.package_dir.as_deref().unwrap_or("")));
    let features = shard.and_then(|s| s.features.clone()).unwrap_or_default();
    Row {
        summary,
        shard: if features.is_empty() {
            package
        } else {
            format!("{package} ({features})")
        },
        bench: bench_name(summary),
        baseline: shard
            .and_then(|s| s.baseline.clone())
            .unwrap_or_else(|| base.to_owned()),
        rustc: shard.and_then(|s| s.rustc.clone()).unwrap_or_default(),
    }
}

/// The last non-empty segment of a package directory.
fn package_from_dir(dir: &str) -> String {
    dir.split('/')
        .rfind(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_owned()
}

/// The module path past its bench-file segment, and the case name when there is
/// one.
fn bench_name(summary: &Summary) -> String {
    let mut out = summary
        .module_path
        .split("::")
        .skip(1)
        .collect::<Vec<_>>()
        .join("::");
    if let Some(id) = summary.id.as_deref().filter(|id| !id.is_empty()) {
        out.push(' ');
        out.push_str(id);
    }
    out
}

// ── Rendering ──────────────────────────────────────────────────────────

/// Renders every summary as one Markdown report.
///
/// Rows are sorted by shard and bench up front: jobs merge by concatenation and
/// artifacts download in no particular order, so input order is not an order.
fn render(summaries: &[Summary], base: &str) -> Result<String, String> {
    let mut rows: Vec<Row<'_>> = summaries.iter().map(|s| row(s, base)).collect();
    rows.sort_by(|a, b| (&a.shard, &a.bench).cmp(&(&b.shard, &b.bench)));

    let bases = unique(rows.iter().map(|r| r.baseline.clone()));
    // One column header can only name one baseline. Jobs that disagree take a
    // generic header and a legend below carrying the per-job labels.
    let header = match bases.as_slice() {
        [only] if only.is_empty() => "no baseline",
        [only] => only.as_str(),
        _ => "baseline",
    };
    let rustcs = unique(
        rows.iter()
            .map(|r| r.rustc.clone())
            .filter(|r| !r.is_empty()),
    );
    let dupes = duplicates(rows.iter().map(Row::key));

    let mut out: Vec<String> = vec!["## Instruction counts".to_owned(), String::new()];
    if !rustcs.is_empty() {
        out.push(format!("Built by {}.", rustcs.join(", ")));
        out.push(String::new());
    }
    out.push(format!(
        "Callgrind instructions (`Ir`) per bench: pull request vs {header}."
    ));
    out.push("Advisory: numbers never block a merge; a bench that stops running does.".to_owned());
    out.push("A **bold** delta crossed a provisional threshold and syncs the".to_owned());
    out.push("`affects-performance` label; nothing else happens.".to_owned());
    if !dupes.is_empty() {
        let named = dupes
            .iter()
            .map(|d| format!("`{d}`"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push(String::new());
        out.push(format!(
            "**Duplicate shard identity**: {named} appears more than once. Either two jobs \
             stamped themselves alike, or one package has two bench files whose group, bench \
             and case names coincide (the bench-file stem is not part of the name). Either way \
             the rows below cannot be told apart."
        ));
    }
    if bases.len() > 1 {
        out.push(String::new());
        out.push("Baseline per shard:".to_owned());
        for (baseline, shard) in legend(&rows) {
            let named = if baseline.is_empty() {
                "*none measured*"
            } else {
                baseline.as_str()
            };
            out.push(format!("- `{shard}` — {named}"));
        }
    }
    out.push(String::new());
    out.push(format!("| Shard | Bench | PR | {header} | Δ |"));
    out.push("| --- | --- | ---: | ---: | ---: |".to_owned());
    for r in &rows {
        out.push(match r.tool(Tool::Callgrind).and_then(|m| m.get("Ir")) {
            None => format!(
                "| {} | {} | {DASH} | {DASH} | *no callgrind profile* |",
                r.shard, r.bench
            ),
            Some(ir) => format!(
                "| {} | {} | {} | {} | {} |",
                r.shard,
                r.bench,
                cell(new_side(ir)),
                old_side(ir).unwrap_or_else(|| DASH.to_owned()),
                marked(
                    delta(ir, r.absent())?,
                    flag_pct_increase(ir, IR_THRESHOLD_PCT)?
                ),
            ),
        });
    }

    if rows.iter().any(|r| r.tool(Tool::Dhat).is_some()) {
        out.push(String::new());
        out.push("## Heap (DHAT)".to_owned());
        out.push(String::new());
        out.push(format!(
            "DHAT heap blocks and peak bytes per bench: pull request vs {header}."
        ));
        out.push(String::new());
        out.push(format!("| Shard | Bench | Metric | PR | {header} | Δ |"));
        out.push("| --- | --- | --- | ---: | ---: | ---: |".to_owned());
        for r in &rows {
            let Some(dhat) = r.tool(Tool::Dhat) else {
                continue;
            };
            for key in HEAP_METRICS {
                let Some(metric) = dhat.get(key) else {
                    continue;
                };
                let flagged = if key == "TotalBlocks" {
                    flag_abs_delta(metric, BLOCKS_THRESHOLD_ABS)?
                } else {
                    flag_pct_increase(metric, PEAK_THRESHOLD_PCT)?
                };
                out.push(format!(
                    "| {} | {} | {key} | {} | {} | {} |",
                    r.shard,
                    r.bench,
                    cell(new_side(metric)),
                    old_side(metric).unwrap_or_else(|| DASH.to_owned()),
                    marked(delta(metric, r.absent())?, flagged),
                ));
            }
        }
    }

    out.push(String::new());
    out.push("<details><summary>All metrics</summary>".to_owned());
    out.push(String::new());
    for tool in [Tool::Callgrind, Tool::Dhat] {
        for r in &rows {
            let Some(metrics) = r.tool(tool) else {
                continue;
            };
            out.push(format!("**{} — {}** — {}", r.shard, r.bench, tool.label()));
            out.push(String::new());
            out.push(format!("| Metric | PR | {header} | Δ |"));
            out.push("| --- | ---: | ---: | ---: |".to_owned());
            for (key, metric) in &metrics.0 {
                out.push(format!(
                    "| {key} | {} | {} | {} |",
                    cell(new_side(metric)),
                    old_side(metric).unwrap_or_else(|| DASH.to_owned()),
                    delta(metric, r.absent())?,
                ));
            }
            out.push(String::new());
        }
    }
    out.push("</details>".to_owned());
    Ok(out.join("\n"))
}

/// The PR column, which names an absent value where the baseline column would
/// dash it.
fn cell(side: Option<String>) -> String {
    side.unwrap_or_else(|| "null".to_owned())
}

/// Sorted, with repeats collapsed.
fn unique(items: impl Iterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = items.collect();
    out.sort();
    out.dedup();
    out
}

/// Every key carried by more than one row, sorted.
fn duplicates(keys: impl Iterator<Item = String>) -> Vec<String> {
    let mut all: Vec<String> = keys.collect();
    all.sort();
    let mut out = Vec::new();
    for window in all.chunk_by(|a, b| a == b) {
        if window.len() > 1 {
            out.push(window[0].clone());
        }
    }
    out
}

/// The per-shard baseline legend, ordered by baseline and then by shard.
fn legend(rows: &[Row<'_>]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.baseline.clone(), r.shard.clone()))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Whether any metric this report flags crossed its threshold.
///
/// A second pass over the same summaries with the same definitions, so the flag
/// a row shows and the flag the label workflow reads cannot disagree.
fn regressions(summaries: &[Summary]) -> Result<bool, String> {
    let mut any = false;
    for summary in summaries {
        if let Some(ir) = tool_metrics(summary, Tool::Callgrind).and_then(|m| m.get("Ir")) {
            any |= flag_pct_increase(ir, IR_THRESHOLD_PCT)?;
        }
        if let Some(dhat) = tool_metrics(summary, Tool::Dhat) {
            if let Some(blocks) = dhat.get("TotalBlocks") {
                any |= flag_abs_delta(blocks, BLOCKS_THRESHOLD_ABS)?;
            }
            if let Some(peak) = dhat.get("AtTGmaxBytes") {
                any |= flag_pct_increase(peak, PEAK_THRESHOLD_PCT)?;
            }
        }
    }
    Ok(any)
}

// ── The self-test ──────────────────────────────────────────────────────

/// One summary carrying an instruction count over its threshold and a DHAT
/// profile over both of its own.
const HOT: &str = r#"{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":107000},{"Int":100000}]},"diffs":{"diff_pct":"7.0"}}}}}}},{"tool":"DHAT","summaries":{"total":{"summary":{"Dhat":{"TotalBlocks":{"metrics":{"Both":[{"Int":38},{"Int":40}]},"diffs":{"diff_pct":"-5.0"}},"AtTGmaxBytes":{"metrics":{"Both":[{"Int":4342},{"Int":4096}]},"diffs":{"diff_pct":"6.0"}}}}}}}]}"#;

/// One summary that moved nothing past a threshold.
const QUIET: &str = r#"{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}"#;

/// Three jobs of one matrix run. The first two are the same package and the
/// same bench built two ways, indistinguishable without the stamp; the third
/// measured no baseline.
const MATRIX: &str = concat!(
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"nested_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":211000}}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":61000},{"Int":60000}]},"diffs":{"diff_pct":"1.67"}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-core","features":"default","baseline":""},"package_dir":"/w/crates/spate-core","module_path":"chain_gungraun::chain::forward","id":"one_stage","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":50000}}}}}}}}]}"#,
    "\n",
);

/// Two jobs that agree on the compiler.
const STAMPED: &str = concat!(
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"","rustc":"rustc 1.98.1 (48a229cea 2026-09-01)"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":100000}}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-core","features":"default","baseline":"","rustc":"rustc 1.98.1 (48a229cea 2026-09-01)"},"package_dir":"/w/crates/spate-core","module_path":"chain_gungraun::chain::forward","id":"one_stage","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":50000}}}}}}}}]}"#,
    "\n",
);

/// Two jobs that disagree about the compiler. Every distinct value is named, so
/// a split matrix shows as one.
const SPLIT_RUSTC: &str = concat!(
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"","rustc":"rustc 1.98.1 (48a229cea 2026-09-01)"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":100000}}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-core","features":"default","baseline":"","rustc":"rustc 1.99.0 (aaaaaaaaa 2026-10-13)"},"package_dir":"/w/crates/spate-core","module_path":"chain_gungraun::chain::forward","id":"one_stage","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":50000}}}}}}}}]}"#,
    "\n",
);

/// Two jobs that stamped themselves identically, which is an aggregation bug
/// the report has to name.
const COLLIDE: &str = concat!(
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":61000},{"Int":60000}]},"diffs":{"diff_pct":"1.67"}}}}}}}]}"#,
    "\n",
    r#"{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":62000},{"Int":60000}]},"diffs":{"diff_pct":"3.33"}}}}}}}]}"#,
    "\n",
);

/// The write side of the `perf-label.yml` contract, executable: the flag file
/// must hold the bare `true` or `false` the label workflow's `case` accepts, and
/// threshold crossings must mark rows.
///
/// The fixtures pin schema v6, so bumping [`SCHEMA_VERSION`] fails at the
/// version gate until they are rebuilt.
pub(crate) fn self_test(explain: bool) -> Outcome {
    if explain {
        println!("(renders the fixtures this check carries)");
        return Ok(());
    }
    let dir = std::env::temp_dir().join(format!("spate-xtask-perf-report-{}", std::process::id()));
    let outcome = run_self_test(&dir);
    drop(std::fs::remove_dir_all(&dir));
    outcome?;
    println!(
        "perf-report: self-test ok: the flag file is the bare boolean perf-label.yml parses, \
         markers track the thresholds, and merged jobs keep their shard identity"
    );
    Ok(())
}

fn run_self_test(dir: &Path) -> Outcome {
    std::fs::create_dir_all(dir).map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
    let fail = |what: &str| Error::msg(format!("perf-report --self-test: {what}"));

    // Asserts exactly `want` lines carry `needle`, by count: a merged report's
    // failure mode is a row appearing twice, which presence alone cannot see.
    let count_is = |report: &str, want: usize, needle: &str, desc: &str| -> Outcome {
        let got = report.lines().filter(|l| l.contains(needle)).count();
        if got == want {
            return Ok(());
        }
        Err(fail(&format!(
            "{desc}: expected {want} line(s) matching '{needle}', found {got}"
        )))
    };

    let render_fixture = |name: &str, body: &str| -> Result<(String, String), Error> {
        let summaries = dir.join(format!("{name}.jsonl"));
        let flag = dir.join("flag");
        std::fs::write(&summaries, body)
            .map_err(|e| Error::msg(format!("{}: {e}", summaries.display())))?;
        let report = produce(&summaries, "self-test", Some(&flag))?;
        let held = std::fs::read_to_string(&flag)
            .map_err(|e| Error::msg(format!("{}: {e}", flag.display())))?;
        Ok((report, held))
    };

    let (report, flag) = render_fixture("hot", HOT)?;
    if flag != "true\n" {
        return Err(fail(&format!(
            "hot fixture: flag file holds '{}', not the bare string 'true'",
            flag.trim_end()
        )));
    }
    if !report.contains("(over threshold)") {
        return Err(fail(
            "hot fixture: no row carries the over-threshold marker",
        ));
    }
    // The unstamped path a single-job run takes: the package still has to be
    // named, from `package_dir`, with no feature arm invented.
    count_is(
        &report,
        1,
        "| spate-json | decode::decode_value flat_record | 107000 |",
        "hot fixture",
    )?;

    let (report, flag) = render_fixture("quiet", QUIET)?;
    if flag != "false\n" {
        return Err(fail(&format!(
            "quiet fixture: flag file holds '{}', not the bare string 'false'",
            flag.trim_end()
        )));
    }
    if report.contains("(over threshold)") {
        return Err(fail("quiet fixture: a row is marked over threshold"));
    }

    let (report, flag) = render_fixture("matrix", MATRIX)?;
    if flag != "false\n" {
        return Err(fail(&format!(
            "matrix fixture: flag file holds '{}', not the bare string 'false'",
            flag.trim_end()
        )));
    }
    // One bench, two feature arms, two rows that name which is which.
    count_is(
        &report,
        1,
        "| spate-json (default) | decode::decode_value flat_record | 100000 |",
        "matrix fixture",
    )?;
    count_is(
        &report,
        1,
        "| spate-json (simd) | decode::decode_value flat_record | 61000 |",
        "matrix fixture",
    )?;
    // A job whose merge-base leg failed must not read like a bench that is new,
    // and a new bench in a job that did measure one must not read like a
    // missing baseline.
    count_is(
        &report,
        1,
        "| spate-core (default) | chain::forward one_stage | 50000 | — | *no baseline* |",
        "matrix fixture",
    )?;
    count_is(
        &report,
        1,
        "| spate-json (default) | decode::decode_value nested_record | 211000 | — | *new* |",
        "matrix fixture",
    )?;
    if !report.contains("Baseline per shard") {
        return Err(fail(
            "matrix fixture: shards disagree about the baseline and no legend says so",
        ));
    }
    if report.contains("Duplicate shard identity") {
        return Err(fail(
            "matrix fixture: distinct shards were reported as a collision",
        ));
    }
    // An unstamped run names no compiler at all.
    count_is(&report, 0, "Built by", "matrix fixture")?;

    // The compiler both legs ran under, named once when every shard agrees.
    let (report, _) = render_fixture("stamped", STAMPED)?;
    count_is(
        &report,
        1,
        "Built by rustc 1.98.1 (48a229cea 2026-09-01).",
        "stamped fixture",
    )?;

    let (report, _) = render_fixture("split-rustc", SPLIT_RUSTC)?;
    count_is(
        &report,
        1,
        "Built by rustc 1.98.1 (48a229cea 2026-09-01), rustc 1.99.0 (aaaaaaaaa 2026-10-13).",
        "split-rustc fixture",
    )?;

    let (report, _) = render_fixture("collide", COLLIDE)?;
    if !report.contains("Duplicate shard identity") {
        return Err(fail(
            "collision fixture: two identically stamped jobs are reported as one shard, unremarked",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
