//! What the renderer makes of a summary: the tables, the thresholds and the
//! shard identity, as string tables over the pure halves.

use super::*;

/// One summary object, with `body` spliced in after the version.
fn summary(body: &str) -> String {
    format!(r#"{{"version":"6",{body}}}"#)
}

/// One callgrind profile holding the metric entries named, in that order.
fn callgrind_profile(entries: &str) -> String {
    format!(
        r#"{{"tool":"Callgrind","summaries":{{"total":{{"summary":{{"Callgrind":{{{entries}}}}}}}}}}}"#
    )
}

/// One DHAT profile holding the metric entries named, in that order.
fn dhat_profile(entries: &str) -> String {
    format!(r#"{{"tool":"DHAT","summaries":{{"total":{{"summary":{{"Dhat":{{{entries}}}}}}}}}}}"#)
}

/// A profile list.
fn profiles(each: &[String]) -> String {
    format!(r#""profiles":[{}]"#, each.join(","))
}

/// A profile list holding one callgrind `Ir` metric.
fn callgrind(metrics: &str) -> String {
    profiles(&[callgrind_profile(&format!(r#""Ir":{metrics}"#))])
}

/// A `Left` side alone: a bench with no comparison.
fn left(n: u32) -> String {
    format!(r#"{{"metrics":{{"Left":{{"Int":{n}}}}}}}"#)
}

/// A `Both` comparison with a percentage.
fn both(new: &str, old: &str, pct: &str) -> String {
    format!(
        r#"{{"metrics":{{"Both":[{{"Int":{new}}},{{"Int":{old}}}]}},"diffs":{{"diff_pct":"{pct}"}}}}"#
    )
}

/// An unstamped summary of `spate-json` carrying one callgrind `Ir` metric.
fn ir(metrics: &str, shard: Option<&str>) -> String {
    let stamp = shard.map_or(String::new(), |s| format!(r#""spate_shard":{s},"#));
    summary(&format!(
        r#"{stamp}"package_dir":"/w/crates/spate-json","module_path":"b_gungraun::g::case","id":"one",{}"#,
        callgrind(metrics)
    ))
}

/// One stamped summary with no comparison, as a line of a merged file.
fn stamped(shard: &str, module_path: &str, id: &str, n: u32) -> String {
    format!(
        "{}\n",
        summary(&format!(
            r#""spate_shard":{shard},"module_path":"{module_path}","id":"{id}",{}"#,
            callgrind(&left(n))
        ))
    )
}

/// A summary of `p` carrying the profile list named.
fn p_summary(list: &str) -> String {
    summary(&format!(
        r#""package_dir":"/w/c/p","module_path":"b_gungraun::g::case","id":"one",{list}"#
    ))
}

/// A summary of `p` carrying a DHAT profile alone.
fn dhat_summary(metrics: &str) -> String {
    p_summary(&profiles(&[dhat_profile(metrics)]))
}

/// The diagnostic a summary this report cannot take reports.
fn parse_err(text: &str) -> String {
    parse(text).err().expect("the summary is rejected")
}

fn rendered(text: &str, base: &str) -> String {
    render(&parse(text).unwrap(), base).unwrap()
}

/// The rows of the first all-metrics fold table.
fn fold_rows(report: &str) -> Vec<&str> {
    report
        .lines()
        .skip_while(|l| !l.starts_with("| --- | ---: | ---: |"))
        .skip(1)
        .take_while(|l| l.starts_with("| "))
        .collect()
}

/// The line of the instruction-count table naming a bench, without the leading
/// header rows.
fn ir_rows(report: &str) -> Vec<&str> {
    report
        .lines()
        .skip_while(|l| !l.starts_with("| --- | --- | ---: |"))
        .skip(1)
        .take_while(|l| l.starts_with("| "))
        .collect()
}

// ── The whole report ───────────────────────────────────────────────────

/// The report a single unstamped job renders, whole. Every other case asserts
/// against a slice of this shape.
#[test]
fn one_unstamped_job_renders_the_whole_report() {
    let report = rendered(&ir(&both("107000", "100000", "7.0"), None), "baseline");
    assert_eq!(
        report,
        "\
## Instruction counts

Callgrind instructions (`Ir`) per bench: pull request vs baseline.
Advisory: numbers never block a merge; a bench that stops running does.
A **bold** delta crossed a provisional threshold and syncs the
`affects-performance` label; nothing else happens.

| Shard | Bench | PR | baseline | Δ |
| --- | --- | ---: | ---: | ---: |
| spate-json | g::case one | 107000 | 100000 | **+7%** (over threshold) |

<details><summary>All metrics</summary>

**spate-json — g::case one** — callgrind

| Metric | PR | baseline | Δ |
| --- | ---: | ---: | ---: |
| Ir | 107000 | 100000 | +7% |

</details>"
    );
}

/// A DHAT profile adds its own table between the counts and the fold, and a
/// second fold block of its own.
#[test]
fn a_dhat_profile_renders_a_heap_table_and_its_own_fold_block() {
    let text = summary(
        r#""package_dir":"/w/crates/spate-json","module_path":"b_gungraun::g::case","id":"one","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100},{"Int":100}]},"diffs":{"diff_pct":"0.0"}}}}}}},{"tool":"DHAT","summaries":{"total":{"summary":{"Dhat":{"TotalBlocks":{"metrics":{"Both":[{"Int":38},{"Int":40}]},"diffs":{"diff_pct":"-5.0"}},"AtTGmaxBytes":{"metrics":{"Both":[{"Int":4342},{"Int":4096}]},"diffs":{"diff_pct":"6.0"}}}}}}}]"#,
    );
    assert_eq!(
        rendered(&text, "base"),
        "\
## Instruction counts

Callgrind instructions (`Ir`) per bench: pull request vs base.
Advisory: numbers never block a merge; a bench that stops running does.
A **bold** delta crossed a provisional threshold and syncs the
`affects-performance` label; nothing else happens.

| Shard | Bench | PR | base | Δ |
| --- | --- | ---: | ---: | ---: |
| spate-json | g::case one | 100 | 100 | 0% |

## Heap (DHAT)

DHAT heap blocks and peak bytes per bench: pull request vs base.

| Shard | Bench | Metric | PR | base | Δ |
| --- | --- | --- | ---: | ---: | ---: |
| spate-json | g::case one | TotalBlocks | 38 | 40 | **-5%** (over threshold) |
| spate-json | g::case one | AtTGmaxBytes | 4342 | 4096 | **+6%** (over threshold) |

<details><summary>All metrics</summary>

**spate-json — g::case one** — callgrind

| Metric | PR | base | Δ |
| --- | ---: | ---: | ---: |
| Ir | 100 | 100 | 0% |

**spate-json — g::case one** — DHAT

| Metric | PR | base | Δ |
| --- | ---: | ---: | ---: |
| TotalBlocks | 38 | 40 | -5% |
| AtTGmaxBytes | 4342 | 4096 | +6% |

</details>"
    );
}

/// No summary at all still renders the header, an empty table and an empty
/// fold, so an empty download does not look like a crash.
#[test]
fn no_summaries_render_an_empty_table() {
    assert_eq!(
        rendered("\n", "baseline"),
        "\
## Instruction counts

Callgrind instructions (`Ir`) per bench: pull request vs baseline.
Advisory: numbers never block a merge; a bench that stops running does.
A **bold** delta crossed a provisional threshold and syncs the
`affects-performance` label; nothing else happens.

| Shard | Bench | PR | baseline | Δ |
| --- | --- | ---: | ---: | ---: |

<details><summary>All metrics</summary>

</details>"
    );
}

/// A report never ends in a blank line, so the step summary it is appended to
/// keeps one gap between sections.
#[test]
fn a_report_ends_at_its_closing_tag() {
    for text in ["\n", &ir(&both("1", "1", "0.0"), None)] {
        let report = rendered(text, "baseline");
        assert!(report.ends_with("\n</details>"), "{report}");
    }
}

// ── Deltas ─────────────────────────────────────────────────────────────

/// The delta column, over the percentage strings a summary can carry.
#[test]
fn a_percentage_renders_rounded_to_two_places() {
    let table = [
        ("7.0", "**+7%** (over threshold)"),
        ("1.0", "+1%"),
        ("0.0", "0%"),
        ("-5.0", "-5%"),
        ("1.67", "+1.67%"),
        ("1.23456789", "+1.23%"),
        ("7.005", "**+7.01%** (over threshold)"),
        ("-0.001", "-0%"),
        ("12", "**+12%** (over threshold)"),
        ("1e3", "**+1000%** (over threshold)"),
        ("inf", "**+∞%** (over threshold)"),
        ("-inf", "-∞%"),
        ("infinite", "**+∞%** (over threshold)"),
        ("NaN", "n/a"),
    ];
    for (pct, want) in table {
        let report = rendered(&ir(&both("105000", "100000", pct), None), "baseline");
        assert_eq!(
            ir_rows(&report),
            [format!(
                "| spate-json | g::case one | 105000 | 100000 | {want} |"
            )],
            "{pct}"
        );
    }
}

/// The instruction threshold is an increase of at least five percent, read from
/// the unrounded percentage, so a value that rounds up to it does not cross it.
#[test]
fn the_instruction_threshold_is_read_before_rounding() {
    let table = [
        ("4.996", "+5%", false),
        ("4.999", "+5%", false),
        ("5.0", "+5%", true),
        ("5.001", "+5%", true),
    ];
    for (pct, shown, flagged) in table {
        let text = ir(&both("105000", "100000", pct), None);
        let want = if flagged {
            format!("**{shown}** (over threshold)")
        } else {
            shown.to_owned()
        };
        assert_eq!(
            ir_rows(&rendered(&text, "baseline")),
            [format!(
                "| spate-json | g::case one | 105000 | 100000 | {want} |"
            )],
            "{pct}"
        );
        assert_eq!(
            regressions(&parse(&text).unwrap()).unwrap(),
            flagged,
            "{pct}"
        );
    }
}

/// The heap-block threshold is an absolute move of more than one block, in
/// either direction, taken from the two sides.
#[test]
fn the_heap_block_threshold_is_an_absolute_move_of_more_than_one() {
    for (new, old, flagged) in [
        (40, 40, false),
        (41, 40, false),
        (42, 40, true),
        (38, 40, true),
    ] {
        let text = dhat_summary(&format!(
            r#""TotalBlocks":{}"#,
            both(&new.to_string(), &old.to_string(), "0.0")
        ));
        assert_eq!(
            regressions(&parse(&text).unwrap()).unwrap(),
            flagged,
            "{new} vs {old}"
        );
    }
}

/// The peak-bytes threshold is a percentage increase like the instruction one.
#[test]
fn the_peak_bytes_threshold_is_a_percentage_increase() {
    for (pct, flagged) in [("4.99", false), ("5.0", true), ("-9.0", false)] {
        let text = dhat_summary(&format!(r#""AtTGmaxBytes":{}"#, both("4200", "4096", pct)));
        assert_eq!(
            regressions(&parse(&text).unwrap()).unwrap(),
            flagged,
            "{pct}"
        );
    }
}

/// A metric with no comparison never flags, whether or not its shard measured a
/// baseline.
#[test]
fn a_metric_with_no_comparison_never_flags() {
    for shard in [None, Some(r#"{"package":"p","baseline":""}"#)] {
        let text = ir(r#"{"metrics":{"Left":{"Int":211000}}}"#, shard);
        assert!(!regressions(&parse(&text).unwrap()).unwrap(), "{shard:?}");
    }
}

// ── Sides ──────────────────────────────────────────────────────────────

/// The new side is the first of `Both` and the old side the second, and a
/// missing old side dashes its column while a missing new one is named.
#[test]
fn the_sides_render_from_both_left_and_right() {
    let table = [
        (r#"{"metrics":{"Both":[{"Int":7},{"Int":3}]}}"#, "7", "3"),
        (r#"{"metrics":{"Left":{"Int":7}}}"#, "7", "—"),
        (r#"{"metrics":{"Right":{"Int":3}}}"#, "null", "3"),
        (r#"{"metrics":{"Left":{"Float":0.25}}}"#, "0.25", "—"),
        (r#"{"metrics":{"Left":{"Int":0}}}"#, "0", "—"),
    ];
    for (metrics, new, old) in table {
        let report = rendered(&ir(metrics, None), "baseline");
        assert_eq!(
            ir_rows(&report),
            [format!(
                "| spate-json | g::case one | {new} | {old} | *new* |"
            )],
            "{metrics}"
        );
    }
}

/// A metric value renders as the literal the summary holds, so a count is not
/// reformatted on its way through.
#[test]
fn a_metric_renders_as_the_literal_the_summary_holds() {
    let table = [
        ("{\"Int\":4342}", "4342"),
        ("{\"Float\":4342.0}", "4342.0"),
        ("{\"Float\":0.5000}", "0.5000"),
        ("{\"Float\":-0.0}", "-0.0"),
        ("{\"Int\":18446744073709551615}", "18446744073709551615"),
    ];
    for (literal, want) in table {
        let report = rendered(
            &ir(&format!(r#"{{"metrics":{{"Left":{literal}}}}}"#), None),
            "b",
        );
        assert_eq!(
            ir_rows(&report),
            [format!("| spate-json | g::case one | {want} | — | *new* |")],
            "{literal}"
        );
    }
}

// ── Shard identity ─────────────────────────────────────────────────────

/// The shard column: the stamp when there is one, the last segment of the
/// package directory when there is not, and `unknown` when there is neither.
#[test]
fn the_shard_column_names_the_package_and_the_feature_arm() {
    let table = [
        (
            Some(r#"{"package":"p","features":"simd"}"#),
            "/w/c/d",
            "p (simd)",
        ),
        (Some(r#"{"package":"p","features":""}"#), "/w/c/d", "p"),
        (Some(r#"{"package":"p"}"#), "/w/c/d", "p"),
        (Some(r#"{"features":"simd"}"#), "/w/c/d", "d (simd)"),
        (Some("{}"), "/w/c/d", "d"),
        (None, "/w/crates/spate-json", "spate-json"),
        (None, "/w/crates/spate-json/", "spate-json"),
        (None, "", "unknown"),
        (None, "/", "unknown"),
    ];
    for (shard, dir, want) in table {
        let stamp = shard.map_or(String::new(), |s| format!(r#""spate_shard":{s},"#));
        let text = summary(&format!(
            r#"{stamp}"package_dir":"{dir}","module_path":"b_gungraun::g::case","id":"one",{}"#,
            callgrind(&left(1))
        ));
        assert_eq!(
            ir_rows(&rendered(&text, "baseline")),
            [format!("| {want} | g::case one | 1 | — | *new* |")],
            "{shard:?} {dir}"
        );
    }
}

/// A package directory the stamp does not override is read even when the stamp
/// is present but names no package.
#[test]
fn an_empty_stamped_package_is_the_shard_name() {
    let text = summary(
        r#""spate_shard":{"package":"","features":""},"package_dir":"/w/c/d","module_path":"b_gungraun::g::case","id":"one","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":1}}}}}}}}]"#,
    );
    assert_eq!(
        ir_rows(&rendered(&text, "baseline")),
        ["|  | g::case one | 1 | — | *new* |"]
    );
}

/// The bench column drops the bench-file segment of the module path and appends
/// the case name when there is one.
#[test]
fn the_bench_column_drops_the_bench_file_segment() {
    let table = [
        ("b_gungraun::g::case", r#","id":"one""#, "g::case one"),
        ("b_gungraun::g::case", r#","id":"""#, "g::case"),
        ("b_gungraun::g::case", "", "g::case"),
        ("only_one", r#","id":"one""#, " one"),
        ("only_one", "", ""),
        ("a::b", r#","id":"one""#, "b one"),
    ];
    for (path, id, want) in table {
        let text = summary(&format!(
            r#""package_dir":"/w/c/p","module_path":"{path}"{id},{}"#,
            callgrind(&left(1))
        ));
        assert_eq!(
            ir_rows(&rendered(&text, "baseline")),
            [format!("| p | {want} | 1 | — | *new* |")],
            "{path}{id}"
        );
    }
}

// ── Baselines ──────────────────────────────────────────────────────────

/// A shard that measured no baseline reads *no baseline* and one that did
/// reads *new*, so a failed merge-base leg and a new bench are told apart.
#[test]
fn a_shard_with_no_baseline_is_told_apart_from_a_new_bench() {
    let table = [
        (r#""baseline":"main @ ab""#, "*new*"),
        (r#""baseline":"""#, "*no baseline*"),
    ];
    for (baseline, want) in table {
        let text = ir(
            r#"{"metrics":{"Left":{"Int":1}}}"#,
            Some(&format!(r#"{{"package":"p",{baseline}}}"#)),
        );
        assert_eq!(
            ir_rows(&rendered(&text, "fallback")),
            [format!("| p | g::case one | 1 | — | {want} |")],
            "{baseline}"
        );
    }
}

/// A shard naming no baseline at all falls back to the label the caller passes,
/// so an empty label reads as no baseline.
#[test]
fn an_unstamped_baseline_falls_back_to_the_label() {
    for (label, want) in [("fallback", "*new*"), ("", "*no baseline*")] {
        let text = ir(
            r#"{"metrics":{"Left":{"Int":1}}}"#,
            Some(r#"{"package":"p"}"#),
        );
        assert_eq!(
            ir_rows(&rendered(&text, label)),
            [format!("| p | g::case one | 1 | — | {want} |")],
            "{label}"
        );
    }
}

/// One baseline names the column. Several take a generic header and a legend
/// ordered by baseline and then by shard.
#[test]
fn shards_that_disagree_take_a_legend() {
    // `p1` carries two benches, which the legend names once.
    let rows: String = [
        (r#"{"package":"p3","baseline":"zzz"}"#, "one", 1),
        (r#"{"package":"p1","baseline":"zzz"}"#, "one", 2),
        (r#"{"package":"p1","baseline":"zzz"}"#, "two", 5),
        (r#"{"package":"p2","baseline":"aaa"}"#, "one", 3),
        (r#"{"package":"p0","baseline":""}"#, "one", 4),
    ]
    .iter()
    .map(|(shard, id, n)| stamped(shard, "b_gungraun::g::case", id, *n))
    .collect();
    let report = rendered(&rows, "baseline");
    let legend: Vec<&str> = report
        .lines()
        .skip_while(|l| *l != "Baseline per shard:")
        .skip(1)
        .take_while(|l| l.starts_with("- "))
        .collect();
    assert_eq!(
        legend,
        [
            "- `p0` — *none measured*",
            "- `p2` — aaa",
            "- `p1` — zzz",
            "- `p3` — zzz",
        ]
    );
    assert!(
        report.contains("| Shard | Bench | PR | baseline | Δ |"),
        "{report}"
    );
}

/// One shard measuring no baseline names the column so, since a header naming
/// the fallback label would claim a comparison that was never made.
#[test]
fn a_single_unmeasured_baseline_names_the_column() {
    let text = ir(
        r#"{"metrics":{"Left":{"Int":1}}}"#,
        Some(r#"{"package":"p","baseline":""}"#),
    );
    let report = rendered(&text, "fallback");
    assert!(
        report.contains("| Shard | Bench | PR | no baseline | Δ |"),
        "{report}"
    );
    assert!(!report.contains("Baseline per shard"), "{report}");
}

// ── Merge ──────────────────────────────────────────────────────────────

/// Rows sort by shard and then bench whatever order the summaries arrive in,
/// and the sort is by codepoint.
#[test]
fn rows_sort_by_shard_then_bench() {
    let rows: String = [
        (
            r#"{"package":"zeta","baseline":"b"}"#,
            "m_gungraun::z::z",
            "a",
            3,
        ),
        (
            r#"{"package":"alpha","features":"simd","baseline":"b"}"#,
            "m_gungraun::a::a",
            "b",
            1,
        ),
        (
            r#"{"package":"alpha","baseline":"b"}"#,
            "m_gungraun::a::a",
            "a",
            2,
        ),
        (
            r#"{"package":"Ålpha","baseline":"b"}"#,
            "m_gungraun::a::a",
            "a",
            4,
        ),
        (
            r#"{"package":"alpha","baseline":"b"}"#,
            "m_gungraun::a::a",
            "A",
            5,
        ),
    ]
    .iter()
    .map(|(shard, path, id, n)| stamped(shard, path, id, *n))
    .collect();
    assert_eq!(
        ir_rows(&rendered(&rows, "baseline")),
        [
            "| alpha | a::a A | 5 | — | *new* |",
            "| alpha | a::a a | 2 | — | *new* |",
            "| alpha (simd) | a::a b | 1 | — | *new* |",
            "| zeta | z::z a | 3 | — | *new* |",
            "| Ålpha | a::a a | 4 | — | *new* |",
        ]
    );
}

/// Two jobs stamped alike are named, and each key is named once however many
/// rows carry it.
#[test]
fn a_repeated_shard_identity_is_named_once() {
    let rows: String = [
        ("p", "x", 1),
        ("p", "x", 2),
        ("p", "x", 3),
        ("q", "y", 4),
        ("r", "w", 5),
        ("r", "w", 6),
    ]
    .iter()
    .map(|(pkg, id, n)| {
        stamped(
            &format!(r#"{{"package":"{pkg}","features":"a","baseline":"b"}}"#),
            "m_gungraun::a::a",
            id,
            *n,
        )
    })
    .collect();
    let report = rendered(&rows, "baseline");
    let named: Vec<&str> = report
        .lines()
        .filter(|l| l.starts_with("**Duplicate shard identity**"))
        .collect();
    assert_eq!(
        named,
        [
            "**Duplicate shard identity**: `p (a) — a::a x`, `r (a) — a::a w` appears more than once. \
          Either two jobs stamped themselves alike, or one package has two bench files whose group, \
          bench and case names coincide (the bench-file stem is not part of the name). Either way \
          the rows below cannot be told apart."
        ]
    );
}

/// Distinct shards are not a collision, whatever they share.
#[test]
fn distinct_shards_are_not_a_collision() {
    let rows = format!(
        "{}{}",
        stamped(
            r#"{"package":"p","features":"a","baseline":"b"}"#,
            "m_gungraun::a::a",
            "x",
            1
        ),
        stamped(
            r#"{"package":"p","features":"c","baseline":"b"}"#,
            "m_gungraun::a::a",
            "x",
            2
        ),
    );
    assert!(!rendered(&rows, "baseline").contains("Duplicate shard identity"));
}

/// Every distinct compiler is named once, in sorted order, and an unstamped run
/// names none.
#[test]
fn every_distinct_compiler_is_named() {
    let table: [(&[&str], Option<&str>); 4] = [
        (&[r#","rustc":"rustc 1""#], Some("Built by rustc 1.")),
        (
            &[
                r#","rustc":"rustc 2""#,
                r#","rustc":"rustc 1""#,
                r#","rustc":"rustc 2""#,
            ],
            Some("Built by rustc 1, rustc 2."),
        ),
        (&[r#","rustc":"""#], None),
        (&[""], None),
    ];
    for (stamps, want) in table {
        let rows: String = stamps
            .iter()
            .enumerate()
            .map(|(i, rustc)| {
                stamped(
                    &format!(r#"{{"package":"p{i}","baseline":"b"{rustc}}}"#),
                    "m_gungraun::a::a",
                    "x",
                    1,
                )
            })
            .collect();
        let report = rendered(&rows, "baseline");
        let built: Vec<&str> = report
            .lines()
            .filter(|l| l.starts_with("Built by"))
            .collect();
        assert_eq!(built, want.as_slice().to_vec(), "{stamps:?}");
    }
}

// ── Profiles ───────────────────────────────────────────────────────────

/// A summary with no instruction count says so in place of a number, rather
/// than dropping the bench from the table.
#[test]
fn a_summary_with_no_instruction_count_renders_an_explicit_row() {
    let bodies = [
        r#""profiles":[]"#,
        r#""profiles":[{"tool":"DHAT","summaries":{"total":{"summary":{"Dhat":{}}}}}]"#,
        r#""profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Dr":{"metrics":{"Left":{"Int":9}}}}}}}}]"#,
        r#""profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{}}}}]"#,
        r#""profiles":[{"tool":"Callgrind","summaries":{}}]"#,
        r#""profiles":[{"tool":"Callgrind"}]"#,
        r#""profiles":[{"summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":9}}}}}}}}]"#,
    ];
    for body in bodies {
        let text = summary(&format!(
            r#""package_dir":"/w/c/p","module_path":"b_gungraun::g::case","id":"one",{body}"#
        ));
        assert_eq!(
            ir_rows(&rendered(&text, "baseline")),
            ["| p | g::case one | — | — | *no callgrind profile* |"],
            "{body}"
        );
    }
}

/// The first profile a summary carries for a tool is the one read.
#[test]
fn the_first_profile_for_a_tool_is_the_one_read() {
    let text = summary(
        r#""package_dir":"/w/c/p","module_path":"b_gungraun::g::case","id":"one","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":1}}}}}}}},{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":2}}}}}}}}]"#,
    );
    assert_eq!(
        ir_rows(&rendered(&text, "baseline")),
        ["| p | g::case one | 1 | — | *new* |"]
    );
}

/// The heap table carries blocks then peak bytes whatever order the summary
/// holds them in, and the fold carries every metric in the summary's own order.
#[test]
fn the_heap_table_is_ordered_and_the_fold_is_not() {
    let text = dhat_summary(&format!(
        r#""AtTGmaxBytes":{},"TotalBytes":{},"TotalBlocks":{}"#,
        both("4200", "4096", "2.5"),
        both("9", "9", "0.0"),
        both("44", "40", "10.0"),
    ));
    let report = rendered(&text, "base");
    let heap: Vec<&str> = report
        .lines()
        .skip_while(|l| !l.starts_with("| --- | --- | --- |"))
        .skip(1)
        .take_while(|l| l.starts_with("| "))
        .collect();
    assert_eq!(
        heap,
        [
            "| p | g::case one | TotalBlocks | 44 | 40 | **+10%** (over threshold) |",
            "| p | g::case one | AtTGmaxBytes | 4200 | 4096 | +2.5% |",
        ]
    );
    let fold: Vec<&str> = report
        .lines()
        .skip_while(|l| !l.starts_with("| --- | ---: | ---: |"))
        .skip(1)
        .take_while(|l| l.starts_with("| "))
        .collect();
    assert_eq!(
        fold,
        [
            "| AtTGmaxBytes | 4200 | 4096 | +2.5% |",
            "| TotalBytes | 9 | 9 | 0% |",
            "| TotalBlocks | 44 | 40 | +10% |",
        ]
    );
}

/// The fold carries every callgrind metric, including ones the tables never
/// name, in the order the summary holds them.
#[test]
fn the_fold_keeps_the_summary_order() {
    let text = p_summary(&profiles(&[callgrind_profile(&format!(
        r#""Ir":{},"Dr":{},"Bc":{},"I1MissRate":{{"metrics":{{"Left":{{"Float":0.5}}}}}}"#,
        left(1),
        left(2),
        left(3),
    ))]));
    let report = rendered(&text, "base");
    assert_eq!(
        fold_rows(&report),
        [
            "| Ir | 1 | — | *new* |",
            "| Dr | 2 | — | *new* |",
            "| Bc | 3 | — | *new* |",
            "| I1MissRate | 0.5 | — | *new* |",
        ]
    );
}

/// The heap table appears only where a summary carries a DHAT profile.
#[test]
fn no_dhat_profile_means_no_heap_table() {
    let report = rendered(&ir(&both("1", "1", "0.0"), None), "baseline");
    assert!(!report.contains("## Heap (DHAT)"), "{report}");
}

// ── The schema gate ────────────────────────────────────────────────────

/// A version this report was not written against is an error naming every
/// version seen, sorted.
#[test]
fn a_drifted_schema_version_is_an_error_naming_it() {
    let table = [
        (vec![r#""version":"7""#], "v7"),
        (
            vec![r#""version":"7""#, r#""version":"5""#, r#""version":"7""#],
            "v5 7",
        ),
        (vec![r#""version":"6""#, r#""version":"7""#], "v7"),
        (vec![r#""version":6"#], "v6"),
        (vec![r#""nothing":0"#], "vnull"),
    ];
    for (versions, want) in table {
        let text: String = versions
            .iter()
            .map(|v| {
                format!(
                    r#"{{{v},"package_dir":"/w/c/p","module_path":"b_gungraun::g::c","profiles":[]}}"#
                ) + "\n"
            })
            .collect();
        let err = parse_err(&text);
        assert_eq!(
            err,
            format!(
                "gungraun summary schema is {want}, this report is written against v6. \
                 Update xtask/src/checks/perf_report.rs against the new schema before \
                 trusting its output"
            ),
            "{versions:?}"
        );
    }
}

/// The version gate runs before the summary is walked, so a file that is both
/// drifted and unreadable reports the drift.
#[test]
fn the_version_gate_runs_before_the_summary_is_walked() {
    let err = parse_err(r#"{"version":"7"}"#);
    assert!(err.starts_with("gungraun summary schema is v7,"), "{err}");
}

/// A summary this report cannot walk fails the whole report.
#[test]
fn a_summary_that_cannot_be_walked_is_an_error() {
    for text in [
        r#"{"version":"6"#,
        r#"{"version":"6","module_path":"a::b"}"#,
        r#"{"version":"6","profiles":[]}"#,
        "[1,2]",
    ] {
        let err = parse_err(text);
        assert!(
            err.starts_with("cannot read a gungraun summary: "),
            "{text}: {err}"
        );
    }
}

/// A percentage that is neither of the three named forms nor a number this
/// report can read is an error naming it, on a metric the tables show and on
/// one only the fold reaches.
#[test]
fn an_unreadable_percentage_is_an_error() {
    for pct in ["abc", "nan", "-nan", "NAN", "0x10"] {
        let want = Some(format!("cannot read `{pct}` as a percentage"));
        let flagged = parse(&ir(&both("105000", "100000", pct), None)).unwrap();
        assert_eq!(render(&flagged, "baseline").err(), want, "{pct}");
        assert_eq!(regressions(&flagged).err(), want, "{pct}");

        // `Dr` reaches no threshold, so only the fold renders its delta.
        let folded = parse(&p_summary(&profiles(&[callgrind_profile(&format!(
            r#""Ir":{},"Dr":{}"#,
            both("105000", "100000", "1.0"),
            both("5", "4", pct),
        ))])))
        .unwrap();
        assert_eq!(render(&folded, "baseline").err(), want, "{pct}");
        assert!(!regressions(&folded).unwrap(), "{pct}");
    }
}

/// A side carrying both forms takes the integer, and a null side inside a
/// comparison dashes its column.
#[test]
fn a_side_takes_the_integer_and_a_null_side_is_absent() {
    let table = [
        (r#"{"metrics":{"Left":{"Int":7,"Float":9.5}}}"#, "7", "—"),
        (
            r#"{"metrics":{"Both":[{"Float":9.5},{"Int":3}]}}"#,
            "9.5",
            "3",
        ),
        (r#"{"metrics":{"Both":[{"Int":7},null]}}"#, "7", "—"),
        (r#"{"metrics":{"Both":[null,{"Int":3}]}}"#, "null", "3"),
        (r#"{"metrics":{"Left":{}}}"#, "null", "—"),
        (r#"{"metrics":{}}"#, "null", "—"),
        (r#"{}"#, "null", "—"),
    ];
    for (metrics, new, old) in table {
        assert_eq!(
            ir_rows(&rendered(&ir(metrics, None), "baseline")),
            [format!(
                "| spate-json | g::case one | {new} | {old} | *new* |"
            )],
            "{metrics}"
        );
    }
}

/// A metric name a summary repeats collapses to one entry taking the later
/// value at the earlier position, as a JSON object parse does.
#[test]
fn a_repeated_metric_name_collapses_to_one_entry() {
    let text = p_summary(&profiles(&[callgrind_profile(&format!(
        r#""Ir":{},"Dr":{},"Ir":{}"#,
        left(1),
        left(9),
        left(2),
    ))]));
    let report = rendered(&text, "base");
    assert_eq!(ir_rows(&report), ["| p | g::case one | 2 | — | *new* |"]);
    assert_eq!(
        fold_rows(&report),
        ["| Ir | 2 | — | *new* |", "| Dr | 9 | — | *new* |"]
    );
}

/// One metric over its threshold flags the whole run, wherever it sits among
/// the others.
#[test]
fn one_crossed_threshold_flags_the_whole_run() {
    let hot = ir(&both("107000", "100000", "7.0"), None);
    let quiet = ir(&both("100000", "99000", "1.0"), None);
    for rows in [
        format!("{hot}\n{quiet}\n"),
        format!("{quiet}\n{hot}\n"),
        format!("{quiet}\n{quiet}\n{hot}\n{quiet}\n"),
    ] {
        assert!(regressions(&parse(&rows).unwrap()).unwrap(), "{rows}");
    }
    assert!(!regressions(&parse(&format!("{quiet}\n{quiet}\n")).unwrap()).unwrap());
}

/// A crossed instruction threshold flags the run even where the heap metrics
/// beside it moved nothing.
#[test]
fn a_quiet_heap_does_not_unflag_a_moved_instruction_count() {
    let text = p_summary(&profiles(&[
        callgrind_profile(&format!(r#""Ir":{}"#, both("107000", "100000", "7.0"))),
        dhat_profile(&format!(
            r#""TotalBlocks":{},"AtTGmaxBytes":{}"#,
            both("40", "40", "0.0"),
            both("4096", "4096", "0.0"),
        )),
    ]));
    assert!(regressions(&parse(&text).unwrap()).unwrap());
}

// ── The flag file ──────────────────────────────────────────────────────

/// The flag file holds one of the two words `perf-label.yml` matches on, and a
/// newline. Anything else leaves the label unsynced.
#[test]
fn the_flag_file_is_the_bare_boolean_the_label_workflow_parses() {
    assert_eq!(flag_text(true), "true\n");
    assert_eq!(flag_text(false), "false\n");
}

/// The flag and the markers come from one set of definitions, so a row the
/// report emboldens is a row the label acts on.
#[test]
fn the_flag_agrees_with_the_markers() {
    for pct in ["7.0", "1.0", "5.0", "4.999", "inf", "-inf", "NaN"] {
        let text = ir(&both("105000", "100000", pct), None);
        let rows = parse(&text).unwrap();
        assert_eq!(
            regressions(&rows).unwrap(),
            render(&rows, "baseline")
                .unwrap()
                .contains("(over threshold)"),
            "{pct}"
        );
    }
}
