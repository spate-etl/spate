//! The process-level contract of `cargo xtask bench report` and
//! `cargo xtask tidy perf-report`: the exit status each outcome reports, which
//! stream carries what, and the bytes the flag file holds.
//!
//! The flag file crosses into `perf-label.yml`, so one case here reads that
//! workflow and holds the bytes to the arms it matches on.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// One summary whose instruction count crossed its threshold.
const HOT: &str = r#"{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":107000},{"Int":100000}]},"diffs":{"diff_pct":"7.0"}}}}}}}]}"#;

/// One summary that moved nothing past a threshold.
const QUIET: &str = r#"{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}"#;

/// The whole report [`HOT`] renders.
const HOT_REPORT: &str = "\
## Instruction counts

Callgrind instructions (`Ir`) per bench: pull request vs baseline.
Advisory: numbers never block a merge; a bench that stops running does.
A **bold** delta crossed a provisional threshold and syncs the
`affects-performance` label; nothing else happens.

| Shard | Bench | PR | baseline | Δ |
| --- | --- | ---: | ---: | ---: |
| spate-json | decode::decode_value flat_record | 107000 | 100000 | **+7%** (over threshold) |

<details><summary>All metrics</summary>

**spate-json — decode::decode_value flat_record** — callgrind

| Metric | PR | baseline | Δ |
| --- | ---: | ---: | ---: |
| Ir | 107000 | 100000 | +7% |

</details>
";

/// The usage line every invocation naming no readable summaries file reports.
const USAGE: &str = "xtask: usage: cargo xtask bench report [--regressions-out FILE] <summaries> \
                     [<baseline-label>]. Needs a non-empty, readable summaries file\n";

/// A directory holding this case's fixtures and whatever flag file it writes.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-perf-report-test-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// Writes one summaries file and answers its path.
    fn summaries(&self, body: &str) -> PathBuf {
        let path = self.0.join("summaries.jsonl");
        std::fs::write(&path, body).unwrap();
        path
    }

    fn flag(&self) -> PathBuf {
        self.0.join("has_regressions")
    }

    /// The flag file's bytes, or nothing where none was written.
    fn flag_body(&self) -> Option<String> {
        std::fs::read_to_string(self.flag()).ok()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(args)
        // The annotation prefix would otherwise depend on the host.
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap()
}

fn report(args: &[&str]) -> Output {
    let mut all = vec!["bench", "report"];
    all.extend_from_slice(args);
    xtask(&all)
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn path(p: &Path) -> String {
    p.display().to_string()
}

/// The rendered report reaches stdout whole, nothing reaches stderr, and the
/// flag file holds the bare boolean.
#[test]
fn a_rendered_report_is_stdout_and_the_flag_is_its_own_file() {
    let scratch = Scratch::new("rendered");
    let summaries = scratch.summaries(HOT);
    let out = report(&[
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), HOT_REPORT);
    assert_eq!(stderr(&out), "");
    assert_eq!(scratch.flag_body().as_deref(), Some("true\n"));
}

/// A run that names no flag file writes none, and renders the same report.
#[test]
fn naming_no_flag_file_writes_none() {
    let scratch = Scratch::new("no-flag");
    let summaries = scratch.summaries(HOT);
    let out = report(&[&path(&summaries)]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), HOT_REPORT);
    assert_eq!(stderr(&out), "");
    assert_eq!(scratch.flag_body(), None);
}

/// A run that crossed no threshold still writes the flag, so a pull request
/// that moves nothing has the label taken off it.
#[test]
fn a_run_that_crossed_no_threshold_writes_false() {
    let scratch = Scratch::new("quiet");
    let summaries = scratch.summaries(QUIET);
    let out = report(&[
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stderr(&out), "");
    assert_eq!(scratch.flag_body().as_deref(), Some("false\n"));
}

/// The baseline label names the old column, and an empty one falls back to the
/// generic word, so no row claims its shard measured nothing.
#[test]
fn the_baseline_label_names_the_old_column() {
    let scratch = Scratch::new("label");
    let summaries = scratch.summaries(QUIET);
    for (label, header) in [
        (Some("main @ abcdef"), "main @ abcdef"),
        (Some(""), "baseline"),
        (None, "baseline"),
    ] {
        let mut args = vec![path(&summaries)];
        if let Some(label) = label {
            args.push(label.to_owned());
        }
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = report(&refs);
        assert_eq!(out.status.code(), Some(0), "{label:?}: {}", stderr(&out));
        assert_eq!(
            stdout(&out)
                .lines()
                .filter(|l| l.starts_with("| Shard |"))
                .collect::<Vec<_>>(),
            [format!("| Shard | Bench | PR | {header} | Δ |")],
            "{label:?}"
        );
    }
}

/// Arguments past the summaries file and the label are ignored.
#[test]
fn arguments_past_the_label_are_ignored() {
    let scratch = Scratch::new("extra");
    let summaries = scratch.summaries(HOT);
    let out = report(&[&path(&summaries), "baseline", "extra", "--more"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), HOT_REPORT);
    assert_eq!(stderr(&out), "");
}

/// An invocation naming no readable, non-empty summaries file reports the usage
/// line alone and writes no flag.
#[test]
fn an_unusable_summaries_file_reports_the_usage_line() {
    let scratch = Scratch::new("usage");
    std::fs::write(scratch.0.join("empty.jsonl"), "").unwrap();
    let flag = path(&scratch.flag());
    let cases: Vec<Vec<String>> = vec![
        vec![],
        vec!["--regressions-out".to_owned(), flag.clone()],
        vec![
            "--regressions-out".to_owned(),
            flag.clone(),
            path(&scratch.0.join("absent.jsonl")),
        ],
        vec![
            "--regressions-out".to_owned(),
            flag.clone(),
            path(&scratch.0.join("empty.jsonl")),
        ],
    ];
    for args in cases {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = report(&refs);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {}", stderr(&out));
        assert_eq!(stderr(&out), USAGE, "{args:?}");
        assert_eq!(stdout(&out), "", "{args:?}");
        assert_eq!(scratch.flag_body(), None, "{args:?}");
    }
}

/// A flag file with no name is rejected before anything is read.
#[test]
fn an_empty_flag_file_name_is_rejected() {
    let scratch = Scratch::new("empty-flag-name");
    let summaries = scratch.summaries(HOT);
    let out = report(&["--regressions-out", "", &path(&summaries)]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(
        stderr(&out),
        "xtask: --regressions-out needs a file argument\n"
    );
    assert_eq!(stdout(&out), "");
}

/// A schema this report was not written against is an error naming every
/// version seen, and no flag file is left for the label to read.
#[test]
fn a_drifted_schema_is_an_error_and_writes_no_flag() {
    let scratch = Scratch::new("drift");
    let summaries = scratch.summaries(&format!(
        "{}\n{}\n",
        HOT.replace(r#""version":"6""#, r#""version":"7""#),
        HOT.replace(r#""version":"6""#, r#""version":"5""#),
    ));
    let out = report(&[
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(
        stderr(&out),
        "xtask: gungraun summary schema is v5 7, this report is written against v6. \
         Update xtask/src/checks/perf_report.rs against the new schema before trusting \
         its output\n"
    );
    assert_eq!(stdout(&out), "");
    assert_eq!(scratch.flag_body(), None);
}

/// A summary this report cannot walk leaves no flag behind, so a dead report
/// cannot be read as a run that found nothing.
#[test]
fn a_summary_that_cannot_be_walked_writes_no_flag() {
    let scratch = Scratch::new("malformed");
    let summaries = scratch.summaries(r#"{"version":"6","#);
    let out = report(&[
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
    assert_eq!(scratch.flag_body(), None);
    // The tail is serde_json's own diagnostic, which names the column it
    // stopped at.
    let reported = stderr(&out);
    assert!(
        reported.starts_with("xtask: cannot read a gungraun summary: "),
        "{reported}"
    );
    assert!(reported.ends_with('\n'), "{reported}");
}

/// A report that cannot be rendered writes no flag, even where the flag itself
/// could have been decided. A stale flag beside a dead report would label the
/// pull request off numbers nobody can see.
#[test]
fn a_report_that_cannot_be_rendered_writes_no_flag() {
    let scratch = Scratch::new("render-fails");
    // `Ir` is readable, so the flag pass would succeed; the fold reaches `Dr`,
    // whose percentage it cannot read.
    let summaries = scratch.summaries(&HOT.replace(
        r#""diffs":{"diff_pct":"7.0"}}"#,
        r#""diffs":{"diff_pct":"7.0"}},"Dr":{"metrics":{"Both":[{"Int":5},{"Int":4}]},"diffs":{"diff_pct":"abc"}}"#,
    ));
    let out = report(&[
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
    assert_eq!(stderr(&out), "xtask: cannot read `abc` as a percentage\n");
    assert_eq!(scratch.flag_body(), None);
}

/// On a runner the diagnostic carries the annotation prefix, so a failure the
/// step swallows still shows up.
#[test]
fn a_failure_on_a_runner_carries_the_annotation_prefix() {
    let scratch = Scratch::new("annotation");
    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["bench", "report"])
        .env("GITHUB_ACTIONS", "true")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stderr(&out), format!("::error::{USAGE}"));
    drop(scratch);
}

/// `--explain` names what it would read and write, and touches neither. It
/// precedes the summaries path, which opens a trailing argument list.
#[test]
fn explain_names_what_it_reads_and_writes() {
    let scratch = Scratch::new("explain");
    let summaries = scratch.0.join("absent.jsonl");
    let out = report(&[
        "--explain",
        "--regressions-out",
        &path(&scratch.flag()),
        &path(&summaries),
    ]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        format!(
            "(reads {}, writes {})\n",
            path(&summaries),
            path(&scratch.flag())
        )
    );
    assert_eq!(stderr(&out), "");
    assert_eq!(scratch.flag_body(), None);

    let out = report(&["--explain", &path(&summaries)]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), format!("(reads {})\n", path(&summaries)));
}

/// The tidy check runs the fixtures it carries and reports what it held.
#[test]
fn the_tidy_check_runs_the_fixtures() {
    let out = xtask(&["tidy", "perf-report"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "perf-report: self-test ok: the flag file is the bare boolean perf-label.yml parses, \
         markers track the thresholds, and merged jobs keep their shard identity\n"
    );
    assert_eq!(stderr(&out), "");

    let out = xtask(&["tidy", "perf-report", "--explain"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), "(renders the fixtures this check carries)\n");
}

/// The flag file's bytes match an arm of the `case` `perf-label.yml` runs them
/// through, after that workflow's own whitespace strip. A third value there
/// leaves the label at whatever an earlier push set.
#[test]
fn the_flag_file_matches_an_arm_the_label_workflow_acts_on() {
    let workflow = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(".github/workflows/perf-label.yml"),
    )
    .unwrap();
    let arms: Vec<&str> = workflow
        .lines()
        .map(str::trim)
        .skip_while(|l| !l.starts_with("case \"$flag\" in"))
        .skip(1)
        .take_while(|l| *l != "esac")
        .filter_map(|l| l.split_once(") "))
        .map(|(head, _)| head)
        .collect();
    assert_eq!(arms, ["true", "false", "*"], "{workflow}");

    let scratch = Scratch::new("contract");
    for (body, want) in [(HOT, "true"), (QUIET, "false")] {
        let summaries = scratch.summaries(body);
        let out = report(&[
            "--regressions-out",
            &path(&scratch.flag()),
            &path(&summaries),
        ]);
        assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
        // `tr -d '[:space:]'`, as the workflow spells it.
        let stripped: String = scratch
            .flag_body()
            .unwrap()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert_eq!(stripped, want);
        assert!(arms.contains(&stripped.as_str()), "{stripped}");
    }
}
