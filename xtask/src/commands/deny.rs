//! `cargo xtask deny`: cargo-deny over the workspace, with the advisories check
//! split out so a change that leaves the dependency inputs alone only reports
//! a finding.

use std::fmt::Write as _;
use std::path::Path;

use clap::ValueEnum;
use serde_json::Value;

use super::step_summary::{Summary, fence};
use crate::run::{self, Error, Outcome, Step, Streams};

/// One half of the checks `check all` runs.
#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
pub(crate) enum DenyCheck {
    /// Bans, licenses and sources, which change only with the tree.
    Policy,
    /// The RustSec advisories and yanked crates, which change with the date.
    Advisories,
}

const HEADING: &str = "## Advisories\n\n";

/// Runs the named checks, or every check when none is named.
///
/// `dependency_change` is read with [`DenyCheck::Advisories`] alone: the
/// value `false` turns every finding into a warning, and any other value,
/// the empty string included, leaves findings failing the run.
///
/// # Errors
///
/// With cargo-deny's exit code when a check fails or cargo-deny cannot run,
/// when the advisories check exits 0 without its summary record, and when
/// `dependency_change` is given with another check.
pub(super) fn run(
    root: &Path,
    explain: bool,
    check: Option<DenyCheck>,
    dependency_change: Option<&str>,
) -> Outcome {
    match check {
        Some(DenyCheck::Advisories) => {
            advisories(root, explain, dependency_change != Some("false"))
        }
        _ if dependency_change.is_some() => Err(Error::msg(
            "--dependency-change applies to `deny advisories` alone",
        )),
        Some(DenyCheck::Policy) => run::run(root, explain, &step(&["bans", "licenses", "sources"])),
        None => run::run(root, explain, &step(&["all"])),
    }
}

fn step(checks: &[&str]) -> Step<'static> {
    Step::new("cargo", ["deny", "--all-features", "--locked", "check"]).args(checks)
}

/// The advisories check, its findings written to the job summary.
fn advisories(root: &Path, explain: bool, blocking: bool) -> Outcome {
    // cargo-deny reads its output options only before `check`.
    let mut step = Step::new(
        "cargo",
        [
            "deny",
            "--format",
            "json",
            "--color",
            "never",
            "--all-features",
            "--locked",
            "check",
            "advisories",
            "--hide-inclusion-graph",
        ],
    );
    if !blocking {
        // A finding exits 0 as a warning. A config, metadata or database
        // failure never reaches a diagnostic, so it still exits 1.
        step = step.args(["--warn", "denied"]);
    }
    if explain {
        return run::run(root, true, &step);
    }
    let done = run::complete(root, &step, Streams::Capture)?;
    let report = Report::parse(&done.stderr);
    for line in &report.log {
        eprintln!("{line}");
    }
    let Some(found) = report.found else {
        // Without its summary record cargo-deny stopped before checking, so
        // each diagnostic is a config error and belongs in the step log.
        for line in &report.findings {
            eprintln!("{line}");
        }
        return Err(match done.code {
            0 => Error::msg("cargo-deny did not report its summary"),
            code => Error::status(code),
        });
    };
    let failed = done.code != 0;
    if let Some(text) = report.render(blocking, failed) {
        match std::env::var_os("GITHUB_STEP_SUMMARY") {
            Some(path) => Summary::at(path.into()).write(&text),
            None => print!("{text}"),
        }
    }
    if failed {
        // cargo-deny exits non-zero after its summary only for an error-level
        // finding, so the count is at least one.
        println!(
            "::error::{found} advisory finding(s) fail this change, listed in the job summary"
        );
        return Err(Error::status(done.code));
    }
    if found > 0 {
        println!("::warning::{found} advisory finding(s), listed in the job summary");
    }
    Ok(())
}

/// What one cargo-deny `--format json` run wrote to stderr.
#[derive(Debug, Default)]
struct Report {
    /// Each diagnostic, as the summary lists it.
    findings: Vec<String>,
    /// Log records and lines that are not JSON, kept for the step log since a
    /// tool error arrives as one.
    log: Vec<String>,
    /// Errors plus warnings from the advisories check, when cargo-deny got as
    /// far as its summary.
    found: Option<u64>,
}

impl Report {
    fn parse(stderr: &str) -> Self {
        let mut report = Self::default();
        for line in stderr.lines().filter(|l| !l.trim().is_empty()) {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                report.log.push(line.to_owned());
                continue;
            };
            let fields = &value["fields"];
            match value["type"].as_str() {
                Some("diagnostic") => report.findings.push(finding(fields)),
                Some("summary") => {
                    let counts = &fields["advisories"];
                    let count = |k: &str| counts[k].as_u64().unwrap_or(0);
                    report.found = Some(count("errors") + count("warnings"));
                }
                Some("log") => report.log.push(format!(
                    "{}: {}",
                    fields["level"].as_str().unwrap_or("LOG"),
                    fields["message"].as_str().unwrap_or_default()
                )),
                _ => report.log.push(line.to_owned()),
            }
        }
        report
    }

    /// The job summary, or none when the run found nothing.
    fn render(&self, blocking: bool, failed: bool) -> Option<String> {
        if self.findings.is_empty() {
            return None;
        }
        let body = self.findings.join("\n");
        let fence = fence(&body);
        // cargo-deny fails a run on an error alone, so a blocking run whose
        // findings are all warnings passes.
        let verdict = if failed {
            "This change touches the dependency inputs, so each finding fails it."
        } else if blocking {
            "This change touches the dependency inputs. Each finding is a warning, so none fails it."
        } else {
            "This change leaves the dependency inputs alone, so no finding fails it. \
             The nightly scan raises them against `main`."
        };
        Some(format!("{HEADING}{verdict}\n\n{fence}\n{body}\n{fence}\n"))
    }
}

/// One diagnostic: its code and message, the crates it names, and the advisory
/// it reports, one per line.
fn finding(fields: &Value) -> String {
    let mut text = format!(
        "{}: {}",
        fields["code"].as_str().unwrap_or("diagnostic"),
        fields["message"].as_str().unwrap_or_default()
    );
    for label in fields["labels"].as_array().into_iter().flatten() {
        if let Some(span) = label["span"].as_str() {
            let _ = write!(text, "\n  {span}");
        }
    }
    if let Some(id) = fields["advisory"]["id"].as_str() {
        let _ = write!(text, "\n  {id}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A finding whose text holds a backtick run stays inside the summary's
    /// code block.
    #[test]
    fn fences_outlast_backticks_in_an_advisory() {
        let report = Report::parse(
            r#"{"type":"diagnostic","fields":{"code":"vulnerability","message":"a ````` b","labels":[]}}"#,
        );
        let text = report.render(false, false).expect("a finding");
        assert!(
            text.contains("\n``````\nvulnerability: a ````` b\n``````\n"),
            "{text}"
        );
    }

    /// A record that is neither a diagnostic nor the summary reaches the step
    /// log, and only a summary line sets the count.
    #[test]
    fn a_log_record_reaches_the_step_log() {
        let report = Report::parse(
            "{\"type\":\"log\",\"fields\":{\"level\":\"ERROR\",\"message\":\"failed\"}}\nplain\n",
        );
        assert_eq!(report.log, ["ERROR: failed", "plain"]);
        assert_eq!(report.found, None);
        assert_eq!(report.render(true, true), None);
    }
}
