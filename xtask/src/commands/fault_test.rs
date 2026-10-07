//! `cargo xtask fault-test`: the seeded multi-process fault scenarios in
//! `faults/`, and the summary of their outcomes.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::run::{self, Error, Outcome, Step};

/// Where the scenarios write their run directories, under the root.
const RUNS: &str = "target/fault-runs";

/// The kinds a scenario's `outcome.json` reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Pass,
    Violation,
    Worker,
    Expectation,
    Harness,
}

/// The fields of a scenario's `outcome.json` the summary carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ScenarioOutcome {
    scenario: String,
    kind: Kind,
    message: String,
    replay: String,
}

/// What `summary.json` holds.
#[derive(Debug, Serialize)]
struct Summary {
    seed: String,
    /// nextest exited 0.
    tests_passed: bool,
    outcomes: Vec<ScenarioOutcome>,
    exit_code: i32,
}

/// 1 when any scenario found a violation, a worker failure or a failed
/// expectation; otherwise 3 when any hit an infrastructure failure, or nextest
/// failed with no failing outcome to show for it; otherwise 0.
fn exit_code(tests_passed: bool, outcomes: &[ScenarioOutcome]) -> i32 {
    let failing = |kind: Kind| outcomes.iter().any(|o| o.kind == kind);
    if failing(Kind::Violation) || failing(Kind::Worker) || failing(Kind::Expectation) {
        1
    } else if failing(Kind::Harness) || !tests_passed {
        3
    } else {
        0
    }
}

/// Parses a seed written in decimal or as `0x`-prefixed hex.
fn parse_seed(text: &str) -> Option<u64> {
    match text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => text.parse().ok(),
    }
}

/// The nextest run of every ignored `spate-faults` test matching `filter`,
/// with the seed and run root in the scenarios' environment.
fn nextest(seed: &str, runs: &Path, filter: Option<&str>) -> Step<'static> {
    Step::new(
        "cargo",
        [
            "nextest",
            "run",
            "--profile",
            "faults",
            "-p",
            "spate-faults",
            "--locked",
            "--run-ignored",
            "ignored-only",
            "--ignore-default-filter",
            "--test-threads",
            "1",
        ],
    )
    .args(filter)
    .env("SPATE_FAULT_SEED", seed)
    .env("SPATE_FAULT_RUN_DIR", runs.display().to_string())
}

/// Runs the scenarios matching `filter` under `seed`, or under a seed drawn
/// from the clock, and writes `target/fault-runs/summary.json`.
pub(crate) fn fault_test(
    root: &Path,
    explain: bool,
    seed: Option<&str>,
    filter: Option<&str>,
) -> Outcome {
    let seed = match seed {
        Some(text) => {
            parse_seed(text).ok_or_else(|| Error::msg(format!("{text} is not a seed")))?
        }
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64),
    };
    let seed = format!("0x{seed:016x}");
    println!("fault-test seed {seed}");
    let runs = root.join(RUNS);
    let step = nextest(&seed, &runs, filter);
    if explain {
        return run::run(root, explain, &step);
    }
    match std::fs::remove_dir_all(&runs) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::msg(format!("{}: {e}", runs.display()))),
    }
    let tests_passed = run::run(root, explain, &step).is_ok();
    let outcomes = read_outcomes(&runs)?;
    let summary = Summary {
        exit_code: exit_code(tests_passed, &outcomes),
        seed,
        tests_passed,
        outcomes,
    };
    let path = runs.join("summary.json");
    std::fs::create_dir_all(&runs).map_err(|e| Error::msg(format!("{}: {e}", runs.display())))?;
    let json = serde_json::to_vec_pretty(&summary).map_err(|e| Error::msg(e.to_string()))?;
    std::fs::write(&path, json).map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
    for o in &summary.outcomes {
        println!("{:?} {}: {}", o.kind, o.scenario, o.message);
    }
    match summary.exit_code {
        0 => Ok(()),
        code => Err(Error {
            message: format!("fault-test failed; see {}", path.display()),
            code: Some(code),
        }),
    }
}

/// Every `<run>/outcome.json` under `runs`, ordered by scenario.
fn read_outcomes(runs: &Path) -> Result<Vec<ScenarioOutcome>, Error> {
    let Ok(dirs) = std::fs::read_dir(runs) else {
        return Ok(Vec::new());
    };
    let mut outcomes = Vec::new();
    for dir in dirs.flatten() {
        let path = dir.path().join("outcome.json");
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let outcome: ScenarioOutcome = serde_json::from_slice(&bytes)
            .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
        outcomes.push(outcome);
    }
    outcomes.sort_by(|a, b| a.scenario.cmp(&b.scenario));
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(scenario: &str, kind: Kind) -> ScenarioOutcome {
        ScenarioOutcome {
            scenario: scenario.to_owned(),
            kind,
            message: String::new(),
            replay: String::new(),
        }
    }

    /// A violation, a worker failure or a failed expectation exits 1 whatever
    /// else the run holds, and only a clean run exits 0.
    #[test]
    fn summary_classifies_violation_worker_expectation_and_harness() {
        for kind in [Kind::Violation, Kind::Worker, Kind::Expectation] {
            let outcomes = [
                outcome("a", Kind::Pass),
                outcome("b", Kind::Harness),
                outcome("c", kind),
            ];
            assert_eq!(exit_code(false, &outcomes), 1, "{kind:?}");
            assert_eq!(exit_code(true, &outcomes[2..]), 1, "{kind:?}");
        }
        assert_eq!(exit_code(true, &[outcome("a", Kind::Pass)]), 0);
        assert_eq!(exit_code(true, &[]), 0);
    }

    /// Infrastructure failures alone exit 3, and so does a failed nextest run
    /// with no failing outcome.
    #[test]
    fn exit_code_is_3_for_harness_only() {
        let harness = [outcome("a", Kind::Pass), outcome("b", Kind::Harness)];
        assert_eq!(exit_code(true, &harness), 3);
        assert_eq!(exit_code(false, &harness), 3);
        assert_eq!(exit_code(false, &[]), 3);
        assert_eq!(exit_code(false, &[outcome("a", Kind::Pass)]), 3);
    }

    /// The summary reads each run directory's `outcome.json`, ignoring
    /// directories without one and fields it does not carry.
    #[test]
    fn outcomes_are_read_from_each_run_directory() {
        let runs = std::env::temp_dir().join(format!("xtask-fault-runs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&runs);
        for (dir, body) in [
            (
                "b-1",
                Some(r#"{"scenario":"b","kind":"violation","message":"m","replay":"r","seed":1}"#),
            ),
            (
                "a-1",
                Some(r#"{"scenario":"a","kind":"pass","message":"","replay":""}"#),
            ),
            ("c-1", None),
        ] {
            std::fs::create_dir_all(runs.join(dir)).unwrap();
            if let Some(body) = body {
                std::fs::write(runs.join(dir).join("outcome.json"), body).unwrap();
            }
        }
        let outcomes = read_outcomes(&runs).unwrap();
        std::fs::remove_dir_all(&runs).unwrap();
        assert_eq!(
            outcomes
                .iter()
                .map(|o| (o.scenario.as_str(), o.kind))
                .collect::<Vec<_>>(),
            [("a", Kind::Pass), ("b", Kind::Violation)]
        );
    }

    /// The nextest step carries the seed in `SPATE_FAULT_SEED`.
    #[test]
    fn nextest_passes_the_seed_in_spate_fault_seed() {
        let step = nextest("0x00000000000000ff", Path::new("runs"), None);
        assert!(
            step.env
                .iter()
                .any(|(k, v)| *k == "SPATE_FAULT_SEED" && v == "0x00000000000000ff")
        );
    }

    /// The step runs only the ignored `spate-faults` tests, under the `faults`
    /// profile, narrowed by the filter.
    #[test]
    fn nextest_runs_the_ignored_fault_scenarios() {
        let step = nextest("0x1", Path::new("runs"), Some("nats"));
        for pair in [
            ["--profile", "faults"],
            ["-p", "spate-faults"],
            ["--run-ignored", "ignored-only"],
        ] {
            assert!(step.args.windows(2).any(|w| w == pair), "{pair:?}");
        }
        assert_eq!(step.args.last().map(String::as_str), Some("nats"));
    }

    /// The scenarios run one at a time, so no scenario's workers share the
    /// host's cores with another's.
    #[test]
    fn nextest_runs_one_scenario_at_a_time() {
        let step = nextest("0x1", Path::new("runs"), None);
        assert!(
            step.args.windows(2).any(|w| w == ["--test-threads", "1"]),
            "{:?}",
            step.args
        );
    }

    /// A seed reads the same in decimal and in `0x` hex.
    #[test]
    fn seeds_parse_in_decimal_and_hex() {
        assert_eq!(parse_seed("255"), Some(255));
        assert_eq!(parse_seed("0xff"), Some(255));
        assert_eq!(parse_seed("ff"), None);
    }
}
