//! `cargo xtask fault-test report`: the issue bodies the weekly job files for a
//! failed fault run, one per route, each laid out as the issue form it files
//! under.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::Subcommand;
use serde::Deserialize;

use super::fault_test::{Kind, ScenarioOutcome, Summary, read_outcomes};
use crate::run::{Error, Outcome};

#[derive(Subcommand)]
pub(crate) enum FaultTestCommand {
    /// Write an issue body and title per failure route of a run, and print
    /// the routes, one per line
    Report {
        /// The run's summary; without it the run directories beside it are
        /// reported as a failed nextest run
        #[arg(
            long,
            value_name = "PATH",
            default_value = "target/fault-runs/summary.json"
        )]
        summary: PathBuf,
        /// The CI run, linked from every body
        #[arg(long, value_name = "URL")]
        run_url: String,
        /// Where `<route>.md` and `<route>.title` are written
        #[arg(long, value_name = "DIR", default_value = "target/fault-report")]
        out_dir: PathBuf,
    },
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: FaultTestCommand) -> Outcome {
    let FaultTestCommand::Report {
        summary,
        run_url,
        out_dir,
    } = cmd;
    if explain {
        println!("rustc --version\nuname -sm");
        return Ok(());
    }
    let context = Context {
        run_url,
        sha: sha(root),
        toolchain: format!(
            "{}, {}",
            output(root, "rustc", &["--version"]),
            output(root, "uname", &["-sm"])
        ),
        features: fs::read_to_string(root.join("faults/Cargo.toml"))
            .map_err(|e| Error::msg(format!("faults/Cargo.toml: {e}")))
            .and_then(|text| features(&text))?,
        seed: None,
        summary_written: false,
    };
    for route in report(&root.join(summary), &root.join(out_dir), &context)? {
        println!("{}", route.name());
    }
    Ok(())
}

/// A failed run files one issue per route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Delivery,
    Worker,
    Expectation,
    Harness,
}

impl Route {
    fn name(self) -> &'static str {
        match self {
            Route::Delivery => "delivery",
            Route::Worker => "worker",
            Route::Expectation => "expectation",
            Route::Harness => "harness",
        }
    }

    /// The fixed title, so that a later failing run comments on the open
    /// issue instead of filing another.
    fn title(self) -> &'static str {
        match self {
            Route::Delivery => "[delivery] The weekly fault run found a delivery violation",
            Route::Worker => "[bug] A weekly fault run worker failed with no delivery violation",
            Route::Expectation => "[bug] A weekly fault scenario did not meet its own expectation",
            Route::Harness => "[bug] The weekly fault run hit an infrastructure failure",
        }
    }

    fn kind(self) -> Kind {
        match self {
            Route::Delivery => Kind::Violation,
            Route::Worker => Kind::Worker,
            Route::Expectation => Kind::Expectation,
            Route::Harness => Kind::Harness,
        }
    }
}

/// One route per failing outcome kind present, with `harness` only when no
/// other route applies. A failed nextest run with no failing outcome is
/// `harness`.
fn routes(tests_passed: bool, outcomes: &[ScenarioOutcome]) -> Vec<Route> {
    let present = |route: Route| outcomes.iter().any(|o| o.kind == route.kind());
    let mut routes: Vec<Route> = [Route::Delivery, Route::Worker, Route::Expectation]
        .into_iter()
        .filter(|r| present(*r))
        .collect();
    if routes.is_empty() && (present(Route::Harness) || !tests_passed) {
        routes.push(Route::Harness);
    }
    routes
}

/// Writes `<route>.md` and `<route>.title` under `out_dir` for each route of
/// the run `summary` describes, and returns the routes. Run directories are
/// read from beside `summary`.
///
/// With no `summary.json`, the outcomes on disk are routed as a failed
/// nextest run under the seed they carry; an `outcome.json` that does not
/// parse leaves none, which files `harness`.
fn report(summary_path: &Path, out_dir: &Path, context: &Context) -> Result<Vec<Route>, Error> {
    let summary: Option<Summary> = match fs::read(summary_path) {
        Ok(bytes) => Some(
            serde_json::from_slice(&bytes)
                .map_err(|e| Error::msg(format!("{}: {e}", summary_path.display())))?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(Error::msg(format!("{}: {e}", summary_path.display()))),
    };
    let runs_root = summary_path.parent().unwrap_or(Path::new("."));
    let summary_written = summary.is_some();
    let (tests_passed, outcomes, seed) = match summary {
        Some(s) => (s.tests_passed, s.outcomes, Some(s.seed)),
        None => {
            let outcomes = read_outcomes(runs_root).unwrap_or_default();
            let seed = outcomes.first().and_then(|o| run_seed(runs_root, o));
            (false, outcomes, seed)
        }
    };
    let routes = routes(tests_passed, &outcomes);
    let context = Context {
        seed,
        summary_written,
        ..context.clone()
    };
    fs::create_dir_all(out_dir).map_err(|e| Error::msg(format!("{}: {e}", out_dir.display())))?;
    for route in &routes {
        let runs = outcomes
            .iter()
            .filter(|o| o.kind == route.kind())
            .map(|o| RunDir::load(runs_root, o))
            .collect::<Result<Vec<_>, _>>()?;
        let body = render(*route, &context, &runs);
        for (ext, text) in [("md", body.as_str()), ("title", route.title())] {
            let path = out_dir.join(format!("{}.{ext}", route.name()));
            fs::write(&path, text).map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
        }
    }
    Ok(routes)
}

/// The run seed an outcome's `outcome.json` carries, as `summary.json`
/// writes it.
fn run_seed(runs_root: &Path, entry: &ScenarioOutcome) -> Option<String> {
    let bytes = fs::read(runs_root.join(&entry.dir).join("outcome.json")).ok()?;
    let outcome: RunOutcome = serde_json::from_slice(&bytes).ok()?;
    Some(format!("0x{:016x}", outcome.seed))
}

/// What every body states about the run as a whole.
#[derive(Debug, Clone)]
struct Context {
    run_url: String,
    sha: String,
    /// `rustc --version` and `uname -sm`.
    toolchain: String,
    /// The features `spate-faults` enables on the crates it runs.
    features: String,
    /// The run seed, from the summary or else from an outcome on disk.
    seed: Option<String>,
    /// The run wrote `summary.json`.
    summary_written: bool,
}

/// `GITHUB_SHA`, or the checked-out commit off a runner.
fn sha(root: &Path) -> String {
    std::env::var("GITHUB_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| output(root, "git", &["rev-parse", "HEAD"]))
}

/// A command's trimmed stdout, or `unknown` when it fails.
fn output(root: &Path, program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// The features each `spate-*` dependency of the manifest enables, as
/// `crate: a, b; crate: c`.
fn features(manifest: &str) -> Result<String, Error> {
    let manifest: toml::Table =
        toml::from_str(manifest).map_err(|e| Error::msg(format!("faults/Cargo.toml: {e}")))?;
    let deps = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| Error::msg("faults/Cargo.toml has no [dependencies]"))?;
    let listed: Vec<String> = deps
        .iter()
        .filter(|(name, _)| name.starts_with("spate-"))
        .filter_map(|(name, spec)| {
            let features: Vec<&str> = spec
                .get("features")?
                .as_array()?
                .iter()
                .filter_map(toml::Value::as_str)
                .collect();
            Some(format!("{name}: {}", features.join(", ")))
        })
        .collect();
    Ok(listed.join("; "))
}

/// The fields of a scenario's `outcome.json` the bodies use.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RunOutcome {
    scenario: String,
    store: String,
    instances: u32,
    seed: u64,
    replay: String,
    stage: String,
    message: String,
    violations: Vec<Violation>,
    expectations: Vec<String>,
    faults_fired: Vec<FaultFired>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Violation {
    property: u8,
    check: String,
    key: Option<String>,
    rev: Option<u64>,
    instance: Option<String>,
    pid: Option<u32>,
    detail: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FaultFired {
    incarnation: String,
    fault: String,
    fired: bool,
}

/// A failed scenario's run directory, as the bodies use it.
#[derive(Debug, Default)]
struct RunDir {
    /// The directory's name under the run root.
    name: String,
    outcome: RunOutcome,
    /// Each worker config by incarnation, `w1-2` for `w1-2.json`.
    configs: BTreeMap<String, String>,
    /// Each non-empty worker stderr by incarnation.
    stderr: BTreeMap<String, String>,
    /// The `ev` of every line in the worker journals and `faults.ndjson`.
    events: BTreeSet<String>,
    /// The first record id a worker's sink wrote.
    record: Option<String>,
}

impl RunDir {
    fn load(runs_root: &Path, entry: &ScenarioOutcome) -> Result<RunDir, Error> {
        let dir = runs_root.join(&entry.dir);
        let read = |path: &Path| {
            fs::read_to_string(path).map_err(|e| Error::msg(format!("{}: {e}", path.display())))
        };
        let outcome_path = dir.join("outcome.json");
        let outcome = serde_json::from_str(&read(&outcome_path)?)
            .map_err(|e| Error::msg(format!("{}: {e}", outcome_path.display())))?;
        let mut run = RunDir {
            name: entry.dir.clone(),
            outcome,
            ..RunDir::default()
        };
        let mut files: Vec<PathBuf> = fs::read_dir(&dir)
            .map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?
            .flatten()
            .map(|e| e.path())
            .collect();
        files.sort();
        for path in files {
            let (Some(stem), Some(ext)) = (
                path.file_stem().and_then(|s| s.to_str()),
                path.extension().and_then(|s| s.to_str()),
            ) else {
                continue;
            };
            let worker = stem.starts_with('w');
            match ext {
                "json" if worker => {
                    run.configs.insert(stem.to_owned(), read(&path)?);
                }
                "stderr" => {
                    let text = read(&path)?;
                    if !text.trim().is_empty() {
                        run.stderr.insert(stem.to_owned(), text);
                    }
                }
                "ndjson" if worker || stem == "faults" => run.journal(&read(&path)?),
                _ => {}
            }
        }
        Ok(run)
    }

    /// Notes the events of one journal and its first record id; a line that
    /// does not parse, such as one a kill cut short, is skipped.
    fn journal(&mut self, text: &str) {
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let Some(ev) = value.get("ev").and_then(|v| v.as_str()) else {
                continue;
            };
            if ev == "rows" && self.record.is_none() {
                self.record = value["ids"][0].as_str().map(str::to_owned);
            }
            self.events.insert(ev.to_owned());
        }
    }

    /// The config of the instance the first attributed violation names, or
    /// of the first worker with stderr output, or the first config.
    fn config(&self) -> Option<(&str, &str)> {
        let named = self
            .outcome
            .violations
            .iter()
            .find_map(|v| v.instance.as_deref())
            .or_else(|| self.stderr.keys().next().and_then(|k| k.split('-').next()));
        named
            .and_then(|instance| {
                self.configs
                    .iter()
                    .find(|(k, _)| k.split('-').next() == Some(instance))
            })
            .or_else(|| self.configs.iter().next())
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

/// One field of an issue form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Field {
    id: &'static str,
    label: &'static str,
    /// The form's `render:` language; the body fences the answer in it.
    render: Option<&'static str>,
}

const fn field(id: &'static str, label: &'static str) -> Field {
    Field {
        id,
        label,
        render: None,
    }
}

const fn fenced(id: &'static str, label: &'static str, render: &'static str) -> Field {
    Field {
        id,
        label,
        render: Some(render),
    }
}

/// `.github/ISSUE_TEMPLATE/1-delivery-correctness.yml`, field by field.
const DELIVERY: [Field; 10] = [
    field("guarantee", "What broke"),
    field("version", "Version"),
    field("features", "Cargo features"),
    field("toolchain", "Rust version and platform"),
    field("components", "Which components"),
    field("circumstances", "Was any of this happening at the time?"),
    field("evidence", "How you know"),
    field("data", "A sample record"),
    fenced("config", "Pipeline configuration", "yaml"),
    fenced("logs", "Logs", "shell"),
];

/// `.github/ISSUE_TEMPLATE/2-bug.yml`, field by field.
const BUG: [Field; 10] = [
    field("components", "Where"),
    field("version", "Version"),
    field("features", "Cargo features"),
    field("toolchain", "Rust version and platform"),
    field("what", "What happened"),
    field("expected", "What you expected instead"),
    fenced("repro", "Reproduction", "rust"),
    field("data", "A sample record"),
    fenced("config", "Configuration", "yaml"),
    fenced("logs", "Output", "shell"),
];

// Answers quoted from the forms' options.
const LOST: &str =
    "Records were lost — never reached the sink, and the watermark advanced past them";
const AHEAD: &str = "The watermark advanced past data that was not acknowledged";
const DUPLICATES: &str = "Duplicates appeared without a crash or restart";
const NOT_SURE: &str = "Not sure — something is missing and I cannot account for it";
const COMPONENTS: [&str; 2] = ["S3 source (spate-s3)", "Coordination (spate-coordination)"];
const CIRCUMSTANCES: [&str; 6] = [
    "A consumer-group rebalance, or an instance joining or leaving",
    "A graceful shutdown (SIGTERM, drain)",
    "A hard kill (SIGKILL, OOM, node loss)",
    "A sink was failing or retrying",
    "Backpressure was engaged",
    "None of these — steady state",
];
const WHERE_COORDINATION: &str = "Coordination (spate-coordination)";
const WHERE_CI: &str = "Build, CI, or the examples";

/// The five delivery properties the oracle checks, by number.
const PROPERTIES: [&str; 5] = [
    "every record arrives",
    "duplicates appear only inside a fault or replay window",
    "no committed position runs ahead of durable rows",
    "no split completes twice or goes missing, and a killed leader is replaced",
    "no two owners commit on one split",
];

/// GitHub rejects an issue body over 65,536 characters; each part is capped
/// well inside it.
const MAX_LOG_CHARS: usize = 25_000;
const MAX_TEXT_CHARS: usize = 2_000;
const MAX_ITEM_CHARS: usize = 300;
const MAX_CONFIG_CHARS: usize = 4_000;
const MAX_VIOLATIONS: usize = 40;
const MAX_OTHERS: usize = 10;
const LOG_LINES: usize = 200;

fn render(route: Route, context: &Context, runs: &[RunDir]) -> String {
    let mut body = match route {
        Route::Delivery => delivery(context, runs),
        Route::Worker | Route::Expectation | Route::Harness => bug(route, context, runs),
    };
    let replay = match (runs, &context.seed) {
        ([], Some(seed)) => format!("cargo xtask fault-test --seed {seed}"),
        _ => runs
            .iter()
            .map(|r| r.outcome.replay.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    };
    section(&mut body, "Replay", Some("sh"), &replay);
    let mut artifacts = String::from(
        "The run's `fault-junit` artifact holds the junit report, and `fault-runs` \
         holds each failed scenario's run directory: worker configs, journals and \
         stderr, `faults.ndjson`, `health.ndjson` and `outcome.json`.\n",
    );
    for run in runs.iter().take(MAX_OTHERS) {
        let _ = write!(artifacts, "\n- `{}`: `{}`", run.outcome.scenario, run.name);
    }
    let _ = write!(artifacts, "\n\nRun: {}", context.run_url);
    section(&mut body, "Artifacts", None, &artifacts);
    body
}

fn form<const N: usize>(fields: &[Field; N], answers: [String; N]) -> String {
    let mut body = String::new();
    for (field, answer) in fields.iter().zip(answers) {
        section(&mut body, field.label, field.render, &answer);
    }
    body
}

/// One `###` section; an empty answer reads `_No response_`, as GitHub
/// renders an unanswered field.
fn section(body: &mut String, label: &str, render: Option<&str>, answer: &str) {
    let answer = answer.trim_end();
    let _ = write!(body, "### {label}\n\n");
    if answer.is_empty() {
        body.push_str("_No response_\n\n");
        return;
    }
    match render {
        Some(lang) => {
            let fence = fence(answer);
            let _ = write!(body, "{fence}{lang}\n{answer}\n{fence}\n\n");
        }
        None => {
            let _ = write!(body, "{answer}\n\n");
        }
    }
}

/// A backtick fence longer than any backtick run in `text`.
fn fence(text: &str) -> String {
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    "`".repeat(longest.max(2) + 1)
}

fn delivery(context: &Context, runs: &[RunDir]) -> String {
    let first = runs.first();
    let violations = first.map_or(&[][..], |r| &r.outcome.violations[..]);
    let properties: BTreeSet<u8> = violations.iter().map(|v| v.property).collect();
    let mut guarantee = what_broke(violations).to_owned();
    if !properties.is_empty() {
        let named: Vec<String> = properties
            .iter()
            .map(|p| match PROPERTIES.get(usize::from(*p).wrapping_sub(1)) {
                Some(name) => format!("{p} ({name})"),
                None => p.to_string(),
            })
            .collect();
        let _ = write!(guarantee, "\n\nProperties violated: {}.", named.join(", "));
    }

    let components = format!("{}\n\n{}", COMPONENTS.join(", "), scenarios_line(first));

    let mut evidence = String::new();
    for v in violations.iter().take(MAX_VIOLATIONS) {
        let _ = writeln!(evidence, "- {}", describe(v));
    }
    if violations.len() > MAX_VIOLATIONS {
        let _ = writeln!(
            evidence,
            "- and {} more in `outcome.json`",
            violations.len() - MAX_VIOLATIONS
        );
    }
    if let Some(first) = first
        && !first.outcome.expectations.is_empty()
    {
        let _ = write!(
            evidence,
            "\nExpectations that also failed: {}\n",
            clip(&first.outcome.expectations.join("; "), MAX_TEXT_CHARS)
        );
    }
    others(&mut evidence, runs, "Other scenarios with violations");

    form(
        &DELIVERY,
        [
            guarantee,
            context.sha.clone(),
            context.features.clone(),
            context.toolchain.clone(),
            components,
            circumstances(first),
            evidence,
            sample(first),
            config(first),
            logs(first),
        ],
    )
}

/// The dropdown option for the lowest-numbered property violated. Property 1
/// answers as lost only for a `RecordMissing`; a `RecordUnknown` alone is
/// `NOT_SURE`.
fn what_broke(violations: &[Violation]) -> &'static str {
    match violations.iter().map(|v| v.property).min() {
        Some(1) if violations.iter().any(|v| v.check == "RecordMissing") => LOST,
        Some(2) => DUPLICATES,
        Some(3) => AHEAD,
        _ => NOT_SURE,
    }
}

fn describe(v: &Violation) -> String {
    let mut text = format!("P{} {}", v.property, v.check);
    if let Some(key) = &v.key {
        let _ = write!(text, " on `{key}`");
    }
    if let Some(rev) = v.rev {
        let _ = write!(text, " at rev {rev}");
    }
    match (&v.instance, v.pid) {
        (Some(instance), Some(pid)) => {
            let _ = write!(text, ", written by {instance} (pid {pid})");
        }
        (Some(instance), None) => {
            let _ = write!(text, ", written by {instance}");
        }
        _ => {}
    }
    if !v.detail.is_empty() {
        let _ = write!(text, ": {}", clip(&v.detail, MAX_ITEM_CHARS));
    }
    text
}

fn scenarios_line(run: Option<&RunDir>) -> String {
    match run {
        Some(run) => format!(
            "Scenario `{}` on {}, {} instance(s), seed 0x{:016x}.",
            run.outcome.scenario, run.outcome.store, run.outcome.instances, run.outcome.seed
        ),
        None => "No scenario wrote an outcome.".to_owned(),
    }
}

/// The scenarios after the first, each with its message.
fn others(text: &mut String, runs: &[RunDir], heading: &str) {
    if runs.len() < 2 {
        return;
    }
    let _ = write!(text, "\n{heading}:\n\n");
    for run in runs.iter().skip(1).take(MAX_OTHERS) {
        let _ = writeln!(
            text,
            "- `{}` on {}, {} instance(s): {}",
            run.outcome.scenario,
            run.outcome.store,
            run.outcome.instances,
            clip(&run.outcome.message, MAX_ITEM_CHARS)
        );
    }
    if runs.len() > MAX_OTHERS + 1 {
        let _ = writeln!(text, "- and {} more", runs.len() - MAX_OTHERS - 1);
    }
}

/// The form's checkboxes: a hard kill for kills and aborts, an instance
/// joining or leaving for respawns and stops, then the faults drawn.
fn circumstances(run: Option<&RunDir>) -> String {
    let events = run.map(|r| &r.events);
    let any = |names: &[&str]| events.is_some_and(|e| names.iter().any(|n| e.contains(*n)));
    let joining = any(&["respawn", "sigstop", "stop"]);
    let killed = any(&["kill", "abort"]);
    let ticked = [joining, false, killed, false, false, !joining && !killed];
    let mut text = String::new();
    for (label, ticked) in CIRCUMSTANCES.iter().zip(ticked) {
        let mark = if ticked { 'x' } else { ' ' };
        let _ = writeln!(text, "- [{mark}] {label}");
    }
    if let Some(run) = run {
        text.push_str("\nFaults drawn per process incarnation:\n\n```text\n");
        if run.outcome.faults_fired.is_empty() {
            text.push_str("none\n");
        }
        for f in &run.outcome.faults_fired {
            let fired = if f.fired { "fired" } else { "not fired" };
            let _ = writeln!(text, "{}: {} ({fired})", f.incarnation, f.fault);
        }
        text.push_str("```\n");
    }
    text
}

/// One generated record, its padding elided.
fn sample(run: Option<&RunDir>) -> String {
    run.and_then(|r| r.record.as_deref())
        .map(|id| format!("{{\"k\":\"{id}\",\"pad\":\"…\"}}"))
        .unwrap_or_default()
}

fn config(run: Option<&RunDir>) -> String {
    run.and_then(RunDir::config)
        .map(|(name, text)| format!("# {name}.json\n{}", clip(text.trim_end(), MAX_CONFIG_CHARS)))
        .unwrap_or_default()
}

/// The last [`LOG_LINES`] lines of each worker's stderr, the whole cut from
/// the front to [`MAX_LOG_CHARS`].
fn logs(run: Option<&RunDir>) -> String {
    let Some(run) = run else {
        return String::new();
    };
    let mut text = String::new();
    for (name, stderr) in &run.stderr {
        let lines: Vec<&str> = stderr.lines().collect();
        let tail = &lines[lines.len().saturating_sub(LOG_LINES)..];
        let _ = writeln!(text, "==> {name}.stderr <==");
        for line in tail {
            let _ = writeln!(text, "{line}");
        }
    }
    let count = text.chars().count();
    if count <= MAX_LOG_CHARS {
        return text;
    }
    let cut: String = text.chars().skip(count - MAX_LOG_CHARS).collect();
    let cut = cut.split_once('\n').map_or(cut.as_str(), |(_, rest)| rest);
    format!("[earlier lines cut; the fault-runs artifact holds every stderr file]\n{cut}")
}

/// `text` cut to `max` characters, marked where it was cut.
fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let kept: String = text.chars().take(max).collect();
    format!("{kept} [cut; see outcome.json]")
}

fn bug(route: Route, context: &Context, runs: &[RunDir]) -> String {
    let first = runs.first();
    let (component, happened, expected) = match route {
        Route::Worker => (
            WHERE_COORDINATION,
            "A worker process failed while the oracle found no delivery violation and every \
             container stayed healthy",
            "Every worker process exits 0 once its splits are complete, and none ends on a \
             signal the harness did not send.",
        ),
        Route::Expectation => (
            WHERE_COORDINATION,
            "The scenario did not meet its own expectation",
            "Every fault the scenario draws fires, and the worker journals show the recovery \
             the scenario checks for.",
        ),
        Route::Harness | Route::Delivery => (
            WHERE_CI,
            "The run hit an infrastructure failure",
            "Every container starts and stays reachable for the whole run, and every worker \
             can write its journal.",
        ),
    };
    let mut what = String::new();
    match first {
        Some(run) => {
            let _ = write!(
                what,
                "{happened}. {} Stage: {}.\n\n{}\n",
                scenarios_line(Some(run)),
                run.outcome.stage,
                clip(&run.outcome.message, MAX_TEXT_CHARS)
            );
            if route == Route::Expectation && !run.outcome.expectations.is_empty() {
                what.push_str("\nExpectations that failed:\n\n");
                for e in run.outcome.expectations.iter().take(MAX_VIOLATIONS) {
                    let _ = writeln!(what, "- {}", clip(e, MAX_ITEM_CHARS));
                }
            }
        }
        None => match (&context.seed, context.summary_written) {
            (Some(seed), true) => {
                let _ = writeln!(
                    what,
                    "nextest failed under seed {seed} and no scenario left a failing outcome. \
                     The run log names the scenarios whose test failed."
                );
            }
            (Some(seed), false) => {
                let _ = writeln!(
                    what,
                    "The fault run wrote no `summary.json`, and no scenario under seed {seed} \
                     left a failing outcome. The run log has the cause."
                );
            }
            (None, _) => what.push_str(
                "The fault run wrote no `summary.json`. The run log has its seed and the \
                 cause.\n",
            ),
        },
    }
    if route == Route::Harness {
        what.push_str(
            "\nRerun the job before treating this as a defect. A container that fails to start \
             or an image pull that fails is usually transient.\n",
        );
    }
    others(&mut what, runs, "Other scenarios that failed the same way");
    let repro = match first {
        Some(run) => format!(
            "// `{}` in faults/tests/fault_run.rs; Replay below runs it under the same seed.",
            run.outcome.scenario
        ),
        None => String::new(),
    };
    form(
        &BUG,
        [
            component.to_owned(),
            context.sha.clone(),
            context.features.clone(),
            context.toolchain.clone(),
            what,
            expected.to_owned(),
            repro,
            sample(first),
            config(first),
            logs(first),
        ],
    )
}

#[cfg(test)]
mod tests;
