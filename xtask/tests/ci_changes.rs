//! The process-level contract of `cargo xtask ci-changes`: the event each run
//! classifies under, the note it prints when a diff cannot be resolved, the
//! bytes it appends to `$GITHUB_OUTPUT`, and the keys `ci.yml` reads back.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The variables the classifier and its `git` invocations read, cleared so
/// nothing on the host reaches the child.
const READS: [&str; 10] = [
    "EVENT_NAME",
    "BASE_SHA",
    "HEAD_SHA",
    "MERGE_BASE_SHA",
    "MERGE_HEAD_SHA",
    "EVENT_BEFORE",
    "PR_AUTHOR",
    "PR_LABELS",
    "GITHUB_OUTPUT",
    "GITHUB_ACTIONS",
];

/// The keys the workflow reads, in the order the block writes them.
const KEYS: [&str; 12] = [
    "rust",
    "site",
    "fuzz",
    "containers",
    "container-args",
    "semver",
    "semver-pkgs",
    "bench",
    "manifests",
    "release-tooling",
    "bench-shards",
    "clickhouse-lanes",
];

/// The block one documentation page selects. Nothing here builds or tests
/// Rust.
const SITE_ONLY: &str = "rust=false\n\
     site=true\n\
     fuzz=false\n\
     containers=false\n\
     container-args=\n\
     semver=false\n\
     semver-pkgs=\n\
     bench=false\n\
     manifests=false\n\
     release-tooling=false\n\
     bench-shards=[]\n\
     clickhouse-lanes=[]\n";

fn xtask(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command.args(args);
    for name in READS {
        command.env_remove(name);
    }
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().unwrap()
}

/// The exit status, stdout and stderr of one run, all three held at once.
#[track_caller]
fn held(args: &[&str], env: &[(&str, &str)], code: i32, out: &str, err: &str) {
    let got = xtask(args, env);
    let stdout = String::from_utf8_lossy(&got.stdout);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert_eq!(stdout, out, "stdout");
    assert_eq!(stderr, err, "stderr");
    assert_eq!(got.status.code(), Some(code), "{stdout}{stderr}");
}

/// One key's value in a classification block.
#[track_caller]
fn field<'a>(block: &'a str, key: &str) -> &'a str {
    block
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("no '{key}=' line in:\n{block}"))
}

/// The block a run with no diff to reason about writes, held to the keys the
/// workflow reads and to the jobs it turns on.
fn force_all() -> String {
    let got = xtask(&["ci-changes"], &[("EVENT_NAME", "workflow_dispatch")]);
    assert_eq!(String::from_utf8_lossy(&got.stderr), "", "stderr");
    assert_eq!(got.status.code(), Some(0));
    let block = String::from_utf8(got.stdout).unwrap();
    let keys: Vec<&str> = block
        .lines()
        .map(|line| line.split_once('=').expect("key=value").0)
        .collect();
    assert_eq!(keys, KEYS);
    assert_eq!(field(&block, "rust"), "true");
    assert_eq!(field(&block, "manifests"), "true");
    block
}

/// A pull request whose merge base cannot be resolved says so and classifies
/// as the event that runs everything.
#[test]
fn an_unresolvable_diff_reports_the_fallback_and_runs_everything() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "pull_request")],
        0,
        &format!("note: no usable diff; running everything.\n{}", force_all()),
        "",
    );
}

/// A push with no `before..HEAD` to read says so and leaves the manifest gate
/// on. Nothing else in a push run checks the manifests.
#[test]
fn a_push_with_no_before_fails_the_manifest_gate_closed() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "push")],
        0,
        &format!(
            "note: no usable before..HEAD diff; the manifest gate fails closed.\n{}",
            force_all()
        ),
        "",
    );
}

/// A path list is classified as a pull request whatever the environment names,
/// so a push run over a documentation page selects the site alone.
#[test]
fn a_path_list_classifies_as_a_pull_request_under_any_event() {
    let dir = Dir::new("a_path_list_classifies_as_a_pull_request_under_any_event");
    let list = dir.write("paths", b"docs/a.md\0");
    held(
        &["ci-changes", "--classify-paths", &list],
        &[("EVENT_NAME", "push")],
        0,
        SITE_ONLY,
        "",
    );
}

/// A path list that cannot be read names the file and the reason, and writes
/// no block a job could act on.
#[test]
fn a_path_list_that_cannot_be_read_is_refused() {
    let dir = Dir::new("a_path_list_that_cannot_be_read_is_refused");
    let missing = dir.path().join("absent").to_string_lossy().into_owned();
    held(
        &["ci-changes", "--classify-paths", &missing],
        &[("EVENT_NAME", "push")],
        1,
        "",
        &format!("xtask: {missing}: No such file or directory (os error 2)\n"),
    );
}

/// `$GITHUB_OUTPUT` receives the block stdout carried, appended to what the
/// steps before this one wrote.
#[test]
fn the_github_output_file_gains_the_block_that_was_printed() {
    let dir = Dir::new("the_github_output_file_gains_the_block_that_was_printed");
    let list = dir.write("paths", b"docs/a.md\0");
    let earlier = "an-earlier-step=value\n";
    let output = dir.write("github-output", earlier.as_bytes());

    held(
        &["ci-changes", "--classify-paths", &list],
        &[("EVENT_NAME", "push"), ("GITHUB_OUTPUT", &output)],
        0,
        SITE_ONLY,
        "",
    );
    assert_eq!(
        std::fs::read_to_string(&output).unwrap(),
        format!("{earlier}{SITE_ONLY}")
    );
}

/// The `changes` outputs `ci.yml` reads and the keys the classifier writes are
/// the same set. A job gated on a key nothing writes compares the empty string
/// to `'true'` and skips every run; a key nothing reads selects nothing.
///
/// The key runs to the end of its identifier, `_` and digits included, so a
/// `bench_shards` typo fails here rather than truncating to a `bench` that is
/// in `KEYS`.
#[test]
fn the_workflow_and_the_classifier_agree_on_the_output_keys() {
    let text = ci_yml();

    let mut seen: Vec<&str> = Vec::new();
    for prefix in ["needs.changes.outputs.", "steps.classify.outputs."] {
        for (at, _) in text.match_indices(prefix) {
            let rest = &text[at + prefix.len()..];
            let end = rest
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
                .unwrap_or(rest.len());
            let key = &rest[..end];
            assert!(
                KEYS.contains(&key),
                "{prefix}{key} is read by ci.yml and written by nothing"
            );
            seen.push(key);
        }
    }
    assert!(!seen.is_empty(), "ci.yml reads no classifier output");
    for key in KEYS {
        assert!(
            seen.contains(&key),
            "the classifier writes {key} and ci.yml reads it nowhere"
        );
    }
}

/// Every key reaches the `changes` job's `outputs:` mapping, so a job's `if:`
/// never reads a key the job does not export.
#[test]
fn every_key_is_exported_by_the_changes_job() {
    let text = ci_yml();
    for key in KEYS {
        let line = format!("      {key}: ${{{{ steps.classify.outputs.{key} }}}}");
        assert!(
            text.lines().any(|l| l == line),
            "the changes job does not export {key}"
        );
    }
}

/// `CI gate` needs every other job, so a failing job cannot pass the gate.
#[test]
fn ci_gate_needs_every_job() {
    let text = ci_yml();
    let body = &text[text.find("\njobs:\n").unwrap()..];
    let jobs: Vec<&str> = body
        .lines()
        .filter_map(|l| l.strip_prefix("  ")?.strip_suffix(':'))
        .filter(|k| !k.starts_with(' ') && !k.starts_with('#') && *k != "ci-gate")
        .collect();
    let gate = &body[body.find("\n  ci-gate:\n").unwrap()..];
    let needs = gate
        .lines()
        .find_map(|l| l.trim().strip_prefix("needs: ["))
        .unwrap()
        .trim_end_matches(']');
    let needs: Vec<&str> = needs.split(", ").collect();
    assert!(!jobs.is_empty());
    for job in &jobs {
        assert!(needs.contains(job), "CI gate does not need {job}");
    }
}

/// The text of `ci.yml`.
fn ci_yml() -> String {
    std::fs::read_to_string(workflows().join("ci.yml")).unwrap()
}

/// `.github/workflows` in this checkout.
fn workflows() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join(".github/workflows")
}

fn yaml(text: &str) -> serde_yaml::Value {
    serde_yaml::from_str(text).unwrap()
}

/// One job of `ci.yml` that calls a workflow in this directory.
struct Unit {
    caller: String,
    caller_job: serde_yaml::Value,
    file: String,
    text: String,
}

/// Each workflow `ci.yml` calls by local path, with the job that calls it.
fn ci_units() -> Vec<Unit> {
    let ci = yaml(&ci_yml());
    let jobs = ci["jobs"].as_mapping().unwrap();
    jobs.iter()
        .filter_map(|(caller, job)| {
            let uses = job["uses"].as_str()?;
            let file = uses.strip_prefix("./.github/workflows/")?.to_owned();
            let text = std::fs::read_to_string(workflows().join(&file)).unwrap();
            Some(Unit {
                caller: caller.as_str().unwrap().to_owned(),
                caller_job: job.clone(),
                file,
                text,
            })
        })
        .collect()
}

/// Each `inputs.<key>` the text reads, the key running to the end of its
/// identifier.
fn inputs_read(text: &str) -> Vec<&str> {
    text.match_indices("inputs.")
        .map(|(at, _)| {
            let rest = &text[at + "inputs.".len()..];
            let end = rest
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .collect()
}

/// Every input a unit reads is a string input it declares as required, named
/// for a classifier key, and passed by its caller from that key. A boolean
/// input compared with `'true'` is always false, and a dropped `with:` line
/// reads as the empty string; either skips the job with `CI gate` green.
#[test]
fn every_unit_input_is_declared_passed_and_read() {
    let units = ci_units();
    assert!(!units.is_empty(), "ci.yml calls no unit");
    for unit in &units {
        let file = &unit.file;
        let parsed = yaml(&unit.text);
        let declared = parsed["on"]["workflow_call"]["inputs"]
            .as_mapping()
            .cloned()
            .unwrap_or_default();
        let read = inputs_read(&unit.text);
        for key in &read {
            assert!(
                declared.contains_key(*key),
                "{file} reads inputs.{key} and does not declare it"
            );
        }
        for (key, spec) in &declared {
            let key = key.as_str().unwrap();
            assert!(
                KEYS.contains(&key),
                "{file} declares {key}, not a classifier key"
            );
            assert_eq!(spec["type"].as_str(), Some("string"), "{file}: {key}");
            assert_eq!(spec["required"].as_bool(), Some(true), "{file}: {key}");
            assert!(
                read.contains(&key),
                "{file} declares {key} and reads it nowhere"
            );
            assert_eq!(
                unit.caller_job["with"][key].as_str(),
                Some(format!("${{{{ needs.changes.outputs.{key} }}}}").as_str()),
                "{} does not pass {key} to {file}",
                unit.caller
            );
        }
        if let Some(with) = unit.caller_job["with"].as_mapping() {
            for key in with.keys() {
                assert!(
                    declared.contains_key(key),
                    "{} passes an undeclared {key:?}",
                    unit.caller
                );
            }
        }
    }
}

/// A unit runs only as a call, and every callable workflow here has a caller.
/// `concurrency` stays on `ci.yml`, whose group every unit shares.
#[test]
fn every_unit_is_called_from_ci_and_runs_only_there() {
    let units = ci_units();
    assert!(!units.is_empty(), "ci.yml calls no unit");
    for unit in &units {
        let parsed = yaml(&unit.text);
        let on: Vec<&str> = parsed["on"]
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| k.as_str().unwrap())
            .collect();
        assert_eq!(on, ["workflow_call"], "{} has another trigger", unit.file);
        assert!(
            !unit.text.contains("concurrency:"),
            "{} sets its own concurrency",
            unit.file
        );
    }
    let mut texts = Vec::new();
    for entry in std::fs::read_dir(workflows()).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        texts.push((name, std::fs::read_to_string(&path).unwrap()));
    }
    for (name, text) in &texts {
        if yaml(text)["on"].get("workflow_call").is_none() {
            continue;
        }
        let call = format!("uses: ./.github/workflows/{name}");
        assert!(
            texts.iter().any(|(_, t)| t.contains(&call)),
            "nothing calls {name}"
        );
    }
}

/// Whether a `permissions:` value hands out an OIDC token, by name or through
/// `write-all`.
fn grants_id_token(permissions: &serde_yaml::Value) -> bool {
    match permissions {
        serde_yaml::Value::String(all) => all == "write-all",
        serde_yaml::Value::Mapping(map) => map.contains_key("id-token"),
        _ => false,
    }
}

/// No job `ci.yml` runs holds a deployment credential: none enters an
/// environment, is granted an OIDC token at the job or the workflow level, or
/// reads a Cloudflare secret, and no unit takes the name of a workflow that
/// does.
#[test]
fn no_ci_unit_enters_an_environment() {
    let units = ci_units();
    assert!(!units.is_empty(), "ci.yml calls no unit");
    let mut files = vec![("ci.yml".to_owned(), ci_yml())];
    files.extend(units.into_iter().map(|u| (u.file, u.text)));
    for (file, text) in &files {
        assert!(
            !["release.yml", "release-build.yml", "scheduled.yml"].contains(&file.as_str()),
            "ci.yml calls {file}"
        );
        assert!(
            !text.contains("secrets.CLOUDFLARE_"),
            "{file} reads a Cloudflare secret"
        );
        let parsed = yaml(text);
        assert!(
            !grants_id_token(&parsed["permissions"]),
            "{file} grants every job an OIDC token"
        );
        for (job, body) in parsed["jobs"].as_mapping().unwrap() {
            let job = job.as_str().unwrap();
            assert!(
                body.get("environment").is_none(),
                "{file}: {job} enters an environment"
            );
            assert!(
                !grants_id_token(&body["permissions"]),
                "{file}: {job} requests an OIDC token"
            );
        }
    }
}

/// The changelog and licence checks run in a unit only `CI gate` waits on, so a
/// missing release note fails the gate without skipping the test tier.
#[test]
fn tier_two_does_not_wait_on_the_changelog() {
    let units = ci_units();
    let jobs_of = |u: &Unit| -> Vec<String> {
        yaml(&u.text)["jobs"]
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| k.as_str().unwrap().to_owned())
            .collect()
    };
    let policy = units
        .iter()
        .find(|u| jobs_of(u).iter().any(|j| j == "changelog"))
        .expect("no unit holds the changelog job");
    assert!(
        jobs_of(policy).iter().any(|j| j == "deny"),
        "deny is not beside changelog"
    );
    for unit in &units {
        let needs = &unit.caller_job["needs"];
        let waits = match needs {
            serde_yaml::Value::String(one) => one == &policy.caller,
            serde_yaml::Value::Sequence(all) => all
                .iter()
                .any(|n| n.as_str() == Some(policy.caller.as_str())),
            _ => false,
        };
        assert!(!waits, "{} waits on {}", unit.caller, policy.caller);
    }
}

/// `CI gate` keeps the context name the ruleset requires, runs whatever its
/// dependencies did unless the run was cancelled, and fails on `cancelled`.
#[test]
fn the_gate_keeps_its_name_condition_and_cancel_rule() {
    let ci = yaml(&ci_yml());
    let gate = &ci["jobs"]["ci-gate"];
    assert_eq!(gate["name"].as_str(), Some("CI gate"));
    assert_eq!(gate["if"].as_str(), Some("${{ !cancelled() }}"));
    let script = gate["steps"][0]["run"].as_str().unwrap();
    assert!(
        script.contains(r#".value.result == "cancelled""#),
        "{script}"
    );
    let bad = script
        .split(r#"if [ -n "$bad" ]; then"#)
        .nth(1)
        .expect("the gate tests no bad result");
    assert!(
        bad.lines()
            .take_while(|l| l.trim() != "fi")
            .any(|l| l.trim() == "exit 1"),
        "the gate does not fail on a bad result"
    );
}

/// `setup-rust` sets the debuginfo level every job builds with before the
/// cache step hashes `CARGO*` variables into its key, and no workflow it serves
/// sets them at the top, where a called workflow would not see them.
#[test]
fn setup_rust_exports_the_profile_env_before_the_cache() {
    let action =
        std::fs::read_to_string(workflows().join("../actions/setup-rust/action.yml")).unwrap();
    let steps = yaml(&action)["runs"]["steps"]
        .as_sequence()
        .unwrap()
        .clone();
    let export = steps
        .iter()
        .position(|s| {
            s["run"].as_str().is_some_and(|r| {
                r.contains(r#">>"$GITHUB_ENV""#)
                    && r.contains("CARGO_TERM_COLOR=always")
                    && r.contains("CARGO_PROFILE_DEV_DEBUG=line-tables-only")
                    && r.contains("CARGO_PROFILE_TEST_DEBUG=line-tables-only")
            })
        })
        .expect("setup-rust exports no CARGO_PROFILE_* debuginfo level");
    let cache = steps
        .iter()
        .position(|s| {
            s["uses"]
                .as_str()
                .is_some_and(|u| u.starts_with("Swatinem/rust-cache@"))
        })
        .expect("setup-rust has no rust-cache step");
    assert!(export < cache, "the export follows the cache step");
    let mut files = vec![("ci.yml".to_owned(), ci_yml())];
    files.extend(ci_units().into_iter().map(|u| (u.file, u.text)));
    for (file, text) in &files {
        assert!(
            yaml(text).get("env").is_none(),
            "{file} sets a workflow-level env"
        );
    }
}

/// A directory of this run's own, removed with its contents.
struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-ci-changes-{name}.{}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Writes one file, answering the path to give the child.
    fn write(&self, name: &str, body: &[u8]) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, body).unwrap();
        path.to_string_lossy().into_owned()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}
