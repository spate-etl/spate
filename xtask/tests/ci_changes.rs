//! The process-level contract of `cargo xtask ci-changes`: the event each run
//! classifies under, the note it prints when a diff cannot be resolved, the
//! bytes it appends to `$GITHUB_OUTPUT`, and the keys `ci.yml` reads back.
//! Also the structure of `ci.yml`, the units it calls and the scheduled
//! workflows.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The variables the classifier and its `git` invocations read, cleared so
/// nothing on the host reaches the child.
const READS: [&str; 9] = [
    "EVENT_NAME",
    "BASE_SHA",
    "HEAD_SHA",
    "MERGE_BASE_SHA",
    "MERGE_HEAD_SHA",
    "EVENT_BEFORE",
    "PR_AUTHOR",
    "GITHUB_OUTPUT",
    "GITHUB_ACTIONS",
];

/// The keys the workflow reads, in the order the block writes them.
const KEYS: [&str; 15] = [
    "rust",
    "site",
    "fuzz",
    "containers",
    "container-args",
    "semver",
    "semver-pkgs",
    "bench",
    "manifests",
    "advisories",
    "release-tooling",
    "macos",
    "bench-shards",
    "clickhouse-lanes",
    "s3-lanes",
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
     advisories=false\n\
     release-tooling=false\n\
     macos=false\n\
     bench-shards=[]\n\
     clickhouse-lanes=[]\n\
     s3-lanes=[]\n";

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
    assert_eq!(field(&block, "advisories"), "true");
    assert_eq!(field(&block, "macos"), "true");
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

/// A push with no `before..HEAD` to read says so and leaves the manifest and
/// advisories gates on. Nothing else in a push run checks either.
#[test]
fn a_push_with_no_before_fails_the_manifest_gate_closed() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "push")],
        0,
        &format!(
            "note: no usable before..HEAD diff; the manifest and advisories gates fail closed.\n{}",
            force_all()
        ),
        "",
    );
}

/// Runs git in `repo` with an identity and no signing, returning trimmed stdout.
fn git(repo: &Path, args: &[&str]) -> String {
    let got = Command::new("git")
        .args([
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(got.status.success(), "git {args:?}");
    String::from_utf8(got.stdout).unwrap().trim().to_owned()
}

/// Classifies a push of `before..HEAD` in a scratch repository whose last
/// commit writes `changed`, or nothing when `changed` is `None`.
fn push_of(name: &str, changed: Option<&str>) -> String {
    let repo = std::env::temp_dir().join(format!("spate-push-{name}.{}", std::process::id()));
    drop(std::fs::remove_dir_all(&repo));
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q"]);
    std::fs::write(repo.join("README.md"), "a\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "one"]);
    let before = git(&repo, &["rev-parse", "HEAD"]);
    if let Some(path) = changed {
        std::fs::write(repo.join(path), "\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "two"]);
    }
    let git_dir = repo.join(".git");
    let got = xtask(
        &["ci-changes"],
        &[
            ("EVENT_NAME", "push"),
            ("EVENT_BEFORE", &before),
            ("GIT_DIR", git_dir.to_str().unwrap()),
        ],
    );
    drop(std::fs::remove_dir_all(&repo));
    let stdout = String::from_utf8(got.stdout).unwrap();
    assert_eq!(got.status.code(), Some(0), "{stdout}");
    stdout
}

/// A push that changes `deny.toml` alone turns the advisories gate on and
/// leaves the manifest gate off, and a push that changes nothing turns both
/// off.
#[test]
fn a_push_selects_the_advisories_gate_on_its_own_diff() {
    let block = push_of("deny", Some("deny.toml"));
    assert_eq!(field(&block, "manifests"), "false");
    assert_eq!(field(&block, "advisories"), "true");
    let block = push_of("empty", None);
    assert_eq!(field(&block, "manifests"), "false");
    assert_eq!(field(&block, "advisories"), "false");
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

/// A pull request's labels select nothing: a documentation page selects the
/// site alone whatever `PR_LABELS` carries.
#[test]
fn a_label_in_the_environment_selects_nothing() {
    let dir = Dir::new("a_label_in_the_environment_selects_nothing");
    let list = dir.write("paths", b"docs/a.md\0");
    held(
        &["ci-changes", "--classify-paths", &list],
        &[("PR_LABELS", "ci: docker,ci: bench")],
        0,
        SITE_ONLY,
        "",
    );
}

/// A merge-queue entry whose base cannot be resolved runs everything, the
/// macOS lane included.
#[test]
fn an_unresolvable_merge_group_runs_everything() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "merge_group")],
        0,
        &format!("note: no usable diff; running everything.\n{}", force_all()),
        "",
    );
}

/// A lockfile change selects the macOS lane on a pull request, and a crate
/// source change leaves it to the merge queue.
#[test]
fn a_lockfile_path_reaches_the_macos_block() {
    let dir = Dir::new("a_lockfile_path_reaches_the_macos_block");
    for (paths, macos) in [
        (&b"Cargo.lock\0"[..], "true"),
        (&b"crates/spate-core/src/lib.rs\0"[..], "false"),
    ] {
        let list = dir.write("paths", paths);
        let got = xtask(&["ci-changes", "--classify-paths", &list], &[]);
        assert_eq!(got.status.code(), Some(0));
        let block = String::from_utf8(got.stdout).unwrap();
        assert_eq!(field(&block, "macos"), macos, "{block}");
    }
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
        if !declared.is_empty() {
            let needs = &unit.caller_job["needs"];
            let waits = needs.as_str() == Some("changes")
                || needs
                    .as_sequence()
                    .is_some_and(|n| n.iter().any(|j| j.as_str() == Some("changes")));
            assert!(
                waits,
                "{} reads needs.changes.outputs without needing changes",
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
    let deployers = [
        "release.yml",
        "release-build.yml",
        "nightly.yml",
        "weekly.yml",
    ];
    for name in deployers {
        assert!(workflows().join(name).is_file(), "{name} does not exist");
    }
    let units = ci_units();
    assert!(!units.is_empty(), "ci.yml calls no unit");
    let mut files = vec![("ci.yml".to_owned(), ci_yml())];
    files.extend(units.into_iter().map(|u| (u.file, u.text)));
    for (file, text) in &files {
        assert!(!deployers.contains(&file.as_str()), "ci.yml calls {file}");
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
    files.extend(SCHEDULED.map(|f| (f.to_owned(), read_workflow(f))));
    for (file, text) in &files {
        assert!(
            yaml(text).get("env").is_none(),
            "{file} sets a workflow-level env"
        );
    }
}

/// The workflows that run on a cron.
const SCHEDULED: [&str; 2] = ["nightly.yml", "weekly.yml"];

/// The text of one file in `.github/workflows`.
fn read_workflow(name: &str) -> String {
    let path = workflows().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Each job key of a parsed workflow, with its body.
fn jobs(parsed: &serde_yaml::Value) -> Vec<(String, serde_yaml::Value)> {
    parsed["jobs"]
        .as_mapping()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.as_str().unwrap().to_owned(), v.clone()))
        .collect()
}

/// A job's `needs`, whether written as one name or a list.
fn needs(job: &serde_yaml::Value) -> Vec<String> {
    match &job["needs"] {
        serde_yaml::Value::String(one) => vec![one.clone()],
        serde_yaml::Value::Sequence(all) => {
            all.iter().map(|n| n.as_str().unwrap().to_owned()).collect()
        }
        _ => Vec::new(),
    }
}

/// Each scheduled workflow fires on one cron or a manual dispatch, so no job
/// has to work out which cron started the run.
#[test]
fn each_scheduled_workflow_has_one_cron_and_a_dispatch() {
    for file in SCHEDULED {
        let text = read_workflow(file);
        let parsed = yaml(&text);
        let on: Vec<&str> = parsed["on"]
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| k.as_str().unwrap())
            .collect();
        assert_eq!(on, ["schedule", "workflow_dispatch"], "{file}");
        assert_eq!(
            parsed["on"]["schedule"].as_sequence().map(Vec::len),
            Some(1),
            "{file} has more than one cron"
        );
        assert!(
            !text.contains("github.event.schedule"),
            "{file} reads which cron fired"
        );
        assert!(
            parsed["jobs"].get("plan").is_none(),
            "{file} has a plan job"
        );
    }
}

/// A cron run leaves `inputs.tier` empty, so every nightly job is gated on not
/// being a `docs` dispatch. A gate on `== 'nightly'` would skip the whole night
/// and report green.
#[test]
fn the_nightly_jobs_run_when_the_cron_leaves_the_tier_empty() {
    let parsed = yaml(&read_workflow("nightly.yml"));
    let tier = &parsed["on"]["workflow_dispatch"]["inputs"]["tier"];
    assert_eq!(tier["default"].as_str(), Some("nightly"));
    let options: Vec<&str> = tier["options"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|o| o.as_str().unwrap())
        .collect();
    assert_eq!(options, ["nightly", "docs"]);
    let ungated = [
        "docs-build",
        "docs-deploy",
        "publish-dry-run-report",
        "report",
    ];
    assert!(
        parsed["jobs"]["docs-build"].get("if").is_none(),
        "docs-build does not run on every trigger"
    );
    for (name, job) in jobs(&parsed) {
        let cond = job["if"].as_str().unwrap_or("").trim();
        if ungated.contains(&name.as_str()) {
            assert!(
                !cond.contains("inputs.tier"),
                "nightly.yml: {name} reads the tier"
            );
        } else {
            assert_eq!(cond, "inputs.tier != 'docs'", "nightly.yml: {name}");
        }
    }
    assert!(
        !read_workflow("weekly.yml").contains("inputs."),
        "weekly.yml reads an input"
    );
}

/// Each scheduled job runs in the file whose cadence its purpose needs.
#[test]
fn each_scheduled_job_runs_in_one_file() {
    let keys = |file| -> Vec<String> {
        let mut k: Vec<String> = jobs(&yaml(&read_workflow(file)))
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        k.sort();
        k
    };
    assert_eq!(
        keys("nightly.yml"),
        [
            "advisories",
            "attribution",
            "docs-build",
            "docs-deploy",
            "examples-containers",
            "fuzz",
            "loom",
            "minimal-versions",
            "osv",
            "publish-dry-run",
            "publish-dry-run-report",
            "release-verify",
            "report",
        ]
    );
    assert_eq!(
        keys("weekly.yml"),
        [
            "bench",
            "beta",
            "coverage-containers",
            "features-powerset",
            "report",
        ]
    );
}

/// In each scheduled workflow a failed job files one issue: its own, the one
/// the job that needs it files, or the `report` job's, under a title that
/// names the workflow.
#[test]
fn every_scheduled_job_reports_its_failure_once() {
    let mut titles = Vec::new();
    for file in SCHEDULED {
        let parsed = yaml(&read_workflow(file));
        let all = jobs(&parsed);
        let files_own = |job: &serde_yaml::Value| {
            job["steps"].as_sequence().unwrap().iter().any(|s| {
                s["uses"].as_str() == Some("./.github/actions/file-issue")
                    || s["run"]
                        .as_str()
                        .is_some_and(|r| r.contains("gh issue create"))
            })
        };
        let mut reported: Vec<String> = Vec::new();
        for (name, job) in &all {
            if name != "report" && files_own(job) {
                reported.push(name.clone());
                reported.extend(needs(job));
            }
        }
        let report = &parsed["jobs"]["report"];
        let report_needs = needs(report);
        for (name, _) in &all {
            if name == "report" {
                continue;
            }
            let in_report = report_needs.contains(name);
            let elsewhere = reported.contains(name);
            assert!(
                in_report != elsewhere,
                "{file}: {name} is reported {}",
                if in_report { "twice" } else { "nowhere" }
            );
        }
        for need in &report_needs {
            assert!(
                all.iter().any(|(n, _)| n == need),
                "{file}: report needs {need}"
            );
        }
        let title = report["steps"]
            .as_sequence()
            .unwrap()
            .iter()
            .find_map(|s| s["with"]["title"].as_str())
            .unwrap()
            .to_owned();
        let tier = file.trim_end_matches(".yml");
        assert!(title.to_lowercase().contains(tier), "{file}: {title}");
        titles.push(title);
    }
    assert_ne!(titles[0], titles[1]);
}

/// A job that files an issue about the jobs it needs runs when one of them
/// failed. Without `always()` its `if` is ANDed with `success()`, so it would
/// skip exactly then.
#[test]
fn each_scheduled_reporter_runs_when_a_need_failed() {
    for file in SCHEDULED {
        for (name, job) in jobs(&yaml(&read_workflow(file))) {
            if needs(&job).is_empty() {
                continue;
            }
            let files_issue = job["steps"]
                .as_sequence()
                .unwrap()
                .iter()
                .any(|s| s["uses"].as_str() == Some("./.github/actions/file-issue"));
            if !files_issue {
                continue;
            }
            let cond = job["if"].as_str().unwrap_or("");
            assert!(
                cond.trim_start().starts_with("always() &&"),
                "{file}: {name} skips when a job it needs fails"
            );
        }
    }
}

/// The step that runs `bench ab` times out below its job, and the step that
/// writes the job summary runs under `always()`, so a failed or cut
/// measurement still reports what the kept legs hold.
#[test]
fn the_weekly_summary_runs_after_a_cut_measurement() {
    let parsed = yaml(&read_workflow("weekly.yml"));
    let job = &parsed["jobs"]["bench"];
    let steps = job["steps"].as_sequence().unwrap();
    let run = |s: &serde_yaml::Value| s["run"].as_str().unwrap_or("").to_owned();
    let measure = steps
        .iter()
        .find(|s| run(s).contains("cargo xtask bench ab "))
        .expect("a step runs bench ab");
    let summary = steps
        .iter()
        .find(|s| run(s).contains("GITHUB_STEP_SUMMARY"))
        .expect("a step writes the summary");
    let job_limit = job["timeout-minutes"].as_u64().unwrap();
    let step_limit = measure["timeout-minutes"].as_u64().unwrap();
    assert!(step_limit < job_limit, "{step_limit} >= {job_limit}");
    let cond = summary["if"].as_str().unwrap_or("");
    assert!(cond.trim_start().starts_with("always() &&"), "{cond}");
    assert!(
        run(summary).contains("bench compare"),
        "the summary re-renders from the legs"
    );
}

/// Each scheduled workflow queues a second run behind the first and never
/// cancels one: a cancelled run ends `cancelled`, which its `report` job does
/// not file, and a release's docs dispatch waits for a running night.
#[test]
fn each_scheduled_workflow_queues_and_never_cancels() {
    for file in SCHEDULED {
        let parsed = yaml(&read_workflow(file));
        let group = format!("{}-${{{{ github.ref }}}}", file.trim_end_matches(".yml"));
        assert_eq!(
            parsed["concurrency"]["group"].as_str(),
            Some(group.as_str()),
            "{file}"
        );
        assert_eq!(
            parsed["concurrency"]["cancel-in-progress"].as_bool(),
            Some(false),
            "{file}"
        );
    }
}

/// The site deploy is the one job that enters `cloudflare-workers`, and it keeps
/// the posture a production credential needs: read-only token, `main` only,
/// serialised and never cancelled, no checkout or restored cache, an artifact
/// from its own run, and npm lifecycle scripts off around the token.
#[test]
fn the_docs_deploy_holds_its_credential_alone() {
    let mut deployers = Vec::new();
    for entry in std::fs::read_dir(workflows()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        let file = path.file_name().unwrap().to_str().unwrap().to_owned();
        let parsed = yaml(&std::fs::read_to_string(&path).unwrap());
        for (name, job) in jobs(&parsed) {
            let env = job["environment"]["name"]
                .as_str()
                .or_else(|| job["environment"].as_str());
            if env == Some("cloudflare-workers") {
                deployers.push((file.clone(), name.clone(), job.clone()));
            } else {
                assert!(
                    !serde_yaml::to_string(&job)
                        .unwrap()
                        .contains("secrets.CLOUDFLARE_"),
                    "{file}: {name} reads a Cloudflare secret"
                );
            }
        }
    }
    assert_eq!(deployers.len(), 1, "jobs entering cloudflare-workers");
    let (file, name, job) = &deployers[0];
    assert_eq!(
        (file.as_str(), name.as_str()),
        ("nightly.yml", "docs-deploy")
    );
    assert_eq!(job["permissions"], yaml("contents: read"));
    assert_eq!(
        job["if"].as_str().map(str::trim),
        Some("github.ref == 'refs/heads/main'"),
        "the deploy runs off main"
    );
    assert!(needs(job).contains(&"docs-build".to_owned()));
    assert_eq!(
        job["concurrency"],
        yaml("group: cloudflare-workers\ncancel-in-progress: false")
    );
    let steps = job["steps"].as_sequence().unwrap();
    for step in steps {
        let uses = step["uses"].as_str().unwrap_or("");
        let action = uses.split('@').next().unwrap();
        assert!(
            ![
                "actions/checkout",
                "actions/cache",
                "actions/cache/restore",
                "./.github/actions/setup-rust",
            ]
            .contains(&action),
            "the deploy uses {action}"
        );
        if action == "actions/download-artifact" {
            assert!(step["with"].get("run-id").is_none());
            assert!(step["with"].get("github-token").is_none());
        }
        if action == "cloudflare/wrangler-action" {
            assert_eq!(
                step["env"]["NPM_CONFIG_IGNORE_SCRIPTS"].as_str(),
                Some("true")
            );
        }
    }
    assert!(
        steps.iter().any(|s| s["uses"]
            .as_str()
            .is_some_and(|u| u.starts_with("cloudflare/wrangler-action@"))),
        "the deploy has no wrangler step"
    );
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

/// The advisories check alone waits on a dependency change: the policy half
/// blocks on every event, the advisories half reads the classifier's key
/// through `env:`, and nothing in the unit lets a failure through.
#[test]
fn the_advisory_check_alone_waits_on_a_dependency_change() {
    let unit = ci_units()
        .into_iter()
        .find(|u| u.file == "policy.yml")
        .expect("ci.yml calls policy.yml");
    let parsed = yaml(&unit.text);
    for (name, job) in parsed["jobs"].as_mapping().unwrap() {
        assert!(job.get("continue-on-error").is_none(), "{name:?}");
        for step in job["steps"].as_sequence().unwrap() {
            assert!(
                step.get("continue-on-error").is_none(),
                "{name:?}: {step:?}"
            );
        }
    }
    let steps = parsed["jobs"]["deny"]["steps"].as_sequence().unwrap();
    let at = steps
        .iter()
        .position(|s| s["run"].as_str() == Some("cargo xtask deny policy"))
        .expect("a policy step");
    assert_eq!(steps[at].get("if"), None);
    assert_eq!(steps[at].get("env"), None);
    let advisories = &steps[at + 1];
    assert_eq!(
        advisories["run"].as_str(),
        Some(r#"cargo xtask deny advisories --dependency-change "$DEPENDENCY_CHANGE""#)
    );
    assert_eq!(
        advisories["env"],
        yaml("DEPENDENCY_CHANGE: ${{ inputs.advisories }}")
    );
    assert_eq!(advisories["if"].as_str(), Some("${{ !cancelled() }}"));
    let deny_steps = steps
        .iter()
        .filter(|s| s["run"].as_str().is_some_and(|r| r.contains("xtask deny")))
        .count();
    assert_eq!(deny_steps, 2);
}

/// The arm64 lane runs what the x64 jobs run: the container suites through the
/// one matrix job, and the workspace suite with the `test` job's condition and
/// nextest arguments, less the coverage wrapper. No job or step of the lane
/// lets a failure through to `CI gate`.
#[test]
fn the_arm64_lane_runs_what_the_x64_jobs_run() {
    for (file, name) in [
        ("tests.yml", "test-arm64"),
        ("containers.yml", "containers"),
    ] {
        let parsed = yaml(&read_workflow(file));
        let job = &parsed["jobs"][name];
        assert!(job.get("continue-on-error").is_none(), "{file}");
        for step in job["steps"].as_sequence().unwrap() {
            assert!(step.get("continue-on-error").is_none(), "{file}: {step:?}");
        }
    }

    let containers = yaml(&read_workflow("containers.yml"));
    let job = &containers["jobs"]["containers"];
    assert_eq!(job["runs-on"].as_str(), Some("${{ matrix.runner }}"));
    let legs = job["strategy"]["matrix"]["include"].as_sequence().unwrap();
    assert_eq!(
        *legs,
        vec![
            yaml("{arch: x64, runner: ubuntu-latest}"),
            yaml("{arch: arm64, runner: ubuntu-26.04-arm}"),
        ]
    );

    let tests = yaml(&read_workflow("tests.yml"));
    let x64 = &tests["jobs"]["test"];
    let arm64 = &tests["jobs"]["test-arm64"];
    assert_eq!(arm64["runs-on"].as_str(), Some("ubuntu-26.04-arm"));
    assert_eq!(arm64["if"], x64["if"]);
    let nextest = |job: &serde_yaml::Value| -> Vec<String> {
        let runs: Vec<&str> = job["steps"]
            .as_sequence()
            .unwrap()
            .iter()
            .filter_map(|s| s["run"].as_str())
            .filter(|r| r.contains("nextest"))
            .collect();
        assert_eq!(runs.len(), 1, "{runs:?}");
        runs[0].split_whitespace().map(str::to_owned).collect()
    };
    let x64 = nextest(x64);
    // `cargo llvm-cov nextest` is `cargo nextest run` under coverage.
    assert_eq!(x64[..3], ["cargo", "llvm-cov", "nextest"]);
    let mut expected = vec!["cargo".to_owned(), "nextest".to_owned(), "run".to_owned()];
    let mut words = x64.into_iter().skip(3);
    while let Some(word) = words.next() {
        match word.as_str() {
            "--lcov" => {}
            "--output-path" | "--ignore-filename-regex" => drop(words.next()),
            _ => expected.push(word),
        }
    }
    assert_eq!(nextest(arm64), expected);
}

/// The counted tier runs each shard on the runner the classifier names for its
/// architecture, keys both caches and each artifact by that architecture, and
/// warms one cache entry per architecture the shards carry. No job or step
/// lets a failure through to `CI gate` except the merge-base leg.
#[test]
fn the_counted_tier_runs_on_each_architecture() {
    let dir = Dir::new("the_counted_tier_runs_on_each_architecture");
    let list = dir.write("paths", b".github/workflows/perf.yml\0");
    let got = xtask(&["ci-changes", "--classify-paths", &list], &[]);
    assert_eq!(got.status.code(), Some(0));
    let block = String::from_utf8(got.stdout).unwrap();
    let shards: Vec<serde_json::Value> =
        serde_json::from_str(field(&block, "bench-shards")).unwrap();
    let pair = |leg: &dyn Fn(&str) -> String| (leg("arch"), leg("runner"));
    let emitted: std::collections::BTreeSet<(String, String)> = shards
        .iter()
        .map(|s| pair(&|k| s[k].as_str().unwrap().to_owned()))
        .collect();
    assert_eq!(emitted.len(), 2, "{block}");

    let perf = yaml(&read_workflow("perf.yml"));
    let counters = &perf["jobs"]["perf-counters"];
    assert_eq!(counters["runs-on"].as_str(), Some("${{ matrix.runner }}"));
    assert_eq!(
        counters["strategy"]["matrix"]["include"].as_str(),
        Some("${{ fromJSON(inputs.bench-shards) }}")
    );
    let steps = counters["steps"].as_sequence().unwrap();
    let with_of = |action: &str| -> Vec<&serde_yaml::Value> {
        steps
            .iter()
            .filter(|s| s["uses"].as_str().unwrap_or_default().starts_with(action))
            .map(|s| &s["with"])
            .collect()
    };
    let keys = with_of("actions/cache@");
    assert_eq!(keys.len(), 2);
    for with in keys {
        let key = with["key"].as_str().unwrap();
        assert!(key.contains("${{ runner.arch }}"), "{key}");
    }
    let uploads = with_of("actions/upload-artifact@");
    assert_eq!(uploads.len(), 1);
    let name = uploads[0]["name"].as_str().unwrap();
    assert!(name.starts_with("perf-summaries-"), "{name}");
    assert!(name.contains("${{ matrix.arch }}"), "{name}");

    let warm = &perf["jobs"]["perf-warm"];
    assert_eq!(warm["runs-on"].as_str(), Some("${{ matrix.runner }}"));
    let legs: std::collections::BTreeSet<(String, String)> = warm["strategy"]["matrix"]["include"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|l| pair(&|k| l[k].as_str().unwrap().to_owned()))
        .collect();
    assert_eq!(legs, emitted);

    for name in ["perf-warm", "perf-counters"] {
        let job = &perf["jobs"][name];
        assert!(job.get("continue-on-error").is_none(), "{name}");
        for step in job["steps"].as_sequence().unwrap() {
            if step["id"].as_str() == Some("base") {
                continue;
            }
            assert!(step.get("continue-on-error").is_none(), "{name}: {step:?}");
        }
    }
}

/// The macOS lane runs the arm64 lane's workspace suite on its own runner,
/// gated by the classifier's `macos` key alone, and lets no failure through
/// to `CI gate`.
#[test]
fn the_macos_lane_runs_what_the_arm64_lane_runs() {
    let tests = yaml(&read_workflow("tests.yml"));
    let arm64 = &tests["jobs"]["test-arm64"];
    let macos = &tests["jobs"]["test-macos"];
    assert_eq!(macos["runs-on"].as_str(), Some("macos-26"));
    assert_eq!(macos["if"].as_str(), Some("inputs.macos == 'true'"));
    assert!(macos["timeout-minutes"].as_u64().is_some());
    assert!(macos.get("continue-on-error").is_none());
    // Every step matches once the check and artifact names, which differ per
    // lane, are set aside.
    let steps = |job: &serde_yaml::Value| -> Vec<serde_yaml::Value> {
        let mut steps = job["steps"].as_sequence().unwrap().clone();
        for step in &mut steps {
            assert!(step.get("continue-on-error").is_none(), "{step:?}");
            let uses = step["uses"].as_str().unwrap_or_default().to_owned();
            if let Some(with) = step.get_mut("with").and_then(|w| w.as_mapping_mut()) {
                if uses.starts_with("mikepenz/action-junit-report@") {
                    with.remove("check_name");
                } else if uses.starts_with("actions/upload-artifact@") {
                    with.remove("name");
                }
            }
        }
        steps
    };
    assert_eq!(steps(macos), steps(arm64));
}

/// Each JUnit check and artifact in a CI run has its own name: upload-artifact
/// rejects a duplicate within one run, and two checks sharing a name cannot be
/// told apart. A matrix job's name carries a matrix value, since every leg
/// publishes one.
#[test]
fn each_report_and_artifact_in_a_ci_run_has_its_own_name() {
    let mut texts = vec![ci_yml()];
    texts.extend(ci_units().into_iter().map(|u| u.text));
    let mut seen = std::collections::BTreeSet::new();
    for text in &texts {
        for (job_name, job) in jobs(&yaml(text)) {
            let matrix = job["strategy"].get("matrix").is_some();
            for step in job["steps"].as_sequence().into_iter().flatten() {
                let uses = step["uses"].as_str().unwrap_or_default();
                let key = if uses.starts_with("mikepenz/action-junit-report@") {
                    "check_name"
                } else if uses.starts_with("actions/upload-artifact@") {
                    "name"
                } else {
                    continue;
                };
                let name = step["with"][key].as_str().unwrap().to_owned();
                assert!(
                    !matrix || name.contains("${{ matrix."),
                    "{job_name}: {name:?}"
                );
                assert!(seen.insert((key, name.clone())), "{job_name}: {name:?}");
            }
        }
    }
}
