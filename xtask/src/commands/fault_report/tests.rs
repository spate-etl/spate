use super::*;

const ROUTES: [Route; 4] = [
    Route::Delivery,
    Route::Worker,
    Route::Expectation,
    Route::Harness,
];

/// One field of an issue form as the form file declares it.
#[derive(Debug, Default)]
struct FormField {
    id: String,
    label: String,
    render: Option<String>,
    options: Vec<String>,
}

fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn form_file(name: &str) -> String {
    fs::read_to_string(repo().join(".github/ISSUE_TEMPLATE").join(name)).unwrap()
}

/// The non-markdown fields of an issue form, in order, read line by line from
/// the layout the forms use: `  - type:` opens an item, `    id:` and
/// `      label:`/`render:` sit at fixed indents, and options are
/// `        - ` items, with a `label: ` prefix on checkboxes.
fn form_fields(yaml: &str) -> Vec<FormField> {
    let unquote = |s: &str| s.trim().trim_matches(|c| c == '"' || c == '\'').to_owned();
    let mut fields: Vec<(String, FormField)> = Vec::new();
    for line in yaml.lines() {
        if let Some(kind) = line.strip_prefix("  - type: ") {
            fields.push((kind.trim().to_owned(), FormField::default()));
            continue;
        }
        let Some((_, field)) = fields.last_mut() else {
            continue;
        };
        if let Some(id) = line.strip_prefix("    id: ") {
            field.id = unquote(id);
        } else if let Some(label) = line.strip_prefix("      label: ") {
            field.label = unquote(label);
        } else if let Some(render) = line.strip_prefix("      render: ") {
            field.render = Some(unquote(render));
        } else if let Some(option) = line.strip_prefix("        - ") {
            let option = option.strip_prefix("label: ").unwrap_or(option);
            field.options.push(unquote(option));
        }
    }
    fields
        .into_iter()
        .filter(|(kind, _)| kind != "markdown")
        .map(|(_, f)| f)
        .collect()
}

fn declared(fields: &[Field]) -> Vec<(String, String, Option<String>)> {
    fields
        .iter()
        .map(|f| {
            (
                f.id.to_owned(),
                f.label.to_owned(),
                f.render.map(str::to_owned),
            )
        })
        .collect()
}

fn read(form: &[FormField]) -> Vec<(String, String, Option<String>)> {
    form.iter()
        .map(|f| (f.id.clone(), f.label.clone(), f.render.clone()))
        .collect()
}

fn options<'a>(form: &'a [FormField], id: &str) -> &'a [String] {
    &form.iter().find(|f| f.id == id).unwrap().options
}

fn context() -> Context {
    Context {
        run_url: "https://example.invalid/runs/1".to_owned(),
        sha: "0123abcd".to_owned(),
        toolchain: "rustc 1.97.0, Linux x86_64".to_owned(),
        features: "spate-s3: testing".to_owned(),
        seed: None,
    }
}

fn violation(property: u8, check: &str) -> Violation {
    Violation {
        property,
        check: check.to_owned(),
        key: Some("split.a".to_owned()),
        rev: Some(7),
        instance: Some("w1".to_owned()),
        pid: Some(4242),
        detail: "found it".to_owned(),
    }
}

fn run(scenario: &str, violations: Vec<Violation>) -> RunDir {
    let mut run = RunDir {
        name: format!("{scenario}-00000000000000ff"),
        outcome: RunOutcome {
            scenario: scenario.to_owned(),
            store: "nats".to_owned(),
            instances: 3,
            seed: 0xff,
            replay: format!("cargo xtask fault-test --seed 0x00000000000000ff {scenario}"),
            stage: "oracle".to_owned(),
            message: "w1 (pid 4242) exited Some(2)".to_owned(),
            violations,
            expectations: Vec::new(),
            faults_fired: vec![FaultFired {
                incarnation: "w1-1".to_owned(),
                fault: "kill at 1500 ms".to_owned(),
                fired: true,
            }],
        },
        ..RunDir::default()
    };
    run.configs
        .insert("w0-1".to_owned(), r#"{"instance":"w0"}"#.to_owned());
    run.configs
        .insert("w1-1".to_owned(), r#"{"instance":"w1"}"#.to_owned());
    run.stderr.insert(
        "w1-2".to_owned(),
        "spate-faults-worker: lease lost\n".to_owned(),
    );
    run.record = Some("o001-r000002".to_owned());
    run.events = ["rows", "send", "kill", "respawn"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    run
}

fn summary(kinds: &[Kind], tests_passed: bool) -> Summary {
    Summary {
        seed: "0x00000000000000ff".to_owned(),
        tests_passed,
        outcomes: kinds
            .iter()
            .enumerate()
            .map(|(i, kind)| ScenarioOutcome {
                scenario: format!("s{i}"),
                kind: *kind,
                message: String::new(),
                replay: String::new(),
                dir: format!("s{i}-00000000000000ff"),
            })
            .collect(),
        exit_code: 1,
    }
}

fn headings(body: &str) -> Vec<&str> {
    body.lines()
        .filter_map(|l| l.strip_prefix("### "))
        .collect()
}

/// The answer under the heading `label`, up to the next heading.
fn answer<'a>(body: &'a str, label: &str) -> &'a str {
    let start = body.find(&format!("### {label}\n\n")).unwrap() + label.len() + 6;
    let rest = &body[start..];
    rest.find("\n### ").map_or(rest, |end| &rest[..end])
}

/// Each failing kind files under its own route, so a worker failure or a
/// failed expectation never reaches the delivery form, and `harness` files
/// only when nothing else does.
#[test]
fn report_prints_a_route_per_kind_and_harness_only_alone() {
    let all = summary(
        &[
            Kind::Harness,
            Kind::Expectation,
            Kind::Pass,
            Kind::Worker,
            Kind::Violation,
        ],
        false,
    );
    assert_eq!(
        routes(Some(&all)),
        [Route::Delivery, Route::Worker, Route::Expectation]
    );
    assert_eq!(
        routes(Some(&summary(&[Kind::Worker, Kind::Harness], false))),
        [Route::Worker]
    );
    assert_eq!(
        routes(Some(&summary(&[Kind::Expectation], false))),
        [Route::Expectation]
    );
    assert_eq!(
        routes(Some(&summary(&[Kind::Pass, Kind::Harness], false))),
        [Route::Harness]
    );
    assert_eq!(routes(Some(&summary(&[Kind::Pass], true))), []);
}

/// A failed nextest run that left no failing outcome, or no summary, files
/// under `harness`.
#[test]
fn a_run_with_no_failing_outcome_files_under_harness() {
    assert_eq!(routes(Some(&summary(&[], false))), [Route::Harness]);
    assert_eq!(
        routes(Some(&summary(&[Kind::Pass], false))),
        [Route::Harness]
    );
    assert_eq!(routes(None), [Route::Harness]);
}

/// The renderer's delivery fields are the delivery form's fields, with the
/// same ids, labels and render languages, in the same order.
#[test]
fn delivery_fields_match_the_delivery_form() {
    let form = form_fields(&form_file("1-delivery-correctness.yml"));
    assert_eq!(read(&form), declared(&DELIVERY));
}

/// The renderer's bug fields are the bug form's fields, with the same ids,
/// labels and render languages, in the same order.
#[test]
fn bug_fields_match_the_bug_form() {
    let form = form_fields(&form_file("2-bug.yml"));
    assert_eq!(read(&form), declared(&BUG));
}

/// Every dropdown answer the bodies give is one of its form's options, and
/// the checkbox list is the form's in full.
#[test]
fn answers_are_options_of_their_forms() {
    let delivery = form_fields(&form_file("1-delivery-correctness.yml"));
    for answer in [LOST, AHEAD, DUPLICATES, NOT_SURE] {
        assert!(
            options(&delivery, "guarantee").iter().any(|o| o == answer),
            "{answer}"
        );
    }
    for answer in COMPONENTS {
        assert!(
            options(&delivery, "components").iter().any(|o| o == answer),
            "{answer}"
        );
    }
    assert_eq!(options(&delivery, "circumstances"), CIRCUMSTANCES);
    let bug = form_fields(&form_file("2-bug.yml"));
    for answer in [WHERE_COORDINATION, WHERE_CI] {
        assert!(
            options(&bug, "components").iter().any(|o| o == answer),
            "{answer}"
        );
    }
}

/// The form reader sees a relabelled, added or reordered field, so the
/// field checks above fail on a form edit.
#[test]
fn the_form_reader_sees_a_changed_field() {
    let text = form_file("1-delivery-correctness.yml");
    let baseline = read(&form_fields(&text));
    for edited in [
        text.replace("label: Logs", "label: Log output"),
        text.replace("render: yaml", "render: json"),
        text.replace(
            "  - type: textarea\n    id: logs",
            "  - type: input\n    id: extra\n    attributes:\n      label: Extra\n\n  - type: textarea\n    id: logs",
        ),
        text.replace("    id: version", "    id: release"),
    ] {
        assert_ne!(read(&form_fields(&edited)), baseline);
    }
}

/// The delivery body has the form's labels as `###` headings in order, then
/// Replay and Artifacts.
#[test]
fn delivery_body_follows_the_form_labels_in_order() {
    let form = form_fields(&form_file("1-delivery-correctness.yml"));
    let body = render(
        Route::Delivery,
        &context(),
        &[run(
            "nats_three_instances",
            vec![violation(3, "AheadOfRows")],
        )],
    );
    let mut expected: Vec<&str> = form.iter().map(|f| f.label.as_str()).collect();
    expected.extend(["Replay", "Artifacts"]);
    assert_eq!(headings(&body), expected);
}

/// Each `render:` field's answer sits in a fence of the form's language, and
/// no other field's answer is fenced at its start.
#[test]
fn render_fields_use_the_forms_fences() {
    let cases = [
        ("1-delivery-correctness.yml", Route::Delivery),
        ("2-bug.yml", Route::Worker),
        ("2-bug.yml", Route::Expectation),
        ("2-bug.yml", Route::Harness),
    ];
    for (file, route) in cases {
        let body = render(
            route,
            &context(),
            &[run(
                "dynamodb_one_instance",
                vec![violation(1, "RecordMissing")],
            )],
        );
        for field in form_fields(&form_file(file)) {
            let answer = answer(&body, &field.label);
            match &field.render {
                Some(lang) => assert!(
                    answer.starts_with(&format!("```{lang}\n")),
                    "{route:?} {}: {answer}",
                    field.label
                ),
                None => assert!(
                    !answer.starts_with("```"),
                    "{route:?} {}: {answer}",
                    field.label
                ),
            }
        }
    }
}

/// The worker, expectation and harness bodies have the bug form's labels as
/// headings in order, then Replay and Artifacts, and each names what its
/// route reports.
#[test]
fn worker_expectation_and_harness_bodies_follow_the_bug_form() {
    let form = form_fields(&form_file("2-bug.yml"));
    let mut expected: Vec<&str> = form.iter().map(|f| f.label.as_str()).collect();
    expected.extend(["Replay", "Artifacts"]);
    let mut failed = run("nats_one_instance", Vec::new());
    failed.outcome.expectations = vec!["fault not exercised: no err_after_land line".to_owned()];
    for route in [Route::Worker, Route::Expectation, Route::Harness] {
        let body = render(route, &context(), std::slice::from_ref(&failed));
        assert_eq!(headings(&body), expected, "{route:?}");
        assert!(
            answer(&body, "Replay")
                .contains("cargo xtask fault-test --seed 0x00000000000000ff nats_one_instance"),
            "{route:?}"
        );
    }
    let worker = render(Route::Worker, &context(), std::slice::from_ref(&failed));
    assert!(answer(&worker, "What happened").contains("w1 (pid 4242) exited Some(2)"));
    assert!(
        answer(&worker, "Output").contains("==> w1-2.stderr <==\nspate-faults-worker: lease lost")
    );
    let expectation = render(
        Route::Expectation,
        &context(),
        std::slice::from_ref(&failed),
    );
    assert!(
        answer(&expectation, "What happened")
            .contains("- fault not exercised: no err_after_land line")
    );
    failed.outcome.stage = "running".to_owned();
    failed.outcome.message = "container nats was down at t_ms 17".to_owned();
    let harness = render(Route::Harness, &context(), std::slice::from_ref(&failed));
    let what = answer(&harness, "What happened");
    assert!(what.contains("Stage: running."), "{what}");
    assert!(what.contains("container nats was down"), "{what}");
    assert!(what.contains("Rerun the job"), "{what}");
    assert_eq!(answer(&harness, "Where").trim(), WHERE_CI);
    assert_eq!(answer(&worker, "Where").trim(), WHERE_COORDINATION);
}

/// The harness body for a run that wrote no `summary.json` says so, leaves
/// the seed to the run log and still renders every field.
#[test]
fn a_harness_body_without_a_summary_renders_every_field() {
    let body = render(Route::Harness, &context(), &[]);
    let form = form_fields(&form_file("2-bug.yml"));
    let mut expected: Vec<&str> = form.iter().map(|f| f.label.as_str()).collect();
    expected.extend(["Replay", "Artifacts"]);
    assert_eq!(headings(&body), expected);
    let what = answer(&body, "What happened");
    assert!(what.contains("wrote no `summary.json`"), "{what}");
    assert!(what.contains("The run log has its seed"), "{what}");
    assert!(what.contains("Rerun the job"), "{what}");
    assert_eq!(answer(&body, "Replay").trim(), "_No response_");
}

/// A harness body for a failed nextest run with no failing outcome names the
/// summary's seed and replays it.
#[test]
fn runless_harness_body_names_the_seed() {
    let dir = std::env::temp_dir().join(format!("arb-fault-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let runs = dir.join("runs");
    let out = dir.join("out");
    fs::create_dir_all(&runs).unwrap();
    let mut s = summary(&[Kind::Pass], false);
    s.seed = "0x1234abcd5678ef00".to_owned();
    s.exit_code = 3;
    fs::write(runs.join("summary.json"), serde_json::to_vec(&s).unwrap()).unwrap();
    let routes = report(&runs.join("summary.json"), &out, &context()).unwrap();
    let body = fs::read_to_string(out.join("harness.md")).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    assert_eq!(routes, [Route::Harness]);
    let what = answer(&body, "What happened");
    assert!(
        what.contains("nextest failed under seed 0x1234abcd5678ef00"),
        "{what}"
    );
    assert!(
        what.contains("no scenario left a failing outcome"),
        "{what}"
    );
    assert_eq!(
        answer(&body, "Replay").trim(),
        "```sh\ncargo xtask fault-test --seed 0x1234abcd5678ef00\n```"
    );
}

/// The What broke answer is the option for the lowest-numbered property
/// violated, followed by every property violated.
#[test]
fn what_broke_follows_the_lowest_violated_property() {
    let cases = [
        (
            vec![violation(5, "TwoOwners"), violation(1, "RecordMissing")],
            LOST,
        ),
        (
            vec![
                violation(4, "Unfinished"),
                violation(2, "UnexplainedDuplicate"),
            ],
            DUPLICATES,
        ),
        (
            vec![violation(5, "EpochRegressed"), violation(3, "AheadOfRows")],
            AHEAD,
        ),
        (vec![violation(4, "Unfinished")], NOT_SURE),
        (vec![violation(5, "TwoOwners")], NOT_SURE),
        (Vec::new(), NOT_SURE),
    ];
    for (violations, option) in cases {
        let body = render(Route::Delivery, &context(), &[run("s", violations)]);
        let what = answer(&body, "What broke");
        assert!(what.starts_with(option), "{what}");
    }
    let body = render(
        Route::Delivery,
        &context(),
        &[run(
            "s",
            vec![violation(5, "TwoOwners"), violation(1, "RecordMissing")],
        )],
    );
    assert!(answer(&body, "What broke").contains(
        "Properties violated: 1 (every record arrives), 5 (no two owners commit on one split)."
    ));
}

/// A property 1 violation of only `RecordUnknown` is not answered as lost.
#[test]
fn record_unknown_is_not_answered_as_lost() {
    let r = run("s", vec![violation(1, "RecordUnknown")]);
    let body = render(Route::Delivery, &context(), &[r]);
    let a = answer(&body, "What broke");
    assert!(!a.starts_with(LOST), "RecordUnknown answered as lost: {a}");
}

/// How you know lists each violation with its property, key, revision,
/// writer and pid, then the other failing scenarios.
#[test]
fn evidence_names_each_violation_and_the_other_scenarios() {
    let body = render(
        Route::Delivery,
        &context(),
        &[
            run("a", vec![violation(5, "TwoOwners")]),
            run("b", vec![violation(1, "RecordMissing")]),
        ],
    );
    let evidence = answer(&body, "How you know");
    assert!(
        evidence
            .contains("- P5 TwoOwners on `split.a` at rev 7, written by w1 (pid 4242): found it")
    );
    assert!(evidence.contains("Other scenarios with violations:\n\n- `b` on nats, 3 instance(s)"));
    assert!(answer(&body, "Replay").contains("--seed 0x00000000000000ff b"));
}

/// A hard kill is ticked for kills and aborts, an instance joining or leaving
/// for respawns and stops, and steady state only when neither applies.
#[test]
fn checkboxes_tick_kills_aborts_respawns_and_stops() {
    let ticked = |events: &[&str]| {
        let mut r = run("s", Vec::new());
        r.events = events.iter().map(|e| (*e).to_owned()).collect();
        let body = render(Route::Delivery, &context(), &[r]);
        answer(&body, "Was any of this happening at the time?")
            .lines()
            .filter_map(|l| l.strip_prefix("- [x] "))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let (joining, kill, steady) = (CIRCUMSTANCES[0], CIRCUMSTANCES[2], CIRCUMSTANCES[5]);
    assert_eq!(ticked(&["rows", "kill"]), [kill]);
    assert_eq!(ticked(&["abort"]), [kill]);
    assert_eq!(ticked(&["respawn"]), [joining]);
    assert_eq!(ticked(&["sigstop"]), [joining]);
    assert_eq!(ticked(&["stop"]), [joining]);
    assert_eq!(ticked(&["kill", "respawn"]), [joining, kill]);
    assert_eq!(ticked(&["rows", "send", "done"]), [steady]);
}

/// The schedule block lists each fault drawn with whether it fired.
#[test]
fn circumstances_list_the_faults_drawn() {
    let body = render(Route::Delivery, &context(), &[run("s", Vec::new())]);
    assert!(
        answer(&body, "Was any of this happening at the time?")
            .contains("```text\nw1-1: kill at 1500 ms (fired)\n```")
    );
}

/// The config shown is the first config of the instance the first attributed
/// violation names, else of the first worker with stderr output, else the
/// first config.
#[test]
fn config_is_the_violating_instances() {
    let mut attributed = run("s", vec![violation(5, "TwoOwners")]);
    attributed.stderr.clear();
    let body = render(Route::Delivery, &context(), &[attributed]);
    assert_eq!(
        answer(&body, "Pipeline configuration").trim(),
        "```yaml\n# w1-1.json\n{\"instance\":\"w1\"}\n```"
    );
    let mut unattributed = run("s", Vec::new());
    unattributed.stderr.clear();
    let body = render(Route::Delivery, &context(), &[unattributed]);
    assert!(answer(&body, "Pipeline configuration").contains("# w0-1.json"));
    let body = render(Route::Worker, &context(), &[run("s", Vec::new())]);
    assert!(
        answer(&body, "Configuration").contains("# w1-1.json"),
        "the worker with stderr output"
    );
}

/// Output keeps the last 200 lines of each worker's stderr.
#[test]
fn output_keeps_the_last_200_lines_of_each_stderr() {
    let mut r = run("s", Vec::new());
    let long: String = (0..250).map(|i| format!("line {i}\n")).collect();
    r.stderr.insert("w0-1".to_owned(), long);
    let body = render(Route::Worker, &context(), &[r]);
    let output = answer(&body, "Output");
    assert!(
        output.contains("==> w0-1.stderr <==\nline 50\n"),
        "{output}"
    );
    assert!(!output.contains("line 49\n"));
    assert!(output.contains("line 249\n"));
    assert!(output.contains("==> w1-2.stderr <=="));
}

/// A body stays under GitHub's 65,536-character limit however much the run
/// produced.
#[test]
fn bodies_stay_under_the_issue_size_limit() {
    let mut huge = run(
        "s",
        (0..5_000).map(|_| violation(1, "RecordMissing")).collect(),
    );
    huge.outcome.message = "m".repeat(200_000);
    huge.outcome.expectations = vec!["e".repeat(200_000); 100];
    for i in 0..20 {
        huge.stderr.insert(
            format!("w{i}-1"),
            format!("{}\n", "x".repeat(300)).repeat(400),
        );
    }
    huge.configs.insert("w1-1".to_owned(), "c".repeat(100_000));
    let mut runs = vec![huge];
    for i in 0..30 {
        let mut other = run(&format!("o{i}"), Vec::new());
        other.outcome.message = "m".repeat(50_000);
        runs.push(other);
    }
    for route in ROUTES {
        let body = render(route, &context(), &runs);
        assert!(
            body.chars().count() < 65_536,
            "{route:?}: {}",
            body.chars().count()
        );
    }
}

/// A fence around text holding backticks is longer than any run in it.
#[test]
fn fences_outlast_backticks_in_the_answer() {
    assert_eq!(fence("plain"), "```");
    assert_eq!(fence("a ``` b"), "````");
}

/// The features line lists each `spate-*` dependency with the features it
/// enables.
#[test]
fn features_list_each_spate_dependency() {
    let manifest = r#"
[dependencies]
serde = { workspace = true, features = ["derive"] }
spate-coordination = { workspace = true, features = ["dynamodb", "nats", "testing"] }
spate-core = { workspace = true }
spate-s3 = { workspace = true, features = ["testing"] }
"#;
    assert_eq!(
        features(manifest).unwrap(),
        "spate-coordination: dynamodb, nats, testing; spate-s3: testing"
    );
    assert!(
        !features(&fs::read_to_string(repo().join("faults/Cargo.toml")).unwrap())
            .unwrap()
            .is_empty()
    );
}

/// DEVELOPING.md names each route's issue title.
#[test]
fn developing_md_names_every_title() {
    let text = fs::read_to_string(repo().join("DEVELOPING.md")).unwrap();
    for route in ROUTES {
        assert!(text.contains(route.title()), "{}", route.title());
    }
}

/// `report` writes a body and a title per route from the run directories
/// beside the summary, leaving out empty stderr files, and reports a missing
/// summary as `harness`.
#[test]
fn report_writes_a_body_and_title_per_route() {
    let dir = std::env::temp_dir().join(format!("xtask-fault-report-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let runs = dir.join("runs");
    let out = dir.join("out");
    let mut s = summary(&[Kind::Violation, Kind::Worker], false);
    s.outcomes[0].dir = "a-1".to_owned();
    s.outcomes[1].dir = "b-1".to_owned();
    for (name, scenario) in [("a-1", "a"), ("b-1", "b")] {
        let d = runs.join(name);
        fs::create_dir_all(&d).unwrap();
        fs::write(
            d.join("outcome.json"),
            format!(
                r#"{{"scenario":"{scenario}","store":"nats","instances":1,"seed":1,"replay":"r {scenario}","stage":"oracle","kind":"violation","message":"m","violations":[{{"property":4,"check":"Unfinished","key":"split.x","rev":null,"instance":null,"pid":null,"detail":"d"}}],"expectations":[],"faults_fired":[]}}"#
            ),
        )
        .unwrap();
        fs::write(d.join("w0-1.json"), "{}").unwrap();
        fs::write(d.join("w0-1.stderr"), "boom\n").unwrap();
        fs::write(d.join("w1-1.stderr"), "").unwrap();
        fs::write(
            d.join("w0-1.ndjson"),
            "{\"t_ms\":1,\"ev\":\"rows\",\"ids\":[\"o000-r000001\"]}\n{\"t_ms\":2,\"ev\":\"ab",
        )
        .unwrap();
        fs::write(
            d.join("faults.ndjson"),
            "{\"t_ms\":1,\"ev\":\"kill\",\"instance\":\"w0\",\"pid\":1}\n",
        )
        .unwrap();
    }
    fs::write(runs.join("summary.json"), serde_json::to_vec(&s).unwrap()).unwrap();
    let routes = report(&runs.join("summary.json"), &out, &context()).unwrap();
    assert_eq!(routes, [Route::Delivery, Route::Worker]);
    let delivery = fs::read_to_string(out.join("delivery.md")).unwrap();
    assert!(answer(&delivery, "How you know").contains("P4 Unfinished on `split.x`"));
    assert!(answer(&delivery, "A sample record").contains(r#"{"k":"o000-r000001","pad":"…"}"#));
    assert!(answer(&delivery, "Logs").contains("boom"));
    assert!(
        !delivery.contains("w1-1.stderr"),
        "an empty stderr is left out"
    );
    assert!(
        answer(&delivery, "Was any of this happening at the time?")
            .contains(&format!("- [x] {}", CIRCUMSTANCES[2]))
    );
    assert_eq!(
        fs::read_to_string(out.join("delivery.title")).unwrap(),
        Route::Delivery.title()
    );
    assert!(
        fs::read_to_string(out.join("worker.md"))
            .unwrap()
            .contains("`b` on nats")
    );
    assert!(!out.join("harness.md").exists());

    let routes = report(&dir.join("absent/summary.json"), &out, &context()).unwrap();
    assert_eq!(routes, [Route::Harness]);
    assert!(out.join("harness.md").exists());
    fs::remove_dir_all(&dir).unwrap();
}
