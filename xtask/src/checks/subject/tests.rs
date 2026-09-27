use std::path::Path;

use super::*;

/// The areas this tree names, read from the real `crates/`.
fn tree_areas() -> Vec<String> {
    areas(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
}

/// `ok` or the first problem's opening words, for a subject under the commit
/// limit.
fn verdict(subject: &str) -> String {
    problems(subject, &tree_areas(), Some(LIMIT))
        .first()
        .map_or_else(|| "ok".to_owned(), Clone::clone)
}

#[test]
fn a_crate_area_is_its_directory_less_the_prefix() {
    let areas = tree_areas();
    for area in [
        "core",
        "kafka",
        "clickhouse-derive",
        "coordination",
        "spate",
    ] {
        assert!(
            areas.iter().any(|a| a == area),
            "{area} missing from {areas:?}"
        );
    }
    assert!(!areas.iter().any(|a| a.starts_with("spate-")), "{areas:?}");
    for area in AREAS {
        assert!(
            areas.iter().any(|a| a == area),
            "{area} missing from {areas:?}"
        );
    }
}

#[test]
fn the_rule_over_a_table_of_subjects() {
    let table: &[(&str, &str)] = &[
        (
            "kafka: start a partition's fetcher before its lane is handed out",
            "ok",
        ),
        ("core: `Clock` moves into spate_core::clock", "ok"),
        ("coordination: make a TLS alert fatal", "ok"),
        ("spate: re-export the clock module", "ok"),
        (
            "workspace: bump the cargo group across 1 directory with 5 updates",
            "ok",
        ),
        ("docs: describe the admin section", "ok"),
        ("kafka: 3 retries are enough", "ok"),
        ("release: v0.3.0", "ok"),
        ("release: v0.10.12", "ok"),
        (
            "fix(spate-kafka): start the fetcher",
            "`fix(spate-kafka)` is not one area",
        ),
        ("feat!: rename the framework", "`feat!` is not one area"),
        ("kafka,core: move the clock", "`kafka,core` is not one area"),
        (
            "kafka, core: move the clock",
            "`kafka, core` is not one area",
        ),
        (
            "spate-kafka: start the fetcher",
            "`spate-kafka` is not an area",
        ),
        ("fix: start the fetcher", "`fix` is not an area"),
        ("Kafka: start the fetcher", "`Kafka` is not an area"),
        (
            "kafka: Start the fetcher",
            "the description starts with a capital",
        ),
        (
            "kafka: TLS alerts are fatal",
            "the description starts with a capital",
        ),
        ("kafka: start the fetcher.", "it ends with a period"),
        (
            "kafka:  start the fetcher",
            "the description starts with whitespace",
        ),
        ("kafka: ", "the description after the area is empty"),
        ("kafka:start the fetcher", "it names no area"),
        ("start the fetcher", "it names no area"),
        ("release: verify the tag", "`release:` is reserved"),
        ("release: v0.3", "`release:` is reserved"),
        ("release: v0.3.0-rc.1", "`release:` is reserved"),
        ("chore: release v0.3.0", "`chore` is not an area"),
    ];
    for (subject, want) in table {
        let got = verdict(subject);
        assert!(
            got.starts_with(want),
            "{subject:?}: got {got:?}, want {want:?}"
        );
    }
}

#[test]
fn every_problem_is_reported_at_once() {
    let got = problems("Fix(core): Start it.", &tree_areas(), Some(10));
    assert_eq!(got.len(), 4, "{got:?}");
}

#[test]
fn the_limit_counts_characters() {
    let areas = tree_areas();
    let exact = format!("kafka: {}", "é".repeat(LIMIT - "kafka: ".len()));
    assert_eq!(exact.chars().count(), LIMIT);
    assert!(problems(&exact, &areas, Some(LIMIT)).is_empty());
    let over = format!("{exact}x");
    assert_eq!(
        problems(&over, &areas, Some(LIMIT)),
        vec![format!(
            "it is {} characters, over the limit of {LIMIT}",
            LIMIT + 1
        )]
    );
    assert!(problems(&over, &areas, None).is_empty());
}

#[test]
fn the_subject_is_the_first_line_git_keeps() {
    let message = "\n\n# Please enter the commit message\nkafka: start it\n\nbody\n";
    assert_eq!(subject_of(message, '#'), "kafka: start it");
    assert_eq!(
        subject_of("; note\nkafka: start it\n", ';'),
        "kafka: start it"
    );
    assert_eq!(
        subject_of("#kafka: not a comment here\n", ';'),
        "#kafka: not a comment here"
    );
    assert_eq!(subject_of("kafka: start it\r\n", '#'), "kafka: start it");
    assert_eq!(subject_of("# only comments\n\n", '#'), "");
}

/// A pull request's fields for the gate.
fn pull_request(title: &str, number: &str, author: &str) -> Fields {
    Fields {
        event: "pull_request".to_owned(),
        title: title.to_owned(),
        number: number.to_owned(),
        author: author.to_owned(),
    }
}

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

#[test]
fn a_title_leaves_room_for_the_number_the_merge_appends() {
    // 64 characters, and ` (#1234)` makes 72.
    let title = format!("kafka: {}", "x".repeat(57));
    assert_eq!(title.chars().count(), 64);
    assert!(
        title_gate(
            root(),
            &pull_request(&title, "1234", "someone"),
            true,
            "pull_request"
        )
        .is_ok()
    );
    let longer = format!("{title}x");
    assert!(
        title_gate(
            root(),
            &pull_request(&longer, "1234", "someone"),
            true,
            "pull_request"
        )
        .is_err()
    );
}

#[test]
fn only_dependabot_is_let_off_the_length() {
    let title = format!(
        "ci: bump {} from 1.0.0 to 1.0.1 in /ci/clickhouse/stable",
        "x".repeat(30)
    );
    assert!(
        title_gate(
            root(),
            &pull_request(&title, "9", "dependabot[bot]"),
            true,
            "pull_request"
        )
        .is_ok()
    );
    assert!(
        title_gate(
            root(),
            &pull_request(&title, "9", "someone"),
            true,
            "pull_request"
        )
        .is_err()
    );
    assert!(
        title_gate(
            root(),
            &pull_request("chore(ci): bump x", "9", "dependabot[bot]"),
            true,
            "pull_request"
        )
        .is_err()
    );
}

#[test]
fn a_pull_request_run_without_its_fields_fails_closed() {
    assert!(title_gate(root(), &Fields::default(), true, "pull_request").is_err());
    assert!(title_gate(root(), &Fields::default(), true, "pull_request_target").is_err());
    assert!(title_gate(root(), &Fields::default(), true, "push").is_ok());
    assert!(title_gate(root(), &Fields::default(), false, "").is_ok());
    assert!(
        title_gate(
            root(),
            &pull_request("kafka: start it", "", "someone"),
            true,
            "pull_request"
        )
        .is_err()
    );
    assert!(
        title_gate(
            root(),
            &pull_request("kafka: start it", "12a", "someone"),
            true,
            "pull_request"
        )
        .is_err()
    );
}

#[test]
fn a_refusal_prints_no_workflow_command() {
    assert_eq!(printable("x\r::error::y\n\tz"), "x\\r::error::y\\n\\tz");
    assert_eq!(printable("core: `Clock` \"é\""), "core: `Clock` \"é\"");
    assert_eq!(
        printable("fix: a ##[error]forged"),
        "fix: a #\\u{23}[error]forged"
    );
    assert_eq!(printable("###[x"), "##\\u{23}[x");
    assert!(!printable("a ##[b ##[c").contains("##["));
}
