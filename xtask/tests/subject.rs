//! The process-level contract of `cargo xtask commit-msg` and `cargo xtask
//! tidy title`: the exit status each outcome reports and what reaches stderr.

use std::path::PathBuf;
use std::process::{Command, Output};

mod support;

/// The variables the title gate reads, cleared so nothing on the host reaches
/// the child.
const READS: [&str; 6] = [
    "EVENT_NAME",
    "PR_TITLE",
    "PR_NUMBER",
    "PR_AUTHOR",
    "GITHUB_ACTIONS",
    "GITHUB_EVENT_NAME",
];

fn xtask(args: &[&str], env: &[(&str, &str)]) -> Output {
    support::run(args, &READS, env)
}

/// One commit message written to a file of this test's own, removed with it.
struct Message(support::Scratch);

impl Message {
    fn new(name: &str, body: &str) -> Self {
        let dir = support::Scratch::new(&format!("spate-xtask-subject-{name}")).unwrap();
        std::fs::write(dir.join("COMMIT_EDITMSG"), body).unwrap();
        Self(dir)
    }

    fn path(&self) -> PathBuf {
        self.0.join("COMMIT_EDITMSG")
    }
}

/// A subject following the rule commits, one breaking it is refused with each
/// problem named, and a subject git generates passes through.
#[test]
fn the_hook_reads_the_subject_git_will_record() {
    let cases = [
        (
            "ok",
            "# a template comment\n\nkafka: start the fetcher\n\nWhy.\n",
            0,
        ),
        ("fixup", "fixup! kafka: start the fetcher\n", 0),
        ("squash", "squash! kafka: start the fetcher\n", 0),
        ("amend", "amend! kafka: start the fetcher\n", 0),
        ("revert", "Revert \"kafka: start the fetcher\"\n", 0),
        ("merge", "Merge branch 'main' into topic\n", 0),
        ("empty", "# nothing but comments\n", 0),
        ("typed", "fix(spate-kafka): start the fetcher\n", 1),
        ("capital", "Kafka: Bad.\n", 1),
    ];
    for (name, body, code) in cases {
        let message = Message::new(name, body);
        let out = xtask(&["commit-msg", message.path().to_str().unwrap()], &[]);
        assert_eq!(
            out.status.code(),
            Some(code),
            "{name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let message = Message::new("refusal", "Kafka: Bad.\n");
    let out = xtask(&["commit-msg", message.path().to_str().unwrap()], &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.starts_with("subject: Kafka: Bad.\n  - `Kafka` is not an area."),
        "{stderr}"
    );
    assert!(stderr.contains("\n  - it ends with a period\n"), "{stderr}");
}

/// A message file that is not there is an error, not a pass.
#[test]
fn a_missing_message_file_fails() {
    let out = xtask(&["commit-msg", "/nonexistent/spate-commit-msg"], &[]);
    assert_eq!(out.status.code(), Some(1));
}

/// The title gate counts the merge's ` (#N)` against the limit, and a run with
/// no pull request checks nothing.
#[test]
fn the_title_gate_reads_the_pull_request_fields() {
    let title = format!("kafka: {}", "x".repeat(58));
    let pr = |title: &str| {
        xtask(
            &["tidy", "title"],
            &[
                ("EVENT_NAME", "pull_request"),
                ("PR_TITLE", title),
                ("PR_NUMBER", "1234"),
                ("PR_AUTHOR", "someone"),
            ],
        )
    };
    assert_eq!(pr(&title[..title.len() - 1]).status.code(), Some(0));
    let out = pr(&title);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("it is 65 characters, over the limit of 64"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = xtask(&["tidy", "title"], &[("EVENT_NAME", "merge_group")]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "subject: no pull request title to check (EVENT_NAME='merge_group').\n"
    );
}

/// The hook skips comment lines by the repository's own `core.commentChar`: a
/// single character is used as given, and a multi-character value such as
/// `auto` falls back to `#`, so an `avro:` subject is not read as a comment.
#[test]
fn the_hook_honours_the_configured_comment_character() {
    for (name, setting, body) in [
        (
            "semicolon",
            ";",
            "; Please enter the commit message\nkafka: start it\n",
        ),
        (
            "auto",
            "auto",
            "# Please enter the commit message\navro: start it\n",
        ),
    ] {
        let scratch =
            support::Scratch::new(&format!("spate-xtask-subject-comment-{name}")).unwrap();
        let dir = scratch.dir();
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(dir)
                .env_remove("GIT_DIR")
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["init", "--quiet", "."]);
        git(&["config", "core.commentChar", setting]);
        let message = Message::new(&format!("message-{name}"), body);
        let git_dir = dir.join(".git");
        let out = support::run(
            &["commit-msg", message.path().to_str().unwrap()],
            &["GITHUB_ACTIONS"],
            &[("GIT_DIR", git_dir.to_str().unwrap())],
        );
        assert_eq!(
            out.status.code(),
            Some(0),
            "{name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
