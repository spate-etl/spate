use super::*;

/// The classifier's verdict, spelt as the table spells it.
fn verdict(subject: &str) -> &'static str {
    if needs_entry(subject) {
        "need"
    } else {
        "exempt"
    }
}

/// The crate scopes, read from `crates/`. A tenth crate must not become exempt
/// by being left out of a list.
fn crate_scopes() -> Vec<String> {
    let root = crate::repo_root().unwrap();
    let mut out: Vec<String> = std::fs::read_dir(root.join("crates"))
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    out.sort();
    out
}

/// The classifier's verdict on every row of the table, each row grouped under
/// what it is there to hold.
#[test]
fn the_classifier_agrees_with_the_table() {
    const TABLE: &[(&str, &str)] = &[
        // Crate-scoped and user-visible: a fragment is required.
        ("feat(spate-core): a windowed operator", "need"),
        ("fix(spate-kafka): stop dropping offsets on revoke", "need"),
        ("perf(spate-clickhouse): halve the encode cost", "need"),
        ("feat(spate-core,docs): a thing and its page", "need"),
        (
            "fix(spate-core,docs): stop the quarantine wait consuming the ladder",
            "need",
        ),
        (
            "feat(spate-avro,bench): decode datums straight into typed records",
            "need",
        ),
        // Crate-scoped, but the type says nobody upgrading cares.
        (
            "docs(spate-core): rewrite the module documentation",
            "exempt",
        ),
        (
            "test(spate-kafka): retry the container suite once",
            "exempt",
        ),
        ("chore(spate-core): tidy an import", "exempt"),
        ("refactor(spate-core): extract a helper", "exempt"),
        ("style(spate-kafka): rustfmt", "exempt"),
        ("ci(spate-core): pin an action", "exempt"),
        // Reverting a release and moving an MSRV floor are not that.
        ("revert(spate-core): back out the windowed operator", "need"),
        ("build(spate-core): raise the MSRV floor to 1.95", "need"),
        // A user-visible type, but the scope names no crate.
        (
            "feat(docs): give Spate a mark that works on a square canvas",
            "exempt",
        ),
        ("feat(ci): a new job", "exempt"),
        ("feat(website): restyle the navigation", "exempt"),
        (
            "fix(bench): pin the iteration count for both legs",
            "exempt",
        ),
        ("fix(ci,docs): lowercase the Pages project name", "exempt"),
        // The automation's own subjects, verbatim from its config.
        (
            "chore(workspace): bump the cargo-compatible group",
            "exempt",
        ),
        ("chore(ci): bump mikepenz/action-junit-report", "exempt"),
        ("chore(website): bump typescript in /website", "exempt"),
        ("chore(examples): bump a dependency", "exempt"),
        ("chore: release v0.2.0", "exempt"),
        // The breaking marker decides on its own, before either axis.
        ("refactor(spate-core)!: rename a public trait", "need"),
        ("perf(spate-kafka)!: change the batch shape", "need"),
        ("feat(spate-s3)!: fence the split leases", "need"),
        // `docs(workspace)!:` is real history (c6a7a5c) and it carried a
        // BREAKING CHANGE to `breaker.open_for` inside a documentation scope.
        // Reading the scope first exempted it and 0.2.0 shipped without the
        // entry.
        ("docs(workspace)!: migrate CLAUDE.md to AGENTS.md", "need"),
        ("chore(ci)!: drop a workflow input", "need"),
        (
            "test(spate-core)!: rename a test helper somebody imports",
            "need",
        ),
        // No scope is not an exemption. All five are real history.
        ("refactor!: rename the framework to spate", "need"),
        (
            "feat!: leader-computed sticky assignment for source coordination",
            "need",
        ),
        (
            "feat: dynamic work-stealing source coordination over NATS JetStream KV",
            "need",
        ),
        (
            "feat: multi-sink split — per-type ClickHouse tables from one pipeline",
            "need",
        ),
        (
            "feat: record-aware sink sharding with ClickHouse Distributed parity",
            "need",
        ),
        // Nothing unparseable gets a free pass.
        (
            "Relicense under Apache-2.0, drop the LGPL dependency",
            "need",
        ),
        ("Update the readme", "need"),
        ("WIP", "need"),
        // One keystroke from an exemption is not an exemption.
        ("feature(spate-core): the type is misspelt", "need"),
        ("feat(spate-kafkaa): the scope is misspelt", "need"),
        ("feat(sapte-core): the scope is transposed", "need"),
        // Tolerated spellings that must still classify.
        ("feat( spate-core ): a spaced scope", "need"),
        ("FEAT(spate-core): a shouty type", "need"),
        ("DOCS(spate-core): a shouty exemption", "exempt"),
        ("Refactor(spate-core): a titled exemption", "exempt"),
    ];

    for (subject, want) in TABLE {
        assert_eq!(verdict(subject), *want, "{subject}");
    }
}

/// Every crate under `crates/` is a scope the classifier reads, and the type
/// axis still exempts one. The table above stays green if the crate list is
/// never read, because every scope would become unrecognized and every case
/// would still classify as `need`.
#[test]
fn every_crate_is_read_on_the_scope_axis_and_exempted_on_the_type_axis() {
    let crates = crate_scopes();
    assert!(
        !crates.is_empty(),
        "no crate scopes derived from crates/, so this guard is checking nothing"
    );
    for name in &crates {
        assert_eq!(
            verdict(&format!("feat({name}): x")),
            "need",
            "'feat({name}): x' is exempt, so crates/ is not being read"
        );
        assert_eq!(
            verdict(&format!("docs({name}): x")),
            "exempt",
            "'docs({name}): x' needs a fragment, so the type axis is dead"
        );
    }
}

/// Every exempt scope exempts a user-visible type, and none of them names a
/// crate.
#[test]
fn every_exempt_scope_exempts_and_names_no_crate() {
    let root = crate::repo_root().unwrap();
    assert_eq!(
        EXEMPT_SCOPES,
        ["ci", "docs", "examples", "bench", "workspace", "website"],
        "a scope left this list stops exempting, and one added starts"
    );
    for scope in EXEMPT_SCOPES {
        assert_eq!(
            verdict(&format!("feat({scope}): x")),
            "exempt",
            "'{scope}' is in EXEMPT_SCOPES but still requires a fragment"
        );
        assert!(
            !root.join("crates").join(scope).is_dir(),
            "EXEMPT_SCOPES names 'crates/{scope}', a crate"
        );
    }
}

/// The five user-visible types require a fragment under a crate scope, and
/// every type on the internal list is exempt under one.
#[test]
fn both_type_axes_are_read() {
    for kind in ["feat", "fix", "perf", "revert", "build"] {
        assert_eq!(verdict(&format!("{kind}(spate-core): x")), "need", "{kind}");
    }
    for kind in INTERNAL_TYPES {
        assert_eq!(
            verdict(&format!("{kind}(spate-core): x")),
            "exempt",
            "{kind}"
        );
    }
}

/// A subject the pattern does not match requires a fragment, whatever it looks
/// like.
#[test]
fn an_unparseable_subject_requires_a_fragment() {
    for subject in [
        "",
        ":",
        "feat:",
        "feat: ",
        "feat:\t",
        "(spate-core): x",
        "feat(a)(b): x",
        "feat(a)b: x",
        "feat(a)!x: y",
        "feat(a: x",
        "feat2(ci): x",
        "(ci): x",
        "9(ci): x",
        "9feat: x",
        "docs:",
        "docs: ",
        "docs:\t",
        "féat: x",
        "Merge branch 'main' into topic",
    ] {
        assert!(needs_entry(subject), "{subject:?}");
    }
}

/// The scope group runs to the first `)`, and a type is ASCII letters.
#[test]
fn a_subject_parses_into_its_three_fields() {
    assert_eq!(
        parse_subject("feat(a(b): x"),
        Some(Parsed {
            kind: "feat",
            scopes: "a(b",
            bang: false
        })
    );
    assert_eq!(
        parse_subject("FEAT(spate-core)!:x"),
        Some(Parsed {
            kind: "FEAT",
            scopes: "spate-core",
            bang: true
        })
    );
    assert_eq!(
        parse_subject("feat: \u{a0}"),
        Some(Parsed {
            kind: "feat",
            scopes: "",
            bang: false
        }),
        "only ASCII whitespace separates the colon from the text"
    );
    assert_eq!(parse_subject("feat(a)(b): x"), None);
}

/// A comma splits the scope list, a trailing comma closes the last scope, and
/// an empty list has no scopes at all.
#[test]
fn a_scope_list_splits_on_commas() {
    assert_eq!(scope_list(""), Vec::<&str>::new());
    assert_eq!(scope_list("a,b"), ["a", "b"]);
    assert_eq!(scope_list("a,,b"), ["a", "", "b"]);
    assert_eq!(scope_list("a,"), ["a"]);
    assert_eq!(scope_list(",a"), ["", "a"]);
    assert_eq!(scope_list(","), [""]);
    assert_eq!(scope_list(",,"), ["", ""]);
    assert_eq!(scope_list(" a , b "), [" a ", " b "]);
}

/// An empty scope is not a known scope, so it reaches a crate.
#[test]
fn an_empty_scope_reaches_a_crate() {
    assert_eq!(verdict("feat(): x"), "need");
    assert_eq!(verdict("feat(,): x"), "need");
    assert_eq!(verdict("feat( ): x"), "need");
    assert_eq!(verdict("docs(): x"), "exempt");
}

/// A scope list needs a fragment when any one of its scopes is unrecognized.
#[test]
fn any_unrecognized_scope_requires_a_fragment() {
    assert_eq!(verdict("feat(docs,ci): x"), "exempt");
    assert_eq!(verdict("feat(docs,spate-core): x"), "need");
    assert_eq!(verdict("feat(spate-core,docs): x"), "need");
    assert_eq!(verdict("feat( docs , ci ): x"), "exempt");
}

/// Membership is a run of the list's own spelling, so a scope holding a space
/// can name two of them at once.
#[test]
fn membership_matches_a_space_delimited_run() {
    assert!(in_list(EXEMPT_SCOPES, "docs"));
    assert!(!in_list(EXEMPT_SCOPES, "doc"));
    assert!(!in_list(EXEMPT_SCOPES, ""));
    assert!(in_list(EXEMPT_SCOPES, "docs examples"));
}

/// The breaking marker decides before either axis, including for a type and a
/// scope both exempt.
#[test]
fn the_breaking_marker_decides_before_either_axis() {
    assert_eq!(verdict("docs(docs)!: x"), "need");
    assert_eq!(verdict("chore(ci)!: x"), "need");
    assert_eq!(verdict("docs(docs): x"), "exempt");
}

/// A fragment filename carries its type, one directory level down, and nothing
/// else counts as one.
#[test]
fn a_fragment_filename_carries_its_type() {
    assert_eq!(
        fragment_type("changelog.d/retry-ladder.fixed.md"),
        Some("fixed")
    );
    assert_eq!(fragment_type("retry-ladder.fixed.md"), Some("fixed"));
    assert_eq!(fragment_type(".fixed.md"), Some("fixed"));
    assert_eq!(fragment_type("changelog.d/README.md"), None);
    assert_eq!(fragment_type("changelog.d/x.typo.md"), None);
    assert_eq!(fragment_type("changelog.d/x.fixed.txt"), None);
    assert_eq!(fragment_type("changelog.d/x.fixed"), None);
    assert_eq!(fragment_type("changelog.d/fixed.md"), None);
    // A release globs one level, so the gate would pass and the release would
    // omit it.
    assert_eq!(fragment_type("changelog.d/sub/x.fixed.md"), None);
}

/// A fragment has to say something, where a blank outside ASCII says nothing
/// and a byte that is not UTF-8 says something.
#[test]
fn an_empty_fragment_is_not_prose() {
    assert!(!has_prose(b""));
    assert!(!has_prose(b"   \n\n\t\n"));
    assert!(has_prose(b"A real note.\n"));
    assert!(!has_prose("\u{a0}\u{2003}\n".as_bytes()));
    assert!(has_prose(b"\xff"));
}

/// The trailer is read in any casing, with whatever spacing, and a sentence
/// that merely opens with the word is not one.
#[test]
fn the_trailer_is_read_in_any_casing() {
    assert!(trailer_says_none("Changelog: none"));
    assert!(trailer_says_none("changelog:none"));
    assert!(trailer_says_none("CHANGELOG:   NONE   "));
    assert!(trailer_says_none("Signed-off-by: A\nChangelog: none\r"));
    assert!(!trailer_says_none("Changelog: none but"));
    assert!(!trailer_says_none("Changelogs: none"));
    assert!(!trailer_says_none(" Changelog: none"));
    assert!(!trailer_says_none(""));
}

/// The offending line pads the subject so the origins line up, and a subject
/// past the column is not truncated.
#[test]
fn an_offending_line_pads_the_subject() {
    assert_eq!(
        offender_line("feat: x", "commit abc1234"),
        format!("    feat: x{} (commit abc1234)", " ".repeat(63))
    );
    let long = "f".repeat(80);
    assert_eq!(
        offender_line(&long, "pull request title"),
        format!("    {long} (pull request title)")
    );
}

/// A `git` invocation that failed, and one that answered with nothing, both
/// answer nothing here.
#[test]
fn a_git_invocation_that_answered_nothing_yields_none() {
    let root = crate::repo_root().unwrap();
    assert_eq!(
        capture(&root, &["rev-parse", "no-such-ref-xyz^{commit}"]),
        None
    );
    assert_eq!(
        capture(&root, &["ls-files", "--", "no-such-path-xyz"]),
        None
    );
    assert!(capture(&root, &["rev-parse", "HEAD"]).is_some());
}

/// An event the pull request gate did not raise evaluates nothing, and a pull
/// request with no merge base is a refusal.
#[test]
fn the_comparison_follows_the_event() {
    let root = crate::repo_root().unwrap();
    for event in ["merge_group", "push", "schedule", "workflow_dispatch"] {
        let fields = Fields {
            event: event.to_owned(),
            ..Fields::default()
        };
        assert_eq!(select(&root, &fields).unwrap(), Mode::Structure, "{event}");
    }
    let fields = Fields {
        event: "pull_request".to_owned(),
        base_sha: "deadbee".to_owned(),
        ..Fields::default()
    };
    let refused = select(&root, &fields).unwrap_err();
    assert_eq!(
        refused.message,
        "no merge base for deadbee..?. Does the checkout still set fetch-depth: 0?"
    );
}

/// A pull request run inside GitHub Actions that reached structure-only would
/// report success having evaluated nothing.
#[test]
fn a_pull_request_that_evaluated_nothing_is_refused() {
    let required = Mode::Require {
        base: "abc".to_owned(),
        head: None,
    };
    assert!(evaluated_nothing(&Mode::Structure, true, "pull_request"));
    assert!(!evaluated_nothing(&Mode::Structure, false, "pull_request"));
    assert!(!evaluated_nothing(&Mode::Structure, true, "push"));
    assert!(!evaluated_nothing(&required, true, "pull_request"));
}

/// The pull request title is a subject in its own right, and each of its lines
/// is one, so a title carrying a second line cannot smuggle one past the gate.
#[test]
fn the_title_is_a_subject_of_its_own() {
    let root = crate::repo_root().unwrap();
    let got = subjects(&root, "HEAD..HEAD", "feat(ci): a\n\nfeat(spate-core): b");
    assert_eq!(
        got,
        vec![
            Subject {
                text: "feat(ci): a".to_owned(),
                origin: "pull request title".to_owned(),
                source: Source::Body,
            },
            Subject {
                text: "feat(spate-core): b".to_owned(),
                origin: "pull request title".to_owned(),
                source: Source::Body,
            },
        ]
    );
    assert!(subjects(&root, "HEAD..HEAD", "").is_empty());
}

/// A commit's subject and its short sha come back as one subject, split at the
/// tab between them, so a subject carrying spaces survives whole.
#[test]
fn a_commit_subject_names_the_commit_it_came_from() {
    let repo = Repo::new("a_commit_subject_names_the_commit_it_came_from");
    let base = repo.git(&["rev-parse", "HEAD"]);
    let text = "fix(spate-kafka): stop dropping offsets on revoke";
    let head = repo.commit(text);
    let short = repo.git(&["log", "-1", "--format=%h", &head]);

    assert_eq!(
        subjects(repo.path(), &format!("{base}..{head}"), ""),
        vec![Subject {
            text: text.to_owned(),
            origin: format!("commit {short}"),
            source: Source::Commit(short),
        }]
    );
}

/// A range git cannot resolve yields no subjects, and the gate carries on.
#[test]
fn an_unresolvable_range_yields_no_subjects() {
    let root = crate::repo_root().unwrap();
    assert!(subjects(&root, "0000000000000000000000000000000000000000..HEAD", "").is_empty());
}

/// A working directory of this test's own.
fn scratch(name: &str) -> Scratch {
    Scratch::new(&format!("spate-xtask-changelog-{name}")).unwrap()
}

/// A throwaway repository, removed with its contents. Each test names its own,
/// because `cargo test` runs them in one process.
struct Repo(Scratch);

impl Repo {
    fn new(name: &str) -> Self {
        let repo = Self(scratch(name));
        repo.git(&["init", "--quiet", "-b", "main", "."]);
        repo.write("changelog.d/README.md", "the conventions\n");
        repo.git(&["add", "-A"]);
        repo.commit("chore: the first commit");
        repo
    }

    fn path(&self) -> &std::path::Path {
        self.0.dir()
    }

    fn git(&self, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .current_dir(self.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn write(&self, rel: &str, body: &str) {
        let path = self.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// Commits everything in the tree, answering the new commit's sha.
    fn commit(&self, message: &str) -> String {
        self.git(&["add", "-A"]);
        self.git(&["commit", "--quiet", "--allow-empty", "-m", message]);
        self.git(&["rev-parse", "HEAD"])
    }
}

/// The laptop arm prefers `origin/main`, and a branch whose merge base is the
/// tip itself gives it nothing to compare against.
#[test]
fn the_laptop_arm_reads_the_first_upstream_that_is_behind_head() {
    let repo = Repo::new("the_laptop_arm_reads_the_first_upstream_that_is_behind_head");
    let first = repo.git(&["rev-parse", "HEAD"]);
    let second = repo.commit("chore: a second commit");
    assert_eq!(laptop_base(repo.path()), None, "main is the tip itself");

    repo.git(&["checkout", "--quiet", "-b", "topic"]);
    repo.commit("feat(spate-core): a thing");
    assert_eq!(laptop_base(repo.path()), Some(second.clone()));

    repo.git(&["update-ref", "refs/remotes/origin/main", &first]);
    assert_eq!(laptop_base(repo.path()), Some(first));
}

/// A trailer on a commit excuses that commit's subject alone. A subject with
/// nothing excusing it is reported with where it came from, and the pull
/// request title is reported even where a commit carries the same text.
#[test]
fn a_trailer_excuses_one_subject_and_leaves_the_rest() {
    let repo = Repo::new("a_trailer_excuses_one_subject_and_leaves_the_rest");
    let scratch = scratch("a_trailer_excuses_one_subject_and_leaves_the_rest");
    let excused = repo.commit("feat(spate-core): never released\n\nChangelog: none\n");
    let subjects = vec![
        Subject {
            text: "feat(spate-core): never released".to_owned(),
            origin: "commit abc1234".to_owned(),
            source: Source::Commit(excused),
        },
        Subject {
            text: "fix(spate-kafka): a real one".to_owned(),
            origin: "commit def5678".to_owned(),
            source: Source::Commit(repo.commit("fix(spate-kafka): a real one")),
        },
        Subject {
            text: "feat(spate-core): never released".to_owned(),
            origin: "pull request title".to_owned(),
            source: Source::Body,
        },
    ];

    let (offending, excused) = offenders(repo.path(), &scratch, &subjects);
    assert_eq!(excused, 1);
    assert_eq!(
        offending,
        vec![
            offender_line("fix(spate-kafka): a real one", "commit def5678"),
            offender_line("feat(spate-core): never released", "pull request title"),
        ]
    );
}

/// A fragment written but not yet committed counts only for a run with no head
/// of its own.
#[test]
fn the_worktree_counts_only_without_a_head() {
    let repo = Repo::new("the_worktree_counts_only_without_a_head");
    let base = repo.git(&["rev-parse", "HEAD"]);
    let head = repo.commit("chore: a commit adding nothing");
    repo.write("changelog.d/untracked.fixed.md", "A real note.\n");

    assert_eq!(
        fragments_added(repo.path(), &base, Some(&head)),
        (0, vec![])
    );
    assert_eq!(fragments_added(repo.path(), &base, None), (1, vec![]));
}

/// With no event named, the comparison is the laptop arm's.
#[test]
fn no_event_orients_against_the_upstream() {
    let repo = Repo::new("no_event_orients_against_the_upstream");
    let base = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["checkout", "--quiet", "-b", "topic"]);
    repo.commit("feat(spate-core): a thing");

    assert_eq!(
        select(repo.path(), &Fields::default()).unwrap(),
        Mode::Require { base, head: None }
    );
}

/// A merge commit's subject is unparseable, so reading one would demand a
/// fragment for it.
#[test]
fn a_merge_subject_is_left_out() {
    let repo = Repo::new("a_merge_subject_is_left_out");
    let base = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["checkout", "--quiet", "-b", "topic"]);
    repo.commit("docs(ci): a page");
    repo.git(&["checkout", "--quiet", "main"]);
    repo.commit("docs(ci): another page");
    repo.git(&[
        "merge",
        "--quiet",
        "--no-ff",
        "-m",
        "Merge branch 'topic'",
        "topic",
    ]);

    let texts: Vec<String> = subjects(repo.path(), &format!("{base}..HEAD"), "")
        .into_iter()
        .map(|s| s.text)
        .collect();
    assert_eq!(texts.len(), 2, "{texts:?}");
    assert!(!texts.iter().any(|t| t.starts_with("Merge")), "{texts:?}");
}

/// A fragment is written with the template, and a second one at the same path
/// is refused with the first left as it was.
#[test]
fn a_new_fragment_carries_the_template() {
    let repo = Repo::new("a_new_fragment_carries_the_template");
    new(repo.path(), false, "fixed", "retry-ladder").unwrap();
    let path = repo.path().join("changelog.d/retry-ladder.fixed.md");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), TEMPLATE);
    let refused = new(repo.path(), false, "fixed", "retry-ladder").unwrap_err();
    assert_eq!(
        refused.message,
        "changelog.d/retry-ladder.fixed.md already exists"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), TEMPLATE);
}

/// A type or a slug that is not there is a usage error naming the six.
#[test]
fn a_missing_type_or_slug_is_a_usage_error() {
    let scratch = scratch("a_missing_type_or_slug_is_a_usage_error");
    for (kind, slug) in [("", "a-thing"), ("fixed", ""), ("", "")] {
        let refused = new(scratch.dir(), false, kind, slug).unwrap_err();
        assert_eq!(
            refused.message,
            "usage: cargo xtask changelog new <type> <slug>\n  \
             type is one of: added changed deprecated removed fixed security"
        );
    }
}

/// The scaffolder makes the directory it writes into.
#[test]
fn a_new_fragment_makes_the_directory() {
    let scratch = scratch("a_new_fragment_makes_the_directory");
    new(scratch.dir(), false, "added", "a-thing").unwrap();
    assert!(scratch.join("changelog.d/a-thing.added.md").is_file());
}

/// The gate refuses a tree with no fragment directory, and one with no README
/// inside it, before it looks at any event.
#[test]
fn the_gate_refuses_a_tree_missing_the_fragment_directory() {
    let scratch = scratch("the_gate_refuses_a_tree_missing_the_fragment_directory");
    let refused = check(scratch.dir(), false).unwrap_err();
    assert_eq!(
        refused.message,
        "changelog.d/ not found. It holds the changelog fragments"
    );
    std::fs::create_dir(scratch.join("changelog.d")).unwrap();
    let refused = check(scratch.dir(), false).unwrap_err();
    assert_eq!(
        refused.message,
        "changelog.d/README.md not found. It states the format and, less obviously,\n  \
         is what keeps the directory in git once a release has consumed every fragment."
    );
}

/// The fields a pull request run reads, over one range.
fn pull_request(base: &str, head: &str, title: &str) -> Fields {
    Fields {
        event: "pull_request".to_owned(),
        base_sha: base.to_owned(),
        head_sha: head.to_owned(),
        title: title.to_owned(),
        body: String::new(),
    }
}

/// A pull request is judged on the fragments its own head carries, so one
/// sitting in the worktree satisfies nothing.
#[test]
fn the_gate_counts_the_fragments_of_the_head_it_was_given() {
    let repo = Repo::new("the_gate_counts_the_fragments_of_the_head_it_was_given");
    let base = repo.git(&["rev-parse", "HEAD"]);
    let head = repo.commit("feat(spate-core): a windowed operator");
    repo.write("changelog.d/untracked.fixed.md", "A real note.\n");

    let refused = gate(repo.path(), &pull_request(&base, &head, ""), false, "").unwrap_err();
    assert_eq!(refused.code, Some(1));
    assert_eq!(refused.message, "");
}

/// A fragment added with nothing in it is refused by name, so an empty file
/// cannot stand in for the release note.
#[test]
fn the_gate_names_an_added_fragment_that_is_empty() {
    let repo = Repo::new("the_gate_names_an_added_fragment_that_is_empty");
    let base = repo.git(&["rev-parse", "HEAD"]);
    repo.write("changelog.d/silent.fixed.md", "   \n\n");
    let head = repo.commit("feat(spate-core): a windowed operator");

    let refused = gate(repo.path(), &pull_request(&base, &head, ""), false, "").unwrap_err();
    assert_eq!(
        refused.message,
        "these fragment(s) were added but are empty:\n\n    \
         changelog.d/silent.fixed.md\n  \
         A fragment is the release note. Write what the change means for somebody\n  \
         upgrading. changelog.d/README.md has the conventions."
    );
}

/// An added fragment counts, an added fragment with nothing in it is named
/// instead, and a file that is not a fragment is neither.
#[test]
fn an_added_fragment_counts_and_an_empty_one_is_named() {
    let repo = Repo::new("an_added_fragment_counts_and_an_empty_one_is_named");
    let base = repo.git(&["rev-parse", "HEAD"]);
    repo.write("changelog.d/said.fixed.md", "A real note.\n");
    repo.write("changelog.d/silent.fixed.md", "   \n\n");
    repo.write("changelog.d/notes.txt", "not a fragment\n");
    let head = repo.commit("feat(spate-core): a thing");

    assert_eq!(
        added_fragments(repo.path(), &base, Some(&head)),
        (1, vec!["changelog.d/silent.fixed.md".to_owned()])
    );

    // The range ends at the named head. A later commit's fragments belong to
    // a later range.
    repo.write("changelog.d/later.fixed.md", "A later note.\n");
    repo.commit("feat(spate-core): a later thing");
    assert_eq!(
        added_fragments(repo.path(), &base, Some(&head)),
        (1, vec!["changelog.d/silent.fixed.md".to_owned()])
    );
}

/// An edited fragment is not this change's release note.
#[test]
fn an_edited_fragment_does_not_count() {
    let repo = Repo::new("an_edited_fragment_does_not_count");
    repo.write("changelog.d/said.fixed.md", "A real note.\n");
    let base = repo.commit("feat(spate-core): a thing");
    repo.write("changelog.d/said.fixed.md", "A corrected note.\n");
    let head = repo.commit("fix(spate-core): a typo");

    assert_eq!(
        added_fragments(repo.path(), &base, Some(&head)),
        (0, vec![])
    );
}

/// A fragment written but not yet committed counts, whether or not it is
/// staged, and one that says nothing does not.
#[test]
fn an_uncommitted_fragment_counts() {
    let repo = Repo::new("an_uncommitted_fragment_counts");
    assert_eq!(uncommitted_fragments(repo.path()), 0);
    repo.write("changelog.d/untracked.fixed.md", "A real note.\n");
    assert_eq!(uncommitted_fragments(repo.path()), 1);
    repo.git(&["add", "changelog.d/untracked.fixed.md"]);
    assert_eq!(uncommitted_fragments(repo.path()), 1);
    repo.write("changelog.d/silent.fixed.md", "\n\n");
    assert_eq!(uncommitted_fragments(repo.path()), 1);
    repo.write("changelog.d/notes.txt", "not a fragment\n");
    assert_eq!(uncommitted_fragments(repo.path()), 1);
}

/// An edited fragment sitting in the worktree is not a new one.
#[test]
fn an_uncommitted_edit_does_not_count() {
    let repo = Repo::new("an_uncommitted_edit_does_not_count");
    repo.write("changelog.d/said.fixed.md", "A real note.\n");
    repo.commit("feat(spate-core): a thing");
    repo.write("changelog.d/said.fixed.md", "A corrected note.\n");
    assert_eq!(uncommitted_fragments(repo.path()), 0);
}

/// A trailer on a commit excuses that commit, and a body line that merely opens
/// with the word does not.
#[test]
fn a_commit_trailer_excuses_its_own_subject() {
    let repo = Repo::new("a_commit_trailer_excuses_its_own_subject");
    let scratch = scratch("a_commit_trailer_excuses_its_own_subject");
    let excused = repo.commit("feat(spate-core): a thing\n\nChangelog: none\n");
    let plain = repo.commit("feat(spate-core): another\n\nChangelog: none of this applies\n");

    assert!(commit_says_none(repo.path(), &scratch, &excused));
    assert!(!commit_says_none(repo.path(), &scratch, &plain));
    assert!(!commit_says_none(repo.path(), &scratch, "0000000"));
}

/// The pull request body's trailer is read from the body's last block, so a
/// body that is only the trailer is a subject line and excuses nothing.
#[test]
fn the_body_trailer_is_read_from_the_last_block() {
    let repo = Repo::new("the_body_trailer_is_read_from_the_last_block");
    let scratch = scratch("the_body_trailer_is_read_from_the_last_block");
    let read = |body: &str| body_says_none(repo.path(), &scratch, body);

    assert!(read("why this exists\n\nChangelog: none"));
    assert!(read(
        "why this exists\n\nSigned-off-by: A <a@a>\nChangelog: none"
    ));
    assert!(!read("Changelog: none"));
    assert!(!read(""));
    assert!(!read("Changelog: none\n\nmore prose after the block"));
    assert!(!read("The Changelog: none of it applies"));
}

/// A fragment on disk with nothing in it is not prose, and a path that is not
/// there is not either.
#[test]
fn a_fragment_on_disk_has_to_say_something() {
    let repo = Repo::new("a_fragment_on_disk_has_to_say_something");
    repo.write("changelog.d/said.fixed.md", "A real note.\n");
    repo.write("changelog.d/silent.fixed.md", " \t\n");
    assert!(fragment_has_prose(repo.path(), "changelog.d/said.fixed.md"));
    assert!(!fragment_has_prose(
        repo.path(),
        "changelog.d/silent.fixed.md"
    ));
    assert!(!fragment_has_prose(
        repo.path(),
        "changelog.d/absent.fixed.md"
    ));
}
