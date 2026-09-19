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

// ---------------------------------------------------------------------------
// The release assembly.
// ---------------------------------------------------------------------------

/// A changelog with an empty Unreleased section, one prior release and the link
/// foot the rewrite reads.
const SKELETON: &str = "\
# Changelog

## [Unreleased]

## [0.2.0] — 2026-08-22

### Fixed

- An older thing. ([#7])

[#7]: https://github.com/spate-etl/spate/pull/7

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
";

/// A lookup that answers nothing, so an entry with no reference of its own
/// takes the commit link.
fn unanswered(_: &std::path::Path, _: &str) -> Result<Option<String>, Error> {
    Ok(None)
}

/// The message of a refusal, where success is a failure of the test.
#[track_caller]
fn refused<T>(outcome: Result<T, Error>) -> String {
    match outcome {
        Ok(_) => panic!("this was expected to be refused"),
        Err(e) => e.message,
    }
}

/// A record ends at a newline, and a final newline closes the last record
/// rather than opening an empty one.
#[test]
fn a_final_newline_closes_the_last_record() {
    assert_eq!(records(""), Vec::<&str>::new());
    assert_eq!(records("\n"), vec![""]);
    assert_eq!(records("a\n"), vec!["a"]);
    assert_eq!(records("a"), vec!["a"]);
    assert_eq!(records("a\n\n"), vec!["a", ""]);
    assert_eq!(records("a\nb"), vec!["a", "b"]);
}

/// The fence marker is either kind, indented by no more than three spaces, and
/// a line that merely carries one is not a fence.
#[test]
fn a_fence_is_either_marker_under_four_spaces_of_indent() {
    for line in ["```", "~~~", "   ```", "  ~~~rust", "```markdown"] {
        assert!(is_fence(line), "{line}");
    }
    for line in ["    ```", "     ~~~", "a ```", "``", "~~", "- ```"] {
        assert!(!is_fence(line), "{line}");
    }
}

/// Only opening blank lines are dropped, and a line carrying a space carries
/// something.
#[test]
fn the_opening_blank_lines_are_dropped() {
    assert_eq!(opening_blanks_dropped("\n\na\nb\n\n"), "a\nb");
    assert_eq!(opening_blanks_dropped("\n \na\n"), " \na");
    assert_eq!(opening_blanks_dropped("\n\n"), "");
    assert_eq!(opening_blanks_dropped(""), "");
}

/// The section runs from its heading to the next one, and the heading has to be
/// followed by something, so a bare `## [x]` is not one.
#[test]
fn the_section_runs_to_the_next_heading() {
    let text = "## [Unreleased]\n\n## [0.3.0] — d\n\n### Added\n\n- A thing.\n\n## [0.2.0] — d\n\n- Older.\n";
    assert_eq!(
        scan(text, "## [0.3.0] "),
        Ok("\n### Added\n\n- A thing.\n\n".to_owned())
    );
    assert_eq!(scan(text, "## [0.9.0] "), Err(Scan::Missing));
    assert_eq!(
        scan("## [0.3.0]\n\n- A thing.\n", "## [0.3.0] "),
        Err(Scan::Missing)
    );
}

/// The link foot ends the last section, so it never leaks into the notes.
#[test]
fn the_link_foot_ends_the_last_section() {
    let body = section_notes(SKELETON, "0.2.0", "CHANGELOG.md").unwrap();
    assert_eq!(
        body,
        "### Fixed\n\n- An older thing. ([#7])\n\n[#7]: https://github.com/spate-etl/spate/pull/7\n"
    );
}

/// A boundary quoted inside a fence is content, for either fence marker.
#[test]
fn a_boundary_inside_a_fence_is_content() {
    let text = "\
## [0.3.0] — d

### Changed

- **The heading writer** — emits this shape:

  ```markdown
## [Unreleased]
[Unreleased]: quoted-inside-a-fence
  ```

  and keeps going.

## [0.2.0] — d
";
    assert_eq!(
        scan(text, "## [0.3.0] ").unwrap(),
        "\n### Changed\n\n- **The heading writer** — emits this shape:\n\n  ```markdown\n## [Unreleased]\n[Unreleased]: quoted-inside-a-fence\n  ```\n\n  and keeps going.\n\n"
    );
    let tilde =
        "## [1.1.0] — d\n\n~~~text\n## [Unreleased]\n~~~\n\n- A thing.\n\n[Unreleased]: x\n";
    assert_eq!(
        scan(tilde, "## [1.1.0] ").unwrap(),
        "\n~~~text\n## [Unreleased]\n~~~\n\n- A thing.\n\n"
    );
}

/// A second heading for one version is the part-finished assembly, and is
/// refused rather than spliced.
#[test]
fn two_headings_for_one_version_are_refused() {
    let text = "## [0.5.0] — a\n\n- One body.\n\n## [0.5.0] — b\n\n- Another body.\n";
    assert_eq!(scan(text, "## [0.5.0] "), Err(Scan::Duplicate));
    assert_eq!(
        refused(section_notes(text, "0.5.0", "CHANGELOG.md")),
        "two '## [0.5.0]' headings in CHANGELOG.md. A part-finished assembly has to be\n  \
         undone before its section can be read."
    );
}

/// A version the file does not carry is a refusal naming what writes the
/// section, and an empty section is a refusal of its own.
#[test]
fn a_missing_and_an_empty_section_are_distinct_refusals() {
    assert_eq!(
        refused(section_notes(SKELETON, "9.9.9", "CHANGELOG.md")),
        "no '## [9.9.9]' section in CHANGELOG.md. The notes read what the assembly wrote,\n  \
         so the release is assembled first."
    );
    assert_eq!(
        refused(section_notes(
            "## [0.7.0] — d\n\n\n## [0.6.0] — d\n\n- A thing.\n",
            "0.7.0",
            "CHANGELOG.md"
        )),
        "the '## [0.7.0]' section in CHANGELOG.md is empty"
    );
    assert_eq!(
        refused(section_notes(
            "## [Unreleased]\n\n## [0.9.0]\n\n- A thing.\n\n[Unreleased]: x\n",
            "0.9.0",
            "CHANGELOG.md"
        )),
        "no '## [0.9.0]' section in CHANGELOG.md. The notes read what the assembly wrote,\n  \
         so the release is assembled first."
    );
}

/// Every reference the slice uses has to be defined inside it, or the release
/// body renders the literal text. The first one missing is the one named.
#[test]
fn a_reference_with_no_definition_in_the_slice_is_refused() {
    assert_eq!(
        refused(section_notes(
            "## [0.6.0] — d\n\n- A thing. ([#9]) and ([#10])\n",
            "0.6.0",
            "CHANGELOG.md"
        )),
        "the '## [0.6.0]' section uses [#10] with no definition in the section"
    );
    let defined = "## [0.6.0] — d\n\n- A thing. ([#9])\n\n[#9]: https://example.invalid/9\n";
    assert!(section_notes(defined, "0.6.0", "CHANGELOG.md").is_ok());
}

/// The slice keeps its own blank lines and drops the opening ones.
#[test]
fn the_slice_drops_its_opening_blank_lines() {
    let text = "## [1.0.0] — d\n\n\n\n### Added\n\n- A thing.\n\n## [0.9.0] — d\n";
    assert_eq!(
        section_notes(text, "1.0.0", "CHANGELOG.md").unwrap(),
        "### Added\n\n- A thing.\n"
    );
}

/// A fragment's prose loses the whitespace at the end of every line and the
/// blank lines at either end.
#[test]
fn a_fragment_body_loses_its_edges() {
    assert_eq!(
        entry_body("\n\n  a thing   \n\nand more\t\n\n\n"),
        "  a thing\n\nand more"
    );
    assert_eq!(entry_body("one line, no newline"), "one line, no newline");
    assert_eq!(entry_body("   \n\t\n"), "");
}

/// The bullet goes on the first line and two spaces of continuation on every
/// line carrying anything. Blank lines stay blank.
#[test]
fn an_entry_is_indented_as_one_list_item() {
    assert_eq!(bullet("a\n\nb"), "- a\n\n  b\n");
    assert_eq!(bullet("a"), "- a\n");
}

/// The six render under the heading Keep a Changelog spells.
#[test]
fn a_type_renders_in_sentence_case() {
    let headings: Vec<String> = TYPES.iter().map(|kind| sentence_case(kind)).collect();
    assert_eq!(
        headings,
        [
            "Added",
            "Changed",
            "Deprecated",
            "Removed",
            "Fixed",
            "Security"
        ]
    );
    assert_eq!(sentence_case(""), "");
}

/// A definition points at the pull request of that number.
#[test]
fn a_definition_points_at_the_pull_request() {
    assert_eq!(
        link_line("31"),
        "[#31]: https://github.com/spate-etl/spate/pull/31"
    );
}

/// The definitions come out once each, ordered by the number rather than by the
/// text, and two spellings of one number stay two definitions.
#[test]
fn the_definitions_are_deduplicated_then_ordered_by_number() {
    let links = vec![
        link_line("100"),
        link_line("9"),
        link_line("10"),
        link_line("9"),
        link_line("031"),
        link_line("31"),
    ];
    assert_eq!(
        sorted_links(links),
        vec![
            link_line("9"),
            link_line("10"),
            link_line("031"),
            link_line("31"),
            link_line("100"),
        ]
    );
}

/// The key is the number after the first `#`, and text carrying none sorts as
/// zero.
#[test]
fn the_key_is_the_number_after_the_first_hash() {
    assert_eq!(numeric_key("[#31]: https://example.invalid/pull/31"), 31);
    assert_eq!(numeric_key("[#031]: x"), 31);
    assert_eq!(numeric_key("[#]: x"), 0);
    assert_eq!(numeric_key("no hash here"), 0);
}

/// Every `[#N]` in the prose is read, in order, and a malformed one is not.
#[test]
fn every_reference_in_the_prose_is_read() {
    assert_eq!(
        issue_references("cites [#12] then [#3] then [#12] again"),
        vec!["12", "3", "12"]
    );
    assert_eq!(issue_references("[#]"), Vec::<&str>::new());
    assert_eq!(issue_references("[##12]"), Vec::<&str>::new());
    assert_eq!(issue_references("[#12a]"), Vec::<&str>::new());
    assert_eq!(issue_references("[#12"), Vec::<&str>::new());
    assert_eq!(issue_references("[#1][#2]"), vec!["1", "2"]);
}

/// An entry's own reference is the last thing in it. A citation anywhere else
/// belongs to another pull request and does not stand in for the derived one.
#[test]
fn only_a_trailing_reference_stands_in_for_the_derived_one() {
    assert!(ends_with_reference("A thing. ([#31])"));
    assert!(ends_with_reference("A thing.\n([#31])  "));
    assert!(!ends_with_reference("A thing citing ([#12]) and going on."));
    assert!(!ends_with_reference("A thing. ([#31]) and more"));
    assert!(!ends_with_reference("A thing. ([#])"));
    assert!(!ends_with_reference("A thing.\n([#31])\nand more"));
}

/// The number GitHub appends to a squash subject, over the table the three
/// shapes come from. All of them are real history.
#[test]
fn the_subject_parser_agrees_with_the_table() {
    const TABLE: &[(&str, Option<&str>)] = &[
        // A squash subject, which GitHub numbers.
        (
            "fix(spate-core): enforce max_pending_batches at the poll boundary (#200)",
            Some("200"),
        ),
        (
            "feat(spate-avro,bench): decode datums into typed records (#31)",
            Some("31"),
        ),
        // A rebase merge appends nothing.
        (
            "fix(spate-kafka): count logical coordinator links toward broker_up",
            None,
        ),
        (
            "refactor(examples)!: name the JSON example for what it teaches",
            None,
        ),
        ("chore: release v0.2.0", None),
        // A citation mid-subject is not the merge's own number.
        (
            "docs(workspace): supersede (#12) with a record of its own",
            None,
        ),
        (
            "fix(spate-s3): restore what (#42) changed, and pin the ETag",
            None,
        ),
        // The last one wins when the subject ends in two.
        ("fix(spate-core): revert (#41) (#57)", Some("57")),
        // Neither shape is a number.
        ("fix: a thing (#)", None),
        ("fix: a thing (##12)", None),
    ];
    for (subject, want) in TABLE {
        assert_eq!(pr_from_subject(subject), *want, "{subject}");
    }
}

/// The digit run at the end is maximal, and text ending in none has none.
#[test]
fn the_trailing_digit_run_is_maximal() {
    assert_eq!(trailing_digits("abc123"), 3);
    assert_eq!(trailing_digits("123"), 3);
    assert_eq!(trailing_digits("abc"), 0);
    assert_eq!(trailing_digits(""), 0);
}

/// An answer is used only where the call succeeded and it is a number. A commit
/// the API does not know takes the commit link; any other failure aborts.
#[test]
fn the_lookup_answer_decides_between_a_number_and_a_refusal() {
    let sha = "0123456789abcdef0123456789abcdef01234567";
    assert_eq!(
        classify(true, "57\n", "", sha).unwrap(),
        Some("57".to_owned())
    );
    assert_eq!(classify(true, "", "", sha).unwrap(), None);
    assert_eq!(
        classify(false, "{\"status\": \"422\"}", "", sha).unwrap(),
        None
    );
    assert_eq!(
        classify(false, "{\"status\":\"422\"}", "", sha).unwrap(),
        None
    );
    assert_eq!(
        classify(false, "No commit found for SHA", "", sha).unwrap(),
        None
    );
    assert_eq!(
        refused(classify(true, "gh: not a number\n", "", sha)),
        "the pull-request lookup for 0123456789ab answered with something that is\n  \
         not a number: gh: not a number"
    );
    assert_eq!(
        refused(classify(
            false,
            "{\"status\": \"401\"}",
            "bad credentials\n",
            sha
        )),
        "the pull-request lookup for 0123456789ab failed rather than answering:\n  \
         {\"status\": \"401\"} bad credentials\n  \
         Fix the token or the network and assemble again; falling back to a\n  \
         commit link here would look identical to a commit that has no pull request."
    );
}

/// The lookup asks for the merged pull requests of one commit in this
/// repository.
#[test]
fn the_lookup_asks_for_one_commit_s_pull_requests() {
    assert_eq!(
        pulls_query("abc123"),
        [
            "api".to_owned(),
            "repos/spate-etl/spate/commits/abc123/pulls".to_owned(),
            "--jq".to_owned(),
            "map(select(.merged_at)) | first | .number // empty".to_owned(),
        ]
    );
}

/// The count comes off a shortlog line, and a line carrying none is left whole.
#[test]
fn the_count_comes_off_a_shortlog_line() {
    assert_eq!(shortlog_name("    12\tMarcus Kainth"), "Marcus Kainth");
    assert_eq!(shortlog_name("1\tt"), "t");
    assert_eq!(shortlog_name("  no count here"), "  no count here");
    assert_eq!(shortlog_name(""), "");
}

/// The Unreleased section holds nothing until the next heading, where a line of
/// whitespace is nothing and a fence is not modelled.
#[test]
fn the_unreleased_section_has_to_be_empty() {
    assert!(unreleased_is_empty(SKELETON));
    assert!(unreleased_is_empty(
        "## [Unreleased]\n\n   \n\n## [0.2.0] — d\n- A thing.\n"
    ));
    assert!(unreleased_is_empty(
        "# Changelog\n\n- Not under the heading.\n"
    ));
    assert!(!unreleased_is_empty(
        "## [Unreleased]\n\n- Written by hand.\n\n## [0.2.0] — d\n"
    ));
    assert!(!unreleased_is_empty(
        "## [Unreleased]\n\n- Written by hand.\n"
    ));
}

/// The new section goes below the Unreleased heading and the two link
/// references at the foot are rewritten, every one of them.
#[test]
fn the_section_and_the_links_are_written_together() {
    let written = insert(SKELETON, "0.3.0", "2026-09-19", "### Fixed\n\n- A thing.\n").unwrap();
    assert_eq!(
        written,
        "\
# Changelog

## [Unreleased]

## [0.3.0] — 2026-09-19

### Fixed

- A thing.

## [0.2.0] — 2026-08-22

### Fixed

- An older thing. ([#7])

[#7]: https://github.com/spate-etl/spate/pull/7

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/spate-etl/spate/releases/tag/v0.3.0
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
"
    );
}

/// The heading and the link reference are separate failures, so a changelog
/// missing one says which.
#[test]
fn the_heading_and_the_link_reference_fail_apart() {
    assert_eq!(
        refused(insert(
            "# Changelog\n\n[Unreleased]: x\n",
            "0.3.0",
            "d",
            "b\n"
        )),
        "the Unreleased heading vanished mid-write"
    );
    assert_eq!(
        refused(insert(
            "# Changelog\n\n## [Unreleased]\n",
            "0.3.0",
            "d",
            "b\n"
        )),
        "no [Unreleased]: link reference to rewrite"
    );
}

/// What a finished assembly reports.
#[test]
fn the_summary_names_the_section_it_wrote() {
    assert_eq!(
        summary("0.3.0", "2026-09-19"),
        "changelog: wrote ## [0.3.0] — 2026-09-19 into CHANGELOG.md and consumed the fragments.\n  \
         Read what it wrote before committing: the assembly is mechanical, the release note is not.\n"
    );
}

/// A throwaway repository holding a changelog and its fragments.
struct Release(Repo);

impl Release {
    fn new(name: &str) -> Self {
        let release = Self(Repo::new(name));
        release.0.write(CHANGELOG, SKELETON);
        release
    }

    fn path(&self) -> &std::path::Path {
        self.0.path()
    }

    /// Writes a fragment and stages it, so the tracked check passes.
    fn fragment(&self, name: &str, body: &str) {
        self.0.write(&format!("{FRAGMENTS}/{name}"), body);
        self.0.git(&["add", "-f", &format!("{FRAGMENTS}/{name}")]);
    }

    fn changelog(&self) -> String {
        std::fs::read_to_string(self.path().join(CHANGELOG)).unwrap()
    }

    /// The fragment directory's contents, sorted.
    fn listing(&self) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(self.path().join(FRAGMENTS))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        out.sort();
        out
    }
}

/// Only a name one level down carrying one of the six is a fragment, and one
/// opening with a dot is not.
#[test]
fn the_fragment_listing_is_one_level_of_the_six() {
    let release = Release::new("the_fragment_listing_is_one_level_of_the_six");
    release.fragment("b.fixed.md", "B.\n");
    release.fragment("a.added.md", "A.\n");
    release.fragment("c.unknown.md", "C.\n");
    release.fragment("notes.txt", "N.\n");
    release.fragment(".hidden.fixed.md", "H.\n");
    release.0.write("changelog.d/nested/d.fixed.md", "D.\n");

    assert_eq!(
        fragment_names(release.path()),
        vec!["changelog.d/a.added.md", "changelog.d/b.fixed.md"]
    );
    assert_eq!(
        fragments_of(release.path(), "fixed"),
        vec!["changelog.d/b.fixed.md"]
    );
    assert_eq!(
        fragments_of(release.path(), "security"),
        Vec::<String>::new()
    );
    assert_eq!(
        fragment_names(release.path().join("nowhere").as_path()),
        Vec::<String>::new()
    );
}

/// A staged file is tracked; one sitting in the worktree is not.
#[test]
fn a_staged_fragment_is_tracked() {
    let release = Release::new("a_staged_fragment_is_tracked");
    release.fragment("a.fixed.md", "A.\n");
    release.0.write("changelog.d/b.added.md", "B.\n");
    assert!(tracked(release.path(), "changelog.d/a.fixed.md"));
    assert!(!tracked(release.path(), "changelog.d/b.added.md"));
}

/// The newest version tag bounds the range, read by version rather than by
/// text, and a tag that is not one is left out.
#[test]
fn the_newest_version_tag_bounds_the_range() {
    let release = Release::new("the_newest_version_tag_bounds_the_range");
    assert_eq!(previous_tag(release.path()), None);
    release.0.git(&["tag", "v0.2.0"]);
    release.0.git(&["tag", "v0.10.0"]);
    release.0.git(&["tag", "nightly"]);
    assert_eq!(previous_tag(release.path()), Some("v0.10.0".to_owned()));
}

/// The contributors over a range are the authors, most commits first, with the
/// bots left out.
#[test]
fn the_contributors_are_the_authors_of_the_range() {
    let release = Release::new("the_contributors_are_the_authors_of_the_range");
    let base = release.0.git(&["rev-parse", "HEAD"]);
    for message in ["chore: one", "chore: two"] {
        release.0.git(&[
            "-c",
            "user.name=Zoe",
            "-c",
            "user.email=zoe@t",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            message,
        ]);
    }
    release.0.git(&[
        "-c",
        "user.name=dependabot[bot]",
        "-c",
        "user.email=bot@t",
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        "chore: three",
    ]);
    release.0.git(&[
        "-c",
        "user.name=Ada",
        "-c",
        "user.email=ada@t",
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        "chore: four",
    ]);
    assert_eq!(
        contributors(release.path(), &format!("{base}..HEAD")),
        vec!["Zoe".to_owned(), "Ada".to_owned()]
    );
    assert_eq!(
        contributors(release.path(), "nothing..HEAD"),
        Vec::<String>::new()
    );
}

/// The date the heading carries is today's, in UTC, as the heading spells it.
#[test]
fn the_date_is_a_calendar_day() {
    let root = crate::repo_root().unwrap();
    let today = today(&root).unwrap();
    assert_eq!(today.len(), 10, "{today}");
    let parts: Vec<&str> = today.split('-').collect();
    assert_eq!(parts.len(), 3, "{today}");
    assert!(
        parts.iter().all(|p| p.bytes().all(|b| b.is_ascii_digit()))
            && [4, 2, 2] == [parts[0].len(), parts[1].len(), parts[2].len()],
        "{today}"
    );
}

/// The subject of the commit that added the fragment answers first, the lookup
/// answers next, and the commit itself answers last. A fragment with no history
/// has no reference at all.
#[test]
fn the_reference_takes_the_first_of_the_three_sources() {
    let release = Release::new("the_reference_takes_the_first_of_the_three_sources");
    release.0.write("changelog.d/a.fixed.md", "A.\n");
    assert_eq!(
        fragment_reference(release.path(), "changelog.d/a.fixed.md", &unanswered).unwrap(),
        None
    );

    let numbered = release.0.commit("fix(spate-core): a thing (#77)");
    assert_eq!(
        fragment_reference(release.path(), "changelog.d/a.fixed.md", &unanswered).unwrap(),
        Some(Reference::Pull("77".to_owned()))
    );

    release.0.write("changelog.d/b.added.md", "B.\n");
    let plain = release.0.commit("feat(spate-core): a thing with no number");
    assert_ne!(plain, numbered);
    assert_eq!(
        fragment_reference(release.path(), "changelog.d/b.added.md", &unanswered).unwrap(),
        Some(Reference::Commit(plain.clone()))
    );

    let asked = std::cell::RefCell::new(String::new());
    let answering = |_: &std::path::Path, sha: &str| {
        asked.borrow_mut().push_str(sha);
        Ok(Some("99".to_owned()))
    };
    assert_eq!(
        fragment_reference(release.path(), "changelog.d/b.added.md", &answering).unwrap(),
        Some(Reference::Pull("99".to_owned()))
    );
    assert_eq!(*asked.borrow(), plain);
}

/// The block groups the entries by type in the order the six are declared, one
/// blank line between groups, with the contributors and the definitions under
/// them.
#[test]
fn the_block_renders_the_six_in_order() {
    let release = Release::new("the_block_renders_the_six_in_order");
    release.fragment("z.security.md", "A security thing.\n");
    release.fragment("a.added.md", "An added thing citing ([#12]) in passing.\n");
    release.fragment("b.added.md", "Another added thing. ([#3])\n");
    release.fragment("c.fixed.md", "A fixed thing.\n");

    let block = assemble(release.path(), "nothing..HEAD", None, &unanswered).unwrap();
    assert_eq!(
        block,
        "\
### Added

- An added thing citing ([#12]) in passing.
- Another added thing. ([#3])

### Fixed

- A fixed thing.

### Security

- A security thing.

[#3]: https://github.com/spate-etl/spate/pull/3
[#12]: https://github.com/spate-etl/spate/pull/12
"
    );
}

/// A tree with nothing to release is a refusal naming the tag the range starts
/// at.
#[test]
fn a_tree_with_no_fragments_has_nothing_to_release() {
    let release = Release::new("a_tree_with_no_fragments_has_nothing_to_release");
    assert_eq!(
        refused(assemble(
            release.path(),
            "nothing..HEAD",
            Some("v0.2.0"),
            &unanswered
        )),
        "no fragments in changelog.d/, so nothing to release.\n  \
         Every user-visible change since v0.2.0 should have left one; if the release\n  \
         genuinely contains none, write the section by hand and say why in the commit."
    );
}

/// An entry with no reference of its own takes the derived one on a line of its
/// own, because a fragment may end in a fenced code block.
#[test]
fn a_derived_reference_goes_on_its_own_line() {
    let release = Release::new("a_derived_reference_goes_on_its_own_line");
    release.0.write(
        "changelog.d/a.added.md",
        "A thing that ends in a fence:\n\n```rust\nlet x = 1;\n```\n",
    );
    release.0.commit("feat(spate-core): a fenced thing (#12)");
    let block = assemble(release.path(), "nothing..HEAD", None, &unanswered).unwrap();
    assert_eq!(
        block,
        "\
### Added

- A thing that ends in a fence:

  ```rust
  let x = 1;
  ```
  ([#12])

[#12]: https://github.com/spate-etl/spate/pull/12
"
    );
}

/// An entry ending in a reference of its own keeps that one, and the reference
/// the commit would have derived is neither appended nor defined.
#[test]
fn a_trailing_reference_stands_in_for_the_derived_one() {
    let release = Release::new("a_trailing_reference_stands_in_for_the_derived_one");
    release.0.write(
        "changelog.d/a.fixed.md",
        "A thing that landed elsewhere. ([#31])\n",
    );
    release.0.commit("fix(spate-core): a thing (#77)");
    assert_eq!(
        assemble(release.path(), "nothing..HEAD", None, &unanswered).unwrap(),
        "\
### Fixed

- A thing that landed elsewhere. ([#31])

[#31]: https://github.com/spate-etl/spate/pull/31
"
    );
}

/// A commit with no pull request links to itself, by its short sha, and gets no
/// definition in the list.
#[test]
fn a_commit_with_no_pull_request_links_to_itself() {
    let release = Release::new("a_commit_with_no_pull_request_links_to_itself");
    release.0.write("changelog.d/a.fixed.md", "A thing.\n");
    let sha = release.0.commit("fix(spate-core): a thing with no number");
    let block = assemble(release.path(), "nothing..HEAD", None, &unanswered).unwrap();
    assert_eq!(
        block,
        format!(
            "### Fixed\n\n- A thing.\n  ([`{}`](https://github.com/spate-etl/spate/commit/{sha}))\n",
            &sha[..7]
        )
    );
}

/// The assembly writes the section, consumes the fragments and takes them out
/// of the index.
#[test]
fn the_assembly_consumes_what_it_wrote_into_the_changelog() {
    let release = Release::new("the_assembly_consumes_what_it_wrote_into_the_changelog");
    release.fragment("a.fixed.md", "A thing.\n");
    release.fragment("b.added.md", "Another thing. ([#31])\n");
    let today = today(release.path()).unwrap();
    build(release.path(), false, "0.3.0").unwrap();

    let section = "\
### Added

- Another thing. ([#31])

### Fixed

- A thing.

### Contributors

- t

[#31]: https://github.com/spate-etl/spate/pull/31
";
    assert_eq!(
        release.changelog(),
        format!(
            "\
# Changelog

## [Unreleased]

## [0.3.0] — {today}

{section}
## [0.2.0] — 2026-08-22

### Fixed

- An older thing. ([#7])

[#7]: https://github.com/spate-etl/spate/pull/7

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/spate-etl/spate/releases/tag/v0.3.0
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
"
        )
    );
    assert_eq!(release.listing(), vec!["README.md".to_owned()]);
    assert_eq!(
        release.0.git(&["ls-files", FRAGMENTS]),
        "changelog.d/README.md"
    );
    assert_eq!(
        section_notes(&release.changelog(), "0.3.0", CHANGELOG).unwrap(),
        section
    );
}

/// The refusals an assembly reads off the changelog, each one leaving the tree
/// as it was.
#[test]
fn the_assembly_refuses_a_changelog_it_cannot_write_into() {
    let release = Release::new("the_assembly_refuses_a_changelog_it_cannot_write_into");
    release.fragment("a.fixed.md", "A thing.\n");

    release
        .0
        .write(CHANGELOG, "# Changelog\n\n## [0.2.0] — d\n");
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "no '## [Unreleased]' heading in CHANGELOG.md. The new release is inserted below\n  \
         it, so a release that removed it has to put it back, empty, before the next one."
    );

    release.0.write(CHANGELOG, SKELETON);
    assert_eq!(
        refused(build(release.path(), false, "0.2.0")),
        "CHANGELOG.md already has a '## [0.2.0]' section. Pick the next version,\n  \
         or if the previous attempt failed part-way, undo it before running this again."
    );

    release.0.write(
        CHANGELOG,
        &SKELETON.replace("## [Unreleased]\n", "## [Unreleased]\n\n- By hand.\n"),
    );
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "the '## [Unreleased]' section in CHANGELOG.md is not empty.\n\n  \
         The assembly reads changelog.d/, and anything written under that heading by\n  \
         hand would be swept into '## [0.3.0]' below the link definitions rather than\n  \
         read as part of it. Move it into a fragment, one file per entry, typed by its\n  \
         Keep a Changelog section, and run this again."
    );

    release
        .0
        .write(CHANGELOG, "# Changelog\n\n## [Unreleased]\n");
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "no [Unreleased]: link reference to rewrite"
    );
    assert_eq!(release.changelog(), "# Changelog\n\n## [Unreleased]\n");

    std::fs::remove_file(release.path().join(CHANGELOG)).unwrap();
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "CHANGELOG.md not found"
    );
    assert_eq!(
        release.listing(),
        vec!["README.md".to_owned(), "a.fixed.md".to_owned()]
    );
}

/// A fragment saying nothing, and one that never reached git, are both refused
/// with the changelog left as it was.
#[test]
fn the_assembly_refuses_a_fragment_it_cannot_release() {
    let release = Release::new("the_assembly_refuses_a_fragment_it_cannot_release");
    release.fragment("a.fixed.md", "   \n\t\n");
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "changelog.d/a.fixed.md is empty. A fragment is the release note: write it, or delete the file."
    );

    release.fragment("a.fixed.md", "A thing.\n");
    release
        .0
        .write("changelog.d/b.added.md", "Not committed.\n");
    assert_eq!(
        refused(build(release.path(), false, "0.3.0")),
        "changelog.d/b.added.md is not tracked. Commit it before assembling a release:\n  \
         a fragment that never reached git is not part of what is being released."
    );
    assert_eq!(release.changelog(), SKELETON);
    assert_eq!(
        release.listing(),
        vec![
            "README.md".to_owned(),
            "a.fixed.md".to_owned(),
            "b.added.md".to_owned()
        ]
    );
}

/// A version nobody named is a usage error, and `--explain` writes nothing.
#[test]
fn a_missing_version_is_a_usage_error() {
    let release = Release::new("a_missing_version_is_a_usage_error");
    release.fragment("a.fixed.md", "A thing.\n");
    assert_eq!(
        refused(build(release.path(), false, "")),
        "usage: cargo xtask changelog build <version>"
    );
    assert_eq!(
        refused(notes(release.path(), false, "")),
        "usage: cargo xtask changelog notes <version>"
    );
    build(release.path(), true, "0.3.0").unwrap();
    notes(release.path(), true, "0.3.0").unwrap();
    assert_eq!(release.changelog(), SKELETON);
    assert_eq!(
        release.listing(),
        vec!["README.md".to_owned(), "a.fixed.md".to_owned()]
    );
}

/// The notes read the changelog, so a tree without one says so.
#[test]
fn the_notes_read_a_changelog_that_is_there() {
    let release = Release::new("the_notes_read_a_changelog_that_is_there");
    notes(release.path(), false, "0.2.0").unwrap();
    std::fs::remove_file(release.path().join(CHANGELOG)).unwrap();
    assert_eq!(
        refused(notes(release.path(), false, "0.2.0")),
        "CHANGELOG.md not found"
    );
}
