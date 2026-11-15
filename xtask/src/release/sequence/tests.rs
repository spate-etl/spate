use std::path::Path;
use std::time::Duration;

use super::*;
use crate::release::fakes::*;
use crate::release::io::{DryForge, DryGit, Forge, Git, Issue, Pull, Registry, Workspace};

/// A tweak to the fakes before an `assemble` guard case runs.
type Setup = fn(&mut FakeGit<'_>, &mut FakeWorkspace<'_>);

const CRATES: [&str; 3] = ["spate-core", "spate-kafka", "spate"];

/// A release subject names its version, with or without the squash suffix;
/// near misses do not, so the publish never fires on them.
#[test]
fn the_release_subject_names_its_version() {
    let table = [
        ("release: v0.3.0", Some("0.3.0")),
        ("release: v0.3.0 (#309)", Some("0.3.0")),
        ("release: v10.20.30 (#1)", Some("10.20.30")),
        ("release: v0.3", None),
        ("release: v0.3.0 and a trailer", None),
        ("release: v0.3.0 (#)", None),
        ("release: v0.3.0 (#3a)", None),
        ("release: verify the tag", None),
        ("workspace: release v0.3.0", None),
        ("chore: release v0.3.0", None),
        ("docs: mention release: v0.3.0 in a page", None),
    ];
    for (subject, want) in table {
        assert_eq!(version_from_subject(subject), want.map(v), "{subject}");
    }
}

/// Only a same-repository branch of the exact release shape is a release
/// branch.
#[test]
fn a_release_branch_has_the_exact_shape() {
    assert!(is_release_branch("release/v0.3.0"));
    assert!(!is_release_branch("release/v2-planning"));
    assert!(!is_release_branch("release/v0.3"));
    assert!(!is_release_branch("feature/release/v0.3.0"));
}

/// The happy path commits, pushes the branch, opens the pull request and
/// enables auto-merge, closing only same-repository release branches at
/// another version.
#[test]
fn assemble_supersedes_only_other_release_branches() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.pulls = vec![
        Pull {
            number: 1,
            head: "release/v0.2.9".into(),
            cross_repository: false,
        },
        Pull {
            number: 2,
            head: "release/v0.2.9".into(),
            cross_repository: true,
        },
        Pull {
            number: 3,
            head: "release/v2-planning".into(),
            cross_repository: false,
        },
        Pull {
            number: 4,
            head: "core/a-change".into(),
            cross_repository: false,
        },
    ];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    assert_eq!(
        log.entries(),
        [
            "generate 0.3.0",
            "commit release: v0.3.0",
            "close #1",
            "push HEAD:refs/heads/release/v0.3.0 force=true",
            "create pull release: v0.3.0 release/v0.3.0 release",
            "auto-merge #99",
        ]
    );
}

/// Re-dispatching a version reuses its open pull request.
#[test]
fn assemble_reuses_the_open_pull_request() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.pulls = vec![Pull {
        number: 7,
        head: "release/v0.3.0".into(),
        cross_repository: false,
    }];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    assert!(!log.has("create pull"));
    assert!(log.has("auto-merge #7"));
}

/// Every guard refuses before anything is generated.
#[test]
fn assemble_guards_refuse_before_generating() {
    let cases: [(&str, Setup, &str); 8] = [
        ("dirty", |g, _| g.clean = false, "not clean"),
        ("not a version", |_, _| {}, "is not X.Y.Z"),
        (
            "same version",
            |_, w| w.version = v("0.3.0"),
            "already at 0.3.0",
        ),
        (
            "half finished",
            |g, _| g.last_tag = "v0.1.0".into(),
            "half-finished",
        ),
        (
            "tagged",
            |g, _| {
                g.remote.borrow_mut().insert("v0.3.0".into(), OTHER.into());
            },
            "already tagged",
        ),
        (
            "derive disagrees",
            |_, w| w.derived = v("0.2.1"),
            "derives 0.2.1",
        ),
        (
            "no release post",
            |_, w| std::fs::remove_file(w.post("2026-10-11-spate-0-3.md")).unwrap(),
            "no post with 'slug: releases/0.3'",
        ),
        (
            "two release posts",
            |_, w| {
                std::fs::create_dir(w.post("drafts")).unwrap();
                std::fs::write(w.post("drafts/0-3.mdx"), POST_0_3).unwrap();
            },
            "more than one post",
        ),
    ];
    for (name, setup, want) in cases {
        let log = Log::default();
        let mut git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        setup(&mut git, &mut workspace);
        let input = if name == "not a version" {
            "0.3"
        } else {
            "0.3.0"
        };
        let pauses = Pauses::default();
        let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), input).unwrap_err();
        assert!(err.message.contains(want), "{name}: {}", err.message);
        assert!(log.entries().is_empty(), "{name}: {:?}", log.entries());
    }
}

fn issue(number: u64) -> Issue {
    Issue {
        number,
        title: format!("defect {number}"),
        url: format!("https://github.com/spate-etl/spate/issues/{number}"),
    }
}

/// A minor release is gated on its own milestone, and a patch release also on
/// the next minor's.
#[test]
fn the_security_gate_reads_the_release_and_next_minor_milestones() {
    assert_eq!(security_milestones(v("0.5.0")), ["v0.5.0"]);
    assert_eq!(security_milestones(v("0.4.1")), ["v0.4.1", "v0.5.0"]);
    assert_eq!(security_milestones(v("1.2.3")), ["v1.2.3", "v1.3.0"]);
    assert_eq!(security_milestones(v("1.0.0")), ["v1.0.0"]);
}

/// An open `security` issue on the release's milestone refuses the release
/// before anything is generated, and the refusal names every one.
#[test]
fn assemble_refuses_an_open_security_issue_on_its_milestone() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.security = vec![
        ("v0.3.0", issue(11)),
        ("v0.3.0", issue(12)),
        ("v0.4.0", issue(13)),
    ];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap_err();
    assert!(
        err.message.contains(
            "v0.3.0 cannot release while these security issues are open:\n  \
             #11 on v0.3.0: defect 11 (https://github.com/spate-etl/spate/issues/11)\n  \
             #12 on v0.3.0: defect 12 (https://github.com/spate-etl/spate/issues/12)"
        ),
        "{}",
        err.message
    );
    assert!(!err.message.contains("#13"), "{}", err.message);
    assert_eq!(*forge.security_queried.borrow(), ["v0.3.0"]);
    assert!(log.entries().is_empty(), "{:?}", log.entries());
}

/// A patch release ships `main`, so an open `security` issue on the next
/// minor's milestone refuses it.
#[test]
fn assemble_refuses_a_patch_on_the_next_minors_security_issue() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.security = vec![("v0.3.0", issue(21))];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    workspace.derived = v("0.2.1");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.2.1").unwrap_err();
    assert!(err.message.contains("#21 on v0.3.0"), "{}", err.message);
    assert_eq!(*forge.security_queried.borrow(), ["v0.2.1", "v0.3.0"]);
    assert!(log.entries().is_empty(), "{:?}", log.entries());
}

/// A patch release with open `security` issues on its own milestone and on
/// the next minor's reads both and names every one.
#[test]
fn assemble_refuses_a_patch_naming_the_issues_on_both_milestones() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.security = vec![("v0.2.1", issue(41)), ("v0.3.0", issue(42))];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    workspace.derived = v("0.2.1");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.2.1").unwrap_err();
    assert!(
        err.message.contains(
            "#41 on v0.2.1: defect 41 (https://github.com/spate-etl/spate/issues/41)\n  \
             #42 on v0.3.0: defect 42 (https://github.com/spate-etl/spate/issues/42)"
        ),
        "{}",
        err.message
    );
    assert_eq!(*forge.security_queried.borrow(), ["v0.2.1", "v0.3.0"]);
}

/// A re-dispatch refused for an open `security` issue closes the release pull
/// request the first dispatch opened, so its auto-merge cannot publish.
#[test]
fn a_refused_redispatch_closes_the_open_release_pull_request() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.pulls = vec![Pull {
        number: 7,
        head: "release/v0.3.0".to_owned(),
        cross_repository: false,
    }];
    forge.security = vec![("v0.3.0", issue(11))];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap_err();
    assert!(err.message.contains("cannot release"), "{}", err.message);
    assert_eq!(log.entries(), ["close #7"]);
}

/// The refusal escapes workflow-command text in an issue title, since the
/// Actions runner reads `##[` anywhere in a line as a command.
#[test]
fn the_refusal_prints_no_workflow_command_from_an_issue_title() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.security = vec![(
        "v0.3.0",
        Issue {
            number: 41,
            title: "a defect ##[set-output name=released;]true".into(),
            url: "https://github.com/spate-etl/spate/issues/41".into(),
        },
    )];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap_err();
    assert!(err.message.contains("#41 on v0.3.0"), "{}", err.message);
    assert!(!err.message.contains("##["), "{}", err.message);
}

/// A refused security issue listing stops a real `assemble`.
#[test]
fn assemble_stops_on_a_refused_security_listing() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.security_reads_fail = true;
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap_err();
    assert!(
        err.message.contains("injected: security"),
        "{}",
        err.message
    );
    assert!(log.entries().is_empty(), "{:?}", log.entries());
}

/// A rehearsal reports an open `security` issue and assembles anyway. A
/// refused listing fails a strict rehearsal and reads as empty in a lenient
/// one.
#[test]
fn a_dry_assemble_reports_open_security_issues_and_continues() {
    let log = Log::default();
    let mut forge = FakeForge::new(&log);
    forge.security = vec![("v0.3.0", issue(31))];
    let dry = DryForge(&forge, true);
    assert!(dry.open_security_issues("v0.3.0").unwrap().is_empty());

    let git = FakeGit::new(&log);
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let (dry_git, dry_forge) = (DryGit(&git), DryForge(&forge, true));
    assemble(
        &host!(&dry_git, &dry_forge, &registry, &workspace, pauses),
        "0.3.0",
    )
    .unwrap();
    assert_eq!(log.entries(), ["generate 0.3.0", "commit release: v0.3.0"]);

    let mut refused = FakeForge::new(&log);
    refused.security_reads_fail = true;
    let err = DryForge(&refused, true)
        .open_security_issues("v0.3.0")
        .unwrap_err();
    assert!(
        err.message.contains("injected: security"),
        "{}",
        err.message
    );
    assert!(
        DryForge(&refused, false)
            .open_security_issues("v0.3.0")
            .unwrap()
            .is_empty()
    );
}

/// A patch release requires no post, and assembles without one.
#[test]
fn assemble_needs_no_post_for_a_patch_release() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    workspace.derived = v("0.2.1");
    std::fs::remove_file(workspace.post("2026-10-11-spate-0-3.md")).unwrap();
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.2.1").unwrap();
    assert!(log.has("commit release: v0.2.1"), "{:?}", log.entries());
}

/// A draft flag the site build reads as a draft but the release commit would
/// not remove stops `assemble`, so the release never links an unbuilt post.
#[test]
fn a_post_docusaurus_keeps_as_draft_is_not_assembled() {
    for line in [
        "draft: True",
        "draft: true # until the release",
        "draft: \"true\"",
    ] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let path = workspace.post("2026-10-11-spate-0-3.md");
        std::fs::write(&path, POST_0_3.replace("draft: true", line)).unwrap();
        let pauses = Pauses::default();
        let result = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0");
        let still_draft = std::fs::read_to_string(&path).unwrap().contains(line);
        assert!(
            result.is_err() || !still_draft,
            "{line}: assembled and committed with the post still a draft"
        );
    }
}

/// A post in a file or directory the blog build excludes does not count as
/// the release's post.
#[test]
fn an_excluded_post_does_not_satisfy_the_guard() {
    for name in ["_2026-10-11-spate-0-3.md", "_wip/2026-10-11-spate-0-3.md"] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        std::fs::remove_file(workspace.post("2026-10-11-spate-0-3.md")).unwrap();
        let path = workspace.post(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, POST_0_3).unwrap();
        let pauses = Pauses::default();
        let result = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0");
        assert!(
            result.is_err(),
            "{name}: assembled on a post the site build excludes"
        );
    }
}

/// A `draft` key in any YAML spelling the site build reads as a draft stops
/// `assemble` unless the release commit removes it.
#[test]
fn a_draft_key_spelled_otherwise_is_not_assembled() {
    for line in [
        "draft : true",
        "\"draft\": true",
        "'draft': true",
        "draft\t: true",
        "? draft\n: true",
    ] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let path = workspace.post("2026-10-11-spate-0-3.md");
        std::fs::write(&path, POST_0_3.replace("draft: true", line)).unwrap();
        let pauses = Pauses::default();
        let result = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0");
        let still_draft = std::fs::read_to_string(&path).unwrap().contains(line);
        assert!(
            result.is_err() || !still_draft,
            "{line:?}: assembled and committed with the post still a draft"
        );
    }
}

/// A hidden post, which the blog build's glob never matches, does not count
/// as the release's post.
#[test]
fn a_hidden_post_does_not_satisfy_the_guard() {
    for name in [".2026-10-11-spate-0-3.md", ".wip/2026-10-11-spate-0-3.md"] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        std::fs::remove_file(workspace.post("2026-10-11-spate-0-3.md")).unwrap();
        let path = workspace.post(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, POST_0_3).unwrap();
        let pauses = Pauses::default();
        let result = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0");
        assert!(
            result.is_err(),
            "{name}: assembled on a post the site build excludes"
        );
    }
}

/// The release commit carries the post with its `draft: true` line removed and
/// every other byte as written, so the post goes live with the release.
#[test]
fn the_release_commit_publishes_the_post() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let path = workspace.post("2026-10-11-spate-0-3.md");
    git.watch = Some(path.clone());
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    let published = POST_0_3.replace("draft: true\n", "");
    assert_eq!(git.committed.borrow().as_deref(), Some(published.as_str()));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), published);
}

/// A second assemble over a post with no draft flag passes and leaves the post
/// as it is.
#[test]
fn assemble_leaves_a_post_with_no_draft_flag_as_it_is() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let path = workspace.post("2026-10-11-spate-0-3.md");
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    let published = std::fs::read_to_string(&path).unwrap();
    assert_eq!(published, POST_0_3.replace("draft: true\n", ""));

    forge.pulls = vec![Pull {
        number: 99,
        head: "release/v0.3.0".into(),
        cross_repository: false,
    }];
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), published);
    assert_eq!(
        log.entries()[5..],
        [
            "generate 0.3.0",
            "commit release: v0.3.0",
            "push HEAD:refs/heads/release/v0.3.0 force=true",
            "auto-merge #99",
        ]
    );
}

/// A fresh release packages every crate and excludes none.
#[test]
fn prepare_selects_every_crate_on_a_fresh_release() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let prepared = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap();
    assert_eq!(prepared.pending, CRATES);
    assert!(prepared.excludes.is_empty());
    assert_eq!(log.entries(), ["package excluding []"]);
}

/// An upload that fails midway is resumed by a re-run that excludes what
/// landed, verifies it came from this commit, and publishes the rest.
#[test]
fn an_upload_that_fails_midway_resumes_from_the_registry() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.registry = Some(&registry);
    workspace.upload_stops_after = Some(1);
    let pauses = Pauses::default();
    let host = host!(&git, &forge, &registry, &workspace, pauses);

    let staged = artifacts(true);
    prepare(&host, Some(SHA)).unwrap();
    assert!(upload(&host, true, staged.dir()).is_err());

    let second = prepare(&host, Some(SHA)).unwrap();
    assert_eq!(second.excludes, ["spate-core"]);
    assert_eq!(second.pending, ["spate-kafka", "spate"]);
    assert_eq!(
        pauses.of(API_INTERVAL),
        1,
        "one trustpub read for the excluded crate"
    );

    // The upload reads the registry itself, so a re-run of the publish alone
    // excludes what the first attempt landed.
    workspace.upload_stops_after = None;
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    upload(&host, true, staged.dir()).unwrap();
    assert!(
        log.has("publish excluding [spate-core]"),
        "{:?}",
        log.entries()
    );
    for c in CRATES {
        assert_eq!(registry.trustpub_sha(c, v("0.3.0")).unwrap(), SHA, "{c}");
    }
}

/// A crate already at the version from another commit means the release is
/// split across trees, and nothing is packaged.
#[test]
fn prepare_refuses_a_release_split_across_trees() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    registry.publish("spate-core", "0.3.0", "x", OTHER);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    let err = prepare(&host, Some(SHA)).unwrap_err();
    assert!(
        err.message.contains("split across trees"),
        "{}",
        err.message
    );
    let err = prepare(&host, None).unwrap_err();
    assert!(err.message.contains("no EXPECTED_SHA"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// A crate the registry has never heard of, and an index that cannot be read,
/// each stop the run.
#[test]
fn prepare_refuses_an_unknown_crate_or_an_unreadable_index() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();

    let registry = FakeRegistry::with(&CRATES[..2]);
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("not on the registry at all"),
        "{}",
        err.message
    );

    let mut registry = FakeRegistry::with(&CRATES);
    registry.status = Some("503".into());
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(err.message.contains("answered 503"), "{}", err.message);
}

/// A head that is not a release commit, or disagrees with the manifest, is not
/// published.
#[test]
fn prepare_requires_the_release_commit() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();

    git.subject = "core: a change".into();
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("not a release commit"),
        "{}",
        err.message
    );

    git.subject = "release: v0.3.0".into();
    workspace.version = v("0.2.0");
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("Cargo.toml says 0.2.0"),
        "{}",
        err.message
    );
}

/// A finished registry state: every crate at 0.3.0 from `SHA`, packaged by
/// this run with matching bytes.
fn published(log: &Log) -> (FakeRegistry, FakeWorkspace<'_>) {
    let registry = FakeRegistry::with(&CRATES);
    for c in CRATES {
        registry.publish(c, "0.3.0", &digest(&format!("sum-{c}")), SHA);
    }
    let workspace = FakeWorkspace::new(log, &CRATES);
    workspace.package(&[]).unwrap();
    log.0.borrow_mut().clear();
    (registry, workspace)
}

/// A staged artifacts directory: every crate at 0.3.0 with the bytes the fake
/// registry serves, an SBOM each, `SHA256SUMS`, and an attestation bundle when
/// `attested`.
fn artifacts(attested: bool) -> Scratch {
    let dir = Scratch::new("spate-xtask-release-artifacts").unwrap();
    let mut sums = String::new();
    for c in CRATES {
        for (name, body) in [
            (format!("{c}-0.3.0.crate"), format!("sum-{c}")),
            (format!("{c}-0.3.0.cdx.json"), "{}".to_owned()),
        ] {
            std::fs::write(dir.join(&name), &body).unwrap();
            sums.push_str(&format!("{}  {name}\n", digest(&body)));
        }
    }
    std::fs::write(dir.join(SUMS), sums).unwrap();
    if attested {
        std::fs::write(dir.join("provenance.intoto.jsonl"), "{\"a\":1}\n").unwrap();
    }
    dir
}

/// The happy path tags, releases with every asset, and deploys the docs.
#[test]
fn finish_tags_releases_and_deploys() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert_eq!(
        log.entries(),
        [
            format!("tag v0.3.0 {SHA}"),
            "push refs/tags/v0.3.0 force=false".to_owned(),
            "draft v0.3.0".to_owned(),
            "upload v0.3.0 spate-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 spate-core-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 spate-kafka-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 SHA256SUMS clobber=true".to_owned(),
            "upload v0.3.0 spate-v0.3.0.intoto.jsonl clobber=true".to_owned(),
            "publish v0.3.0".to_owned(),
            "verify release v0.3.0".to_owned(),
            "dispatch docs".to_owned(),
        ]
    );
    assert_eq!(pauses.of(API_INTERVAL), CRATES.len());
}

/// A tag push that reaches origin and then reports failure stops the run, and
/// the re-run finds the tag on this commit and carries on without tagging
/// again.
#[test]
fn a_tag_push_that_fails_after_landing_resumes() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    git.push_lands_then_fails = true;
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    assert!(
        finish(
            &host!(&git, &forge, &registry, &workspace, pauses),
            v("0.3.0"),
            SHA,
            staged.dir(),
            "spate-etl/spate"
        )
        .is_err()
    );
    assert!(!log.has("draft"));

    git.push_lands_then_fails = false;
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    let tags = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("tag "))
        .count();
    assert_eq!(tags, 1, "{:?}", log.entries());
    let pushes = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("push refs/tags/"))
        .count();
    assert_eq!(pushes, 1, "{:?}", log.entries());
    assert!(log.has("draft v0.3.0"));
}

/// A tag on origin naming another commit is never moved.
#[test]
fn finish_refuses_a_tag_on_another_commit() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote
        .borrow_mut()
        .insert("v0.3.0".into(), OTHER.into());
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("points at"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// A crate the registry attributes to another commit, and served bytes that
/// differ from the packaged ones, each stop the run before it tags.
#[test]
fn finish_refuses_a_foreign_commit_or_foreign_bytes() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let pauses = Pauses::default();

    let (registry, workspace) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate")
        .unwrap()
        .last_mut()
        .unwrap()
        .2 = OTHER.into();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("trustpub sha"), "{}", err.message);

    let (registry, workspace) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate-kafka")
        .unwrap()
        .last_mut()
        .unwrap()
        .1 = "tampered".into();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("registry serves tampered"),
        "{}",
        err.message
    );
    assert!(!log.has("tag"));
}

/// An index lagging the upload is waited out, and so is a consumer resolution
/// the CDN has not caught up with.
#[test]
fn finish_waits_out_registry_lag() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    registry.lag.borrow_mut().insert("spate-kafka".into(), 2);
    *workspace.resolve_failures.borrow_mut() = 3;
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert_eq!(
        pauses.of(LAG_INTERVAL),
        5,
        "two index waits and three resolution waits"
    );
}

/// Lag that outlasts every attempt fails with cargo's answer.
#[test]
fn finish_gives_up_on_a_release_that_never_resolves() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    *workspace.resolve_failures.borrow_mut() = 5;
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("cannot resolve"), "{}", err.message);
    assert_eq!(pauses.of(LAG_INTERVAL), 5);
    assert!(!log.has("tag"));
}

/// An index that never serves the version stops `finish` after the full wait,
/// before tagging.
#[test]
fn finish_gives_up_on_an_index_that_never_serves() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    registry.lag.borrow_mut().insert("spate-kafka".into(), 5);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("never served"), "{}", err.message);
    assert_eq!(pauses.of(LAG_INTERVAL), 5);
    assert!(!log.has("tag"));
}

/// An index status other than 200 or 404 stops `finish` at once and names the
/// code.
#[test]
fn finish_stops_on_an_index_status() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (mut registry, workspace) = published(&log);
    registry.status = Some("403".into());
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert_eq!(pauses.of(LAG_INTERVAL), 0, "a 403 is waited out as lag");
    assert!(err.message.contains("403"), "{}", err.message);
    assert!(!log.has("tag"));
}

/// A rehearsed `assemble` commits and makes no push, close, pull request or
/// auto-merge, whether the forge's listings answer or are refused.
#[test]
fn a_dry_assemble_writes_nothing_outside_the_tree() {
    for reads_fail in [false, true] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let mut forge = FakeForge::new(&log);
        forge.reads_fail = reads_fail;
        forge.pulls = vec![
            Pull {
                number: 1,
                head: "release/v0.2.9".into(),
                cross_repository: false,
            },
            Pull {
                number: 7,
                head: "release/v0.3.0".into(),
                cross_repository: false,
            },
        ];
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let pauses = Pauses::default();
        let (git, forge) = (DryGit(&git), DryForge(&forge, false));
        assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
        assert_eq!(
            log.entries(),
            ["generate 0.3.0", "commit release: v0.3.0"],
            "reads_fail={reads_fail}"
        );
    }
}

/// A strict rehearsal fails on a refused pull request listing, and a lenient
/// one reads it as empty; the strict one pushes nothing.
#[test]
fn a_strict_dry_forge_passes_a_refused_listing_through() {
    let log = Log::default();
    let mut forge = FakeForge::new(&log);
    forge.reads_fail = true;

    let strict = DryForge(&forge, true);
    assert!(strict.open_pulls().is_err());
    assert!(strict.pull_for_head("release/v0.3.0").is_err());
    let lenient = DryForge(&forge, false);
    assert!(lenient.open_pulls().unwrap().is_empty());
    assert_eq!(lenient.pull_for_head("release/v0.3.0").unwrap(), None);

    let git = FakeGit::new(&log);
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    let (git, forge) = (DryGit(&git), DryForge(&forge, true));
    let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap_err();
    assert!(err.message.contains("injected: list"), "{}", err.message);
    assert_eq!(log.entries(), ["generate 0.3.0", "commit release: v0.3.0"]);
}

/// A rehearsal's git passes the commit through and never tags or pushes.
#[test]
fn a_dry_git_never_tags_or_pushes() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let dry = DryGit(&git);
    dry.tag("v0.3.0", SHA).unwrap();
    dry.push("refs/tags/v0.3.0", false).unwrap();
    dry.push("HEAD:refs/heads/release/v0.3.0", true).unwrap();
    dry.commit_all("release: v0.3.0", "").unwrap();
    assert_eq!(log.entries(), ["commit release: v0.3.0"]);
}

/// A release created moments after its tag is retried once.
#[test]
fn finish_retries_the_release_once() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    *forge.create_release_failures.borrow_mut() = 1;
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert!(log.has("draft v0.3.0"));
    assert_eq!(pauses.of(Duration::from_secs(10)), 1);
}

/// A release with no provenance bundle among its assets fails with the
/// recovery steps, and a refused bundle upload stops the run before the
/// publish.
#[test]
fn finish_requires_a_provenance_bundle() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        artifacts(false).dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("gh attestation download"),
        "{}",
        err.message
    );

    let mut forge = FakeForge::new(&log);
    forge.refuse_bundle = true;
    let staged = artifacts(true);
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert_eq!(err.message, "injected: upload bundle");
    assert!(!log.has("publish"), "{:?}", log.entries());
}

/// A trailing space after the version is not a release subject.
#[test]
fn a_release_subject_carries_nothing_after_its_suffix() {
    assert_eq!(version_from_subject("release: v0.3.0 "), None);
    assert_eq!(version_from_subject("release: v0.3.0 (#1) "), None);
}

/// Each failing write in the pull request step stops the step there.
#[test]
fn assemble_stops_at_the_first_failing_write() {
    let cases: [(&str, Option<&'static str>, bool, &[&str]); 4] = [
        (
            "close",
            Some("close"),
            false,
            &["generate 0.3.0", "commit release: v0.3.0"],
        ),
        (
            "push",
            None,
            true,
            &["generate 0.3.0", "commit release: v0.3.0", "close #1"],
        ),
        (
            "create",
            Some("create"),
            false,
            &[
                "generate 0.3.0",
                "commit release: v0.3.0",
                "close #1",
                "push HEAD:refs/heads/release/v0.3.0 force=true",
            ],
        ),
        (
            "merge",
            Some("merge"),
            false,
            &[
                "generate 0.3.0",
                "commit release: v0.3.0",
                "close #1",
                "push HEAD:refs/heads/release/v0.3.0 force=true",
                "create pull release: v0.3.0 release/v0.3.0 release",
            ],
        ),
    ];
    for (name, forge_fails, push_fails, want) in cases {
        let log = Log::default();
        let mut git = FakeGit::new(&log);
        git.push_fails = push_fails;
        let mut forge = FakeForge::new(&log);
        forge.fails = forge_fails;
        forge.pulls = vec![Pull {
            number: 1,
            head: "release/v0.2.9".into(),
            cross_repository: false,
        }];
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let pauses = Pauses::default();
        assert!(
            assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").is_err(),
            "{name}"
        );
        assert_eq!(log.entries(), want, "{name}");
    }
}

/// A publishable crate missing its description or license stops `prepare`
/// before anything is packaged.
#[test]
fn prepare_refuses_missing_metadata() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.missing_metadata = vec!["spate-kafka".into()];
    let pauses = Pauses::default();
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(err.message.contains("in: spate-kafka"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// The upload refuses to run without a token, skips when the registry holds
/// every crate, and refuses a crate the build did not stage.
#[test]
fn upload_needs_a_token_and_a_staged_crate() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let pauses = Pauses::default();
    let staged = artifacts(true);

    let (registry, workspace) = published(&log);
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    assert!(upload(&host, false, staged.dir()).is_err());
    upload(&host, true, staged.dir()).unwrap();
    assert!(log.entries().is_empty());

    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    std::fs::remove_file(staged.join("spate-kafka-0.3.0.crate")).unwrap();
    let err = upload(
        &host!(&git, &forge, &registry, &workspace, pauses),
        true,
        staged.dir(),
    )
    .unwrap_err();
    assert!(
        err.message.contains("spate-kafka 0.3.0 not staged"),
        "{}",
        err.message
    );
    assert!(log.entries().is_empty());
}

/// An index status other than 200 or 404 stops the upload before it publishes.
#[test]
fn upload_stops_on_an_index_status() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let pauses = Pauses::default();
    let staged = artifacts(true);
    let mut registry = FakeRegistry::with(&CRATES);
    registry.status = Some("403".into());
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let err = upload(
        &host!(&git, &forge, &registry, &workspace, pauses),
        true,
        staged.dir(),
    )
    .unwrap_err();
    assert!(err.message.contains("403"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// A crate the index does not hold at all is still to publish, so it must be
/// staged.
#[test]
fn a_crate_missing_from_the_index_is_pending() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let pauses = Pauses::default();
    let staged = artifacts(true);
    std::fs::remove_file(staged.join("spate-0.3.0.crate")).unwrap();
    let registry = FakeRegistry::with(&["spate-core", "spate-kafka"]);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let err = upload(
        &host!(&git, &forge, &registry, &workspace, pauses),
        true,
        staged.dir(),
    )
    .unwrap_err();
    assert!(
        err.message.contains("spate 0.3.0 not staged"),
        "{}",
        err.message
    );
}

/// A later attempt keeps the release's existing `SHA256SUMS` and bundle.
#[test]
fn finish_keeps_an_earlier_attempts_pair() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Draft);
    forge
        .assets
        .borrow_mut()
        .extend([SUMS.to_owned(), "spate-v0.3.0.intoto.jsonl".to_owned()]);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert!(
        !log.has(&format!("upload v0.3.0 {SUMS}")),
        "{:?}",
        log.entries()
    );
    assert!(
        !log.has("upload v0.3.0 spate-v0.3.0.intoto.jsonl"),
        "{:?}",
        log.entries()
    );
    assert!(log.has("dispatch docs"));
}

/// `prepare`'s outputs are appended to the file, after whatever it held, with
/// the pending crates as a JSON list for a job matrix.
#[test]
fn the_outputs_append_five_keys() {
    let scratch = Scratch::new("spate-xtask-release-outputs").unwrap();
    let path = scratch.join("output");
    std::fs::write(&path, "earlier=1\n").unwrap();
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: vec!["spate".into()],
        excludes: vec!["spate-core".into(), "spate-kafka".into()],
    };
    append_outputs(&path, &prepared).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "earlier=1\nversion=0.3.0\nexcludes=spate-core spate-kafka\npending=1\ncrates=[\"spate\"]\nstaged=false\n"
    );
}

/// A tag push that fails before reaching origin is retried by the re-run,
/// which tags again.
#[test]
fn a_tag_push_that_never_landed_is_retried() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    git.push_fails = true;
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    assert!(
        finish(
            &host!(&git, &forge, &registry, &workspace, pauses),
            v("0.3.0"),
            SHA,
            staged.dir(),
            "spate-etl/spate"
        )
        .is_err()
    );
    git.push_fails = false;
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    let tags = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("tag "))
        .count();
    assert_eq!(tags, 2, "{:?}", log.entries());
}

/// A re-run finding the release already made uploads its assets and deploys
/// without creating it again; a failed asset upload stops before the deploy.
#[test]
fn finish_completes_an_existing_release_and_stops_on_a_failed_upload() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Draft);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert!(
        !log.has("draft ") && !log.has("tag "),
        "{:?}",
        log.entries()
    );
    assert!(log.has("dispatch docs"));

    let log = Log::default();
    let mut forge = FakeForge::new(&log);
    forge.fails = Some("upload");
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    );
    assert!(err.is_err());
    assert!(!log.has("dispatch docs"));
}

const SIGNER: &str = "spate-etl/spate/.github/workflows/release-build.yml";

/// Staging copies each packaged crate, writes an SBOM per publishable crate,
/// and lists both in `SHA256SUMS` by name.
#[test]
fn stage_collects_crates_sboms_and_their_sums() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.package(&[]).unwrap();
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: CRATES.iter().map(|c| (*c).to_owned()).collect(),
        excludes: vec![],
    };
    let out = Scratch::new("spate-xtask-release-stage").unwrap();
    let pauses = Pauses::default();
    stage(
        &host!(&git, &forge, &registry, &workspace, pauses),
        &prepared,
        out.dir(),
    )
    .unwrap();
    let sums = parse_sums(&std::fs::read_to_string(out.join(SUMS)).unwrap()).unwrap();
    let names: Vec<&str> = sums.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "spate-0.3.0.cdx.json",
            "spate-0.3.0.crate",
            "spate-core-0.3.0.cdx.json",
            "spate-core-0.3.0.crate",
            "spate-kafka-0.3.0.cdx.json",
            "spate-kafka-0.3.0.crate",
        ]
    );
    assert_eq!(sums[1].1, digest("sum-spate"));
}

/// A build after part of the version landed stages nothing, so the publish
/// reads the first build's set.
#[test]
fn stage_writes_nothing_once_a_crate_is_published() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.package(&["spate-core".to_owned()]).unwrap();
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: vec!["spate-kafka".into(), "spate".into()],
        excludes: vec!["spate-core".into()],
    };
    let out = Scratch::new("spate-xtask-release-stage-resumed").unwrap();
    let dir = out.join("release");
    let pauses = Pauses::default();
    stage(
        &host!(&git, &forge, &registry, &workspace, pauses),
        &prepared,
        &dir,
    )
    .unwrap();
    assert!(!dir.try_exists().unwrap());
}

/// A pending crate this run did not package cannot be staged.
#[test]
fn stage_refuses_an_unpackaged_crate() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: vec!["spate".into()],
        excludes: vec![],
    };
    let out = Scratch::new("spate-xtask-release-stage-missing").unwrap();
    let pauses = Pauses::default();
    let err = stage(
        &host!(&git, &forge, &registry, &workspace, pauses),
        &prepared,
        out.dir(),
    )
    .unwrap_err();
    assert!(err.message.contains("not packaged"), "{}", err.message);
}

/// Verification checks every digest, then the provenance of `SHA256SUMS` and
/// the provenance and SBOM attestation of each crate against the signer and
/// commit, then that this checkout packages the same bytes.
#[test]
fn verify_artifacts_checks_sums_attestations_and_repackaged_bytes() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    verify_artifacts(
        &host!(&git, &forge, &registry, &workspace, pauses),
        staged.dir(),
        SIGNER,
        SHA,
    )
    .unwrap();
    let mut want = vec![format!("verify provenance {SUMS} by {SIGNER}")];
    for n in [
        "spate-0.3.0.crate",
        "spate-core-0.3.0.crate",
        "spate-kafka-0.3.0.crate",
    ] {
        want.push(format!("verify provenance {n} by {SIGNER}"));
        want.push(format!("verify sbom {n} by {SIGNER}"));
    }
    want.push("repackage excluding []".to_owned());
    assert_eq!(log.entries(), want);
}

/// A digest that disagrees, a file the list omits, a missing bundle, an
/// unpublished crate the build did not stage, an attestation that does not
/// verify, one naming another commit, and repackaged bytes that differ each
/// fail the check.
#[test]
fn verify_artifacts_refuses_any_mismatch() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let check = |forge: &FakeForge<'_>, workspace: &FakeWorkspace<'_>, dir: &Path, commit: &str| {
        verify_artifacts(
            &host!(&git, forge, &registry, workspace, pauses),
            dir,
            SIGNER,
            commit,
        )
    };

    let staged = artifacts(true);
    std::fs::write(staged.join("spate-0.3.0.crate"), "tampered").unwrap();
    let err = check(&forge, &workspace, staged.dir(), SHA).unwrap_err();
    assert!(
        err.message.contains("spate-0.3.0.crate: SHA256SUMS says"),
        "{}",
        err.message
    );

    let staged = artifacts(true);
    std::fs::write(staged.join("extra-0.3.0.crate"), "x").unwrap();
    let err = check(&forge, &workspace, staged.dir(), SHA).unwrap_err();
    assert!(err.message.contains("extra-0.3.0.crate"), "{}", err.message);

    let staged = artifacts(false);
    let err = check(&forge, &workspace, staged.dir(), SHA).unwrap_err();
    assert!(
        err.message.contains("no attestation bundle"),
        "{}",
        err.message
    );

    let staged = artifacts(true);
    let sums = std::fs::read_to_string(staged.join(SUMS)).unwrap();
    let kept: String = sums
        .lines()
        .filter(|l| !l.ends_with("spate-0.3.0.crate"))
        .map(|l| format!("{l}\n"))
        .collect();
    std::fs::write(staged.join(SUMS), kept).unwrap();
    std::fs::remove_file(staged.join("spate-0.3.0.crate")).unwrap();
    let err = check(&forge, &workspace, staged.dir(), SHA).unwrap_err();
    assert!(
        err.message.contains("spate 0.3.0 not staged"),
        "{}",
        err.message
    );

    let staged = artifacts(true);
    assert!(check(&forge, &workspace, staged.dir(), "another-commit").is_err());

    forge.unattested = Some("spate-kafka-0.3.0.crate".into());
    assert!(check(&forge, &workspace, staged.dir(), SHA).is_err());
    forge.unattested = None;

    workspace.repackage_differs = true;
    let err = check(&forge, &workspace, staged.dir(), SHA).unwrap_err();
    assert!(
        err.message.contains("bytes no attestation covers"),
        "{}",
        err.message
    );
}

/// With every crate already on the registry there is nothing to upload, so
/// nothing is verified or repackaged.
#[test]
fn verify_artifacts_accepts_a_fully_published_release() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let staged = artifacts(false);
    let mut sums = String::new();
    for c in CRATES {
        std::fs::remove_file(staged.join(&format!("{c}-0.3.0.crate"))).unwrap();
        sums.push_str(&format!("{}  {c}-0.3.0.cdx.json\n", digest("{}")));
    }
    std::fs::write(staged.join(SUMS), sums).unwrap();
    let pauses = Pauses::default();
    verify_artifacts(
        &host!(&git, &forge, &registry, &workspace, pauses),
        staged.dir(),
        SIGNER,
        SHA,
    )
    .unwrap();
    assert!(log.entries().is_empty());
}

/// A resumed run that staged no crate leaves the release's attested
/// `SHA256SUMS` and bundle in place, and needs them to be there.
#[test]
fn finish_without_staged_crates_keeps_the_attested_assets() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Draft);
    let (registry, workspace) = published(&log);
    let staged = artifacts(false);
    for c in CRATES {
        std::fs::remove_file(staged.join(&format!("{c}-0.3.0.crate"))).unwrap();
    }
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("lacks SHA256SUMS"), "{}", err.message);
    assert!(
        !log.has(&format!("upload v0.3.0 {SUMS}")),
        "{:?}",
        log.entries()
    );

    forge
        .assets
        .borrow_mut()
        .extend([SUMS.to_owned(), "spate-v0.3.0.intoto.jsonl".to_owned()]);
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert!(
        !log.has(&format!("upload v0.3.0 {SUMS}")),
        "{:?}",
        log.entries()
    );
}

/// Staging refuses a directory that already holds files, since everything in
/// it would be listed and attested.
#[test]
fn stage_refuses_a_directory_in_use() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.package(&[]).unwrap();
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: vec!["spate".into()],
        excludes: vec![],
    };
    let out = Scratch::new("spate-xtask-release-stage-used").unwrap();
    std::fs::write(out.join("stale.crate"), "old").unwrap();
    let pauses = Pauses::default();
    let err = stage(
        &host!(&git, &forge, &registry, &workspace, pauses),
        &prepared,
        out.dir(),
    )
    .unwrap_err();
    assert!(err.message.contains("not empty"), "{}", err.message);
}

/// A checksum line is a 64-digit hex digest, two spaces and a name.
#[test]
fn checksum_lines_are_strict() {
    let good = format!("{}  a.crate\n", "0".repeat(64));
    assert_eq!(
        parse_sums(&good).unwrap(),
        [("a.crate".to_owned(), "0".repeat(64))]
    );
    assert!(parse_sums("abc  a.crate\n").is_err());
    assert!(parse_sums(&format!("{} a.crate\n", "0".repeat(64))).is_err());
}

/// Every bundle file in a directory joins into one JSON-lines file, blank
/// lines dropped.
#[test]
fn bundles_join_into_one_file() {
    let dir = Scratch::new("spate-xtask-release-bundles").unwrap();
    std::fs::write(dir.join("b.intoto.jsonl"), "{\"b\":1}\n\n").unwrap();
    std::fs::write(dir.join("a.intoto.jsonl"), "{\"a\":1}\n").unwrap();
    std::fs::write(dir.join("other.json"), "{}").unwrap();
    let out = dir.join("all.intoto.jsonl");
    assert_eq!(join_bundles(dir.dir(), &out).unwrap(), 2);
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "{\"a\":1}\n{\"b\":1}\n"
    );
    assert_eq!(
        join_bundles(dir.dir(), &out).unwrap(),
        2,
        "the output is not read back in"
    );
}

/// A repository that publishes mutable releases fails `finish` after the
/// publish, naming the setting.
#[test]
fn finish_requires_an_immutable_release() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.mutable = true;
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("not immutable"), "{}", err.message);
    assert!(!log.has("dispatch docs"));
}

/// An already published immutable release is never edited: no upload and no
/// publish, only the check that it is complete and its attestation verifies.
#[test]
fn finish_leaves_a_published_immutable_release_alone() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Published { immutable: true });
    forge.assets.borrow_mut().extend(
        CRATES
            .iter()
            .map(|c| format!("{c}-0.3.0.cdx.json"))
            .chain([SUMS.to_owned(), "spate-v0.3.0.intoto.jsonl".to_owned()]),
    );
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert_eq!(log.entries(), ["verify release v0.3.0", "dispatch docs"]);
}

/// A draft missing an SBOM is never published.
#[test]
fn finish_publishes_only_a_complete_draft() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    std::fs::remove_file(staged.join("spate-kafka-0.3.0.cdx.json")).unwrap();
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("lacks spate-kafka-0.3.0.cdx.json"),
        "{}",
        err.message
    );
    assert!(!log.has("publish"));
}

/// A finished release: tagged on `SHA`, every crate published from it, and an
/// immutable release carrying every asset.
fn released(log: &Log) -> (FakeGit<'_>, FakeForge<'_>) {
    let git = FakeGit::new(log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(log);
    *forge.release.borrow_mut() = Some(ReleaseState::Published { immutable: true });
    forge.assets.borrow_mut().extend(
        CRATES
            .iter()
            .map(|c| format!("{c}-0.3.0.cdx.json"))
            .chain([SUMS.to_owned(), "spate-v0.3.0.intoto.jsonl".to_owned()]),
    );
    (git, forge)
}

/// `verify` checks each crate's origin and attestations against the tagged
/// commit, and the release's immutability and assets.
#[test]
fn verify_judges_a_finished_release() {
    let log = Log::default();
    let (git, forge) = released(&log);
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap();
    let signer = "spate-etl/spate/.github/workflows/release-build.yml";
    let mut want = Vec::new();
    for c in CRATES {
        want.push(format!("verify provenance {c}-0.3.0.crate by {signer}"));
        want.push(format!("verify sbom {c}-0.3.0.crate by {signer}"));
    }
    want.push("verify release v0.3.0".to_owned());
    assert_eq!(log.entries(), want);
}

/// A missing tag, a crate from another commit, an unattested crate and a
/// mutable release each fail `verify`.
#[test]
fn verify_refuses_each_broken_link() {
    let pauses = Pauses::default();
    let run = |git: &FakeGit<'_>,
               forge: &FakeForge<'_>,
               registry: &FakeRegistry,
               workspace: &FakeWorkspace<'_>| {
        verify(
            &host!(git, forge, registry, workspace, pauses),
            v("0.3.0"),
            "spate-etl/spate",
        )
    };

    let log = Log::default();
    let (git, forge) = released(&log);
    let (registry, workspace) = published(&log);
    git.remote.borrow_mut().clear();
    let err = run(&git, &forge, &registry, &workspace).unwrap_err();
    assert_eq!(err.message, "v0.3.0 is not tagged in spate-etl/spate");

    let log = Log::default();
    let (git, forge) = released(&log);
    let (registry, workspace) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate")
        .unwrap()
        .last_mut()
        .unwrap()
        .2 = OTHER.into();
    let err = run(&git, &forge, &registry, &workspace).unwrap_err();
    assert!(err.message.contains("not the tagged"), "{}", err.message);

    let log = Log::default();
    let (git, mut forge) = released(&log);
    let (registry, workspace) = published(&log);
    forge.unattested = Some("spate-core-0.3.0.crate".into());
    let err = run(&git, &forge, &registry, &workspace).unwrap_err();
    assert_eq!(err.message, "injected: verify");

    let log = Log::default();
    let (git, forge) = released(&log);
    let (registry, workspace) = published(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Published { immutable: false });
    let err = run(&git, &forge, &registry, &workspace).unwrap_err();
    assert!(
        err.message.contains("not published and immutable"),
        "{}",
        err.message
    );
}

/// `gh release view --json isDraft,isImmutable` maps onto the published
/// states, and a view without `isImmutable` is an error.
#[test]
fn the_release_view_reads_as_a_state() {
    use crate::release::io::release_state;
    assert_eq!(
        release_state(r#"{"isDraft":true,"isImmutable":false}"#).unwrap(),
        ReleaseState::Draft
    );
    assert_eq!(
        release_state(r#"{"isDraft":false,"isImmutable":true}"#).unwrap(),
        ReleaseState::Published { immutable: true }
    );
    assert_eq!(
        release_state(r#"{"isDraft":false,"isImmutable":false}"#).unwrap(),
        ReleaseState::Published { immutable: false }
    );
    assert!(release_state(r#"{"isDraft":false}"#).is_err());
}

/// A release already published mutable stops `finish` before any upload, with
/// the recovery in the message.
#[test]
fn finish_refuses_a_published_mutable_release_before_any_upload() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Published { immutable: false });
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("gh release delete v0.3.0")
            && err.message.contains("without --cleanup-tag"),
        "{}",
        err.message
    );
    assert!(!log.has("upload"), "{:?}", log.entries());
}

/// A release that does not read as published after the publish stops
/// `finish` with its own message, before the docs deploy.
#[test]
fn finish_stops_when_the_release_reads_back_unpublished() {
    for state in [ReleaseState::Draft, ReleaseState::Missing] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let mut forge = FakeForge::new(&log);
        forge.after_publish = Some(state);
        let (registry, workspace) = published(&log);
        let staged = artifacts(true);
        let pauses = Pauses::default();
        let err = finish(
            &host!(&git, &forge, &registry, &workspace, pauses),
            v("0.3.0"),
            SHA,
            staged.dir(),
            "spate-etl/spate",
        )
        .unwrap_err();
        assert!(
            err.message
                .contains(&format!("reads as {state:?} after publishing")),
            "{}",
            err.message
        );
        assert!(!log.has("dispatch docs"));
    }
}

/// A release attestation that does not verify straight after the publish is
/// checked once more after a pause.
#[test]
fn finish_verifies_the_release_again_after_a_pause() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    *forge.verify_release_failures.borrow_mut() = 1;
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert_eq!(pauses.of(Duration::from_secs(10)), 1);
    assert!(log.has("verify release v0.3.0"));
    assert!(log.has("dispatch docs"));
}

/// `verify` runs only from a checkout of the tag it verifies.
#[test]
fn verify_refuses_a_checkout_of_another_commit() {
    let log = Log::default();
    let (mut git, forge) = released(&log);
    git.head = OTHER;
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("this checkout is at 2222222"),
        "{}",
        err.message
    );
    assert!(log.entries().is_empty(), "{:?}", log.entries());
}

/// A downloaded `.crate` whose sha256 differs from the index cksum fails
/// `verify`.
#[test]
fn verify_refuses_a_download_the_index_does_not_list() {
    let log = Log::default();
    let (git, forge) = released(&log);
    let (mut registry, workspace) = published(&log);
    registry.download_differs = true;
    let pauses = Pauses::default();
    let err = verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(err.message.contains("the download is"), "{}", err.message);
}

/// A checksum list an earlier upload left broken is replaced, so a re-run
/// completes the draft.
#[test]
fn finish_replaces_an_asset_an_upload_left_broken() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = Some(ReleaseState::Draft);
    forge.broken.borrow_mut().push(SUMS.to_owned());
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    assert!(
        log.has(&format!("upload v0.3.0 {SUMS} clobber=true")),
        "{:?}",
        log.entries()
    );
    assert!(log.has("publish v0.3.0"));
}

/// A published immutable release missing an asset fails with a message that
/// says the gap is permanent.
#[test]
fn finish_reports_a_gap_in_a_locked_release_as_permanent() {
    let log = Log::default();
    let (git, forge) = released(&log);
    forge.assets.borrow_mut().retain(|a| a != SUMS);
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("lacks SHA256SUMS")
            && err.message.contains("only a new version can carry them"),
        "{}",
        err.message
    );
    assert!(!log.has("upload"), "{:?}", log.entries());
}

/// A create that lands and then reports failure is not repeated: the state is
/// read again first.
#[test]
fn finish_does_not_create_a_release_that_landed() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    *forge.create_lands_then_fails.borrow_mut() = true;
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap();
    let drafts = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("draft "))
        .count();
    assert_eq!(drafts, 1, "{:?}", log.entries());
}

/// `verify` fails on a release missing an SBOM.
#[test]
fn verify_refuses_a_release_missing_an_asset() {
    let log = Log::default();
    let (git, forge) = released(&log);
    forge
        .assets
        .borrow_mut()
        .retain(|a| a != "spate-kafka-0.3.0.cdx.json");
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("lacks spate-kafka-0.3.0.cdx.json"),
        "{}",
        err.message
    );
}

/// A release attestation that still does not verify after the pause stops
/// `finish` before the docs deploy.
#[test]
fn finish_stops_when_the_release_never_verifies() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    *forge.verify_release_failures.borrow_mut() = 2;
    let (registry, workspace) = published(&log);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        staged.dir(),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert_eq!(
        err.message,
        "injected: verify release before the attestation"
    );
    assert_eq!(pauses.of(Duration::from_secs(10)), 1);
    assert!(!log.has("dispatch docs"), "{:?}", log.entries());
}

/// `verify` refuses a checkout with uncommitted changes, whose manifests could
/// leave a published crate unchecked.
#[test]
fn verify_refuses_a_dirty_checkout() {
    let log = Log::default();
    let (mut git, forge) = released(&log);
    git.clean = false;
    let (registry, _) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate-kafka")
        .unwrap()
        .last_mut()
        .unwrap()
        .2 = OTHER.into();
    let workspace = FakeWorkspace::new(&log, &["spate-core", "spate"]);
    let pauses = Pauses::default();
    let err = verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap_err();
    assert!(
        err.message.contains("uncommitted changes"),
        "{}",
        err.message
    );
}

/// `verify_artifacts` checks each attestation against the staged bundle, and
/// `verify` reads each one from the attestation store.
#[test]
fn attestations_come_from_the_bundle_before_publish_and_the_store_after() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let staged = artifacts(true);
    let pauses = Pauses::default();
    verify_artifacts(
        &host!(&git, &forge, &registry, &workspace, pauses),
        staged.dir(),
        SIGNER,
        SHA,
    )
    .unwrap();
    let before = forge.bundled.take();
    assert_eq!(before.len(), 7);
    assert!(before.iter().all(|b| *b), "{before:?}");

    let log = Log::default();
    let (git, forge) = released(&log);
    let (registry, workspace) = published(&log);
    verify(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        "spate-etl/spate",
    )
    .unwrap();
    let after = forge.bundled.take();
    assert_eq!(after.len(), 6);
    assert!(after.iter().all(|b| !*b), "{after:?}");
}

/// A `draft` key hidden behind a merge key or a key that is not a plain
/// string stops `assemble`, since the site build still reads it as a draft.
#[test]
fn a_hidden_draft_key_is_not_assembled() {
    let mut shipped = Vec::new();
    for line in [
        "<<: {draft: true}",
        "<<:\n  draft: true",
        "x: &a {draft: true}\n<<: *a",
        "<<: [{draft: true}]",
        "!!merge <<: {draft: true}",
        "<<: {<<: {draft: true}}",
        "x: &a {draft: true}\n<<: [*a]",
        "? [draft]\n: true",
        "[draft]: true",
        "? - draft\n: true",
        "<<: {\"dr\\x61ft\": true}",
        "[\"dr\\x61ft\"]: true",
    ] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let path = workspace.post("2026-10-11-spate-0-3.md");
        std::fs::write(&path, POST_0_3.replace("draft: true", line)).unwrap();
        let pauses = Pauses::default();
        let result = assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0");
        let still_draft = std::fs::read_to_string(&path).unwrap().contains(line);
        if result.is_ok() && still_draft {
            shipped.push(line);
        }
    }
    assert!(shipped.is_empty(), "shipped still a draft: {shipped:?}");
}
