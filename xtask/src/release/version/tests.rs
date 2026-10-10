use super::*;
use crate::checks::scratch::Scratch;

fn v(s: &str) -> Version {
    Version::parse(s).unwrap()
}

/// The manifest rewriter moves every pin shape the file carries and leaves
/// `rust-version`, the versionless `spate-bench` entry and a third-party crate
/// whose version collides with ours alone. The spaceless pin is legal TOML.
#[test]
fn the_manifest_rewrite_moves_the_version_and_every_pin() {
    let input = r#"[workspace.package]
version = "0.2.0"
rust-version = "1.94"

[workspace.dependencies]
spate-core = { version = "=0.2.0", path = "crates/spate-core" }
# Defaults off keeps async-nats out of a memory-only embedding.
spate-coordination = { version = "=0.2.0", path = "crates/spate-coordination", default-features = false }
spate-s3 = {version = "=0.2.0", path = "crates/spate-s3"}
spate-bench = { path = "bench" }
foldhash = "0.2"
"#;
    let want = r#"[workspace.package]
version = "0.3.0"
rust-version = "1.94"

[workspace.dependencies]
spate-core = { version = "=0.3.0", path = "crates/spate-core" }
# Defaults off keeps async-nats out of a memory-only embedding.
spate-coordination = { version = "=0.3.0", path = "crates/spate-coordination", default-features = false }
spate-s3 = {version = "=0.3.0", path = "crates/spate-s3"}
spate-bench = { path = "bench" }
foldhash = "0.2"
"#;
    assert_eq!(rewrite_manifest(input, v("0.3.0")).unwrap(), want);
}

/// A manifest with two workspace version lines is refused.
#[test]
fn the_manifest_rewrite_refuses_two_version_lines() {
    let input = "version = \"0.2.0\"\nversion = \"0.2.0\"\nspate-x = { version = \"=0.2.0\", path = \"x\" }\n";
    assert!(rewrite_manifest(input, v("0.3.0")).is_err());
}

/// A pin carrying an `=` requirement in a shape the rewriter does not cover
/// aborts the rewrite.
#[test]
fn the_manifest_rewrite_refuses_a_pin_it_cannot_reach() {
    let input = "version = \"0.2.0\"\nspate-core = { path = \"x\", version = \"=0.2.0\" }\n";
    let err = rewrite_manifest(input, v("0.3.0")).unwrap_err();
    assert!(err.message.contains("cannot reach"), "{}", err.message);
}

/// A manifest with no pin has nothing for the release to move.
#[test]
fn the_manifest_rewrite_refuses_a_manifest_without_pins() {
    assert!(rewrite_manifest("version = \"0.2.0\"\n", v("0.3.0")).is_err());
}

/// The snippet rewriter moves the real snippet shapes and leaves a non-spate
/// dependency in the same fence and a trailing comment byte-identical.
#[test]
fn the_snippet_rewrite_moves_only_the_version_string() {
    let input = r#"spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }
serde = { version = "1", features = ["derive"] }
spate-test = "0.2"
spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka"
"#;
    let want = r#"spate = { version = "0.3", features = ["kafka", "clickhouse", "avro"] }
serde = { version = "1", features = ["derive"] }
spate-test = "0.3"
spate = { version = "0.3", features = ["kafka-tls"] }   # implies "kafka"
"#;
    assert_eq!(rewrite_snippets(input, "0.3").as_deref(), Some(want));
}

/// A file with no snippet answers `None`, so `bump` can name the file.
#[test]
fn the_snippet_rewrite_reports_a_file_without_a_snippet() {
    assert_eq!(rewrite_snippets("no snippet here\n", "0.3"), None);
}

/// Which lines the scan treats as snippets, and which of those the rewriters
/// reach. A snippet wrapped onto several lines is caught, and `myspate` does
/// not match at all.
#[test]
fn the_scan_classifies_snippets_by_reach() {
    #[derive(Debug, PartialEq)]
    enum Verdict {
        Rewritable,
        Wide,
        Clean,
    }
    use Verdict::*;
    let table = [
        // The real snippets.
        (
            r#"spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }"#,
            Rewritable,
        ),
        (r#"spate-test = "0.2""#, Rewritable),
        (
            r#"spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka""#,
            Rewritable,
        ),
        (r#"spate-object-store = "0.2""#, Rewritable),
        (r#"  spate = "0.2""#, Rewritable),
        // Snippets the scan reports and the rewriters cannot reach.
        (r#"Add `spate = "0.2"` to your manifest."#, Wide),
        (r#"spate = "0.2.0""#, Wide),
        (r#"spate-test = { version = "0.2.0" }"#, Wide),
        ("spate = {", Wide),
        (r#"spate-kafka = { features = ["tls"],"#, Wide),
        // Not snippets at all.
        (r#"foldhash = "0.2""#, Clean),
        (r#"myspate = "0.2""#, Clean),
        (r#"my-spate = "0.2""#, Clean),
        (r#"serde = { version = "1", features = ["derive"] }"#, Clean),
        (
            r#"spate-core = { version = "=0.2.0", path = "crates/spate-core" }"#,
            Clean,
        ),
        (
            "the default for retry.jitter is 0.2, a plain number in prose",
            Clean,
        ),
        ("release: v0.2.0", Clean),
        (r#"spate = { features = ["x"] } version = "0.2""#, Clean),
    ];
    for (line, want) in table {
        let got = if !looks_like_snippet(line) {
            Clean
        } else if is_rewritable(line) {
            Rewritable
        } else {
            Wide
        };
        assert_eq!(got, want, "{line}");
    }
}

/// A version in a trailing comment does not change the version a snippet
/// carries.
#[test]
fn the_snippet_version_is_read_from_the_construct() {
    assert_eq!(
        snippet_version(
            r#"spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka", added in "0.3""#
        ),
        Some("0.2")
    );
    assert_eq!(snippet_version(r#"spate-test = "0.2""#), Some("0.2"));
}

/// Version arithmetic and ordering, including the `X.Y` form MSRV uses.
#[test]
fn version_arithmetic_and_ordering() {
    assert_eq!(v("0.2.0").next(Bump::Minor), v("0.3.0"));
    assert_eq!(v("0.2.9").next(Bump::Patch), v("0.2.10"));
    assert_eq!(v("0.10.3").minor_of(), "0.10");
    assert!(v("0.2.0") < v("0.10.0"), "a string compare?");
    assert!(version_lt("0.2.0", "0.10.0").unwrap());
    assert!(version_lt("1.94", "1.95").unwrap());
    assert!(!version_lt("1.94", "1.94").unwrap());
    assert!(!version_lt("0.3.0", "0.3.0").unwrap());
    assert!(version_lt("1.x", "1.95").is_err());
    assert_eq!(Version::parse("0.2"), None);
    assert_eq!(Version::parse("0.2.0.1"), None);
    assert_eq!(Version::parse("0.2.+1"), None);
    assert_eq!(Version::parse("v0.2.0"), None);
    assert_eq!(Version::parse("0.03.0"), None);
    assert_eq!(Version::parse("0.10.0"), Some(v("0.10.0")));
}

/// The workspace version is the one line opening `version = "`.
#[test]
fn the_workspace_version_is_the_single_version_line() {
    assert_eq!(
        workspace_version("[workspace.package]\nversion = \"0.2.0\"\n").unwrap(),
        v("0.2.0")
    );
    assert!(workspace_version("version = \"0.2.0\"\nversion = \"0.2.0\"\n").is_err());
    assert!(workspace_version("[package]\n").is_err());
    assert!(workspace_version("version = \"0.2\"\n").is_err());
}

/// A pin off the workspace version is a problem; a versionless one is not.
#[test]
fn a_pin_off_the_workspace_version_is_reported() {
    let manifest = "spate-core = { version = \"=0.2.0\", path = \"a\" }\n\
                    spate-s3 = { version = \"=0.1.0\", path = \"b\" }\n\
                    spate-bench = { path = \"bench\" }\n";
    let problems = pin_problems(manifest, v("0.2.0")).unwrap();
    assert_eq!(problems.len(), 1);
    assert!(problems[0].contains("spate-s3"), "{problems:?}");
    assert!(pin_problems("foldhash = \"0.2\"\n", v("0.2.0")).is_err());
}

/// A snippet outside the rewritten set, one the rewriters cannot reach and one
/// at the wrong version are each reported with their location.
#[test]
fn the_snippet_problems_name_file_and_line() {
    let stray = snippet_problems("docs/elsewhere.md", "x\nspate = \"0.2\"\n", "0.2");
    assert_eq!(stray.len(), 1);
    assert!(stray[0].starts_with("docs/elsewhere.md:2 "), "{stray:?}");

    let text = "spate = \"0.2.0\"\nspate-test = \"0.1\"\nspate = \"0.2\"\n";
    let known = snippet_problems("README.md", text, "0.2");
    assert_eq!(known.len(), 2, "{known:?}");
    assert!(known[0].contains("cannot reach"), "{known:?}");
    assert!(known[1].contains("carries 0.1"), "{known:?}");
}

/// The changelog and the attribution are generated, and the decision records
/// are immutable, so the scan leaves them out.
#[test]
fn generated_and_historical_files_are_left_out_of_the_scan() {
    for path in [
        "CHANGELOG.md",
        "THIRD-PARTY.md",
        "changelog.d/x.fixed.md",
        "docs/adr/0001-x.md",
    ] {
        assert!(scan_excluded(path), "{path}");
    }
    assert!(!scan_excluded("README.md"));
    assert!(!scan_excluded("docs/user-guide/x.mdx"));
}

/// The upload rejects a publishable crate with no description or license; a
/// crate marked `publish = []` is never uploaded.
#[test]
fn missing_metadata_names_only_publishable_crates() {
    let metadata = r#"{"packages": [
        {"name": "a", "publish": null, "description": "A", "license": "MIT"},
        {"name": "b", "publish": null, "description": null, "license": "MIT"},
        {"name": "c", "publish": ["crates-io"], "description": "C", "license": ""},
        {"name": "d", "publish": [], "description": null, "license": null}
    ]}"#;
    assert_eq!(missing_metadata(metadata).unwrap(), vec!["b", "c"]);
}

/// A working tree of this test's own.
fn tree(name: &str) -> Scratch {
    Scratch::new(&format!("spate-xtask-release-version-{name}")).unwrap()
}

fn write(root: &Path, rel: &str, body: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

const MANIFEST_AT_0_2_0: &str = "[workspace.package]\nversion = \"0.2.0\"\nrust-version = \"1.94\"\n\n\
     [workspace.dependencies]\nspate-core = { version = \"=0.2.0\", path = \"crates/spate-core\" }\n";

/// A snippet file the rewriter finds nothing in refuses the bump before any
/// file is written.
#[test]
fn a_bump_that_cannot_rewrite_every_file_changes_nothing() {
    let dir = tree("a_bump_that_cannot_rewrite_every_file_changes_nothing");
    let root = dir.dir();
    write(root, MANIFEST, MANIFEST_AT_0_2_0);
    for path in SNIPPET_FILES {
        write(root, path, "spate = \"0.2\"\n");
    }
    write(root, SNIPPET_FILES[2], "the snippet was removed\n");

    let err = bump(root, false, "0.3.0").unwrap_err();
    assert!(err.message.contains(SNIPPET_FILES[2]), "{}", err.message);
    assert_eq!(read(root, MANIFEST).unwrap(), MANIFEST_AT_0_2_0);
    assert_eq!(read(root, SNIPPET_FILES[0]).unwrap(), "spate = \"0.2\"\n");
}

/// A write that fails partway leaves every target as it was and no sibling
/// behind.
#[test]
fn a_failed_rewrite_writes_nothing() {
    let dir = tree("a_failed_rewrite_writes_nothing");
    let root = dir.dir();
    write(root, "a.md", "old\n");
    let files = [
        ("a.md", "new\n".to_owned()),
        ("missing.md", "new\n".to_owned()),
    ];
    assert!(replace_all(root, &files).is_err());
    assert_eq!(read(root, "a.md").unwrap(), "old\n");
    let left: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(left, ["a.md"]);
}

/// A bump to the current version, or behind it, is refused.
#[test]
fn a_bump_must_move_forward() {
    let dir = tree("a_bump_must_move_forward");
    let root = dir.dir();
    write(root, MANIFEST, MANIFEST_AT_0_2_0);
    assert!(
        bump(root, false, "0.2.0")
            .unwrap_err()
            .message
            .contains("already at")
    );
    assert!(
        bump(root, false, "0.1.9")
            .unwrap_err()
            .message
            .contains("behind")
    );
    assert!(
        bump(root, false, "0.3")
            .unwrap_err()
            .message
            .contains("not X.Y.Z")
    );
}

/// A throwaway repository at a tagged release, removed with its contents.
struct Repo(Scratch);

impl Repo {
    fn new(name: &str) -> Self {
        let repo = Self(tree(name));
        repo.git(&["init", "--quiet", "-b", "main", "."]);
        repo.write(MANIFEST, MANIFEST_AT_0_2_0);
        repo.write("changelog.d/README.md", "the conventions\n");
        repo.commit("release: v0.2.0");
        repo.git(&["tag", "-a", "v0.2.0", "-m", "v0.2.0"]);
        repo
    }

    fn path(&self) -> &Path {
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
                "-c",
                "tag.gpgsign=false",
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
        write(self.path(), rel, body);
    }

    fn commit(&self, message: &str) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "--quiet", "--allow-empty", "-m", message]);
    }
}

/// With no break and no MSRV raise since the tag, the next version is a patch.
#[test]
fn derive_answers_a_patch_by_default() {
    let repo = Repo::new("derive_answers_a_patch_by_default");
    repo.write("changelog.d/a.fixed.md", "A fix.\n");
    repo.commit("core: a fix");
    assert_eq!(derive(repo.path()).unwrap().0, v("0.2.1"));
}

/// A fragment opening with `**Breaking:**` makes the next version a minor.
#[test]
fn derive_answers_a_minor_for_a_breaking_fragment() {
    let repo = Repo::new("derive_answers_a_minor_for_a_breaking_fragment");
    repo.write("changelog.d/a.changed.md", "**Breaking:** a change.\n");
    repo.commit("core: a break");
    let (next, reason) = derive(repo.path()).unwrap();
    assert_eq!(next, v("0.3.0"));
    assert!(reason.contains("Breaking"), "{reason}");
}

/// A raised `rust-version` makes the next version a minor; a lowered one does
/// not.
#[test]
fn derive_answers_a_minor_for_a_raised_rust_version() {
    let repo = Repo::new("derive_answers_a_minor_for_a_raised_rust_version");
    repo.write(MANIFEST, &MANIFEST_AT_0_2_0.replace("1.94", "1.93"));
    repo.commit("workspace: lower the msrv");
    assert_eq!(derive(repo.path()).unwrap().0, v("0.2.1"));

    repo.write(MANIFEST, &MANIFEST_AT_0_2_0.replace("1.94", "1.95"));
    repo.commit("workspace: raise the msrv");
    let (next, reason) = derive(repo.path()).unwrap();
    assert_eq!(next, v("0.3.0"));
    assert!(reason.contains("1.94 to 1.95"), "{reason}");
}

/// A manifest ahead of the last tag is a release still in flight.
#[test]
fn derive_refuses_a_half_finished_release() {
    let repo = Repo::new("derive_refuses_a_half_finished_release");
    repo.write(MANIFEST, &MANIFEST_AT_0_2_0.replace("0.2.0", "0.2.1"));
    repo.commit("release: v0.2.1");
    let err = derive(repo.path()).unwrap_err();
    assert!(err.message.contains("half-finished"), "{}", err.message);
}

/// Nothing since the tag is nothing to release.
#[test]
fn derive_refuses_an_empty_range() {
    let repo = Repo::new("derive_refuses_an_empty_range");
    let err = derive(repo.path()).unwrap_err();
    assert!(
        err.message.contains("no commits since v0.2.0"),
        "{}",
        err.message
    );
}

/// A repository with no release tag has no history to derive from.
#[test]
fn derive_refuses_a_repository_without_a_tag() {
    let repo = Repo::new("derive_refuses_a_repository_without_a_tag");
    repo.git(&["tag", "-d", "v0.2.0"]);
    repo.git(&["tag", "not-a-release"]);
    let err = derive(repo.path()).unwrap_err();
    assert!(err.message.contains("no vX.Y.Z tag"), "{}", err.message);
}

/// A tree whose literals agree passes `check`; a snippet outside the rewritten
/// set, or a rewritten file that lost its snippet, fails it.
#[test]
fn check_reads_the_tracked_tree() {
    let repo = Repo::new("check_reads_the_tracked_tree");
    for path in SNIPPET_FILES {
        repo.write(path, "spate = \"0.2\"\n");
    }
    repo.commit("docs: the snippets");
    assert!(check(repo.path()).is_ok());

    repo.write("docs/stray.md", "spate-test = \"0.2\"\n");
    repo.git(&["add", "docs/stray.md"]);
    let err = check(repo.path()).unwrap_err();
    assert!(
        err.message.starts_with("1 version literal"),
        "{}",
        err.message
    );

    repo.git(&["rm", "--quiet", "--cached", "docs/stray.md"]);
    repo.write(SNIPPET_FILES[0], "no snippet\n");
    let err = check(repo.path()).unwrap_err();
    assert!(
        err.message.starts_with("1 version literal"),
        "{}",
        err.message
    );
}

/// A comment after the `rust-version` value does not change the value read.
#[test]
fn derive_reads_a_raised_rust_version_behind_a_quoted_comment() {
    let repo = Repo::new("derive_reads_a_raised_rust_version_behind_a_quoted_comment");
    repo.write(
        MANIFEST,
        &MANIFEST_AT_0_2_0.replace(
            "rust-version = \"1.94\"",
            "rust-version = \"1.95\" # see \"MSRV policy\"",
        ),
    );
    repo.commit("workspace: raise the msrv");
    let got = derive(repo.path());
    assert_eq!(got.map(|(v, _)| v).map_err(|e| e.message), Ok(v("0.3.0")));
}

/// A CRLF manifest is read and rewritten with its terminators kept.
#[test]
fn a_crlf_manifest_is_accepted() {
    let input = "[workspace.package]\r\nversion = \"0.2.0\"\r\n\r\n[workspace.dependencies]\r\nspate-core = { version = \"=0.2.0\", path = \"crates/spate-core\" }\r\n";
    assert_eq!(workspace_version(input).unwrap(), v("0.2.0"));
    assert_eq!(
        rewrite_manifest(input, v("0.3.0")).unwrap(),
        input.replace("0.2.0", "0.3.0")
    );
    assert!(pin_problems(input, v("0.2.0")).unwrap().is_empty());
    let snippets = "spate = \"0.2\"\r\nspate = { version = \"0.2\" }\r\n";
    assert_eq!(
        rewrite_snippets(snippets, "0.3").unwrap(),
        snippets.replace("0.2", "0.3")
    );
    assert!(snippet_problems("README.md", snippets, "0.2").is_empty());
}

/// A file with no trailing newline keeps that shape through both rewriters.
#[test]
fn a_missing_final_newline_is_kept() {
    let manifest = "version = \"0.2.0\"\nspate-core = { version = \"=0.2.0\", path = \"x\" }";
    assert_eq!(
        rewrite_manifest(manifest, v("0.3.0")).unwrap(),
        manifest.replace("0.2.0", "0.3.0")
    );
    assert_eq!(
        rewrite_snippets("spate = \"0.2\"", "0.3").unwrap(),
        "spate = \"0.3\""
    );
}

/// A `rust-version` that is not `X.Y` or `X.Y.Z` stops the derivation.
#[test]
fn derive_refuses_a_malformed_rust_version() {
    let repo = Repo::new("derive_refuses_a_malformed_rust_version");
    repo.write(MANIFEST, &MANIFEST_AT_0_2_0.replace("1.94", "1.x"));
    repo.commit("workspace: a typo in the msrv");
    let err = derive(repo.path()).unwrap_err();
    assert!(
        err.message.contains("'1.x' is not X.Y or X.Y.Z"),
        "{}",
        err.message
    );
}

/// A replaced file keeps its mode.
#[cfg(unix)]
#[test]
fn a_rewrite_keeps_the_mode() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tree("a_rewrite_keeps_the_mode");
    let root = dir.dir();
    write(root, "a.md", "old\n");
    std::fs::set_permissions(root.join("a.md"), std::fs::Permissions::from_mode(0o640)).unwrap();
    replace_all(root, &[("a.md", "new\n".to_owned())]).unwrap();
    assert_eq!(read(root, "a.md").unwrap(), "new\n");
    let mode = std::fs::metadata(root.join("a.md"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o640);
}

const WORKSPACE_AT_0_2_0: &str = "[workspace]\nmembers = [\"crates/spate-core\"]\nresolver = \"2\"\n\n\
    [workspace.package]\nversion = \"0.2.0\"\nrust-version = \"1.94\"\n\n\
    [workspace.dependencies]\nspate-core = { version = \"=0.2.0\", path = \"crates/spate-core\" }\n";

/// A repository holding a one-member workspace at 0.2.0.
fn workspace_repo(name: &str) -> Repo {
    let repo = Repo::new(name);
    repo.write(MANIFEST, WORKSPACE_AT_0_2_0);
    repo.write(
        "crates/spate-core/Cargo.toml",
        "[package]\nname = \"spate-core\"\nversion.workspace = true\nedition = \"2021\"\n",
    );
    repo.write("crates/spate-core/src/lib.rs", "");
    repo
}

/// A bump rewrites the manifest, the pins and every snippet file, and
/// refreshes the lockfile.
#[test]
fn a_bump_rewrites_every_literal() {
    let repo = workspace_repo("a_bump_rewrites_every_literal");
    for path in SNIPPET_FILES {
        repo.write(
            path,
            "spate = { version = \"0.2\", features = [\"kafka\"] }\n",
        );
    }
    repo.commit("docs: the snippets");
    bump(repo.path(), false, "0.3.0").unwrap();
    assert_eq!(
        read(repo.path(), MANIFEST).unwrap(),
        WORKSPACE_AT_0_2_0.replace("0.2.0", "0.3.0")
    );
    for path in SNIPPET_FILES {
        assert_eq!(
            read(repo.path(), path).unwrap(),
            "spate = { version = \"0.3\", features = [\"kafka\"] }\n",
            "{path}"
        );
    }
    let lock = read(repo.path(), "Cargo.lock").unwrap();
    assert!(
        lock.contains("name = \"spate-core\"\nversion = \"0.3.0\""),
        "{lock}"
    );
}

/// A bump whose result fails `check` is reported as a failure.
#[test]
fn a_bump_runs_check_after_the_rewrite() {
    let repo = workspace_repo("a_bump_runs_check_after_the_rewrite");
    for path in SNIPPET_FILES {
        repo.write(path, "spate = \"0.2\"\n");
    }
    repo.write("docs/stray.md", "spate-test = \"0.2\"\n");
    repo.commit("docs: the snippets and a stray");
    let err = bump(repo.path(), false, "0.3.0").unwrap_err();
    assert!(err.message.contains("disagree"), "{}", err.message);
}
