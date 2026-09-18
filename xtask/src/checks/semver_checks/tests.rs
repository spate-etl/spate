use super::*;

// ── The sparse-index path ──────────────────────────────────────────────

#[test]
fn the_index_path_keys_on_the_name_length() {
    assert_eq!(index_path("spate"), "sp/at/spate");
    assert_eq!(index_path("spate-core"), "sp/at/spate-core");
    assert_eq!(index_path("spate-clickhouse"), "sp/at/spate-clickhouse");
    assert_eq!(index_path("abcd"), "ab/cd/abcd");
    assert_eq!(index_path("abc"), "3/a/abc");
    assert_eq!(index_path("ab"), "2/ab");
    assert_eq!(index_path("a"), "1/a");
}

// ── The tool's verdict ─────────────────────────────────────────────────

/// 0 is clean and 100 is a break. Every other code is the tool failing to
/// complete, so a gate that cannot evaluate never passes.
#[test]
fn only_zero_is_clean_and_only_one_hundred_is_breaking() {
    assert_eq!(classify_exit(0), Verdict::Clean);
    assert_eq!(classify_exit(100), Verdict::Breaking);
    for code in [1, 2, 99, 101, 137, -1, i32::MAX] {
        assert_eq!(classify_exit(code), Verdict::Error, "{code}");
    }
}

// ── The conventional breaking marker ───────────────────────────────────

#[test]
fn the_marker_is_a_type_an_optional_scope_and_a_bang() {
    for subject in [
        "feat(spate-core)!: seal the framework configuration sections",
        "refactor!: rename the framework",
        "docs(workspace)!: migrate one file to another",
        "feat()!: an empty scope",
    ] {
        assert!(subject_is_breaking(subject), "{subject}");
    }
    for subject in [
        "feat(spate-core): a windowed operator",
        "chore: release v0.2.0",
        "revert(spate-core): back out the windowed operator!",
        "",
        "!: no type",
        "feat(unterminated!: a scope with no close",
        "feat(!: a scope opened and never closed",
        "féat!: a non-ascii letter in the type",
        "feat(a)b!: a scope followed by more type",
        " feat!: a leading space",
        "feat1!: a digit in the type",
        "feat! no colon",
        "feat(spate-core)! no colon",
        "feat!",
    ] {
        assert!(!subject_is_breaking(subject), "{subject}");
    }
}

/// A scope may hold anything but a close paren, including a newline, since the
/// title is matched whole.
#[test]
fn a_scope_holds_anything_but_a_close_paren() {
    assert!(subject_is_breaking(
        "feat(a b, c-d/e)!: spaces and punctuation"
    ));
    assert!(subject_is_breaking(
        "feat(a\nb)!: a newline inside the scope"
    ));
    assert!(!subject_is_breaking("feat(a)(b)!: two scopes"));
}

/// The log scan anchors per line, so a marker on any line of any commit body
/// announces the break.
#[test]
fn the_log_scan_anchors_each_line() {
    assert!(log_has_marker("chore: a subject\n\nfeat!: a body line\n"));
    assert!(log_has_marker("feat!: the first line"));
    assert!(!log_has_marker(
        "chore: a subject\n\n  feat!: an indented line\n"
    ));
    assert!(!log_has_marker(""));
    assert!(!log_has_marker("a feat!: mid-line"));
}

// ── The baseline a crate is compared against ───────────────────────────

/// One index entry, as the sparse index spells it.
fn entry(vers: &str, yanked: bool) -> String {
    format!(r#"{{"name":"spate","vers":"{vers}","yanked":{yanked}}}"#)
}

/// The index is ordered by publish time, so the baseline is the last live
/// entry. A higher version published earlier does not win.
#[test]
fn the_baseline_is_the_last_live_entry_in_publish_order() {
    let body = format!(
        "{}\n{}\n{}\n",
        entry("0.1.0", false),
        entry("0.9.0", false),
        entry("0.2.0", false)
    );
    assert_eq!(baseline_version(&body).unwrap().as_deref(), Some("0.2.0"));
}

#[test]
fn a_yanked_entry_is_not_a_baseline() {
    let body = format!(
        "{}\n{}\n{}\n",
        entry("0.1.0", false),
        entry("0.2.0", false),
        entry("0.3.0", true)
    );
    assert_eq!(baseline_version(&body).unwrap().as_deref(), Some("0.2.0"));
}

#[test]
fn a_crate_whose_every_version_is_yanked_has_no_baseline() {
    let body = format!("{}\n{}\n", entry("0.1.0", true), entry("0.2.0", true));
    assert_eq!(baseline_version(&body).unwrap(), None);
    assert_eq!(baseline_version("").unwrap(), None);
    assert_eq!(baseline_version("\n\n  \n").unwrap(), None);
}

/// `false` and `null` leave a version live; anything else withdraws it.
#[test]
fn only_false_and_null_leave_a_version_live() {
    let live = [
        r#"{"vers":"1.0.0","yanked":false}"#,
        r#"{"vers":"1.0.0","yanked":null}"#,
        r#"{"vers":"1.0.0"}"#,
    ];
    for body in live {
        assert_eq!(
            baseline_version(body).unwrap().as_deref(),
            Some("1.0.0"),
            "{body}"
        );
    }
    let withdrawn = [
        r#"{"vers":"1.0.0","yanked":true}"#,
        r#"{"vers":"1.0.0","yanked":0}"#,
        r#"{"vers":"1.0.0","yanked":""}"#,
        r#"{"vers":"1.0.0","yanked":[]}"#,
    ];
    for body in withdrawn {
        assert_eq!(baseline_version(body).unwrap(), None, "{body}");
    }
}

/// A live entry naming no version, or an empty one, leaves nothing to diff
/// against.
#[test]
fn an_entry_with_no_usable_version_names_no_baseline() {
    assert_eq!(
        baseline_version(r#"{"name":"spate","yanked":false}"#)
            .unwrap()
            .as_deref(),
        Some("null")
    );
    assert_eq!(baseline_version(&entry("", false)).unwrap(), None);
}

/// A `vers` that is not a string carries through in its JSON spelling, so a
/// malformed entry never silently reads as no baseline.
#[test]
fn a_version_field_that_is_not_a_string_keeps_its_json_spelling() {
    assert_eq!(
        baseline_version(r#"{"vers":1,"yanked":false}"#)
            .unwrap()
            .as_deref(),
        Some("1")
    );
    assert_eq!(
        baseline_version(r#"{"vers":["1.0.0"],"yanked":false}"#)
            .unwrap()
            .as_deref(),
        Some(r#"["1.0.0"]"#)
    );
}

/// The last entry decides, so an empty version after a live one drops the
/// baseline the earlier entry named.
#[test]
fn the_last_live_entry_decides_even_when_it_names_nothing() {
    let body = format!("{}\n{}\n", entry("0.1.0", false), entry("", false));
    assert_eq!(baseline_version(&body).unwrap(), None);
}

#[test]
fn a_body_that_does_not_parse_is_an_error() {
    assert!(baseline_version(r#"{"vers":"1.0.0""#).is_err());
    assert!(baseline_version("not json").is_err());
}

/// An index body that parses as JSON but lists something other than objects is
/// a parse failure, so a reply the gate cannot read fails closed.
#[test]
fn an_entry_that_is_not_an_object_is_an_error() {
    for body in [r#""hello""#, "42", "true", "[]", "null"] {
        assert!(baseline_version(body).is_err(), "{body}");
    }
}

// ── The curl reply ─────────────────────────────────────────────────────

#[test]
fn the_status_code_is_the_tail_of_the_reply() {
    assert_eq!(
        split_reply("{\"vers\":\"1\"}\n200"),
        ("{\"vers\":\"1\"}", "200")
    );
    assert_eq!(split_reply("\n404"), ("", "404"));
    assert_eq!(split_reply("a\nb\n500"), ("a\nb", "500"));
}

/// A command substitution drops trailing newlines before the split, so a body
/// ending in one still yields its own code.
#[test]
fn trailing_newlines_are_dropped_before_the_split() {
    assert_eq!(split_reply("body\n200\n\n"), ("body", "200"));
}

/// A reply carrying no newline at all leaves the whole string in both halves,
/// which no status code matches.
#[test]
fn a_reply_with_no_newline_is_neither_a_body_nor_a_code() {
    assert_eq!(split_reply("mangled"), ("mangled", "mangled"));
}

// ── The workspace version ──────────────────────────────────────────────

#[test]
fn the_workspace_version_is_the_manifests_own_version_key() {
    assert_eq!(
        workspace_version("[workspace.package]\nversion = \"0.2.0\"\nedition = \"2024\"\n"),
        "0.2.0"
    );
    assert_eq!(workspace_version("  version = \"0.2.0\"\n"), "");
    assert_eq!(workspace_version("version = \"0.2.0\" # a comment\n"), "");
    assert_eq!(
        workspace_version("spate-core = { version = \"=0.2.0\" }\n"),
        ""
    );
    assert_eq!(workspace_version(""), "");
}

/// Several matching lines join, so a manifest carrying two never compares
/// equal to a published version.
#[test]
fn two_version_lines_join_on_a_newline() {
    assert_eq!(
        workspace_version("version = \"0.1.0\"\nversion = \"0.2.0\"\n"),
        "0.1.0\n0.2.0"
    );
}

/// The manifest this workspace ships parses to the version its crates declare.
#[test]
fn the_shipped_manifest_names_one_version() {
    let root = crate::repo_root().unwrap();
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let version = workspace_version(&manifest);
    assert!(!version.is_empty());
    assert!(!version.contains('\n'), "{version}");
}

// ── The --packages validator ───────────────────────────────────────────

#[test]
fn the_package_list_names_crate_directories() {
    let root = crate::repo_root().unwrap();
    for list in ["spate", "spate spate-kafka", " spate  spate-s3 "] {
        assert!(packages_valid(&root, list), "{list}");
    }
    for list in [
        "",
        " ",
        "   ",
        "spate-*",
        "../etc",
        "crates/spate",
        "spate;rm -rf /",
        "spate nonexistent-crate",
        "spate .",
        "spate $(id)",
    ] {
        assert!(!packages_valid(&root, list), "{list}");
    }
}

/// An empty selection is an error, so a workflow expression resolving to
/// nothing fails the gate.
#[test]
fn an_empty_package_list_is_rejected() {
    let root = crate::repo_root().unwrap();
    assert!(!packages_valid(&root, ""));
    let message = registry(&root, true, Some("")).unwrap_err().message;
    assert!(
        message.starts_with("--packages needs a space-separated list"),
        "{message}"
    );
    assert!(message.contains("and got ''."), "{message}");
}

/// A field split takes the default separators and nothing else, so a name is
/// never split on a character the validator would have rejected.
#[test]
fn the_field_split_takes_space_tab_and_newline() {
    assert_eq!(
        fields("a b\tc\nd").collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(fields("  a   b  ").collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(fields("").count(), 0);
    assert_eq!(fields("a\u{a0}b").collect::<Vec<_>>(), ["a\u{a0}b"]);
}

// ── The baseline cache key ─────────────────────────────────────────────

/// The key is the platform, the tag, and the two versions a cached rustdoc is
/// only readable under.
#[test]
fn the_cache_key_carries_the_platform_tag_and_both_versions() {
    assert_eq!(
        cache_key(
            "Linux",
            "v0.2.0",
            "rustc 1.96.0 (1159e78c4 2026-09-14)\n",
            "cargo-semver-checks 0.45.0\n"
        ),
        "semver-baseline-Linux-v0.2.0-rustc-1.96.0--1159e78c4-2026-09-14--cargo-semver-checks-0.45.0"
    );
}

/// Every byte outside the class becomes a hyphen and exactly one trailing
/// hyphen is dropped, so the trailing newline of a version line leaves no mark
/// and the character before it does.
#[test]
fn the_key_fields_keep_only_alphanumerics_and_dots() {
    assert_eq!(sanitize("rustc 1.96.0\n"), "rustc-1.96.0");
    assert_eq!(sanitize("a)\n"), "a-");
    assert_eq!(sanitize(""), "");
    assert_eq!(sanitize("-"), "");
    assert_eq!(sanitize("--"), "-");
    assert_eq!(sanitize("a_b"), "a-b");
    assert_eq!(sanitize("a.b"), "a.b");
}

/// A multi-byte character becomes one hyphen per byte, as a byte-wise
/// translation produces.
#[test]
fn a_multibyte_character_becomes_one_hyphen_per_byte() {
    assert_eq!(sanitize("é"), "-");
    assert_eq!(sanitize("éa"), "--a");
}

// ── The cargo invocation ───────────────────────────────────────────────

/// Every package of a group rides one invocation, and the pinned group carries
/// the release type after them.
#[test]
fn a_group_is_one_invocation_naming_every_package() {
    let pkgs = ["spate".to_owned(), "spate-s3".to_owned()];
    assert_eq!(
        group_step(None, &pkgs).display(),
        "cargo semver-checks --package spate --package spate-s3"
    );
    assert_eq!(
        group_step(Some("minor"), &pkgs).display(),
        "cargo semver-checks --package spate --package spate-s3 --release-type minor"
    );
}

#[test]
fn an_empty_group_runs_nothing() {
    let root = crate::repo_root().unwrap();
    let mut checked = 7usize;
    let mut breaking = vec!["kept".to_owned()];
    check_group(&root, None, &[], &mut checked, &mut breaking).unwrap();
    assert_eq!(checked, 7);
    assert_eq!(breaking, ["kept"]);
}

// ── Discovery ──────────────────────────────────────────────────────────

/// Discovery is the directories under `crates/`, sorted, so a new crate is
/// covered on arrival.
#[test]
fn the_crates_are_the_sorted_directories_under_crates() {
    let root = crate::repo_root().unwrap();
    let found = crates_now(&root);
    assert!(found.contains(&"spate-core".to_owned()), "{found:?}");
    let mut sorted = found.clone();
    sorted.sort();
    assert_eq!(found, sorted);
    assert!(found.iter().all(|n| !n.starts_with('.')), "{found:?}");
    assert!(found.iter().all(|n| packages_valid(&root, n)), "{found:?}");
}

/// Discovery takes directories and nothing else, so a file or a dotted
/// directory beside the crates never reaches the index.
#[test]
fn discovery_takes_directories_that_are_not_dotted() {
    let scratch = crate::checks::scratch::Scratch::new("spate-xtask-semver-crates").unwrap();
    let crates = scratch.join("crates");
    for name in ["beta", "alpha", ".hidden"] {
        std::fs::create_dir_all(crates.join(name)).unwrap();
    }
    std::fs::write(crates.join("README.md"), "").unwrap();
    assert_eq!(crates_now(scratch.dir()), ["alpha", "beta"]);
}

/// A tree with no `crates/` yields no crate, so nothing probes the index for a
/// name a glob left behind.
#[test]
fn a_tree_with_no_crates_directory_yields_none() {
    assert!(crates_now(Path::new("/nonexistent-tree-for-a-test")).is_empty());
}
