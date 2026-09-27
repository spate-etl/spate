use super::*;

/// A root manifest holding `entries` under `[workspace.dependencies]`.
fn root(entries: &str) -> String {
    format!("[workspace]\nmembers = []\n\n[workspace.dependencies]\n{entries}")
}

/// A crate manifest named `name` with `rest` after its `[package]` table.
fn krate(name: &str, rest: &str) -> String {
    format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n\n{rest}")
}

/// The snapshot one root manifest and its crate manifests make.
fn snapshot(root: &str, crates: &[String]) -> Snapshot {
    Snapshot {
        table: table(root).unwrap(),
        declared: declared(crates).unwrap(),
    }
}

fn spec(version: &str) -> Spec {
    Spec {
        package: None,
        version: version.to_owned(),
        default_features: true,
        features: BTreeSet::new(),
    }
}

fn names(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|name| (*name).to_owned()).collect()
}

/// The string, inline table, multi-line inline table and subtable forms parse
/// to the same shape, and a `package` equal to its key reads as no rename.
#[test]
fn the_table_reads_every_toml_form() {
    let table = table(&root(
        "\
plain = \"1.2\"
inline = { version = \"0.3\", default-features = false, features = [\"b\", \"a\"] }
wrapped = { version = \"4.5\", features = [
    \"std\",
    \"help\",
] }
renamed = { package = \"other\", version = \"0.10\" }
same = { package = \"same\", version = \"2\" }
underscored = { version = \"1\", default_features = false }

[workspace.dependencies.sub]
version = \"0.9\"
features = [\"x\"]
",
    ))
    .unwrap();

    let mut inline = spec("0.3");
    inline.default_features = false;
    inline.features = names(&["a", "b"]);
    let mut wrapped = spec("4.5");
    wrapped.features = names(&["help", "std"]);
    let mut renamed = spec("0.10");
    renamed.package = Some("other".to_owned());
    let mut underscored = spec("1");
    underscored.default_features = false;
    let mut sub = spec("0.9");
    sub.features = names(&["x"]);

    let expected: BTreeMap<String, Spec> = [
        ("plain", spec("1.2")),
        ("inline", inline),
        ("wrapped", wrapped),
        ("renamed", renamed),
        ("same", spec("2")),
        ("underscored", underscored),
        ("sub", sub),
    ]
    .into_iter()
    .map(|(key, spec)| (key.to_owned(), spec))
    .collect();
    assert_eq!(table, expected);
}

/// A first-party path pin and a versionless path entry are not requirements,
/// and a third-party entry carrying `git` or a `path` elsewhere keeps its
/// `version`.
#[test]
fn a_first_party_or_versionless_entry_is_left_out() {
    let table = table(&root(
        "\
own-crate = { version = \"=0.1.0\", path = \"crates/own-crate\" }
harness = { path = \"bench\" }
forked = { version = \"1.1\", git = \"https://example.com/forked\" }
vendored = { version = \"0.2\", path = \"vendor/vendored\" }
",
    ))
    .unwrap();
    assert_eq!(table.keys().collect::<Vec<_>>(), vec!["forked", "vendored"]);
}

/// A root manifest with no `[workspace.dependencies]` table reads as an empty
/// one.
#[test]
fn a_manifest_without_the_table_reads_as_empty() {
    assert_eq!(
        table("[workspace.package]\nversion = \"0.2.0\"\n").unwrap(),
        BTreeMap::new()
    );
}

/// A manifest that does not parse is an error, never an empty table.
#[test]
fn a_manifest_that_does_not_parse_is_refused() {
    assert!(table("[workspace.dependencies\n").is_err());
    assert!(table(&root("plain = 1\n")).is_err());
    assert!(declared(&["[package\n".to_owned()]).is_err());
    assert!(declared(&["[dependencies]\n".to_owned()]).is_err());
}

/// A normal, build or target-specific declaration counts, and a dev-dependency
/// does not; a crate's own requirement counts only as a declaration in any
/// form.
#[test]
fn a_target_or_build_declaration_counts() {
    let declared = declared(&[
        krate("normal", "[dependencies]\nfoo = { workspace = true }\n"),
        krate(
            "build",
            "[build-dependencies]\nfoo = { workspace = true }\n",
        ),
        krate(
            "target",
            "[target.'cfg(unix)'.dependencies]\nfoo = { workspace = true, features = [\"x\"] }\n",
        ),
        krate(
            "target-build",
            "[target.'cfg(unix)'.build-dependencies]\nfoo = { workspace = true }\n",
        ),
        krate("dev", "[dev-dependencies]\nfoo = { workspace = true }\n"),
        krate("own", "[dependencies]\nfoo = \"1\"\n"),
    ])
    .unwrap();
    assert_eq!(
        declared.inherited.get("foo"),
        Some(&names(&["build", "normal", "target", "target-build"]))
    );
    assert_eq!(
        declared.any.get("foo"),
        Some(&names(&[
            "build",
            "normal",
            "own",
            "target",
            "target-build"
        ]))
    );
}

/// A feature-only change and a `default-features` change each yield a line,
/// and the same features in another order yield none.
#[test]
fn a_feature_or_default_features_change_yields_a_line() {
    let crates = [krate(
        "user",
        "[dependencies]\nfeat = { workspace = true }\ndefaults = { workspace = true }\norder = { workspace = true }\n",
    )];
    let before = snapshot(
        &root(
            "\
feat = { version = \"0.39\", features = [\"ssl\"] }
defaults = \"1.52\"
order = { version = \"1\", features = [\"a\", \"b\"] }
",
        ),
        &crates,
    );
    let now = snapshot(
        &root(
            "\
feat = { version = \"0.39\", features = [\"ssl\", \"zstd\"] }
defaults = { version = \"1.52\", default-features = false }
order = { version = \"1\", features = [\"b\", \"a\"] }
",
        ),
        &crates,
    );
    let keys: Vec<String> = lines(&before, &now).into_iter().map(|l| l.key).collect();
    assert_eq!(keys, vec!["defaults", "feat"]);
}

/// A key only in the new table, one only in the old table, and a changed
/// `package` each yield one line of their kind, with the crates of the side
/// that declares it.
#[test]
fn added_removed_and_renamed_each_get_a_line() {
    let before = snapshot(
        &root("gone = \"1.2\"\nyaml = \"0.9\"\n"),
        &[
            krate("old-user", "[dependencies]\ngone = { workspace = true }\n"),
            krate("yaml-user", "[dependencies]\nyaml = { workspace = true }\n"),
        ],
    );
    let now = snapshot(
        &root("fresh = \"2\"\nyaml = { package = \"yaml_fork\", version = \"0.10\" }\n"),
        &[
            krate("new-user", "[dependencies]\nfresh = { workspace = true }\n"),
            krate("yaml-user", "[dependencies]\nyaml = { workspace = true }\n"),
        ],
    );
    let mut renamed = spec("0.10");
    renamed.package = Some("yaml_fork".to_owned());
    assert_eq!(
        lines(&before, &now),
        vec![
            Line {
                key: "fresh".to_owned(),
                crates: names(&["new-user"]),
                change: Move::Added(spec("2")),
            },
            Line {
                key: "gone".to_owned(),
                crates: names(&["old-user"]),
                change: Move::Removed(spec("1.2")),
            },
            Line {
                key: "yaml".to_owned(),
                crates: names(&["yaml-user"]),
                change: Move::Changed(spec("0.9"), renamed),
            },
        ]
    );
}

/// A moved first-party pin, a dependency only a dev-dependency inherits, and
/// one no crate inherits yield no line, each with a crate that declares it.
#[test]
fn path_dev_only_and_undeclared_entries_yield_nothing() {
    let crates = [krate(
        "user",
        "[dependencies]\nown-crate = { workspace = true }\n\n[dev-dependencies]\ntesting = { workspace = true }\n",
    )];
    let before = snapshot(
        &root(
            "\
own-crate = { version = \"=0.1.0\", path = \"crates/own-crate\" }
testing = \"1\"
unused = \"1\"
",
        ),
        &crates,
    );
    let now = snapshot(
        &root(
            "\
own-crate = { version = \"=0.2.0\", path = \"crates/own-crate\" }
testing = \"2\"
unused = \"2\"
",
        ),
        &crates,
    );
    assert_eq!(lines(&before, &now), Vec::new());
}

/// A dependency that moves between the root table and a crate's own manifest
/// is neither added nor removed for that crate.
#[test]
fn a_dependency_the_crate_declares_itself_is_not_added_or_removed() {
    let before = snapshot(
        &root("moved-in = \"1\"\n"),
        &[krate(
            "user",
            "[dependencies]\nmoved-out = \"1\"\nmoved-in = { workspace = true }\n",
        )],
    );
    let now = snapshot(
        &root("moved-out = \"1\"\n"),
        &[krate(
            "user",
            "[dependencies]\nmoved-out = { workspace = true }\nmoved-in = \"1\"\n",
        )],
    );
    assert_eq!(lines(&before, &now), Vec::new());
}

/// No moved requirement renders no entry.
#[test]
fn no_moved_requirement_renders_nothing() {
    assert_eq!(render(&[]), None);
}

/// The entry's exact text: the deduplicated crate list in the title, the
/// prose, and one line per key in each of its forms.
#[test]
fn the_entry_reads_as_the_conventions_ask() {
    let mut tokio_old = spec("1.52");
    tokio_old.default_features = false;
    let mut tokio_new = spec("1.53");
    tokio_new.default_features = false;
    let mut kafka_old = spec("0.39");
    kafka_old.features = names(&["ssl"]);
    let mut kafka_new = spec("0.39");
    kafka_new.features = names(&["ssl", "zstd"]);
    let mut yaml_new = spec("0.10");
    yaml_new.package = Some("yaml_fork".to_owned());
    let mut both = spec("2");
    both.default_features = false;
    both.features = names(&["a", "b"]);

    let lines = [
        Line {
            key: "added".to_owned(),
            crates: names(&["crate-s"]),
            change: Move::Added(both),
        },
        Line {
            key: "kafka".to_owned(),
            crates: names(&["crate-k"]),
            change: Move::Changed(kafka_old, kafka_new),
        },
        Line {
            key: "removed".to_owned(),
            crates: names(&["crate-s"]),
            change: Move::Removed(spec("1.2")),
        },
        Line {
            key: "tokio".to_owned(),
            crates: names(&["crate-c", "crate-k"]),
            change: Move::Changed(tokio_old, tokio_new),
        },
        Line {
            key: "yaml".to_owned(),
            crates: names(&["crate-c"]),
            change: Move::Changed(spec("0.9"), yaml_new),
        },
    ];
    assert_eq!(
        render(&lines).unwrap(),
        "\
**Dependency requirements** (`crate-c`, `crate-k`, `crate-s`)

The published manifests of these crates carry new requirements for the
dependencies listed below. Cargo resolves your lockfile against these
requirements, so a raised requirement can raise the version your project builds
with. Each line shows the requirement in this release first, then the
requirement in the previous release. A crate can enable more features than a
line shows.

- `added` (`crate-s`): `2` without default features, with features `a`, `b`. New in this release.
- `kafka` (`crate-k`): `0.39` with features `ssl`, `zstd`. Previously `0.39` with features `ssl`.
- `removed` (`crate-s`): no longer a dependency. Previously `1.2`.
- `tokio` (`crate-c`, `crate-k`): `1.53` without default features. Previously `1.52` without default features.
- `yaml` (`crate-c`): `0.10` of the `yaml_fork` package. Previously `0.9` of the `yaml` package.
"
    );
}

/// Only `crates/<name>/Cargo.toml` is a crate manifest.
#[test]
fn a_crate_manifest_is_one_level_under_crates() {
    assert!(is_crate_manifest("crates/spate-core/Cargo.toml"));
    for path in [
        "Cargo.toml",
        "crates/Cargo.toml",
        "crates//Cargo.toml",
        "crates/a/b/Cargo.toml",
        "fuzz/Cargo.toml",
    ] {
        assert!(!is_crate_manifest(path), "{path}");
    }
}

/// A changed line names only the crates inheriting the key at both revisions,
/// so its "Previously" is true for each crate it names. A crate that inherited
/// nothing before, one that declared its own requirement before, and one that
/// declares its own now are left off.
#[test]
fn a_changed_line_names_only_crates_that_inherited_before() {
    let before = snapshot(
        &root("foo = \"1.0\"\n"),
        &[
            krate("steady", "[dependencies]\nfoo = { workspace = true }\n"),
            krate("owner", "[dependencies]\nfoo = \"0.9\"\n"),
            krate("leaver", "[dependencies]\nfoo = { workspace = true }\n"),
        ],
    );
    let now = snapshot(
        &root("foo = \"1.1\"\n"),
        &[
            krate("steady", "[dependencies]\nfoo = { workspace = true }\n"),
            krate("owner", "[dependencies]\nfoo = { workspace = true }\n"),
            krate("newcomer", "[dependencies]\nfoo = { workspace = true }\n"),
            krate("leaver", "[dependencies]\nfoo = \"1.1\"\n"),
        ],
    );
    assert_eq!(
        lines(&before, &now),
        vec![Line {
            key: "foo".to_owned(),
            crates: names(&["steady"]),
            change: Move::Changed(spec("1.0"), spec("1.1")),
        }]
    );
}

/// A renamed entry keeps naming its package when only its version moves.
#[test]
fn a_renamed_entry_names_its_package_on_a_version_bump() {
    let mut old = spec("0.9");
    old.package = Some("yaml_fork".to_owned());
    let mut new = spec("0.10");
    new.package = Some("yaml_fork".to_owned());
    let rendered = render(&[Line {
        key: "yaml".to_owned(),
        crates: names(&["user"]),
        change: Move::Changed(old, new),
    }])
    .unwrap();
    assert!(
        rendered.contains(
            "- `yaml` (`user`): `0.10` of the `yaml_fork` package. Previously `0.9` of the `yaml_fork` package.\n"
        ),
        "{rendered}"
    );
}
