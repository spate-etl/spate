//! Regenerates `THIRD-PARTY.md`, the committed inventory of the dependency
//! licenses a release carries.
//!
//! The workspace's own crates are first-party and Apache-2.0, and
//! `about.toml`'s `private = { ignore }` only reaches unpublished members, so
//! the publishable members `cargo metadata` names are filtered out here.
//!
//! `cargo about` emits one row per license *text*, in an order that follows
//! directory-read order and so varies between machines. The rebuild makes the
//! output a property of its content alone: the crate table is sorted by
//! (license id, crate, version) with duplicate rows collapsed, and the summary
//! counts are recomputed from those rows. The generator's own counts are row
//! counts.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;

use serde::Deserialize;

use crate::checks::scratch::{Scratch, nonce};
use crate::run::{self, Error, Outcome, Step};

const MD_TEMPLATE: &str = "about/md.hbs";
const INVENTORY: &str = "THIRD-PARTY.md";

const SUMMARY_RULE: &str = "|---|---|";
const CRATE_HEADER: &str = "| Crate | Version | License |";

pub(crate) fn generate(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("{}", metadata_step().display());
        println!("{}", about_step(Path::new("TMPFILE")).display());
        println!("(filters the first-party and undistributed rows into {INVENTORY})");
        return Ok(());
    }

    let meta = Metadata::parse(&run::capture(root, &metadata_step())?)?;
    let first_party = first_party(&meta)?;
    let distributed = distributed(&meta)?;

    let scratch = Scratch::new("attribution")?;
    let file = scratch.join("attribution.md");
    run::run(root, false, &about_step(&file))?;

    let inventory = rebuild(&read(&file)?, &first_party, &distributed)?;
    install(&root.join(INVENTORY), &inventory.text)?;
    println!(
        "attribution: wrote {INVENTORY} ({} third-party crates, {} first-party row(s) filtered, \
         {} undistributed row(s) filtered, {} duplicate notice row(s) collapsed)",
        inventory.crates, inventory.first_party, inventory.undistributed, inventory.collapsed
    );
    Ok(())
}

/// Reads the whole workspace metadata, so a member anywhere in the tree is
/// covered and a stray directory under `crates/` cannot be mistaken for one.
/// The resolve graph comes with it, under the same feature union the generator
/// is asked for, so both filters read one answer.
fn metadata_step() -> Step<'static> {
    Step::new(
        "cargo",
        [
            "metadata",
            "--format-version",
            "1",
            "--all-features",
            "--locked",
        ],
    )
}

/// `--fail` is the gate: non-zero if any crate's license cannot be determined.
/// `--frozen` would take the run offline, dropping the clarification lookups
/// and silently degrading accuracy.
fn about_step(out: &Path) -> Step<'static> {
    Step::new(
        "cargo",
        [
            "about",
            "generate",
            "--workspace",
            "--all-features",
            "--locked",
            "--fail",
            "-o",
        ],
    )
    .arg(out.to_string_lossy())
    .arg(MD_TEMPLATE)
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    #[serde(default)]
    workspace_members: Vec<String>,
    #[serde(default)]
    resolve: Option<Resolve>,
}

#[derive(Deserialize)]
struct Package {
    #[serde(default)]
    id: String,
    name: String,
    publish: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    id: String,
    deps: Vec<NodeDep>,
}

#[derive(Deserialize)]
struct NodeDep {
    pkg: String,
    #[serde(default)]
    dep_kinds: Vec<DepKind>,
}

#[derive(Deserialize)]
struct DepKind {
    /// `None` for a normal dependency, `"dev"` or `"build"` otherwise.
    kind: Option<String>,
}

impl Metadata {
    fn parse(metadata: &str) -> Result<Self, Error> {
        serde_json::from_str(metadata).map_err(|e| Error::msg(format!("cargo metadata: {e}")))
    }

    /// Whether a package is a member of this workspace. A metadata run that
    /// carries dependencies lists every registry crate here too, and a
    /// registry crate's `publish` is null.
    fn is_member(&self, pkg: &Package) -> bool {
        self.workspace_members.is_empty() || self.workspace_members.contains(&pkg.id)
    }
}

fn publishable(pkg: &Package) -> bool {
    pkg.publish.as_ref().is_none_or(|allow| !allow.is_empty())
}

/// The publishable workspace members, in the order the metadata lists them.
/// `publish: []` marks a member that is never uploaded.
fn first_party(meta: &Metadata) -> Result<Vec<String>, Error> {
    let names: Vec<String> = meta
        .packages
        .iter()
        .filter(|p| meta.is_member(p) && publishable(p))
        .map(|p| p.name.clone())
        .collect();
    if names.is_empty() {
        return Err(Error::msg(
            "the workspace metadata names no publishable packages, so the\n  \
             first-party filter is blind",
        ));
    }
    Ok(names)
}

/// Every crate a published release carries: reachable from a publishable
/// member over normal and build edges. Dev edges are left out, matching
/// `about.toml`'s `ignore-dev-dependencies`.
///
/// Anything outside it is build-time tooling for this repository, which no
/// release distributes and so nothing has to attribute.
fn distributed(meta: &Metadata) -> Result<HashSet<String>, Error> {
    let Some(resolve) = &meta.resolve else {
        return Err(Error::msg(
            "cargo metadata carried no resolve graph, so the distribution\n  \
             filter is blind",
        ));
    };
    let nodes: HashMap<&str, &Node> = resolve.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let name_of: HashMap<&str, &str> = meta
        .packages
        .iter()
        .map(|p| (p.id.as_str(), p.name.as_str()))
        .collect();

    let mut seen: HashSet<&str> = HashSet::new();
    let mut queue: Vec<&str> = meta
        .packages
        .iter()
        .filter(|p| meta.is_member(p) && publishable(p))
        .map(|p| p.id.as_str())
        .collect();
    let roots = queue.len();
    while let Some(id) = queue.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(node) = nodes.get(id) else { continue };
        for dep in &node.deps {
            let carried = dep
                .dep_kinds
                .iter()
                .any(|k| k.kind.as_deref().is_none_or(|k| k == "build"));
            if carried {
                queue.push(dep.pkg.as_str());
            }
        }
    }

    if seen.len() <= roots {
        return Err(Error::msg(
            "the resolve graph reaches nothing beyond the publishable members,\n  \
             so the distribution filter would drop every third-party row",
        ));
    }
    Ok(seen
        .into_iter()
        .filter_map(|id| name_of.get(id).map(|n| (*n).to_string()))
        .collect())
}

// ---------------------------------------------------------------------------
// THIRD-PARTY.md.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Inventory {
    text: String,
    crates: usize,
    first_party: usize,
    undistributed: usize,
    collapsed: usize,
}

/// Rebuilds the generated Markdown with the crate table filtered, sorted and
/// collapsed, and the summary counts recomputed from what is left.
fn rebuild(
    generated: &str,
    first_party: &[String],
    distributed: &HashSet<String>,
) -> Result<Inventory, Error> {
    let lines = records(generated);

    // The summary rule is two columns and the crate rule is three, so matching
    // the whole line keeps them apart.
    let summary_rule = position(&lines, SUMMARY_RULE)
        .ok_or_else(|| Error::msg("summary table not found in generated output"))?;
    let crate_header = position(&lines, CRATE_HEADER)
        .ok_or_else(|| Error::msg("crate table header not found in generated output"))?;
    let crate_rule = crate_header + 1; // the |---|---|---| under the header

    // The summary rows run from the rule to the first line that is not a row.
    let mut summary_end = summary_rule;
    while lines.get(summary_end).is_some_and(|l| l.starts_with("| ")) {
        summary_end += 1;
    }
    if summary_end == summary_rule {
        return Err(Error::msg("summary table has no rows"));
    }

    // The crate table runs to EOF. Anything else down there would be silently
    // dropped by the rebuild below, so the run stops on it.
    let below = lines.get(crate_rule..).unwrap_or_default();
    if let Some(trailing) = below.iter().find(|l| !l.starts_with("| ") && !l.is_empty()) {
        return Err(Error::msg(format!(
            "unexpected line below the crate table: {trailing}"
        )));
    }

    let rows_raw: Vec<&str> = below
        .iter()
        .copied()
        .filter(|l| l.starts_with("| "))
        .collect();
    if rows_raw.is_empty() {
        return Err(Error::msg("crate table is empty"));
    }

    // cargo-about's own summary against its own table, before any filtering: a
    // row the generator lost would otherwise read as a legitimate removal on
    // the next run.
    let from_rows: BTreeSet<&str> = rows_raw.iter().map(|r| field(r, 4)).collect();
    let from_summary: BTreeSet<&str> = lines[summary_rule..summary_end]
        .iter()
        .map(|r| field(r, 2))
        .collect();
    if from_rows != from_summary {
        return Err(Error::msg(
            "cargo-about's summary and table disagree on license ids",
        ));
    }

    // Drop the first-party rows, and the rows for crates no release carries,
    // before anything is counted, so every check below judges the filtered
    // table.
    let names: HashSet<&str> = first_party.iter().map(String::as_str).collect();
    let rows_carried: Vec<&str> = rows_raw
        .iter()
        .copied()
        .filter(|r| distributed.contains(field(r, 2)))
        .collect();
    let undistributed = rows_raw.len() - rows_carried.len();
    let rows_third: Vec<&str> = rows_carried
        .iter()
        .copied()
        .filter(|r| !names.contains(field(r, 2)))
        .collect();
    if rows_third.is_empty() {
        return Err(Error::msg(
            "every row was filtered; the crate table is gone",
        ));
    }

    // Each first-party crate is a publishable workspace member, so cargo-about
    // lists it and the filter must drop at least one row for it. Zero dropped
    // means a crate was renamed away from its directory and the filter has gone
    // blind for it.
    for name in first_party {
        let cell = format!("| `{name}` |");
        if rows_third.iter().any(|r| r.contains(&cell)) {
            return Err(Error::msg(format!(
                "first-party crate '{name}' survived into the table"
            )));
        }
        if !rows_raw.iter().any(|r| r.contains(&cell)) {
            return Err(Error::msg(format!(
                "first-party crate '{name}' never appeared in the generated table: does its\n  \
                 package name still match its directory under crates/?"
            )));
        }
    }

    // Rows tying on all three keys are byte-identical, so the dedup collapses
    // the duplicate notices a clarified crate produces.
    let mut rows = rows_third.clone();
    rows.sort_unstable_by(|a, b| {
        field(a, 4)
            .cmp(field(b, 4))
            .then_with(|| field(a, 2).cmp(field(b, 2)))
            .then_with(|| field(a, 3).cmp(field(b, 3)))
            .then_with(|| a.cmp(b))
    });
    rows.dedup();

    let mut text = String::with_capacity(generated.len());
    for line in &lines[..summary_rule] {
        text.push_str(line);
        text.push('\n');
    }
    for (id, n) in summary(&rows) {
        text.push_str(&format!("| `{id}` | {n} |\n"));
    }
    for line in lines.get(summary_end..crate_rule).unwrap_or_default() {
        text.push_str(line);
        text.push('\n');
    }
    for row in &rows {
        text.push_str(row);
        text.push('\n');
    }

    Ok(Inventory {
        crates: rows.len(),
        first_party: rows_carried.len() - rows_third.len(),
        undistributed,
        collapsed: rows_third.len() - rows.len(),
        text,
    })
}

/// Counts by license id, ordered as cargo-about orders them: most used first,
/// ties by id ascending.
fn summary<'a>(rows: &[&'a str]) -> Vec<(&'a str, usize)> {
    let mut tally: HashMap<&str, usize> = HashMap::new();
    for row in rows {
        *tally.entry(field(row, 4)).or_default() += 1;
    }
    let mut out: Vec<(&str, usize)> = tally.into_iter().collect();
    out.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    out
}

/// The one-based backtick-separated field, empty when the line has no such
/// field.
///
/// Rows look like ``| `crate` | version | `LICENSE` |``: field 2 is the bare
/// crate name, field 3 the version and field 4 the license id. The backtick
/// delimiter keeps the name bare, so `spate` sorts before `spate-test`.
fn field(line: &str, n: usize) -> &str {
    line.split('`').nth(n - 1).unwrap_or_default()
}

fn position(lines: &[&str], exact: &str) -> Option<usize> {
    lines.iter().position(|l| *l == exact).map(|i| i + 1)
}

// ---------------------------------------------------------------------------
// Files.
// ---------------------------------------------------------------------------

/// The input split on `\n` alone, with one trailing newline dropped.
fn records(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    text.strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .collect()
}

fn read(path: &Path) -> Result<String, Error> {
    fs::read_to_string(path).map_err(|e| Error::msg(format!("{}: {e}", path.display())))
}

/// Writes through a sibling of the destination, so the rename stays on one
/// filesystem and a failure mid-write leaves no truncated artifact behind.
fn install(out: &Path, text: &str) -> Result<(), Error> {
    let dir = out.parent().unwrap_or(Path::new("."));
    let staged = dir.join(format!(".attribution.{}.{}", std::process::id(), nonce()));
    let write = || -> std::io::Result<()> {
        // An exclusive create, so an existing file or symlink at the name is
        // not followed.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        file.write_all(text.as_bytes())?;
        // The inventory is committed, so the mode is set here and a
        // restrictive umask cannot leave it unreadable.
        readable(&file)?;
        drop(file);
        fs::rename(&staged, out)
    };
    write().map_err(|e| {
        drop(fs::remove_file(&staged));
        Error::msg(format!("{}: {e}", out.display()))
    })
}

#[cfg(unix)]
fn readable(file: &fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o644))
}

#[cfg(not(unix))]
fn readable(_file: &fs::File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every crate a fixture names, so the distribution filter drops nothing
    /// and the test isolates the behaviour it is about.
    fn carries_all(text: &str) -> HashSet<String> {
        records(text)
            .iter()
            .filter(|l| l.starts_with("| "))
            .map(|l| field(l, 2).to_string())
            .collect()
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    // -- the invocations ---------------------------------------------------

    #[test]
    fn the_generator_is_asked_for_the_whole_feature_union_and_fails_loud() {
        assert_eq!(
            about_step(Path::new("/tmp/x")).display(),
            "cargo about generate --workspace --all-features --locked --fail -o /tmp/x about/md.hbs"
        );
    }

    #[test]
    fn the_filters_read_one_metadata_run_over_the_same_feature_union() {
        assert_eq!(
            metadata_step().display(),
            "cargo metadata --format-version 1 --all-features --locked"
        );
    }

    /// A metadata run carrying dependencies lists every registry crate, and a
    /// registry crate's `publish` is null, so membership decides first.
    #[test]
    fn a_registry_crate_is_not_first_party() {
        let json = r#"{"packages":[
            {"id":"m","name":"mine","publish":null},
            {"id":"r","name":"serde","publish":null}],
            "workspace_members":["m"]}"#;
        assert_eq!(
            first_party(&Metadata::parse(json).unwrap()).unwrap(),
            names(&["mine"])
        );
    }

    /// Normal and build edges from a publishable member are carried; a dev
    /// edge is not, and neither is anything only an unpublished member needs.
    #[test]
    fn the_distribution_set_is_what_a_release_carries() {
        let json = r#"{"packages":[
            {"id":"pub","name":"spate","publish":null},
            {"id":"priv","name":"spate-xtask","publish":[]},
            {"id":"dep","name":"serde","publish":null},
            {"id":"bd","name":"cc","publish":null},
            {"id":"dev","name":"proptest","publish":null},
            {"id":"tool","name":"clap","publish":null}],
            "workspace_members":["pub","priv"],
            "resolve":{"nodes":[
              {"id":"pub","deps":[
                {"pkg":"dep","dep_kinds":[{"kind":null}]},
                {"pkg":"bd","dep_kinds":[{"kind":"build"}]},
                {"pkg":"dev","dep_kinds":[{"kind":"dev"}]}]},
              {"id":"priv","deps":[{"pkg":"tool","dep_kinds":[{"kind":null}]}]},
              {"id":"dep","deps":[]},{"id":"bd","deps":[]},
              {"id":"dev","deps":[]},{"id":"tool","deps":[]}]}}"#;
        let got = distributed(&Metadata::parse(json).unwrap()).unwrap();
        let mut got: Vec<&str> = got.iter().map(String::as_str).collect();
        got.sort_unstable();
        assert_eq!(got, ["cc", "serde", "spate"]);
    }

    #[test]
    fn metadata_without_a_resolve_graph_is_refused() {
        let json = r#"{"packages":[{"id":"a","name":"a","publish":null}],
            "workspace_members":["a"]}"#;
        assert!(
            distributed(&Metadata::parse(json).unwrap())
                .unwrap_err()
                .message
                .starts_with("cargo metadata carried no resolve graph")
        );
    }

    /// A resolve graph reaching nothing would filter every third-party row
    /// away, which reads as a clean run rather than a broken one.
    #[test]
    fn a_distribution_set_of_members_alone_is_refused() {
        let json = r#"{"packages":[{"id":"a","name":"a","publish":null}],
            "workspace_members":["a"],
            "resolve":{"nodes":[{"id":"a","deps":[]}]}}"#;
        assert!(
            distributed(&Metadata::parse(json).unwrap())
                .unwrap_err()
                .message
                .starts_with("the resolve graph reaches nothing beyond")
        );
    }

    /// A crate no publishable member reaches is build-time tooling for this
    /// repository, and no release carries it.
    #[test]
    fn an_undistributed_row_is_dropped_and_counted() {
        // `anyhow` holds two rows in the fixture, one per license text.
        let carried: HashSet<String> = ["serde", "spate", "spate-core"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let out = rebuild(MD, &names(&["spate", "spate-core"]), &carried).unwrap();
        assert!(
            !out.text.contains("| `anyhow` |"),
            "a crate nothing distributes stayed in the inventory"
        );
        assert_eq!(out.undistributed, 2);
    }

    #[test]
    fn a_member_published_nowhere_is_not_first_party() {
        let json = r#"{"packages":[
            {"name":"a","publish":null},
            {"name":"b","publish":[]},
            {"name":"c","publish":["registry"]}]}"#;
        assert_eq!(
            first_party(&Metadata::parse(json).unwrap()).unwrap(),
            names(&["a", "c"])
        );
    }

    #[test]
    fn a_workspace_with_nothing_publishable_is_refused() {
        let json = r#"{"packages":[{"name":"a","publish":[]}]}"#;
        assert!(
            first_party(&Metadata::parse(json).unwrap())
                .unwrap_err()
                .message
                .starts_with("the workspace metadata names no publishable packages")
        );
    }

    // -- THIRD-PARTY.md ----------------------------------------------------

    /// `cargo about` groups by license text, so `spate` sits among third-party
    /// rows and `serde` appears twice under one id.
    const MD: &str = "\
# Third-party licenses

| License | Crates |
|---|---|
| `MIT` | 4 |
| `Apache-2.0` | 2 |

## Crates

Prose.

| Crate | Version | License |
|---|---|---|
| `serde` | 1.0.0 | `MIT` |
| `spate` | 0.1.0 | `MIT` |
| `anyhow` | 1.0.0 | `MIT` |
| `serde` | 1.0.0 | `MIT` |
| `anyhow` | 1.0.0 | `Apache-2.0` |
| `spate-core` | 0.1.0 | `Apache-2.0` |
";

    fn inventory(text: &str) -> Inventory {
        rebuild(text, &names(&["spate", "spate-core"]), &carries_all(text)).unwrap()
    }

    #[test]
    fn the_table_is_sorted_by_license_then_crate_then_version() {
        assert_eq!(
            inventory(MD).text,
            "\
# Third-party licenses

| License | Crates |
|---|---|
| `MIT` | 2 |
| `Apache-2.0` | 1 |

## Crates

Prose.

| Crate | Version | License |
|---|---|---|
| `anyhow` | 1.0.0 | `Apache-2.0` |
| `anyhow` | 1.0.0 | `MIT` |
| `serde` | 1.0.0 | `MIT` |
"
        );
    }

    #[test]
    fn a_duplicate_notice_row_is_collapsed_and_counted_as_one() {
        let out = inventory(MD);
        assert_eq!((out.crates, out.first_party, out.collapsed), (3, 2, 1));
    }

    /// The summary is rebuilt from the filtered rows, so the generator's own
    /// counts never reach the artifact.
    #[test]
    fn the_summary_counts_crates_and_not_generated_rows() {
        let out = inventory(MD);
        assert!(out.text.contains("| `MIT` | 2 |"));
        assert!(!out.text.contains("| `MIT` | 4 |"));
    }

    #[test]
    fn ties_in_the_summary_are_broken_by_id_ascending() {
        let rows = [
            "| `b` | 1 | `Zlib` |",
            "| `a` | 1 | `MIT` |",
            "| `c` | 1 | `MIT` |",
            "| `d` | 1 | `ISC` |",
        ];
        assert_eq!(summary(&rows), vec![("MIT", 2), ("ISC", 1), ("Zlib", 1)]);
    }

    /// Rows whose order the comparator alone decides, each pair listed the
    /// wrong way round. One license id throughout, a name and a name it
    /// prefixes, two versions of one crate, and two rows tying on every key.
    const MD_ORDER: &str = "\
# Third-party licenses

| License | Crates |
|---|---|
| `MIT` | 7 |

## Crates

| Crate | Version | License |
|---|---|---|
| `rand_core` | 0.6.4 | `MIT` |
| `rand` | 0.9.2 | `MIT` |
| `rand` | 0.8.5 | `MIT` |
| `anyhow` | 1.0.0 | `MIT` |
| `anyhow` | 1.0.0 | `MIT` (see NOTICE) |
| `spate` | 0.1.0 | `MIT` |
| `spate-core` | 0.1.0 | `MIT` |
";

    /// The crate rows of the rebuilt `MD_ORDER` inventory, in order.
    fn ordered() -> Vec<String> {
        rebuild(
            MD_ORDER,
            &names(&["spate", "spate-core"]),
            &carries_all(MD_ORDER),
        )
        .unwrap()
        .text
        .lines()
        .skip_while(|l| *l != "|---|---|---|")
        .skip(1)
        .map(str::to_owned)
        .collect()
    }

    fn precedes(a: &str, b: &str) -> bool {
        let rows = ordered();
        let at = |row: &str| {
            rows.iter()
                .position(|r| r == row)
                .unwrap_or_else(|| panic!("row absent from the rebuilt table: {row}"))
        };
        at(a) < at(b)
    }

    /// Backtick fields sort names as names; the pipe would sort `rand_core`
    /// before `rand` on the closing backtick.
    #[test]
    fn a_crate_name_sorts_without_its_delimiters() {
        assert_eq!(field("| `spate-test` | 1.0 | `MIT` |", 2), "spate-test");
        assert_eq!(field("| `spate-test` | 1.0 | `MIT` |", 3), " | 1.0 | ");
        assert_eq!(field("| `spate-test` | 1.0 | `MIT` |", 4), "MIT");
        assert!(precedes(
            "| `rand` | 0.9.2 | `MIT` |",
            "| `rand_core` | 0.6.4 | `MIT` |"
        ));
    }

    #[test]
    fn two_versions_of_one_crate_sort_by_version() {
        assert!(precedes(
            "| `rand` | 0.8.5 | `MIT` |",
            "| `rand` | 0.9.2 | `MIT` |"
        ));
    }

    /// Two distinct rows tying on every key order by the whole line, so the
    /// table does not inherit the order the generator emitted them in.
    #[test]
    fn rows_tying_on_every_key_sort_by_the_whole_line() {
        assert!(precedes(
            "| `anyhow` | 1.0.0 | `MIT` (see NOTICE) |",
            "| `anyhow` | 1.0.0 | `MIT` |"
        ));
    }

    fn refuse(text: &str) -> String {
        rebuild(text, &names(&["spate", "spate-core"]), &carries_all(text))
            .unwrap_err()
            .message
    }

    #[test]
    fn a_missing_summary_rule_is_refused() {
        assert_eq!(
            refuse(&MD.replacen("|---|---|\n", "", 1)),
            "summary table not found in generated output"
        );
    }

    #[test]
    fn a_missing_crate_header_is_refused() {
        assert_eq!(
            refuse(&MD.replace("| Crate | Version | License |", "| Crate | License |")),
            "crate table header not found in generated output"
        );
    }

    #[test]
    fn a_summary_with_no_rows_is_refused() {
        let text = MD.replace("| `MIT` | 4 |\n| `Apache-2.0` | 2 |\n", "");
        assert_eq!(refuse(&text), "summary table has no rows");
    }

    /// A line below the crate table would be dropped by the rebuild, so it
    /// stops the run instead.
    #[test]
    fn a_line_below_the_crate_table_is_refused() {
        let text = format!("{MD}Footnote.\n");
        assert_eq!(
            refuse(&text),
            "unexpected line below the crate table: Footnote."
        );
    }

    #[test]
    fn a_blank_line_below_the_crate_table_is_allowed_and_dropped() {
        let out = inventory(&format!("{MD}\n"));
        assert!(out.text.ends_with("| `serde` | 1.0.0 | `MIT` |\n"));
    }

    #[test]
    fn an_empty_crate_table_is_refused() {
        let cut = MD.split("|---|---|---|\n").next().unwrap();
        let text = format!("{cut}|---|---|---|\n");
        assert_eq!(refuse(&text), "crate table is empty");
    }

    #[test]
    fn a_summary_naming_an_id_the_table_does_not_is_refused() {
        let text = MD.replace("| `Apache-2.0` | 2 |", "| `ISC` | 2 |");
        assert_eq!(
            refuse(&text),
            "cargo-about's summary and table disagree on license ids"
        );
    }

    /// A row with no backtick cannot carry a license id under either reading,
    /// so the comparison against the summary catches it.
    #[test]
    fn a_row_without_backticks_is_refused() {
        let text = MD.replace("| `serde` | 1.0.0 | `MIT` |\n", "| serde 1.0.0 MIT |\n");
        assert_eq!(
            refuse(&text),
            "cargo-about's summary and table disagree on license ids"
        );
    }

    #[test]
    fn a_first_party_crate_the_filter_misses_is_refused() {
        assert_eq!(
            rebuild(
                MD,
                &names(&["spate", "spate-core", "spate-kafka"]),
                &carries_all(MD)
            )
            .unwrap_err()
            .message,
            "first-party crate 'spate-kafka' never appeared in the generated table: does its\n  \
             package name still match its directory under crates/?"
        );
    }

    /// The row filter reads the crate cell alone, so a first-party name in any
    /// other cell reaches the rebuilt table and the last check over it refuses.
    #[test]
    fn a_first_party_name_surviving_in_another_cell_is_refused() {
        let text = MD
            .replace(
                "| `Apache-2.0` | 2 |",
                "| `Apache-2.0` | 1 |\n| `spate` | 1 |",
            )
            .replace(
                "| `anyhow` | 1.0.0 | `Apache-2.0` |",
                "| `anyhow` | 1.0.0 | `spate` |",
            );
        assert_eq!(
            refuse(&text),
            "first-party crate 'spate' survived into the table"
        );
    }

    #[test]
    fn a_table_of_nothing_but_first_party_rows_is_refused() {
        let text = MD.replace("| `serde` | 1.0.0 | `MIT` |", "| `spate` | 1.0.0 | `MIT` |");
        let names = names(&["spate", "spate-core", "anyhow", "serde"]);
        assert_eq!(
            rebuild(&text, &names, &carries_all(&text))
                .unwrap_err()
                .message,
            "every row was filtered; the crate table is gone"
        );
    }

    /// `records` splits on `\n` alone, so a `\r` stays on its line. One
    /// trailing newline is dropped, and empty input yields no records.
    #[test]
    fn a_carriage_return_is_not_a_line_terminator() {
        assert_eq!(records("a\r\nb\n"), ["a\r", "b"]);
        assert_eq!(records("a\n\n"), ["a", ""]);
        assert_eq!(records("a"), ["a"]);
        assert!(records("").is_empty());
    }

    /// The generator's directory is the owner's alone and the artifact is
    /// world-readable, whatever the umask.
    #[cfg(unix)]
    #[test]
    fn the_scratch_directory_is_private_and_the_artifact_is_readable() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let scratch = Scratch::new("attribution").unwrap();
        let out = scratch.join("artifact.md");
        install(&out, "body\n").unwrap();
        assert_eq!(fs::read_to_string(&out).unwrap(), "body\n");
        assert_eq!(mode(scratch.dir()), 0o700);
        assert_eq!(mode(&out), 0o644);
    }
}
