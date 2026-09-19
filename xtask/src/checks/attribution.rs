//! Regenerates the dependency attribution artifacts: the committed
//! `THIRD-PARTY.md` inventory and the site's license page.
//!
//! Both artifacts are third-party inventories. The workspace's own crates are
//! first-party and Apache-2.0, and `about.toml`'s `private = { ignore }` only
//! reaches unpublished members, so the publishable members `cargo metadata`
//! names are filtered out here instead. Both artifacts generate through this
//! module, so they agree.
//!
//! `cargo about` emits one row per license *text*, in an order that follows
//! directory-read order and so varies between machines. The Markdown rebuild
//! makes the output a property of its content alone: the crate table is sorted
//! by (license id, crate, version) with duplicate rows collapsed, and the
//! summary counts are recomputed from those rows. The generator's own counts
//! are row counts.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;

use serde::Deserialize;

use crate::checks::scratch::{Scratch, nonce};
use crate::run::{self, Error, Outcome, Step};

const MD_TEMPLATE: &str = "about/md.hbs";
const HTML_TEMPLATE: &str = "about/html.hbs";
const INVENTORY: &str = "THIRD-PARTY.md";

/// The crate chip the page filter reads, one per line in the page template.
const CHIP: &str = "<code data-crate=\"";
const BEGIN: &str = "<!-- BEGIN-LICENSE ";
const END: &str = "<!-- END-LICENSE -->";
const TOC_ROW: &str = "<li data-license-id=\"";

const SUMMARY_RULE: &str = "|---|---|";
const CRATE_HEADER: &str = "| Crate | Version | License |";

pub(crate) fn generate(root: &Path, explain: bool, html: Option<&str>) -> Outcome {
    let (template, tag, out) = match html {
        Some(path) => (HTML_TEMPLATE, "html", path),
        None => (MD_TEMPLATE, "md", INVENTORY),
    };
    if explain {
        println!("{}", metadata_step().display());
        println!("{}", about_step(template, Path::new("TMPFILE")).display());
        println!("(filters the first-party rows into {out})");
        return Ok(());
    }

    let first_party = first_party(&run::capture(root, &metadata_step())?)?;

    let scratch = Scratch::new("attribution")?;
    let file = scratch.join(&format!("attribution.{tag}"));
    run::run(root, false, &about_step(template, &file))?;
    let generated = read(&file)?;

    let (text, report) = if html.is_some() {
        (filter_page(&generated, &first_party, out)?, String::new())
    } else {
        let inventory = rebuild(&generated, &first_party)?;
        let report = format!(
            " ({} third-party crates, {} first-party row(s) filtered, {} duplicate notice row(s) collapsed)",
            inventory.crates, inventory.first_party, inventory.collapsed
        );
        (inventory.text, report)
    };

    install(&root.join(out), &text)?;
    println!("attribution: wrote {out}{report}");
    Ok(())
}

/// Reads the whole workspace metadata, so a member anywhere in the tree is
/// covered and a stray directory under `crates/` cannot be mistaken for one.
fn metadata_step() -> Step<'static> {
    Step::new("cargo", ["metadata", "--no-deps", "--format-version", "1"])
}

/// `--fail` is the gate: non-zero if any crate's license cannot be determined.
/// `--frozen` would take the run offline, dropping the clarification lookups
/// and silently degrading accuracy.
fn about_step(template: &str, out: &Path) -> Step<'static> {
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
    .arg(template)
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    publish: Option<Vec<String>>,
}

/// The publishable workspace members, in the order the metadata lists them.
/// `publish: []` marks a member that is never uploaded.
fn first_party(metadata: &str) -> Result<Vec<String>, Error> {
    let parsed: Metadata =
        serde_json::from_str(metadata).map_err(|e| Error::msg(format!("cargo metadata: {e}")))?;
    let names: Vec<String> = parsed
        .packages
        .into_iter()
        .filter(|p| p.publish.as_ref().is_none_or(|allow| !allow.is_empty()))
        .map(|p| p.name)
        .collect();
    if names.is_empty() {
        return Err(Error::msg(
            "the workspace metadata names no publishable packages, so the\n  \
             first-party filter is blind",
        ));
    }
    Ok(names)
}

// ---------------------------------------------------------------------------
// The /licenses/ page.
// ---------------------------------------------------------------------------

/// Drops the first-party chips, the sections they empty and the table-of-
/// contents rows that reach zero, rewriting the surviving counts.
///
/// Two passes. The first counts the crates each section keeps; the second
/// emits. Handlebars HTML-escapes the license text, so a text cannot fake a
/// sentinel or a chip.
fn filter_page(generated: &str, first_party: &[String], out: &str) -> Result<String, Error> {
    // Both halves per crate, mirroring the Markdown path: every first-party
    // package must appear in the generated page, or the filter has nothing to
    // drop for it and has gone blind for that name.
    for name in first_party {
        if !generated.contains(&format!("data-crate=\"{name}\"")) {
            return Err(Error::msg(format!(
                "first-party crate '{name}' never appeared in the generated page"
            )));
        }
    }

    let names: HashSet<&str> = first_party.iter().map(String::as_str).collect();

    // A crate shipping several notices sits in several sections, and the
    // inventory counts it once, so each id counts distinct (crate, version)
    // pairs here and the two agree. A chip outside a sentinel pair counts
    // toward nothing.
    let mut keep: HashMap<usize, usize> = HashMap::new();
    let mut count: HashMap<&str, usize> = HashMap::new();
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    let mut block = 0usize;
    let mut inside = false;
    let mut id = "";
    for line in records(generated) {
        if let Some(rest) = line.strip_prefix(BEGIN) {
            block += 1;
            inside = true;
            id = rest.strip_suffix(" -->").unwrap_or(rest);
        } else if line == END {
            inside = false;
        } else if inside && line.contains(CHIP) && !names.contains(crate_of(line)) {
            *keep.entry(block).or_default() += 1;
            if seen.insert((id, chip_of(line))) {
                *count.entry(id).or_default() += 1;
            }
        }
    }

    let mut page = String::with_capacity(generated.len());
    let mut blockno = 0usize;
    let mut inside = false;
    let mut skip = false;
    let mut dropped = 0usize;
    let mut toc = 0usize;
    for line in records(generated) {
        if line.starts_with(BEGIN) {
            inside = true;
            blockno += 1;
            skip = keep.get(&blockno).copied().unwrap_or(0) == 0;
            continue;
        }
        if line == END {
            inside = false;
            skip = false;
            continue;
        }
        if inside && skip {
            continue;
        }
        if inside && line.contains(CHIP) && names.contains(crate_of(line)) {
            dropped += 1;
            continue;
        }
        if line.contains(TOC_ROW) {
            let n = count
                .get(id_of(line, "data-license-id="))
                .copied()
                .unwrap_or(0);
            if n < 1 {
                continue;
            }
            let row = rewrite_count(line, n).ok_or_else(|| {
                Error::msg(format!(
                    "a TOC row lost the count shape the rewrite expects: {line}"
                ))
            })?;
            toc += 1;
            page.push_str(&row);
            page.push('\n');
            continue;
        }
        page.push_str(line);
        page.push('\n');
    }

    if inside {
        return Err(Error::msg("unterminated BEGIN-LICENSE section"));
    }
    if blockno < 1 {
        return Err(Error::msg(
            "no BEGIN-LICENSE sections; the template lost its sentinels",
        ));
    }
    if dropped < 1 {
        return Err(Error::msg(
            "no first-party crate chip was dropped; the filter has gone blind",
        ));
    }
    // Every id that kept a crate keeps its TOC row, and no other row survives;
    // a drift between the overview ids and the section ids would otherwise thin
    // the TOC silently.
    if toc != count.len() {
        return Err(Error::msg(format!(
            "{toc} TOC rows kept for {} license ids with crates",
            count.len()
        )));
    }

    // Nothing first-party may survive, name by name.
    for name in first_party {
        if page.contains(&format!("data-crate=\"{name}\"")) {
            return Err(Error::msg(format!(
                "first-party crate '{name}' survived into {out}"
            )));
        }
    }
    Ok(page)
}

/// The text after the last occurrence of `marker`, or the whole line when it
/// holds none.
fn after_last<'a>(line: &'a str, marker: &str) -> &'a str {
    line.rfind(marker)
        .map_or(line, |i| &line[i + marker.len()..])
}

/// The crate named by the last chip on the line.
fn crate_of(line: &str) -> &str {
    let rest = after_last(line, CHIP);
    rest.split('"').next().unwrap_or(rest)
}

/// The last chip's own text, `name version`. The inventory keeps one row per
/// (crate, version), so two linked versions of one crate count twice there and
/// must count twice here.
fn chip_of(line: &str) -> &str {
    let rest = after_last(line, CHIP);
    let body = rest.find("\">").map_or(rest, |i| &rest[i + "\">".len()..]);
    body.split("</code>").next().unwrap_or(body)
}

/// The attribute value after the last occurrence of `marker`.
fn id_of<'a>(line: &'a str, marker: &str) -> &'a str {
    let rest = after_last(line, &format!("{marker}\""));
    rest.split('"').next().unwrap_or(rest)
}

/// The row with its crate count replaced, or `None` when the row carries no
/// count to replace. The leftmost digit run followed by ` crates</li>` is the
/// one rewritten.
fn rewrite_count(line: &str, n: usize) -> Option<String> {
    const TAIL: &str = " crates</li>";
    let noun = if n == 1 { "crate" } else { "crates" };
    let bytes = line.as_bytes();
    let mut from = 0;
    while let Some(offset) = line[from..].find(TAIL) {
        let at = from + offset;
        let mut start = at;
        while start > 0 && bytes[start - 1].is_ascii_digit() {
            start -= 1;
        }
        if start < at {
            return Some(format!(
                "{}{n} {noun}</li>{}",
                &line[..start],
                &line[at + TAIL.len()..]
            ));
        }
        from = at + 1;
    }
    None
}

// ---------------------------------------------------------------------------
// THIRD-PARTY.md.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Inventory {
    text: String,
    crates: usize,
    first_party: usize,
    collapsed: usize,
}

/// Rebuilds the generated Markdown with the crate table filtered, sorted and
/// collapsed, and the summary counts recomputed from what is left.
fn rebuild(generated: &str, first_party: &[String]) -> Result<Inventory, Error> {
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

    // Drop the first-party rows before anything is counted, so every check
    // below judges the filtered table.
    let names: HashSet<&str> = first_party.iter().map(String::as_str).collect();
    let rows_third: Vec<&str> = rows_raw
        .iter()
        .copied()
        .filter(|r| !names.contains(field(r, 2)))
        .collect();
    if rows_third.is_empty() {
        return Err(Error::msg(
            "every row was filtered as first-party; the crate table is gone",
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
        first_party: rows_raw.len() - rows_third.len(),
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

/// The input split on `\n` alone, so a `\r` a license text carries survives
/// into the artifact.
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
        // Both artifacts are committed and served, so the mode is set here and
        // a restrictive umask cannot leave them unreadable.
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

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    // -- the invocations ---------------------------------------------------

    #[test]
    fn the_generator_is_asked_for_the_whole_feature_union_and_fails_loud() {
        assert_eq!(
            about_step(MD_TEMPLATE, Path::new("/tmp/x")).display(),
            "cargo about generate --workspace --all-features --locked --fail -o /tmp/x about/md.hbs"
        );
    }

    #[test]
    fn the_first_party_set_comes_from_the_workspace_metadata() {
        assert_eq!(
            metadata_step().display(),
            "cargo metadata --no-deps --format-version 1"
        );
    }

    #[test]
    fn a_member_published_nowhere_is_not_first_party() {
        let json = r#"{"packages":[
            {"name":"a","publish":null},
            {"name":"b","publish":[]},
            {"name":"c","publish":["registry"]}]}"#;
        assert_eq!(first_party(json).unwrap(), names(&["a", "c"]));
    }

    #[test]
    fn a_workspace_with_nothing_publishable_is_refused() {
        let json = r#"{"packages":[{"name":"a","publish":[]}]}"#;
        assert!(
            first_party(json)
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
        rebuild(text, &names(&["spate", "spate-core"])).unwrap()
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
        rebuild(MD_ORDER, &names(&["spate", "spate-core"]))
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
        rebuild(text, &names(&["spate", "spate-core"]))
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
            rebuild(MD, &names(&["spate", "spate-core", "spate-kafka"]))
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
            rebuild(&text, &names).unwrap_err().message,
            "every row was filtered as first-party; the crate table is gone"
        );
    }

    // -- the /licenses/ page -----------------------------------------------

    const PAGE: &str = "\
<ul>
  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> — 3 crates</li>
  <li data-license-id=\"Zlib\">Zlib <code>(Zlib)</code> — 1 crates</li>
</ul>
<!-- BEGIN-LICENSE MIT -->
<h2>
  <code data-crate=\"serde\">serde 1.0.0</code>
  <code data-crate=\"spate\">spate 0.1.0</code>
  — MIT <code>(MIT)</code>
</h2>
<pre>MIT text</pre>
<!-- END-LICENSE -->
<!-- BEGIN-LICENSE Zlib -->
<h2>
  <code data-crate=\"spate-core\">spate-core 0.1.0</code>
  — Zlib <code>(Zlib)</code>
</h2>
<pre>Zlib text</pre>
<!-- END-LICENSE -->
";

    fn page(text: &str) -> Result<String, Error> {
        filter_page(text, &names(&["spate", "spate-core"]), "out.html")
    }

    /// The sentinels leave with the sections they marked, an emptied section
    /// takes its TOC row with it, and the surviving row carries the count of
    /// what is left.
    #[test]
    fn an_emptied_section_and_its_toc_row_are_dropped() {
        assert_eq!(
            page(PAGE).unwrap(),
            "\
<ul>
  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> — 1 crate</li>
</ul>
<h2>
  <code data-crate=\"serde\">serde 1.0.0</code>
  — MIT <code>(MIT)</code>
</h2>
<pre>MIT text</pre>
"
        );
    }

    #[test]
    fn a_section_keeping_two_crates_reads_as_plural() {
        let text = PAGE.replace(
            "  <code data-crate=\"spate\">spate 0.1.0</code>\n",
            "  <code data-crate=\"anyhow\">anyhow 1.0.0</code>\n  \
             <code data-crate=\"spate\">spate 0.1.0</code>\n",
        );
        assert!(page(&text).unwrap().contains("— 2 crates</li>"));
    }

    /// One crate's two notices under one id are two sections and one count, so
    /// the page agrees with the inventory.
    #[test]
    fn a_crate_with_two_notices_counts_once() {
        let text = PAGE.replace(
            "<!-- BEGIN-LICENSE Zlib -->",
            "<!-- BEGIN-LICENSE MIT -->\n<h2>\n  \
             <code data-crate=\"serde\">serde 1.0.0</code>\n</h2>\n\
             <pre>other MIT text</pre>\n<!-- END-LICENSE -->\n\
             <!-- BEGIN-LICENSE Zlib -->",
        );
        assert!(page(&text).unwrap().contains("— 1 crate</li>"));
    }

    /// Two versions of one crate under MIT, and one of them under Zlib as
    /// well.
    const PAGE_TWICE: &str = "\
<ul>
  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> — 4 crates</li>
  <li data-license-id=\"Zlib\">Zlib <code>(Zlib)</code> — 2 crates</li>
</ul>
<!-- BEGIN-LICENSE MIT -->
<h2>
  <code data-crate=\"serde\">serde 1.0.0</code>
  <code data-crate=\"serde\">serde 2.0.0</code>
  <code data-crate=\"spate\">spate 0.1.0</code>
</h2>
<pre>MIT text</pre>
<!-- END-LICENSE -->
<!-- BEGIN-LICENSE Zlib -->
<h2>
  <code data-crate=\"serde\">serde 1.0.0</code>
  <code data-crate=\"spate-core\">spate-core 0.1.0</code>
</h2>
<pre>Zlib text</pre>
<!-- END-LICENSE -->
";

    /// The `PAGE_TWICE` table-of-contents row for `id`, or `<dropped>`.
    fn toc_row(id: &str) -> String {
        let page = page(PAGE_TWICE).unwrap();
        let marker = format!("data-license-id=\"{id}\"");
        page.lines()
            .find(|l| l.contains(&marker))
            .unwrap_or("<dropped>")
            .to_owned()
    }

    /// The count key is the chip, so two linked versions of one crate are two
    /// crates under that id.
    #[test]
    fn two_versions_of_one_crate_count_twice() {
        assert_eq!(
            toc_row("MIT"),
            "  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> — 2 crates</li>"
        );
    }

    /// The count key carries the license id, so a crate under two ids counts
    /// under each.
    #[test]
    fn a_crate_under_two_ids_counts_under_both() {
        assert_eq!(
            toc_row("Zlib"),
            "  <li data-license-id=\"Zlib\">Zlib <code>(Zlib)</code> — 1 crate</li>"
        );
    }

    /// The drop branch reads chips inside a section only, so a first-party chip
    /// outside every section reaches the page and the last check over it
    /// refuses.
    #[test]
    fn a_first_party_chip_surviving_outside_a_section_is_refused() {
        let text = format!("{PAGE}<code data-crate=\"spate\">spate 0.1.0</code>\n");
        assert_eq!(
            page(&text).unwrap_err().message,
            "first-party crate 'spate' survived into out.html"
        );
    }

    #[test]
    fn a_first_party_crate_the_page_never_names_is_refused() {
        let e = filter_page(PAGE, &names(&["spate", "spate-kafka"]), "out.html").unwrap_err();
        assert_eq!(
            e.message,
            "first-party crate 'spate-kafka' never appeared in the generated page"
        );
    }

    #[test]
    fn a_page_with_nothing_to_drop_is_refused() {
        let text = PAGE.replace("data-crate=\"spate\"", "data-crate=\"serde\"");
        let e = filter_page(&text, &names(&["spate-core"]), "out.html").unwrap_err();
        assert_eq!(
            e.message,
            "no first-party crate chip was dropped; the filter has gone blind"
        );
    }

    #[test]
    fn a_page_without_sentinels_is_refused() {
        let text = PAGE
            .replace("<!-- BEGIN-LICENSE MIT -->\n", "")
            .replace("<!-- BEGIN-LICENSE Zlib -->\n", "")
            .replace("<!-- END-LICENSE -->\n", "");
        assert_eq!(
            page(&text).unwrap_err().message,
            "no BEGIN-LICENSE sections; the template lost its sentinels"
        );
    }

    #[test]
    fn an_unterminated_section_is_refused() {
        let text = PAGE.replace("<pre>Zlib text</pre>\n<!-- END-LICENSE -->\n", "");
        assert_eq!(
            page(&text).unwrap_err().message,
            "unterminated BEGIN-LICENSE section"
        );
    }

    #[test]
    fn a_toc_row_without_a_count_is_refused() {
        let text = PAGE.replace("— 3 crates</li>", "</li>");
        assert_eq!(
            page(&text).unwrap_err().message,
            concat!(
                "a TOC row lost the count shape the rewrite expects: ",
                "  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> </li>"
            )
        );
    }

    /// An id with crates and no TOC row would thin the overview silently.
    #[test]
    fn a_missing_toc_row_is_refused() {
        let text = PAGE.replace(
            "  <li data-license-id=\"MIT\">MIT <code>(MIT)</code> — 3 crates</li>\n",
            "",
        );
        assert_eq!(
            page(&text).unwrap_err().message,
            "0 TOC rows kept for 1 license ids with crates"
        );
    }

    /// A chip after the last section is outside every sentinel pair, so it
    /// counts toward no id and keeps no section from being dropped.
    #[test]
    fn a_chip_outside_a_section_keeps_nothing_alive() {
        let text = format!("{PAGE}<code data-crate=\"anyhow\">anyhow 1.0.0</code>\n");
        let out = page(&text).unwrap();
        assert!(!out.contains("Zlib text"));
        assert!(out.contains("data-crate=\"anyhow\""));
    }

    #[test]
    fn a_chip_yields_its_crate_and_its_version() {
        let line = "  <code data-crate=\"serde\">serde 1.0.0</code>";
        assert_eq!(crate_of(line), "serde");
        assert_eq!(chip_of(line), "serde 1.0.0");
        assert_eq!(
            id_of("<li data-license-id=\"MIT\">x</li>", "data-license-id="),
            "MIT"
        );
    }

    /// A `\r` a license text carries is part of the line and reaches the page
    /// unchanged.
    #[test]
    fn a_carriage_return_is_not_a_line_terminator() {
        assert_eq!(records("a\r\nb\n"), ["a\r", "b"]);
        assert_eq!(records("a\n\n"), ["a", ""]);
        assert_eq!(records("a"), ["a"]);
        assert!(records("").is_empty());
        let text = PAGE.replace("<pre>MIT text</pre>", "<pre>MIT\r\ntext\r</pre>");
        assert!(page(&text).unwrap().contains("<pre>MIT\r\ntext\r</pre>"));
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

    #[test]
    fn only_the_first_count_in_a_row_is_rewritten() {
        assert_eq!(
            rewrite_count("<li>12 crates</li> 3 crates</li>", 7).as_deref(),
            Some("<li>7 crates</li> 3 crates</li>")
        );
        assert_eq!(rewrite_count("<li>crates</li>", 2), None);
    }
}
