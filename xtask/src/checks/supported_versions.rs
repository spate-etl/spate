//! Holds a supported-versions table to the servers CI pins.
//!
//! A connector page states a support guarantee; `ci/<service>/` states what CI
//! runs. Every version a table names must be a release line the service still
//! pins, so the guarantee cannot outlive the lane under it.
//!
//! The rule is a subset. A lane may exist without appearing, which is what lets
//! a row read "Newest stable release" with no version in it, and it is why
//! moving the `stable` lane never fails here. Moving an LTS line removes the
//! number a table names, which is the case this catches.
//!
//! No service or page is named here. A service opts in by listing its pages in
//! `ci/<service>/DOCS`, one path per line.

use std::collections::BTreeSet;
use std::path::Path;

use crate::run::{self, Error, Outcome, Step};

const HEADING: &str = "## Supported";

/// Resolves a lane to its `name:tag`, so a test can supply the pins without a
/// tree of Dockerfiles.
type Resolve<'a> = &'a dyn Fn(&str, &str) -> Result<String, Error>;

pub(crate) fn check(root: &Path) -> Outcome {
    let ci_root = root.join("ci");
    let resolve = |service: &str, lane: &str| {
        run::capture(
            root,
            &Step::new("./scripts/container-image.sh", [service, lane]),
        )
        .map(|s| s.trim().to_owned())
    };

    let pairs = pairs(&ci_root)?;
    let mut failures = 0;
    for (service, page) in &pairs {
        if let Err(e) = check_page(root, &ci_root, service, page, &resolve) {
            eprintln!("supported-versions: {}", e.message);
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(Error::msg(format!("{failures} table(s) do not match")));
    }
    println!(
        "supported-versions: {} table(s) match the lines CI pins",
        pairs.len()
    );
    Ok(())
}

/// Every `(service, page)` pair a `DOCS` file declares. A service without one
/// is skipped.
fn pairs(ci_root: &Path) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    for service in services(ci_root)? {
        let docs = ci_root.join(&service).join("DOCS");
        let Ok(text) = std::fs::read_to_string(&docs) else {
            continue;
        };
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or_default();
            let page: String = line.chars().filter(|c| !c.is_whitespace()).collect();
            if !page.is_empty() {
                out.push((service.clone(), page));
            }
        }
    }
    Ok(out)
}

/// Every service with pinned images, in directory order.
fn services(ci_root: &Path) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(ci_root)
        .map_err(|e| Error::msg(format!("{}: {e}", ci_root.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::msg(format!("{}: {e}", ci_root.display())))?;
        if entry.path().is_dir() {
            out.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    out.sort();
    Ok(out)
}

fn check_page(
    root: &Path,
    ci_root: &Path,
    service: &str,
    page: &str,
    resolve: Resolve<'_>,
) -> Result<(), Error> {
    let path = root.join(page);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Err(Error::msg(format!(
            "ci/{service}/DOCS names {page}, which does not exist"
        )));
    };
    let pinned = pinned_lines(ci_root, service, resolve)?;
    verify(service, page, &text, &pinned)
}

/// The rule itself: the section exists, and every line the table names is
/// pinned. A lane that appears in no row is allowed, which is the subset rule.
fn verify(service: &str, page: &str, text: &str, pinned: &BTreeSet<String>) -> Result<(), Error> {
    if !text.lines().any(|l| l.starts_with(HEADING)) {
        return Err(Error::msg(format!(
            "{page}: no '{HEADING}' heading; the section moved or was renamed"
        )));
    }
    let bad: Vec<_> = claimed_lines(text).difference(pinned).cloned().collect();
    if !bad.is_empty() {
        return Err(Error::msg(format!(
            "{page}: claims {}, which ci/{service} no longer pins (pinned: {}).\n  \
             Update the table, or the lane under ci/{service}/.",
            bad.join(" "),
            pinned.iter().cloned().collect::<Vec<_>>().join(" ")
        )));
    }
    Ok(())
}

/// Every release line a service pins, as `<major>.<minor>`, across its lanes.
fn pinned_lines(
    ci_root: &Path,
    service: &str,
    resolve: Resolve<'_>,
) -> Result<BTreeSet<String>, Error> {
    let mut out = BTreeSet::new();
    for lane in services(&ci_root.join(service))? {
        let reference = resolve(service, &lane)?;
        let tag = reference.rsplit(':').next().unwrap_or_default();
        out.insert(major_minor(tag));
    }
    Ok(out)
}

/// The first two dot-separated fields, which is what `cut -d. -f1-2` gave.
fn major_minor(tag: &str) -> String {
    tag.split('.').take(2).collect::<Vec<_>>().join(".")
}

/// The release lines a table names: the leading `<major>.<minor>` of a row's
/// first cell, within the section under the support heading. A cell with no
/// number contributes nothing, so a row naming a moving target by description
/// is exempt.
fn claimed_lines(page: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut inside = false;
    for line in page.lines() {
        if line.starts_with("## ") || line.starts_with(HEADING) {
            inside = line.starts_with(HEADING);
            continue;
        }
        if let (true, Some(line)) = (inside, row_line(line)) {
            out.insert(line);
        }
    }
    out
}

/// The `<major>.<minor>` opening a table row's first cell, where the row has a
/// closing pipe and the number is not part of a longer digit run.
fn row_line(line: &str) -> Option<String> {
    let rest = line.strip_prefix('|')?.trim_start();
    let mut chars = rest.char_indices();
    let mut end = 0;
    let mut dot = false;
    let mut digits = 0;
    for (i, c) in chars.by_ref() {
        if c.is_ascii_digit() {
            digits += 1;
            end = i + 1;
        } else if c == '.' && !dot && digits > 0 {
            dot = true;
            end = i + 1;
        } else {
            break;
        }
    }
    let candidate = &rest[..end];
    let (major, minor) = candidate.split_once('.')?;
    if major.is_empty() || minor.is_empty() {
        return None;
    }
    // A later cell has to exist, which is what the trailing pipe in the
    // original expression required.
    if !rest[end..].contains('|') {
        return None;
    }
    Some(candidate.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = "\
## Supported server versions

| Vendor | Support |
| --- | --- |
| 9.4 LTS | Guaranteed |
| 9.1 LTS | Guaranteed |
| Newest stable release | Guaranteed |

## Something else

| 1.0 | not a version table |
";

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_numbered_row_is_claimed_and_a_described_row_is_not() {
        assert_eq!(claimed_lines(PAGE), set(&["9.1", "9.4"]));
    }

    #[test]
    fn a_table_under_another_heading_is_not_the_support_section() {
        assert!(!claimed_lines(PAGE).contains("1.0"));
    }

    #[test]
    fn a_patch_version_is_read_as_its_release_line() {
        assert_eq!(row_line("| 9.4.1.2 | x |").as_deref(), Some("9.4"));
        assert_eq!(major_minor("9.4.1.2"), "9.4");
    }

    #[test]
    fn a_row_with_no_second_cell_is_not_a_claim() {
        assert_eq!(row_line("| 9.4"), None);
    }

    #[test]
    fn a_row_whose_first_cell_opens_with_prose_is_not_a_claim() {
        assert_eq!(row_line("| Newest stable release | Guaranteed |"), None);
    }

    /// The lanes as `ci/<service>/<lane>/Dockerfile` would pin them.
    fn pinned(lanes: &[(&str, &str)]) -> BTreeSet<String> {
        lanes.iter().map(|(_, tag)| major_minor(tag)).collect()
    }

    const BASE: &[(&str, &str)] = &[
        ("lts", "9.4.1.2"),
        ("lts-previous", "9.1.7.3"),
        ("stable", "9.4.1.2"),
    ];
    const STABLE_MOVED: &[(&str, &str)] = &[
        ("lts", "9.4.1.2"),
        ("lts-previous", "9.1.7.3"),
        ("stable", "9.5.0.1"),
    ];
    const LTS_MOVED: &[(&str, &str)] = &[
        ("lts", "9.6.0.1"),
        ("lts-previous", "9.1.7.3"),
        ("stable", "9.5.0.1"),
    ];

    #[test]
    fn the_pinned_set_collapses_lanes_sharing_a_line() {
        assert_eq!(pinned(BASE), set(&["9.1", "9.4"]));
    }

    /// Dependabot's move of the stable lane adds a line and removes none, so
    /// the subset rule leaves it alone.
    #[test]
    fn a_stable_lane_move_is_not_a_failure() {
        assert!(verify("db", "page.mdx", PAGE, &pinned(STABLE_MOVED)).is_ok());
    }

    /// Moving an LTS line takes away a number the table still names.
    #[test]
    fn an_lts_line_move_is_caught() {
        let e = verify("db", "page.mdx", PAGE, &pinned(LTS_MOVED)).unwrap_err();
        assert!(
            e.message.starts_with(
                "page.mdx: claims 9.4, which ci/db no longer pins (pinned: 9.1 9.5 9.6)"
            ),
            "{}",
            e.message
        );
    }

    #[test]
    fn a_renamed_section_is_caught() {
        let renamed = PAGE.replace("## Supported server versions", "## Versions");
        let e = verify("db", "page.mdx", &renamed, &pinned(BASE)).unwrap_err();
        assert_eq!(
            e.message,
            "page.mdx: no '## Supported' heading; the section moved or was renamed"
        );
    }

    #[test]
    fn a_renamed_section_claims_nothing() {
        let renamed = PAGE.replace("## Supported server versions", "## Versions");
        assert!(claimed_lines(&renamed).is_empty());
    }
}
