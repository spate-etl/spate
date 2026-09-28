//! Holds a supported-versions table, and the images a page's code blocks name,
//! to the servers CI pins.
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
//! An image reference such as `nats:2.11-alpine` inside a fenced block is held
//! to the same set. A floating tag with no `<major>.<minor>` is not checked.
//!
//! No service or page is named here. A service opts in by listing its pages in
//! `ci/<service>/DOCS`, one path per line. Lanes listed in
//! `ci/<service>/UNSUPPORTED` pin a server the software refuses, and back no
//! claim.

use std::collections::BTreeSet;
use std::path::Path;

use crate::run::{Error, Outcome};

const HEADING: &str = "## Supported";

/// The file listing a service's pages.
const DOCS: &str = "DOCS";

/// The file listing lanes that back no support claim.
const UNSUPPORTED: &str = "UNSUPPORTED";

/// Resolves a `(service, lane)` pair to the `name:tag` it pins.
type Resolve<'a> = &'a dyn Fn(&str, &str) -> Result<String, Error>;

pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    let ci_root = root.join("ci");
    let pairs = pairs(&ci_root)?;
    if explain {
        for (service, page) in &pairs {
            println!(
                "(reads ci/{service}/*/Dockerfile and ci/{service}/{UNSUPPORTED} against {page})"
            );
        }
        return Ok(());
    }

    let resolve =
        |service: &str, lane: &str| crate::checks::container_image::tagged_for(root, service, lane);
    let mut failures = 0;
    for (service, page) in &pairs {
        if let Err(e) = check_page(root, &ci_root, service, page, &resolve) {
            eprintln!("supported-versions: {}", e.message);
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(Error::msg(format!("{failures} page(s) do not match")));
    }
    println!(
        "supported-versions: {} page(s) match the lines CI pins",
        pairs.len()
    );
    Ok(())
}

/// Every `(service, page)` pair a `DOCS` file declares. A service without one
/// is skipped.
fn pairs(ci_root: &Path) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    for service in services(ci_root)? {
        for page in entries(&ci_root.join(&service).join(DOCS)) {
            out.push((service.clone(), page));
        }
    }
    Ok(out)
}

/// The entries of a list file under `ci/<service>/`: one per line, with `#`
/// opening a comment and whitespace dropped. A missing file lists nothing.
fn entries(path: &Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .map(|line| line.split('#').next().unwrap_or_default())
        .map(|line| {
            line.chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>()
        })
        .filter(|entry| !entry.is_empty())
        .collect()
}

/// Every lane a service pins, in directory order.
fn lanes(ci_root: &Path, service: &str) -> Result<Vec<String>, Error> {
    services(&ci_root.join(service))
}

/// The lanes a support claim may rest on: every lane less those `UNSUPPORTED`
/// lists. An entry naming no lane is an error.
fn supported_lanes(ci_root: &Path, service: &str) -> Result<Vec<String>, Error> {
    let lanes = lanes(ci_root, service)?;
    let unsupported = entries(&ci_root.join(service).join(UNSUPPORTED));
    if let Some(stale) = unsupported.iter().find(|l| !lanes.contains(l)) {
        return Err(Error::msg(format!(
            "ci/{service}/{UNSUPPORTED} names {stale}, which is not a lane"
        )));
    }
    Ok(lanes
        .into_iter()
        .filter(|l| !unsupported.contains(l))
        .collect())
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
            "ci/{service}/{DOCS} names {page}, which does not exist"
        )));
    };
    let pinned = pinned(&supported_lanes(ci_root, service)?, service, resolve)?;
    verify(service, page, &text, &pinned)
}

/// The rule itself: the section exists, and every line the table or a fenced
/// image reference names is pinned. A lane that appears nowhere is allowed.
fn verify(service: &str, page: &str, text: &str, pinned: &Pinned) -> Result<(), Error> {
    if !text.lines().any(|l| l.starts_with(HEADING)) {
        return Err(Error::msg(format!(
            "{page}: no '{HEADING}' heading; the section moved or was renamed"
        )));
    }
    let lines = pinned.lines.iter().cloned().collect::<Vec<_>>().join(" ");
    let mut failures = Vec::new();
    let bad: Vec<_> = claimed_lines(text)
        .difference(&pinned.lines)
        .cloned()
        .collect();
    if !bad.is_empty() {
        failures.push(format!(
            "{page}: claims {}, which no supported lane of ci/{service} pins (pinned: {lines})\n  \
             Update the table, or the lanes under ci/{service}/.",
            bad.join(" "),
        ));
    }
    let bad: Vec<_> = image_references(text, &pinned.images)
        .into_iter()
        .filter(|(_, line)| !pinned.lines.contains(line))
        .map(|(reference, _)| reference)
        .collect();
    if !bad.is_empty() {
        failures.push(format!(
            "{page}: runs {}, whose line no supported lane of ci/{service} pins (pinned: {lines})\n  \
             Update the image tag, or the lanes under ci/{service}/.",
            bad.join(" "),
        ));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(Error::msg(failures.join("\n")))
    }
}

/// What a service's supported lanes pin.
struct Pinned {
    /// The image names, the `name` of each `name:tag`.
    images: BTreeSet<String>,
    /// The release lines, as `<major>.<minor>`. Two lanes on one line collapse.
    lines: BTreeSet<String>,
}

fn pinned(lanes: &[String], service: &str, resolve: Resolve<'_>) -> Result<Pinned, Error> {
    let mut out = Pinned {
        images: BTreeSet::new(),
        lines: BTreeSet::new(),
    };
    for lane in lanes {
        let reference = resolve(service, lane)?;
        let (image, tag) = reference.rsplit_once(':').unwrap_or((&reference, ""));
        out.images.insert(image.to_owned());
        out.lines.insert(major_minor(tag));
    }
    Ok(out)
}

/// The first two dot-separated fields of a tag.
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
        // A heading is tested as an opening only when no section is open, so a
        // second support heading closes the section and starts nothing.
        if inside && line.starts_with("## ") {
            inside = false;
            continue;
        }
        if !inside {
            inside = line.starts_with(HEADING);
            continue;
        }
        if let Some(claim) = row_line(line) {
            out.insert(claim);
        }
    }
    out
}

/// Every `<image>:<tag>` inside a fenced block whose image is one of `images`
/// and whose tag opens with `<major>.<minor>`, paired with that line.
///
/// A registry or namespace prefix is allowed, so `docker.io/library/nats:2.10`
/// is a `nats` reference and `mynats:2.10` is not.
fn image_references(page: &str, images: &BTreeSet<String>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut fenced = false;
    for line in page.lines() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if !fenced {
            continue;
        }
        for image in images {
            let prefix = format!("{image}:");
            for (at, _) in line.match_indices(&prefix) {
                let continues = line[..at]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || "._-".contains(c));
                if continues {
                    continue;
                }
                let rest = &line[at + prefix.len()..];
                let tag_len = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || "._-".contains(c)))
                    .unwrap_or(rest.len());
                let tag = &rest[..tag_len];
                if let Some(end) = leading_line(tag) {
                    out.push((format!("{prefix}{tag}"), tag[..end].to_owned()));
                }
            }
        }
    }
    out
}

/// The `<major>.<minor>` opening a table row's first cell, where a pipe follows
/// the number and the number is not part of a longer digit run. Only spaces
/// separate the opening pipe from the number.
fn row_line(line: &str) -> Option<String> {
    let rest = line.strip_prefix('|')?.trim_start_matches(' ');
    let end = leading_line(rest)?;
    rest[end..].contains('|').then(|| rest[..end].to_owned())
}

/// The length of the `<major>.<minor>` opening `text`: a digit run, one dot and
/// a digit run.
fn leading_line(text: &str) -> Option<usize> {
    let mut end = 0;
    let mut dot = false;
    let mut digits = 0;
    for (i, c) in text.char_indices() {
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
    let (major, minor) = text[..end].split_once('.')?;
    (!major.is_empty() && !minor.is_empty()).then_some(end)
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

    /// A resolver pinning `image` at the tag given for each lane, in the form
    /// the pinned-image resolver returns: `name:tag`, digest already stripped.
    fn resolver<'a>(
        image: &'a str,
        lanes: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str, &str) -> Result<String, Error> + 'a {
        move |_service: &str, lane: &str| {
            lanes
                .iter()
                .find(|(l, _)| *l == lane)
                .map(|(_, tag)| format!("{image}:{tag}"))
                .ok_or_else(|| Error::msg(format!("no such lane: {lane}")))
        }
    }

    /// What `image` pins at the lanes and tags given.
    fn pinned_image(image: &str, lanes: &[(&str, &str)]) -> Pinned {
        let names: Vec<String> = lanes.iter().map(|(l, _)| (*l).to_owned()).collect();
        pinned(&names, "db", &resolver(image, lanes)).unwrap()
    }

    fn pinned_db(lanes: &[(&str, &str)]) -> Pinned {
        pinned_image("vendor/db", lanes)
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
        let pinned = pinned_db(BASE);
        assert_eq!(pinned.lines, set(&["9.1", "9.4"]));
        assert_eq!(pinned.images, set(&["vendor/db"]));
    }

    /// Dependabot's move of the stable lane adds a line and removes none, so
    /// the subset rule leaves it alone.
    #[test]
    fn a_stable_lane_move_is_not_a_failure() {
        assert!(verify("db", "page.mdx", PAGE, &pinned_db(STABLE_MOVED)).is_ok());
    }

    /// Moving an LTS line takes away a number the table still names.
    #[test]
    fn an_lts_line_move_is_caught() {
        let e = verify("db", "page.mdx", PAGE, &pinned_db(LTS_MOVED)).unwrap_err();
        assert_eq!(
            e.message,
            "page.mdx: claims 9.4, which no supported lane of ci/db pins (pinned: 9.1 9.5 9.6)\n  \
             Update the table, or the lanes under ci/db/."
        );
    }

    #[test]
    fn a_renamed_section_is_caught() {
        let renamed = PAGE.replace("## Supported server versions", "## Versions");
        let e = verify("db", "page.mdx", &renamed, &pinned_db(BASE)).unwrap_err();
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

    /// The second heading ends the section it sits under and starts nothing, so
    /// the table below it is outside.
    #[test]
    fn a_second_support_heading_closes_the_section() {
        let page = "## Supported server versions\n## Supported, continued\n\n| 9.9 | x |\n";
        assert!(claimed_lines(page).is_empty());
    }

    #[test]
    fn only_a_space_separates_the_opening_pipe_from_the_number() {
        assert_eq!(row_line("|   9.4 | x |").as_deref(), Some("9.4"));
        assert_eq!(row_line("|\t9.4 | x |"), None);
        assert_eq!(row_line("|\u{a0}9.4 | x |"), None);
    }

    const NATS_PAGE: &str = "\
## Requirements

```yaml
services:
  nats:
    image: nats:2.11-alpine
    environment:
      NATS_URL: nats://nats:4222
```

## Supported server versions

| NATS | Support |
| --- | --- |
| 2.11 | Guaranteed |
";

    #[test]
    fn a_fenced_image_reference_is_read_as_its_line() {
        let images = set(&["nats"]);
        let page = "```sh\ndocker run nats:2.10.29-alpine -js\ndocker pull docker.io/library/nats:2.12\n```\n";
        assert_eq!(
            image_references(page, &images),
            vec![
                ("nats:2.10.29-alpine".to_owned(), "2.10".to_owned()),
                ("nats:2.12".to_owned(), "2.12".to_owned()),
            ]
        );
    }

    #[test]
    fn a_reference_naming_no_line_or_another_image_is_not_read() {
        let images = set(&["nats"]);
        let page = "```yaml\nurl: nats://nats:4222\nhost: nats:4222\nimage: mynats:2.10\nimage: nats:latest\nimage: nats:2-alpine\n```\n";
        assert!(image_references(page, &images).is_empty());
    }

    /// A fence inside a list item is indented, as in a numbered setup step.
    #[test]
    fn an_indented_fence_is_read() {
        let images = set(&["nats"]);
        let page = "1. Start a server:\n\n   ```sh\n   docker run nats:2.10-alpine -js\n   ```\n";
        assert_eq!(
            image_references(page, &images),
            vec![("nats:2.10-alpine".to_owned(), "2.10".to_owned())]
        );
    }

    #[test]
    fn an_image_reference_in_prose_is_not_read() {
        let images = set(&["nats"]);
        let page = "A server such as `nats:2.10-alpine` is refused.\n\n```yaml\nimage: nats:2.11\n```\n\nNor is nats:2.9.\n";
        assert_eq!(
            image_references(page, &images),
            vec![("nats:2.11".to_owned(), "2.11".to_owned())]
        );
    }

    #[test]
    fn a_stale_image_tag_is_caught() {
        let page = NATS_PAGE.replace("| 2.11 |", "| 2.12 |");
        let e = verify(
            "nats",
            "page.mdx",
            &page,
            &pinned_image("nats", &[("floor", "2.12.0-alpine")]),
        )
        .unwrap_err();
        assert_eq!(
            e.message,
            "page.mdx: runs nats:2.11-alpine, whose line no supported lane of ci/nats pins (pinned: 2.12)\n  \
             Update the image tag, or the lanes under ci/nats/."
        );
    }

    /// A directory under the system temporary directory, removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("spate-xtask-{}-{name}", std::process::id()));
            drop(std::fs::remove_dir_all(&dir));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn service(&self, name: &str) -> &Self {
            std::fs::create_dir_all(self.0.join(name)).unwrap();
            self
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }

    #[test]
    fn docs_pairs_skip_comments_and_blank_lines() {
        let scratch = Scratch::new("pairs");
        scratch.service("db");
        std::fs::write(
            scratch.0.join("db/DOCS"),
            "# a comment\n\n  docs/page.mdx  \ndocs/other.mdx # inline\n",
        )
        .unwrap();
        assert_eq!(
            pairs(&scratch.0).unwrap(),
            vec![
                ("db".to_owned(), "docs/page.mdx".to_owned()),
                ("db".to_owned(), "docs/other.mdx".to_owned()),
            ]
        );
    }

    /// Moving the floor moves the fixture below it onto the old floor's line.
    /// Excluding the fixture keeps that line from passing as supported.
    #[test]
    fn a_floor_move_with_its_fixture_following_is_caught() {
        let scratch = Scratch::new("floor-move");
        scratch
            .service("ci/nats/floor")
            .service("ci/nats/below-floor");
        std::fs::write(scratch.0.join("ci/nats/UNSUPPORTED"), "below-floor\n").unwrap();
        std::fs::write(scratch.0.join("page.mdx"), NATS_PAGE).unwrap();
        let ci_root = scratch.0.join("ci");
        let check = |lanes: &[(&str, &str)]| {
            check_page(
                &scratch.0,
                &ci_root,
                "nats",
                "page.mdx",
                &resolver("nats", lanes),
            )
        };

        assert!(
            check(&[
                ("floor", "2.11.17-alpine"),
                ("below-floor", "2.10.29-alpine")
            ])
            .is_ok()
        );
        let e = check(&[
            ("floor", "2.12.0-alpine"),
            ("below-floor", "2.11.17-alpine"),
        ])
        .unwrap_err();
        assert_eq!(
            e.message,
            "page.mdx: claims 2.11, which no supported lane of ci/nats pins (pinned: 2.12)\n  \
             Update the table, or the lanes under ci/nats/.\n\
             page.mdx: runs nats:2.11-alpine, whose line no supported lane of ci/nats pins (pinned: 2.12)\n  \
             Update the image tag, or the lanes under ci/nats/."
        );
    }

    #[test]
    fn an_unsupported_entry_naming_no_lane_is_an_error() {
        let scratch = Scratch::new("stale-unsupported");
        scratch.service("db/lts");
        std::fs::write(scratch.0.join("db/UNSUPPORTED"), "# fixtures\nold\n").unwrap();
        assert_eq!(
            supported_lanes(&scratch.0, "db").unwrap_err().message,
            "ci/db/UNSUPPORTED names old, which is not a lane"
        );
    }

    #[test]
    fn a_service_without_docs_is_skipped() {
        let scratch = Scratch::new("no-docs");
        scratch.service("db");
        assert!(pairs(&scratch.0).unwrap().is_empty());
    }
}
