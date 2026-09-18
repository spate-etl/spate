//! The digest-pinned image a service lane runs, and the pull that makes a
//! local tag resolve to those bytes.
//!
//! A lane is a directory under `ci/<service>/` holding a `Dockerfile` whose
//! single `FROM` carries an exact tag and the digest that tag resolves to.
//! Services and lanes are discovered from that tree, so adding either is a
//! change inside `ci/`.
//!
//! [`Mode::Pull`] fetches by digest and re-tags locally. testcontainers builds
//! its image reference as `name:tag` and has no digest form, so a run reaches
//! the pinned bytes through the local tag.

use std::path::{Path, PathBuf};

use crate::run::{self, Error, Outcome, Step};

/// The tree holding one directory per service.
const DIR: &str = "ci";

/// The file naming the lane a service runs when nothing selects one.
const PRIMARY: &str = "PRIMARY";

/// The digest marker a pinned reference carries.
const DIGEST: &str = "@sha256:";

/// How a resolved lane is reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// `name:tag`, the form testcontainers takes.
    Tagged,
    /// `name:tag@sha256:...`.
    Reference,
    /// Fetch by digest, re-tag locally, print the local `name:tag`.
    Pull,
}

/// Resolves one lane of one service, taking the selected lane when none is
/// named.
pub(crate) fn run(
    root: &Path,
    explain: bool,
    mode: Mode,
    service: &str,
    lane: Option<&str>,
) -> Outcome {
    let lane = requested_lane(root, service, lane)?;
    let reported = match mode {
        Mode::Tagged | Mode::Reference => {
            if explain {
                println!("(reads {})", manifest_name(service, &lane));
                return Ok(());
            }
            resolve(root, mode, service, &lane)?
        }
        Mode::Pull => pull_lane(root, explain, service, &lane)?,
    };
    if !explain {
        println!("{reported}");
    }
    Ok(())
}

/// The lane to resolve: the one named, or the service's selected lane when no
/// name is given.
fn requested_lane(root: &Path, service: &str, lane: Option<&str>) -> Result<String, Error> {
    match lane.and_then(selecting) {
        Some(lane) => Ok(lane.to_owned()),
        None => selected_lane(root, service),
    }
}

/// What a mode reports for a lane.
fn resolve(root: &Path, mode: Mode, service: &str, lane: &str) -> Result<String, Error> {
    let reference = reference_for(root, service, lane)?;
    Ok(match mode {
        Mode::Reference => reference,
        _ => tagged(&reference).to_owned(),
    })
}

/// Pulls every service's selected lane.
pub(crate) fn pull_all(root: &Path, explain: bool) -> Outcome {
    for service in services(root) {
        let lane = selected_lane(root, &service)?;
        if !explain {
            eprintln!("{service}: {lane}");
        }
        pull_lane(root, explain, &service, &lane)?;
    }
    Ok(())
}

/// Prints the lanes needing a CI job of their own, one per line.
pub(crate) fn print_extra_lanes(root: &Path, explain: bool, service: &str) -> Outcome {
    if explain {
        println!("(reads {DIR}/{service}/)");
        return Ok(());
    }
    for lane in extra_lanes(root, service)? {
        println!("{lane}");
    }
    Ok(())
}

/// The `name:tag` a lane pins.
pub(crate) fn tagged_for(root: &Path, service: &str, lane: &str) -> Result<String, Error> {
    reference_for(root, service, lane).map(|r| tagged(&r).to_owned())
}

/// The lanes of a service needing a CI job beyond the one covering its primary
/// lane: those resolving to a different image. The primary lane resolves to its
/// own reference, so it drops out here too.
///
/// A lane that does not resolve is reported and kept, so a broken pin reaches a
/// job rather than disappearing from the matrix.
pub(crate) fn extra_lanes(root: &Path, service: &str) -> Result<Vec<String>, Error> {
    let primary = primary_lane(root, service)?;
    let primary_ref = reference_for(root, service, &primary)?;
    let mut out = Vec::new();
    for lane in lanes(root, service) {
        let reference = reference_for(root, service, &lane).unwrap_or_else(|e| {
            eprintln!("container-image: {}", e.message);
            String::new()
        });
        if reference != primary_ref {
            out.push(lane);
        }
    }
    Ok(out)
}

/// Fetches a lane by digest, re-tags it locally, and returns the local
/// `name:tag`.
fn pull_lane(root: &Path, explain: bool, service: &str, lane: &str) -> Result<String, Error> {
    let reference = reference_for(root, service, lane)?;
    let local = tagged(&reference).to_owned();
    let by_digest = by_digest(&reference);
    let pull = Step::new("docker", ["pull", "--quiet", &by_digest]);
    let retag = Step::new("docker", ["tag", &by_digest, &local]);
    if explain {
        println!("{}", pull.display());
        println!("{}", retag.display());
        return Ok(local);
    }
    // docker's own line belongs on stderr, so stdout carries the reference
    // alone and a caller can pull and use the result in one invocation.
    eprint!("{}", run::capture(root, &pull)?);
    run::quiet(root, false, &retag)?;
    Ok(local)
}

/// The `name:tag@sha256:...` a lane pins, from its Dockerfile's first `FROM`.
///
/// An unpinned lane would pull whatever the tag points at today, so a reference
/// carrying no digest is rejected.
fn reference_for(root: &Path, service: &str, lane: &str) -> Result<String, Error> {
    let name = manifest_name(service, lane);
    let path = manifest_path(root, service, lane);
    if !path.is_file() {
        return Err(Error::msg(format!("no such lane: {name}")));
    }
    let text = std::fs::read_to_string(&path).map_err(|e| Error::msg(format!("{name}: {e}")))?;
    let reference = from_line(&text).filter(|r| !r.is_empty());
    let Some(reference) = reference else {
        return Err(Error::msg(format!("{name} has no FROM line")));
    };
    if !reference.contains(DIGEST) {
        return Err(Error::msg(format!(
            "{name} is not pinned by digest: {reference}"
        )));
    }
    Ok(reference.to_owned())
}

/// The image reference the first `FROM` names, which is the possibly empty run
/// of non-space characters after the keyword.
///
/// `FROM` is followed by at least one space, so a tab after the keyword is not
/// a `FROM` line and a tab inside the reference is part of it.
fn from_line(dockerfile: &str) -> Option<&str> {
    dockerfile.split('\n').find_map(|line| {
        let rest = line.strip_prefix("FROM ")?.trim_start_matches(' ');
        Some(rest.split(' ').next().unwrap_or_default())
    })
}

/// The `name:tag` half of a reference, cut at the first digest marker.
fn tagged(reference: &str) -> &str {
    reference
        .split_once(DIGEST)
        .map_or(reference, |(tagged, _)| tagged)
}

/// The `name@sha256:...` form docker pulls, with the tag dropped and the last
/// digest kept.
///
/// The tag is cut at the last colon, so the registry port in
/// `localhost:5000/db:1.2` survives.
fn by_digest(reference: &str) -> String {
    let tagged = tagged(reference);
    let name = tagged.rsplit_once(':').map_or(tagged, |(name, _)| name);
    let digest = reference
        .rsplit_once(DIGEST)
        .map_or(reference, |(_, digest)| digest);
    format!("{name}{DIGEST}{digest}")
}

/// The lane selected for a service: `SPATE_<SERVICE>_LANE`, else its primary.
fn selected_lane(root: &Path, service: &str) -> Result<String, Error> {
    match std::env::var(lane_var(service))
        .ok()
        .as_deref()
        .and_then(from_env)
    {
        Some(lane) => Ok(lane.to_owned()),
        None => primary_lane(root, service),
    }
}

/// The variable naming a service's lane.
fn lane_var(service: &str) -> String {
    format!(
        "SPATE_{}_LANE",
        service.to_ascii_uppercase().replace('-', "_")
    )
}

/// The lane an environment value names. Trailing newlines are not part of it.
fn from_env(value: &str) -> Option<&str> {
    selecting(value.trim_end_matches('\n'))
}

/// A name that selects a lane. The empty name selects none.
fn selecting(name: &str) -> Option<&str> {
    (!name.is_empty()).then_some(name)
}

/// The lane `ci/<service>/PRIMARY` names: its first line, trailing whitespace
/// dropped.
fn primary_lane(root: &Path, service: &str) -> Result<String, Error> {
    let name = format!("{DIR}/{service}/{PRIMARY}");
    let path = root.join(DIR).join(service).join(PRIMARY);
    if !path.is_file() {
        return Err(Error::msg(format!(
            "{name} is missing; every service declares a primary lane"
        )));
    }
    let text = std::fs::read_to_string(&path).map_err(|e| Error::msg(format!("{name}: {e}")))?;
    let lane = text
        .split('\n')
        .next()
        .unwrap_or_default()
        .trim_end_matches(|c: char| c.is_whitespace());
    if lane.is_empty() {
        return Err(Error::msg(format!("{name} is empty")));
    }
    Ok(lane.to_owned())
}

/// Every service with pinned images, sorted.
fn services(root: &Path) -> Vec<String> {
    subdirectories(&root.join(DIR))
}

/// Every lane of a service, sorted.
fn lanes(root: &Path, service: &str) -> Vec<String> {
    subdirectories(&root.join(DIR).join(service))
}

/// The directory names under a path, sorted, skipping files and dotted names.
/// A path that cannot be read holds nothing.
fn subdirectories(path: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.starts_with('.'))
        .collect();
    out.sort();
    out
}

fn manifest_name(service: &str, lane: &str) -> String {
    format!("{DIR}/{service}/{lane}/Dockerfile")
}

fn manifest_path(root: &Path, service: &str, lane: &str) -> PathBuf {
    root.join(DIR).join(service).join(lane).join("Dockerfile")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── The FROM line ──────────────────────────────────────────────────

    #[test]
    fn the_reference_is_the_first_from_lines_first_word() {
        assert_eq!(
            from_line("# a comment\n\nFROM vendor/db:9.4 AS build\nFROM other:1\n"),
            Some("vendor/db:9.4")
        );
    }

    #[test]
    fn a_run_of_spaces_after_the_keyword_is_not_part_of_the_reference() {
        assert_eq!(from_line("FROM    vendor/db:9.4\n"), Some("vendor/db:9.4"));
    }

    /// The keyword is followed by a space, so neither a tab after it nor a
    /// lower-case spelling nor an indented line opens a reference.
    #[test]
    fn only_a_space_separates_the_keyword_from_the_reference() {
        assert_eq!(from_line("FROM\tvendor/db:9.4\n"), None);
        assert_eq!(from_line("from vendor/db:9.4\n"), None);
        assert_eq!(from_line("  FROM vendor/db:9.4\n"), None);
        assert_eq!(from_line("FROMvendor/db:9.4\n"), None);
    }

    /// Only a space ends the reference, so a tab inside one is part of it and
    /// the lane is reported as unpinned rather than silently truncated.
    #[test]
    fn a_tab_inside_the_reference_is_part_of_it() {
        assert_eq!(from_line("FROM a\tb c\n"), Some("a\tb"));
    }

    #[test]
    fn a_keyword_with_no_reference_after_it_yields_an_empty_one() {
        assert_eq!(from_line("FROM \n"), Some(""));
        assert_eq!(from_line("FROM  \n"), Some(""));
    }

    #[test]
    fn a_file_with_no_from_line_yields_nothing() {
        assert_eq!(from_line("# nothing here\n"), None);
    }

    #[test]
    fn a_carriage_return_stays_in_the_reference() {
        assert_eq!(from_line("FROM vendor/db:9.4\r\n"), Some("vendor/db:9.4\r"));
    }

    // ── Splitting a reference ──────────────────────────────────────────

    #[test]
    fn the_tagged_half_is_cut_at_the_first_digest_marker() {
        assert_eq!(tagged("vendor/db:9.4@sha256:aa@sha256:bb"), "vendor/db:9.4");
        assert_eq!(tagged("vendor/db:9.4"), "vendor/db:9.4");
    }

    #[test]
    fn the_pull_form_drops_the_tag_and_keeps_the_last_digest() {
        assert_eq!(
            by_digest("vendor/db:9.4@sha256:aa@sha256:bb"),
            "vendor/db@sha256:bb"
        );
    }

    /// The tag is cut at the last colon, so a registry port survives.
    #[test]
    fn a_registry_port_is_not_mistaken_for_a_tag() {
        assert_eq!(
            by_digest("localhost:5000/db:1.2@sha256:aa"),
            "localhost:5000/db@sha256:aa"
        );
    }

    /// A reference pinned by digest alone carries no tag to drop.
    #[test]
    fn a_reference_with_no_tag_keeps_its_whole_name() {
        assert_eq!(by_digest("vendor/db@sha256:aa"), "vendor/db@sha256:aa");
        assert_eq!(tagged("vendor/db@sha256:aa"), "vendor/db");
    }

    // ── Lane selection ─────────────────────────────────────────────────

    #[test]
    fn the_variable_name_upper_cases_the_service_and_replaces_hyphens() {
        assert_eq!(lane_var("clickhouse"), "SPATE_CLICKHOUSE_LANE");
        assert_eq!(lane_var("object-store"), "SPATE_OBJECT_STORE_LANE");
    }

    #[test]
    fn an_environment_value_loses_its_trailing_newlines() {
        assert_eq!(from_env("old\n\n"), Some("old"));
        assert_eq!(from_env("\n"), None);
    }

    #[test]
    fn an_empty_name_selects_no_lane() {
        assert_eq!(selecting(""), None);
        assert_eq!(selecting("lts"), Some("lts"));
    }

    // ── Against a fixture tree ─────────────────────────────────────────

    /// A directory under the system temporary directory, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        /// One fictional service `db` with three lanes, and a second service
        /// whose only lane carries no digest.
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("spate-xtask-image-{}-{name}", std::process::id()));
            drop(std::fs::remove_dir_all(&dir));
            let me = Self(dir);
            let digest = "a".repeat(64);
            me.primary("db", "vendor-main\n");
            me.lane(
                "db",
                "vendor-main",
                &format!("FROM vendor/db:9.4.1.2@sha256:{digest}\n"),
            );
            me.lane(
                "db",
                "old",
                &format!("FROM vendor/db:9.1.7.3@sha256:{digest}\n"),
            );
            // Byte-identical to the primary lane, which is where a `stable`
            // lane sits whenever the newest release is also the newest LTS.
            me.lane(
                "db",
                "tracking",
                &format!("FROM vendor/db:9.4.1.2@sha256:{digest}\n"),
            );
            me.primary("bad", "unpinned\n");
            me.lane("bad", "unpinned", "FROM vendor/db:9.4\n");
            me
        }

        fn root(&self) -> &Path {
            &self.0
        }

        fn primary(&self, service: &str, text: &str) -> &Self {
            let dir = self.0.join(DIR).join(service);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(PRIMARY), text).unwrap();
            self
        }

        fn lane(&self, service: &str, lane: &str, text: &str) -> &Self {
            let dir = self.0.join(DIR).join(service).join(lane);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("Dockerfile"), text).unwrap();
            self
        }

        fn file(&self, relative: &str, text: &str) -> &Self {
            let path = self.0.join(DIR).join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
            self
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }

    #[test]
    fn the_primary_lane_comes_from_the_primary_file() {
        let s = Scratch::new("primary");
        assert_eq!(primary_lane(s.root(), "db").unwrap(), "vendor-main");
    }

    /// The first line only, and trailing whitespace is not part of the name.
    #[test]
    fn the_primary_file_is_read_as_one_trimmed_line() {
        let s = Scratch::new("primary-trim");
        s.primary("db", "  old \t\nignored\n");
        assert_eq!(primary_lane(s.root(), "db").unwrap(), "  old");
    }

    #[test]
    fn a_missing_primary_file_is_rejected() {
        let s = Scratch::new("primary-missing");
        assert_eq!(
            primary_lane(s.root(), "nope").unwrap_err().message,
            "ci/nope/PRIMARY is missing; every service declares a primary lane"
        );
    }

    #[test]
    fn an_empty_primary_file_is_rejected() {
        let s = Scratch::new("primary-empty");
        s.primary("db", "\n \n");
        assert_eq!(
            primary_lane(s.root(), "db").unwrap_err().message,
            "ci/db/PRIMARY is empty"
        );
    }

    #[test]
    fn an_unset_variable_falls_back_to_the_primary_lane() {
        let s = Scratch::new("selected");
        assert_eq!(selected_lane(s.root(), "db").unwrap(), "vendor-main");
    }

    #[test]
    fn a_lane_resolves_to_its_reference_and_its_tagged_half() {
        let s = Scratch::new("resolve");
        let digest = "a".repeat(64);
        assert_eq!(
            reference_for(s.root(), "db", "old").unwrap(),
            format!("vendor/db:9.1.7.3@sha256:{digest}")
        );
        assert_eq!(
            tagged_for(s.root(), "db", "vendor-main").unwrap(),
            "vendor/db:9.4.1.2"
        );
    }

    /// The reference mode keeps the digest and every other mode drops it.
    #[test]
    fn each_mode_reports_its_own_half_of_the_reference() {
        let s = Scratch::new("modes");
        let digest = "a".repeat(64);
        assert_eq!(
            resolve(s.root(), Mode::Reference, "db", "old").unwrap(),
            format!("vendor/db:9.1.7.3@sha256:{digest}")
        );
        for mode in [Mode::Tagged, Mode::Pull] {
            assert_eq!(
                resolve(s.root(), mode, "db", "old").unwrap(),
                "vendor/db:9.1.7.3"
            );
        }
    }

    #[test]
    fn an_unknown_lane_is_rejected() {
        let s = Scratch::new("unknown-lane");
        assert_eq!(
            reference_for(s.root(), "db", "nope").unwrap_err().message,
            "no such lane: ci/db/nope/Dockerfile"
        );
    }

    #[test]
    fn a_lane_that_is_a_file_is_not_a_lane() {
        let s = Scratch::new("lane-is-file");
        s.file("db/notes", "FROM vendor/db:9.4@sha256:aa\n");
        assert!(!lanes(s.root(), "db").contains(&"notes".to_owned()));
        assert_eq!(
            reference_for(s.root(), "db", "notes").unwrap_err().message,
            "no such lane: ci/db/notes/Dockerfile"
        );
    }

    #[test]
    fn a_lane_with_no_digest_is_rejected() {
        let s = Scratch::new("unpinned");
        assert_eq!(
            reference_for(s.root(), "bad", "unpinned")
                .unwrap_err()
                .message,
            "ci/bad/unpinned/Dockerfile is not pinned by digest: vendor/db:9.4"
        );
    }

    #[test]
    fn a_manifest_with_no_from_line_is_rejected() {
        let s = Scratch::new("no-from");
        s.lane("db", "blank", "# nothing\n");
        s.lane("db", "bare", "FROM \n");
        for lane in ["blank", "bare"] {
            assert_eq!(
                reference_for(s.root(), "db", lane).unwrap_err().message,
                format!("ci/db/{lane}/Dockerfile has no FROM line")
            );
        }
    }

    /// A lane pinned by digest with no tag resolves, and its name is the whole
    /// tagged half.
    #[test]
    fn a_lane_pinned_without_a_tag_resolves() {
        let s = Scratch::new("no-tag");
        s.lane("db", "digest-only", "FROM vendor/db@sha256:aa\n");
        assert_eq!(
            tagged_for(s.root(), "db", "digest-only").unwrap(),
            "vendor/db"
        );
    }

    #[test]
    fn the_services_and_lanes_are_the_sorted_directories() {
        let s = Scratch::new("discovery");
        s.file("db/DOCS", "docs/page.mdx\n");
        assert_eq!(services(s.root()), vec!["bad", "db"]);
        assert_eq!(
            lanes(s.root(), "db"),
            vec!["old", "tracking", "vendor-main"]
        );
    }

    /// A dotted directory is not a lane, so a scratch or editor directory
    /// beside the lanes never reaches the matrix.
    #[test]
    fn a_dotted_directory_is_not_a_lane() {
        let s = Scratch::new("dotted");
        s.lane("db", ".scratch", "FROM vendor/db:9.9@sha256:bb\n");
        assert_eq!(
            lanes(s.root(), "db"),
            vec!["old", "tracking", "vendor-main"]
        );
        assert_eq!(extra_lanes(s.root(), "db").unwrap(), vec!["old"]);
    }

    /// An empty lane name selects nothing, so it falls back the same way an
    /// absent one does.
    #[test]
    fn a_named_lane_wins_and_an_empty_name_falls_back() {
        let s = Scratch::new("requested");
        assert_eq!(requested_lane(s.root(), "db", Some("old")).unwrap(), "old");
        assert_eq!(
            requested_lane(s.root(), "db", Some("")).unwrap(),
            "vendor-main"
        );
        assert_eq!(requested_lane(s.root(), "db", None).unwrap(), "vendor-main");
    }

    #[test]
    fn a_service_with_no_lanes_has_none() {
        let s = Scratch::new("no-lanes");
        s.primary("empty", "only\n");
        assert!(lanes(s.root(), "empty").is_empty());
        assert!(lanes(s.root(), "absent").is_empty());
    }

    /// Only a lane whose image differs from the primary one needs a job.
    #[test]
    fn the_extra_lanes_drop_the_primary_and_anything_matching_it() {
        let s = Scratch::new("extra");
        assert_eq!(extra_lanes(s.root(), "db").unwrap(), vec!["old"]);
    }

    /// A lane that does not resolve differs from the primary reference, so it
    /// stays in the matrix.
    #[test]
    fn a_lane_that_does_not_resolve_is_still_an_extra_lane() {
        let s = Scratch::new("extra-broken");
        s.lane("db", "broken", "# no FROM\n");
        assert_eq!(extra_lanes(s.root(), "db").unwrap(), vec!["broken", "old"]);
    }

    #[test]
    fn extra_lanes_of_an_unknown_service_is_rejected() {
        let s = Scratch::new("extra-unknown");
        assert_eq!(
            extra_lanes(s.root(), "nope").unwrap_err().message,
            "ci/nope/PRIMARY is missing; every service declares a primary lane"
        );
    }

    /// A primary lane naming no directory fails before any lane is walked.
    #[test]
    fn a_primary_naming_no_lane_is_rejected() {
        let s = Scratch::new("primary-not-a-lane");
        s.primary("db", "ghost\n");
        assert_eq!(
            extra_lanes(s.root(), "db").unwrap_err().message,
            "no such lane: ci/db/ghost/Dockerfile"
        );
    }

    // ── Against the lanes this repository ships ────────────────────────

    /// Every shipped lane parses and carries a digest of 64 lower-case hex
    /// characters. Discovered, so a new service or lane is covered on arrival.
    #[test]
    fn every_shipped_lane_is_pinned_by_a_well_formed_digest() {
        let root = crate::repo_root().unwrap();
        let mut checked = 0;
        for service in services(&root) {
            for lane in lanes(&root, &service) {
                let reference = reference_for(&root, &service, &lane).unwrap();
                let digest = reference.rsplit_once(DIGEST).unwrap().1;
                assert_eq!(digest.len(), 64, "{service}/{lane}: {digest}");
                assert!(
                    digest
                        .bytes()
                        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
                    "{service}/{lane}: {digest}"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "no service pins a lane");
    }

    /// Every shipped service declares a primary lane, and that lane exists.
    #[test]
    fn every_shipped_service_has_a_primary_lane() {
        let root = crate::repo_root().unwrap();
        for service in services(&root) {
            let primary = primary_lane(&root, &service).unwrap();
            assert!(
                lanes(&root, &service).contains(&primary),
                "{service}: PRIMARY names '{primary}', which is not a lane"
            );
        }
    }

    /// The primary lane is covered by the job every service already gets, so it
    /// never needs one of its own.
    #[test]
    fn no_shipped_primary_lane_is_an_extra_lane() {
        let root = crate::repo_root().unwrap();
        for service in services(&root) {
            let primary = primary_lane(&root, &service).unwrap();
            assert!(
                !extra_lanes(&root, &service).unwrap().contains(&primary),
                "{service}: the primary lane '{primary}' is in the extra lanes"
            );
        }
    }
}
