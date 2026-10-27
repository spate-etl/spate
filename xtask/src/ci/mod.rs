//! The `ci-changes` entry point and the four modules it drives.

pub(crate) mod classify;
mod event;
mod graph;
mod outputs;
mod scope;

use std::path::Path;

use event::Diff;
pub(crate) use scope::Scope;

use crate::checks::container_image;
use crate::repo_root;

/// Classifies the current event and writes the job selections to stdout and,
/// when the runner provides one, to `$GITHUB_OUTPUT`.
pub(crate) fn changes(args: &[String]) -> Result<(), String> {
    let root = repo_root()?;
    let graph = graph::Graph::load(&root)?;
    let lanes = extra_lanes(&root)?;
    let (ev, ctx) = event::from_environment();

    // An argument, so no exported variable can turn a real classification
    // into a synthetic one.
    let (ev, diff): (classify::Event, Box<dyn Diff>) = match args.first().map(String::as_str) {
        Some("--classify-paths") => {
            let file = args.get(1).ok_or("--classify-paths needs a file")?;
            let text = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
            // The list stands in for a diff, so it classifies as one whatever
            // event the environment names.
            (
                classify::Event::PullRequest,
                Box::new(event::PathList::from_nul_separated(&text)),
            )
        }
        Some(other) => return Err(format!("unknown option '{other}'")),
        None => (ev, Box::new(event::GitDiff::new(&root, ev))),
    };

    let (ev, paths, fell_back) = event::resolve(ev, diff.as_ref());
    if fell_back {
        println!("note: no usable diff; running everything.");
    }

    let mut out = classify::classify(&paths, ev, &ctx, &graph, &lanes);

    // Push mode force-runs every other job as the last line of defence, while
    // the packaging and floors gates select on their own diff.
    if std::env::var("EVENT_NAME").as_deref() == Ok("push") {
        match event::reach(diff.as_ref(), classify::is_manifest) {
            Some(reached) => {
                out.manifests = reached;
                out.advisories =
                    event::reach(diff.as_ref(), classify::is_advisory_input).unwrap_or(true);
            }
            None => {
                println!(
                    "note: no usable before..HEAD diff; the manifest and advisories gates fail closed."
                );
                out.manifests = true;
                out.advisories = true;
            }
        }
    }

    print!("{out}");
    if let Ok(path) = std::env::var("GITHUB_OUTPUT") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("{path}: {e}"))?;
        write!(f, "{out}").map_err(|e| format!("{path}: {e}"))?;
    }
    Ok(())
}

/// What a local run needs to cover for the changes since `base`, falling back
/// to everything when the diff cannot be read.
pub(crate) fn scope_since(root: &Path, base: &str) -> Result<Scope, String> {
    let git = event::GitDiff::new(root, classify::Event::PullRequest);
    let Some(paths) = git.since(base) else {
        return Ok(Scope::Full);
    };
    Ok(scope::scope(&paths, &graph::Graph::load(root)?))
}

/// The merge base of `base` and `HEAD`. `None` when `base` does not resolve or
/// shares no history with `HEAD`.
pub(crate) fn merge_base(root: &Path, base: &str) -> Option<String> {
    event::GitDiff::new(root, classify::Event::PullRequest).merge_base(base, "HEAD")
}

/// The ClickHouse server lanes and S3 store lanes needing a job beyond the
/// primary one, each S3 lane with its store's capabilities. The lanes live in
/// the repository's `ci/clickhouse/` and `ci/s3/`, so adding or repointing one
/// needs no edit here.
fn extra_lanes(root: &Path) -> Result<classify::ExtraLanes, String> {
    let clickhouse = container_image::extra_lanes(root, "clickhouse").map_err(|e| e.message)?;
    let s3 = container_image::extra_lanes(root, "s3")
        .map_err(|e| e.message)?
        .iter()
        .map(|lane| {
            container_image::lane_capabilities(root, "s3", lane)
                .map(|caps| outputs::StoreLane::new(lane, &caps))
                .map_err(|e| e.message)
        })
        .collect::<Result<_, _>>()?;
    Ok(classify::ExtraLanes { clickhouse, s3 })
}

#[cfg(test)]
mod tests {
    /// Each extra S3 lane carries its own capabilities into its filter, and
    /// the extra ClickHouse lanes come through beside them.
    #[test]
    fn extra_lanes_reads_each_s3_lane_with_its_capabilities() {
        use super::outputs::StoreLane;
        use crate::checks::container_image::Capability;

        let root =
            std::env::temp_dir().join(format!("spate-xtask-extra-lanes-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&root));
        let pin = |service: &str, lane: &str, tag: &str| {
            let dir = root.join("ci").join(service).join(lane);
            std::fs::create_dir_all(&dir).unwrap();
            let digest = "a".repeat(64);
            std::fs::write(
                dir.join("Dockerfile"),
                format!("FROM vendor/{service}:{tag}@sha256:{digest}\n"),
            )
            .unwrap();
        };
        pin("clickhouse", "lts", "2");
        pin("clickhouse", "old", "1");
        std::fs::write(root.join("ci/clickhouse/PRIMARY"), "lts\n").unwrap();
        pin("s3", "main", "1");
        pin("s3", "notifying", "2");
        pin("s3", "plain", "3");
        std::fs::write(root.join("ci/s3/PRIMARY"), "main\n").unwrap();
        std::fs::write(root.join("ci/s3/notifying/CAPABILITIES"), "notifications\n").unwrap();

        let lanes = super::extra_lanes(&root);
        drop(std::fs::remove_dir_all(&root));
        let lanes = lanes.unwrap();
        assert_eq!(lanes.clickhouse, ["old"]);
        assert_eq!(
            lanes.s3,
            [
                StoreLane::new("notifying", &[Capability::Notifications]),
                StoreLane::new("plain", &[]),
            ]
        );
    }

    /// The `rgw` lane declares `notifications`, so its leg runs the
    /// notification suite. A missing or misspelt `CAPABILITIES` file drops
    /// that suite from the leg and leaves it green.
    #[test]
    fn the_rgw_lane_runs_the_notification_suite() {
        use super::outputs::StoreLane;
        use crate::checks::container_image::Capability;

        let root = crate::repo_root().unwrap();
        let lanes = super::extra_lanes(&root).unwrap();
        assert!(
            lanes
                .s3
                .contains(&StoreLane::new("rgw", &[Capability::Notifications])),
            "{:?}",
            lanes.s3
        );
    }

    /// Each argument arm refuses what it does not define. The command line
    /// parser answers both shapes first, so neither refusal is reachable
    /// through the binary.
    #[test]
    fn an_argument_the_classifier_does_not_define_is_refused() {
        assert_eq!(
            super::changes(&["--classify-paths".to_owned()]),
            Err("--classify-paths needs a file".to_owned())
        );
        assert_eq!(
            super::changes(&["--paths".to_owned(), "list".to_owned()]),
            Err("unknown option '--paths'".to_owned())
        );
    }
}
