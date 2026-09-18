//! The `ci-changes` entry point and the four modules it drives.

mod classify;
mod event;
mod graph;
mod outputs;

use std::path::Path;

use event::Diff;

use crate::repo_root;

/// Classifies the current event and writes the job selections to stdout and,
/// when the runner provides one, to `$GITHUB_OUTPUT`.
pub(crate) fn changes(args: &[String]) -> Result<(), String> {
    let root = repo_root()?;
    let graph = graph::Graph::load(&root)?;
    let lanes = extra_clickhouse_lanes(&root)?;
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
        out.manifests = event::manifest_reach(diff.as_ref()).unwrap_or_else(|| {
            println!("note: no usable before..HEAD diff; the manifest gate fails closed.");
            true
        });
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

/// The ClickHouse lanes needing a job beyond the primary one. The lane names
/// live in the repository's `ci/clickhouse/`, so adding or repointing one
/// needs no edit here.
fn extra_clickhouse_lanes(root: &Path) -> Result<Vec<String>, String> {
    crate::checks::container_image::extra_lanes(root, "clickhouse").map_err(|e| e.message)
}
