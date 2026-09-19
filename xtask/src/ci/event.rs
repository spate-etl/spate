//! The event shape a run was started from, and the diff it implies.

use std::path::Path;
use std::process::Command;

use super::classify::{Context, Event};

/// The changed paths a run should classify.
pub(crate) trait Diff {
    /// `None` means no usable diff, and the caller runs everything.
    fn changed_paths(&self) -> Option<Vec<String>>;
    /// The `before..HEAD` paths a push compares for manifest reach.
    fn push_paths(&self) -> Option<Vec<String>>;
}

/// Reads the environment GitHub Actions sets, and shells out to git.
#[derive(Debug)]
pub(crate) struct GitDiff<'a> {
    root: &'a Path,
    event: Event,
}

/// `github.event.before` on a branch creation.
const ZERO_SHA: &str = "0000000000000000000000000000000000000000";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

/// The event shape, and what the run knows about its pull request.
pub(crate) fn from_environment() -> (Event, Context) {
    // `github.actor` changes on a re-run, so the author comes from the pull
    // request itself.
    from_values(&env("EVENT_NAME"), &env("PR_AUTHOR"), &env("PR_LABELS"))
}

/// The event shape and the context one set of runner values names. An unset
/// variable arrives as an empty string.
fn from_values(event_name: &str, author: &str, labels: &str) -> (Event, Context) {
    let event = match event_name {
        "pull_request" => Event::PullRequest,
        "merge_group" => Event::MergeGroup,
        // push, schedule and workflow_dispatch: a push to main is the last line
        // of defence, so it runs everything.
        _ => Event::ForceAll,
    };
    let ctx = Context {
        author: author.to_string(),
        labels: labels
            .split(',')
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect(),
    };
    (event, ctx)
}

impl<'a> GitDiff<'a> {
    pub(crate) fn new(root: &'a Path, event: Event) -> Self {
        Self { root, event }
    }

    fn merge_base(&self, base: &str, head: &str) -> Option<String> {
        if base.is_empty() || head.is_empty() {
            return None;
        }
        let out = Command::new("git")
            .args(["merge-base", base, head])
            .current_dir(self.root)
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let base = String::from_utf8_lossy(&out.stdout).trim().to_string();
        (!base.is_empty()).then_some(base)
    }

    /// `--no-renames` because rename detection prints the destination alone, so
    /// a source file moved under `docs/` would read as a docs-only change.
    /// `-z` because `core.quotePath` C-quotes a non-ASCII path, which matches
    /// no pattern.
    fn name_only(&self, from: &str, to: &str) -> Option<Vec<String>> {
        let out = Command::new("git")
            .args([
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--name-only",
                "-z",
                "--no-renames",
                from,
                to,
            ])
            .current_dir(self.root)
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        Some(
            String::from_utf8_lossy(&out.stdout)
                .split('\0')
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }
}

impl Diff for GitDiff<'_> {
    fn changed_paths(&self) -> Option<Vec<String>> {
        match self.event {
            // Against the merge base. `base.sha` is the base branch tip, so a
            // two-dot diff also reports what main gained since this branch
            // last moved.
            Event::PullRequest => {
                let (base, head) = (env("BASE_SHA"), env("HEAD_SHA"));
                let merge_base = self.merge_base(&base, &head)?;
                self.name_only(&merge_base, &head)
            }
            // Through `merge-base` because nothing documents
            // `merge_group.base_sha` as an ancestor of `head_sha`.
            Event::MergeGroup => {
                let (base, head) = (env("MERGE_BASE_SHA"), env("MERGE_HEAD_SHA"));
                let merge_base = self.merge_base(&base, &head)?;
                self.name_only(&merge_base, &head)
            }
            Event::ForceAll => None,
        }
    }

    fn push_paths(&self) -> Option<Vec<String>> {
        let before = env("EVENT_BEFORE");
        if before.is_empty() || before == ZERO_SHA {
            return None;
        }
        self.name_only(&before, "HEAD")
    }
}

/// The event to classify under, and the paths to classify. A diff that cannot
/// be resolved falls back to running everything.
pub(crate) fn resolve(event: Event, diff: &dyn Diff) -> (Event, Vec<String>, bool) {
    match diff.changed_paths() {
        Some(paths) => (event, paths, false),
        None => (Event::ForceAll, Vec::new(), event != Event::ForceAll),
    }
}

/// Whether a push reaches a manifest. Push mode force-runs every other job, so
/// the packaging and floors gates select on their own diff.
pub(crate) fn manifest_reach(diff: &dyn Diff) -> Option<bool> {
    diff.push_paths()
        .map(|paths| paths.iter().any(|p| super::classify::is_manifest(p)))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake {
        changed: Option<Vec<String>>,
        push: Option<Vec<String>>,
    }

    impl Diff for Fake {
        fn changed_paths(&self) -> Option<Vec<String>> {
            self.changed.clone()
        }
        fn push_paths(&self) -> Option<Vec<String>> {
            self.push.clone()
        }
    }

    fn unresolvable() -> Fake {
        Fake {
            changed: None,
            push: None,
        }
    }

    /// The two events carrying a diff are the two that select one.
    #[test]
    fn the_two_events_with_a_diff_select_themselves() {
        assert_eq!(from_values("pull_request", "", "").0, Event::PullRequest);
        assert_eq!(from_values("merge_group", "", "").0, Event::MergeGroup);
    }

    /// Every other event name runs everything, an unset `EVENT_NAME` and a
    /// name this code has never heard of included.
    #[test]
    fn every_other_event_name_runs_everything() {
        for name in ["push", "schedule", "workflow_dispatch", "", "pull-request"] {
            assert_eq!(
                from_values(name, "", "").0,
                Event::ForceAll,
                "EVENT_NAME='{name}'"
            );
        }
    }

    /// The author the deferrals key on is the one `PR_AUTHOR` carries.
    #[test]
    fn the_author_reaches_the_context() {
        assert_eq!(
            from_values("pull_request", "dependabot[bot]", "").1.author,
            "dependabot[bot]"
        );
        assert_eq!(from_values("push", "", "").1.author, "");
    }

    /// `PR_LABELS` is one comma-separated line, and a label carries spaces.
    #[test]
    fn the_labels_split_on_commas_and_drop_the_empty_entries() {
        let labels = |text| from_values("pull_request", "", text).1.labels;
        assert_eq!(labels("ci: docker,ci: loom"), ["ci: docker", "ci: loom"]);
        assert_eq!(
            labels(" ci: docker , ci: loom "),
            ["ci: docker", "ci: loom"]
        );
        assert_eq!(labels(",, ,ci: bench,"), ["ci: bench"]);
        assert!(labels("").is_empty());
    }

    #[test]
    fn an_unresolvable_diff_runs_everything() {
        // A force push, a branch creation's zero SHA, a base sharing no
        // history and an unreachable `before` all reach this.
        let (event, paths, noted) = resolve(Event::PullRequest, &unresolvable());
        assert_eq!(event, Event::ForceAll);
        assert!(paths.is_empty());
        assert!(noted, "the fallback is reported");
    }

    #[test]
    fn a_merge_group_without_a_base_runs_everything() {
        let (event, _, _) = resolve(Event::MergeGroup, &unresolvable());
        assert_eq!(event, Event::ForceAll);
    }

    #[test]
    fn a_resolved_diff_keeps_its_event() {
        let fake = Fake {
            changed: Some(vec!["docs/a.md".into()]),
            push: None,
        };
        let (event, paths, noted) = resolve(Event::PullRequest, &fake);
        assert_eq!(event, Event::PullRequest);
        assert_eq!(paths, ["docs/a.md"]);
        assert!(!noted);
    }

    #[test]
    fn a_push_without_a_usable_before_fails_closed() {
        assert_eq!(manifest_reach(&unresolvable()), None);
    }

    #[test]
    fn a_push_reaching_a_manifest_is_seen() {
        let fake = Fake {
            changed: None,
            push: Some(vec!["crates/spate/Cargo.toml".into()]),
        };
        assert_eq!(manifest_reach(&fake), Some(true));
        let fake = Fake {
            changed: None,
            push: Some(vec!["docs/a.md".into()]),
        };
        assert_eq!(manifest_reach(&fake), Some(false));
    }
}

/// A path list given on the command line, for asserting what each arm selects.
#[derive(Debug)]
pub(crate) struct PathList(Vec<String>);

impl PathList {
    pub(crate) fn from_nul_separated(text: &str) -> Self {
        Self(
            text.split('\0')
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }
}

impl Diff for PathList {
    fn changed_paths(&self) -> Option<Vec<String>> {
        Some(self.0.clone())
    }
    fn push_paths(&self) -> Option<Vec<String>> {
        Some(self.0.clone())
    }
}
