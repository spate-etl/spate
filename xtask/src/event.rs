//! The event shape a run was started from, and the diff it implies.

use std::path::Path;
use std::process::Command;

use crate::classify::{Context, Event};

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
    let event = match env("EVENT_NAME").as_str() {
        "pull_request" => Event::PullRequest,
        "merge_group" => Event::MergeGroup,
        // push, schedule and workflow_dispatch: a push to main is the last line
        // of defence, so it runs everything.
        _ => Event::ForceAll,
    };
    let ctx = Context {
        // `github.actor` changes on a re-run, so the author comes from the
        // pull request itself.
        author: env("PR_AUTHOR"),
        labels: env("PR_LABELS")
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
            // Against the merge base rather than the base branch tip: `base.sha`
            // is the tip, so a two-dot diff also reports what main gained since
            // this branch last moved.
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
        .map(|paths| paths.iter().any(|p| crate::classify::is_manifest(p)))
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
