//! Installs the tracked git hooks under `.githooks/` for this clone.

use std::path::Path;

use crate::run::{self, Completed, Error, Outcome, Step, Streams};

/// The tracked hook directory, relative to the worktree root.
const HOOKS: &str = ".githooks";

/// Points `core.hooksPath` at `.githooks`. The setting lives in the shared
/// repository config, so it covers every worktree of the clone.
///
/// Refuses to replace a different `core.hooksPath`, and warns when the
/// default hook directory holds hooks that stop running.
pub(crate) fn install(root: &Path, explain: bool) -> Outcome {
    let current = git(root, &["config", "--get", "core.hooksPath"]);
    if current.as_deref() == Some(HOOKS) {
        println!("hooks: core.hooksPath is already {HOOKS}.");
        return Ok(());
    }
    if let Some(other) = current {
        return Err(Error::msg(format!(
            "core.hooksPath is already '{other}'. Unset it, or call {HOOKS}/commit-msg\n  \
             from the hooks there, and run this again."
        )));
    }

    let common = git(root, &["rev-parse", "--git-common-dir"])
        .ok_or_else(|| Error::msg("not inside a git repository"))?;
    let default = root.join(common).join("hooks");
    let live: Vec<String> = std::fs::read_dir(&default)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| !name.ends_with(".sample"))
                .collect()
        })
        .unwrap_or_default();

    run::run(
        root,
        explain,
        &Step::new("git", ["config", "core.hooksPath", HOOKS]),
    )?;
    if explain {
        return Ok(());
    }
    println!("hooks: core.hooksPath is {HOOKS}; a commit now checks its subject.");
    if !live.is_empty() {
        println!(
            "  These hooks in {} no longer run: {}",
            default.display(),
            live.join(" ")
        );
    }
    Ok(())
}

/// One `git` invocation's first line of stdout, or `None` where it failed or
/// answered with nothing.
fn git(root: &Path, args: &[&str]) -> Option<String> {
    let step = Step::new("git", args.iter().copied());
    let Ok(Completed { code: 0, stdout }) = run::complete(root, &step, Streams::Collect) else {
        return None;
    };
    let first = stdout.split('\n').next().unwrap_or_default();
    (!first.is_empty()).then(|| first.to_owned())
}
