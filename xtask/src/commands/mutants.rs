//! `cargo xtask mutants`: cargo-mutants over the diff against a base, with the
//! survivors written to the job summary on a CI runner.

use std::path::Path;

use crate::run::{self, Error, Outcome, Step, Streams};

/// Where the patch and cargo-mutants' `mutants.out` land, under the root. The
/// second half must match `output` in `.cargo/mutants.toml`.
const OUT: &str = "target/mutants";

/// Writes the diff against the merge base of `base` and `HEAD`, runs
/// cargo-mutants over it, and appends the survivors to `$GITHUB_STEP_SUMMARY`
/// when the runner sets it.
///
/// # Errors
///
/// When `base` shares no history with `HEAD`, when cargo-mutants is not
/// installed, and with cargo-mutants' own exit code when it finds a survivor,
/// a timeout or cannot run.
pub(super) fn run(root: &Path, explain: bool, base: &str, in_place: bool) -> Outcome {
    let out = root.join(OUT);
    let diff = out.join("pr.diff");
    let step = Step::new("cargo", mutants_args(&diff.to_string_lossy(), in_place));
    if explain {
        println!(
            "git diff --no-renames $(git merge-base {base} HEAD) > {}",
            diff.display()
        );
        return run::run(root, true, &step);
    }
    if !run::on_path("cargo-mutants") {
        return Err(Error::msg(
            "cargo-mutants is not installed: cargo install --locked cargo-mutants",
        ));
    }
    let patch = diff_since(root, base)?;
    let report = out.join("mutants.out");
    // A run that stops before testing leaves no report, and a stale one would
    // be summarised as this run's.
    match std::fs::remove_dir_all(&report) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(Error::msg(format!("{}: {e}", report.display())));
        }
        _ => {}
    }
    std::fs::create_dir_all(&out).map_err(|e| Error::msg(format!("{}: {e}", out.display())))?;
    std::fs::write(&diff, patch).map_err(|e| Error::msg(format!("{}: {e}", diff.display())))?;

    let done = run::complete(root, &step, Streams::Inherit)?;

    if let Ok(path) = std::env::var("GITHUB_STEP_SUMMARY") {
        let read = |name: &str| std::fs::read_to_string(report.join(name)).unwrap_or_default();
        let text = summary(done.code, &read("missed.txt"), &read("timeout.txt"));
        use std::io::Write;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| f.write_all(text.as_bytes()))
            .map_err(|e| Error::msg(format!("{path}: {e}")))?;
    }
    if done.code == 0 {
        Ok(())
    } else {
        Err(Error::status(done.code))
    }
}

/// The patch from the merge base of `base` and `HEAD` to the working tree:
/// commits, staged and unstaged edits. Untracked files are not in it.
fn diff_since(root: &Path, base: &str) -> Result<String, Error> {
    let merge_base = crate::ci::merge_base(root, base).ok_or_else(|| {
        Error::msg(format!(
            "{base} does not resolve or shares no history with HEAD"
        ))
    })?;
    run::capture(
        root,
        &Step::new("git", ["diff", "--no-renames", &merge_base]),
    )
}

/// The cargo arguments. Everything else cargo-mutants reads from
/// `.cargo/mutants.toml`, so a bare `cargo mutants` run agrees with this one.
fn mutants_args(diff: &str, in_place: bool) -> Vec<String> {
    let mut args = vec![
        "mutants".to_owned(),
        "--in-diff".to_owned(),
        diff.to_owned(),
    ];
    if in_place {
        args.push("--in-place".to_owned());
    }
    args
}

/// The job summary for a run that exited `code`, given the contents of
/// `missed.txt` and `timeout.txt`.
fn summary(code: i32, missed: &str, timeout: &str) -> String {
    let mut text = String::from("## Mutants over the diff\n\n");
    // 0, 2 and 3 are the codes of a run that tested every mutant.
    if ![0, 2, 3].contains(&code) {
        text.push_str(&format!(
            "The run did not finish: cargo-mutants exited {code}. The step log has the cause.\n"
        ));
        return text;
    }
    let lines = |s: &'static str, body: &str| {
        body.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| format!("- {s}`{l}`\n"))
            .collect::<String>()
    };
    let listed = lines("", missed) + &lines("timeout: ", timeout);
    if listed.is_empty() {
        text.push_str("No surviving mutants.\n");
    } else {
        text.push_str(&listed);
    }
    text.push_str(
        "\nThe run generates function, operator and return-value mutants. A swapped `match` \
         arm, a deleted early `return`, `break` or `continue`, and a constant moved across its \
         bound stay a review step.\n",
    );
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_arguments_name_the_diff_and_mutate_in_place_only_when_asked() {
        assert_eq!(
            mutants_args("d.diff", false),
            ["mutants", "--in-diff", "d.diff"]
        );
        assert_eq!(
            mutants_args("d.diff", true),
            ["mutants", "--in-diff", "d.diff", "--in-place"]
        );
    }

    #[test]
    fn a_run_with_no_survivors_says_so() {
        let text = summary(0, "", "\n");
        assert!(text.contains("No surviving mutants."), "{text}");
        assert!(!text.contains("- "), "{text}");
        assert!(text.contains("stay a review step"), "{text}");
    }

    #[test]
    fn each_survivor_and_timeout_is_listed() {
        let text = summary(
            2,
            "a.rs:1:1: replace f\nb.rs:2:2: replace g\n",
            "c.rs:3:3: replace h\n",
        );
        assert!(text.contains("- `a.rs:1:1: replace f`\n"), "{text}");
        assert!(text.contains("- `b.rs:2:2: replace g`\n"), "{text}");
        assert!(
            text.contains("- timeout: `c.rs:3:3: replace h`\n"),
            "{text}"
        );
        assert!(!text.contains("No surviving mutants."), "{text}");
        assert!(text.contains("stay a review step"), "{text}");
    }

    /// A baseline failure or a bad diff leaves no listing to trust, so the
    /// summary names the exit code and claims no clean run.
    #[test]
    fn a_run_that_did_not_finish_says_so_instead_of_listing() {
        for code in [1, 4, 5, 6, 70, 137] {
            let text = summary(code, "", "");
            assert!(text.contains(&format!("exited {code}")), "{text}");
            assert!(!text.contains("No surviving mutants."), "{text}");
        }
        assert!(!summary(3, "", "x").contains("did not finish"));
    }

    #[test]
    fn the_patch_holds_commits_and_staged_and_unstaged_edits() {
        let dir = std::env::temp_dir().join(format!("spate-mutants-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let write = |name: &str, text: &str| std::fs::write(dir.join(name), text).expect("write");
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .status()
                .expect("git")
                .success();
            assert!(ok, "git {args:?}");
        };
        git(&["init", "-q", "-b", "main"]);
        for name in ["committed", "staged", "unstaged"] {
            write(name, "base\n");
        }
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "base"]);
        git(&["checkout", "-q", "-b", "topic"]);
        write("committed", "committed-edit\n");
        git(&["commit", "-q", "-am", "topic"]);
        write("staged", "staged-edit\n");
        git(&["add", "staged"]);
        write("unstaged", "unstaged-edit\n");
        write("untracked", "untracked-edit\n");

        let patch = diff_since(&dir, "main").expect("a readable diff");
        for hunk in ["+committed-edit", "+staged-edit", "+unstaged-edit"] {
            assert!(patch.contains(hunk), "{hunk} missing from {patch}");
        }
        assert!(!patch.contains("untracked-edit"), "{patch}");
        assert!(diff_since(&dir, "no-such-ref").is_err());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }
}
