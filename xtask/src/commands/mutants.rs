//! `cargo xtask mutants`: cargo-mutants over the diff against a base, with the
//! survivors written to the job summary on a CI runner.

use std::path::Path;

use crate::run::{self, Error, Outcome, Step, Streams};

/// Where the patch and cargo-mutants' `mutants.out` land, under the root. The
/// second half must match `output` in `.cargo/mutants.toml`.
const OUT: &str = "target/mutants";

const HEADING: &str = "## Mutants over the diff\n\n";

/// Writes the diff against the merge base of `base` and `HEAD`, runs
/// cargo-mutants over it, and appends the survivors to `$GITHUB_STEP_SUMMARY`
/// when the runner sets it.
///
/// # Errors
///
/// When `base` shares no history with `HEAD`, and with cargo-mutants' own exit
/// code when it finds a survivor, a timeout or cannot run.
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
    let dir = out.join("mutants.out");
    let ran = (|| {
        let patch = diff_since(root, base)?;
        clear_report(&dir)?;
        std::fs::create_dir_all(&out).map_err(|e| Error::msg(format!("{}: {e}", out.display())))?;
        std::fs::write(&diff, patch).map_err(|e| Error::msg(format!("{}: {e}", diff.display())))?;
        Ok(run::complete(root, &step, Streams::Inherit)?.code)
    })();
    let summary_path = std::env::var_os("GITHUB_STEP_SUMMARY");
    finish(ran, &dir, summary_path.as_deref().map(Path::new))
}

/// Removes the previous run's report. A run that stops before testing writes
/// none, and a stale one would be summarised as this run's.
fn clear_report(dir: &Path) -> Outcome {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(Error::msg(format!("{}: {e}", dir.display())))
        }
        _ => Ok(()),
    }
}

/// Appends the summary to `summary_path`, when given, and turns the run into
/// the outcome: cargo-mutants' exit code from the report in `dir`, or the
/// error that stopped the run before cargo-mutants finished.
fn finish(ran: Result<i32, Error>, dir: &Path, summary_path: Option<&Path>) -> Outcome {
    let text = match &ran {
        Ok(code) => {
            let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap_or_default();
            summary(*code, &read("missed.txt"), &read("timeout.txt"))
        }
        Err(e) => unfinished(&e.message),
    };
    if let Some(path) = summary_path {
        use std::io::Write;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| f.write_all(text.as_bytes()))
            .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
    }
    match ran? {
        0 => Ok(()),
        code => Err(Error::status(code)),
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
    // cargo-mutants reads the paths after a `b/` prefix, so a user's prefix,
    // color or external-diff settings would leave it matching nothing.
    let args = [
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-color",
        "--src-prefix=a/",
        "--dst-prefix=b/",
        "--no-renames",
        &merge_base,
    ];
    run::capture(root, &Step::new("git", args))
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
    // 0, 2 and 3 are the codes of a run that tested every mutant.
    if ![0, 2, 3].contains(&code) {
        return unfinished(&format!("cargo-mutants exited {code}"));
    }
    let mut text = String::from(HEADING);
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
        "\ncargo-mutants replaces function bodies, operators and match guards, and deletes \
         match arms. It does not swap a `match` arm, delete an early `return`, `break` or \
         `continue`, or move a constant across its bound; those stay a review step.\n",
    );
    text
}

/// The job summary for a run that stopped before testing every mutant.
fn unfinished(cause: &str) -> String {
    format!("{HEADING}The run did not finish: {cause}. The step log has the detail.\n")
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

    /// A directory of this test's own, removed with its contents.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("spate-mutants-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// A previous report is removed, a missing one is not an error, and any
    /// other failure to remove it is.
    #[test]
    fn clearing_removes_a_report_and_refuses_what_it_cannot_remove() {
        let dir = scratch("clear");
        let report = dir.join("mutants.out");
        std::fs::create_dir_all(&report).expect("report dir");
        std::fs::write(report.join("missed.txt"), "stale\n").expect("write");
        assert!(clear_report(&report).is_ok());
        assert!(!report.exists());
        assert!(
            clear_report(&report).is_ok(),
            "a missing report is not an error"
        );
        let file = dir.join("a-file");
        std::fs::write(&file, "").expect("write");
        assert!(clear_report(&file.join("child")).is_err());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    /// The summary is appended after what the file already holds, and the
    /// exit code passes through unchanged.
    #[test]
    fn the_report_appends_the_summary_and_keeps_the_exit_code() {
        let dir = scratch("report");
        std::fs::write(dir.join("missed.txt"), "a.rs:1:1: replace f\n").expect("write");
        let path = dir.join("summary.md");
        std::fs::write(&path, "earlier\n").expect("write");

        let err = finish(Ok(2), &dir, Some(&path)).expect_err("survivors exit 2");
        assert_eq!(err.code, Some(2));
        let text = std::fs::read_to_string(&path).expect("summary");
        assert!(
            text.starts_with("earlier\n## Mutants over the diff\n"),
            "{text}"
        );
        assert!(text.contains("- `a.rs:1:1: replace f`"), "{text}");

        assert!(finish(Ok(0), &dir, None).is_ok());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    /// A run stopped before cargo-mutants finished still writes a summary,
    /// naming the cause, and returns the same error.
    #[test]
    fn a_failure_before_the_run_is_summarised() {
        let dir = scratch("early");
        let path = dir.join("summary.md");
        let err = finish(Err(Error::msg("main does not resolve")), &dir, Some(&path))
            .expect_err("the error passes through");
        assert_eq!(err.message, "main does not resolve");
        let text = std::fs::read_to_string(&path).expect("summary");
        assert!(
            text.contains("did not finish: main does not resolve"),
            "{text}"
        );
        assert!(!text.contains("No surviving mutants."), "{text}");
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    /// cargo-mutants writes its report under `output`, and `run` reads it
    /// under `OUT`; a drift would summarise every run as clean.
    #[test]
    fn the_report_directory_is_where_the_config_writes_it() {
        let config = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.cargo/mutants.toml");
        let text = std::fs::read_to_string(&config).expect("mutants.toml");
        let line = format!("output = \"{OUT}\"");
        assert!(
            text.lines().any(|l| l == line),
            "{line} missing from {text}"
        );
    }

    #[test]
    fn the_patch_holds_commits_and_staged_and_unstaged_edits() {
        let dir = std::env::temp_dir().join(format!("spate-mutants-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let write = |name: &str, text: &str| std::fs::write(dir.join(name), text).expect("write");
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(["-c", "user.name=t", "-c", "user.email=t@t"])
                .args(["-c", "commit.gpgsign=false"])
                .args(args)
                .current_dir(&dir)
                .status()
                .expect("git")
                .success();
            assert!(ok, "git {args:?}");
        };
        git(&["init", "-q", "-b", "main"]);
        // Settings that would otherwise change the patch's paths and bytes.
        for (key, value) in [
            ("diff.mnemonicPrefix", "true"),
            ("diff.noprefix", "true"),
            ("color.diff", "always"),
        ] {
            git(&["config", key, value]);
        }
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
        for path in ["committed", "staged", "unstaged"] {
            assert!(
                patch.contains(&format!("+++ b/{path}\n")),
                "{path} in {patch}"
            );
        }
        assert!(!patch.contains('\u{1b}'), "{patch}");
        for hunk in ["+committed-edit", "+staged-edit", "+unstaged-edit"] {
            assert!(patch.contains(hunk), "{hunk} missing from {patch}");
        }
        assert!(!patch.contains("untracked-edit"), "{patch}");
        assert!(diff_since(&dir, "no-such-ref").is_err());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }
}
