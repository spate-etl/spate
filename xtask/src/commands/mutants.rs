//! `cargo xtask mutants`: cargo-mutants over the diff against a base, with the
//! survivors written to the job summary on a CI runner.

use std::path::{Path, PathBuf};

use crate::run::{self, Error, Outcome, Step, Streams};

/// Where the patch and cargo-mutants' `mutants.out` land, under the root. It
/// must match `output` in `.cargo/mutants.toml`.
const OUT: &str = "target/mutants";

/// The `git diff` flags that keep a user's diff settings out of the patch.
/// cargo-mutants reads each path after a `b/` prefix and parses plain text, so
/// a changed prefix, color or an external or textconv driver would leave it
/// unable to match or parse the patch. A renamed file contributes only its
/// edited lines.
const DIFF_FLAGS: [&str; 6] = [
    "--no-ext-diff",
    "--no-textconv",
    "--no-color",
    "--src-prefix=a/",
    "--dst-prefix=b/",
    "--find-renames",
];

const HEADING: &str = "## Mutants over the diff\n\n";

/// Writes the diff against the merge base of `base` and `HEAD`, runs
/// cargo-mutants over it, and writes the survivors to `$GITHUB_STEP_SUMMARY`
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
            "git diff {} $(git merge-base {base} HEAD) > {}",
            DIFF_FLAGS.join(" "),
            diff.display()
        );
        return run::run(root, true, &step);
    }
    let summary = std::env::var_os("GITHUB_STEP_SUMMARY").map(|p| Summary::at(p.into()));
    let ran = prepare(root, base, &out, &diff).and_then(|()| {
        // A step timeout signals this process before `finish` can write, so
        // the summary says the run stopped until cargo-mutants returns.
        if let Some(summary) = &summary {
            summary.write(&unfinished(
                "the step stopped before cargo-mutants returned",
            ));
        }
        Ok(run::complete(root, &step, Streams::Inherit)?.code)
    });
    finish(ran, &out.join("mutants.out"), summary.as_ref())
}

/// Writes the patch to `diff` and removes the previous run's report under
/// `out`. A run that stops before testing writes no report, and a stale one
/// would be summarised as this run's.
fn prepare(root: &Path, base: &str, out: &Path, diff: &Path) -> Outcome {
    let patch = diff_since(root, base)?;
    clear_report(&out.join("mutants.out"))?;
    std::fs::create_dir_all(out).map_err(|e| Error::msg(format!("{}: {e}", out.display())))?;
    std::fs::write(diff, patch).map_err(|e| Error::msg(format!("{}: {e}", diff.display())))
}

fn clear_report(dir: &Path) -> Outcome {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(Error::msg(format!("{}: {e}", dir.display())))
        }
        _ => Ok(()),
    }
}

/// The job summary file. Each write replaces what this run wrote before and
/// keeps what the file held when the run started.
struct Summary {
    path: PathBuf,
    from: u64,
}

impl Summary {
    fn at(path: PathBuf) -> Self {
        let from = std::fs::metadata(&path).map_or(0, |m| m.len());
        Self { path, from }
    }

    /// Writes `text` after what the file held at the start. A failure is
    /// reported on stderr and is not an error of the run.
    fn write(&self, text: &str) {
        use std::io::{Seek, SeekFrom, Write};
        let written = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .and_then(|mut f| {
                f.set_len(self.from)?;
                f.seek(SeekFrom::End(0))?;
                f.write_all(text.as_bytes())
            });
        if let Err(e) = written {
            eprintln!("{}: {e}", self.path.display());
        }
    }
}

/// Writes the summary to the summary file, or to stdout when there is none,
/// and turns the run into the outcome: cargo-mutants' exit code from the
/// report in `dir`, or the error that stopped the run before cargo-mutants
/// finished.
fn finish(ran: Result<i32, Error>, dir: &Path, summary: Option<&Summary>) -> Outcome {
    let text = match &ran {
        Ok(code) => {
            let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap_or_default();
            self::summary(*code, &read("missed.txt"), &read("timeout.txt"))
        }
        Err(e) => unfinished(&e.message),
    };
    match summary {
        Some(summary) => summary.write(&text),
        None => print!("{text}"),
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
    run::capture(
        root,
        &Step::new("git", ["diff"]).args(DIFF_FLAGS).arg(&merge_base),
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
         match arms and struct-literal fields. It does not swap a `match` arm, delete an \
         early `return`, `break` or `continue`, or move a constant across its bound; those \
         stay a review step.\n",
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
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("spate-mutants-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// Runs git in `dir` with an identity and no signing, so a commit needs no
    /// global configuration.
    fn git(dir: &Path, args: &[&str]) {
        let ok = std::process::Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git")
            .success();
        assert!(ok, "git {args:?}");
    }

    /// A repository on branch `topic`, forked from `main`, with one committed,
    /// one staged, one unstaged and one untracked edit, a file renamed and
    /// edited on `topic`, and a commit `main` gained after the fork. Its config
    /// sets every diff option `DIFF_FLAGS` overrides.
    fn fixture(name: &str) -> PathBuf {
        let dir = scratch(name);
        let write = |name: &str, text: &str| std::fs::write(dir.join(name), text).expect("write");
        git(&dir, &["init", "-q", "-b", "main"]);
        for (key, value) in [
            ("diff.mnemonicPrefix", "true"),
            ("diff.noprefix", "true"),
            ("diff.renames", "false"),
            ("color.diff", "always"),
            ("diff.external", "false"),
            ("diff.t.textconv", "false"),
        ] {
            git(&dir, &["config", key, value]);
        }
        std::fs::write(dir.join(".git/info/attributes"), "* diff=t\n").expect("attributes");
        for name in ["committed", "staged", "unstaged"] {
            write(name, "base\n");
        }
        write("moved", "one\ntwo\nthree\nfour\nfive\n");
        git(&dir, &["add", "."]);
        git(&dir, &["commit", "-q", "-m", "base"]);
        git(&dir, &["branch", "topic"]);
        write("main-only", "main-later\n");
        git(&dir, &["add", "main-only"]);
        git(&dir, &["commit", "-q", "-m", "main moves on"]);
        git(&dir, &["checkout", "-q", "topic"]);
        write("committed", "committed-edit\n");
        git(&dir, &["mv", "moved", "renamed"]);
        write("renamed", "one\ntwo\nthree-edited\nfour\nfive\n");
        git(&dir, &["commit", "-q", "-am", "topic"]);
        write("staged", "staged-edit\n");
        git(&dir, &["add", "staged"]);
        write("unstaged", "unstaged-edit\n");
        write("untracked", "untracked-edit\n");
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

    /// Preparing writes the patch and removes a stale report, and stops when
    /// the report cannot be removed.
    #[test]
    fn preparing_writes_the_patch_and_clears_the_last_report() {
        let dir = fixture("prepare");
        let out = dir.join("out");
        let diff = out.join("pr.diff");
        std::fs::create_dir_all(out.join("mutants.out")).expect("report dir");
        std::fs::write(out.join("mutants.out/missed.txt"), "stale\n").expect("write");
        prepare(&dir, "main", &out, &diff).expect("prepared");
        assert!(!out.join("mutants.out").exists());
        let patch = std::fs::read_to_string(&diff).expect("patch");
        assert!(patch.contains("+committed-edit"), "{patch}");

        std::fs::remove_file(&diff).expect("remove patch");
        std::fs::write(out.join("mutants.out"), "").expect("a file in the way");
        assert!(prepare(&dir, "main", &out, &diff).is_err());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    /// The summary lands after what the file held before the run, replaces
    /// the run's own earlier line, lists survivors and timeouts, and the exit
    /// code passes through unchanged.
    #[test]
    fn the_summary_replaces_the_runs_own_line_and_keeps_the_exit_code() {
        let dir = scratch("report");
        std::fs::write(dir.join("missed.txt"), "a.rs:1:1: replace f\n").expect("write");
        std::fs::write(dir.join("timeout.txt"), "c.rs:3:3: replace h\n").expect("write");
        let path = dir.join("summary.md");
        std::fs::write(&path, "earlier\n").expect("write");
        let summary = Summary::at(path.clone());
        summary.write(&unfinished(
            "the step stopped before cargo-mutants returned",
        ));
        assert!(
            std::fs::read_to_string(&path)
                .expect("summary")
                .contains("did not finish")
        );

        let err = finish(Ok(2), &dir, Some(&summary)).expect_err("survivors exit 2");
        assert_eq!(err.code, Some(2));
        let text = std::fs::read_to_string(&path).expect("summary");
        assert!(
            text.starts_with("earlier\n## Mutants over the diff\n"),
            "{text}"
        );
        assert!(text.contains("- `a.rs:1:1: replace f`"), "{text}");
        assert!(text.contains("- timeout: `c.rs:3:3: replace h`"), "{text}");
        assert!(!text.contains("did not finish"), "{text}");

        assert!(finish(Ok(0), &dir, None).is_ok());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    /// A run stopped before cargo-mutants finished still writes a summary,
    /// naming the cause, and returns the same error.
    #[test]
    fn a_failure_before_the_run_is_summarised() {
        let dir = scratch("early");
        let path = dir.join("summary.md");
        let summary = Summary::at(path.clone());
        let err = finish(
            Err(Error::msg("main does not resolve")),
            &dir,
            Some(&summary),
        )
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

    /// A summary that cannot be written leaves the run's own outcome intact.
    #[test]
    fn a_failed_summary_write_keeps_the_outcome() {
        let dir = scratch("unwritable");
        // A directory cannot be opened for writing.
        let summary = Summary::at(dir.clone());
        let err = finish(Ok(2), &dir, Some(&summary)).expect_err("survivors exit 2");
        assert_eq!(err.code, Some(2));
        let err = finish(
            Err(Error::msg("main does not resolve")),
            &dir,
            Some(&summary),
        )
        .expect_err("the error passes through");
        assert_eq!(err.message, "main does not resolve");
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

    /// The patch runs from the merge base to the working tree, whatever the
    /// user's diff settings: what `main` gained after the fork and untracked
    /// files are not in it, and a renamed file brings only its edited line.
    #[test]
    fn the_patch_holds_the_branch_from_the_merge_base_and_nothing_else() {
        let dir = fixture("patch");
        let patch = diff_since(&dir, "main").expect("a readable diff");
        for path in ["committed", "staged", "unstaged", "renamed"] {
            assert!(
                patch.contains(&format!("+++ b/{path}\n")),
                "{path} in {patch}"
            );
        }
        assert!(!patch.contains('\u{1b}'), "{patch}");
        for hunk in [
            "+committed-edit",
            "+staged-edit",
            "+unstaged-edit",
            "+three-edited",
        ] {
            assert!(patch.contains(hunk), "{hunk} missing from {patch}");
        }
        assert!(!patch.contains("main-later"), "{patch}");
        assert!(
            !patch.contains("+one\n"),
            "unedited lines of a renamed file: {patch}"
        );
        assert!(!patch.contains("untracked-edit"), "{patch}");
        assert!(diff_since(&dir, "no-such-ref").is_err());
        std::fs::remove_dir_all(&dir).expect("remove temp dir");
    }
}
