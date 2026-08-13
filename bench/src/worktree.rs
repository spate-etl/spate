//! The base leg's checkout, and getting rid of it afterwards.
//!
//! An A/B run needs the reference's tree on disk to build it. That tree is a
//! detached `git worktree`, created under a cache directory and removed when
//! the run ends.
//!
//! # Never inside the repository
//!
//! A worktree under the repository would be picked up by `cargo metadata`,
//! `git status`, the docs build's file walk and every `git grep` in the tree —
//! and, worst, a `cargo bench --workspace` would find a second copy of every
//! package. The default location is under `$TMPDIR`, overridable with
//! `SPATE_BENCH_CACHE`, and the constructor refuses a path inside the
//! repository outright.
//!
//! # Cleanup, including the ways a program does not reach its last line
//!
//! [`Worktree`] removes itself on [`Drop`], which covers a normal return and a
//! panic. Ctrl-C is the case a destructor cannot cover on its own, so
//! [`install_interrupt_handler`] sets a flag that the run loop checks between
//! steps — for `SIGINT`, `SIGTERM` and `SIGHUP` alike. The interrupt reaches
//! the child processes too, so the loop notices quickly and unwinds through
//! the same `Drop`. A second interrupt exits
//! immediately, on the principle that somebody pressing it twice means it —
//! and that one *does* leave the checkout behind, which is why
//! [`Worktree::add`] says how to remove a leftover rather than only refusing
//! one.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};

/// Overrides where base-leg worktrees and target directories are cached.
pub const CACHE_ENV: &str = "SPATE_BENCH_CACHE";

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// The cache root: `$SPATE_BENCH_CACHE`, or `spate-bench` under the system
/// temporary directory.
#[must_use]
pub fn cache_root() -> PathBuf {
    let configured = std::env::var_os(CACHE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("spate-bench"));

    // Absolutised against the working directory, so a relative
    // `SPATE_BENCH_CACHE=cache` is refused for being inside the repository —
    // which it is — rather than failing later with "cannot resolve any
    // ancestor".
    if configured.is_absolute() {
        configured
    } else {
        std::env::current_dir().map_or(configured.clone(), |cwd| cwd.join(configured))
    }
}

/// The cached target directory one feature arm builds into.
///
/// Keyed by the flags rather than named after them: a feature list is an
/// arbitrary string, and `--features spate-json/simd,other` is not a directory
/// name. Keyed by the leg as well, so two arms never share a directory even when
/// they were asked for identically — which is a legitimate run, being the A/A
/// that shows what two builds of the same source cost against each other. A
/// second run of the same pair reuses both, a cache in the ordinary sense where
/// `rm -rf` costs a rebuild and nothing else.
///
/// Neither arm uses the repository's own `target/`: cargo keeps one build per
/// directory, so the arms would overwrite each other, and using the warm one
/// would charge the next ordinary `cargo build` for a rebuild it did not ask
/// for.
#[must_use]
pub fn arm_target_dir(leg: &str, feature_args: &[String]) -> PathBuf {
    use std::hash::Hasher as _;

    let mut hasher = twox_hash::XxHash64::with_seed(0);
    for arg in feature_args {
        // Lengths folded in as well, so `["--features", "a,b"]` and
        // `["--features", "a", "b"]` cannot key the same directory.
        hasher.write(&(arg.len() as u64).to_le_bytes());
        hasher.write(arg.as_bytes());
    }
    cache_root().join(format!("target-arm-{leg}-{:016x}", hasher.finish()))
}

/// Whether Ctrl-C has been seen.
#[must_use]
pub fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::Relaxed)
}

/// Makes an interrupt set [`interrupted`] instead of killing the process
/// outright.
///
/// Without this, an interrupted run leaves its worktree behind: the default
/// disposition terminates the process, and a terminated process runs no
/// destructor. `SIGTERM` and `SIGHUP` are handled alongside `SIGINT` because a
/// `kill` and a closed terminal leave exactly the same mess as a Ctrl-C, and
/// the mess is not inert — the leftover checkout stays registered in the
/// repository, and the next run against that commit refuses.
pub fn install_interrupt_handler() {
    // SAFETY: `signal` with a handler function pointer is the documented C
    // interface, and the handler below does nothing that is not
    // async-signal-safe — one relaxed atomic store, or `_exit`.
    for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
        // SAFETY: as above; the handler is the same async-signal-safe function
        // for each.
        unsafe {
            libc::signal(signal, on_interrupt as *const () as libc::sighandler_t);
        }
    }
}

extern "C" fn on_interrupt(_signal: libc::c_int) {
    if INTERRUPTED.swap(true, Ordering::Relaxed) {
        // The second one. Nothing here can wait for a lock or allocate — not
        // even to print a path — so the only honest way out is the one that
        // skips the destructors. `Worktree::add` is where a leftover checkout
        // is explained, on the next run that trips over it.
        // SAFETY: `_exit` is async-signal-safe and does not return.
        unsafe { libc::_exit(130) };
    }
}

/// Refuses a path that would put harness output inside the repository.
///
/// Checked against the nearest existing ancestor rather than the path itself,
/// which does not exist yet — and the check runs *before* anything is created,
/// so a refusal leaves no directory behind. On macOS `$TMPDIR` is a symlink, so
/// comparing uncanonicalised forms would prove nothing.
///
/// # Errors
///
/// When `path` resolves inside `repo`, or when no ancestor of it can be
/// resolved at all.
pub fn ensure_outside(repo: &Path, path: &Path, what: &str) -> Result<(), String> {
    let mut candidate = path;
    let resolved = loop {
        if let Ok(real) = candidate.canonicalize() {
            break real;
        }
        candidate = candidate
            .parent()
            .ok_or_else(|| format!("cannot resolve any ancestor of {}", path.display()))?;
    };

    if resolved.starts_with(repo) {
        return Err(format!(
            "{} is inside the repository, and {what} there would be found by cargo, \
             git and the docs build. Set {CACHE_ENV} to a path outside it.",
            path.display()
        ));
    }
    Ok(())
}

/// A detached checkout of one reference, removed on drop.
#[derive(Debug)]
pub struct Worktree {
    repo: PathBuf,
    path: PathBuf,
}

impl Worktree {
    /// Resolves `git_ref` to a commit without creating anything.
    ///
    /// Called before any building, so a mistyped reference costs nothing.
    ///
    /// # Errors
    ///
    /// When the reference does not name a commit in this repository.
    pub fn resolve(repo: &Path, git_ref: &str) -> Result<String, String> {
        let output = Command::new("git")
            .current_dir(repo)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("{git_ref}^{{commit}}"),
            ])
            .output()
            .map_err(|e| format!("could not run git: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "'{git_ref}' does not name a commit in {}",
                repo.display()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Checks `commit` out into `path`, detached.
    ///
    /// # Errors
    ///
    /// When `path` is inside the repository, when it already exists, or when
    /// git refuses.
    pub fn add(repo: &Path, commit: &str, path: &Path) -> Result<Self, String> {
        let repo_abs = repo
            .canonicalize()
            .map_err(|e| format!("cannot resolve {}: {e}", repo.display()))?;
        // The parent is canonicalised rather than the path itself, which does
        // not exist yet — and on macOS `$TMPDIR` is a symlink, so comparing the
        // uncanonicalised forms would miss nothing but would also prove
        // nothing.
        ensure_outside(&repo_abs, path, "a worktree")?;
        let parent = path
            .parent()
            .ok_or_else(|| format!("{} has no parent directory", path.display()))?;
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        if path.exists() {
            return Err(format!(
                "{} already exists. A previous run was killed without unwinding; \
                 remove it with `git worktree remove --force {}` (or \
                 `git worktree prune` if the directory is already gone).",
                path.display(),
                path.display()
            ));
        }

        let status = Command::new("git")
            .current_dir(repo)
            .args([
                "worktree",
                "add",
                "--detach",
                "--quiet",
                &path.display().to_string(),
                commit,
            ])
            .status()
            .map_err(|e| format!("could not run git: {e}"))?;
        if !status.success() {
            return Err(format!(
                "git worktree add {} {commit} failed with {status}",
                path.display()
            ));
        }

        Ok(Self {
            repo: repo_abs,
            path: path.to_owned(),
        })
    }

    /// Where the checkout is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Worktree {
    fn drop(&mut self) {
        // `--force` because the build wrote into it. Failure is reported and
        // not propagated: a destructor that panicked would replace whatever
        // error brought us here.
        let removed = Command::new("git")
            .current_dir(&self.repo)
            .args([
                "worktree",
                "remove",
                "--force",
                &self.path.display().to_string(),
            ])
            .status();
        let left_behind = match removed {
            Ok(status) if status.success() => false,
            _ => self.path.exists(),
        };
        if left_behind {
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr(),
                "spate-bench: could not remove the worktree at {}. \
                 Remove it with `git worktree remove --force {}`.",
                self.path.display(),
                self.path.display()
            );
        }
    }
}

/// `git describe --always --dirty` for a checkout, and whether it is dirty.
///
/// Provenance only. Nothing downstream resolves a reference from it — a `run`
/// against a worktree must report the checkout it measured, not the name it was
/// asked for.
#[must_use]
pub fn describe(dir: &Path) -> (Option<String>, bool) {
    let text = Command::new("git")
        .current_dir(dir)
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|text| !text.is_empty());
    let dirty = text.as_deref().is_some_and(|t| t.ends_with("-dirty"));
    (text, dirty)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{CACHE_ENV, Worktree, arm_target_dir, cache_root, describe};

    /// The property the leg is in the key for. Two arms asked for identically
    /// is a legitimate run — it is the A/A of the mode — and cargo keeps one
    /// build per directory, so sharing one would have each arm overwrite the
    /// other and both legs measure whichever built last.
    #[test]
    fn two_arms_never_share_a_target_directory_even_with_identical_flags() {
        let flags = vec!["--features".to_owned(), "spate-json/simd".to_owned()];
        assert_ne!(
            arm_target_dir("base", &flags),
            arm_target_dir("head", &flags)
        );
    }

    /// Keyed by the flags, so a second run of the same pair reuses the build,
    /// and a different pair does not collide with it.
    #[test]
    fn an_arm_directory_is_stable_for_its_flags_and_distinct_between_them() {
        let simd = vec!["--features".to_owned(), "spate-json/simd".to_owned()];
        assert_eq!(arm_target_dir("head", &simd), arm_target_dir("head", &simd));
        assert_ne!(arm_target_dir("head", &simd), arm_target_dir("head", &[]));
        assert_ne!(
            arm_target_dir("head", &simd),
            arm_target_dir("head", &["--all-features".to_owned()])
        );
    }

    /// The lengths are folded in for the reason the corpus folds them: without
    /// them, one flag holding two features and two flags holding one each hash
    /// to the same directory, and the two arms would build over each other.
    #[test]
    fn an_arm_directory_distinguishes_a_re_split_flag_list() {
        assert_ne!(
            arm_target_dir("head", &["--features".to_owned(), "a,b".to_owned()]),
            arm_target_dir(
                "head",
                &["--features".to_owned(), "a".to_owned(), "b".to_owned()]
            )
        );
    }

    /// Under the cache root, so `outside_repo` accepts it and cargo and git
    /// never find the artifacts.
    #[test]
    fn an_arm_directory_sits_under_the_cache_root() {
        let dir = arm_target_dir("base", &[]);
        assert!(dir.starts_with(cache_root()), "{}", dir.display());
    }

    #[test]
    fn the_cache_root_is_outside_any_repository_by_default() {
        // Read rather than set: `cargo test` shares one process, so setting the
        // variable here would decide another test's answer.
        let root = cache_root();
        assert!(root.is_absolute(), "{}", root.display());
        if std::env::var_os(CACHE_ENV).is_none() {
            assert!(root.ends_with("spate-bench"), "{}", root.display());
        }
    }

    #[test]
    fn a_worktree_inside_the_repository_is_refused_without_creating_it() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
        let inside = repo.join("spate-bench-refusal-probe/wt");
        let err = Worktree::add(repo, "HEAD", &inside).expect_err("refused");
        assert!(err.contains("inside the repository"), "{err}");
        assert!(err.contains(CACHE_ENV), "{err}");
        assert!(
            !inside.parent().expect("has a parent").exists(),
            "the refusal created the directory it refused"
        );
    }

    #[test]
    fn a_path_outside_the_repository_is_accepted() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .expect("resolves");
        assert!(
            super::ensure_outside(&repo, &std::env::temp_dir().join("spate-bench/x"), "a leg")
                .is_ok()
        );
        assert!(super::ensure_outside(&repo, &repo.join("target/x"), "a leg").is_err());
    }

    #[test]
    fn a_reference_that_names_nothing_is_refused_without_creating_anything() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
        let err = Worktree::resolve(repo, "definitely-not-a-ref-9f3c").expect_err("refused");
        assert!(err.contains("does not name a commit"), "{err}");

        // And the happy path, so the failure above is about the reference
        // rather than about git being unavailable.
        assert_eq!(Worktree::resolve(repo, "HEAD").expect("resolves").len(), 40);
    }

    #[test]
    fn describe_reports_the_checkout_it_was_pointed_at() {
        let (text, _) = describe(Path::new(env!("CARGO_MANIFEST_DIR")));
        assert!(
            text.expect("this crate lives in a git repository").len() >= 7,
            "git describe --always yields at least an abbreviated object name"
        );

        // A directory that is not a checkout has no provenance to report, and
        // must say nothing rather than inherit the driver's own.
        let outside = tempfile::tempdir().expect("scratch directory");
        assert_eq!(describe(outside.path()), (None, false));
    }
}
