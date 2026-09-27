//! The process-level contract of `cargo xtask hooks install`: what it sets,
//! what it refuses to replace, and what it reports.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// A repository of this test's own. `GIT_DIR` points the command's `git`
/// invocations at it.
struct Repo(PathBuf);

impl Repo {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("spate-xtask-hooks-{name}.{}", std::process::id()));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let repo = Self(dir);
        repo.git(&["init", "--quiet", "-b", "main", "."]);
        repo
    }

    fn git_dir(&self) -> PathBuf {
        self.0.join(".git")
    }

    fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(&self.0)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn install(&self) -> Output {
        Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
            .args(["hooks", "install"])
            .env("GIT_DIR", self.git_dir())
            .env("GIT_WORK_TREE", &self.0)
            .env_remove("GITHUB_ACTIONS")
            .output()
            .unwrap()
    }
}

impl Drop for Repo {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The first install sets the path, and a second one says it is already set.
#[test]
fn install_sets_the_hook_path_once() {
    let repo = Repo::new("once");
    let out = repo.install();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        repo.git(&["config", "--get", "core.hooksPath"]),
        ".githooks"
    );

    let again = repo.install();
    assert_eq!(again.status.code(), Some(0));
    assert_eq!(
        stdout(&again),
        "hooks: core.hooksPath is already .githooks.\n"
    );
}

/// A hook path pointing elsewhere is refused and left as it was.
#[test]
fn install_refuses_to_replace_another_hook_path() {
    let repo = Repo::new("other");
    repo.git(&["config", "core.hooksPath", "tools/hooks"]);
    let out = repo.install();
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("core.hooksPath is already 'tools/hooks'"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        repo.git(&["config", "--get", "core.hooksPath"]),
        "tools/hooks"
    );
}

/// A live hook in the default directory is named, since it stops running.
#[test]
fn install_names_the_hooks_that_stop_running() {
    let repo = Repo::new("live");
    let hooks = repo.git_dir().join("hooks");
    std::fs::create_dir_all(&hooks).unwrap();
    std::fs::write(hooks.join("pre-push"), "#!/bin/sh\n").unwrap();
    let out = repo.install();
    assert_eq!(out.status.code(), Some(0));
    assert!(
        stdout(&out).contains(" no longer run: pre-push\n"),
        "{}",
        stdout(&out)
    );
    assert!(!stdout(&out).contains(".sample"), "{}", stdout(&out));
    assert!(Path::new(&hooks).join("pre-push").is_file());
}
