//! The process-level contract of `cargo xtask hooks install`: what it sets,
//! what it refuses to replace, and what it reports.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

mod support;

/// A repository of this test's own. `GIT_DIR` points the command's `git`
/// invocations at it.
struct Repo(support::Scratch);

impl Repo {
    fn new(name: &str) -> Self {
        let repo = Self(support::Scratch::new(&format!("spate-xtask-hooks-{name}")).unwrap());
        repo.git(&["init", "--quiet", "-b", "main", "."]);
        repo
    }

    fn git_dir(&self) -> PathBuf {
        self.0.join(".git")
    }

    fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(self.0.dir())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn install(&self) -> Output {
        let git_dir = self.git_dir();
        support::run(
            &["hooks", "install"],
            &["GITHUB_ACTIONS"],
            &[
                ("GIT_DIR", git_dir.to_str().unwrap()),
                ("GIT_WORK_TREE", self.0.dir().to_str().unwrap()),
            ],
        )
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
