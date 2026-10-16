//! The filing contract of `.github/actions/file-issue/file-issue.sh`: which
//! issue it comments on, what it files when a rendered file is missing, and
//! that a refused type leaves the step green.
//!
//! `gh` is a recording shim ahead of anything else on `PATH`, so nothing here
//! contacts GitHub. The script runs under `bash` and needs `jq`, as the action
//! does. The shim is a shell script, so this binary is empty off unix.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const RUN: &str = "https://example.invalid/runs/1";

/// A directory holding a `gh` that records its argv.
struct Shim(PathBuf);

impl Shim {
    /// A `gh` that answers `issue list` with `list`, prints a URL for `issue
    /// create`, and exits `edit_rc` for `issue edit`.
    fn new(name: &str, list: &str, edit_rc: u8) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-file-issue-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("list.json"), list).unwrap();
        // One NUL-terminated field per argument, one line per invocation.
        let gh = format!(
            "#!/usr/bin/env bash\n\
             printf '%s\\0' \"$@\" >>'{log}'\n\
             printf '\\n' >>'{log}'\n\
             case \"$1 $2\" in\n\
             'issue list') cat '{list}' ;;\n\
             'issue create') echo https://example.invalid/issues/9 ;;\n\
             'issue edit') exit {edit_rc} ;;\n\
             esac\n",
            log = dir.join("argv").display(),
            list = dir.join("list.json").display(),
        );
        std::fs::write(dir.join("gh"), gh).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.join("gh"), std::fs::Permissions::from_mode(0o755)).unwrap();
        Self(dir)
    }

    /// Every invocation of the shim so far, in order.
    fn calls(&self) -> Vec<Vec<String>> {
        std::fs::read_to_string(self.0.join("argv"))
            .unwrap_or_default()
            .lines()
            .map(|l| {
                l.split('\0')
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .collect()
    }

    /// One run of the script, with `COMMENT_FILE` set only when `comment` is.
    fn run(&self, title: &str, body: &Path, comment: Option<&Path>) -> Output {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(".github/actions/file-issue/file-issue.sh");
        let mut dirs = vec![self.0.clone()];
        dirs.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let mut cmd = Command::new("bash");
        cmd.arg(script)
            .env("PATH", std::env::join_paths(dirs).unwrap())
            .env("TITLE", title)
            .env("BODY_FILE", body)
            .env("RUN_URL", RUN);
        match comment {
            Some(c) => cmd.env("COMMENT_FILE", c),
            None => cmd.env_remove("COMMENT_FILE"),
        };
        cmd.output().unwrap()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

/// An empty body file still files the issue, with a line naming the run, and
/// the issue's type is set to Bug.
#[test]
fn an_empty_body_files_with_the_run_link() {
    let shim = Shim::new("empty-body", "[]", 0);
    let body = shim.0.join("body.md");
    std::fs::write(&body, "").unwrap();
    let out = shim.run("[bug] X", &body, None);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let calls = shim.calls();
    let create = calls
        .iter()
        .find(|c| c[..2] == ["issue", "create"])
        .unwrap();
    assert_eq!(create[4], "--body");
    assert!(create[5].contains(RUN));
    assert!(
        calls
            .iter()
            .any(|c| c[..2] == ["issue", "edit"] && c.ends_with(&["--type".into(), "Bug".into()]))
    );
}

/// An open issue with exactly the title gets a "Still failing" comment when
/// the comment file is missing; a near match gets a new issue.
#[test]
fn only_an_exact_title_takes_the_comment() {
    let list = r#"[{"number":7,"title":"[bug] X"},{"number":8,"title":"[bug] X again"}]"#;
    let shim = Shim::new("exact", list, 0);
    let body = shim.0.join("body.md");
    std::fs::write(&body, "### What happened\n\nx\n").unwrap();
    let missing = shim.0.join("absent.md");
    let out = shim.run("[bug] X", &body, Some(&missing));
    assert!(out.status.success());
    assert_eq!(
        shim.calls().last().unwrap(),
        &[
            "issue",
            "comment",
            "7",
            "--body",
            &format!("Still failing: {RUN}")
        ]
    );
    assert!(shim.calls().iter().any(|c| c[..2] == ["issue", "list"]
        && c.windows(2).any(|w| w == ["--state", "open"])
        && c.windows(2).any(|w| w == ["--search", "[bug] X in:title"])));
    let near = Shim::new("near", list, 0);
    let out = near.run("[bug] X agai", &body, None);
    assert!(out.status.success());
    assert!(near.calls().iter().any(|c| c[..2] == ["issue", "create"]));
}

/// A refused type leaves the created issue and a warning, and the step passes.
#[test]
fn a_refused_type_warns_and_passes() {
    let shim = Shim::new("refused", "[]", 1);
    let body = shim.0.join("body.md");
    std::fs::write(&body, "x\n").unwrap();
    let out = shim.run("[bug] X", &body, None);
    assert!(out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stdout)
            .contains("::warning::Could not set the type on https://example.invalid/issues/9")
    );
}
