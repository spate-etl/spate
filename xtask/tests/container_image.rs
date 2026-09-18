//! The process-level contract of `cargo xtask container-image`: which lane it
//! resolves, which stream each line lands on, and the argv it hands `docker`.
//!
//! `docker` is a recording shim on `PATH`, so nothing here contacts a daemon.
//! The shim is a shell script, so this binary is empty off unix.
#![cfg(unix)]

use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::{Command, Output};

/// The service every case resolves.
const SERVICE: &str = "clickhouse";

/// A lane name no tree holds.
const GHOST: &str = "ghost-lane";

/// What the shim writes on a `pull`, standing in for the digest line docker
/// prints there.
const PULLED: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// A directory holding a `docker` that records its argv and pulls nothing.
struct Shim(PathBuf);

impl Shim {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("spate-xtask-docker-{}-{name}", std::process::id()));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let me = Self(dir);
        // One NUL-terminated field per argument, one line per invocation.
        std::fs::write(
            me.0.join("docker"),
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do printf '%s\\0' \"$a\" >>\"$SPATE_DOCKER_LOG\"; done\n\
                 printf '\\n' >>\"$SPATE_DOCKER_LOG\"\n\
                 if [ \"$1\" = pull ]; then echo '{PULLED}'; fi\n\
                 exit 0\n"
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(me.0.join("docker"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        me
    }

    fn log(&self) -> PathBuf {
        self.0.join("argv")
    }

    /// Every invocation of the shim so far, in order.
    fn calls(&self) -> Vec<Vec<String>> {
        let Ok(text) = std::fs::read_to_string(self.log()) else {
            return Vec::new();
        };
        text.lines()
            .map(|line| {
                line.split('\0')
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .collect()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

/// One run of the task runner, with the shim ahead of anything else on `PATH`
/// and the lane variable under the case's control.
fn xtask(shim: &Shim, lane: Option<&OsStr>, args: &[&str]) -> Output {
    let path = match std::env::var_os("PATH") {
        Some(p) => {
            let mut dirs = vec![shim.0.clone()];
            dirs.extend(std::env::split_paths(&p));
            std::env::join_paths(dirs).unwrap()
        }
        None => shim.0.clone().into_os_string(),
    };
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command
        .arg("container-image")
        .args(args)
        .env("PATH", path)
        .env("SPATE_DOCKER_LOG", shim.log())
        // The annotation prefix would otherwise depend on the host.
        .env_remove("GITHUB_ACTIONS");
    match lane {
        Some(lane) => command.env("SPATE_CLICKHOUSE_LANE", lane),
        None => command.env_remove("SPATE_CLICKHOUSE_LANE"),
    };
    command.output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The `name:tag` the primary lane pins, taken from the runner itself so a
/// bump moving the pin moves the expectation with it.
fn primary_tag(shim: &Shim) -> String {
    let out = xtask(shim, None, &[SERVICE]);
    assert!(out.status.success(), "{}", stderr(&out));
    stdout(&out).trim().to_owned()
}

/// A pull fetches the digest, re-tags it locally, and leaves stdout carrying
/// the local tag alone, which is what a caller parses.
#[test]
fn a_pull_reaches_the_digest_and_prints_the_local_tag_alone() {
    let shim = Shim::new("pull");
    let tagged = primary_tag(&shim);

    let out = xtask(&shim, None, &["--pull", SERVICE]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), format!("{tagged}\n"));
    assert!(stderr(&out).contains(PULLED), "{}", stderr(&out));

    let calls = shim.calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    let by_digest = calls[0].get(2).cloned().unwrap_or_default();
    assert!(by_digest.contains("@sha256:"), "{calls:?}");
    assert_eq!(calls[0], ["pull", "--quiet", &by_digest]);
    assert_eq!(calls[1], ["tag", &by_digest, &tagged]);
}

/// The lane variable selects the lane every mode resolves. A lane no tree holds
/// fails, so the primary lane's image cannot stand in for it.
#[test]
fn the_lane_variable_selects_the_lane() {
    let shim = Shim::new("variable");
    let ghost = OsStr::new(GHOST);
    let missing = format!("no such lane: ci/{SERVICE}/{GHOST}/Dockerfile");
    for args in [
        vec![SERVICE],
        vec!["--ref", SERVICE],
        vec!["--pull", SERVICE],
        vec!["--pull-all"],
    ] {
        let out = xtask(&shim, Some(ghost), &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {}", stderr(&out));
        let reported = stderr(&out);
        assert!(
            reported.ends_with(&format!("xtask: {missing}\n")),
            "{args:?}: {reported}"
        );
        assert_eq!(stdout(&out), "", "{args:?}");
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// A value that is not UTF-8 names no lane, and fails rather than resolving.
#[test]
fn a_lane_variable_that_is_not_utf8_fails() {
    use std::os::unix::ffi::OsStrExt;

    let shim = Shim::new("variable-bytes");
    let out = xtask(&shim, Some(OsStr::from_bytes(b"lt\xffs")), &[SERVICE]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stderr(&out), "xtask: SPATE_CLICKHOUSE_LANE is not UTF-8\n");
    assert_eq!(stdout(&out), "");
}

/// `--pull-all` names each service and the lane it took on stderr, and pulls
/// that lane.
#[test]
fn pull_all_names_each_service_and_pulls_its_selected_lane() {
    let shim = Shim::new("pull-all");
    let tagged = primary_tag(&shim);

    let out = xtask(&shim, None, &["--pull-all"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
    assert!(
        stderr(&out).contains(&format!("{SERVICE}: ")),
        "{}",
        stderr(&out)
    );

    let calls = shim.calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(calls[0][0], "pull");
    assert_eq!(calls[1], ["tag", &calls[0][2], &tagged]);
}

/// `--explain` prints the plan and runs none of it.
#[test]
fn explain_prints_the_plan_and_runs_nothing() {
    let shim = Shim::new("explain");
    let out = xtask(&shim, None, &["--pull", SERVICE, "--explain"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let printed = stdout(&out);
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].starts_with("docker pull --quiet "), "{lines:?}");
    assert!(lines[1].starts_with("docker tag "), "{lines:?}");
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());

    // Resolving reads a manifest and runs nothing, so `--explain` names the
    // file instead of the reference it holds.
    let out = xtask(&shim, None, &[SERVICE, "--explain"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let printed = stdout(&out);
    assert!(
        printed.starts_with(&format!("(reads ci/{SERVICE}/")),
        "stdout: {printed:?}"
    );
    assert!(printed.ends_with("/Dockerfile)\n"), "stdout: {printed:?}");
}
