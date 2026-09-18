//! The process-level contract of `cargo xtask bench gungraun`: the exit status
//! each outcome reports, the argv it hands `cargo`, and what the child
//! inherits.
//!
//! `cargo` is a recording shim on `PATH`, so nothing here builds or runs a
//! bench. The shim is a shell script, so this binary is empty off unix.
#![cfg(unix)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

/// A crate the tree benches, and the number of targets it owns.
const PKG: &str = "spate-core";

/// A crate name no bench belongs to.
const GHOST: &str = "__no_such_package__";

/// What the harness writes to the runner's own stdin. A child inheriting it
/// would swallow the bytes and report them back.
const SENTINEL: &str = "sentinel";

/// A directory holding a `cargo` that records its argv and builds nothing.
struct Shim(PathBuf);

impl Shim {
    /// A shim whose every invocation exits with `code`.
    fn new(name: &str, code: i32) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-gungraun-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let me = Self(dir);
        // One NUL-terminated field per argument, one line per invocation, then
        // the GUNGRAUN_* environment and whatever stdin held.
        std::fs::write(
            me.0.join("cargo"),
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do printf '%s\\0' \"$a\" >>\"$SPATE_CARGO_LOG\"; done\n\
                 printf '\\n' >>\"$SPATE_CARGO_LOG\"\n\
                 env | grep '^GUNGRAUN_' | sort >>\"$SPATE_CARGO_LOG.env\"\n\
                 printf 'stdin=[%s]\\n' \"$(cat)\" >>\"$SPATE_CARGO_LOG.stdin\"\n\
                 exit {code}\n"
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(me.0.join("cargo"), std::fs::Permissions::from_mode(0o755))
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

    fn side(&self, suffix: &str) -> String {
        std::fs::read_to_string(self.0.join(format!("argv.{suffix}"))).unwrap_or_default()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

/// One run of the task runner with the shim ahead of anything else on `PATH`,
/// its stdin carrying [`SENTINEL`].
fn xtask(shim: &Shim, args: &[&str]) -> Output {
    let path = match std::env::var_os("PATH") {
        Some(p) => {
            let mut dirs = vec![shim.0.clone()];
            dirs.extend(std::env::split_paths(&p));
            std::env::join_paths(dirs).unwrap()
        }
        None => shim.0.clone().into_os_string(),
    };
    run_with(shim, path, args)
}

/// One run whose `PATH` holds no `cargo` at all.
fn xtask_without_cargo(shim: &Shim, args: &[&str]) -> Output {
    let empty = shim.0.join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    run_with(shim, empty.into_os_string(), args)
}

fn run_with(shim: &Shim, path: std::ffi::OsString, args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["bench", "gungraun"])
        .args(args)
        .env("PATH", path)
        .env("SPATE_CARGO_LOG", shim.log())
        .env("GUNGRAUN_SAVE_BASELINE", "base")
        // The annotation prefix would otherwise depend on the host.
        .env_remove("GITHUB_ACTIONS")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(SENTINEL.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The `pkg bench` lines bare discovery prints, which every other case counts
/// against.
fn listing(shim: &Shim) -> Vec<String> {
    let out = xtask(shim, &[]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    stdout(&out).lines().map(str::to_owned).collect()
}

/// Every bench succeeding is success, and each one reaches cargo named, locked
/// and by target.
#[test]
fn a_run_that_succeeds_exits_zero_and_names_every_target() {
    let shim = Shim::new("run-ok", 0);
    let lines = listing(&shim);
    assert!(!lines.is_empty(), "the tree holds no gungraun bench");

    let out = xtask(&shim, &["--run"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");

    let calls = shim.calls();
    assert_eq!(calls.len(), lines.len(), "{calls:?}");
    for (call, line) in calls.iter().zip(&lines) {
        let (pkg, bench) = line.split_once(' ').unwrap();
        assert_eq!(
            call,
            &["bench", "-p", pkg, "--locked", "--bench", bench],
            "{line}"
        );
    }
}

/// A failing bench is exit 1, every remaining target still runs, and each
/// failure is named.
#[test]
fn a_failing_bench_exits_one_and_the_rest_still_run() {
    let shim = Shim::new("run-fail", 3);
    let out = xtask(&shim, &["--run", PKG]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));

    let calls = shim.calls();
    assert!(!calls.is_empty(), "{calls:?}");
    let reported = stderr(&out);
    for call in &calls {
        let bench = call.last().unwrap();
        assert!(
            reported.contains(&format!("gungraun-benches: {PKG} --bench {bench} failed\n")),
            "{reported}"
        );
    }
    assert!(!reported.contains("failed to build"), "{reported}");
}

/// An empty selection is exit 2, invokes no cargo, and reports one line naming
/// the filter that matched nothing.
#[test]
fn an_empty_selection_exits_two_and_names_the_filter() {
    let shim = Shim::new("empty-selection", 0);
    for args in [
        vec!["--run", GHOST],
        vec!["--check", GHOST],
        vec!["--features", "simd", "--run", GHOST],
        vec!["--features", "", "--run", GHOST],
    ] {
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {}", stderr(&out));
        assert_eq!(
            stderr(&out),
            format!("gungraun-benches: no bench target belongs to {GHOST}\n"),
            "{args:?}"
        );
        assert_eq!(stdout(&out), "", "{args:?}");
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// A failing cargo and an empty selection must not look alike, whatever the
/// child's own status was.
#[test]
fn an_empty_selection_is_two_even_when_cargo_would_fail() {
    let shim = Shim::new("empty-selection-failing-cargo", 3);
    let out = xtask(&shim, &["--features", "simd", "--run", GHOST]);
    assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// A cargo that cannot be spawned reaches the failed-bench arm at exit 1.
#[test]
fn a_cargo_that_is_not_on_path_exits_one() {
    let shim = Shim::new("no-cargo", 0);
    let out = xtask_without_cargo(&shim, &["--run", PKG]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let reported = stderr(&out);
    assert!(reported.contains("gungraun-benches: cargo: "), "{reported}");
    assert!(reported.contains("--bench"), "{reported}");
}

/// A filter selects the crates it names, and drops a name no crate carries.
#[test]
fn a_filter_selects_the_crates_it_names() {
    let shim = Shim::new("filter", 0);
    let out = xtask(&shim, &["--run", PKG, GHOST]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let calls = shim.calls();
    assert!(!calls.is_empty(), "{calls:?}");
    for call in &calls {
        assert_eq!(call[1..3], ["-p", PKG], "{call:?}");
    }
    let expected = listing(&shim)
        .iter()
        .filter(|l| l.starts_with(&format!("{PKG} ")))
        .count();
    assert_eq!(calls.len(), expected, "{calls:?}");
}

/// The feature arm reaches every cargo invocation and no other, as one
/// argument whatever it holds. A bench measured under a different arm than its
/// siblings is not comparable with them.
#[test]
fn a_feature_arm_reaches_every_invocation_and_no_other() {
    for (name, features) in [("one", "simd"), ("two", "simd,other"), ("spaced", "a b")] {
        let shim = Shim::new(&format!("features-{name}"), 0);
        let out = xtask(&shim, &["--features", features, "--run", PKG]);
        assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
        let calls = shim.calls();
        assert!(!calls.is_empty(), "{calls:?}");
        for call in &calls {
            assert_eq!(call[call.len() - 2..], ["--features", features], "{call:?}");
        }
    }
}

/// The empty arm is the crate's default features, and passes no flag.
#[test]
fn the_default_arm_passes_no_feature_flag() {
    let shim = Shim::new("features-default", 0);
    for args in [
        vec!["--features", "", "--run", PKG],
        vec!["--features", "", "--check", PKG],
        vec!["--run", PKG],
    ] {
        std::fs::remove_file(shim.log()).ok();
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(0), "{args:?}: {}", stderr(&out));
        let calls = shim.calls();
        assert!(!calls.is_empty(), "{args:?}");
        for call in &calls {
            assert!(!call.iter().any(|a| a == "--features"), "{call:?}");
        }
    }
}

/// `--check` builds and runs nothing, and says so when a target fails.
#[test]
fn a_check_builds_without_running() {
    let shim = Shim::new("check", 0);
    let out = xtask(&shim, &["--features", "simd", "--check", PKG]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let calls = shim.calls();
    assert!(!calls.is_empty(), "{calls:?}");
    for call in &calls {
        assert_eq!(call[..2], ["bench", "--no-run"], "{call:?}");
        assert_eq!(call[call.len() - 2..], ["--features", "simd"], "{call:?}");
    }

    let shim = Shim::new("check-fail", 3);
    let out = xtask(&shim, &["--check", PKG]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains(&format!("gungraun-benches: {PKG} --bench")),
        "{}",
        stderr(&out)
    );
    assert!(stderr(&out).contains("failed to build"), "{}", stderr(&out));
}

/// The child is handed a closed stdin, so it cannot swallow what the runner is
/// reading, and it inherits the variables the base leg sets.
#[test]
fn the_child_gets_no_stdin_and_inherits_the_environment() {
    let shim = Shim::new("child", 0);
    let out = xtask(&shim, &["--run", PKG]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let stdin = shim.side("stdin");
    let seen: Vec<&str> = stdin.lines().collect();
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|l| *l == "stdin=[]"), "{seen:?}");
    assert!(!stdin.contains(SENTINEL));

    let inherited = shim.side("env");
    let env: Vec<&str> = inherited.lines().collect();
    assert!(!env.is_empty());
    assert!(
        env.iter().all(|l| *l == "GUNGRAUN_SAVE_BASELINE=base"),
        "{env:?}"
    );
}

/// Each invocation is traced on stderr ahead of the child's own output, and
/// the trace is the shell form of what ran.
#[test]
fn every_invocation_is_traced_on_stderr() {
    let shim = Shim::new("trace", 0);
    let out = xtask(&shim, &["--features", "a b", "--run", PKG]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let reported = stderr(&out);
    let traced: Vec<&str> = reported.lines().collect();
    assert_eq!(traced.len(), shim.calls().len(), "{traced:?}");
    for line in &traced {
        assert!(line.starts_with("+ cargo bench -p "), "{line}");
        assert!(line.ends_with(" --features 'a b'"), "{line}");
    }
}

/// `--pkgs-json` prints the crates owning a target, and takes precedence over
/// a selection given alongside it.
#[test]
fn pkgs_json_prints_the_owning_crates_and_runs_nothing() {
    let shim = Shim::new("pkgs-json", 0);
    let mut expected: Vec<String> = listing(&shim)
        .iter()
        .map(|l| l.split(' ').next().unwrap().to_owned())
        .collect();
    expected.dedup();
    let rendered = format!(
        "[{}]\n",
        expected
            .iter()
            .map(|p| format!("\"{p}\""))
            .collect::<Vec<_>>()
            .join(",")
    );

    for args in [
        vec!["--pkgs-json"],
        vec!["--features", "simd", "--pkgs-json"],
        vec!["--pkgs-json", "--run", PKG],
        vec!["--pkgs-json", "--check", PKG],
    ] {
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(0), "{args:?}: {}", stderr(&out));
        assert_eq!(stdout(&out), rendered, "{args:?}");
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// Naming no mode prints what would run, whether or not an arm is named.
#[test]
fn naming_no_mode_prints_the_discovered_targets() {
    let shim = Shim::new("listing", 0);
    let bare = listing(&shim);
    for args in [vec!["--features", "simd"], vec!["--features", ""]] {
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(0), "{args:?}: {}", stderr(&out));
        assert_eq!(stdout(&out).lines().collect::<Vec<_>>(), bare, "{args:?}");
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// `--explain` prints the plan and runs none of it.
#[test]
fn explain_prints_the_plan_and_runs_nothing() {
    let shim = Shim::new("explain", 0);
    let expected = listing(&shim).len();

    let out = xtask(&shim, &["--run", "--explain"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let printed = stdout(&out);
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(lines.len(), expected, "{lines:?}");
    assert!(
        lines.iter().all(|l| l.starts_with("cargo bench -p ")),
        "{lines:?}"
    );
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// The listing and the package array both name what they would read under
/// `--explain`, and read nothing.
#[test]
fn explain_names_what_the_listing_and_the_package_array_read() {
    let shim = Shim::new("explain-reads", 0);
    for args in [vec!["--explain"], vec!["--pkgs-json", "--explain"]] {
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(0), "{args:?}: {}", stderr(&out));
        assert_eq!(
            stdout(&out),
            "(reads crates/*/benches/*_gungraun.rs)\n",
            "{args:?}"
        );
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}

/// The counted tier runs the benches, then guards the collected regions. A
/// failing bench stops it before the guard.
#[test]
fn the_counted_tier_runs_the_benches_then_the_region_guard() {
    let shim = Shim::new("counted", 0);
    let expected = listing(&shim).len();

    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["bench", "counted", "--explain"])
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let printed = stdout(&out);
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(lines.len(), expected + 1, "{lines:?}");
    assert!(
        lines[..expected]
            .iter()
            .all(|l| l.starts_with("cargo bench -p ")),
        "{lines:?}"
    );
    assert_eq!(lines[expected], "./scripts/gungraun-collected-region.sh");

    let shim = Shim::new("counted-fail", 3);
    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["bench", "counted"])
        .env("PATH", {
            let mut dirs = vec![shim.0.clone()];
            dirs.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
            std::env::join_paths(dirs).unwrap()
        })
        .env("SPATE_CARGO_LOG", shim.log())
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(shim.calls().len(), expected, "{:?}", shim.calls());
    let reported = stderr(&out);
    assert!(!reported.contains("collected-region"), "{reported}");
}

/// `tidy` reaches the gate, which reports every target it held to its stanza.
#[test]
fn the_tidy_check_runs_the_gate() {
    let shim = Shim::new("tidy", 0);
    let expected = listing(&shim).len();

    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["tidy", "gungraun-benches"])
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        format!("gungraun-benches: {expected} bench target(s) are declared correctly\n")
    );
}

/// A malformed command line is rejected before anything runs.
#[test]
fn a_malformed_command_line_runs_nothing() {
    let shim = Shim::new("usage", 0);
    for args in [vec!["--bogus"], vec![PKG], vec!["--features"]] {
        let out = xtask(&shim, &args);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {}", stderr(&out));
        assert_eq!(stdout(&out), "", "{args:?}");
    }
    assert!(shim.calls().is_empty(), "{:?}", shim.calls());
}
