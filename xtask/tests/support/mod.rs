//! The scratch directory, shim writer and runner the xtask integration tests
//! share.
//!
//! `Scratch` is the binary's own `checks/scratch.rs`, included by path because
//! spate-xtask has no library target for a test to import it from.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(
    dead_code,
    unused_imports,
    reason = "each target uses a different subset"
)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[path = "../../src/checks/scratch.rs"]
mod scratch;

pub(crate) use scratch::Scratch;

/// The xtask binary, with the environment the test runner passed it.
pub(crate) fn xtask() -> Command {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
}

/// One run of the binary with `args`, every variable in `clear` removed and
/// `env` set after it.
pub(crate) fn run(args: &[&str], clear: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = xtask();
    command.args(args);
    for name in clear {
        command.env_remove(name);
    }
    command.envs(env.iter().copied());
    command.output().unwrap()
}

/// The exit status, stdout and stderr of one run, all three held at once.
#[track_caller]
pub(crate) fn held(args: &[&str], got: &Output, code: i32, out: &str, err: &str) {
    let stdout = String::from_utf8_lossy(&got.stdout);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert_eq!(stdout, out, "stdout of {args:?}");
    assert_eq!(stderr, err, "stderr of {args:?}");
    assert_eq!(got.status.code(), Some(code), "{stdout}{stderr}");
}

/// Writes an executable `name` holding `body` into `dir`.
#[cfg(unix)]
pub(crate) fn shim(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// `PATH` with `dir` ahead of what this process inherited, so a shim there
/// shadows the real program.
pub(crate) fn path_with(dir: &Path) -> OsString {
    let mut dirs = vec![dir.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        dirs.extend(std::env::split_paths(&path));
    }
    std::env::join_paths(dirs).unwrap()
}
