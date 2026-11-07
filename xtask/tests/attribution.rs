//! The process-level contract of `cargo xtask attribution` around the
//! generator: the status it reports, and what it does with a dropped
//! clarification.
//!
//! `cargo` is a shim on `PATH` that answers `about` and hands everything else
//! to the real one. The run stops before it writes `THIRD-PARTY.md`. The shim
//! is a shell script, so this binary is empty off unix.
#![cfg(unix)]

use std::path::PathBuf;
use std::process::{Command, Output};

const DROPPED: &str = "[WARN] failed to validate all files specified in clarification for crate \
                       miniz_oxide 0.9.1: checksum mismatch, expected '00'";

/// A directory holding a `cargo` whose `about` logs `log` and exits `code`.
struct Shim(PathBuf);

impl Shim {
    fn new(name: &str, log: &str, code: i32) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-attribution-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let cargo = env!("CARGO");
        std::fs::write(
            dir.join("cargo"),
            format!(
                "#!/bin/sh\n\
                 if [ \"$1\" = about ]; then echo \"{log}\" >&2; exit {code}; fi\n\
                 exec '{cargo}' \"$@\"\n"
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.join("cargo"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        Self(dir)
    }

    fn attribution(&self) -> Output {
        let mut dirs = vec![self.0.clone()];
        dirs.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
            .arg("attribution")
            .env("PATH", std::env::join_paths(dirs).unwrap())
            .output()
            .unwrap()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// A dropped clarification fails a run the generator passed, naming the crate,
/// and the warning reaches the log once. Regression for #802.
#[test]
fn a_dropped_clarification_fails_the_run() {
    let out = Shim::new("dropped", DROPPED, 0).attribution();
    let stderr = stderr(&out);
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("dropped the clarification of:\n\n    miniz_oxide 0.9.1\n"),
        "{stderr}"
    );
    assert_eq!(stderr.matches(DROPPED).count(), 1, "{stderr}");
}

/// The generator's own failure keeps its status and names the command.
#[test]
fn a_failed_generator_keeps_its_status() {
    let out = Shim::new("failed", "[ERROR] no license", 3).attribution();
    let stderr = stderr(&out);
    assert_eq!(out.status.code(), Some(3), "{stderr}");
    assert!(stderr.contains("cargo about generate"), "{stderr}");
    assert!(stderr.contains("exited 3"), "{stderr}");
    assert!(stderr.contains("[ERROR] no license\n"), "{stderr}");
}
