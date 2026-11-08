//! The process-level contract of `cargo xtask attribution` around the
//! generator: the status it reports, and what it does with a dropped
//! clarification.
//!
//! `cargo` is a shim on `PATH` that answers `about` and hands everything else
//! to the real one. The run stops before it writes `THIRD-PARTY.md`. The shim
//! is a shell script, so this binary is empty off unix.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const DROPPED: &str = "[WARN] failed to validate all files specified in clarification for crate \
                       miniz_oxide 0.9.1: checksum mismatch, expected '00'";

/// A directory holding a `cargo` that runs a script for `about`.
struct Shim(PathBuf);

impl Shim {
    /// `about` prints to stdout, logs `log` and exits `code`.
    fn new(name: &str, log: &str, code: i32) -> Self {
        Self::running(
            name,
            &format!("echo on-stdout; echo \"{log}\" >&2; exit {code}"),
        )
    }

    fn running(name: &str, about: &str) -> Self {
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
                 if [ \"$1\" = about ]; then {about}; fi\n\
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
        self.command().output().unwrap()
    }

    fn command(&self) -> Command {
        let mut dirs = vec![self.0.clone()];
        dirs.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
        command
            .arg("attribution")
            .env("PATH", std::env::join_paths(dirs).unwrap());
        command
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
    assert_eq!(String::from_utf8_lossy(&out.stdout), "on-stdout\n");
}

/// A generator's log line reaches stderr while it runs, so a step killed at its
/// timeout still shows what it printed.
#[test]
fn a_log_line_arrives_while_the_generator_runs() {
    let dir = std::env::temp_dir().join(format!(
        "spate-xtask-attribution-{}-held",
        std::process::id()
    ));
    let release = dir.join("release");
    let shim = Shim::running(
        "held",
        &format!(
            "echo holding >&2; read -r _ < '{}'; exit 7",
            release.display()
        ),
    );
    assert!(
        Command::new("mkfifo")
            .arg(&release)
            .status()
            .unwrap()
            .success()
    );
    // Open until the run ends, so the shell's open always finds a writer and
    // the release waits in the pipe however late the shell opens it. Read and
    // write, so neither the open nor the write waits for a reader.
    let mut release = std::fs::File::options()
        .read(true)
        .write(true)
        .open(&release)
        .unwrap();

    let mut child = shim
        .command()
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stream = child.stderr.take().unwrap();
    let (lines, arrived) = mpsc::channel();
    #[expect(
        clippy::disallowed_methods,
        reason = "ends when the child closes stderr"
    )]
    std::thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            if lines.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let held = loop {
        match arrived.recv_timeout(Duration::from_secs(60)) {
            Ok(line) if line == "holding" => break true,
            Ok(_) => {}
            Err(_) => break false,
        }
    };

    release.write_all(b"\n").unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(7));
    assert!(held, "the line arrived only once the generator ended");
}
