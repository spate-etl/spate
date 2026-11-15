//! The install contract of `.github/actions/setup-rust/install-toolchain.sh`:
//! the rustup argv it builds, when it retries, and the status it ends with.
//!
//! `rustup` and `sleep` are recording shims ahead of anything else on `PATH`,
//! so nothing here downloads a toolchain or waits. The script runs under
//! `/bin/bash`, which is bash 3.2 on macOS, as the macOS runner runs it. The
//! shims are shell scripts, so this binary is empty off unix.
#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Output};

mod support;

/// rustup's two lines for a checksum failure during a release-server update.
const CHECKSUM: &str = "error: checksum failed for 'https://static.rust-lang.org/dist/x.tar.xz', \
                        expected: 'a', calculated: 'b'\n";
const SERVER: &str = "warn: this is likely due to an ongoing update of the official release \
                      server, please try again later\n";

/// A directory holding a `rustup` and a `sleep` that record their argv, and a
/// working directory with one file in it for a glob to match.
struct Shim(support::Scratch);

impl Shim {
    fn new(name: &str) -> Self {
        let dir = support::Scratch::new(&format!("spate-xtask-setup-rust-{name}")).unwrap();
        for sub in ["bin", "fix", "cwd"] {
            std::fs::create_dir(dir.join(sub)).unwrap();
        }
        std::fs::write(dir.join("cwd/held"), "").unwrap();
        // One NUL-terminated field per argument, one line per invocation. The
        // nth `toolchain install` fails when `fix/fail.<n>` exists, printing it
        // to stderr and exiting `fix/code.<n>`, or 1.
        let rustup = format!(
            "#!/bin/sh\n\
             printf '%s\\0' \"$@\" >>'{log}.rustup'\n\
             printf '\\n' >>'{log}.rustup'\n\
             case \"$1\" in\n\
             --version) echo 'rustup 1.29.3' ;;\n\
             toolchain)\n\
               n=1\n\
               if [ -f '{log}.n' ]; then n=$(( $(cat '{log}.n') + 1 )); fi\n\
               echo \"$n\" >'{log}.n'\n\
               if [ -f '{fix}/fail.'$n ]; then\n\
                 cat '{fix}/fail.'$n >&2\n\
                 code=1\n\
                 if [ -f '{fix}/code.'$n ]; then code=$(cat '{fix}/code.'$n); fi\n\
                 exit $code\n\
               fi\n\
               echo \"installed $3\" ;;\n\
             esac\n",
            log = dir.join("argv").display(),
            fix = dir.join("fix").display(),
        );
        let sleep = format!(
            "#!/bin/sh\n\
             printf '%s\\0' \"$@\" >>'{log}.sleep'\n\
             printf '\\n' >>'{log}.sleep'\n",
            log = dir.join("argv").display(),
        );
        support::shim(&dir.join("bin"), "rustup", &rustup);
        support::shim(&dir.join("bin"), "sleep", &sleep);
        Self(dir)
    }

    /// The `n`th install fails with `stderr` and exits `code`.
    fn fails(&self, n: u32, stderr: &str, code: i32) -> &Self {
        std::fs::write(self.0.join(&format!("fix/fail.{n}")), stderr).unwrap();
        std::fs::write(self.0.join(&format!("fix/code.{n}")), code.to_string()).unwrap();
        self
    }

    /// Every invocation of one shim so far, in order.
    fn calls(&self, program: &str) -> Vec<Vec<String>> {
        std::fs::read_to_string(self.0.join(&format!("argv.{program}")))
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

    /// The `rustup` calls that were installs.
    fn installs(&self) -> usize {
        self.calls("rustup")
            .iter()
            .filter(|c| c.first().is_some_and(|a| a == "toolchain"))
            .count()
    }

    /// Whether `rustup default` ran.
    fn defaulted(&self) -> bool {
        self.calls("rustup")
            .iter()
            .any(|c| c.first().is_some_and(|a| a == "default"))
    }

    /// One run of the script installing `stable` with `components`.
    fn run(&self, components: &str) -> Output {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(".github/actions/setup-rust/install-toolchain.sh");
        Command::new("/bin/bash")
            .arg(script)
            .current_dir(self.0.join("cwd"))
            .env("PATH", support::path_with(&self.0.join("bin")))
            .env("TOOLCHAIN", "stable")
            .env("COMPONENTS", components)
            .output()
            .unwrap()
    }
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|&a| a.to_owned()).collect()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// One install with a flag per component, split on commas and spaces with
/// globbing off, then the default; rustup's output reaches the log.
#[test]
fn a_clean_install_sets_the_default() {
    let shim = Shim::new("clean");
    let out = shim.run("rustfmt, clippy *");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        shim.calls("rustup"),
        [
            args(&["--version"]),
            args(&[
                "toolchain",
                "install",
                "stable",
                "--component",
                "rustfmt",
                "--component",
                "clippy",
                "--component",
                "*",
                "--profile",
                "minimal",
                "--no-self-update",
            ]),
            args(&["default", "stable"]),
        ]
    );
    assert!(shim.calls("sleep").is_empty());
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("installed stable\n"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// No components gives no `--component` flag at all.
#[test]
fn no_components_passes_no_component_flag() {
    let shim = Shim::new("none");
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        shim.calls("rustup")[1],
        args(&[
            "toolchain",
            "install",
            "stable",
            "--profile",
            "minimal",
            "--no-self-update",
        ])
    );
}

/// A checksum failure rustup blames on a release-server update is retried
/// after a pause, and the default follows the install that succeeds.
#[test]
fn a_release_server_checksum_failure_retries() {
    let shim = Shim::new("retry");
    let both = format!("{CHECKSUM}{SERVER}");
    shim.fails(1, &both, 1).fails(2, &both, 1);
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(shim.installs(), 3);
    assert_eq!(shim.calls("sleep"), [args(&["30"]), args(&["30"])]);
    assert!(shim.defaulted());
}

/// Any other failure ends the step at once with rustup's status.
#[test]
fn another_failure_does_not_retry() {
    let shim = Shim::new("other");
    shim.fails(1, "error: no such toolchain\n", 7);
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(7), "{}", stderr(&out));
    assert_eq!(shim.installs(), 1);
    assert!(shim.calls("sleep").is_empty());
    assert!(!shim.defaulted());
}

/// A checksum failure with no word of the release server is not retried.
#[test]
fn a_checksum_line_without_the_server_line_does_not_retry() {
    let shim = Shim::new("checksum-only");
    shim.fails(1, CHECKSUM, 3);
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(shim.installs(), 1);
    assert!(shim.calls("sleep").is_empty());
}

/// The release-server line on its own is not retried.
#[test]
fn a_server_line_without_the_checksum_line_does_not_retry() {
    let shim = Shim::new("server-only");
    shim.fails(1, SERVER, 3);
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(shim.installs(), 1);
    assert!(shim.calls("sleep").is_empty());
}

/// Each attempt is judged on its own output, so a different failure after a
/// retried one ends the step.
#[test]
fn a_later_failure_is_judged_on_its_own_output() {
    let shim = Shim::new("later");
    shim.fails(1, &format!("{CHECKSUM}{SERVER}"), 1)
        .fails(2, "error: no such toolchain\n", 7);
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(7), "{}", stderr(&out));
    assert_eq!(shim.installs(), 2);
    assert_eq!(shim.calls("sleep"), [args(&["30"])]);
    assert!(!shim.defaulted());
}

/// Retries stop after five attempts with the last status, and no default.
#[test]
fn retries_stop_after_five_attempts() {
    let shim = Shim::new("bound");
    let both = format!("{CHECKSUM}{SERVER}");
    for n in 1..=6 {
        shim.fails(n, &both, 9);
    }
    let out = shim.run("");
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(shim.installs(), 5);
    assert_eq!(shim.calls("sleep").len(), 4);
    assert!(!shim.defaulted());
}
