//! The `absolute_paths` lint as the workspace configures it, and the
//! `-D warnings` the clippy gate appends, which a warn-level entry relies on.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// A scratch directory removed when it goes out of scope, on a failed
/// assertion too.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("spate-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("src")).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(args)
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap()
}

/// The entries under `[workspace.lints.clippy]` in the root manifest, as the
/// lines the manifest holds.
fn workspace_clippy_lints() -> String {
    let manifest = fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let section = manifest
        .split_once("[workspace.lints.clippy]\n")
        .expect("the root manifest has a [workspace.lints.clippy] table")
        .1;
    let end = section.find("\n[").map_or(section.len(), |at| at + 1);
    section[..end].to_owned()
}

/// The line, counted from 1, of the first line of `source` holding `needle`.
fn line_of(source: &str, needle: &str) -> usize {
    source
        .lines()
        .position(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no line holds {needle}"))
        + 1
}

/// A crate that takes the workspace's clippy lint table and the repository's
/// `clippy.toml` calls one standard library item through a three-segment path
/// and another through a four-segment path. The four-segment path fails
/// `clippy -D warnings` and the three-segment path does not, which pins both
/// the `absolute_paths` entry and the limit in `clippy.toml`.
#[test]
fn absolute_paths_limit_is_three() {
    let scratch = Scratch::new("clippy-limit");
    let root = &scratch.0;

    let manifest = format!(
        "[package]\nname = \"limit-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
         [workspace]\n\n[lints.clippy]\n{}",
        workspace_clippy_lints()
    );
    fs::write(root.join("Cargo.toml"), manifest).unwrap();
    fs::write(
        root.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"limit-probe\"\nversion = \"0.0.0\"\n",
    )
    .unwrap();
    fs::copy(repo_root().join("clippy.toml"), root.join("clippy.toml")).unwrap();

    let source = "\
pub fn three_segments() -> std::sync::Mutex<u8> {
    std::sync::Mutex::new(0)
}

pub fn four_segments() -> usize {
    std::collections::hash_map::HashMap::<u8, u8>::new().len()
}
";
    fs::write(root.join("src/lib.rs"), source).unwrap();

    let got = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args([
            "clippy",
            "--locked",
            "--message-format",
            "short",
            "--",
            "-D",
            "warnings",
        ])
        .current_dir(root)
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&got.stderr);

    assert!(!got.status.success(), "{stderr}");
    assert!(
        stderr.contains("consider bringing this path into scope"),
        "{stderr}"
    );
    let four = line_of(source, "hash_map::HashMap");
    assert!(stderr.contains(&format!("src/lib.rs:{four}:")), "{stderr}");
    for line in [
        line_of(source, "-> std::sync::Mutex"),
        line_of(source, "Mutex::new"),
    ] {
        assert!(
            !stderr.contains(&format!("src/lib.rs:{line}:")),
            "line {line} is a three-segment path\n{stderr}"
        );
    }
}

/// The gate appends `-D warnings`, so a lint at `warn` fails the run, and
/// `--no-deny-warnings` leaves it off.
#[test]
fn clippy_denies_warnings_by_default() {
    let denied = xtask(&["--explain", "clippy"]);
    let stdout = String::from_utf8_lossy(&denied.stdout);
    assert_eq!(denied.status.code(), Some(0), "{stdout}");
    assert!(stdout.trim_end().ends_with("-- -D warnings"), "{stdout}");

    let allowed = xtask(&["--explain", "clippy", "--no-deny-warnings"]);
    let stdout = String::from_utf8_lossy(&allowed.stdout);
    assert_eq!(allowed.status.code(), Some(0), "{stdout}");
    assert!(!stdout.contains("-D warnings"), "{stdout}");
}
