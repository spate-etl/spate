//! The `absolute_paths` lint, the thread spawn bans and the std map bans as
//! the workspace configures them, and the `-D warnings` the clippy gate
//! appends, which a warn-level entry relies on.

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

/// Runs `cargo clippy -D warnings` over a crate holding `source` as its
/// library, which takes the workspace's clippy lint table and the
/// repository's `clippy.toml`, and returns whether it passed and its stderr.
fn clippy_probe(name: &str, source: &str) -> (bool, String) {
    let scratch = Scratch::new(name);
    let root = &scratch.0;

    let manifest = format!(
        "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
         [workspace]\n\n[lints.clippy]\n{}",
        workspace_clippy_lints()
    );
    fs::write(root.join("Cargo.toml"), manifest).unwrap();
    fs::write(
        root.join("Cargo.lock"),
        format!("version = 4\n\n[[package]]\nname = \"{name}\"\nversion = \"0.0.0\"\n"),
    )
    .unwrap();
    fs::copy(repo_root().join("clippy.toml"), root.join("clippy.toml")).unwrap();
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
    (
        got.status.success(),
        String::from_utf8_lossy(&got.stderr).into_owned(),
    )
}

/// A crate that takes the workspace's clippy lint table and the repository's
/// `clippy.toml` calls one standard library item through a three-segment path
/// and another through a four-segment path. The four-segment path fails
/// `clippy -D warnings` and the three-segment path does not, which pins both
/// the `absolute_paths` entry and the limit in `clippy.toml`.
#[test]
fn absolute_paths_limit_is_three() {
    let source = "\
pub fn three_segments() -> std::sync::Mutex<u8> {
    std::sync::Mutex::new(0)
}

pub fn four_segments() -> u8 {
    std::sync::atomic::AtomicU8::new(0).into_inner()
}
";
    let (passed, stderr) = clippy_probe("limit-probe", source);

    assert!(!passed, "{stderr}");
    assert!(
        stderr.contains("consider bringing this path into scope"),
        "{stderr}"
    );
    let four = line_of(source, "atomic::AtomicU8");
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

/// A crate under the repository's `clippy.toml` that starts a thread through
/// `std::thread::spawn` and through `std::thread::Builder::spawn` fails
/// `clippy -D warnings` with `disallowed_methods` at each call, which pins
/// both thread entries.
#[test]
fn disallowed_thread_spawns_fail_the_gate() {
    let source = "\
use std::thread;

pub fn plain() {
    thread::spawn(|| ()).join().unwrap();
}

pub fn built() {
    thread::Builder::new().spawn(|| ()).unwrap().join().unwrap();
}

pub fn unchecked() {
    // SAFETY: the closure borrows nothing, so it outlives no reference.
    let handle = unsafe { thread::Builder::new().spawn_unchecked(|| ()) };
    handle.unwrap().join().unwrap();
}
";
    let (passed, stderr) = clippy_probe("spawn-probe", source);

    assert!(!passed, "{stderr}");
    for (needle, path) in [
        ("thread::spawn(", "std::thread::spawn"),
        (".spawn(|| ())", "std::thread::Builder::spawn"),
        (".spawn_unchecked(", "std::thread::Builder::spawn_unchecked"),
    ] {
        let line = line_of(source, needle);
        let reported = format!("src/lib.rs:{line}:");
        let message = format!("use of a disallowed method `{path}`");
        assert!(
            stderr
                .lines()
                .any(|l| l.starts_with(&reported) && l.contains(&message)),
            "no `{path}` finding at line {line}\n{stderr}"
        );
    }
}

/// A crate under the repository's `clippy.toml` that names std's
/// `HashMap` or `HashSet`, by import, by annotation, by a type-relative call
/// or through the `hash_map` module, fails `clippy -D warnings` with
/// `disallowed_types` at each line, which pins both entries.
#[test]
fn disallowed_std_maps_fail_the_gate() {
    let source = "\
use std::collections::HashMap;
use std::collections::HashSet;

pub fn annotated() -> usize {
    let set: std::collections::HashSet<u8> = [1].into_iter().collect();
    set.len()
}

pub fn constructed() -> usize {
    HashMap::<u8, u8>::new().len()
}

pub fn through_module(map: &std::collections::hash_map::HashMap<u8, u8>) -> usize {
    map.len()
}

pub fn imported(set: &HashSet<u8>) -> usize {
    set.len()
}
";
    let (passed, stderr) = clippy_probe("map-probe", source);

    assert!(!passed, "{stderr}");
    for (needle, path) in [
        ("use std::collections::HashMap;", "std::collections::HashMap"),
        ("use std::collections::HashSet;", "std::collections::HashSet"),
        ("let set: std::collections::HashSet", "std::collections::HashSet"),
        ("HashMap::<u8, u8>::new()", "std::collections::HashMap"),
        ("hash_map::HashMap<u8, u8>", "std::collections::HashMap"),
    ] {
        let line = line_of(source, needle);
        let reported = format!("src/lib.rs:{line}:");
        let message = format!("use of a disallowed type `{path}`");
        assert!(
            stderr
                .lines()
                .any(|l| l.starts_with(&reported) && l.contains(&message)),
            "no `{path}` finding at line {line}\n{stderr}"
        );
    }
}

/// An alias over std's map with another hasher, defined under an `expect`,
/// passes the gate when it is used as an annotation, through `default`,
/// through `collect` and through a method call, which is the shape
/// `spate_core::hash` takes.
#[test]
fn std_map_aliases_pass_the_gate() {
    let source = "\
use std::hash::{BuildHasherDefault, DefaultHasher};

#[expect(clippy::disallowed_types, reason = \"the alias is the one sanctioned spelling\")]
pub type Map<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<DefaultHasher>>;

#[expect(clippy::disallowed_types, reason = \"the alias is the one sanctioned spelling\")]
pub type Set<T> = std::collections::HashSet<T, BuildHasherDefault<DefaultHasher>>;

pub fn used() -> usize {
    let mut map: Map<u8, u8> = Map::default();
    map.insert(1, 1);
    let set = [1u8].into_iter().collect::<Set<_>>();
    map.len() + set.len()
}
";
    let (passed, stderr) = clippy_probe("alias-probe", source);

    assert!(passed, "{stderr}");
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
