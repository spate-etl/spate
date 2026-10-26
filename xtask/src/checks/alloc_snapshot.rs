//! The allocation snapshots: each `crates/*/benches/*_alloc.rs` target runs in
//! the release profile on the pinned stable toolchain, once per feature arm,
//! and what it prints is compared with the committed
//! `benches/alloc/<target>.<arm>.<arch>.tsv`.
//!
//! The binary prints its own header keys and one line per case. This module
//! prepends the keys every file carries (the compiler, the arm's features, the
//! architecture, what the figures cover), so a compiler or backend change is
//! reported by name before any figure.

use std::collections::{BTreeMap, BTreeSet};
use std::env::consts;
use std::io::Write;
use std::path::Path;

use crate::ci::classify::feature_arms_for;
use crate::run::{self, Error, Outcome, Step};

const SUFFIX: &str = "_alloc.rs";

/// The toolchain the committed counts were taken with. A floating `stable`
/// would turn every open pull request red on a release that moves a count.
const PIN: &str = ".github/toolchains/stable/rust-toolchain.toml";

/// The architectures files are committed for. `HashMap` group width differs
/// between them, so any case that allocates a hash table differs in bytes.
const GATED: [&str; 2] = ["aarch64", "x86_64"];

const COVERS: &str = "Rust global allocator only; allocations C code makes through malloc \
                      (libzstd) are not counted; steady state after one warm pass";

const COLUMNS: &str = "case\tallocs\treallocs\tbytes";

/// One snapshot target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) pkg: String,
    pub(crate) bench: String,
}

/// One target under one feature arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Invocation {
    pub(crate) pkg: String,
    pub(crate) bench: String,
    pub(crate) arm: &'static str,
    pub(crate) features: &'static str,
}

impl Invocation {
    /// The cargo arguments that build and run it. One package and one bench
    /// per process, with no `--all-features`, so no other crate's features
    /// unify into the counted build.
    pub(crate) fn args(&self, pin: &str) -> Vec<String> {
        let mut args: Vec<String> = [
            &format!("+{pin}"),
            "bench",
            "-p",
            &self.pkg,
            "--bench",
            &self.bench,
            "--locked",
        ]
        .map(ToOwned::to_owned)
        .to_vec();
        if !self.features.is_empty() {
            args.extend(["--features".to_owned(), self.features.to_owned()]);
        }
        args
    }

    /// The committed file for `arch`, relative to the repository root.
    pub(crate) fn file(&self, arch: &str) -> String {
        format!(
            "crates/{}/benches/alloc/{}.{}.{arch}.tsv",
            self.pkg, self.bench, self.arm
        )
    }
}

/// Every snapshot target under `crates/`, sorted by package then bench.
pub(crate) fn discover(root: &Path) -> Vec<Target> {
    let mut out = Vec::new();
    for pkg in names(&root.join("crates")) {
        for file in names(&root.join("crates").join(&pkg).join("benches")) {
            if let Some(stem) = file.strip_suffix(".rs")
                && file.ends_with(SUFFIX)
            {
                out.push(Target {
                    pkg: pkg.clone(),
                    bench: stem.to_owned(),
                });
            }
        }
    }
    out.sort_by(|a, b| (&a.pkg, &a.bench).cmp(&(&b.pkg, &b.bench)));
    out
}

fn names(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

/// Each target under each of its package's feature arms.
pub(crate) fn invocations(targets: &[Target]) -> Vec<Invocation> {
    targets
        .iter()
        .flat_map(|t| {
            feature_arms_for(&t.pkg)
                .into_iter()
                .map(|(arm, features)| Invocation {
                    pkg: t.pkg.clone(),
                    bench: t.bench.clone(),
                    arm,
                    features,
                })
        })
        .collect()
}

/// Refuses a host whose counts no committed file holds.
pub(crate) fn gated_arch(os: &str, arch: &str) -> Result<(), String> {
    if os == "linux" && GATED.contains(&arch) {
        return Ok(());
    }
    Err(format!(
        "allocation snapshots are committed for Linux x86_64 and aarch64, and this host is \
         {os} {arch}; a CI run's `alloc-snapshots-<arch>` artifact holds the files for a change"
    ))
}

/// The `channel` a `rust-toolchain.toml` names.
fn channel(text: &str) -> Result<String, String> {
    text.lines()
        .find_map(|l| {
            l.strip_prefix("channel = \"")
                .and_then(|v| v.strip_suffix('"'))
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("{PIN} names no `channel = \"…\"`"))
}

/// Files under any `benches/alloc/` that no invocation writes on a gated
/// architecture, as repository-relative paths.
pub(crate) fn orphans(root: &Path, invocations: &[Invocation]) -> Vec<String> {
    let expected: BTreeSet<String> = invocations
        .iter()
        .flat_map(|i| GATED.map(|arch| i.file(arch)))
        .collect();
    let mut out = Vec::new();
    for pkg in names(&root.join("crates")) {
        for file in names(&root.join("crates").join(&pkg).join("benches/alloc")) {
            let rel = format!("crates/{pkg}/benches/alloc/{file}");
            if !expected.contains(&rel) {
                out.push(rel);
            }
        }
    }
    out.sort();
    out
}

/// The header lines this module adds above what the binary prints.
fn header(rustc: &str, inv: &Invocation, arch: &str) -> String {
    let features = if inv.features.is_empty() {
        "none"
    } else {
        inv.features
    };
    format!("# rustc: {rustc}\n# features: {features}\n# arch: {arch}\n# counts: {COVERS}\n")
}

/// A parsed file: header keys in order, then each case's three figures.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    keys: Vec<(String, String)>,
    rows: BTreeMap<String, [u64; 3]>,
}

fn parse(text: &str) -> Result<Snapshot, String> {
    let mut keys = Vec::new();
    let mut rows = BTreeMap::new();
    let mut columns = false;
    for (n, line) in text.lines().enumerate() {
        let at = |what: &str| format!("line {}: {what}", n + 1);
        if let Some(rest) = line.strip_prefix("# ") {
            let (k, v) = rest
                .split_once(": ")
                .ok_or_else(|| at("a header line is `# key: value`"))?;
            keys.push((k.to_owned(), v.to_owned()));
        } else if line == COLUMNS {
            columns = true;
        } else if !columns {
            return Err(at(&format!("expected `{COLUMNS}` before the first case")));
        } else {
            let fields: Vec<&str> = line.split('\t').collect();
            let [name, a, r, b] = fields[..] else {
                return Err(at("a case line has four tab-separated fields"));
            };
            let figure = |f: &str| {
                f.parse::<u64>()
                    .map_err(|_| at(&format!("`{f}` is not a count")))
            };
            if rows
                .insert(name.to_owned(), [figure(a)?, figure(r)?, figure(b)?])
                .is_some()
            {
                return Err(at(&format!("case `{name}` appears twice")));
            }
        }
    }
    Ok(Snapshot { keys, rows })
}

/// One line per difference between a committed file and a run.
fn differences(file: &Snapshot, run: &Snapshot) -> Vec<String> {
    let mut out = Vec::new();
    let file_keys: BTreeMap<&str, &str> = file
        .keys
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let run_keys: BTreeMap<&str, &str> = run
        .keys
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let all: BTreeSet<&str> = file_keys.keys().chain(run_keys.keys()).copied().collect();
    for k in all {
        let (old, new) = (file_keys.get(k), run_keys.get(k));
        if old != new {
            let show =
                |v: Option<&&str>| v.map_or_else(|| "(absent)".to_owned(), |v| format!("`{v}`"));
            out.push(format!(
                "header `{k}`: {} in the file, {} in this run",
                show(old),
                show(new)
            ));
        }
    }
    for (name, figures) in &run.rows {
        match file.rows.get(name) {
            None => out.push(format!("case `{name}`: not in the file")),
            Some(old) if old != figures => {
                let moved: Vec<String> = ["allocs", "reallocs", "bytes"]
                    .iter()
                    .zip(old.iter().zip(figures))
                    .filter(|(_, (o, n))| o != n)
                    .map(|(label, (o, n))| format!("{label} {o} -> {n}"))
                    .collect();
                out.push(format!("case `{name}`: {}", moved.join(", ")));
            }
            Some(_) => {}
        }
    }
    for name in file.rows.keys() {
        if !run.rows.contains_key(name) {
            out.push(format!(
                "case `{name}`: in the file, not produced by this run"
            ));
        }
    }
    out
}

/// Compares one run's full text with its committed file, or writes the file
/// under `bless`. On a difference or a missing file, the text is written under
/// `target/alloc-snapshots/` at the same relative path.
fn check_one(
    root: &Path,
    inv: &Invocation,
    arch: &str,
    text: &str,
    bless: bool,
) -> Result<(), String> {
    let rel = inv.file(arch);
    let no_case = || {
        format!(
            "{}: the run printed no case; a snapshot target is a `[[bench]]` with \
             `harness = false` whose `main` calls `spate_test_support::alloc::main_with`",
            inv.bench
        )
    };
    // A target left on the libtest harness exits 0 and prints libtest's
    // summary, which has no column line.
    if !text.lines().any(|l| l == COLUMNS) {
        return Err(no_case());
    }
    let run = parse(text).map_err(|e| format!("{}: {e}", inv.bench))?;
    if run.rows.is_empty() {
        return Err(no_case());
    }
    if bless {
        return write(&root.join(&rel), text);
    }
    let diffs = match std::fs::read_to_string(root.join(&rel)) {
        Ok(committed) => match parse(&committed) {
            Ok(file) => differences(&file, &run),
            Err(e) => vec![format!("the file does not parse: {e}")],
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => vec!["not committed".to_owned()],
        Err(e) => return Err(format!("{rel}: {e}")),
    };
    if diffs.is_empty() {
        return Ok(());
    }
    let replacement = format!("target/alloc-snapshots/{rel}");
    write(&root.join(&replacement), text)?;
    Err(format!(
        "{rel} differs from this run:\n  {}\nThe file this run produced is at {replacement}. \
         Commit it, or run `cargo xtask bench alloc --bless` on Linux {arch}.",
        diffs.join("\n  ")
    ))
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))
}

/// The pinned channel and every invocation, after refusing a tree with no
/// target or with a file no invocation writes.
fn plan(root: &Path) -> Result<(String, Vec<Invocation>), String> {
    let pin_text = std::fs::read_to_string(root.join(PIN)).map_err(|e| format!("{PIN}: {e}"))?;
    let pin = channel(&pin_text)?;
    let invs = invocations(&discover(root));
    if invs.is_empty() {
        return Err(format!(
            "no allocation snapshot target was discovered; a target is `crates/<c>/benches/<name>{SUFFIX}`"
        ));
    }
    let orphaned = orphans(root, &invs);
    if !orphaned.is_empty() {
        return Err(format!(
            "no snapshot target and arm writes these files:\n  {}",
            orphaned.join("\n  ")
        ));
    }
    Ok((pin, invs))
}

/// `cargo xtask bench alloc`: compares every snapshot on this host's
/// architecture, or rewrites them under `bless`.
pub(crate) fn run(root: &Path, explain: bool, bless: bool) -> Outcome {
    let arch = consts::ARCH;
    gated_arch(consts::OS, arch)?;
    compare(root, arch, explain, bless, &mut std::io::stdout(), |step| {
        run::capture(root, step)
    })
}

/// The body of [`run()`] past the host gate, with each step's stdout taken from
/// `capture` and `--explain`'s lines written to `out`.
fn compare(
    root: &Path,
    arch: &str,
    explain: bool,
    bless: bool,
    out: &mut impl Write,
    mut capture: impl FnMut(&Step<'_>) -> Result<String, Error>,
) -> Outcome {
    let (pin, invs) = plan(root)?;
    let rustc = Step::new("rustc", [format!("+{pin}"), "--version".to_owned()]);
    let steps: Vec<Step<'_>> = invs
        .iter()
        .map(|i| Step::new("cargo", i.args(&pin)))
        .collect();
    if explain {
        for s in std::iter::once(&rustc).chain(&steps) {
            writeln!(out, "{}", s.display()).map_err(|e| Error::msg(format!("stdout: {e}")))?;
        }
        return Ok(());
    }
    let version = capture(&rustc)?;
    let mut failures = Vec::new();
    for (inv, step) in invs.iter().zip(&steps) {
        eprintln!("+ {}", step.display());
        // A failed arm is reported with the rest, so the differences earlier
        // arms found still print.
        let printed = match capture(step) {
            Ok(printed) => printed,
            Err(e) => {
                failures.push(e.message);
                continue;
            }
        };
        let text = header(version.trim(), inv, arch) + &printed;
        if let Err(e) = check_one(root, inv, arch, &text, bless) {
            failures.push(e);
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    Err(Error::msg(failures.join("\n\n")))
}

#[cfg(test)]
mod tests {
    use super::{
        Invocation, PIN, Snapshot, Target, channel, check_one, compare, differences, discover,
        gated_arch, header, invocations, orphans, parse, plan,
    };
    use crate::checks::scratch::Scratch;
    use crate::run::{Error, Step};

    fn inv(pkg: &str, bench: &str, arm: &'static str, features: &'static str) -> Invocation {
        Invocation {
            pkg: pkg.to_owned(),
            bench: bench.to_owned(),
            arm,
            features,
        }
    }

    fn target(pkg: &str, bench: &str) -> Target {
        Target {
            pkg: pkg.to_owned(),
            bench: bench.to_owned(),
        }
    }

    fn snap(text: &str) -> Snapshot {
        parse(text).unwrap()
    }

    const ONE: &str = "# rustc: r1\ncase\tallocs\treallocs\tbytes\na\t3\t1\t40\n";

    #[test]
    fn a_case_missing_from_the_file_fails() {
        let run = snap("# rustc: r1\ncase\tallocs\treallocs\tbytes\na\t3\t1\t40\nb\t0\t0\t0\n");
        assert_eq!(
            differences(&snap(ONE), &run),
            vec!["case `b`: not in the file"]
        );
    }

    #[test]
    fn a_stale_entry_fails() {
        let run = snap("# rustc: r1\ncase\tallocs\treallocs\tbytes\n");
        assert_eq!(
            differences(&snap(ONE), &run),
            vec!["case `a`: in the file, not produced by this run"]
        );
    }

    #[test]
    fn one_count_off_by_one_fails() {
        for (i, label) in ["allocs", "reallocs", "bytes"].iter().enumerate() {
            for delta in [1_i64, -1] {
                let mut figures = [3_i64, 1, 40];
                figures[i] += delta;
                let run = snap(&format!(
                    "# rustc: r1\ncase\tallocs\treallocs\tbytes\na\t{}\t{}\t{}\n",
                    figures[0], figures[1], figures[2]
                ));
                let old = [3, 1, 40][i];
                assert_eq!(
                    differences(&snap(ONE), &run),
                    vec![format!("case `a`: {label} {old} -> {}", figures[i])]
                );
            }
        }
    }

    #[test]
    fn equal_snapshots_have_no_difference() {
        assert_eq!(differences(&snap(ONE), &snap(ONE)), Vec::<String>::new());
    }

    #[test]
    fn a_header_change_names_the_key() {
        let file = snap(
            "# rustc: r1\n# backend: simd-json\n# simd-json: AVX2\ncase\tallocs\treallocs\tbytes\n",
        );
        let run =
            snap("# rustc: r2\n# backend: simd-json\ncase\tallocs\treallocs\tbytes\n# x: y\n");
        assert_eq!(
            differences(&file, &run),
            vec![
                "header `rustc`: `r1` in the file, `r2` in this run",
                "header `simd-json`: `AVX2` in the file, (absent) in this run",
                "header `x`: (absent) in the file, `y` in this run",
            ]
        );
    }

    #[test]
    fn a_malformed_file_is_refused_with_its_line() {
        for (text, err) in [
            (
                "a\t1\t1\t1\n",
                "line 1: expected `case\tallocs\treallocs\tbytes` before the first case",
            ),
            (
                "#rustc\n",
                "line 1: expected `case\tallocs\treallocs\tbytes` before the first case",
            ),
            ("# rustc\n", "line 1: a header line is `# key: value`"),
            (
                "case\tallocs\treallocs\tbytes\na\t1\t1\n",
                "line 2: a case line has four tab-separated fields",
            ),
            (
                "case\tallocs\treallocs\tbytes\na\t1\t-1\t1\n",
                "line 2: `-1` is not a count",
            ),
            (
                "case\tallocs\treallocs\tbytes\na\t1\t1\t1\na\t1\t1\t1\n",
                "line 3: case `a` appears twice",
            ),
        ] {
            assert_eq!(parse(text), Err(err.to_owned()), "{text:?}");
        }
    }

    const RUN: &str = "# rustc: r1\ncase\tallocs\treallocs\tbytes\na\t3\t2\t40\n";

    #[test]
    fn a_mismatch_names_the_file_case_and_command() {
        let scratch = Scratch::new("alloc-mismatch").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        let rel = i.file("x86_64");
        super::write(&scratch.join(&rel), ONE).unwrap();
        assert_eq!(
            check_one(scratch.dir(), &i, "x86_64", RUN, false),
            Err("crates/spate-x/benches/alloc/d_alloc.default.x86_64.tsv differs from this run:\n  \
                 case `a`: reallocs 1 -> 2\n\
                 The file this run produced is at \
                 target/alloc-snapshots/crates/spate-x/benches/alloc/d_alloc.default.x86_64.tsv. \
                 Commit it, or run `cargo xtask bench alloc --bless` on Linux x86_64."
                .to_owned())
        );
        let written =
            std::fs::read_to_string(scratch.join(&format!("target/alloc-snapshots/{rel}")))
                .unwrap();
        assert_eq!(written, RUN);
        assert_eq!(std::fs::read_to_string(scratch.join(&rel)).unwrap(), ONE);
    }

    #[test]
    fn a_matching_file_passes_and_writes_nothing() {
        let scratch = Scratch::new("alloc-match").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        super::write(&scratch.join(&i.file("x86_64")), ONE).unwrap();
        assert_eq!(check_one(scratch.dir(), &i, "x86_64", ONE, false), Ok(()));
        assert!(!scratch.join("target").exists());
    }

    #[test]
    fn a_target_with_no_file_fails() {
        let scratch = Scratch::new("alloc-missing").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        let e = check_one(scratch.dir(), &i, "aarch64", ONE, false).unwrap_err();
        assert!(
            e.starts_with(
                "crates/spate-x/benches/alloc/d_alloc.default.aarch64.tsv differs from this run:\n  not committed\n"
            ),
            "{e}"
        );
    }

    #[test]
    fn an_empty_run_is_refused() {
        let scratch = Scratch::new("alloc-empty").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        let empty = "# rustc: r1\ncase\tallocs\treallocs\tbytes\n";
        for bless in [false, true] {
            assert_eq!(
                check_one(scratch.dir(), &i, "x86_64", empty, bless),
                Err(
                    "d_alloc: the run printed no case; a snapshot target is a `[[bench]]` with \
                     `harness = false` whose `main` calls `spate_test_support::alloc::main_with`"
                        .to_owned()
                )
            );
        }
        assert!(!scratch.join("crates").exists());
    }

    #[test]
    fn bless_writes_only_the_arch_that_ran() {
        let scratch = Scratch::new("alloc-bless").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        super::write(&scratch.join(&i.file("x86_64")), ONE).unwrap();
        super::write(&scratch.join(&i.file("aarch64")), ONE).unwrap();
        assert_eq!(check_one(scratch.dir(), &i, "x86_64", RUN, true), Ok(()));
        assert_eq!(
            std::fs::read_to_string(scratch.join(&i.file("x86_64"))).unwrap(),
            RUN
        );
        assert_eq!(
            std::fs::read_to_string(scratch.join(&i.file("aarch64"))).unwrap(),
            ONE
        );
        assert!(!scratch.join("target").exists());
    }

    #[test]
    fn an_orphaned_file_fails() {
        let scratch = Scratch::new("alloc-orphan").unwrap();
        let invs = [inv("spate-x", "d_alloc", "default", "")];
        for rel in [
            "crates/spate-x/benches/alloc/d_alloc.default.x86_64.tsv",
            "crates/spate-x/benches/alloc/d_alloc.default.aarch64.tsv",
            "crates/spate-x/benches/alloc/d_alloc.simd.x86_64.tsv",
            "crates/spate-x/benches/alloc/d_alloc.default.riscv64.tsv",
            "crates/spate-y/benches/alloc/gone_alloc.default.x86_64.tsv",
        ] {
            super::write(&scratch.join(rel), ONE).unwrap();
        }
        assert_eq!(
            orphans(scratch.dir(), &invs),
            vec![
                "crates/spate-x/benches/alloc/d_alloc.default.riscv64.tsv",
                "crates/spate-x/benches/alloc/d_alloc.simd.x86_64.tsv",
                "crates/spate-y/benches/alloc/gone_alloc.default.x86_64.tsv",
            ]
        );
    }

    #[test]
    fn discovery_takes_the_suffix_only() {
        let scratch = Scratch::new("alloc-discover").unwrap();
        for rel in [
            "crates/spate-b/benches/z_alloc.rs",
            "crates/spate-a/benches/d_alloc.rs",
            "crates/spate-a/benches/d_gungraun.rs",
            "crates/spate-a/benches/d_alloc.rs.orig",
            "crates/spate-a/benches/alloc/d_alloc.default.x86_64.tsv",
        ] {
            super::write(&scratch.join(rel), "").unwrap();
        }
        assert_eq!(
            discover(scratch.dir()),
            vec![target("spate-a", "d_alloc"), target("spate-b", "z_alloc")]
        );
    }

    /// Every committed file belongs to a target and arm, and every target and
    /// arm has a file for each gated architecture.
    #[test]
    fn the_tree_commits_one_file_per_target_arm_and_arch() {
        let root = crate::repo_root().unwrap();
        let invs = invocations(&discover(&root));
        assert!(!invs.is_empty(), "the tree holds no snapshot target");
        assert_eq!(orphans(&root, &invs), Vec::<String>::new());
        for i in &invs {
            for arch in super::GATED {
                assert!(root.join(i.file(arch)).is_file(), "{}", i.file(arch));
            }
        }
    }

    #[test]
    fn the_command_refuses_a_host_it_does_not_gate() {
        assert_eq!(gated_arch("linux", "x86_64"), Ok(()));
        assert_eq!(gated_arch("linux", "aarch64"), Ok(()));
        for (os, arch) in [
            ("macos", "aarch64"),
            ("linux", "riscv64"),
            ("windows", "x86_64"),
        ] {
            assert_eq!(
                gated_arch(os, arch),
                Err(format!(
                    "allocation snapshots are committed for Linux x86_64 and aarch64, and this \
                     host is {os} {arch}; a CI run's `alloc-snapshots-<arch>` artifact holds the \
                     files for a change"
                ))
            );
        }
    }

    #[test]
    fn snapshot_runs_the_counted_arms() {
        let targets = [
            target("spate-json", "decode_alloc"),
            target("spate-kafka", "encode_alloc"),
        ];
        let argv: Vec<Vec<String>> = invocations(&targets)
            .iter()
            .map(|i| i.args("1.0.0"))
            .collect();
        let line = |s: &str| s.split(' ').map(ToOwned::to_owned).collect::<Vec<_>>();
        assert_eq!(
            argv,
            vec![
                line("+1.0.0 bench -p spate-json --bench decode_alloc --locked"),
                line("+1.0.0 bench -p spate-json --bench decode_alloc --locked --features simd"),
                line("+1.0.0 bench -p spate-kafka --bench encode_alloc --locked"),
            ]
        );
    }

    /// The pin names an exact release, so a compiler move arrives as its own
    /// change.
    #[test]
    fn the_pin_names_an_exact_stable() {
        let text = std::fs::read_to_string(crate::repo_root().unwrap().join(PIN)).unwrap();
        let pin = channel(&text).unwrap();
        assert_eq!(pin.split('.').count(), 3, "{pin}");
        assert!(pin.split('.').all(|p| p.parse::<u32>().is_ok()), "{pin}");
        assert_eq!(
            channel("[toolchain]\n"),
            Err(format!("{PIN} names no `channel = \"…\"`"))
        );
    }

    #[test]
    fn the_header_names_the_compiler_features_and_arch() {
        let covers = super::COVERS;
        assert_eq!(
            header(
                "rustc 1.0.0",
                &inv("spate-x", "d_alloc", "default", ""),
                "x86_64"
            ),
            format!("# rustc: rustc 1.0.0\n# features: none\n# arch: x86_64\n# counts: {covers}\n")
        );
        assert_eq!(
            header(
                "rustc 1.0.0",
                &inv("spate-x", "d_alloc", "simd", "simd"),
                "aarch64"
            ),
            format!(
                "# rustc: rustc 1.0.0\n# features: simd\n# arch: aarch64\n# counts: {covers}\n"
            )
        );
    }

    /// A committed path that exists but cannot be read is reported as that,
    /// and writes no replacement.
    #[test]
    fn an_unreadable_file_is_an_error_and_not_a_missing_one() {
        let scratch = Scratch::new("alloc-unreadable").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        std::fs::create_dir_all(scratch.join(&i.file("x86_64"))).unwrap();
        let e = check_one(scratch.dir(), &i, "x86_64", ONE, false).unwrap_err();
        assert!(
            e.starts_with("crates/spate-x/benches/alloc/d_alloc.default.x86_64.tsv: ")
                && !e.contains("differs"),
            "{e}"
        );
        assert!(!scratch.join("target").exists());
    }

    fn tree(files: &[&str]) -> Scratch {
        let scratch = Scratch::new("alloc-plan").unwrap();
        super::write(&scratch.join(PIN), "[toolchain]\nchannel = \"1.0.0\"\n").unwrap();
        for rel in files {
            super::write(&scratch.join(rel), "").unwrap();
        }
        scratch
    }

    #[test]
    fn the_plan_refuses_an_orphaned_file() {
        let scratch = tree(&[
            "crates/spate-x/benches/d_alloc.rs",
            "crates/spate-x/benches/alloc/gone_alloc.default.x86_64.tsv",
        ]);
        assert_eq!(
            plan(scratch.dir()),
            Err("no snapshot target and arm writes these files:\n  \
                 crates/spate-x/benches/alloc/gone_alloc.default.x86_64.tsv"
                .to_owned())
        );
    }

    #[test]
    fn the_plan_refuses_a_tree_with_no_target() {
        let scratch = tree(&[]);
        assert_eq!(
            plan(scratch.dir()),
            Err("no allocation snapshot target was discovered; a target is \
                 `crates/<c>/benches/<name>_alloc.rs`"
                .to_owned())
        );
    }

    #[test]
    fn the_plan_carries_the_pin_and_each_invocation() {
        let scratch = tree(&["crates/spate-x/benches/d_alloc.rs"]);
        assert_eq!(
            plan(scratch.dir()),
            Ok((
                "1.0.0".to_owned(),
                vec![inv("spate-x", "d_alloc", "default", "")]
            ))
        );
    }

    #[test]
    fn a_libtest_target_is_refused_by_name() {
        let scratch = Scratch::new("alloc-libtest").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        let text = "# rustc: r1\n\nrunning 0 tests\n\ntest result: ok. 0 passed; 0 failed; \
                    0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n\n";
        let err = check_one(scratch.dir(), &i, "x86_64", text, false).unwrap_err();
        assert!(err.contains("harness = false"), "{err}");
    }

    /// A committed file that no longer parses, as after a merge conflict, is
    /// a difference, so the failing leg still leaves its replacement.
    #[test]
    fn an_unparseable_committed_file_still_leaves_the_replacement() {
        let scratch = Scratch::new("alloc-conflict").unwrap();
        let i = inv("spate-x", "d_alloc", "default", "");
        let rel = i.file("x86_64");
        super::write(
            &scratch.join(&rel),
            "# rustc: r1\ncase\tallocs\treallocs\tbytes\n<<<<<<< HEAD\na\t3\t1\t40\n",
        )
        .unwrap();
        let err = check_one(scratch.dir(), &i, "x86_64", RUN, false).unwrap_err();
        assert!(
            err.contains(
                "the file does not parse: line 3: a case line has four tab-separated fields"
            ),
            "{err}"
        );
        assert!(
            scratch
                .join(&format!("target/alloc-snapshots/{rel}"))
                .exists()
        );
    }

    /// The command refuses an ungated host before it plans or runs anything.
    #[test]
    fn the_run_refuses_a_host_it_does_not_gate() {
        let root = crate::repo_root().unwrap();
        let outcome = super::run(&root, true, false).map_err(|e| e.message);
        match gated_arch(super::consts::OS, super::consts::ARCH) {
            Ok(()) => assert!(outcome.is_ok(), "{outcome:?}"),
            Err(e) => assert_eq!(outcome, Err(e)),
        }
    }

    /// A two-arm tree whose files hold what `body` prints for each arm.
    fn two_arms(body: impl Fn(&str) -> String) -> Scratch {
        let scratch = tree(&["crates/spate-json/benches/d_alloc.rs"]);
        for i in [
            inv("spate-json", "d_alloc", "default", ""),
            inv("spate-json", "d_alloc", "simd", "simd"),
        ] {
            let text = header("rustc r1", &i, "x86_64") + &body(i.arm);
            super::write(&scratch.join(&i.file("x86_64")), &text).unwrap();
        }
        scratch
    }

    fn body(arm: &str, allocs: u64) -> String {
        format!("# arm: {arm}\ncase\tallocs\treallocs\tbytes\na\t{allocs}\t0\t8\n")
    }

    /// Runs `compare` over `scratch` with each arm printing `printed(arm)`.
    fn compare_with(
        scratch: &Scratch,
        printed: impl Fn(&str) -> Result<String, Error>,
    ) -> Result<(), String> {
        compare(
            scratch.dir(),
            "x86_64",
            false,
            false,
            &mut Vec::new(),
            |step: &Step<'_>| {
                if step.program == "rustc" {
                    return Ok("rustc r1\n".to_owned());
                }
                printed(if step.args.iter().any(|a| a == "simd") {
                    "simd"
                } else {
                    "default"
                })
            },
        )
        .map_err(|e| e.message)
    }

    #[test]
    fn the_command_compares_every_arm() {
        let scratch = two_arms(|arm| body(arm, 1));
        assert_eq!(compare_with(&scratch, |arm| Ok(body(arm, 1))), Ok(()));
        for moved in ["default", "simd"] {
            let err = compare_with(&scratch, |arm| {
                Ok(body(arm, if arm == moved { 2 } else { 1 }))
            })
            .unwrap_err();
            let file =
                format!("crates/spate-json/benches/alloc/d_alloc.{moved}.x86_64.tsv differs");
            assert!(
                err.starts_with(&file) && err.contains("case `a`: allocs 1 -> 2"),
                "{err}"
            );
            assert_eq!(err.matches("differs").count(), 1, "{err}");
        }
    }

    /// An arm that fails to build is reported alongside an earlier arm's
    /// difference.
    #[test]
    fn a_failed_arm_keeps_the_differences_of_the_others() {
        let scratch = two_arms(|arm| body(arm, 1));
        let err = compare_with(&scratch, |arm| {
            if arm == "simd" {
                Err(Error::msg("`cargo bench` exited 101"))
            } else {
                Ok(body(arm, 2))
            }
        })
        .unwrap_err();
        assert!(
            err.contains("d_alloc.default.x86_64.tsv differs")
                && err.ends_with("`cargo bench` exited 101"),
            "{err}"
        );
    }

    #[test]
    fn explain_prints_every_step_and_runs_none() {
        let scratch = two_arms(|arm| body(arm, 1));
        let mut out = Vec::new();
        compare(
            scratch.dir(),
            "x86_64",
            true,
            false,
            &mut out,
            |step: &Step<'_>| panic!("ran {}", step.display()),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "rustc +1.0.0 --version\n\
             cargo +1.0.0 bench -p spate-json --bench d_alloc --locked\n\
             cargo +1.0.0 bench -p spate-json --bench d_alloc --locked --features simd\n"
        );
    }
}
