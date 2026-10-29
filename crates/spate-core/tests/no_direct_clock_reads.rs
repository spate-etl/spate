//! `spate-core`'s production code reads time through `crate::clock`: no
//! `Instant::now`, `SystemTime::now` or `.elapsed()` outside the clock module.
//! Inline test modules, test files (`tests.rs`, `*_tests.rs`), comments and
//! the paths listed in `SKIPPED` are exempt.

use std::path::{Path, PathBuf};

/// Paths under `src` the scan does not read: the clock module, whose
/// `SystemClock` is the one allowed read, a test-only file not named as one,
/// and the sink pool, which moves onto the tokio clock separately.
const SKIPPED: &[&str] = &["clock.rs", "clock/", "pipeline/fakes.rs", "sink/"];

/// Whether `rel` names a file that holds only tests.
fn is_test_file(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    name == "tests.rs" || name.ends_with("_tests.rs")
}

const READS: &[&str] = &["Instant::now", "SystemTime::now", ".elapsed()"];

/// One-based line numbers in `text` that read the clock directly, outside
/// its top-level inline `#[cfg(test)]` modules.
fn direct_reads(text: &str) -> Vec<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    let mut in_test_module = false;
    for (i, line) in lines.iter().enumerate() {
        if in_test_module {
            // A top-level module closes with a `}` at column zero.
            in_test_module = *line != "}";
            continue;
        }
        if starts_test_module(line, lines.get(i + 1).copied()) {
            in_test_module = true;
            continue;
        }
        if line.trim_start().starts_with("//") {
            continue;
        }
        if READS.iter().any(|read| line.contains(read)) {
            found.push(i + 1);
        }
    }
    found
}

/// Whether `line` opens a top-level inline test module: a `cfg(test)`
/// attribute at column zero with `mod … {` on the next line. Any other
/// `cfg(test)` item, a `mod tests;` declaration included, does not end the
/// scan.
fn starts_test_module(line: &str, next: Option<&str>) -> bool {
    let cfg_test = line.starts_with("#[cfg(test)]") || line.starts_with("#[cfg(all(test");
    cfg_test
        && next.is_some_and(|n| {
            (n.starts_with("mod ") || n.starts_with("pub(crate) mod "))
                && n.trim_end().ends_with('{')
        })
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read src") {
        let path = entry.expect("read entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_direct_clock_reads() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let mut offending = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(&src)
            .expect("under src")
            .to_string_lossy()
            .replace('\\', "/");
        if is_test_file(&rel) || SKIPPED.iter().any(|skip| rel.starts_with(skip)) {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read source");
        offending.extend(
            direct_reads(&text)
                .into_iter()
                .map(|n| format!("{rel}:{n}")),
        );
    }
    assert!(
        offending.is_empty(),
        "read time through `crate::clock` at: {offending:?}"
    );
}

/// Test files are recognised by name, and production files are not.
#[test]
fn test_files_are_recognised_by_name() {
    assert!(is_test_file("ops/tests.rs"));
    assert!(is_test_file("sink/pool_tests.rs"));
    assert!(!is_test_file("ops/chain.rs"));
    assert!(!is_test_file("contests.rs"));
}

/// The scan flags a production read, and skips comments and top-level test
/// modules, reading on after one closes.
#[test]
fn the_scan_skips_comments_and_test_modules() {
    let text = "\
fn tick() -> Instant {
    // Instant::now in a comment
    Instant::now()
}

#[cfg(test)]
fn helper() {}

#[cfg(test)]
mod declared;

fn later() { let _ = start.elapsed(); }

#[cfg(test)]
mod tests {
    fn t() { let _ = Instant::now(); }
}

fn after_tests() { let _ = SystemTime::now(); }
";
    assert_eq!(direct_reads(text), [3, 12, 19]);
}
