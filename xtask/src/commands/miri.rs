//! Runs the lib tests that enter an `unsafe` block under Miri, which reports
//! an out-of-bounds access or an invalid pointer that leaves the output intact.

use std::path::Path;

use crate::run::{self, Outcome, Step};

/// The lib tests that enter an `unsafe` block, by package and nextest
/// filterset.
///
/// An `unsafe` block that a lib test can enter without a foreign call gets a
/// row here. spate-kafka has none: every `unsafe` block it ships, or that its
/// lib tests enter, needs librdkafka, and Miri cannot run a foreign call. The
/// spate-bench row leaves out its `libc` calls for the same reason.
const TARGETS: &[(&str, &str)] = &[
    ("spate-clickhouse", "test(/^native::leaf::/)"),
    ("spate-test-support", "test(/^alloc::/)"),
    ("spate-bench", "test(/^alloc::/)"),
];

/// Appended to any `MIRIFLAGS` already set. Miri then also reports a pointer
/// made from an integer, which has no provenance.
const FLAGS: &str = "-Zmiri-strict-provenance";

pub(crate) fn run(root: &Path, explain: bool, nightly: &str) -> Outcome {
    let toolchain = format!("+{nightly}");
    run::require(
        root,
        explain,
        &Step::new("cargo", [toolchain.as_str(), "miri", "--version"]),
        &format!("Miri on {nightly} (rustup component add --toolchain {nightly} miri rust-src)"),
    )?;

    let inherited = std::env::var("MIRIFLAGS").unwrap_or_default();
    let mut failed = 0;
    for (package, filter) in TARGETS {
        let step = test_step(&toolchain, package, filter, &inherited);
        if explain {
            println!("{}", step.display());
            continue;
        }
        println!("miri: {package} {filter}");
        if run::run(root, false, &step).is_err() {
            println!("::error::Miri failed for {package}");
            failed += 1;
        }
    }
    if explain {
        return Ok(());
    }

    run::tally(
        TARGETS.len(),
        failed,
        "package(s)",
        "no package is listed; this run checked nothing",
    )?;
    println!("miri: {} package(s) passed under Miri.", TARGETS.len());
    Ok(())
}

/// One package's filtered lib tests under Miri.
///
/// `--no-tests fail` is nextest's default, and stated so that `NEXTEST_NO_TESTS`
/// in the environment cannot pass a row whose filter matches nothing.
fn test_step(toolchain: &str, package: &str, filter: &str, inherited: &str) -> Step<'static> {
    let flags = if inherited.is_empty() {
        FLAGS.to_owned()
    } else {
        format!("{inherited} {FLAGS}")
    };
    Step::new("cargo", [toolchain])
        .args(["miri", "nextest", "run", "-p", package, "--lib", "--locked"])
        .args(["--no-tests", "fail", "-E", filter])
        .env("MIRIFLAGS", flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_runs_its_filtered_lib_tests_under_miri() {
        let step = test_step("+nightly", "a", "test(/^m::/)", "");
        assert_eq!(
            step.display(),
            concat!(
                r#"MIRIFLAGS="-Zmiri-strict-provenance" "#,
                "cargo +nightly miri nextest run -p a --lib --locked ",
                "--no-tests fail -E 'test(/^m::/)'"
            )
        );
    }

    /// `MIRIFLAGS` already in the environment reaches the run, ahead of the
    /// flag this command adds.
    #[test]
    fn the_environment_s_miriflags_are_carried_through() {
        let step = test_step("+nightly", "a", "test(/^m::/)", "-Zmiri-seed=1");
        assert!(
            step.display()
                .starts_with(r#"MIRIFLAGS="-Zmiri-seed=1 -Zmiri-strict-provenance" "#),
            "{}",
            step.display()
        );
    }
}
