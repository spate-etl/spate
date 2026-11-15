//! The feature powerset, in shards.
//!
//! `cargo hack check --feature-powerset` checks every combination of features.
//! That is more checks than one job's limit allows. `plan` counts them and
//! writes one shard per `PER_SHARD_CHECKS`, and
//! `run` checks one shard. The weekly workflow runs the shards as the legs of
//! a matrix.

use std::path::Path;

use clap::Subcommand;

use super::{FEATURE_EXCLUDES, FEATURE_RUSTFLAGS};
use crate::ci;
use crate::run::{self, Error, Outcome, Step};

/// Checks in one shard: 3000 seconds, divided by 0.37 seconds a check times the
/// cost ratio 1.217, rounded to 500. The 0.37 comes from the cancelled run of
/// 2026-11-09, which checked 14,496 combinations in 90 minutes. The ratio is
/// the cost of the heaviest checks over the lightest, from two local runs of
/// 696 and 599 checks.
pub(crate) const PER_SHARD_CHECKS: u64 = 6_500;

#[derive(Subcommand)]
pub(crate) enum Powerset {
    /// Counts the checks, and writes the shard list the workflow's matrix reads
    Plan,

    /// Checks one shard
    Run {
        /// The shard, as `M/N` with `1 <= M <= N`
        shard: String,
    },
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: Powerset) -> Outcome {
    match cmd {
        Powerset::Plan => plan(root, explain),
        Powerset::Run { shard } => run_shard(root, explain, &shard),
    }
}

/// The arguments every powerset check takes, before any `--partition`.
fn selection() -> Step<'static> {
    Step::new("cargo", ["hack", "check"])
        .args(["--workspace", "--feature-powerset", "--no-dev-deps"])
        .args(FEATURE_EXCLUDES)
        .env(FEATURE_RUSTFLAGS.0, FEATURE_RUSTFLAGS.1)
}

/// Writes `shards=<json>` to the step-output file when the runner names one,
/// and the list to stdout otherwise.
fn plan(root: &Path, explain: bool) -> Outcome {
    let listing = selection().arg("--print-command-list");
    if explain {
        println!("{}", listing.display());
        return Ok(());
    }
    let total = count_checks(&run::capture(root, &listing)?)?;
    let shards = shard_plan(total, PER_SHARD_CHECKS)?;
    eprintln!("powerset: {total} checks in {} shards", shards.len());
    let json = serde_json::to_string(&shards).map_err(|e| Error::msg(e.to_string()))?;
    match ci::output_file() {
        Some(path) => ci::append_to(&path, &format!("shards={json}\n"))
            .map_err(|e| Error::msg(format!("{}: {e}", path.display()))),
        None => {
            println!("{json}");
            Ok(())
        }
    }
}

fn run_shard(root: &Path, explain: bool, shard: &str) -> Outcome {
    check_shard(shard)?;
    run::run(root, explain, &selection().arg("--partition").arg(shard))
}

/// The checks `cargo hack --print-command-list` names: one `cargo ...` line each.
fn count_checks(listing: &str) -> Result<u64, Error> {
    let count = listing
        .lines()
        .filter(|line| line.starts_with("cargo "))
        .count();
    let total = u64::try_from(count).map_err(|e| Error::msg(e.to_string()))?;
    if total == 0 {
        return Err(Error::msg("cargo hack listed no checks"));
    }
    Ok(total)
}

/// The shard list for `total` checks at `per_shard` checks each, as `M/N`.
fn shard_plan(total: u64, per_shard: u64) -> Result<Vec<String>, Error> {
    if total == 0 {
        return Err(Error::msg("no checks to shard"));
    }
    let shards = total.div_ceil(per_shard);
    Ok((1..=shards).map(|m| format!("{m}/{shards}")).collect())
}

/// Refuses a shard that is not `M/N` with `1 <= M <= N`, before any check runs.
fn check_shard(shard: &str) -> Outcome {
    let invalid = || Error::msg(format!("a shard is M/N with 1 <= M <= N, not {shard:?}"));
    let (m, n) = shard.split_once('/').ok_or_else(invalid)?;
    let m: u64 = m.parse().map_err(|_| invalid())?;
    let n: u64 = n.parse().map_err(|_| invalid())?;
    if 0 < m && m <= n {
        Ok(())
    } else {
        Err(invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_shard_is_one_shard() {
        assert_eq!(shard_plan(6_500, 6_500).unwrap(), ["1/1"]);
    }

    #[test]
    fn one_check_more_is_a_second_shard() {
        assert_eq!(shard_plan(6_501, 6_500).unwrap(), ["1/2", "2/2"]);
    }

    #[test]
    fn the_pin_count_makes_eleven_shards() {
        let shards = shard_plan(69_503, PER_SHARD_CHECKS).unwrap();
        assert_eq!(shards.len(), 11);
        assert_eq!(shards.first().unwrap(), "1/11");
        assert_eq!(shards.last().unwrap(), "11/11");
    }

    #[test]
    fn no_checks_is_an_error() {
        assert!(shard_plan(0, PER_SHARD_CHECKS).is_err());
    }

    #[test]
    fn only_cargo_lines_are_checks() {
        let listing = "info: --no-dev-deps modifies real `Cargo.toml`\n\
                       cargo check --manifest-path a/Cargo.toml\n\
                       cargo check --manifest-path b/Cargo.toml\n";
        assert_eq!(count_checks(listing).unwrap(), 2);
    }

    #[test]
    fn an_empty_listing_is_an_error() {
        assert!(count_checks("").is_err());
    }

    #[test]
    fn a_well_formed_shard_is_accepted() {
        for shard in ["1/11", "11/11", "3/3"] {
            assert!(check_shard(shard).is_ok(), "{shard}");
        }
    }

    #[test]
    fn a_malformed_shard_is_refused() {
        for shard in ["", "3", "0/11", "12/11", "a/b", "1/0"] {
            assert!(check_shard(shard).is_err(), "{shard}");
        }
    }

    #[test]
    fn the_checks_leave_out_what_the_linear_matrix_leaves_out() {
        let shown = selection().display();
        assert!(
            shown.contains("--exclude spate-xtask --exclude spate-fuzz"),
            "{shown}"
        );
        assert!(shown.contains("RUSTFLAGS=\"-D warnings\""), "{shown}");
    }
}
