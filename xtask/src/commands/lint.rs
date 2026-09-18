//! The repository consistency checks: the members of `tidy`.

use std::path::Path;

use clap::ValueEnum;

use crate::run::{self, Outcome, Step};

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
pub(crate) enum TidyCheck {
    /// The workflows carry no known-vulnerable action or pattern
    Zizmor,
    /// The shell scripts lint clean
    Shellcheck,
    /// The checkers themselves still work
    SelfTest,
    /// A user-visible change carries a changelog fragment
    Changelog,
    /// The decision records stay consistent with their index
    Adr,
    /// The perf report's flag file stays parseable by perf-label.yml
    PerfReport,
    /// Every instruction-count bench declares a harness-free target
    GungraunBenches,
    /// The degenerate-region guard still recognizes both shapes
    CollectedRegion,
    /// Every transcluded region a docs page names exists
    Transclusions,
    /// Every supported-versions table matches the servers CI pins
    SupportedVersions,
    /// Every literal version is one the release rewrites
    ReleaseVersion,
}

/// The order `tidy` runs them in, cheapest and most likely to fail first.
pub(super) const ALL: &[TidyCheck] = &[
    TidyCheck::Zizmor,
    TidyCheck::Shellcheck,
    TidyCheck::SelfTest,
    TidyCheck::Adr,
    TidyCheck::PerfReport,
    TidyCheck::GungraunBenches,
    TidyCheck::CollectedRegion,
    TidyCheck::Transclusions,
    TidyCheck::SupportedVersions,
    TidyCheck::ReleaseVersion,
];

/// Runs one named check, or every member of `ALL`.
///
/// `Changelog` sits outside `ALL`. It reads the pull request's title, body and
/// endpoints, and enforces against `origin/main` when they are absent, so a
/// caller without them would demand a fragment while unable to see the
/// exemptions that excuse it.
pub(crate) fn tidy(root: &Path, explain: bool, check: Option<TidyCheck>, list: bool) -> Outcome {
    if list {
        for one in TidyCheck::value_variants() {
            let name = one.to_possible_value().expect("every check is selectable");
            println!(
                "{:<20} {}",
                name.get_name(),
                name.get_help().unwrap_or_default()
            );
        }
        return Ok(());
    }
    match check {
        Some(one) => one_check(root, explain, one),
        None => {
            for one in ALL {
                one_check(root, explain, *one)?;
            }
            Ok(())
        }
    }
}

fn one_check(root: &Path, explain: bool, check: TidyCheck) -> Outcome {
    match check {
        // Without GH_TOKEN the online audits are skipped instead of failing,
        // so a clean run does not mean what it looks like.
        TidyCheck::Zizmor => run::run(
            root,
            explain,
            &Step::new("zizmor", ["--persona=regular", ".github/"]),
        ),
        TidyCheck::Shellcheck => {
            let scripts = shell_scripts(root)?;
            run::run(
                root,
                explain,
                &Step::new("shellcheck", [] as [&str; 0]).args(scripts),
            )
        }
        TidyCheck::SelfTest => run::run(
            root,
            explain,
            &Step::new("cargo", ["test", "-p", "spate-xtask", "--locked"]),
        ),
        TidyCheck::Changelog => script(root, explain, "changelog.sh", "--check"),
        TidyCheck::Adr => crate::checks::adr::check(root, explain),
        TidyCheck::PerfReport => crate::checks::perf_report::self_test(explain),
        TidyCheck::GungraunBenches => crate::checks::gungraun::check(root, explain),
        TidyCheck::CollectedRegion => {
            script(root, explain, "gungraun-collected-region.sh", "--self-test")
        }
        TidyCheck::Transclusions => script(root, explain, "transclude.sh", "--check"),
        TidyCheck::SupportedVersions => crate::checks::supported_versions::check(root, explain),
        TidyCheck::ReleaseVersion => script(root, explain, "release-version.sh", "--check"),
    }
}

fn script(root: &Path, explain: bool, name: &str, mode: &str) -> Outcome {
    run::run(
        root,
        explain,
        &Step::new(&format!("./scripts/{name}"), [mode]),
    )
}

/// The shell scripts, sorted, so the lint covers whatever is present without a
/// list to keep current.
fn shell_scripts(root: &Path) -> Result<Vec<String>, crate::run::Error> {
    let dir = root.join("scripts");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir)
        .map_err(|e| crate::run::Error::msg(format!("{}: {e}", dir.display())))?
    {
        let entry = entry.map_err(|e| crate::run::Error::msg(format!("{}: {e}", dir.display())))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".sh") {
            out.push(format!("scripts/{name}"));
        }
    }
    out.sort();
    Ok(out)
}
