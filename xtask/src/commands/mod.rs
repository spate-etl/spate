//! The command surface: every task CI and a contributor can invoke, and the
//! dispatch that runs one.

mod bench;
mod docs;
mod fuzz;
mod lint;

use std::path::Path;

use clap::Subcommand;

use crate::run::{self, Outcome, Step};

pub(crate) use lint::TidyCheck;

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Everything a pull request must pass
    Ci,

    /// Formatting and clippy together
    Lint,

    /// Format the workspace
    Fmt {
        /// Report what would change and write nothing
        #[arg(long)]
        check: bool,
    },

    /// Lint the workspace
    Clippy {
        /// Report lints as warnings, leaving the build green
        #[arg(long)]
        no_deny_warnings: bool,
    },

    /// Type-check the workspace
    Check,

    /// Unit and integration tests, no containers
    Test,

    /// Doc tests, which nextest does not run
    Doctest,

    /// Rustdoc for every crate, warnings denied
    Doc,

    /// Rustdoc as docs.rs builds it: per crate, on nightly
    Docsrs,

    /// Container-backed suites (needs Docker; lanes per ci/README.md)
    IntegrationTest,

    /// Loom concurrency models (slow)
    Loom,

    /// Every feature alone, the feature-off combinations, and every target
    Hack,

    /// Benchmarks, counted and wall-clock
    Bench {
        #[command(subcommand)]
        cmd: bench::Bench,
    },

    /// Licenses, advisories, bans, sources
    Deny,

    /// Regenerate THIRD-PARTY.md
    Attribution {
        /// Render the licence page to this file instead
        #[arg(long, value_name = "FILE")]
        html: Option<String>,
    },

    /// Repository consistency checks; name one to run it alone
    Tidy {
        #[arg(value_enum)]
        check: Option<TidyCheck>,
        /// Print the check names and run none
        #[arg(long)]
        list: bool,
    },

    /// Decision records
    Adr {
        #[command(subcommand)]
        cmd: AdrCommand,
    },

    /// Changelog fragments
    Changelog {
        #[command(subcommand)]
        cmd: ChangelogCommand,
    },

    /// Fuzz targets
    Fuzz {
        #[command(subcommand)]
        cmd: fuzz::Fuzz,
    },

    /// The documentation site
    Docs {
        /// Serve locally with hot reload instead of building
        #[arg(long)]
        serve: bool,
    },

    /// The release sequence
    Release {
        #[command(subcommand)]
        cmd: ReleaseCommand,
    },

    /// Decide which CI jobs a change needs
    CiChanges {
        /// Classify a NUL-separated path list instead of a diff
        #[arg(long, value_name = "FILE")]
        classify_paths: Option<String>,
    },

    /// The semver gate against the published release
    SemverChecks {
        #[arg(long, conflicts_with = "cache_key")]
        against_registry: bool,
        #[arg(long, value_name = "LIST", requires = "against_registry")]
        packages: Option<String>,
        #[arg(long)]
        cache_key: bool,
    },

    /// The workspace version tool
    ReleaseVersion {
        #[arg(long, value_name = "VERSION", group = "mode")]
        bump: Option<String>,
        #[arg(long, group = "mode")]
        check: bool,
        #[arg(long, group = "mode")]
        derive: bool,
        #[arg(long, group = "mode")]
        check_publish_metadata: bool,
    },

    /// The digest-pinned container image for a lane
    ContainerImage {
        #[arg(long, group = "mode")]
        r#ref: bool,
        #[arg(long, group = "mode")]
        pull: bool,
        #[arg(long, group = "mode")]
        pull_all: bool,
        #[arg(long, value_name = "SERVICE", group = "mode")]
        extra_lanes: Option<String>,
        /// The service the mode applies to, and optionally the lane
        #[arg(value_name = "SERVICE")]
        service: Option<String>,
        #[arg(value_name = "LANE")]
        lane: Option<String>,
    },

    /// Apply .github/labels.yml to the repository
    SyncLabels {
        #[arg(long)]
        dry_run: bool,
        #[arg(long, value_name = "OWNER/NAME")]
        repo: Option<String>,
    },
}

#[derive(Subcommand)]
pub(crate) enum ReleaseCommand {
    /// Build the release commit and open the pull request
    Assemble {
        #[arg(long, value_name = "X.Y.Z")]
        version: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// The credential-free half of the publish
    Prepare {
        #[arg(long)]
        dry_run: bool,
    },
    /// Upload the artifacts, after the registry token is minted
    Upload,
    /// Close the release out, after the app token is minted
    Finish,
    /// The publish up to the point a token would be minted
    Publish,
    /// The whole release locally, nothing pushed or uploaded
    DryRun {
        #[arg(long, value_name = "X.Y.Z")]
        version: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum AdrCommand {
    /// Scaffold the next record
    New {
        #[arg(value_name = "SLUG")]
        slug: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum ChangelogCommand {
    /// Scaffold a fragment
    New {
        #[arg(value_name = "TYPE")]
        kind: String,
        #[arg(value_name = "SLUG")]
        slug: String,
    },
    /// Assemble CHANGELOG.md for a version
    Build {
        #[arg(value_name = "VERSION")]
        version: String,
    },
    /// Print the release notes for a version
    Notes {
        #[arg(value_name = "VERSION")]
        version: String,
    },
}

/// Runs one command.
pub(crate) fn dispatch(root: &Path, explain: bool, cmd: Command) -> Outcome {
    match cmd {
        Command::Ci => ci(root, explain),
        Command::Lint => lint_group(root, explain),
        Command::Fmt { check } => {
            let mut s = Step::new("cargo", ["fmt", "--all"]);
            if check {
                s = s.arg("--check");
            }
            run::run(root, explain, &s)
        }
        Command::Clippy { no_deny_warnings } => {
            let mut s = Step::new(
                "cargo",
                [
                    "clippy",
                    "--workspace",
                    "--all-targets",
                    "--all-features",
                    "--locked",
                    "--",
                ],
            );
            if !no_deny_warnings {
                s = s.args(["-D", "warnings"]);
            }
            run::run(root, explain, &s)
        }
        Command::Check => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                ["check", "--workspace", "--all-features", "--locked"],
            ),
        ),
        Command::Test => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                [
                    "nextest",
                    "run",
                    "--workspace",
                    "--all-features",
                    "--locked",
                ],
            ),
        ),
        Command::Doctest => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                ["test", "--workspace", "--all-features", "--locked", "--doc"],
            ),
        ),
        Command::Doc => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                [
                    "doc",
                    "--workspace",
                    "--no-deps",
                    "--all-features",
                    "--locked",
                ],
            )
            .env("RUSTDOCFLAGS", "-D warnings"),
        ),
        Command::Docsrs => run::run(
            root,
            explain,
            &Step::new("./scripts/docsrs.sh", [] as [&str; 0]),
        ),
        Command::IntegrationTest => run::steps(
            root,
            explain,
            &[
                Step::new("./scripts/container-image.sh", ["--pull-all"]),
                Step::new(
                    "cargo",
                    [
                        "nextest",
                        "run",
                        "--profile",
                        "docker",
                        "--workspace",
                        "--all-features",
                        "--locked",
                        "--run-ignored",
                        "ignored-only",
                    ],
                ),
            ],
        ),
        // `--lib` matters: the models are unit tests inside the crate, and a
        // `--test` run builds integration targets the cfg leaves empty.
        Command::Loom => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                ["test", "-p", "spate-core", "--release", "--lib", "--locked"],
            )
            .env("RUSTFLAGS", "--cfg loom"),
        ),
        Command::Hack => hack(root, explain),
        Command::Bench { cmd } => bench::dispatch(root, explain, cmd),
        Command::Deny => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                ["deny", "--all-features", "--locked", "check", "all"],
            ),
        ),
        Command::Attribution { html } => {
            let mut s = Step::new("./scripts/attribution.sh", [] as [&str; 0]);
            if let Some(out) = html {
                s = s.args(["--html", &out]);
            }
            run::run(root, explain, &s)
        }
        Command::Tidy { check, list } => lint::tidy(root, explain, check, list),
        Command::Adr { cmd } => match cmd {
            AdrCommand::New { slug } => run::run(
                root,
                explain,
                &Step::new("./scripts/adr.sh", ["--new", &slug]),
            ),
        },
        Command::Changelog { cmd } => {
            let s = match &cmd {
                ChangelogCommand::New { kind, slug } => {
                    Step::new("./scripts/changelog.sh", ["--new", kind, slug])
                }
                ChangelogCommand::Build { version } => {
                    Step::new("./scripts/changelog.sh", ["--build", version])
                }
                ChangelogCommand::Notes { version } => {
                    Step::new("./scripts/changelog.sh", ["--notes", version])
                }
            };
            run::run(root, explain, &s)
        }
        Command::Fuzz { cmd } => fuzz::dispatch(root, explain, cmd),
        Command::Docs { serve } => docs::dispatch(root, explain, serve),
        Command::Release { cmd } => {
            let s = match &cmd {
                ReleaseCommand::Assemble { version, dry_run } => {
                    let s = Step::new("./scripts/release.sh", ["assemble", "--version", version]);
                    if *dry_run { s.arg("--dry-run") } else { s }
                }
                ReleaseCommand::Prepare { dry_run } => {
                    let s = Step::new("./scripts/release.sh", ["prepare"]);
                    if *dry_run { s.arg("--dry-run") } else { s }
                }
                ReleaseCommand::Upload => Step::new("./scripts/release.sh", ["upload"]),
                ReleaseCommand::Finish => Step::new("./scripts/release.sh", ["finish"]),
                ReleaseCommand::Publish => {
                    Step::new("./scripts/release.sh", ["publish", "--dry-run"])
                }
                ReleaseCommand::DryRun { version } => {
                    Step::new("./scripts/release.sh", ["dry-run", "--version", version])
                }
            };
            run::run(root, explain, &s)
        }
        Command::CiChanges { classify_paths } => {
            if explain {
                println!("./scripts/container-image.sh --extra-lanes clickhouse");
                println!("(classifies the current event in process)");
                return Ok(());
            }
            let args: Vec<String> = classify_paths
                .map(|f| vec!["--classify-paths".to_owned(), f])
                .unwrap_or_default();
            crate::ci::changes(&args).map_err(Into::into)
        }
        Command::SemverChecks {
            against_registry,
            packages,
            cache_key,
        } => {
            let mut s = Step::new("./scripts/semver-checks.sh", [] as [&str; 0]);
            if against_registry {
                s = s.arg("--against-registry");
            }
            if let Some(p) = &packages {
                s = s.args(["--packages", p]);
            }
            if cache_key {
                s = s.arg("--cache-key");
            }
            run::run(root, explain, &s)
        }
        Command::ReleaseVersion {
            bump,
            check,
            derive,
            check_publish_metadata,
        } => {
            let mut s = Step::new("./scripts/release-version.sh", [] as [&str; 0]);
            if let Some(v) = &bump {
                s = s.args(["--bump", v]);
            }
            if check {
                s = s.arg("--check");
            }
            if derive {
                s = s.arg("--derive");
            }
            if check_publish_metadata {
                s = s.arg("--check-publish-metadata");
            }
            run::run(root, explain, &s)
        }
        Command::ContainerImage {
            r#ref,
            pull,
            pull_all,
            extra_lanes,
            service,
            lane,
        } => {
            let mut s = Step::new("./scripts/container-image.sh", [] as [&str; 0]);
            if r#ref {
                s = s.arg("--ref");
            }
            if pull {
                s = s.arg("--pull");
            }
            if pull_all {
                s = s.arg("--pull-all");
            }
            if let Some(svc) = &extra_lanes {
                s = s.args(["--extra-lanes", svc]);
            }
            if let Some(svc) = &service {
                s = s.arg(svc);
            }
            if let Some(l) = &lane {
                s = s.arg(l);
            }
            run::run(root, explain, &s)
        }
        Command::SyncLabels { dry_run, repo } => {
            // `DRY_RUN` is how the workflow selects it on a fork's pull request.
            let dry_run = dry_run || std::env::var("DRY_RUN").as_deref() == Ok("true");
            crate::checks::sync_labels::sync(root, explain, dry_run, repo.as_deref())
        }
    }
}

/// Everything a pull request must pass.
fn ci(root: &Path, explain: bool) -> Outcome {
    lint_group(root, explain)?;
    dispatch(root, explain, Command::Check)?;
    dispatch(root, explain, Command::Test)?;
    dispatch(root, explain, Command::Doctest)?;
    dispatch(root, explain, Command::Doc)?;
    dispatch(root, explain, Command::Hack)?;
    dispatch(root, explain, Command::Deny)?;
    lint::tidy(root, explain, None, false)?;
    // Outside the default set because CI splits it into a job that carries the
    // pull request's fields. On a laptop it orients against the upstream, so
    // the gate answers before a push.
    lint::tidy(root, explain, Some(TidyCheck::Changelog), false)
}

fn lint_group(root: &Path, explain: bool) -> Outcome {
    dispatch(root, explain, Command::Fmt { check: true })?;
    dispatch(
        root,
        explain,
        Command::Clippy {
            no_deny_warnings: false,
        },
    )
}

/// The feature matrix.
fn hack(root: &Path, explain: bool) -> Outcome {
    run::steps(
        root,
        explain,
        &[
            // `cargo hack --no-dev-deps` rewrites each Cargo.toml as it runs,
            // which a locked build refuses. Do not add `--locked`; it fails.
            Step::new(
                "cargo",
                [
                    "hack",
                    "check",
                    "--workspace",
                    "--each-feature",
                    "--no-dev-deps",
                    "--exclude-features",
                    "full",
                    "--exclude",
                    "spate-xtask",
                ],
            ),
            // Stripping dev-dependencies drops test and bench targets, so the
            // run above reaches no test target in any crate. These two build
            // them, on the axes it covers for the library: features off, then
            // the default set.
            Step::new(
                "cargo",
                [
                    "check",
                    "-p",
                    "spate-coordination",
                    "--no-default-features",
                    "--tests",
                    "--locked",
                ],
            ),
            // Last, because `--no-dev-deps` restores each Cargo.toml only when
            // it is finished and a locked build reads what is on disk.
            Step::new(
                "cargo",
                ["check", "--workspace", "--all-targets", "--locked"],
            ),
        ],
    )
}

#[cfg(test)]
mod tests {
    use clap::{Parser, ValueEnum};

    use super::TidyCheck;
    use crate::Cli;

    /// Whether what precedes an occurrence puts it where a shell would run it.
    ///
    /// A whitelist, because prose names commands too and every way of quoting
    /// prose also appears around a real invocation.
    fn in_command_position(before: &str) -> bool {
        let b = before.trim_end();
        if b.is_empty() {
            return true;
        }
        // A YAML scalar may quote the whole command.
        if let Some(rest) = b.strip_suffix(['"', '\'']) {
            return rest.trim_end().ends_with("run:");
        }
        [
            "run:", "&&", "||", ";", "|", "(", "elif", "if", "then", "else",
        ]
        .iter()
        .any(|p| b.ends_with(p))
    }

    /// The tokens a workflow line passes to this binary, where it invokes it.
    ///
    /// A shell expansion cannot be resolved here, so its token is dropped and
    /// the line is marked as carrying one.
    fn invocations(line: &str) -> Vec<(Vec<String>, bool)> {
        // A comment naming a command is prose, and prose does not parse as
        // argv. Both YAML and shell comments open with `#`.
        if line.trim_start().starts_with('#') {
            return Vec::new();
        }
        let mut rests: Vec<&str> = Vec::new();
        let mut cursor = line;
        // A line may chain more than one invocation.
        while let Some((before, r)) = cursor.split_once("cargo xtask ") {
            if in_command_position(before) {
                rests.push(r);
            }
            cursor = r;
        }
        if rests.is_empty() {
            if !line.contains("spate-xtask") {
                return Vec::new();
            }
            match line.split_once(" -- ") {
                Some((_, r)) => rests.push(r),
                None => return Vec::new(),
            }
        }
        rests.into_iter().map(tokenise).collect()
    }

    /// The argv a single invocation passes, and whether a shell expansion was
    /// dropped from it.
    fn tokenise(rest: &str) -> (Vec<String>, bool) {
        let mut expanded = false;
        let mut out = Vec::new();
        for tok in rest.split_whitespace() {
            // Shell syntax ends the argv. A line continuation ends what can be
            // read from this line, and the command parses without its tail.
            if tok.starts_with(['|', ';', '&', '<', '>']) || tok.contains('>') {
                break;
            }
            if tok.contains("${") || tok.contains("$(") {
                expanded = true;
                continue;
            }
            let tok = tok.strip_suffix(')').unwrap_or(tok);
            // `split_whitespace` yields no empty token, so an empty result
            // came from a quoted empty string and is a value.
            out.push(tok.trim_matches(['"', '\'']).to_owned());
        }
        (out, expanded)
    }

    /// Every invocation in `.github/workflows/` parses against this binary's
    /// own command surface, so a command, flag or check name that CI names and
    /// xtask does not accept fails here.
    #[test]
    fn the_workflows_name_only_commands_that_exist() {
        let dir = crate::repo_root().unwrap().join(".github/workflows");
        let mut checked = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|e| e != "yml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap().replace("\\\n", " ");
            for line in text.lines() {
                for (tokens, expanded) in invocations(line) {
                    checked += 1;
                    let argv = std::iter::once("cargo xtask".to_owned()).chain(tokens);
                    if let Err(e) = Cli::try_parse_from(argv) {
                        use clap::error::ErrorKind;
                        // `--help` and `--version` are reported as errors, and a
                        // dropped expansion can take a required value with it.
                        if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion)
                            || (expanded && e.kind() == ErrorKind::MissingRequiredArgument)
                        {
                            continue;
                        }
                        panic!("{}: {e}\n  in: {}", path.display(), line.trim());
                    }
                }
            }
        }
        assert!(checked > 0, "no workflow invokes this binary");
    }

    #[test]
    fn every_check_is_reachable_from_tidy() {
        let listed = super::lint::ALL;
        for check in TidyCheck::value_variants() {
            let count = listed.iter().filter(|c| *c == check).count();
            let expected = usize::from(*check != TidyCheck::Changelog);
            assert_eq!(
                count,
                expected,
                "`{}` appears {count} time(s) in the set `tidy` runs, expected {expected}",
                check.to_possible_value().unwrap().get_name()
            );
        }
    }
}
