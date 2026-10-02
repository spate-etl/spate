//! The command surface: every task CI and a contributor can invoke, and the
//! dispatch that runs one.

mod bench;
mod docs;
mod fuzz;
mod hooks;
mod lint;

use std::collections::BTreeSet;
use std::path::Path;

use clap::Subcommand;

use crate::run::{self, Error, Outcome, Step};

pub(crate) use lint::TidyCheck;

use crate::ci::Scope;

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Everything a pull request must pass
    Ci {
        /// Run only what the diff against REF can affect (default
        /// origin/main), leaving the rest to CI. Not the pull request bar.
        #[arg(long, value_name = "REF", num_args = 0..=1, default_missing_value = "origin/main")]
        since: Option<String>,
    },

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
    Docsrs {
        #[command(flatten)]
        toolchain: fuzz::Toolchain,
    },

    /// Container-backed suites (needs Docker; lanes per ci/README.md)
    IntegrationTest,

    /// Loom concurrency models for the checkpoint and backpressure primitives
    Loom,

    /// Every feature alone, the feature-off combinations, every target, and
    /// the tests on default features
    Hack,

    /// Benchmarks, counted and wall-clock
    Bench {
        #[command(subcommand)]
        cmd: bench::Bench,
    },

    /// Licenses, advisories, bans, sources
    Deny,

    /// Regenerate THIRD-PARTY.md
    Attribution,

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

    /// Check a commit message's subject; the commit-msg hook runs this
    CommitMsg {
        #[arg(value_name = "FILE")]
        file: std::path::PathBuf,
    },

    /// The git hooks under .githooks
    Hooks {
        #[command(subcommand)]
        cmd: HooksCommand,
    },

    /// Fuzz targets
    Fuzz {
        #[command(subcommand)]
        cmd: fuzz::Fuzz,
    },

    /// Assert the built site is complete
    SiteCheck,

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
    ContainerImage(ImageArgs),

    /// Apply .github/labels.yml to the repository
    SyncLabels {
        #[arg(long)]
        dry_run: bool,
        #[arg(long, value_name = "OWNER/NAME")]
        repo: Option<String>,
    },
}

/// Which lane to resolve, and what to do with it.
#[derive(clap::Args)]
pub(crate) struct ImageArgs {
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
    /// Print `breaking` when the release being prepared announces a break, else `none`
    Breaking,
}

#[derive(Subcommand)]
pub(crate) enum HooksCommand {
    /// Point this clone's core.hooksPath at .githooks
    Install,
}

/// Runs one command.
pub(crate) fn dispatch(root: &Path, explain: bool, cmd: Command) -> Outcome {
    match cmd {
        Command::Ci { since } => ci(root, explain, since.as_deref()),
        Command::Lint => lint_group(root, explain),
        Command::Fmt { check } => {
            let mut s = Step::new("cargo", ["fmt", "--all"]);
            if check {
                s = s.arg("--check");
            }
            run::run(root, explain, &s)
        }
        Command::Clippy { no_deny_warnings } => {
            clippy(root, explain, &Select::Workspace, no_deny_warnings)
        }
        Command::Check => run::run(
            root,
            explain,
            &Step::new("cargo", ["check"])
                .args(Select::Workspace.args())
                .args(["--all-features", "--locked"]),
        ),
        Command::Test => test(root, explain, &Select::Workspace),
        Command::Doctest => doctest(root, explain, &Select::Workspace),
        Command::Doc => doc(root, explain, &Select::Workspace),
        Command::Docsrs { toolchain } => {
            crate::checks::docsrs::run(root, explain, toolchain.nightly())
        }
        Command::IntegrationTest => {
            crate::checks::container_image::pull_all(root, explain)?;
            run::run(
                root,
                explain,
                &Step::new(
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
            )
        }
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
        Command::Hack => hack(root, explain, &Select::Workspace),
        Command::Bench { cmd } => bench::dispatch(root, explain, cmd),
        Command::Deny => run::run(
            root,
            explain,
            &Step::new(
                "cargo",
                ["deny", "--all-features", "--locked", "check", "all"],
            ),
        ),
        Command::Attribution => crate::checks::attribution::generate(root, explain),
        Command::Tidy { check, list } => lint::tidy(root, explain, check, list),
        Command::Adr { cmd } => match cmd {
            AdrCommand::New { slug } => crate::checks::adr::new(root, explain, &slug),
        },
        Command::Changelog { cmd } => match &cmd {
            ChangelogCommand::New { kind, slug } => {
                crate::checks::changelog::new(root, explain, kind, slug)
            }
            ChangelogCommand::Build { version } => {
                crate::checks::changelog::build(root, explain, version)
            }
            ChangelogCommand::Notes { version } => {
                crate::checks::changelog::notes(root, explain, version)
            }
            ChangelogCommand::Breaking => crate::checks::changelog::breaking(root, explain),
        },
        Command::CommitMsg { file } => crate::checks::subject::commit_msg(root, explain, &file),
        Command::Hooks { cmd } => match cmd {
            HooksCommand::Install => hooks::install(root, explain),
        },
        Command::Fuzz { cmd } => fuzz::dispatch(root, explain, cmd),
        Command::SiteCheck => crate::checks::site_check::check(root, explain),
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
            use crate::checks::semver_checks;

            if cache_key {
                semver_checks::print_cache_key(root, explain)
            } else if against_registry {
                semver_checks::registry(root, explain, packages.as_deref())
            } else {
                Err(Error::msg(
                    "usage: cargo xtask semver-checks --against-registry [--packages \"a b\"] \
                     | --cache-key",
                ))
            }
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
        Command::ContainerImage(args) => container_image(root, explain, &args),
        Command::SyncLabels { dry_run, repo } => {
            // `DRY_RUN=true` is how the workflow selects it on a pull request.
            let dry_run = dry_run || std::env::var("DRY_RUN").as_deref() == Ok("true");
            crate::checks::sync_labels::sync(root, explain, dry_run, repo.as_deref())
        }
    }
}

/// Resolves or pulls a pinned container image.
///
/// Each mode takes a fixed number of positional arguments, and clap holds the
/// modes apart, so only their arity is checked here.
fn container_image(root: &Path, explain: bool, args: &ImageArgs) -> Outcome {
    use crate::checks::container_image as image;

    let spare = args.service.is_some();
    if args.pull_all {
        if spare {
            return Err(Error::msg("usage: cargo xtask container-image --pull-all"));
        }
        return image::pull_all(root, explain);
    }
    if let Some(svc) = &args.extra_lanes {
        if spare {
            return Err(Error::msg(
                "usage: cargo xtask container-image --extra-lanes <service>",
            ));
        }
        return image::print_extra_lanes(root, explain, svc);
    }
    let Some(svc) = &args.service else {
        return Err(Error::msg(
            "usage: cargo xtask container-image [--ref|--pull] <service> [lane]",
        ));
    };
    let mode = image_mode(args.r#ref, args.pull);
    image::run(root, explain, mode, svc, args.lane.as_deref())
}

/// The mode a pair of flags selects.
fn image_mode(r#ref: bool, pull: bool) -> crate::checks::container_image::Mode {
    use crate::checks::container_image::Mode;
    if pull {
        Mode::Pull
    } else if r#ref {
        Mode::Reference
    } else {
        Mode::Tagged
    }
}

/// Everything a pull request must pass.
fn ci(root: &Path, explain: bool, since: Option<&str>) -> Outcome {
    if let Some(base) = since {
        return match crate::ci::scope_since(root, base)? {
            Scope::Packages(pkgs) => scoped_ci(root, explain, base, &pkgs),
            Scope::Full => {
                println!("note: the diff reaches shared files; running everything.");
                full_ci(root, explain)
            }
        };
    }
    full_ci(root, explain)
}

fn full_ci(root: &Path, explain: bool) -> Outcome {
    lint_group(root, explain)?;
    dispatch(root, explain, Command::Check)?;
    // Nothing else in this chain compiles the `--cfg loom` arm.
    dispatch(root, explain, Command::Loom)?;
    dispatch(root, explain, Command::Test)?;
    dispatch(root, explain, Command::Doctest)?;
    dispatch(root, explain, Command::Doc)?;
    dispatch(root, explain, Command::Hack)?;
    dispatch(root, explain, Command::Deny)?;
    tidy_gates(root, explain)
}

/// The tidy members, then the changelog gate.
fn tidy_gates(root: &Path, explain: bool) -> Outcome {
    lint::tidy(root, explain, None, false)?;
    // Outside the default set because CI splits it into a job that carries the
    // pull request's fields. On a laptop it orients against the upstream, so
    // the gate answers before a push.
    lint::tidy(root, explain, Some(TidyCheck::Changelog), false)
}

/// The Rust gates over the packages a diff can affect, and every tidy member.
/// The type check, `cargo deny`, the packages outside the set and the
/// workspace-wide builds are left to CI.
fn scoped_ci(root: &Path, explain: bool, base: &str, pkgs: &BTreeSet<String>) -> Outcome {
    let listed = pkgs.iter().cloned().collect::<Vec<_>>().join(", ");
    println!(
        "scoped ci against {base}: {}",
        if pkgs.is_empty() {
            "no Rust package affected".to_owned()
        } else {
            listed
        }
    );
    if !pkgs.is_empty() {
        let sel = Select::Packages(pkgs.iter().cloned().collect());
        dispatch(root, explain, Command::Fmt { check: true })?;
        clippy(root, explain, &sel, false)?;
        if pkgs.contains("spate-core") {
            dispatch(root, explain, Command::Loom)?;
        }
        test(root, explain, &sel)?;
        doctest(root, explain, &sel)?;
        doc(root, explain, &sel)?;
        hack(root, explain, &sel)?;
    }
    tidy_gates(root, explain)?;
    if explain {
        return Ok(());
    }
    println!(
        "scoped ci passed. Not run: cargo deny, spate-fuzz and the packages outside the set. \
         `cargo xtask ci` is the pull request bar."
    );
    Ok(())
}

/// Which packages a cargo invocation covers.
enum Select {
    Workspace,
    Packages(Vec<String>),
}

impl Select {
    fn args(&self) -> Vec<String> {
        match self {
            Self::Workspace => vec!["--workspace".to_owned()],
            Self::Packages(pkgs) => pkgs
                .iter()
                .flat_map(|p| ["-p".to_owned(), p.clone()])
                .collect(),
        }
    }
}

fn lint_group(root: &Path, explain: bool) -> Outcome {
    dispatch(root, explain, Command::Fmt { check: true })?;
    clippy(root, explain, &Select::Workspace, false)
}

fn clippy(root: &Path, explain: bool, sel: &Select, no_deny_warnings: bool) -> Outcome {
    let mut s = Step::new("cargo", ["clippy"]).args(sel.args()).args([
        "--all-targets",
        "--all-features",
        "--locked",
        "--",
    ]);
    if !no_deny_warnings {
        s = s.args(["-D", "warnings"]);
    }
    run::run(root, explain, &s)
}

fn test(root: &Path, explain: bool, sel: &Select) -> Outcome {
    run::run(
        root,
        explain,
        &Step::new("cargo", ["nextest", "run"])
            .args(sel.args())
            .args(["--all-features", "--locked"]),
    )
}

fn doctest(root: &Path, explain: bool, sel: &Select) -> Outcome {
    run::run(
        root,
        explain,
        &Step::new("cargo", ["test"]).args(sel.args()).args([
            "--all-features",
            "--locked",
            "--doc",
        ]),
    )
}

fn doc(root: &Path, explain: bool, sel: &Select) -> Outcome {
    run::run(
        root,
        explain,
        &Step::new("cargo", ["doc"])
            .args(sel.args())
            .args(["--no-deps", "--all-features", "--locked"])
            .env("RUSTDOCFLAGS", "-D warnings"),
    )
}

/// The feature matrix, and the test suite on default features.
fn hack(root: &Path, explain: bool, sel: &Select) -> Outcome {
    // `--exclude` needs `--workspace`, so a package selection already names
    // what to leave out.
    let (fuzz, fuzz_and_xtask): (&[&str], &[&str]) = match sel {
        Select::Workspace => (
            &["--exclude", "spate-fuzz"],
            &["--exclude", "spate-xtask", "--exclude", "spate-fuzz"],
        ),
        Select::Packages(_) => (&[], &[]),
    };
    let mut steps = vec![
        // `cargo hack --no-dev-deps` rewrites each Cargo.toml as it runs,
        // which a locked build refuses. Do not add `--locked`; it fails.
        // It restores each Cargo.toml only when it is finished, so every
        // locked step goes after it.
        Step::new("cargo", ["hack", "check"])
            .args(sel.args())
            .args(["--each-feature", "--no-dev-deps"])
            .args(fuzz_and_xtask),
    ];
    // Stripping dev-dependencies drops test and bench targets, so the run
    // above reaches no test target in any crate. These steps build them, on
    // the axes it covers for the library: features off, then the default set.
    if match sel {
        Select::Workspace => true,
        Select::Packages(pkgs) => pkgs.iter().any(|p| p == "spate-coordination"),
    } {
        steps.push(Step::new(
            "cargo",
            [
                "check",
                "-p",
                "spate-coordination",
                "--no-default-features",
                "--tests",
                "--locked",
            ],
        ));
    }
    // The workspace-wide build on default features. spate-fuzz is excluded
    // because it requires `testing` on spate-s3 and spate-coordination, which
    // the resolver would unify into every other crate in the same invocation.
    steps.push(
        Step::new("cargo", ["check"])
            .args(sel.args())
            .args(["--all-targets", "--locked"])
            .args(fuzz),
    );
    // The test suite on default features, which runs the feature-off arm of
    // tests that `--all-features` skips. spate-fuzz has no tests and
    // spate-xtask no features.
    steps.push(
        Step::new("cargo", ["nextest", "run"])
            .args(sel.args())
            .args(["--locked"])
            .args(fuzz_and_xtask),
    );
    run::steps(root, explain, &steps)
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

    /// Each non-comment line of a YAML file as `(indent, key, value)`, with a
    /// list item's `- ` counted as indent and a trailing ` # comment` dropped.
    /// Enough for the flat files the tests below read.
    fn entries(text: &str) -> Vec<(usize, &str, &str)> {
        text.lines()
            .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
            .map(|line| {
                let body = line.trim_start();
                let indent = line.len() - body.len();
                let (indent, body) = match body.strip_prefix("- ") {
                    Some(item) => (indent + 2, item),
                    None => (indent, body),
                };
                let body = body.split_once(" #").map_or(body, |(b, _)| b);
                let (key, value) = body.split_once(':').unwrap_or((body, ""));
                (indent, key.trim(), value.trim())
            })
            .collect()
    }

    /// `title.yml` runs on `pull_request_target`, with the default branch's
    /// token and cache scope. This pins that it checks out no pull request
    /// code, holds no permission, uses no cache, and passes the title only
    /// through `env:`, in the workflow and in the `setup-rust` action it uses.
    #[test]
    fn the_title_workflow_keeps_its_pull_request_target_posture() {
        let root = crate::repo_root().unwrap();
        let text = std::fs::read_to_string(root.join(".github/workflows/title.yml")).unwrap();
        let all = entries(&text);
        let values = |key: &str| -> Vec<&str> {
            all.iter()
                .filter(|(_, k, _)| *k == key)
                .map(|(_, _, v)| *v)
                .collect()
        };

        // Any other key, `ref`, `repository`, `allow-unsafe-pr-checkout`,
        // `shared-key` and `contents` among them, fails here.
        let allowed = [
            "name",
            "on",
            "pull_request_target",
            "types",
            "concurrency",
            "group",
            "cancel-in-progress",
            "permissions",
            "jobs",
            "title",
            "runs-on",
            "timeout-minutes",
            "steps",
            "uses",
            "with",
            "persist-credentials",
            "env",
            "EVENT_NAME",
            "PR_TITLE",
            "PR_NUMBER",
            "PR_AUTHOR",
            "run",
        ];
        for (_, key, value) in &all {
            assert!(
                allowed.contains(key),
                "title.yml sets `{key}: {value}`, a key this test does not allow"
            );
        }

        let on = all.iter().position(|(_, k, _)| *k == "on").unwrap();
        let triggers: Vec<_> = all[on + 1..]
            .iter()
            .take_while(|(indent, _, _)| *indent > 0)
            .map(|(_, k, v)| (*k, *v))
            .collect();
        assert_eq!(
            triggers,
            [
                ("pull_request_target", ""),
                ("types", "[opened, edited, reopened]")
            ],
            "title.yml runs on another trigger"
        );

        let permissions: Vec<_> = all.iter().filter(|(_, k, _)| *k == "permissions").collect();
        assert_eq!(
            permissions,
            [&(0, "permissions", "{}")],
            "title.yml grants its token a permission"
        );

        let uses = values("uses");
        assert!(
            uses.len() == 2
                && uses[0].starts_with("actions/checkout@")
                && uses[1] == "./.github/actions/setup-rust",
            "title.yml uses {uses:?}, not only the checkout and setup-rust"
        );
        assert_eq!(values("with").len(), 1, "only the checkout takes inputs");
        assert_eq!(
            values("persist-credentials"),
            ["false"],
            "the checkout persists its credential"
        );

        let expressions: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|line| line.contains("${{"))
            .collect();
        assert_eq!(
            expressions,
            [
                "group: title-${{ github.event.pull_request.number }}",
                "PR_TITLE: ${{ github.event.pull_request.title }}",
                "PR_NUMBER: ${{ github.event.pull_request.number }}",
                "PR_AUTHOR: ${{ github.event.pull_request.user.login }}",
            ],
            "title.yml evaluates an expression outside `env:`"
        );
        assert_eq!(
            values("EVENT_NAME"),
            ["pull_request"],
            "`tidy title` passes without checking unless EVENT_NAME is `pull_request`"
        );
        assert_eq!(values("run"), ["cargo xtask tidy title"]);

        // setup-rust runs an action beyond the toolchain only for an input
        // this workflow leaves empty, and reads nothing of the pull request.
        let text =
            std::fs::read_to_string(root.join(".github/actions/setup-rust/action.yml")).unwrap();
        for forbidden in [
            "github.event",
            "github.head_ref",
            "github.token",
            "secrets.",
            "GITHUB_EVENT_PATH",
            "GITHUB_HEAD_REF",
        ] {
            assert!(!text.contains(forbidden), "setup-rust reads `{forbidden}`");
        }
        let composite = entries(&text);
        for (i, (_, key, value)) in composite.iter().enumerate() {
            if *key != "uses" || value.starts_with("dtolnay/rust-toolchain@") {
                continue;
            }
            let guard = if value.starts_with("Swatinem/rust-cache@") {
                "inputs.shared-key != ''"
            } else if value.starts_with("taiki-e/install-action@") {
                "inputs.tools != ''"
            } else {
                panic!("setup-rust uses `{value}`, which title.yml would run");
            };
            assert_eq!(
                composite[i - 1],
                (composite[i].0, "if", guard),
                "setup-rust runs `{value}` without `if: {guard}`"
            );
        }
        // The guards hold only while the inputs title.yml leaves unset stay
        // empty by default.
        for input in ["shared-key", "tools"] {
            assert_eq!(
                input_default(&text, input),
                Some("\"\""),
                "setup-rust's `{input}` input no longer defaults to empty"
            );
        }
    }

    /// The `default:` of one input of a composite action, as written.
    fn input_default<'a>(action: &'a str, input: &str) -> Option<&'a str> {
        let header = format!("  {input}:");
        let mut lines = action.lines().skip_while(|line| *line != header).skip(1);
        lines
            .by_ref()
            .take_while(|line| line.is_empty() || line.starts_with("    "))
            .find_map(|line| line.trim_start().strip_prefix("default:"))
            .map(str::trim)
    }

    /// Each mode takes a fixed number of positional arguments, and a spare one
    /// is a usage error.
    #[test]
    fn a_spare_positional_argument_is_a_usage_error() {
        let root = crate::repo_root().unwrap();
        let call = |pull_all, extra: Option<&str>, service: Option<&str>, lane: Option<&str>| {
            let args = super::ImageArgs {
                r#ref: false,
                pull: false,
                pull_all,
                extra_lanes: extra.map(str::to_owned),
                service: service.map(str::to_owned),
                lane: lane.map(str::to_owned),
            };
            super::container_image(&root, true, &args)
                .unwrap_err()
                .message
        };
        assert_eq!(
            call(true, None, Some("clickhouse"), None),
            "usage: cargo xtask container-image --pull-all"
        );
        assert_eq!(
            call(false, Some("clickhouse"), Some("lts"), None),
            "usage: cargo xtask container-image --extra-lanes <service>"
        );
        assert_eq!(
            call(false, None, None, Some("lts")),
            "usage: cargo xtask container-image [--ref|--pull] <service> [lane]"
        );
        assert_eq!(
            call(false, None, None, None),
            "usage: cargo xtask container-image [--ref|--pull] <service> [lane]"
        );
    }

    #[test]
    fn each_image_flag_selects_its_own_mode() {
        use crate::checks::container_image::Mode;
        assert_eq!(super::image_mode(false, false), Mode::Tagged);
        assert_eq!(super::image_mode(true, false), Mode::Reference);
        assert_eq!(super::image_mode(false, true), Mode::Pull);
    }

    #[test]
    fn every_check_is_reachable_from_tidy() {
        let listed = super::lint::ALL;
        for check in TidyCheck::value_variants() {
            let count = listed.iter().filter(|c| *c == check).count();
            let expected = usize::from(!matches!(check, TidyCheck::Changelog | TidyCheck::Title));
            assert_eq!(
                count,
                expected,
                "`{}` appears {count} time(s) in the set `tidy` runs, expected {expected}",
                check.to_possible_value().unwrap().get_name()
            );
        }
    }
}
