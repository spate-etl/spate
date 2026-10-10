//! The release: the workspace version and the literals that carry it, and the
//! sequence that assembles, publishes and closes out a version.

mod io;
mod sequence;
pub(crate) mod version;

use std::path::{Path, PathBuf};

use clap::Subcommand;

use crate::run::{self, Error, Outcome, Step, Streams};
use io::{CratesIo, DryForge, DryGit, Gh, Host, LocalWorkspace, ProcessGit};
use version::Version;

#[derive(Subcommand)]
pub(crate) enum ReleaseCommand {
    /// The workspace version and the literals that carry it
    Version {
        #[command(subcommand)]
        cmd: version::VersionCommand,
    },
    /// Build the release commit and open the pull request
    Assemble {
        #[arg(long, value_name = "X.Y.Z")]
        version: String,
        /// Build the commit, and print the pushes and pull request changes
        /// instead of making them
        #[arg(long)]
        dry_run: bool,
    },
    /// Verify the release commit, select the pending crates and package them
    Prepare {
        /// Also generate the SBOMs, and print what a real run would do next
        #[arg(long)]
        dry_run: bool,
        /// The commit any already-published crate must have come from
        #[arg(long, env = "EXPECTED_SHA", value_name = "SHA")]
        expected_sha: Option<String>,
        /// Stage the packaged crates, their SBOMs and SHA256SUMS here
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
    },
    /// Verify staged artifacts against their checksums and attestations
    VerifyArtifacts {
        /// The directory `prepare --out` staged, with the attestation bundles
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        /// The workflow that must have signed every attestation, as
        /// OWNER/REPO/.github/workflows/FILE
        #[arg(long, value_name = "WORKFLOW")]
        signer_workflow: Option<String>,
        /// The release commit every attestation must name
        #[arg(long, env = "EXPECTED_SHA", value_name = "SHA")]
        expected_sha: Option<String>,
    },
    /// Upload the crates the registry does not yet hold, with CARGO_REGISTRY_TOKEN
    Upload {
        /// The staged artifacts; every crate uploaded must be among them
        #[arg(long, value_name = "DIR")]
        artifacts: PathBuf,
    },
    /// Verify the registry, then tag, release and deploy the docs
    Finish {
        #[arg(long, env = "VERSION", value_name = "X.Y.Z")]
        version: Option<String>,
        /// The release commit
        #[arg(long, env = "EXPECTED_SHA", value_name = "SHA")]
        expected_sha: Option<String>,
        /// The staged artifacts and their attestation bundles
        #[arg(long, value_name = "DIR")]
        artifacts: PathBuf,
    },
    /// The whole release in a throwaway worktree, nothing pushed or uploaded
    DryRun {
        #[arg(long, value_name = "X.Y.Z")]
        version: String,
        /// Keep the worktree after a successful run
        #[arg(long)]
        keep: bool,
    },
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: ReleaseCommand) -> Outcome {
    if let ReleaseCommand::Version { cmd } = &cmd {
        return version::dispatch(root, explain, cmd);
    }
    if explain {
        println!("{}", describe(&cmd));
        return Ok(());
    }

    let git = ProcessGit { root };
    let forge = Gh { root };
    let registry = CratesIo { root };
    let workspace = LocalWorkspace { root };
    let pause = |d| std::thread::sleep(d);
    let host = Host {
        git: &git,
        forge: &forge,
        registry: &registry,
        workspace: &workspace,
        pause: &pause,
    };

    match cmd {
        ReleaseCommand::Version { .. } => Ok(()),
        ReleaseCommand::Assemble { version, dry_run } => {
            if !dry_run {
                require_env(&["GH_TOKEN", "GITHUB_REPOSITORY"], "assemble")?;
            }
            preflight(root, false)?;
            if dry_run {
                // The release commit is real even in a rehearsal, so it is made
                // only on a detached head, as `dry-run`'s worktree has.
                if !head_is_detached(root)? {
                    return Err(Error::msg(
                        "assemble --dry-run commits the release, so it runs on a detached head;\n  \
                         use `cargo xtask release dry-run`, which makes one in a throwaway worktree",
                    ));
                }
                let (git, forge) = (DryGit(&git), DryForge(&forge));
                sequence::assemble(
                    &Host {
                        git: &git,
                        forge: &forge,
                        ..host
                    },
                    &version,
                )?;
                println!("release: dry run; nothing was pushed and no pull request was changed.");
                Ok(())
            } else {
                sequence::assemble(&host, &version)
            }
        }
        ReleaseCommand::Prepare {
            dry_run,
            expected_sha,
            out,
        } => {
            if dry_run || out.is_some() {
                preflight_sbom(root)?;
            }
            let expected = expected_sha.filter(|s| !s.is_empty());
            let prepared = sequence::prepare(&host, expected.as_deref())?;
            sequence::write_outputs(&prepared)?;
            match (&out, dry_run) {
                (Some(dir), _) => sequence::stage(&host, &prepared, dir)?,
                (None, true) => {
                    let scratch = crate::checks::scratch::Scratch::new("spate-release-stage")?;
                    sequence::stage(&host, &prepared, scratch.dir())?;
                }
                (None, false) => {}
            }
            if dry_run {
                sequence::print_next(&prepared);
            }
            Ok(())
        }
        ReleaseCommand::VerifyArtifacts {
            dir,
            signer_workflow,
            expected_sha,
        } => {
            let expected = expected_sha
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::msg("verify-artifacts needs EXPECTED_SHA"))?;
            require_gh(root, GH_EXACT_SIGNER)?;
            let signer = match signer_workflow.filter(|s| !s.is_empty()) {
                Some(s) => s,
                None => format!(
                    "{}/.github/workflows/release-build.yml",
                    std::env::var("GITHUB_REPOSITORY")
                        .unwrap_or_else(|_| "spate-etl/spate".to_owned())
                ),
            };
            sequence::verify_artifacts(&host, &dir, &signer, &expected)
        }
        ReleaseCommand::Upload { artifacts } => {
            let token = std::env::var("CARGO_REGISTRY_TOKEN").is_ok_and(|t| !t.is_empty());
            sequence::upload(&host, token, &artifacts)
        }
        ReleaseCommand::Finish {
            version,
            expected_sha,
            artifacts,
        } => {
            let version = version
                .filter(|v| !v.is_empty())
                .ok_or_else(|| Error::msg("finish needs VERSION"))?;
            let version = Version::parse(&version)
                .ok_or_else(|| Error::msg(format!("'{version}' is not X.Y.Z")))?;
            let expected_sha = expected_sha
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::msg("finish needs EXPECTED_SHA"))?;
            require_env(
                &["GH_TOKEN", "DISPATCH_TOKEN", "GITHUB_REPOSITORY"],
                "finish",
            )?;
            sequence::finish(&host, version, &expected_sha, &artifacts)
        }
        ReleaseCommand::DryRun { version, keep } => dry_run(root, &version, keep),
    }
}

fn head_is_detached(root: &Path) -> Result<bool, Error> {
    let attached = run::complete(
        root,
        &Step::new("git", ["symbolic-ref", "-q", "HEAD"]),
        Streams::Discard,
    )?;
    Ok(attached.code != 0)
}

/// Fails naming the first of `keys` that is unset or empty.
fn require_env(keys: &[&str], step: &str) -> Outcome {
    match keys
        .iter()
        .find(|k| std::env::var(k).unwrap_or_default().is_empty())
    {
        Some(key) => Err(Error::msg(format!("{step} needs {key}"))),
        None => Ok(()),
    }
}

fn describe(cmd: &ReleaseCommand) -> &'static str {
    match cmd {
        ReleaseCommand::Version { .. } => "",
        ReleaseCommand::Assemble { .. } => {
            "(guards, generates the release commit, and opens or refreshes its pull request)"
        }
        ReleaseCommand::Prepare { .. } => {
            "(verifies the release commit against the registry, then packages the pending crates)"
        }
        ReleaseCommand::VerifyArtifacts { .. } => {
            "(checks SHA256SUMS, then gh attestation verify on each staged artifact)"
        }
        ReleaseCommand::Upload { .. } => {
            "cargo publish --workspace --locked --no-verify --exclude <each crate already published>"
        }
        ReleaseCommand::Finish { .. } => {
            "(verifies the registry, tags, opens the GitHub release with its assets, deploys the docs)"
        }
        ReleaseCommand::DryRun { .. } => {
            "(runs assemble --dry-run and prepare --dry-run in a throwaway worktree)"
        }
    }
}

/// The first `gh` whose `--signer-workflow` matches the whole workflow path
/// rather than a prefix of it.
const GH_EXACT_SIGNER: (u64, u64, u64) = (2, 102, 0);

/// Fails unless the `gh` on the path is at least `min`.
fn require_gh(root: &Path, min: (u64, u64, u64)) -> Outcome {
    let out = run::capture(root, &Step::new("gh", ["--version"]))?;
    let found = gh_version(&out)
        .ok_or_else(|| Error::msg(format!("cannot read a version from `gh --version`: {out}")))?;
    if !gh_at_least(found, min) {
        return Err(Error::msg(format!(
            "gh {}.{}.{} matches --signer-workflow as a prefix; {}.{}.{} or later is required",
            found.0, found.1, found.2, min.0, min.1, min.2
        )));
    }
    Ok(())
}

fn gh_at_least(found: (u64, u64, u64), min: (u64, u64, u64)) -> bool {
    found >= min
}

/// The version on the first line of `gh --version`, `gh version X.Y.Z (date)`.
fn gh_version(out: &str) -> Option<(u64, u64, u64)> {
    let word = out.lines().next()?.split_whitespace().nth(2)?;
    let mut parts = word.split('.').map(|p| p.parse::<u64>().ok());
    Some((parts.next()??, parts.next()??, parts.next()??))
}

/// Names whatever a local release run is missing before any step starts.
fn preflight(root: &Path, sbom: bool) -> Outcome {
    let missing: Vec<&str> = ["gh", "curl"]
        .into_iter()
        .filter(|tool| !run::on_path(tool))
        .collect();
    if !missing.is_empty() {
        return Err(Error::msg(format!(
            "missing tool(s): {}",
            missing.join(" ")
        )));
    }
    // The changelog assembly resolves pull-request numbers through the API;
    // unauthenticated, every derived reference degrades.
    let authenticated = std::env::var("GH_TOKEN").is_ok_and(|t| !t.is_empty())
        || run::complete(root, &Step::new("gh", ["auth", "status"]), Streams::Discard)?.code == 0;
    if !authenticated {
        return Err(Error::msg("gh is not authenticated and GH_TOKEN is unset"));
    }
    let present = |sub: &str| -> Result<bool, Error> {
        Ok(run::complete(
            root,
            &Step::new("cargo", [sub, "--version"]),
            Streams::Discard,
        )?
        .code
            == 0)
    };
    if !present("about")? {
        return Err(Error::msg(
            "cargo-about is required and was not found. Install it with:\n  \
             cargo install cargo-about --locked --features cli",
        ));
    }
    if sbom {
        preflight_sbom(root)?;
    }
    Ok(())
}

fn preflight_sbom(root: &Path) -> Outcome {
    let found = run::complete(
        root,
        &Step::new("cargo", ["cyclonedx", "--version"]),
        Streams::Discard,
    )?
    .code
        == 0;
    if !found {
        return Err(Error::msg(
            "cargo-cyclonedx is required and was not found. Install it with:\n  \
             cargo install cargo-cyclonedx --locked",
        ));
    }
    Ok(())
}

/// Runs `assemble --dry-run` and `prepare --dry-run` in a detached worktree of
/// `HEAD`. The worktree is removed after a successful run unless `keep` is set,
/// and kept after a failure.
fn dry_run(root: &Path, version: &str, keep: bool) -> Outcome {
    preflight(root, true)?;
    // The worktree is made from HEAD, so uncommitted work would be silently
    // left out of the rehearsal.
    if !run::capture(root, &Step::new("git", ["status", "--porcelain"]))?.is_empty() {
        return Err(Error::msg(
            "the working tree is not clean; a release is assembled from committed state only",
        ));
    }
    let parent =
        std::env::temp_dir().join(crate::checks::scratch::unique_name("spate-release-dry-run"));
    let tree = parent.join(format!("v{version}"));
    std::fs::create_dir_all(&parent)
        .map_err(|e| Error::msg(format!("{}: {e}", parent.display())))?;
    let tree_arg = tree.to_string_lossy().into_owned();
    run::quiet(
        root,
        false,
        &Step::new("git", ["worktree", "add", "--detach", &tree_arg, "HEAD"]),
    )?;
    println!("release: dry run in {tree_arg}");
    // The `xtask` binary bakes in its checkout's path, so the worktree's build
    // must not land in a target directory the main checkout shares.
    let target = tree.join("target").to_string_lossy().into_owned();

    let outcome = run::run(
        root,
        false,
        &Step::new(
            "cargo",
            [
                "xtask",
                "release",
                "assemble",
                "--version",
                version,
                "--dry-run",
            ],
        )
        .env("CARGO_TARGET_DIR", target.clone())
        .dir(&tree_arg),
    )
    .and_then(|()| {
        run::run(
            root,
            false,
            &Step::new("cargo", ["xtask", "release", "prepare", "--dry-run"])
                .env("EXPECTED_SHA", "")
                .env("CARGO_TARGET_DIR", target)
                .dir(&tree_arg),
        )
    });

    let remove = format!(
        "git worktree remove --force {tree_arg} && rm -rf {}",
        parent.display()
    );
    let removed = outcome.is_ok()
        && !keep
        && run::quiet(
            root,
            false,
            &Step::new("git", ["worktree", "remove", "--force", &tree_arg]),
        )
        .is_ok();
    if removed {
        drop(std::fs::remove_dir_all(&parent));
        println!("\nrelease: dry run complete; the worktree is removed.");
    } else {
        println!(
            "\nrelease: the worktree is at:\n  {tree_arg}\n\
             Inspect it with:\n  git -C {tree_arg} log --stat -1\n\
             Remove it with:\n  {remove}"
        );
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checks::scratch::Scratch;

    fn git(root: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .current_dir(root)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    /// A checked-out branch is an attached head, and `checkout --detach` is not.
    #[test]
    fn a_branch_checkout_is_not_a_detached_head() {
        let scratch = Scratch::new("spate-xtask-detached").unwrap();
        let root = scratch.dir();
        git(root, &["init", "-q"]);
        git(root, &["commit", "-q", "--allow-empty", "-m", "x"]);
        assert!(!head_is_detached(root).unwrap());
        git(root, &["checkout", "-q", "--detach"]);
        assert!(head_is_detached(root).unwrap());
    }

    /// The version is the third word of `gh --version`'s first line.
    #[test]
    fn the_gh_version_is_read_from_its_banner() {
        assert_eq!(
            gh_version(
                "gh version 2.102.0 (2026-09-29)\nhttps://github.com/cli/cli/releases/tag/v2.102.0\n"
            ),
            Some((2, 102, 0))
        );
        assert!(gh_version("gh version 2.89.0 (2026-03-01)").unwrap() < (2, 102, 0));
        assert_eq!(gh_version("garbage"), None);
    }

    /// 2.102.0 is the first `gh` accepted for verification.
    #[test]
    fn gh_from_2_102_0_matches_the_signer_exactly() {
        assert!(gh_at_least((2, 102, 0), GH_EXACT_SIGNER));
        assert!(gh_at_least((3, 0, 0), GH_EXACT_SIGNER));
        assert!(!gh_at_least((2, 101, 9), GH_EXACT_SIGNER));
    }
}
