//! Everything the release sequence reads from or writes to outside its own
//! logic: git, the forge, the registry and the workspace, each a trait with a
//! process-backed implementation, plus the wrappers a dry run substitutes for
//! the writes.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde::Deserialize;

use super::version::Version;
use crate::checks::scratch::Scratch;
use crate::checks::semver_checks::{UA, fetch_index};
use crate::run::{self, Error, Outcome, Step};

/// The identity the release commit and tag carry.
const BOT_NAME: &str = "spate-release[bot]";
const BOT_EMAIL: &str = "spate-release[bot]@users.noreply.github.com";

const API: &str = "https://crates.io/api/v1/crates";

/// The collaborators one release step works through.
pub(crate) struct Host<'a> {
    pub(crate) git: &'a dyn Git,
    pub(crate) forge: &'a dyn Forge,
    pub(crate) registry: &'a dyn Registry,
    pub(crate) workspace: &'a dyn Workspace,
    /// Waits out a retry interval or a rate limit.
    pub(crate) pause: &'a dyn Fn(Duration),
}

pub(crate) trait Git {
    fn is_clean(&self) -> Result<bool, Error>;
    /// The newest `vX.Y.Z` tag, or empty when there is none.
    fn last_tag(&self) -> Result<String, Error>;
    fn subject(&self) -> Result<String, Error>;
    fn short_head(&self) -> Result<String, Error>;
    /// Commits every tracked change as the release identity.
    fn commit_all(&self, subject: &str, body: &str) -> Outcome;
    /// The commit `tag` names on origin, peeled through an annotated tag, or
    /// `None` when origin has no such tag.
    fn remote_tag(&self, tag: &str) -> Result<Option<String>, Error>;
    /// Creates an annotated tag locally.
    fn tag(&self, name: &str, commit: &str) -> Outcome;
    /// Pushes `refspec` to the repository's origin with the forge token.
    fn push(&self, refspec: &str, force: bool) -> Outcome;
}

/// An open pull request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pull {
    pub(crate) number: u64,
    pub(crate) head: String,
    pub(crate) cross_repository: bool,
}

pub(crate) trait Forge {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error>;
    /// Closes a pull request with a comment and deletes its branch.
    fn close_pull(&self, number: u64, comment: &str) -> Outcome;
    /// The open same-repository pull request from `head`, if any.
    fn pull_for_head(&self, head: &str) -> Result<Option<u64>, Error>;
    /// Opens a pull request, answering its number, or `None` when none was
    /// opened.
    fn create_pull(
        &self,
        title: &str,
        head: &str,
        label: &str,
        body: &str,
    ) -> Result<Option<u64>, Error>;
    /// Squash-merges the pull request once its required checks pass.
    fn auto_merge(&self, number: u64) -> Outcome;
    fn release_state(&self, tag: &str) -> Result<ReleaseState, Error>;
    /// Creates a draft release on an existing tag.
    fn create_release(&self, tag: &str, notes: &str) -> Outcome;
    /// Publishes a draft release.
    fn publish_release(&self, tag: &str) -> Outcome;
    /// Verifies the attestation GitHub signs for an immutable release.
    fn verify_release(&self, tag: &str) -> Outcome;
    fn upload(&self, tag: &str, files: &[PathBuf], clobber: bool) -> Outcome;
    /// The names of the release's fully uploaded assets.
    fn assets(&self, tag: &str) -> Result<Vec<String>, Error>;
    /// Starts the documentation deploy.
    fn dispatch_docs(&self) -> Outcome;
    /// Verifies that an attestation in `bundle`, of `predicate_type`, covers
    /// `file` and was signed by `signer_workflow` running from `main` at
    /// `commit`.
    fn verify_attestation(
        &self,
        file: &Path,
        bundle: Option<&Path>,
        signer_workflow: &str,
        predicate_type: &str,
        commit: &str,
    ) -> Outcome;
}

/// Where a GitHub release stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseState {
    Missing,
    /// Assets can still change.
    Draft,
    /// Published; an immutable release's assets and tag are locked.
    Published {
        immutable: bool,
    },
}

/// One version in a crate's sparse-index file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) vers: String,
    pub(crate) cksum: String,
}

/// What the sparse index answered for one crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Index {
    Found(Vec<IndexEntry>),
    /// The registry holds no crate of that name.
    Missing,
    /// Any other HTTP status.
    Status(String),
}

pub(crate) trait Registry {
    fn index(&self, krate: &str) -> Result<Index, Error>;
    /// The commit a Trusted Publishing upload of this version recorded, or
    /// `"null"` for a version published any other way.
    fn trustpub_sha(&self, krate: &str, version: Version) -> Result<String, Error>;
    /// Downloads the `.crate` the registry serves into `dir`.
    fn download(&self, krate: &str, version: Version, dir: &Path) -> Result<PathBuf, Error>;
}

/// A publishable workspace member and its manifest directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Package {
    pub(crate) name: String,
    pub(crate) dir: PathBuf,
}

/// What a consumer's resolution of the release came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// The lockfile resolved, with this many packages.
    Resolved(usize),
    /// Cargo refused; the tail of its output.
    Failed(String),
}

pub(crate) trait Workspace {
    /// The members `cargo publish --workspace` uploads.
    fn publishable(&self) -> Result<Vec<Package>, Error>;
    /// Publishable members missing a description or a license.
    fn missing_metadata(&self) -> Result<Vec<String>, Error>;
    fn version(&self) -> Result<Version, Error>;
    /// The version the history since the last tag implies.
    fn derive(&self) -> Result<Version, Error>;
    /// Writes every generated artifact of the release commit.
    fn generate(&self, version: Version) -> Outcome;
    /// The changelog section for `version`.
    fn notes(&self, version: Version) -> Result<String, Error>;
    /// Packages and verify-builds every publishable member but `excludes`.
    fn package(&self, excludes: &[String]) -> Outcome;
    /// Packages every publishable member but `excludes` without a verify build.
    fn package_unverified(&self, excludes: &[String]) -> Outcome;
    /// Uploads every member but `excludes`, without verifying again.
    fn publish(&self, excludes: &[String]) -> Outcome;
    /// The `.crate` this run packaged, or `None` when it packaged none for
    /// that crate.
    fn crate_file(&self, krate: &str, version: Version) -> Option<PathBuf>;
    /// The hex sha256 of a file.
    fn sha256(&self, path: &Path) -> Result<String, Error>;
    /// Resolves a scratch consumer of the facade at exactly `version` from the
    /// registry.
    fn resolve(&self, version: Version) -> Result<Resolution, Error>;
    /// Writes one CycloneDX SBOM per publishable crate into `out`.
    fn sboms(&self, version: Version, out: &Path) -> Outcome;
}

// ---------------------------------------------------------------------------
// Process-backed implementations.
// ---------------------------------------------------------------------------

/// Git in the repository at `root`, pushing with `GH_TOKEN` to
/// `GITHUB_REPOSITORY`.
pub(crate) struct ProcessGit<'a> {
    pub(crate) root: &'a Path,
    /// Where tags are read from: a remote name or a URL.
    pub(crate) remote: String,
}

impl ProcessGit<'_> {
    fn capture(&self, args: &[&str]) -> Result<String, Error> {
        run::capture(self.root, &Step::new("git", args))
    }
}

impl Git for ProcessGit<'_> {
    fn is_clean(&self) -> Result<bool, Error> {
        Ok(self.capture(&["status", "--porcelain"])?.is_empty())
    }

    fn last_tag(&self) -> Result<String, Error> {
        crate::checks::semver_checks::last_tag(self.root)
    }

    fn subject(&self) -> Result<String, Error> {
        Ok(self
            .capture(&["log", "-1", "--format=%s"])?
            .trim_end()
            .to_owned())
    }

    fn short_head(&self) -> Result<String, Error> {
        Ok(self
            .capture(&["rev-parse", "--short", "HEAD"])?
            .trim_end()
            .to_owned())
    }

    fn commit_all(&self, subject: &str, body: &str) -> Outcome {
        run::run(
            self.root,
            false,
            &Step::new("git", ["-c"])
                .arg(format!("user.name={BOT_NAME}"))
                .arg("-c")
                .arg(format!("user.email={BOT_EMAIL}"))
                .args([
                    "commit",
                    "--all",
                    "--quiet",
                    "--message",
                    subject,
                    "--message",
                    body,
                ]),
        )?;
        run::run(
            self.root,
            false,
            &Step::new("git", ["show", "--stat", "--format=%h %s", "HEAD"]),
        )
    }

    fn remote_tag(&self, tag: &str) -> Result<Option<String>, Error> {
        // `ls-remote` exits zero with no output for an absent tag, so a network
        // failure aborts here rather than reading as absence.
        let listing = self.capture(&[
            "ls-remote",
            "--tags",
            &self.remote,
            &format!("refs/tags/{tag}"),
            &format!("refs/tags/{tag}^{{}}"),
        ])?;
        Ok(peeled(&listing))
    }

    fn tag(&self, name: &str, commit: &str) -> Outcome {
        run::run(
            self.root,
            false,
            &Step::new("git", ["-c"])
                .arg(format!("user.name={BOT_NAME}"))
                .arg("-c")
                .arg(format!("user.email={BOT_EMAIL}"))
                .args(["tag", "-a", name, "-m", name, commit]),
        )
    }

    fn push(&self, refspec: &str, force: bool) -> Outcome {
        let token = required_env("GH_TOKEN", "push")?;
        let repo = required_env("GITHUB_REPOSITORY", "push")?;
        let mut command = Command::new("git");
        command
            .arg("push")
            .args(force.then_some("--force"))
            .arg(format!(
                "https://x-access-token:{token}@github.com/{repo}.git"
            ))
            .arg(refspec)
            .current_dir(self.root);
        // Run by hand: the URL carries the token, and `run::run` would print it
        // in its failure message.
        let status = command
            .status()
            .map_err(|e| Error::msg(format!("git push: {e}")))?;
        if !status.success() {
            return Err(Error::msg(format!(
                "git push of {refspec} to {repo} failed"
            )));
        }
        Ok(())
    }
}

/// The commit an `ls-remote` listing of one tag names. The `^{}` line is the
/// commit an annotated tag points at; a lightweight tag has only the plain line.
pub(crate) fn peeled(listing: &str) -> Option<String> {
    let lines: Vec<(&str, &str)> = listing.lines().filter_map(|l| l.split_once('\t')).collect();
    lines
        .iter()
        .find(|(_, r)| r.ends_with("^{}"))
        .or_else(|| lines.first())
        .map(|(sha, _)| (*sha).to_owned())
}

fn required_env(key: &str, what: &str) -> Result<String, Error> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::msg(format!("{what} needs {key}")))
}

/// The forge through `gh`, authenticated by `GH_TOKEN`.
pub(crate) struct Gh<'a> {
    pub(crate) root: &'a Path,
    /// The repository as OWNER/NAME, or `None` for the one gh infers from the
    /// checkout.
    pub(crate) repo: Option<&'a str>,
}

impl<'a> Gh<'a> {
    fn step(&self, args: &[&str]) -> Step<'a> {
        let step = Step::new("gh", args);
        match self.repo {
            Some(repo) => step.env("GH_REPO", repo),
            None => step,
        }
    }

    fn capture(&self, args: &[&str]) -> Result<String, Error> {
        run::capture(self.root, &self.step(args))
    }

    fn run(&self, step: &Step<'_>) -> Outcome {
        run::run(self.root, false, step)
    }

    /// `gh release create` making a draft on a tag that already exists.
    fn create_step(&self, tag: &str, notes: &Path) -> Step<'a> {
        self.step(&[
            "release",
            "create",
            tag,
            "--draft",
            "--verify-tag",
            "--title",
            tag,
        ])
        .arg("--notes-file")
        .arg(notes.to_string_lossy())
    }

    fn publish_step(&self, tag: &str) -> Step<'a> {
        self.step(&["release", "edit", tag, "--draft=false"])
    }
}

impl Forge for Gh<'_> {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error> {
        parse_pulls(&self.capture(&[
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            "100",
            "--json",
            "number,headRefName,isCrossRepository",
        ])?)
    }

    fn close_pull(&self, number: u64, comment: &str) -> Outcome {
        self.run(
            &Step::new("gh", ["pr", "close"])
                .arg(number.to_string())
                .args(["--delete-branch", "--comment", comment]),
        )
    }

    fn pull_for_head(&self, head: &str) -> Result<Option<u64>, Error> {
        // `--head` filters on the server, so no listing limit applies.
        let raw = self.capture(&[
            "pr",
            "list",
            "--state",
            "open",
            "--head",
            head,
            "--json",
            "number,headRefName,isCrossRepository",
        ])?;
        head_pull(&raw, head)
    }

    fn create_pull(
        &self,
        title: &str,
        head: &str,
        label: &str,
        body: &str,
    ) -> Result<Option<u64>, Error> {
        let out = self.capture(&[
            "pr", "create", "--title", title, "--label", label, "--head", head, "--body", body,
        ])?;
        pull_number(&out).map(Some).ok_or_else(|| {
            Error::msg(format!(
                "gh pr create printed no pull request number: {out}"
            ))
        })
    }

    fn auto_merge(&self, number: u64) -> Outcome {
        self.run(
            &Step::new("gh", ["pr", "merge"])
                .arg(number.to_string())
                .args(["--auto", "--squash", "--delete-branch"]),
        )
        .map_err(|_| {
            Error::msg(format!(
                "auto-merge could not be enabled on #{number}; is auto-merge still on for the\n  \
                 repository? The pull request is open and merges by hand."
            ))
        })
    }

    fn release_state(&self, tag: &str) -> Result<ReleaseState, Error> {
        let mut command = Command::new("gh");
        command
            .args(["release", "view", tag, "--json", "isDraft,isImmutable"])
            .current_dir(self.root);
        if let Some(repo) = self.repo {
            command.env("GH_REPO", repo);
        }
        let out = command
            .output()
            .map_err(|e| Error::msg(format!("gh release view: {e}")))?;
        release_view(
            tag,
            out.status.success(),
            &String::from_utf8_lossy(&out.stdout),
            &String::from_utf8_lossy(&out.stderr),
        )
    }

    fn create_release(&self, tag: &str, notes: &str) -> Outcome {
        let scratch = Scratch::new("spate-release-notes")?;
        let file = scratch.join("notes.md");
        std::fs::write(&file, notes).map_err(|e| Error::msg(format!("{}: {e}", file.display())))?;
        self.run(&self.create_step(tag, &file))
    }

    fn publish_release(&self, tag: &str) -> Outcome {
        self.run(&self.publish_step(tag))
    }

    fn verify_release(&self, tag: &str) -> Outcome {
        self.run(&self.step(&["release", "verify", tag]))
    }

    fn upload(&self, tag: &str, files: &[PathBuf], clobber: bool) -> Outcome {
        let mut step = self.step(&["release", "upload", tag]);
        if clobber {
            step = step.arg("--clobber");
        }
        self.run(&step.args(files.iter().map(|f| f.to_string_lossy())))
    }

    fn assets(&self, tag: &str) -> Result<Vec<String>, Error> {
        uploaded_assets(&self.capture(&["release", "view", tag, "--json", "assets"])?)
    }

    fn dispatch_docs(&self) -> Outcome {
        // `workflow_dispatch` is the documented exception to the rule that
        // events raised by GITHUB_TOKEN trigger nothing.
        let token = required_env("DISPATCH_TOKEN", "the docs deploy")?;
        self.run(
            &Step::new(
                "gh",
                ["workflow", "run", "scheduled.yml", "--field", "tier=docs"],
            )
            .env("GH_TOKEN", token),
        )
    }

    fn verify_attestation(
        &self,
        file: &Path,
        bundle: Option<&Path>,
        signer_workflow: &str,
        predicate_type: &str,
        commit: &str,
    ) -> Outcome {
        run::quiet(
            self.root,
            false,
            &attestation_step(file, bundle, signer_workflow, predicate_type, commit),
        )
    }
}

/// `gh attestation verify` pinned to `signer_workflow` on `main`, to `commit`,
/// and to hosted runners, with the repository read from the workflow path.
fn attestation_step<'a>(
    file: &Path,
    bundle: Option<&Path>,
    signer_workflow: &str,
    predicate_type: &str,
    commit: &str,
) -> Step<'a> {
    let repo = signer_workflow
        .splitn(3, '/')
        .take(2)
        .collect::<Vec<_>>()
        .join("/");
    Step::new("gh", ["attestation", "verify"])
        .arg(file.to_string_lossy())
        .args(["--repo", &repo, "--signer-workflow"])
        .arg(format!("{signer_workflow}@refs/heads/main"))
        .args(["--source-ref", "refs/heads/main", "--source-digest", commit])
        .arg("--deny-self-hosted-runners")
        .args(["--predicate-type", predicate_type])
        .args(
            bundle
                .map(|b| vec!["--bundle".to_owned(), b.to_string_lossy().into_owned()])
                .unwrap_or_default(),
        )
}

/// The state a `gh release view --json isDraft,isImmutable` run reports: gh's
/// `release not found` reads as `Missing`, and any other failure is an error.
pub(crate) fn release_view(
    tag: &str,
    ok: bool,
    stdout: &str,
    stderr: &str,
) -> Result<ReleaseState, Error> {
    if ok {
        return release_state(stdout);
    }
    if stderr.contains("release not found") {
        return Ok(ReleaseState::Missing);
    }
    Err(Error::msg(format!(
        "gh release view {tag} failed: {}",
        stderr.trim()
    )))
}

/// The names of the assets in `gh release view --json assets` whose upload
/// completed. An upload broken partway leaves an asset by that name in another
/// state, which publishing would lock in.
pub(crate) fn uploaded_assets(raw: &str) -> Result<Vec<String>, Error> {
    #[derive(Deserialize)]
    struct View {
        assets: Vec<Asset>,
    }
    #[derive(Deserialize)]
    struct Asset {
        name: String,
        state: String,
    }
    let view: View = serde_json::from_str(raw)
        .map_err(|e| Error::msg(format!("gh release view --json assets: {e}")))?;
    Ok(view
        .assets
        .into_iter()
        .filter(|a| a.state == "uploaded")
        .map(|a| a.name)
        .collect())
}

/// The state `gh release view --json isDraft,isImmutable` reports.
pub(crate) fn release_state(raw: &str) -> Result<ReleaseState, Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct View {
        is_draft: bool,
        is_immutable: bool,
    }
    let view: View =
        serde_json::from_str(raw).map_err(|e| Error::msg(format!("gh release view: {e}")))?;
    Ok(if view.is_draft {
        ReleaseState::Draft
    } else {
        ReleaseState::Published {
            immutable: view.is_immutable,
        }
    })
}

/// The rows of `gh pr list --json number,headRefName,isCrossRepository`.
pub(crate) fn parse_pulls(raw: &str) -> Result<Vec<Pull>, Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Row {
        number: u64,
        head_ref_name: String,
        is_cross_repository: bool,
    }
    let rows: Vec<Row> =
        serde_json::from_str(raw).map_err(|e| Error::msg(format!("gh pr list: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|r| Pull {
            number: r.number,
            head: r.head_ref_name,
            cross_repository: r.is_cross_repository,
        })
        .collect())
}

/// The pull request in a `gh pr list` listing from `head` in this repository.
/// A fork's branch of the same name is never it.
pub(crate) fn head_pull(raw: &str, head: &str) -> Result<Option<u64>, Error> {
    Ok(parse_pulls(raw)?
        .into_iter()
        .find(|p| !p.cross_repository && p.head == head)
        .map(|p| p.number))
}

/// The trailing number of the URL `gh pr create` prints.
pub(crate) fn pull_number(out: &str) -> Option<u64> {
    let last = out.trim_end().lines().last()?;
    let digits: String = last
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    digits.parse().ok()
}

/// crates.io, through `curl`.
pub(crate) struct CratesIo<'a> {
    pub(crate) root: &'a Path,
}

impl Registry for CratesIo<'_> {
    fn index(&self, krate: &str) -> Result<Index, Error> {
        let (code, body) = fetch_index(self.root, krate)?;
        index_reply(code, &body)
    }

    fn trustpub_sha(&self, krate: &str, version: Version) -> Result<String, Error> {
        let body = run::capture(
            self.root,
            &Step::new("curl", ["-fsS", "--retry", "3", "--max-time", "30", "-H"])
                .arg(format!("User-Agent: {UA}"))
                .arg(format!("{API}/{krate}/{version}")),
        )?;
        let doc: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| Error::msg(format!("crates.io {krate} {version}: {e}")))?;
        Ok(doc["version"]["trustpub_data"]["sha"]
            .as_str()
            .unwrap_or("null")
            .to_owned())
    }

    fn download(&self, krate: &str, version: Version, dir: &Path) -> Result<PathBuf, Error> {
        let file = dir.join(format!("{krate}-{version}.crate"));
        run::run(
            self.root,
            false,
            &Step::new("curl", ["-fsSL", "--retry", "3", "--max-time", "60", "-H"])
                .arg(format!("User-Agent: {UA}"))
                .arg("-o")
                .arg(file.to_string_lossy())
                .arg(format!(
                    "https://static.crates.io/crates/{krate}/{krate}-{version}.crate"
                )),
        )?;
        Ok(file)
    }
}

/// The index answer a status code and body make. A 200 whose body does not
/// parse is an error.
pub(crate) fn index_reply(code: String, body: &str) -> Result<Index, Error> {
    Ok(match code.as_str() {
        "200" => Index::Found(index_entries(body)?),
        "404" => Index::Missing,
        _ => Index::Status(code),
    })
}

/// The entries of a sparse-index file, one JSON object per line.
pub(crate) fn index_entries(body: &str) -> Result<Vec<IndexEntry>, Error> {
    #[derive(Deserialize)]
    struct Line {
        vers: String,
        #[serde(default)]
        cksum: String,
    }
    serde_json::Deserializer::from_str(body)
        .into_iter::<Line>()
        .map(|l| {
            l.map(|l| IndexEntry {
                vers: l.vers,
                cksum: l.cksum,
            })
            .map_err(|e| Error::msg(format!("sparse index: {e}")))
        })
        .collect()
}

/// The checkout at `root`, through cargo and the in-process generators.
pub(crate) struct LocalWorkspace<'a> {
    pub(crate) root: &'a Path,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<MetadataPackage>,
}

#[derive(Deserialize)]
struct MetadataPackage {
    name: String,
    publish: Option<Vec<String>>,
    manifest_path: PathBuf,
}

impl LocalWorkspace<'_> {
    fn metadata(&self) -> Result<String, Error> {
        run::capture(
            self.root,
            &Step::new(
                "cargo",
                ["metadata", "--no-deps", "--format-version", "1", "--locked"],
            ),
        )
    }

    fn members(&self) -> Result<Vec<(Package, bool)>, Error> {
        let parsed: Metadata = serde_json::from_str(&self.metadata()?)
            .map_err(|e| Error::msg(format!("cargo metadata: {e}")))?;
        Ok(parsed
            .packages
            .into_iter()
            .map(|p| {
                let publishable = p.publish.as_ref().is_none_or(|allow| !allow.is_empty());
                let dir = p
                    .manifest_path
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_default();
                (Package { name: p.name, dir }, publishable)
            })
            .collect())
    }

    /// The packaging step, with a verify build when `verify`, excluding the
    /// unpublished members as well as `excludes`. `cargo package --workspace`
    /// packages a `publish = false` member unless it is excluded.
    fn package_step(&self, excludes: &[String], verify: bool) -> Result<Step<'static>, Error> {
        let unpublished = self
            .members()?
            .into_iter()
            .filter_map(|(p, publishable)| (!publishable).then_some(p.name));
        let excludes: Vec<String> = excludes.iter().cloned().chain(unpublished).collect();
        let step = Step::new("cargo", ["package", "--workspace", "--locked"]);
        let step = if verify {
            step
        } else {
            step.arg("--no-verify")
        };
        Ok(step.args(exclude_args(&excludes)))
    }
}

impl Workspace for LocalWorkspace<'_> {
    fn publishable(&self) -> Result<Vec<Package>, Error> {
        Ok(self
            .members()?
            .into_iter()
            .filter_map(|(p, publishable)| publishable.then_some(p))
            .collect())
    }

    fn missing_metadata(&self) -> Result<Vec<String>, Error> {
        super::version::missing_metadata(&self.metadata()?)
    }

    fn version(&self) -> Result<Version, Error> {
        let manifest = std::fs::read_to_string(self.root.join("Cargo.toml"))
            .map_err(|e| Error::msg(format!("Cargo.toml: {e}")))?;
        super::version::workspace_version(&manifest)
    }

    fn derive(&self) -> Result<Version, Error> {
        let (next, reason) = super::version::derive(self.root)?;
        eprintln!("release version: {reason}");
        Ok(next)
    }

    fn generate(&self, version: Version) -> Outcome {
        let v = version.to_string();
        super::version::bump(self.root, false, &v)?;
        crate::checks::changelog::build(self.root, false, &v)?;
        crate::checks::attribution::generate(self.root, false)
    }

    fn notes(&self, version: Version) -> Result<String, Error> {
        crate::checks::changelog::notes_text(self.root, &version.to_string())
    }

    fn package(&self, excludes: &[String]) -> Outcome {
        run::run(self.root, false, &self.package_step(excludes, true)?)
    }

    fn package_unverified(&self, excludes: &[String]) -> Outcome {
        run::run(self.root, false, &self.package_step(excludes, false)?)
    }

    fn publish(&self, excludes: &[String]) -> Outcome {
        run::run(
            self.root,
            false,
            &Step::new(
                "cargo",
                ["publish", "--workspace", "--locked", "--no-verify"],
            )
            .args(exclude_args(excludes)),
        )
    }

    fn crate_file(&self, krate: &str, version: Version) -> Option<PathBuf> {
        let file = self
            .root
            .join(format!("target/package/{krate}-{version}.crate"));
        file.is_file().then_some(file)
    }

    fn sha256(&self, path: &Path) -> Result<String, Error> {
        let file = path.to_string_lossy();
        // Ubuntu runners carry sha256sum; macOS carries shasum.
        let step = if run::on_path("sha256sum") {
            Step::new("sha256sum", [file.as_ref()])
        } else {
            Step::new("shasum", ["-a", "256", file.as_ref()])
        };
        let out = run::capture(self.root, &step)?;
        out.split_whitespace()
            .next()
            .map(str::to_owned)
            .ok_or_else(|| Error::msg(format!("no digest for {file}")))
    }

    fn resolve(&self, version: Version) -> Result<Resolution, Error> {
        let scratch = Scratch::new("spate-release-smoke")?;
        let write = |rel: &str, text: &str| {
            let path = scratch.join(rel);
            std::fs::create_dir_all(path.parent().unwrap_or(scratch.dir()))
                .and_then(|()| std::fs::write(&path, text))
                .map_err(|e| Error::msg(format!("{}: {e}", path.display())))
        };
        write("Cargo.toml", &smoke_manifest(version))?;
        write("src/main.rs", "fn main() {}\n")?;
        let out = Command::new("cargo")
            .arg("generate-lockfile")
            .current_dir(scratch.dir())
            .output()
            .map_err(|e| Error::msg(format!("cargo generate-lockfile: {e}")))?;
        if !out.status.success() {
            let log = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            let tail: Vec<&str> = log.lines().rev().take(20).collect();
            return Ok(Resolution::Failed(
                tail.into_iter().rev().collect::<Vec<_>>().join("\n"),
            ));
        }
        let lock = std::fs::read_to_string(scratch.join("Cargo.lock"))
            .map_err(|e| Error::msg(format!("Cargo.lock: {e}")))?;
        Ok(Resolution::Resolved(
            lock.lines().filter(|l| l.starts_with("name = ")).count(),
        ))
    }

    fn sboms(&self, version: Version, out: &Path) -> Outcome {
        // SOURCE_DATE_EPOCH makes the output a property of the release commit
        // rather than of the wall clock.
        let epoch = run::capture(self.root, &Step::new("git", ["log", "-1", "--format=%ct"]))?;
        run::run(
            self.root,
            false,
            &Step::new(
                "cargo",
                [
                    "cyclonedx",
                    "-f",
                    "json",
                    "--describe",
                    "crate",
                    "--all-features",
                    "--target",
                    "all",
                    "--spec-version",
                    "1.5",
                    "-q",
                ],
            )
            .env("SOURCE_DATE_EPOCH", epoch.trim()),
        )?;
        // The tool writes each SBOM next to its manifest, unpublished members
        // included; only the publishable ones are kept.
        let mut collected = 0;
        for (package, publishable) in self.members()? {
            let written = package.dir.join(format!("{}.cdx.json", package.name));
            if !publishable {
                drop(std::fs::remove_file(&written));
                continue;
            }
            if !written.is_file() {
                return Err(Error::msg(format!(
                    "cargo cyclonedx wrote no SBOM for {}",
                    package.name
                )));
            }
            let target = out.join(format!("{}-{version}.cdx.json", package.name));
            std::fs::copy(&written, &target)
                .and_then(|_| std::fs::remove_file(&written))
                .map_err(|e| Error::msg(format!("{}: {e}", written.display())))?;
            collected += 1;
        }
        println!("collected {collected} SBOMs into {}", out.display());
        Ok(())
    }
}

/// `--exclude NAME` for each name.
fn exclude_args(excludes: &[String]) -> Vec<String> {
    excludes
        .iter()
        .flat_map(|e| ["--exclude".to_owned(), e.clone()])
        .collect()
}

/// A consumer depending on the facade, with every connector feature, and on
/// the test crate, at exactly `version`.
fn smoke_manifest(version: Version) -> String {
    format!(
        "[package]\n\
         name = \"spate-smoke\"\n\
         version = \"0.0.0\"\n\
         edition = \"2021\"\n\
         \n\
         [dependencies]\n\
         spate = {{ version = \"={version}\", features = [\"kafka\", \"clickhouse\", \"avro\", \"s3\", \"json\", \"coordination-nats\", \"coordination-dynamodb\"] }}\n\
         \n\
         [dev-dependencies]\n\
         spate-test = \"={version}\"\n"
    )
}

// ---------------------------------------------------------------------------
// The dry run's substitutes: reads pass through, writes are printed instead.
// ---------------------------------------------------------------------------

fn would(what: &str) {
    println!("would {what}");
}

/// Git whose pushes are printed instead of sent.
pub(crate) struct DryGit<'a>(pub(crate) &'a dyn Git);

impl Git for DryGit<'_> {
    fn is_clean(&self) -> Result<bool, Error> {
        self.0.is_clean()
    }
    fn last_tag(&self) -> Result<String, Error> {
        self.0.last_tag()
    }
    fn subject(&self) -> Result<String, Error> {
        self.0.subject()
    }
    fn short_head(&self) -> Result<String, Error> {
        self.0.short_head()
    }
    fn commit_all(&self, subject: &str, body: &str) -> Outcome {
        self.0.commit_all(subject, body)
    }
    fn remote_tag(&self, tag: &str) -> Result<Option<String>, Error> {
        self.0.remote_tag(tag)
    }
    fn tag(&self, name: &str, commit: &str) -> Outcome {
        would(&format!("tag {name} at {commit}"));
        Ok(())
    }
    fn push(&self, refspec: &str, force: bool) -> Outcome {
        would(&format!(
            "push {refspec}{}",
            if force { " (forced)" } else { "" }
        ));
        Ok(())
    }
}

/// A forge whose writes are printed instead of made.
pub(crate) struct DryForge<'a>(pub(crate) &'a dyn Forge);

impl Forge for DryForge<'_> {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error> {
        // A listing the forge refuses, as on a clone with no default
        // repository, leaves nothing to sweep rather than failing the rehearsal.
        self.0.open_pulls().or_else(|e| {
            would(&format!("list the open pull requests ({})", e.message));
            Ok(Vec::new())
        })
    }
    fn pull_for_head(&self, head: &str) -> Result<Option<u64>, Error> {
        self.0.pull_for_head(head).or_else(|e| {
            would(&format!(
                "look up the pull request from {head} ({})",
                e.message
            ));
            Ok(None)
        })
    }
    fn close_pull(&self, number: u64, comment: &str) -> Outcome {
        would(&format!("close #{number} with \"{comment}\""));
        Ok(())
    }
    fn create_pull(
        &self,
        title: &str,
        head: &str,
        label: &str,
        _body: &str,
    ) -> Result<Option<u64>, Error> {
        would(&format!(
            "open a pull request \"{title}\" from {head}, labeled {label}, set to auto-merge"
        ));
        Ok(None)
    }
    fn auto_merge(&self, number: u64) -> Outcome {
        would(&format!("enable auto-merge on #{number}"));
        Ok(())
    }
    fn release_state(&self, tag: &str) -> Result<ReleaseState, Error> {
        self.0.release_state(tag)
    }
    fn create_release(&self, tag: &str, _notes: &str) -> Outcome {
        would(&format!("create the {tag} release as a draft"));
        Ok(())
    }
    fn publish_release(&self, tag: &str) -> Outcome {
        would(&format!("publish the {tag} release"));
        Ok(())
    }
    fn verify_release(&self, tag: &str) -> Outcome {
        self.0.verify_release(tag)
    }
    fn upload(&self, tag: &str, files: &[PathBuf], _clobber: bool) -> Outcome {
        for f in files {
            would(&format!("upload {} to {tag}", f.display()));
        }
        Ok(())
    }
    fn assets(&self, tag: &str) -> Result<Vec<String>, Error> {
        self.0.assets(tag)
    }
    fn dispatch_docs(&self) -> Outcome {
        would("dispatch the docs deploy");
        Ok(())
    }
    fn verify_attestation(
        &self,
        file: &Path,
        bundle: Option<&Path>,
        signer_workflow: &str,
        predicate_type: &str,
        commit: &str,
    ) -> Outcome {
        self.0
            .verify_attestation(file, bundle, signer_workflow, predicate_type, commit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verification is pinned to the signer's main ref, the release commit
    /// and hosted runners.
    #[test]
    fn attestation_verify_pins_signer_ref_commit_and_runner() {
        let step = attestation_step(
            Path::new("a.crate"),
            Some(Path::new("b.jsonl")),
            "spate-etl/spate/.github/workflows/release-build.yml",
            "https://slsa.dev/provenance/v1",
            "abc123",
        );
        let a = &step.args;
        let pair = |k: &str| a.iter().position(|x| x == k).map(|i| a[i + 1].as_str());
        assert_eq!(pair("--repo"), Some("spate-etl/spate"));
        assert_eq!(
            pair("--signer-workflow"),
            Some("spate-etl/spate/.github/workflows/release-build.yml@refs/heads/main")
        );
        assert_eq!(pair("--source-ref"), Some("refs/heads/main"));
        assert_eq!(pair("--source-digest"), Some("abc123"));
        assert!(a.iter().any(|x| x == "--deny-self-hosted-runners"));
        assert_eq!(
            pair("--predicate-type"),
            Some("https://slsa.dev/provenance/v1")
        );
        assert_eq!(pair("--bundle"), Some("b.jsonl"));
    }

    /// The release is created as a draft and published by clearing the draft
    /// flag.
    #[test]
    fn the_release_is_created_as_a_draft_and_published_from_it() {
        let gh = Gh {
            root: Path::new("."),
            repo: None,
        };
        let create = gh.create_step("v0.3.0", Path::new("notes.md")).args;
        assert!(create.starts_with(&["release", "create", "v0.3.0"].map(String::from)));
        assert!(create.iter().any(|a| a == "--draft"), "{create:?}");
        assert!(create.iter().any(|a| a == "--verify-tag"), "{create:?}");
        assert_eq!(
            gh.publish_step("v0.3.0").args,
            ["release", "edit", "v0.3.0", "--draft=false"]
        );
    }

    /// Only gh's `release not found` reads as a missing release; any other
    /// failure stays an error.
    #[test]
    fn only_release_not_found_reads_as_missing() {
        assert_eq!(
            release_view("v0.3.0", false, "", "release not found\n").unwrap(),
            ReleaseState::Missing
        );
        let err = release_view("v0.3.0", false, "", "HTTP 502: Bad Gateway\n").unwrap_err();
        assert_eq!(
            err.message,
            "gh release view v0.3.0 failed: HTTP 502: Bad Gateway"
        );
        assert_eq!(
            release_view(
                "v0.3.0",
                true,
                r#"{"isDraft":true,"isImmutable":false}"#,
                ""
            )
            .unwrap(),
            ReleaseState::Draft
        );
    }

    /// An asset whose upload did not complete is not listed.
    #[test]
    fn only_uploaded_assets_are_listed() {
        let raw = r#"{"assets":[
            {"name":"spate-0.3.0.cdx.json","state":"uploaded","size":10},
            {"name":"SHA256SUMS","state":"starter","size":0},
            {"name":"spate-v0.3.0.intoto.jsonl","state":"open","size":0}
        ]}"#;
        assert_eq!(uploaded_assets(raw).unwrap(), ["spate-0.3.0.cdx.json"]);
        assert!(uploaded_assets(r#"{"assets":[]}"#).unwrap().is_empty());
        assert!(uploaded_assets("not json").is_err());
    }

    /// Without a bundle the attestation is read from GitHub's attestation store.
    #[test]
    fn attestation_verify_without_a_bundle_reads_the_store() {
        let step = attestation_step(
            Path::new("a.crate"),
            None,
            "spate-etl/spate/.github/workflows/release-build.yml",
            "https://slsa.dev/provenance/v1",
            "abc123",
        );
        assert!(
            !step.args.iter().any(|x| x == "--bundle"),
            "{:?}",
            step.args
        );
        assert!(step.args.iter().any(|x| x == "--deny-self-hosted-runners"));
    }

    /// The commit an `ls-remote` listing names: the peeled line of an annotated
    /// tag, the plain line of a lightweight one, none for an absent tag.
    #[test]
    fn the_remote_tag_is_read_through_an_annotated_tag() {
        let annotated = "aaaa\trefs/tags/v0.3.0\nbbbb\trefs/tags/v0.3.0^{}\n";
        assert_eq!(peeled(annotated).as_deref(), Some("bbbb"));
        assert_eq!(peeled("cccc\trefs/tags/v0.3.0\n").as_deref(), Some("cccc"));
        assert_eq!(peeled(""), None);
    }

    /// The number is the trailing digits of the URL `gh pr create` prints last.
    #[test]
    fn the_pull_number_is_the_urls_tail() {
        assert_eq!(
            pull_number("https://github.com/spate-etl/spate/pull/1058\n"),
            Some(1058)
        );
        assert_eq!(
            pull_number("Creating pull request\nhttps://x/pull/7"),
            Some(7)
        );
        assert_eq!(pull_number("no number here\n"), None);
        assert_eq!(pull_number(""), None);
    }

    /// The sparse index's entries keep their version and cksum and ignore the
    /// fields this step does not read; a line that is not an entry is an error.
    #[test]
    fn index_entries_read_the_version_and_cksum() {
        let body = concat!(
            r#"{"name":"spate","vers":"0.1.0","deps":[{"name":"spate-core","req":"=0.1.0","kind":"normal"}],"cksum":"aa","features":{},"yanked":false,"rust_version":"1.94","v":2}"#,
            "\n",
            r#"{"name":"spate","vers":"0.2.0","deps":[],"cksum":"bb","features":{},"yanked":true}"#,
            "\n",
        );
        assert_eq!(
            index_entries(body).unwrap(),
            [
                IndexEntry {
                    vers: "0.1.0".into(),
                    cksum: "aa".into()
                },
                IndexEntry {
                    vers: "0.2.0".into(),
                    cksum: "bb".into()
                },
            ]
        );
        assert!(index_entries("").unwrap().is_empty());
        assert!(index_entries("<html>").is_err());
        assert!(index_entries(r#"{"name":"spate"}"#).is_err());
    }

    /// The pull request listing keeps the fork flag that the sweep relies on.
    #[test]
    fn the_pull_listing_keeps_the_fork_flag() {
        let raw = r#"[{"number":3,"headRefName":"release/v0.3.0","isCrossRepository":true}]"#;
        assert_eq!(
            parse_pulls(raw).unwrap(),
            [Pull {
                number: 3,
                head: "release/v0.3.0".into(),
                cross_repository: true
            }]
        );
    }

    /// A 200 is read as entries and must parse; 404 is a missing crate; any
    /// other code is kept.
    #[test]
    fn the_index_reply_follows_the_status() {
        assert_eq!(
            index_reply("200".into(), r#"{"vers":"0.2.0","cksum":"bb"}"#).unwrap(),
            Index::Found(vec![IndexEntry {
                vers: "0.2.0".into(),
                cksum: "bb".into()
            }])
        );
        assert!(index_reply("200".into(), "err\n").is_err());
        assert_eq!(index_reply("404".into(), "").unwrap(), Index::Missing);
        assert_eq!(
            index_reply("403".into(), "").unwrap(),
            Index::Status("403".into())
        );
    }

    /// Only this repository's pull request from the branch is reused, never a
    /// fork's branch of the same name.
    #[test]
    fn a_forks_branch_is_never_the_release_pull_request() {
        let raw = r#"[
            {"number":3,"headRefName":"release/v0.3.0","isCrossRepository":true},
            {"number":4,"headRefName":"release/v0.3.0-rc","isCrossRepository":false}
        ]"#;
        assert_eq!(head_pull(raw, "release/v0.3.0").unwrap(), None);
        let raw = r#"[
            {"number":3,"headRefName":"release/v0.3.0","isCrossRepository":true},
            {"number":5,"headRefName":"release/v0.3.0","isCrossRepository":false}
        ]"#;
        assert_eq!(head_pull(raw, "release/v0.3.0").unwrap(), Some(5));
    }

    /// A remote that cannot be reached makes the tag lookup fail.
    #[cfg(unix)]
    #[test]
    fn an_unreachable_origin_is_not_an_absent_tag() {
        let scratch = Scratch::new("spate-xtask-remote-tag").unwrap();
        let root = scratch.dir();
        for args in [
            &["init", "-q"][..],
            &["remote", "add", "origin", "/nonexistent/spate.git"],
        ] {
            assert!(
                std::process::Command::new("git")
                    .args(args)
                    .current_dir(root)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let git = ProcessGit {
            root,
            remote: "origin".into(),
        };
        assert!(git.remote_tag("v0.3.0").is_err());
    }

    /// Each excluded crate becomes one `--exclude` pair.
    #[test]
    fn excludes_become_flag_pairs() {
        assert!(exclude_args(&[]).is_empty());
        assert_eq!(
            exclude_args(&["a".into(), "b".into()]),
            ["--exclude", "a", "--exclude", "b"]
        );
    }

    /// Packaging, verified or not, leaves out every unpublished member,
    /// `spate-faults` among them, and keeps the caller's excludes. Regression
    /// for #1062.
    #[test]
    fn packaging_leaves_out_the_unpublished_members() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let workspace = LocalWorkspace { root };
        let unpublished: Vec<String> = workspace
            .members()
            .unwrap()
            .into_iter()
            .filter_map(|(p, publishable)| (!publishable).then_some(p.name))
            .collect();
        assert!(unpublished.iter().any(|n| n == "spate-faults"));
        let mut expected = vec!["spate-core"];
        expected.extend(unpublished.iter().map(String::as_str));
        for verify in [true, false] {
            let step = workspace
                .package_step(&["spate-core".into()], verify)
                .unwrap();
            let excluded: Vec<&str> = step
                .args
                .windows(2)
                .filter(|w| w[0] == "--exclude")
                .map(|w| w[1].as_str())
                .collect();
            assert_eq!(excluded, expected, "verify: {verify}");
            assert_eq!(step.args.iter().any(|a| a == "--no-verify"), !verify);
        }
    }

    /// The smoke consumer pins the facade and the test crate exactly.
    #[test]
    fn the_smoke_consumer_pins_the_release_exactly() {
        let manifest: toml::Table =
            toml::from_str(&smoke_manifest(Version::parse("0.3.0").unwrap())).unwrap();
        assert_eq!(
            manifest["dependencies"]["spate"]["version"].as_str(),
            Some("=0.3.0")
        );
        assert_eq!(
            manifest["dev-dependencies"]["spate-test"].as_str(),
            Some("=0.3.0")
        );
    }
}
