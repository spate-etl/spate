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
use crate::checks::subject::printable;
use crate::checks::{attribution, changelog, semver_checks};
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
    /// Every `v*` tag on origin with the commit it names, peeled through an
    /// annotated tag.
    fn remote_tags(&self) -> Result<Vec<(String, String)>, Error>;
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

/// An open issue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Issue {
    pub(crate) number: u64,
    pub(crate) title: String,
    pub(crate) url: String,
}

impl Issue {
    /// The issue as a log line naming `milestone`, with the title and url
    /// escaped so neither can issue an Actions workflow command.
    pub(crate) fn describe(&self, milestone: &str) -> String {
        format!(
            "#{} on {milestone}: {} ({})",
            self.number,
            printable(&self.title),
            printable(&self.url)
        )
    }
}

/// A milestone as the forge lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Milestone {
    pub(crate) number: u64,
    pub(crate) title: String,
    pub(crate) open: bool,
}

/// A milestone a write names: by title in what it prints, and by number to the
/// forge. A milestone a dry run would create has no number yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) title: String,
    pub(crate) number: Option<u64>,
}

impl From<&Milestone> for Target {
    fn from(m: &Milestone) -> Self {
        Self {
            title: m.title.clone(),
            number: Some(m.number),
        }
    }
}

/// An issue with what the milestone step reads of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tracked {
    pub(crate) issue: Issue,
    /// `completed`, `not_planned` or `duplicate` once closed.
    pub(crate) state_reason: Option<String>,
    pub(crate) closed_at: Option<String>,
    /// The number of the issue's milestone.
    pub(crate) milestone: Option<u64>,
    pub(crate) labels: Vec<String>,
}

/// A `milestoned` or `demilestoned` event on an issue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MilestoneEvent {
    pub(crate) added: bool,
    /// The milestone's title when the event happened; a later rename does not
    /// change it.
    pub(crate) title: String,
    pub(crate) at: String,
}

pub(crate) trait Forge {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error>;
    /// The open issues labeled `security` on the milestone titled `milestone`,
    /// open or closed. Empty when no milestone has that title.
    fn open_security_issues(&self, milestone: &str) -> Result<Vec<Issue>, Error>;
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
    /// Verifies that an attestation of `predicate_type`, read from `bundle` or
    /// from the attestation store when it is `None`, covers `file` and was
    /// signed by `signer_workflow` running from `main` at `commit`.
    fn verify_attestation(
        &self,
        file: &Path,
        bundle: Option<&Path>,
        signer_workflow: &str,
        predicate_type: &str,
        commit: &str,
    ) -> Outcome;
    /// Every milestone, open or closed.
    fn milestones(&self) -> Result<Vec<Milestone>, Error>;
    /// The closed issues updated after `since`, or every closed issue.
    fn closed_issues(&self, since: Option<&str>) -> Result<Vec<Tracked>, Error>;
    /// The open issues on the milestone numbered `milestone`.
    fn open_issues(&self, milestone: u64) -> Result<Vec<Tracked>, Error>;
    /// The issue's milestone events, oldest first.
    fn milestone_events(&self, issue: u64) -> Result<Vec<MilestoneEvent>, Error>;
    /// When the commit `sha` reached the default branch: the merge time of the
    /// pull request it was merged as, or its committer date.
    fn landed_at(&self, sha: &str) -> Result<String, Error>;
    /// Creates an open milestone, answering its number, or `None` when none
    /// was created.
    fn create_milestone(&self, title: &str) -> Result<Option<u64>, Error>;
    fn set_milestone_open(&self, milestone: &Target, open: bool) -> Outcome;
    /// Sets the issue's milestone, or clears it with `None`.
    fn set_milestone(&self, issue: u64, milestone: Option<&Target>) -> Outcome;
    fn add_label(&self, issue: u64, label: &str) -> Outcome;
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
    /// The blog's source directory, where a release post lives.
    fn blog(&self) -> PathBuf;
    /// The GitHub release body for `version`.
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
        semver_checks::last_tag(self.root)
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

    fn remote_tags(&self) -> Result<Vec<(String, String)>, Error> {
        Ok(peeled_tags(&self.capture(&[
            "ls-remote",
            "--tags",
            &self.remote,
            "refs/tags/v*",
        ])?))
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

/// Each tag in an `ls-remote --tags` listing with the commit [`peeled`] reads
/// from its lines, in the listing's order.
pub(crate) fn peeled_tags(listing: &str) -> Vec<(String, String)> {
    let mut tags: Vec<(String, String)> = Vec::new();
    for line in listing.lines() {
        let Some((_, reference)) = line.split_once('\t') else {
            continue;
        };
        let Some(name) = reference.strip_prefix("refs/tags/") else {
            continue;
        };
        let name = name.strip_suffix("^{}").unwrap_or(name);
        match tags.iter_mut().find(|(n, _)| n == name) {
            Some((_, lines)) => {
                lines.push_str(line);
                lines.push('\n');
            }
            None => tags.push((name.to_owned(), format!("{line}\n"))),
        }
    }
    tags.into_iter()
        .filter_map(|(name, lines)| peeled(&lines).map(|sha| (name, sha)))
        .collect()
}

fn required_env(key: &str, what: &str) -> Result<String, Error> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::msg(format!("{what} needs {key}")))
}

/// The label on a security defect that a release must not ship with.
pub(crate) const SECURITY_LABEL: &str = "security";

/// The label on an issue its milestone's release cannot ship without.
pub(crate) const RELEASE_BLOCKER: &str = "release blocker";

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

    /// A REST path under the repository, every page, as one JSON array.
    fn api(&self, path: &str) -> Result<String, Error> {
        self.capture(&["api", "--paginate", "--slurp", path])
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

    fn open_security_issues(&self, milestone: &str) -> Result<Vec<Issue>, Error> {
        security_issues(|path| self.api(path), milestone)
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
                .args(["--auto", "--squash"]),
        )
        .map_err(|_| {
            Error::msg(format!(
                "auto-merge could not be enabled on #{number}; is auto-merge still on for the\n  \
                 repository? The pull request is open and merges by hand."
            ))
        })
    }

    fn release_state(&self, tag: &str) -> Result<ReleaseState, Error> {
        // Run by hand: gh's stderr decides whether the release is missing.
        let step = self.step(&["release", "view", tag, "--json", "isDraft,isImmutable"]);
        let out = Command::new(step.program)
            .args(&step.args)
            .envs(step.env.iter().map(|(k, v)| (*k, v.as_str())))
            .current_dir(self.root)
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
        self.run(&docs_dispatch_step().env("GH_TOKEN", token))
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

    fn milestones(&self) -> Result<Vec<Milestone>, Error> {
        parse_milestones(&self.api(MILESTONES)?)
    }

    fn closed_issues(&self, since: Option<&str>) -> Result<Vec<Tracked>, Error> {
        parse_tracked(&self.api(&closed_issues_path(since))?)
    }

    fn open_issues(&self, milestone: u64) -> Result<Vec<Tracked>, Error> {
        parse_tracked(&self.api(&format!(
            "repos/{{owner}}/{{repo}}/issues?milestone={milestone}&state=open&per_page=100"
        ))?)
    }

    fn milestone_events(&self, issue: u64) -> Result<Vec<MilestoneEvent>, Error> {
        parse_milestone_events(&self.api(&format!(
            "repos/{{owner}}/{{repo}}/issues/{issue}/events?per_page=100"
        ))?)
    }

    fn landed_at(&self, sha: &str) -> Result<String, Error> {
        landed_at(|path| self.api(path), sha)
    }

    fn create_milestone(&self, title: &str) -> Result<Option<u64>, Error> {
        let out = run::capture(
            self.root,
            &self.step(&[]).args(create_milestone_args(title)),
        )?;
        created_number(&out).map(Some)
    }

    fn set_milestone_open(&self, milestone: &Target, open: bool) -> Outcome {
        run::quiet(
            self.root,
            false,
            &self
                .step(&[])
                .args(milestone_state_args(number_of(milestone)?, open)),
        )
    }

    fn set_milestone(&self, issue: u64, milestone: Option<&Target>) -> Outcome {
        let number = milestone.map(number_of).transpose()?;
        run::quiet(
            self.root,
            false,
            &self.step(&[]).args(issue_milestone_args(issue, number)),
        )
    }

    fn add_label(&self, issue: u64, label: &str) -> Outcome {
        run::quiet(
            self.root,
            false,
            &self.step(&[]).args(add_label_args(issue, label)),
        )
    }
}

const MILESTONES: &str = "repos/{owner}/{repo}/milestones?state=all&per_page=100";

/// The closed issues updated after `since`. Closing an issue updates it, so
/// every issue closed after `since` is among them.
fn closed_issues_path(since: Option<&str>) -> String {
    let since = since.map(|s| format!("&since={s}")).unwrap_or_default();
    format!("repos/{{owner}}/{{repo}}/issues?state=closed{since}&per_page=100")
}

/// The number a write needs, which a milestone a dry run would create lacks.
fn number_of(milestone: &Target) -> Result<u64, Error> {
    milestone
        .number
        .ok_or_else(|| Error::msg(format!("milestone {} has no number", milestone.title)))
}

fn create_milestone_args(title: &str) -> Vec<String> {
    vec![
        "api".into(),
        "-X".into(),
        "POST".into(),
        "repos/{owner}/{repo}/milestones".into(),
        "-f".into(),
        format!("title={title}"),
    ]
}

fn milestone_state_args(number: u64, open: bool) -> Vec<String> {
    let state = if open { "open" } else { "closed" };
    vec![
        "api".into(),
        "-X".into(),
        "PATCH".into(),
        format!("repos/{{owner}}/{{repo}}/milestones/{number}"),
        "-f".into(),
        format!("state={state}"),
    ]
}

/// `-F` sends `null` and a number as JSON, so `None` clears the milestone.
fn issue_milestone_args(issue: u64, number: Option<u64>) -> Vec<String> {
    let value = number.map_or_else(|| "null".to_owned(), |n| n.to_string());
    vec![
        "api".into(),
        "-X".into(),
        "PATCH".into(),
        format!("repos/{{owner}}/{{repo}}/issues/{issue}"),
        "-F".into(),
        format!("milestone={value}"),
    ]
}

fn add_label_args(issue: u64, label: &str) -> Vec<String> {
    vec![
        "api".into(),
        "-X".into(),
        "POST".into(),
        format!("repos/{{owner}}/{{repo}}/issues/{issue}/labels"),
        "-f".into(),
        format!("labels[]={label}"),
    ]
}

/// The number in the milestone `POST milestones` answers with.
pub(crate) fn created_number(out: &str) -> Result<u64, Error> {
    #[derive(Deserialize)]
    struct Created {
        number: u64,
    }
    serde_json::from_str::<Created>(out)
        .map(|c| c.number)
        .map_err(|e| Error::msg(format!("created milestone: {e}")))
}

/// When `sha` reached the default branch, with `get` answering a REST path as
/// `gh api --paginate --slurp` does: the `merged_at` of the pull request whose
/// squash produced it, or else the commit's committer date. The commit's own
/// date is when its pull request was squashed, which a merge queue makes
/// earlier than the merge.
pub(crate) fn landed_at(
    get: impl Fn(&str) -> Result<String, Error>,
    sha: &str,
) -> Result<String, Error> {
    #[derive(Deserialize)]
    struct Pull {
        merge_commit_sha: Option<String>,
        merged_at: Option<String>,
    }
    #[derive(Deserialize)]
    struct Commit {
        commit: Inner,
    }
    #[derive(Deserialize)]
    struct Inner {
        committer: Person,
    }
    #[derive(Deserialize)]
    struct Person {
        date: String,
    }
    let pulls: Vec<Vec<Pull>> = serde_json::from_str(&get(&format!(
        "repos/{{owner}}/{{repo}}/commits/{sha}/pulls"
    ))?)
    .map_err(|e| Error::msg(format!("pulls of {sha}: {e}")))?;
    let merged = pulls
        .into_iter()
        .flatten()
        .find(|p| p.merge_commit_sha.as_deref() == Some(sha))
        .and_then(|p| p.merged_at);
    if let Some(at) = merged {
        return Ok(at);
    }
    let commits: Vec<Commit> =
        serde_json::from_str(&get(&format!("repos/{{owner}}/{{repo}}/commits/{sha}"))?)
            .map_err(|e| Error::msg(format!("commit {sha}: {e}")))?;
    commits
        .into_iter()
        .next()
        .map(|c| c.commit.committer.date)
        .ok_or_else(|| Error::msg(format!("commit {sha}: no such commit")))
}

/// The `milestoned` and `demilestoned` events in a `gh api --paginate --slurp`
/// listing of an issue's events, in the listing's order.
pub(crate) fn parse_milestone_events(raw: &str) -> Result<Vec<MilestoneEvent>, Error> {
    #[derive(Deserialize)]
    struct Row {
        event: String,
        created_at: String,
        milestone: Option<Titled>,
    }
    #[derive(Deserialize)]
    struct Titled {
        title: String,
    }
    let pages: Vec<Vec<Row>> =
        serde_json::from_str(raw).map_err(|e| Error::msg(format!("events: {e}")))?;
    Ok(pages
        .into_iter()
        .flatten()
        .filter_map(|r| {
            let added = match r.event.as_str() {
                "milestoned" => true,
                "demilestoned" => false,
                _ => return None,
            };
            Some(MilestoneEvent {
                added,
                title: r.milestone.map(|m| m.title).unwrap_or_default(),
                at: r.created_at,
            })
        })
        .collect())
}

/// `gh workflow run` for the docs deploy. The workflow it names must declare
/// each `--field` as a `workflow_dispatch` input.
fn docs_dispatch_step() -> Step<'static> {
    Step::new(
        "gh",
        ["workflow", "run", "nightly.yml", "--field", "tier=docs"],
    )
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

/// The open `security` issues on the milestone titled `milestone`, with `get`
/// answering a REST path under the repository as `gh api --paginate --slurp`
/// does. The issues are not read when no milestone has that title.
pub(crate) fn security_issues(
    get: impl Fn(&str) -> Result<String, Error>,
    milestone: &str,
) -> Result<Vec<Issue>, Error> {
    let milestones = get(MILESTONES)?;
    let Some(number) = milestone_number(&milestones, milestone)? else {
        return Ok(Vec::new());
    };
    parse_issues(&get(&format!(
        "repos/{{owner}}/{{repo}}/issues?milestone={number}&labels={SECURITY_LABEL}&state=open&per_page=100"
    ))?)
}

/// The number of the milestone titled `title` in a `gh api --paginate --slurp`
/// listing of milestones, or `None` when none has that title.
pub(crate) fn milestone_number(raw: &str, title: &str) -> Result<Option<u64>, Error> {
    Ok(parse_milestones(raw)?
        .into_iter()
        .find(|m| m.title == title)
        .map(|m| m.number))
}

/// Every milestone in a `gh api --paginate --slurp` listing of milestones.
pub(crate) fn parse_milestones(raw: &str) -> Result<Vec<Milestone>, Error> {
    #[derive(Deserialize)]
    struct Row {
        number: u64,
        title: String,
        #[serde(default)]
        state: String,
    }
    let pages: Vec<Vec<Row>> =
        serde_json::from_str(raw).map_err(|e| Error::msg(format!("milestones: {e}")))?;
    Ok(pages
        .into_iter()
        .flatten()
        .map(|r| Milestone {
            number: r.number,
            open: r.state == "open",
            title: r.title,
        })
        .collect())
}

/// The issues in a `gh api --paginate --slurp` listing of a repository's
/// issues. The endpoint lists pull requests too, and those are dropped.
pub(crate) fn parse_issues(raw: &str) -> Result<Vec<Issue>, Error> {
    Ok(parse_tracked(raw)?.into_iter().map(|t| t.issue).collect())
}

/// The issues in a `gh api --paginate --slurp` listing of a repository's
/// issues, with their state reason, close time, milestone and labels. The
/// endpoint lists pull requests too, and those are dropped.
pub(crate) fn parse_tracked(raw: &str) -> Result<Vec<Tracked>, Error> {
    #[derive(Deserialize)]
    struct Row {
        number: u64,
        title: String,
        html_url: String,
        pull_request: Option<serde_json::Value>,
        state_reason: Option<String>,
        closed_at: Option<String>,
        milestone: Option<Numbered>,
        #[serde(default)]
        labels: Vec<Named>,
    }
    #[derive(Deserialize)]
    struct Numbered {
        number: u64,
    }
    #[derive(Deserialize)]
    struct Named {
        name: String,
    }
    let pages: Vec<Vec<Row>> =
        serde_json::from_str(raw).map_err(|e| Error::msg(format!("issues: {e}")))?;
    Ok(pages
        .into_iter()
        .flatten()
        .filter(|r| r.pull_request.is_none())
        .map(|r| Tracked {
            issue: Issue {
                number: r.number,
                title: r.title,
                url: r.html_url,
            },
            state_reason: r.state_reason,
            closed_at: r.closed_at,
            milestone: r.milestone.map(|m| m.number),
            labels: r.labels.into_iter().map(|l| l.name).collect(),
        })
        .collect())
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
        super::version::tree_version(self.root)
    }

    fn derive(&self) -> Result<Version, Error> {
        let (next, reason) = super::version::derive(self.root)?;
        eprintln!("release version: {reason}");
        Ok(next)
    }

    fn generate(&self, version: Version) -> Outcome {
        let v = version.to_string();
        super::version::bump(self.root, false, &v)?;
        changelog::build(self.root, false, &v)?;
        attribution::generate(self.root, false)
    }

    fn blog(&self) -> PathBuf {
        self.root.join(super::post::BLOG)
    }

    fn notes(&self, version: Version) -> Result<String, Error> {
        changelog::notes_text(self.root, &version.to_string())
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
         spate = {{ version = \"={version}\", features = [\"kafka\", \"clickhouse\", \"avro\", \"s3\", \"sqs\", \"json\"] }}\n\
         \n\
         [dev-dependencies]\n\
         spate-test = \"={version}\"\n"
    )
}

// ---------------------------------------------------------------------------
// The dry run's substitutes: writes are printed instead of made, and reads
// pass through except the open security issues, which are printed as the
// refusal a real run makes and read as none.
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
    fn remote_tags(&self) -> Result<Vec<(String, String)>, Error> {
        self.0.remote_tags()
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

/// A forge whose writes are printed instead of made, and whose open security
/// issues are printed and read as none. A refused pull request or security
/// issue listing reads as empty unless `.1` is set.
pub(crate) struct DryForge<'a>(pub(crate) &'a dyn Forge, pub(crate) bool);

impl Forge for DryForge<'_> {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error> {
        // gh refuses the listing on a clone with no remote on a GitHub host,
        // such as one whose only remote is a local path. Such a clone has no
        // pull requests to sweep. `GH_REPO` removes that case, so with `.1`
        // set a refusal is a real error.
        self.0.open_pulls().or_else(|e| {
            if self.1 {
                return Err(e);
            }
            would(&format!("list the open pull requests ({})", e.message));
            Ok(Vec::new())
        })
    }
    fn open_security_issues(&self, milestone: &str) -> Result<Vec<Issue>, Error> {
        // A rehearsal reports what a real run would refuse and carries on, so
        // an open issue does not fail the scheduled and pull request
        // rehearsals.
        match self.0.open_security_issues(milestone) {
            Ok(issues) => {
                for issue in &issues {
                    would(&format!(
                        "refuse the release for {}",
                        issue.describe(milestone)
                    ));
                }
                Ok(Vec::new())
            }
            Err(e) if self.1 => Err(e),
            Err(e) => {
                would(&format!(
                    "list the open security issues on {milestone} ({})",
                    e.message
                ));
                Ok(Vec::new())
            }
        }
    }
    fn pull_for_head(&self, head: &str) -> Result<Option<u64>, Error> {
        self.0.pull_for_head(head).or_else(|e| {
            if self.1 {
                return Err(e);
            }
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
    fn milestones(&self) -> Result<Vec<Milestone>, Error> {
        self.0.milestones()
    }
    fn closed_issues(&self, since: Option<&str>) -> Result<Vec<Tracked>, Error> {
        self.0.closed_issues(since)
    }
    fn open_issues(&self, milestone: u64) -> Result<Vec<Tracked>, Error> {
        self.0.open_issues(milestone)
    }
    fn milestone_events(&self, issue: u64) -> Result<Vec<MilestoneEvent>, Error> {
        self.0.milestone_events(issue)
    }
    fn landed_at(&self, sha: &str) -> Result<String, Error> {
        self.0.landed_at(sha)
    }
    fn create_milestone(&self, title: &str) -> Result<Option<u64>, Error> {
        would(&format!("create the {title} milestone"));
        Ok(None)
    }
    fn set_milestone_open(&self, milestone: &Target, open: bool) -> Outcome {
        let verb = if open { "reopen" } else { "close" };
        would(&format!("{verb} the {} milestone", milestone.title));
        Ok(())
    }
    fn set_milestone(&self, issue: u64, milestone: Option<&Target>) -> Outcome {
        match milestone {
            Some(m) => would(&format!("set the milestone of #{issue} to {}", m.title)),
            None => would(&format!("clear the milestone of #{issue}")),
        }
        Ok(())
    }
    fn add_label(&self, issue: u64, label: &str) -> Outcome {
        would(&format!("label #{issue} {label}"));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// The docs dispatch names a workflow that declares the tier it asks for
    /// and holds the deploy job.
    #[test]
    fn the_docs_dispatch_runs_the_nightly_docs_tier() {
        let step = docs_dispatch_step();
        assert_eq!(step.program, "gh");
        assert_eq!(
            step.args,
            ["workflow", "run", "nightly.yml", "--field", "tier=docs"]
        );
        let path = crate::repo_root()
            .unwrap()
            .join(".github/workflows")
            .join(&step.args[2]);
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let doc: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
        let inputs = &doc["on"]["workflow_dispatch"]["inputs"];
        for field in step
            .args
            .iter()
            .skip_while(|a| *a != "--field")
            .skip(1)
            .step_by(2)
        {
            let (key, value) = field.split_once('=').unwrap();
            let options = inputs[key]["options"]
                .as_sequence()
                .unwrap_or_else(|| panic!("{} declares no `{key}` options", path.display()));
            assert!(
                options.iter().any(|o| o.as_str() == Some(value)),
                "{} offers no `{key}: {value}`",
                path.display()
            );
        }
        let deploys = doc["jobs"]
            .as_mapping()
            .unwrap()
            .values()
            .any(|job| job["environment"]["name"].as_str() == Some("cloudflare-workers"));
        assert!(
            deploys,
            "{} holds no cloudflare-workers job",
            path.display()
        );
    }

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

    /// A named repository reaches gh through `GH_REPO`; without one gh reads
    /// the checkout's.
    #[test]
    fn a_named_repository_reaches_gh() {
        let named = Gh {
            root: Path::new("."),
            repo: Some("spate-etl/spate"),
        };
        assert_eq!(
            named.publish_step("v0.3.0").env,
            [("GH_REPO", "spate-etl/spate".to_owned())]
        );
        let local = Gh {
            root: Path::new("."),
            repo: None,
        };
        assert!(local.publish_step("v0.3.0").env.is_empty());
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

    /// A milestone is found by exact title on any page, open or closed, and
    /// an absent title reads as none.
    #[test]
    fn a_milestone_is_found_by_its_exact_title_on_any_page() {
        let raw = r#"[
            [{"number":5,"title":"v0.6.0","state":"open"},
             {"number":12,"title":"v0.5.0","state":"open"}],
            [{"number":4,"title":"v0.4.0","state":"closed"}]
        ]"#;
        assert_eq!(milestone_number(raw, "v0.5.0").unwrap(), Some(12));
        assert_eq!(milestone_number(raw, "v0.4.0").unwrap(), Some(4));
        assert_eq!(milestone_number(raw, "v0.5").unwrap(), None);
        assert_eq!(milestone_number("[[]]", "v0.5.0").unwrap(), None);
        assert!(milestone_number(r#"[{"number":1}]"#, "v0.5.0").is_err());
    }

    /// The issues are read on the titled milestone's number with the
    /// `security` label, and not read at all when no milestone has the title.
    #[test]
    fn security_issues_are_read_on_the_milestones_number() {
        let asked = RefCell::new(Vec::new());
        let get = |path: &str| {
            asked.borrow_mut().push(path.to_owned());
            Ok(if path.contains("/milestones?") {
                r#"[[{"number":12,"title":"v0.5.0"}]]"#.to_owned()
            } else {
                r#"[[{"number":7,"title":"a defect","html_url":"https://x/7"}]]"#.to_owned()
            })
        };
        assert_eq!(
            security_issues(get, "v0.5.0").unwrap(),
            [Issue {
                number: 7,
                title: "a defect".into(),
                url: "https://x/7".into()
            }]
        );
        assert_eq!(
            *asked.borrow(),
            [
                "repos/{owner}/{repo}/milestones?state=all&per_page=100",
                "repos/{owner}/{repo}/issues?milestone=12&labels=security&state=open&per_page=100",
            ]
        );

        asked.borrow_mut().clear();
        assert!(security_issues(get, "v0.4.1").unwrap().is_empty());
        assert_eq!(asked.borrow().len(), 1);

        let refused = |_: &str| Err(Error::msg("refused"));
        assert!(security_issues(refused, "v0.5.0").is_err());
    }

    /// A milestone listing that does not parse fails the read, and does not
    /// read as a missing milestone.
    #[test]
    fn an_unreadable_milestone_listing_fails_the_security_read() {
        let get = |path: &str| {
            Ok(if path.contains("/milestones?") {
                r#"{"message":"Not Found"}"#.to_owned()
            } else {
                "[[]]".to_owned()
            })
        };
        let err = security_issues(get, "v0.5.0").unwrap_err();
        assert!(err.message.starts_with("milestones: "), "{}", err.message);
    }

    /// Every milestone is listed with its state; one with no state reads as
    /// closed.
    #[test]
    fn the_milestone_listing_keeps_every_milestone_and_its_state() {
        let raw = r#"[
            [{"number":5,"title":"v0.6.0","state":"open"}],
            [{"number":4,"title":"v0.4.0","state":"closed"},{"number":9,"title":"v0.9.0"}]
        ]"#;
        let m = |number, title: &str, open| Milestone {
            number,
            title: title.to_owned(),
            open,
        };
        assert_eq!(
            parse_milestones(raw).unwrap(),
            [
                m(5, "v0.6.0", true),
                m(4, "v0.4.0", false),
                m(9, "v0.9.0", false)
            ]
        );
    }

    /// An issue row carries its state reason, close time, milestone number and
    /// label names, each absent where the listing leaves it out.
    #[test]
    fn a_tracked_issue_carries_what_the_milestone_step_reads() {
        let raw = r#"[[
            {"number":7,"title":"t","html_url":"https://x/7","state_reason":"completed",
             "closed_at":"2026-10-20T12:00:00Z","milestone":{"number":4,"title":"v0.4.0"},
             "labels":[{"name":"priority: high"},{"name":"area: ci"}]},
            {"number":8,"title":"u","html_url":"https://x/8","state_reason":null,
             "closed_at":null,"milestone":null},
            {"number":9,"title":"pr","html_url":"https://x/9","pull_request":{}}
        ]]"#;
        let tracked = parse_tracked(raw).unwrap();
        assert_eq!(
            tracked,
            [
                Tracked {
                    issue: Issue {
                        number: 7,
                        title: "t".into(),
                        url: "https://x/7".into()
                    },
                    state_reason: Some("completed".into()),
                    closed_at: Some("2026-10-20T12:00:00Z".into()),
                    milestone: Some(4),
                    labels: vec!["priority: high".into(), "area: ci".into()],
                },
                Tracked {
                    issue: Issue {
                        number: 8,
                        title: "u".into(),
                        url: "https://x/8".into()
                    },
                    state_reason: None,
                    closed_at: None,
                    milestone: None,
                    labels: Vec::new(),
                },
            ]
        );
    }

    /// Only milestone events are kept, in order across pages, with the title
    /// they were written under.
    #[test]
    fn the_event_listing_keeps_the_milestone_events_in_order() {
        let raw = r#"[
            [{"event":"milestoned","created_at":"2026-09-26T04:47:43Z","milestone":{"title":"v0.4.0"}},
             {"event":"labeled","created_at":"2026-09-27T00:00:00Z","label":{"name":"x"}}],
            [{"event":"demilestoned","created_at":"2026-10-15T20:09:58Z","milestone":{"title":"v0.4.0"}},
             {"event":"milestoned","created_at":"2026-10-15T20:09:58Z","milestone":{"title":"v0.5.0"}}]
        ]"#;
        let e = |added, title: &str, at: &str| MilestoneEvent {
            added,
            title: title.to_owned(),
            at: at.to_owned(),
        };
        assert_eq!(
            parse_milestone_events(raw).unwrap(),
            [
                e(true, "v0.4.0", "2026-09-26T04:47:43Z"),
                e(false, "v0.4.0", "2026-10-15T20:09:58Z"),
                e(true, "v0.5.0", "2026-10-15T20:09:58Z"),
            ]
        );
        assert!(parse_milestone_events(r#"{"message":"Not Found"}"#).is_err());
    }

    /// A commit landed when the pull request it was squashed from merged; one
    /// that came from no pull request landed at its committer date.
    #[test]
    fn a_commit_lands_at_its_pull_requests_merge() {
        let asked = RefCell::new(Vec::new());
        let get = |pulls: &'static str| {
            let asked = &asked;
            move |path: &str| {
                asked.borrow_mut().push(path.to_owned());
                Ok(if path.ends_with("/pulls") {
                    pulls.to_owned()
                } else {
                    r#"[{"commit":{"committer":{"date":"2026-11-01T16:53:39Z"}}}]"#.to_owned()
                })
            }
        };
        let two = r#"[[
            {"merge_commit_sha":"other","merged_at":"2026-10-01T00:00:00Z"},
            {"merge_commit_sha":"abc","merged_at":"2026-11-01T17:12:38Z"}
        ]]"#;
        assert_eq!(landed_at(get(two), "abc").unwrap(), "2026-11-01T17:12:38Z");
        assert_eq!(*asked.borrow(), ["repos/{owner}/{repo}/commits/abc/pulls"]);

        asked.borrow_mut().clear();
        let unrelated = r#"[[{"merge_commit_sha":"other","merged_at":"2026-10-01T00:00:00Z"}]]"#;
        assert_eq!(
            landed_at(get(unrelated), "abc").unwrap(),
            "2026-11-01T16:53:39Z"
        );
        assert_eq!(
            *asked.borrow(),
            [
                "repos/{owner}/{repo}/commits/abc/pulls",
                "repos/{owner}/{repo}/commits/abc"
            ]
        );
        let unmerged = r#"[[{"merge_commit_sha":"abc","merged_at":null}]]"#;
        assert_eq!(
            landed_at(get(unmerged), "abc").unwrap(),
            "2026-11-01T16:53:39Z"
        );
        let none = |path: &str| {
            Ok(if path.ends_with("/pulls") {
                "[[]]".to_owned()
            } else {
                "[]".to_owned()
            })
        };
        assert!(landed_at(none, "abc").is_err());
    }

    /// The closed-issue listing filters on `since` only when there is one.
    #[test]
    fn the_closed_issue_listing_starts_at_since() {
        assert_eq!(
            closed_issues_path(Some("2026-10-11T22:40:05Z")),
            "repos/{owner}/{repo}/issues?state=closed&since=2026-10-11T22:40:05Z&per_page=100"
        );
        assert_eq!(
            closed_issues_path(None),
            "repos/{owner}/{repo}/issues?state=closed&per_page=100"
        );
    }

    /// Each milestone write is one REST call by number; `-F` sends the number,
    /// and `null` to clear.
    #[test]
    fn the_milestone_writes_address_by_number() {
        assert_eq!(
            create_milestone_args("v0.4.1"),
            [
                "api",
                "-X",
                "POST",
                "repos/{owner}/{repo}/milestones",
                "-f",
                "title=v0.4.1"
            ]
        );
        assert_eq!(
            milestone_state_args(4, true),
            [
                "api",
                "-X",
                "PATCH",
                "repos/{owner}/{repo}/milestones/4",
                "-f",
                "state=open"
            ]
        );
        assert_eq!(milestone_state_args(4, false)[5], "state=closed");
        assert_eq!(
            issue_milestone_args(7, Some(4)),
            [
                "api",
                "-X",
                "PATCH",
                "repos/{owner}/{repo}/issues/7",
                "-F",
                "milestone=4"
            ]
        );
        assert_eq!(issue_milestone_args(7, None)[5], "milestone=null");
        assert_eq!(
            add_label_args(7, "priority: medium"),
            [
                "api",
                "-X",
                "POST",
                "repos/{owner}/{repo}/issues/7/labels",
                "-f",
                "labels[]=priority: medium"
            ]
        );
        let unnumbered = Target {
            title: "v0.4.1".into(),
            number: None,
        };
        assert!(number_of(&unnumbered).is_err());
        assert_eq!(
            number_of(&Target {
                number: Some(4),
                ..unnumbered
            })
            .unwrap(),
            4
        );
    }

    /// The created milestone's number is read from the reply.
    #[test]
    fn a_created_milestone_answers_its_number() {
        assert_eq!(
            created_number(r#"{"number":13,"title":"v0.4.1"}"#).unwrap(),
            13
        );
        assert!(created_number(r#"{"message":"Validation Failed"}"#).is_err());
    }

    /// Each tag on origin reads as the commit it names, through an annotated
    /// tag's peeled line, in the listing's order.
    #[test]
    fn the_origin_tags_read_as_their_commits() {
        let listing = "aaa\trefs/tags/v0.3.0\n\
                       bbb\trefs/tags/v0.4.0\n\
                       ccc\trefs/tags/v0.4.0^{}\n\
                       ddd\trefs/heads/main\n\
                       malformed\n";
        assert_eq!(
            peeled_tags(listing),
            [
                ("v0.3.0".to_owned(), "aaa".to_owned()),
                ("v0.4.0".to_owned(), "ccc".to_owned())
            ]
        );
        assert!(peeled_tags("").is_empty());
    }

    /// The issue listing keeps issues from every page and drops the pull
    /// requests the endpoint also returns.
    #[test]
    fn the_issue_listing_drops_pull_requests() {
        let raw = r#"[
            [{"number":7,"title":"a defect","html_url":"https://x/7"},
             {"number":8,"title":"its fix","html_url":"https://x/8","pull_request":{"url":"u"}}],
            [{"number":9,"title":"another","html_url":"https://x/9"}]
        ]"#;
        assert_eq!(
            parse_issues(raw).unwrap(),
            [
                Issue {
                    number: 7,
                    title: "a defect".into(),
                    url: "https://x/7".into()
                },
                Issue {
                    number: 9,
                    title: "another".into(),
                    url: "https://x/9".into()
                }
            ]
        );
        assert!(parse_issues("[[]]").unwrap().is_empty());
        assert!(parse_issues("not json").is_err());
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
    /// `spate-test-support` among them, and keeps the caller's excludes. Regression
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
        assert!(unpublished.iter().any(|n| n == "spate-test-support"));
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

    /// Every feature the smoke consumer enables is declared by the facade, so
    /// removing a feature fails here and not in the post-publish smoke build.
    #[test]
    fn the_smoke_consumer_names_only_declared_features() {
        let smoke: toml::Table =
            toml::from_str(&smoke_manifest(Version::parse("0.3.0").unwrap())).unwrap();
        let facade = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/spate/Cargo.toml"),
        )
        .unwrap();
        let facade: toml::Table = toml::from_str(&facade).unwrap();
        let declared = facade["features"].as_table().unwrap();
        for feature in smoke["dependencies"]["spate"]["features"]
            .as_array()
            .unwrap()
        {
            let feature = feature.as_str().unwrap();
            assert!(
                declared.contains_key(feature),
                "`{feature}` is not a facade feature"
            );
        }
    }
}
