//! The release sequence: `assemble` builds the release commit and its pull
//! request, and `prepare`, `upload` and `finish` publish what that pull
//! request's squash merge lands. RELEASING.md is the account of the process.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::io::{Host, Index, Resolution};
use super::version::Version;
use crate::checks::scratch::Scratch;
use crate::run::{Error, Outcome};

/// The registry API's documented limit is one request per second.
const API_INTERVAL: Duration = Duration::from_secs(1);
/// The wait between attempts on a read the CDN may still be lagging.
const LAG_INTERVAL: Duration = Duration::from_secs(30);
const LAG_ATTEMPTS: u32 = 5;

/// Folds a phase in the Actions log, or prints a heading elsewhere.
pub(crate) fn group(title: &str) {
    if std::env::var_os("GITHUB_ACTIONS").is_some() {
        println!("::group::{title}");
    } else {
        println!("==> {title}");
    }
}

pub(crate) fn endgroup() {
    if std::env::var_os("GITHUB_ACTIONS").is_some() {
        println!("::endgroup::");
    }
}

/// The version a release commit's subject names. The squash merge appends
/// ` (#N)`; a plain commit carries none.
pub(crate) fn version_from_subject(subject: &str) -> Option<Version> {
    let rest = subject.strip_prefix("release: v")?;
    let (version, suffix_ok) = match rest.split_once(' ') {
        None => (rest, true),
        Some((version, tail)) => (
            version,
            tail.strip_prefix("(#")
                .and_then(|t| t.strip_suffix(')'))
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
        ),
    };
    Version::parse(version).filter(|_| suffix_ok)
}

/// The branch a release pull request is opened from.
fn release_branch(version: Version) -> String {
    format!("release/v{version}")
}

/// Whether a branch has the exact shape of a release branch, so a human branch
/// such as `release/v2-planning` is never swept.
fn is_release_branch(head: &str) -> bool {
    head.strip_prefix("release/v")
        .and_then(Version::parse)
        .is_some()
}

// ---------------------------------------------------------------------------
// assemble
// ---------------------------------------------------------------------------

const COMMIT_BODY: &str = "Every artefact is generated from the version input: the manifest rewrite,\n\
Cargo.lock, CHANGELOG.md assembled from changelog.d/ and the moved dependency\n\
requirements, THIRD-PARTY.md and the install snippets. The squash merge of\n\
this pull request is what triggers the publish.";

const PULL_BODY: &str = "Assembled by `release.yml` from the v{version} dispatch. Every file in this diff is generated; the reviewed prose is the fragments it consumes, which landed with their changes, and the dependency requirement entry the build writes from `Cargo.toml`. The squash merge triggers the publish. The controls are the version input and its derivation check, so a review here is reading the assembled changelog, not the mechanics.";

/// Builds the single release commit and opens, or refreshes, the pull request
/// whose squash merge triggers the publish.
pub(crate) fn assemble(host: &Host<'_>, input: &str) -> Outcome {
    group("Guards");
    if !host.git.is_clean()? {
        return Err(Error::msg(
            "the working tree is not clean; a release is assembled from committed state only",
        ));
    }
    let version =
        Version::parse(input).ok_or_else(|| Error::msg(format!("'{input}' is not X.Y.Z")))?;
    let current = host.workspace.version()?;
    if version == current {
        return Err(Error::msg(format!(
            "the workspace is already at {current}. If its release\n  \
             pull request has merged, the publish runs from that push; resume a failed one\n  \
             by re-running its failed jobs, which reuses the same commit."
        )));
    }
    let last = host.git.last_tag()?;
    if format!("v{current}") != last {
        return Err(Error::msg(format!(
            "Cargo.toml is at {current} but the last tag is {last}: a\n  \
             release is half-finished. Finish it before assembling the next one."
        )));
    }
    if host.git.remote_tag(&format!("v{version}"))?.is_some() {
        return Err(Error::msg(format!(
            "v{version} is already tagged on origin"
        )));
    }
    let expected = host.workspace.derive()?;
    if expected != version {
        return Err(Error::msg(format!(
            "the input says {version} but the history since {last}\n  \
             derives {expected}. One of the two is wrong; nothing proceeds until they agree."
        )));
    }
    endgroup();

    group("Generate every artefact");
    host.workspace.generate(version)?;
    endgroup();

    group("The release commit");
    host.git
        .commit_all(&format!("release: v{version}"), COMMIT_BODY)?;
    endgroup();

    group("The release pull request");
    // Two live auto-merge release pull requests is how an unintended version
    // merges, so every same-repository release branch at another version is
    // superseded and closed.
    let branch = release_branch(version);
    for stale in host
        .forge
        .open_pulls()?
        .iter()
        .filter(|p| !p.cross_repository && is_release_branch(&p.head) && p.head != branch)
    {
        host.forge.close_pull(
            stale.number,
            &format!("Superseded by the v{version} dispatch."),
        )?;
    }

    host.git.push(&format!("HEAD:refs/heads/{branch}"), true)?;

    // Re-dispatching the same version refreshes the branch above and reuses
    // the open pull request.
    let number = match host.forge.pull_for_head(&branch)? {
        Some(number) => Some(number),
        None => host.forge.create_pull(
            &format!("release: v{version}"),
            &branch,
            "release",
            &PULL_BODY.replace("{version}", &version.to_string()),
        )?,
    };
    if let Some(number) = number {
        host.forge.auto_merge(number)?;
        println!(
            "release: pull request #{number} auto-merges when CI gate passes; the publish follows the merge."
        );
    }
    endgroup();
    Ok(())
}

// ---------------------------------------------------------------------------
// prepare
// ---------------------------------------------------------------------------

/// What `prepare` selected, for the steps after it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub(crate) version: Version,
    /// The crates still to publish.
    pub(crate) pending: Vec<String>,
    /// The crates already at this version.
    pub(crate) excludes: Vec<String>,
}

/// Verifies the release commit, selects the crates still to publish, and
/// packages and verify-builds them, all before any credential exists.
pub(crate) fn prepare(host: &Host<'_>, expected_sha: Option<&str>) -> Result<Prepared, Error> {
    group("The release commit names the version");
    let subject = host.git.subject()?;
    let version = version_from_subject(&subject)
        .ok_or_else(|| Error::msg(format!("HEAD's subject is not a release commit: {subject}")))?;
    let manifest = host.workspace.version()?;
    if version != manifest {
        return Err(Error::msg(format!(
            "the subject says {version} but Cargo.toml says {manifest}; the tree is not the release"
        )));
    }
    // `finish` reads this section after the crates are permanent, so it has to
    // exist before anything uploads.
    host.workspace.notes(version)?;
    println!(
        "release: releasing v{version} from {}",
        host.git.short_head()?
    );
    endgroup();

    group("Select the crates still to publish");
    // The sparse index answers per-version presence without the API's rate
    // limit. A yanked version still occupies its number, so it counts.
    let packages = host.workspace.publishable()?;
    let (mut pending, mut excludes) = (Vec::new(), Vec::new());
    for package in &packages {
        let name = &package.name;
        match host.registry.index(name)? {
            Index::Found(entries) if entries.iter().any(|e| e.vers == version.to_string()) => {
                println!("already published, excluding: {name} {version}");
                excludes.push(name.clone());
            }
            Index::Found(_) => {
                println!("to publish: {name} {version}");
                pending.push(name.clone());
            }
            Index::Missing => {
                return Err(Error::msg(format!(
                    "{name} is not on the registry at all. Trusted Publishing cannot create a\n  \
                     crate, so a new name is claimed by hand first; RELEASING.md has the checklist."
                )));
            }
            Index::Status(code) => {
                return Err(Error::msg(format!(
                    "the index answered {code} for {name}; refusing to publish against a\n  \
                     registry the run cannot read, which is how a version gets published twice"
                )));
            }
        }
    }
    endgroup();

    group("No crate published from another tree");
    // A crate already at this version from a different commit means the
    // release is split across trees. A manual token publish records no commit
    // and fails the same way.
    for name in &excludes {
        let Some(expected) = expected_sha else {
            return Err(Error::msg(format!(
                "{name} is already published at {version} and no EXPECTED_SHA is set to verify\n  \
                 which tree it came from. Locally that means the version is already part-released."
            )));
        };
        let sha = host.registry.trustpub_sha(name, version)?;
        if sha != expected {
            return Err(Error::msg(format!(
                "{name} {version} was published from {sha}, not from\n  \
                 {expected}. The release is split across trees; abandon {version}. The squash\n  \
                 subject is the publish trigger, so a hand-opened pull request titled\n  \
                 'release: v<next>' carrying the bump publishes the next patch from one\n  \
                 commit."
            )));
        }
        (host.pause)(API_INTERVAL);
    }
    endgroup();

    group("Required metadata is present");
    // The package step warns and exits 0 on a missing description or license,
    // while the upload rejects it after publishing whatever sorted before it
    // (cargo issue 14249).
    let bad = host.workspace.missing_metadata()?;
    if !bad.is_empty() {
        return Err(Error::msg(format!(
            "missing description or license in: {}. The upload would reject these\n  \
             after publishing whatever sorted before them.",
            bad.join(" ")
        )));
    }
    endgroup();

    if pending.is_empty() {
        println!("release: every crate already carries {version}; nothing to package.");
    } else {
        group("Package and verify every pending crate");
        // Without the excludes the remaining crates would verify against local
        // copies of crates already published, a combination the upload never
        // uses.
        host.workspace.package(&excludes)?;
        endgroup();
    }
    Ok(Prepared {
        version,
        pending,
        excludes,
    })
}

/// Appends `prepare`'s outputs to the file `GITHUB_OUTPUT` names, when it is
/// set.
pub(crate) fn write_outputs(prepared: &Prepared) -> Outcome {
    match std::env::var_os("GITHUB_OUTPUT") {
        Some(path) => append_outputs(Path::new(&path), prepared),
        None => Ok(()),
    }
}

/// Appends `version=`, `excludes=` and `pending=` lines to `path`.
fn append_outputs(path: &Path, prepared: &Prepared) -> Outcome {
    let lines = format!(
        "version={}\nexcludes={}\npending={}\n",
        prepared.version,
        prepared.excludes.join(" "),
        prepared.pending.len()
    );
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .and_then(|mut f| f.write_all(lines.as_bytes()))
        .map_err(|e| Error::msg(format!("GITHUB_OUTPUT: {e}")))
}

/// What a real run does after the point a dry run stops.
pub(crate) fn print_next(host: &Host<'_>, prepared: &Prepared) -> Outcome {
    group("SBOMs generate");
    let scratch = Scratch::new("spate-release-sbom")?;
    host.workspace.sboms(prepared.version, scratch.dir())?;
    endgroup();
    group("What a real run would do next");
    let v = prepared.version;
    let excludes: String = prepared
        .excludes
        .iter()
        .map(|e| format!(" --exclude {e}"))
        .collect();
    println!("would attest target/package/*.crate (actions/attest-build-provenance)");
    println!(
        "would mint the 30-minute registry token (crates-io-auth-action, environment crates-io)"
    );
    println!("would run: cargo publish --workspace --locked --no-verify{excludes}");
    println!("would read back trustpub_data for every crate and require the release commit");
    println!("would compare each packaged crate's sha256 against the index cksum");
    println!("would resolve a scratch project against the registry (the smoke test)");
    println!("would tag v{v}, open the GitHub release with the CHANGELOG section and the");
    println!("  SBOMs and provenance bundle as assets, and deploy the docs");
    endgroup();
    Ok(())
}

// ---------------------------------------------------------------------------
// upload
// ---------------------------------------------------------------------------

/// Uploads the pending crates. The only step that holds the registry token.
///
/// No verify build runs: `prepare` packaged and verify-built every pending
/// crate in the same job, and the token's fixed 30-minute life is not spent
/// compiling the workspace again.
pub(crate) fn upload(host: &Host<'_>, token: bool, pending: u32, excludes: &[String]) -> Outcome {
    if !token {
        return Err(Error::msg("upload needs CARGO_REGISTRY_TOKEN"));
    }
    if pending == 0 {
        println!("release: nothing pending; skipping the upload.");
        return Ok(());
    }
    host.workspace.publish(excludes)
}

// ---------------------------------------------------------------------------
// finish
// ---------------------------------------------------------------------------

/// Verifies what the registry holds, then tags, releases and deploys the
/// documentation. Every step skips what an earlier attempt already did, so a
/// re-run resumes.
pub(crate) fn finish(
    host: &Host<'_>,
    version: Version,
    expected_sha: &str,
    bundle: Option<&Path>,
) -> Outcome {
    let tag = format!("v{version}");
    let packages = host.workspace.publishable()?;

    group("Every crate came from this commit");
    // Judged from the registry rather than from the workflow's own success.
    for package in &packages {
        let sha = host.registry.trustpub_sha(&package.name, version)?;
        if sha != expected_sha {
            return Err(Error::msg(format!(
                "{} {version} reports trustpub sha '{sha}', expected {expected_sha}",
                package.name
            )));
        }
        println!("verified: {} {version}", package.name);
        (host.pause)(API_INTERVAL);
    }
    endgroup();

    group("The registry serves the bytes this run packaged");
    // The index's cksum is the sha256 of the served `.crate`, and it must equal
    // the local file the attestation covers. Only crates packaged in this run
    // have a local file.
    let mut checked = 0;
    for package in &packages {
        let name = &package.name;
        let Some(got) = host.workspace.packaged_sha256(name, version)? else {
            continue;
        };
        let want = served_cksum(host, name, version)?;
        if want != got {
            return Err(Error::msg(format!(
                "{name} {version}: the registry serves {want} but this run packaged\n  \
                 {got}. The attestation would cover bytes the registry does not hold."
            )));
        }
        println!("cksum verified: {name} {version}");
        checked += 1;
    }
    if checked == 0 {
        println!("nothing was packaged in this run; nothing to compare.");
    }
    endgroup();

    group("A consumer resolves the release");
    resolves(host, version)?;
    endgroup();

    group("Tag and release");
    // Tagged after the publish, so the tag names what the registry holds.
    match host.git.remote_tag(&tag)? {
        Some(commit) if commit == expected_sha => {
            println!("{tag} is already tagged on this commit.")
        }
        Some(commit) => {
            return Err(Error::msg(format!(
                "{tag} already exists and points at {commit}, not {expected_sha}"
            )));
        }
        None => {
            host.git.tag(&tag, expected_sha)?;
            host.git.push(&format!("refs/tags/{tag}"), false)?;
        }
    }
    if host.forge.release_exists(&tag)? {
        println!("the {tag} release already exists.");
    } else {
        let notes = host.workspace.notes(version)?;
        if host.forge.create_release(&tag, &notes).is_err() {
            // A tag pushed moments ago can lag replication on the API side.
            (host.pause)(Duration::from_secs(10));
            host.forge.create_release(&tag, &notes)?;
        }
    }
    endgroup();

    group("SBOMs and provenance on the release");
    // `--clobber` for the SBOMs, whose regeneration is byte-stable, so a
    // resumed run completes the set. Not for the bundle: attempts only shrink
    // the pending set, so the first bundle covers the most crates.
    let scratch = Scratch::new("spate-release-assets")?;
    host.workspace.sboms(version, scratch.dir())?;
    let sboms: Vec<PathBuf> = packages
        .iter()
        .map(|p| scratch.join(&format!("{}-{version}.cdx.json", p.name)))
        .collect();
    host.forge.upload(&tag, &sboms, true)?;
    if let Some(bundle) = bundle.filter(|b| b.is_file()) {
        let named = scratch.join(&format!("spate-{tag}-provenance.intoto.jsonl"));
        std::fs::copy(bundle, &named)
            .map_err(|e| Error::msg(format!("{}: {e}", bundle.display())))?;
        if host.forge.upload(&tag, &[named], false).is_err() {
            println!("an attestation bundle is already attached; keeping it.");
        }
    }
    // The bundle lives only on the runner that attested, so a run that died
    // between attesting and uploading has lost its copy.
    let repo = std::env::var("GITHUB_REPOSITORY").unwrap_or_else(|_| "spate-etl/spate".to_owned());
    if !host
        .forge
        .assets(&tag)?
        .iter()
        .any(|a| a.ends_with(".intoto.jsonl"))
    {
        return Err(Error::msg(format!(
            "the release carries no provenance bundle. Recover it from the attestation\n  \
             store: download any published .crate of this version from\n  \
             https://static.crates.io/crates/<name>/<name>-{version}.crate, run\n  \
             'gh attestation download <file> --repo {repo}', and upload the\n  \
             bundle as spate-{tag}-provenance.intoto.jsonl."
        )));
    }
    endgroup();

    group("Deploy the documentation");
    // The site carries the install snippets, so it deploys after the crates
    // are live.
    host.forge.dispatch_docs()?;
    println!("docs deploy dispatched; docs.rs builds on its own and lags the publish.");
    endgroup();
    Ok(())
}

/// The index's cksum for one version. The index is CDN-fronted and the runner
/// primed its cache before the upload, so a missing entry is retried as lag;
/// any status other than 200 or 404 fails at once.
fn served_cksum(host: &Host<'_>, name: &str, version: Version) -> Result<String, Error> {
    let v = version.to_string();
    for attempt in 1..=LAG_ATTEMPTS {
        match host.registry.index(name)? {
            Index::Found(entries) => {
                if let Some(entry) = entries.iter().find(|e| e.vers == v && !e.cksum.is_empty()) {
                    return Ok(entry.cksum.clone());
                }
            }
            Index::Missing => {}
            Index::Status(code) => {
                return Err(Error::msg(format!(
                    "the index answered {code} for {name}; the served bytes cannot be checked"
                )));
            }
        }
        println!("attempt {attempt}: the index has no entry for {name} {version} yet; waiting 30s");
        (host.pause)(LAG_INTERVAL);
    }
    Err(Error::msg(format!(
        "the index never served {name} {version}; the upload reported\n  \
         success, so this is the index lagging beyond patience or the publish being\n  \
         lost. Re-run the failed jobs once the index answers."
    )))
}

/// A scratch consumer resolves the facade at exactly `version`, retried
/// because the CDN can lag the publish by a few minutes.
fn resolves(host: &Host<'_>, version: Version) -> Outcome {
    let mut last = String::new();
    for attempt in 1..=LAG_ATTEMPTS {
        match host.workspace.resolve(version)? {
            Resolution::Resolved(n) => {
                println!("resolved {n} packages from the registry");
                return Ok(());
            }
            Resolution::Failed(tail) => last = tail,
        }
        println!("attempt {attempt}: the registry has not served v{version} yet; waiting 30s");
        (host.pause)(LAG_INTERVAL);
    }
    // Cargo's own answer decides whether this is lag or a graph that does not
    // resolve, and only one of those is fixed by a re-run.
    eprintln!("{last}");
    Err(Error::msg(format!(
        "a consumer cannot resolve spate ={version} from the registry; cargo's answer is above"
    )))
}

#[cfg(test)]
mod tests;
