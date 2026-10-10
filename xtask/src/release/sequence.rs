//! The release sequence: `assemble` builds the release commit and its pull
//! request, and `prepare`, `upload` and `finish` publish what that pull
//! request's squash merge lands. RELEASING.md is the account of the process.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::io::{Host, Index, ReleaseState, Resolution};
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

impl Prepared {
    /// Whether `stage` writes anything: only before any crate of the version
    /// is published, so every staged set covers the whole release.
    pub(crate) fn stages(&self) -> bool {
        self.excludes.is_empty()
    }
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

/// Appends `version=`, `excludes=`, `pending=`, `crates=` and `staged=` lines
/// to `path`.
fn append_outputs(path: &Path, prepared: &Prepared) -> Outcome {
    let lines = format!(
        "version={}\nexcludes={}\npending={}\ncrates={}\nstaged={}\n",
        prepared.version,
        prepared.excludes.join(" "),
        prepared.pending.len(),
        serde_json::to_string(&prepared.pending)
            .map_err(|e| Error::msg(format!("GITHUB_OUTPUT: {e}")))?,
        prepared.stages()
    );
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .and_then(|mut f| f.write_all(lines.as_bytes()))
        .map_err(|e| Error::msg(format!("GITHUB_OUTPUT: {e}")))
}

/// The checksum list `stage` writes beside the artifacts.
pub(crate) const SUMS: &str = "SHA256SUMS";

/// Collects what a release attests into `out`: each packaged `.crate`, one
/// SBOM per publishable crate, and a `SHA256SUMS` over both.
///
/// Writes nothing once part of the version is published. The publish then
/// reads the set the first build staged, which covers every crate.
pub(crate) fn stage(host: &Host<'_>, prepared: &Prepared, out: &Path) -> Outcome {
    let version = prepared.version;
    if !prepared.stages() {
        println!(
            "release: part of {version} is already published, so this build stages nothing;\n  \
             the publish uses the artifacts the first build staged"
        );
        return Ok(());
    }
    group("Stage the artifacts to attest");
    std::fs::create_dir_all(out).map_err(|e| Error::msg(format!("{}: {e}", out.display())))?;
    // Everything in `out` is listed and attested, so it starts empty.
    if std::fs::read_dir(out)
        .map_err(|e| Error::msg(format!("{}: {e}", out.display())))?
        .next()
        .is_some()
    {
        return Err(Error::msg(format!(
            "{} is not empty; the release stages into an empty directory",
            out.display()
        )));
    }
    for name in &prepared.pending {
        let file = host
            .workspace
            .crate_file(name, version)
            .ok_or_else(|| Error::msg(format!("{name} {version} was not packaged by this run")))?;
        let target = out.join(format!("{name}-{version}.crate"));
        std::fs::copy(&file, &target)
            .map_err(|e| Error::msg(format!("{}: {e}", file.display())))?;
    }
    host.workspace.sboms(version, out)?;
    let mut names: Vec<String> = std::fs::read_dir(out)
        .map_err(|e| Error::msg(format!("{}: {e}", out.display())))?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".crate") || n.ends_with(".cdx.json"))
        .collect();
    names.sort();
    let mut sums = String::new();
    for name in &names {
        sums.push_str(&format!(
            "{}  {name}\n",
            host.workspace.sha256(&out.join(name))?
        ));
    }
    std::fs::write(out.join(SUMS), sums).map_err(|e| Error::msg(format!("{SUMS}: {e}")))?;
    println!(
        "staged {} file(s) and {SUMS} in {}",
        names.len(),
        out.display()
    );
    endgroup();
    Ok(())
}

/// What a real run does after the point a dry run stops.
pub(crate) fn print_next(prepared: &Prepared) {
    group("What a real run would do next");
    let v = prepared.version;
    let excludes: String = prepared
        .excludes
        .iter()
        .map(|e| format!(" --exclude {e}"))
        .collect();
    println!("would attest each .crate and {SUMS} with SLSA provenance, and each .crate");
    println!("  with its CycloneDX SBOM, from the release-build.yml reusable workflow");
    println!("would verify every attestation against release-build.yml before any token exists");
    println!(
        "would mint the 30-minute registry token (crates-io-auth-action, environment crates-io)"
    );
    println!("would run: cargo publish --workspace --locked --no-verify{excludes}");
    println!("would read back trustpub_data for every crate and require the release commit");
    println!("would compare each attested crate's sha256 against the index cksum");
    println!("would resolve a scratch project against the registry (the smoke test)");
    println!("would tag v{v} and push the tag");
    println!("would draft the GitHub release with the CHANGELOG section, attach the SBOMs,");
    println!("  {SUMS} and the attestation bundle, publish it immutable, verify its");
    println!("  attestation, and deploy the docs");
    endgroup();
}

// ---------------------------------------------------------------------------
// verify-artifacts
// ---------------------------------------------------------------------------

/// The predicate type of a SLSA provenance attestation.
pub(crate) const PROVENANCE: &str = "https://slsa.dev/provenance/v1";
/// The predicate type of a CycloneDX SBOM attestation.
pub(crate) const CYCLONEDX: &str = "https://cyclonedx.org/bom";

/// The `<sha256>  <name>` lines of a checksum file, by name.
pub(crate) fn parse_sums(text: &str) -> Result<Vec<(String, String)>, Error> {
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (digest, name) = l
                .split_once("  ")
                .ok_or_else(|| Error::msg(format!("{SUMS}: malformed line '{l}'")))?;
            if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::msg(format!("{SUMS}: malformed digest in '{l}'")));
            }
            Ok((name.to_owned(), digest.to_owned()))
        })
        .collect()
}

/// Every attestation bundle in `dir`, joined into one JSON-lines file at
/// `out`. Answers how many attestations it holds.
pub(crate) fn join_bundles(dir: &Path, out: &Path) -> Result<usize, Error> {
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".intoto.jsonl") && p != out)
        .collect();
    names.sort();
    let mut joined = String::new();
    for path in &names {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            joined.push_str(line);
            joined.push('\n');
        }
    }
    std::fs::write(out, &joined).map_err(|e| Error::msg(format!("{}: {e}", out.display())))?;
    Ok(joined.lines().count())
}

/// Checks the staged artifacts before any credential exists.
///
/// `SHA256SUMS` lists exactly the crates and SBOMs present, with their digests.
/// Every crate the registry does not yet hold is staged. Each staged crate
/// carries a provenance and an SBOM attestation, and `SHA256SUMS` a provenance
/// attestation, each signed by `signer_workflow` from `main` at `commit`. This
/// checkout packages each unpublished crate to the same bytes as the staged
/// one, so the upload sends what was attested.
pub(crate) fn verify_artifacts(
    host: &Host<'_>,
    dir: &Path,
    signer_workflow: &str,
    commit: &str,
) -> Outcome {
    group("The staged artifacts match their checksums");
    let sums =
        std::fs::read_to_string(dir.join(SUMS)).map_err(|e| Error::msg(format!("{SUMS}: {e}")))?;
    let listed = parse_sums(&sums)?;
    let mut present: Vec<String> = std::fs::read_dir(dir)
        .map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".crate") || n.ends_with(".cdx.json"))
        .collect();
    present.sort();
    let mut names: Vec<String> = listed.iter().map(|(n, _)| n.clone()).collect();
    names.sort();
    if names != present {
        return Err(Error::msg(format!(
            "{SUMS} lists [{}] but the directory holds [{}]",
            names.join(" "),
            present.join(" ")
        )));
    }
    for (name, digest) in &listed {
        let got = host.workspace.sha256(&dir.join(name))?;
        if &got != digest {
            return Err(Error::msg(format!(
                "{name}: {SUMS} says {digest} but the file is {got}"
            )));
        }
    }
    println!("{} file(s) match {SUMS}", listed.len());
    endgroup();

    let version = host.workspace.version()?;
    let (pending, excludes) = unpublished(host, version)?;
    if pending.is_empty() {
        println!("release: every crate already carries {version}; nothing to upload or verify.");
        return Ok(());
    }
    require_staged(&pending, version, dir)?;
    let crates: Vec<&String> = present.iter().filter(|n| n.ends_with(".crate")).collect();

    group("Every artifact carries its attestations");
    let scratch = Scratch::new("spate-release-verify")?;
    let bundle = scratch.join("bundles.intoto.jsonl");
    if join_bundles(dir, &bundle)? == 0 {
        return Err(Error::msg(format!(
            "no attestation bundle in {}; the packaged crates are unattested",
            dir.display()
        )));
    }
    let sums_path = dir.join(SUMS);
    host.forge.verify_attestation(
        &sums_path,
        Some(&bundle),
        signer_workflow,
        PROVENANCE,
        commit,
    )?;
    println!("provenance verified: {SUMS}");
    for name in crates {
        let file = dir.join(name);
        host.forge
            .verify_attestation(&file, Some(&bundle), signer_workflow, PROVENANCE, commit)?;
        host.forge
            .verify_attestation(&file, Some(&bundle), signer_workflow, CYCLONEDX, commit)?;
        println!("provenance and SBOM verified: {name}");
    }
    endgroup();

    group("This checkout packages the attested bytes");
    // The upload packages again from this checkout, so its bytes are compared
    // before any token exists.
    host.workspace.package_unverified(&excludes)?;
    for name in &pending {
        let file = host.workspace.crate_file(name, version).ok_or_else(|| {
            Error::msg(format!(
                "{name} {version} was not packaged by this checkout"
            ))
        })?;
        let local = host.workspace.sha256(&file)?;
        let staged = host
            .workspace
            .sha256(&dir.join(format!("{name}-{version}.crate")))?;
        if local != staged {
            return Err(Error::msg(format!(
                "{name} {version}: this checkout packages {local} but the attested file is\n  \
                 {staged}; the upload would send bytes no attestation covers"
            )));
        }
        println!("same bytes as attested: {name}");
    }
    endgroup();
    Ok(())
}

// ---------------------------------------------------------------------------
// upload
// ---------------------------------------------------------------------------

/// Uploads every crate the registry does not yet hold, each of which must be
/// staged in `artifacts`. The only step that holds the registry token.
///
/// No verify build runs: the build verify-built every staged crate, and
/// `verify_artifacts` showed this checkout packages the same bytes, so the
/// token's fixed 30-minute life is not spent compiling the workspace again.
pub(crate) fn upload(host: &Host<'_>, token: bool, artifacts: &Path) -> Outcome {
    if !token {
        return Err(Error::msg("upload needs CARGO_REGISTRY_TOKEN"));
    }
    let version = host.workspace.version()?;
    let (pending, excludes) = unpublished(host, version)?;
    if pending.is_empty() {
        println!("release: every crate already carries {version}; skipping the upload.");
        return Ok(());
    }
    require_staged(&pending, version, artifacts)?;
    host.workspace.publish(&excludes)
}

/// The publishable crates the registry does not yet hold at `version`, and the
/// ones it does. Read when each step runs, so a step re-run after part of the
/// release landed sees what landed.
fn unpublished(host: &Host<'_>, version: Version) -> Result<(Vec<String>, Vec<String>), Error> {
    let (mut pending, mut published) = (Vec::new(), Vec::new());
    for package in host.workspace.publishable()? {
        let at_version = match host.registry.index(&package.name)? {
            Index::Found(entries) => entries.iter().any(|e| e.vers == version.to_string()),
            Index::Missing => false,
            Index::Status(code) => {
                return Err(Error::msg(format!(
                    "the index answered {code} for {}; refusing to act on a registry the run\n  \
                     cannot read",
                    package.name
                )));
            }
        };
        if at_version {
            published.push(package.name);
        } else {
            pending.push(package.name);
        }
    }
    Ok((pending, published))
}

/// Every crate in `pending` was staged, and so attested, by the build.
fn require_staged(pending: &[String], version: Version, dir: &Path) -> Outcome {
    let missing: Vec<&str> = pending
        .iter()
        .filter(|c| !dir.join(format!("{c}-{version}.crate")).is_file())
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(Error::msg(format!(
        "{} {version} not staged in {}, so no attestation covers it; these are not\n  \
         the artifacts the run's first build staged",
        missing.join(" "),
        dir.display()
    )))
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
    artifacts: &Path,
    repo: &str,
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

    group("The registry serves the bytes this run attested");
    // The index's cksum is the sha256 of the served `.crate`, and it must equal
    // the staged file the attestations cover.
    let mut checked = 0;
    for package in &packages {
        let name = &package.name;
        let staged = artifacts.join(format!("{name}-{version}.crate"));
        if !staged.is_file() {
            continue;
        }
        let got = host.workspace.sha256(&staged)?;
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
        println!("no crate is staged; nothing to compare.");
    }
    endgroup();

    group("A consumer resolves the release");
    resolves(host, version)?;
    endgroup();

    group("Tag");
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
    endgroup();

    group("The release, a draft until every asset is on it");
    // An immutable release locks its assets and tag once published, so every
    // asset goes onto the draft first.
    let mut state = host.forge.release_state(&tag)?;
    if state == (ReleaseState::Published { immutable: false }) {
        return Err(mutable_release(&tag));
    }
    if state == ReleaseState::Missing {
        let notes = host.workspace.notes(version)?;
        if host.forge.create_release(&tag, &notes).is_err() {
            // A tag pushed moments ago can lag replication on the API side. The
            // state is read again so a create that landed is not repeated.
            (host.pause)(Duration::from_secs(10));
            if host.forge.release_state(&tag)? == ReleaseState::Missing {
                host.forge.create_release(&tag, &notes)?;
            }
        }
        state = ReleaseState::Draft;
    }
    if state == (ReleaseState::Published { immutable: true }) {
        println!("the {tag} release is already published and immutable.");
    } else {
        attach_assets(host, &tag, artifacts, checked > 0)?;
    }
    let names: Vec<String> = packages
        .iter()
        .map(|p| format!("{}-{version}.cdx.json", p.name))
        .collect();
    let locked = state == (ReleaseState::Published { immutable: true });
    require_assets(
        &host.forge.assets(&tag)?,
        &names,
        &tag,
        version,
        repo,
        locked,
    )?;
    if state == ReleaseState::Draft {
        host.forge.publish_release(&tag)?;
    }
    match host.forge.release_state(&tag)? {
        ReleaseState::Published { immutable: true } => {}
        ReleaseState::Published { immutable: false } => return Err(mutable_release(&tag)),
        other => {
            return Err(Error::msg(format!(
                "the {tag} release reads as {other:?} after publishing; re-run the failed jobs"
            )));
        }
    }
    if host.forge.verify_release(&tag).is_err() {
        // GitHub signs the release attestation as it publishes, and a read
        // straight after can precede it.
        (host.pause)(Duration::from_secs(10));
        host.forge.verify_release(&tag)?;
    }
    println!("the {tag} release is immutable and its attestation verifies.");
    endgroup();

    group("Deploy the documentation");
    // The site carries the install snippets, so it deploys after the crates
    // are live.
    host.forge.dispatch_docs()?;
    println!("docs deploy dispatched; docs.rs builds on its own and lags the publish.");
    endgroup();
    Ok(())
}

/// Uploads the SBOMs, and, when crates are staged, the checksum list and the
/// joined bundle.
///
/// `--clobber` for the SBOMs, whose generation in CI is byte-stable, so a
/// resumed run completes the set. The other two are kept when the release
/// already lists them as uploaded, since every attempt stages the same set.
/// Otherwise they go up with `--clobber`, which replaces an asset an earlier
/// upload left broken under the same name.
fn attach_assets(host: &Host<'_>, tag: &str, artifacts: &Path, attested: bool) -> Outcome {
    let mut files: Vec<PathBuf> = std::fs::read_dir(artifacts)
        .map_err(|e| Error::msg(format!("{}: {e}", artifacts.display())))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".cdx.json"))
        .collect();
    files.sort();
    host.forge.upload(tag, &files, true)?;
    if !attested {
        return Ok(());
    }
    let scratch = Scratch::new("spate-release-assets")?;
    let bundle = scratch.join(&format!("spate-{tag}.intoto.jsonl"));
    let mut once = vec![artifacts.join(SUMS)];
    if join_bundles(artifacts, &bundle)? > 0 {
        once.push(bundle);
    }
    let uploaded = host.forge.assets(tag)?;
    for asset in once {
        let name = asset
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if uploaded.contains(&name) {
            println!("{name}: an earlier attempt's asset is kept.");
        } else {
            host.forge.upload(tag, &[asset], true)?;
        }
    }
    Ok(())
}

/// The error for a release published mutable, naming the steps that replace it.
fn mutable_release(tag: &str) -> Error {
    Error::msg(format!(
        "the {tag} release is published but not immutable, and a published release\n  \
         never becomes immutable. Turn on immutable releases in the repository\n  \
         settings, delete the release and keep its tag (`gh release delete {tag}`,\n  \
         without --cleanup-tag), then re-run the failed jobs. Confirm first that\n  \
         `gh release view {tag} --json isImmutable` reads false: an immutable\n  \
         release, once deleted, retires its tag for good."
    ))
}

/// The release carries `SHA256SUMS`, an attestation bundle, and every SBOM in
/// `sboms`. A `locked` release is published and immutable, so a gap in it is
/// reported as permanent.
fn require_assets(
    assets: &[String],
    sboms: &[String],
    tag: &str,
    version: Version,
    repo: &str,
    locked: bool,
) -> Outcome {
    let missing: Vec<&str> = sboms
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(SUMS))
        .filter(|n| !assets.iter().any(|a| a == n))
        .collect();
    let bundled = assets.iter().any(|a| a.ends_with(".intoto.jsonl"));
    if locked && (!missing.is_empty() || !bundled) {
        let mut lacks = missing;
        if !bundled {
            lacks.push("an attestation bundle");
        }
        return Err(Error::msg(format!(
            "the {tag} release is published and immutable but lacks {}. GitHub\n  \
             locks its assets, so only a new version can carry them.",
            lacks.join(" and ")
        )));
    }
    if !missing.is_empty() {
        return Err(Error::msg(format!(
            "the {tag} release lacks {}; an asset upload failed. Re-run the failed jobs.",
            missing.join(" ")
        )));
    }
    if !bundled {
        return Err(Error::msg(format!(
            "the {tag} release carries no attestation bundle. Recover each crate's from the\n  \
             attestation store: download the published .crate from\n  \
             https://static.crates.io/crates/<name>/<name>-{version}.crate, run\n  \
             'gh attestation download <file> --repo {repo}', and upload the joined\n  \
             bundles as spate-{tag}.intoto.jsonl while the release is a draft."
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// The workflow that signs every artifact attestation.
pub(crate) fn build_signer(repo: &str) -> String {
    format!("{repo}/.github/workflows/release-build.yml")
}

/// Judges a published release from what anyone can read, without trusting the
/// run that made it. Runs from a checkout at `version`, whose members are the
/// crates it checks.
pub(crate) fn verify(host: &Host<'_>, version: Version, repo: &str) -> Outcome {
    let tag = format!("v{version}");
    let checkout = host.workspace.version()?;
    if checkout != version {
        return Err(Error::msg(format!(
            "this checkout is at {checkout}; verify {tag} from a checkout of {tag}"
        )));
    }
    let packages = host.workspace.publishable()?;

    group("The tag");
    let commit = host
        .git
        .remote_tag(&tag)?
        .ok_or_else(|| Error::msg(format!("{tag} is not tagged in {repo}")))?;
    println!("{tag} names {commit}.");
    endgroup();

    group("Every crate came from the tagged commit");
    for package in &packages {
        let sha = host.registry.trustpub_sha(&package.name, version)?;
        if sha != commit {
            return Err(Error::msg(format!(
                "{} {version} was published from '{sha}', not the tagged {commit}",
                package.name
            )));
        }
        (host.pause)(API_INTERVAL);
    }
    println!("{} crate(s) published from {commit}.", packages.len());
    endgroup();

    group("The bytes the registry serves are attested");
    let scratch = Scratch::new("spate-release-verify")?;
    let signer = build_signer(repo);
    for package in &packages {
        let file = host
            .registry
            .download(&package.name, version, scratch.dir())?;
        let got = host.workspace.sha256(&file)?;
        let want = served_cksum(host, &package.name, version)?;
        if got != want {
            return Err(Error::msg(format!(
                "{} {version}: the download is {got} but the index lists {want}",
                package.name
            )));
        }
        for predicate in [PROVENANCE, CYCLONEDX] {
            host.forge
                .verify_attestation(&file, None, &signer, predicate, &commit)?;
        }
        println!("provenance and SBOM verified: {} {version}", package.name);
    }
    endgroup();

    group("The GitHub release");
    let state = host.forge.release_state(&tag)?;
    if state != (ReleaseState::Published { immutable: true }) {
        return Err(Error::msg(format!(
            "the {tag} release is {state:?}, not published and immutable"
        )));
    }
    host.forge.verify_release(&tag)?;
    let names: Vec<String> = packages
        .iter()
        .map(|p| format!("{}-{version}.cdx.json", p.name))
        .collect();
    require_assets(&host.forge.assets(&tag)?, &names, &tag, version, repo, true)?;
    println!("the {tag} release is immutable, attested and complete.");
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
