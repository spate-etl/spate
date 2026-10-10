use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::*;
use crate::release::io::{
    DryForge, DryGit, Forge, Git, Index, IndexEntry, Package, Pull, Registry, Resolution, Workspace,
};

const SHA: &str = "1111111111111111111111111111111111111111";
const OTHER: &str = "2222222222222222222222222222222222222222";

fn v(s: &str) -> Version {
    Version::parse(s).unwrap()
}

fn fail(what: &str) -> Error {
    Error::msg(format!("injected: {what}"))
}

/// Every write the fakes saw, in order.
#[derive(Default)]
struct Log(RefCell<Vec<String>>);

impl Log {
    fn push(&self, entry: String) {
        self.0.borrow_mut().push(entry);
    }

    fn entries(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    fn has(&self, prefix: &str) -> bool {
        self.0.borrow().iter().any(|e| e.starts_with(prefix))
    }
}

struct FakeGit<'a> {
    log: &'a Log,
    clean: bool,
    last_tag: String,
    subject: String,
    /// Tags origin holds, by name.
    remote: RefCell<BTreeMap<String, String>>,
    /// A push that reaches origin and then reports failure.
    push_lands_then_fails: bool,
    push_fails: bool,
}

impl<'a> FakeGit<'a> {
    fn new(log: &'a Log) -> Self {
        Self {
            log,
            clean: true,
            last_tag: "v0.2.0".to_owned(),
            subject: "release: v0.3.0 (#42)".to_owned(),
            remote: RefCell::default(),
            push_lands_then_fails: false,
            push_fails: false,
        }
    }
}

impl Git for FakeGit<'_> {
    fn is_clean(&self) -> Result<bool, Error> {
        Ok(self.clean)
    }
    fn last_tag(&self) -> Result<String, Error> {
        Ok(self.last_tag.clone())
    }
    fn subject(&self) -> Result<String, Error> {
        Ok(self.subject.clone())
    }
    fn short_head(&self) -> Result<String, Error> {
        Ok(SHA[..7].to_owned())
    }
    fn commit_all(&self, subject: &str, _body: &str) -> Outcome {
        self.log.push(format!("commit {subject}"));
        Ok(())
    }
    fn remote_tag(&self, tag: &str) -> Result<Option<String>, Error> {
        Ok(self.remote.borrow().get(tag).cloned())
    }
    fn tag(&self, name: &str, commit: &str) -> Outcome {
        self.log.push(format!("tag {name} {commit}"));
        Ok(())
    }
    fn push(&self, refspec: &str, force: bool) -> Outcome {
        if self.push_fails {
            return Err(fail("push"));
        }
        self.log.push(format!("push {refspec} force={force}"));
        if let Some(tag) = refspec.strip_prefix("refs/tags/") {
            self.remote
                .borrow_mut()
                .insert(tag.to_owned(), SHA.to_owned());
        }
        if self.push_lands_then_fails {
            return Err(fail("push after landing"));
        }
        Ok(())
    }
}

#[derive(Default)]
struct FakeForge<'a> {
    log: Option<&'a Log>,
    pulls: Vec<Pull>,
    release: RefCell<bool>,
    assets: RefCell<Vec<String>>,
    /// Asset uploads refused, as an immutable or already-carrying release does.
    refuse_bundle: bool,
    /// The one write that fails: `close`, `create`, `merge` or `upload`.
    fails: Option<&'static str>,
    /// Pull request listings refused, as on a clone with no default repository.
    reads_fail: bool,
    create_release_failures: RefCell<u32>,
}

impl<'a> FakeForge<'a> {
    fn new(log: &'a Log) -> Self {
        Self {
            log: Some(log),
            ..Self::default()
        }
    }

    fn log(&self, entry: String) {
        self.log.unwrap().push(entry);
    }

    fn check(&self, write: &str) -> Outcome {
        if self.fails == Some(write) {
            return Err(fail(write));
        }
        Ok(())
    }
}

impl Forge for FakeForge<'_> {
    fn open_pulls(&self) -> Result<Vec<Pull>, Error> {
        if self.reads_fail {
            return Err(fail("list"));
        }
        Ok(self.pulls.clone())
    }
    fn pull_for_head(&self, head: &str) -> Result<Option<u64>, Error> {
        if self.reads_fail {
            return Err(fail("list"));
        }
        Ok(self
            .pulls
            .iter()
            .find(|p| !p.cross_repository && p.head == head)
            .map(|p| p.number))
    }
    fn close_pull(&self, number: u64, _comment: &str) -> Outcome {
        self.check("close")?;
        self.log(format!("close #{number}"));
        Ok(())
    }
    fn create_pull(
        &self,
        title: &str,
        head: &str,
        label: &str,
        _body: &str,
    ) -> Result<Option<u64>, Error> {
        self.check("create")?;
        self.log(format!("create pull {title} {head} {label}"));
        Ok(Some(99))
    }
    fn auto_merge(&self, number: u64) -> Outcome {
        self.check("merge")?;
        self.log(format!("auto-merge #{number}"));
        Ok(())
    }
    fn release_exists(&self, _tag: &str) -> Result<bool, Error> {
        Ok(*self.release.borrow())
    }
    fn create_release(&self, tag: &str, _notes: &str) -> Outcome {
        let mut left = self.create_release_failures.borrow_mut();
        if *left > 0 {
            *left -= 1;
            return Err(fail("create release"));
        }
        self.log(format!("release {tag}"));
        *self.release.borrow_mut() = true;
        Ok(())
    }
    fn upload(&self, tag: &str, files: &[PathBuf], clobber: bool) -> Outcome {
        for f in files {
            let name = f.file_name().unwrap().to_string_lossy().into_owned();
            if self.refuse_bundle && name.ends_with(".intoto.jsonl") {
                return Err(fail("upload bundle"));
            }
            self.check("upload")?;
            self.log(format!("upload {tag} {name} clobber={clobber}"));
            self.assets.borrow_mut().push(name);
        }
        Ok(())
    }
    fn assets(&self, _tag: &str) -> Result<Vec<String>, Error> {
        Ok(self.assets.borrow().clone())
    }
    fn dispatch_docs(&self) -> Outcome {
        self.log("dispatch docs".to_owned());
        Ok(())
    }
}

/// One published version: its number, its cksum, and the commit it came from.
type Published = (String, String, String);

/// A tweak to the fakes before an `assemble` guard case runs.
type Setup = fn(&mut FakeGit<'_>, &mut FakeWorkspace<'_>);

/// A registry holding, per crate, the versions with their cksum and the commit
/// they were published from.
#[derive(Default)]
struct FakeRegistry {
    crates: RefCell<BTreeMap<String, Vec<Published>>>,
    /// Index reads per crate that still answer without the newest version.
    lag: RefCell<BTreeMap<String, u32>>,
    status: Option<String>,
}

impl FakeRegistry {
    fn with(crates: &[&str]) -> Self {
        let registry = Self::default();
        for c in crates {
            registry.publish(c, "0.2.0", "old", "e");
        }
        registry
    }

    fn publish(&self, krate: &str, vers: &str, cksum: &str, sha: &str) {
        self.crates
            .borrow_mut()
            .entry(krate.to_owned())
            .or_default()
            .push((vers.to_owned(), cksum.to_owned(), sha.to_owned()));
    }
}

impl Registry for FakeRegistry {
    fn index(&self, krate: &str) -> Result<Index, Error> {
        if let Some(code) = &self.status {
            return Ok(Index::Status(code.clone()));
        }
        let crates = self.crates.borrow();
        let Some(versions) = crates.get(krate) else {
            return Ok(Index::Missing);
        };
        let mut lag = self.lag.borrow_mut();
        let lagging = lag.get_mut(krate).is_some_and(|n| {
            let hit = *n > 0;
            *n = n.saturating_sub(1);
            hit
        });
        let shown = if lagging {
            &versions[..versions.len() - 1]
        } else {
            &versions[..]
        };
        Ok(Index::Found(
            shown
                .iter()
                .map(|(vers, cksum, _)| IndexEntry {
                    vers: vers.clone(),
                    cksum: cksum.clone(),
                })
                .collect(),
        ))
    }

    fn trustpub_sha(&self, krate: &str, version: Version) -> Result<String, Error> {
        Ok(self
            .crates
            .borrow()
            .get(krate)
            .and_then(|vs| vs.iter().find(|(v, _, _)| *v == version.to_string()))
            .map_or_else(|| "null".to_owned(), |(_, _, sha)| sha.clone()))
    }
}

struct FakeWorkspace<'a> {
    log: &'a Log,
    crates: Vec<String>,
    version: Version,
    derived: Version,
    /// The crates whose `.crate` this run packaged, with its sha256.
    packaged: RefCell<BTreeMap<String, String>>,
    registry: Option<&'a FakeRegistry>,
    /// Crates the upload publishes before it fails.
    upload_stops_after: Option<usize>,
    missing_metadata: Vec<String>,
    resolve_failures: RefCell<u32>,
}

impl<'a> FakeWorkspace<'a> {
    fn new(log: &'a Log, crates: &[&str]) -> Self {
        Self {
            log,
            crates: crates.iter().map(|c| (*c).to_owned()).collect(),
            version: v("0.3.0"),
            derived: v("0.3.0"),
            packaged: RefCell::default(),
            registry: None,
            upload_stops_after: None,
            missing_metadata: Vec::new(),
            resolve_failures: RefCell::new(0),
        }
    }
}

impl Workspace for FakeWorkspace<'_> {
    fn publishable(&self) -> Result<Vec<Package>, Error> {
        Ok(self
            .crates
            .iter()
            .map(|c| Package {
                name: c.clone(),
                dir: PathBuf::from(c),
            })
            .collect())
    }
    fn missing_metadata(&self) -> Result<Vec<String>, Error> {
        Ok(self.missing_metadata.clone())
    }
    fn version(&self) -> Result<Version, Error> {
        Ok(self.version)
    }
    fn derive(&self) -> Result<Version, Error> {
        Ok(self.derived)
    }
    fn generate(&self, version: Version) -> Outcome {
        self.log.push(format!("generate {version}"));
        Ok(())
    }
    fn notes(&self, version: Version) -> Result<String, Error> {
        Ok(format!("notes for {version}"))
    }
    fn package(&self, excludes: &[String]) -> Outcome {
        self.log
            .push(format!("package excluding [{}]", excludes.join(" ")));
        for c in self.crates.iter().filter(|c| !excludes.contains(c)) {
            self.packaged
                .borrow_mut()
                .insert(c.clone(), format!("sum-{c}"));
        }
        Ok(())
    }
    fn publish(&self, excludes: &[String]) -> Outcome {
        self.log
            .push(format!("publish excluding [{}]", excludes.join(" ")));
        let registry = self.registry.unwrap();
        for (n, c) in self
            .crates
            .iter()
            .filter(|c| !excludes.contains(c))
            .enumerate()
        {
            if self.upload_stops_after == Some(n) {
                return Err(fail("upload midway"));
            }
            registry.publish(c, "0.3.0", &format!("sum-{c}"), SHA);
        }
        Ok(())
    }
    fn packaged_sha256(&self, krate: &str, _version: Version) -> Result<Option<String>, Error> {
        Ok(self.packaged.borrow().get(krate).cloned())
    }
    fn resolve(&self, _version: Version) -> Result<Resolution, Error> {
        let mut left = self.resolve_failures.borrow_mut();
        if *left > 0 {
            *left -= 1;
            return Ok(Resolution::Failed("no matching package".to_owned()));
        }
        Ok(Resolution::Resolved(120))
    }
    fn sboms(&self, version: Version, out: &Path) -> Outcome {
        for c in &self.crates {
            std::fs::write(out.join(format!("{c}-{version}.cdx.json")), "{}").unwrap();
        }
        Ok(())
    }
}

/// The pauses a run asked for.
#[derive(Default)]
struct Pauses(RefCell<Vec<Duration>>);

impl Pauses {
    fn of(&self, d: Duration) -> usize {
        self.0.borrow().iter().filter(|p| **p == d).count()
    }
}

macro_rules! host {
    ($git:expr, $forge:expr, $registry:expr, $workspace:expr, $pauses:expr) => {
        Host {
            git: $git,
            forge: $forge,
            registry: $registry,
            workspace: $workspace,
            pause: &|d| $pauses.0.borrow_mut().push(d),
        }
    };
}

const CRATES: [&str; 3] = ["spate-core", "spate-kafka", "spate"];

/// A release subject names its version, with or without the squash suffix;
/// near misses do not, so the publish never fires on them.
#[test]
fn the_release_subject_names_its_version() {
    let table = [
        ("release: v0.3.0", Some("0.3.0")),
        ("release: v0.3.0 (#309)", Some("0.3.0")),
        ("release: v10.20.30 (#1)", Some("10.20.30")),
        ("release: v0.3", None),
        ("release: v0.3.0 and a trailer", None),
        ("release: v0.3.0 (#)", None),
        ("release: v0.3.0 (#3a)", None),
        ("release: verify the tag", None),
        ("workspace: release v0.3.0", None),
        ("chore: release v0.3.0", None),
        ("docs: mention release: v0.3.0 in a page", None),
    ];
    for (subject, want) in table {
        assert_eq!(version_from_subject(subject), want.map(v), "{subject}");
    }
}

/// Only a same-repository branch of the exact release shape is a release
/// branch.
#[test]
fn a_release_branch_has_the_exact_shape() {
    assert!(is_release_branch("release/v0.3.0"));
    assert!(!is_release_branch("release/v2-planning"));
    assert!(!is_release_branch("release/v0.3"));
    assert!(!is_release_branch("feature/release/v0.3.0"));
}

/// The happy path commits, pushes the branch, opens the pull request and
/// enables auto-merge, closing only same-repository release branches at
/// another version.
#[test]
fn assemble_supersedes_only_other_release_branches() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.pulls = vec![
        Pull {
            number: 1,
            head: "release/v0.2.9".into(),
            cross_repository: false,
        },
        Pull {
            number: 2,
            head: "release/v0.2.9".into(),
            cross_repository: true,
        },
        Pull {
            number: 3,
            head: "release/v2-planning".into(),
            cross_repository: false,
        },
        Pull {
            number: 4,
            head: "core/a-change".into(),
            cross_repository: false,
        },
    ];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    assert_eq!(
        log.entries(),
        [
            "generate 0.3.0",
            "commit release: v0.3.0",
            "close #1",
            "push HEAD:refs/heads/release/v0.3.0 force=true",
            "create pull release: v0.3.0 release/v0.3.0 release",
            "auto-merge #99",
        ]
    );
}

/// Re-dispatching a version reuses its open pull request.
#[test]
fn assemble_reuses_the_open_pull_request() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let mut forge = FakeForge::new(&log);
    forge.pulls = vec![Pull {
        number: 7,
        head: "release/v0.3.0".into(),
        cross_repository: false,
    }];
    let registry = FakeRegistry::default();
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.version = v("0.2.0");
    let pauses = Pauses::default();
    assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
    assert!(!log.has("create pull"));
    assert!(log.has("auto-merge #7"));
}

/// Every guard refuses before anything is generated.
#[test]
fn assemble_guards_refuse_before_generating() {
    let cases: [(&str, Setup, &str); 6] = [
        ("dirty", |g, _| g.clean = false, "not clean"),
        ("not a version", |_, _| {}, "is not X.Y.Z"),
        (
            "same version",
            |_, w| w.version = v("0.3.0"),
            "already at 0.3.0",
        ),
        (
            "half finished",
            |g, _| g.last_tag = "v0.1.0".into(),
            "half-finished",
        ),
        (
            "tagged",
            |g, _| {
                g.remote.borrow_mut().insert("v0.3.0".into(), OTHER.into());
            },
            "already tagged",
        ),
        (
            "derive disagrees",
            |_, w| w.derived = v("0.2.1"),
            "derives 0.2.1",
        ),
    ];
    for (name, setup, want) in cases {
        let log = Log::default();
        let mut git = FakeGit::new(&log);
        let forge = FakeForge::new(&log);
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        setup(&mut git, &mut workspace);
        let input = if name == "not a version" {
            "0.3"
        } else {
            "0.3.0"
        };
        let pauses = Pauses::default();
        let err = assemble(&host!(&git, &forge, &registry, &workspace, pauses), input).unwrap_err();
        assert!(err.message.contains(want), "{name}: {}", err.message);
        assert!(log.entries().is_empty(), "{name}: {:?}", log.entries());
    }
}

/// A fresh release packages every crate and excludes none.
#[test]
fn prepare_selects_every_crate_on_a_fresh_release() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let prepared = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap();
    assert_eq!(prepared.pending, CRATES);
    assert!(prepared.excludes.is_empty());
    assert_eq!(log.entries(), ["package excluding []"]);
}

/// An upload that fails midway is resumed by a re-run that excludes what
/// landed, verifies it came from this commit, and publishes the rest.
#[test]
fn an_upload_that_fails_midway_resumes_from_the_registry() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.registry = Some(&registry);
    workspace.upload_stops_after = Some(1);
    let pauses = Pauses::default();
    let host = host!(&git, &forge, &registry, &workspace, pauses);

    let first = prepare(&host, Some(SHA)).unwrap();
    assert!(upload(&host, true, 3, &first.excludes).is_err());

    let second = prepare(&host, Some(SHA)).unwrap();
    assert_eq!(second.excludes, ["spate-core"]);
    assert_eq!(second.pending, ["spate-kafka", "spate"]);
    assert_eq!(
        pauses.of(API_INTERVAL),
        1,
        "one trustpub read for the excluded crate"
    );

    workspace.upload_stops_after = None;
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    upload(&host, true, 2, &second.excludes).unwrap();
    for c in CRATES {
        assert_eq!(registry.trustpub_sha(c, v("0.3.0")).unwrap(), SHA, "{c}");
    }
}

/// A crate already at the version from another commit means the release is
/// split across trees, and nothing is packaged.
#[test]
fn prepare_refuses_a_release_split_across_trees() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    registry.publish("spate-core", "0.3.0", "x", OTHER);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    let err = prepare(&host, Some(SHA)).unwrap_err();
    assert!(
        err.message.contains("split across trees"),
        "{}",
        err.message
    );
    let err = prepare(&host, None).unwrap_err();
    assert!(err.message.contains("no EXPECTED_SHA"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// A crate the registry has never heard of, and an index that cannot be read,
/// each stop the run.
#[test]
fn prepare_refuses_an_unknown_crate_or_an_unreadable_index() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();

    let registry = FakeRegistry::with(&CRATES[..2]);
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("not on the registry at all"),
        "{}",
        err.message
    );

    let mut registry = FakeRegistry::with(&CRATES);
    registry.status = Some("503".into());
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(err.message.contains("answered 503"), "{}", err.message);
}

/// A head that is not a release commit, or disagrees with the manifest, is not
/// published.
#[test]
fn prepare_requires_the_release_commit() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();

    git.subject = "core: a change".into();
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("not a release commit"),
        "{}",
        err.message
    );

    git.subject = "release: v0.3.0".into();
    workspace.version = v("0.2.0");
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(
        err.message.contains("Cargo.toml says 0.2.0"),
        "{}",
        err.message
    );
}

/// A finished registry state: every crate at 0.3.0 from `SHA`, packaged by
/// this run with matching bytes.
fn published(log: &Log) -> (FakeRegistry, FakeWorkspace<'_>) {
    let registry = FakeRegistry::with(&CRATES);
    for c in CRATES {
        registry.publish(c, "0.3.0", &format!("sum-{c}"), SHA);
    }
    let workspace = FakeWorkspace::new(log, &CRATES);
    workspace.package(&[]).unwrap();
    log.0.borrow_mut().clear();
    (registry, workspace)
}

fn bundle() -> (Scratch, PathBuf) {
    let scratch = Scratch::new("spate-xtask-release-bundle").unwrap();
    let path = scratch.join("attestation.jsonl");
    std::fs::write(&path, "{}").unwrap();
    (scratch, path)
}

/// The happy path tags, releases with every asset, and deploys the docs.
#[test]
fn finish_tags_releases_and_deploys() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    assert_eq!(
        log.entries(),
        [
            format!("tag v0.3.0 {SHA}"),
            "push refs/tags/v0.3.0 force=false".to_owned(),
            "release v0.3.0".to_owned(),
            "upload v0.3.0 spate-core-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 spate-kafka-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 spate-0.3.0.cdx.json clobber=true".to_owned(),
            "upload v0.3.0 spate-v0.3.0-provenance.intoto.jsonl clobber=false".to_owned(),
            "dispatch docs".to_owned(),
        ]
    );
    assert_eq!(pauses.of(API_INTERVAL), CRATES.len());
}

/// A tag push that reaches origin and then reports failure stops the run, and
/// the re-run finds the tag on this commit and carries on without tagging
/// again.
#[test]
fn a_tag_push_that_fails_after_landing_resumes() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    git.push_lands_then_fails = true;
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    assert!(
        finish(
            &host!(&git, &forge, &registry, &workspace, pauses),
            v("0.3.0"),
            SHA,
            Some(&bundle)
        )
        .is_err()
    );
    assert!(!log.has("release"));

    git.push_lands_then_fails = false;
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    let tags = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("tag "))
        .count();
    assert_eq!(tags, 1, "{:?}", log.entries());
    let pushes = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("push refs/tags/"))
        .count();
    assert_eq!(pushes, 1, "{:?}", log.entries());
    assert!(log.has("release v0.3.0"));
}

/// A tag on origin naming another commit is never moved.
#[test]
fn finish_refuses_a_tag_on_another_commit() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote
        .borrow_mut()
        .insert("v0.3.0".into(), OTHER.into());
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(err.message.contains("points at"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// A crate the registry attributes to another commit, and served bytes that
/// differ from the packaged ones, each stop the run before it tags.
#[test]
fn finish_refuses_a_foreign_commit_or_foreign_bytes() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let pauses = Pauses::default();

    let (registry, workspace) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate")
        .unwrap()
        .last_mut()
        .unwrap()
        .2 = OTHER.into();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(err.message.contains("trustpub sha"), "{}", err.message);

    let (registry, workspace) = published(&log);
    registry
        .crates
        .borrow_mut()
        .get_mut("spate-kafka")
        .unwrap()
        .last_mut()
        .unwrap()
        .1 = "tampered".into();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(
        err.message.contains("registry serves tampered"),
        "{}",
        err.message
    );
    assert!(!log.has("tag"));
}

/// An index lagging the upload is waited out, and so is a consumer resolution
/// the CDN has not caught up with.
#[test]
fn finish_waits_out_registry_lag() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    registry.lag.borrow_mut().insert("spate-kafka".into(), 2);
    *workspace.resolve_failures.borrow_mut() = 3;
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    assert_eq!(
        pauses.of(LAG_INTERVAL),
        5,
        "two index waits and three resolution waits"
    );
}

/// Lag that outlasts every attempt fails with cargo's answer.
#[test]
fn finish_gives_up_on_a_release_that_never_resolves() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    *workspace.resolve_failures.borrow_mut() = 5;
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(err.message.contains("cannot resolve"), "{}", err.message);
    assert_eq!(pauses.of(LAG_INTERVAL), 5);
    assert!(!log.has("tag"));
}

/// An index that never serves the version stops `finish` after the full wait,
/// before tagging.
#[test]
fn finish_gives_up_on_an_index_that_never_serves() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    registry.lag.borrow_mut().insert("spate-kafka".into(), 5);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(err.message.contains("never served"), "{}", err.message);
    assert_eq!(pauses.of(LAG_INTERVAL), 5);
    assert!(!log.has("tag"));
}

/// An index status other than 200 or 404 stops `finish` at once and names the
/// code.
#[test]
fn finish_stops_on_an_index_status() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (mut registry, workspace) = published(&log);
    registry.status = Some("403".into());
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert_eq!(pauses.of(LAG_INTERVAL), 0, "a 403 is waited out as lag");
    assert!(err.message.contains("403"), "{}", err.message);
    assert!(!log.has("tag"));
}

/// A rehearsed `assemble` commits and makes no push, close, pull request or
/// auto-merge, whether the forge's listings answer or are refused.
#[test]
fn a_dry_assemble_writes_nothing_outside_the_tree() {
    for reads_fail in [false, true] {
        let log = Log::default();
        let git = FakeGit::new(&log);
        let mut forge = FakeForge::new(&log);
        forge.reads_fail = reads_fail;
        forge.pulls = vec![
            Pull {
                number: 1,
                head: "release/v0.2.9".into(),
                cross_repository: false,
            },
            Pull {
                number: 7,
                head: "release/v0.3.0".into(),
                cross_repository: false,
            },
        ];
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let pauses = Pauses::default();
        let (git, forge) = (DryGit(&git), DryForge(&forge));
        assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").unwrap();
        assert_eq!(
            log.entries(),
            ["generate 0.3.0", "commit release: v0.3.0"],
            "reads_fail={reads_fail}"
        );
    }
}

/// A rehearsal's git passes the commit through and never tags or pushes.
#[test]
fn a_dry_git_never_tags_or_pushes() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let dry = DryGit(&git);
    dry.tag("v0.3.0", SHA).unwrap();
    dry.push("refs/tags/v0.3.0", false).unwrap();
    dry.push("HEAD:refs/heads/release/v0.3.0", true).unwrap();
    dry.commit_all("release: v0.3.0", "").unwrap();
    assert_eq!(log.entries(), ["commit release: v0.3.0"]);
}

/// A release created moments after its tag is retried once.
#[test]
fn finish_retries_the_release_once() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    *forge.create_release_failures.borrow_mut() = 1;
    let (registry, workspace) = published(&log);
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    assert!(log.has("release v0.3.0"));
    assert_eq!(pauses.of(Duration::from_secs(10)), 1);
}

/// A release with no provenance bundle among its assets fails with the
/// recovery steps, and a release already carrying one keeps it.
#[test]
fn finish_requires_a_provenance_bundle() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let pauses = Pauses::default();
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        None,
    )
    .unwrap_err();
    assert!(
        err.message.contains("gh attestation download"),
        "{}",
        err.message
    );

    let mut forge = FakeForge::new(&log);
    forge.refuse_bundle = true;
    forge
        .assets
        .borrow_mut()
        .push("spate-v0.3.0-provenance.intoto.jsonl".into());
    let (_dir, bundle) = bundle();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
}

/// A trailing space after the version is not a release subject.
#[test]
fn a_release_subject_carries_nothing_after_its_suffix() {
    assert_eq!(version_from_subject("release: v0.3.0 "), None);
    assert_eq!(version_from_subject("release: v0.3.0 (#1) "), None);
}

/// Each failing write in the pull request step stops the step there.
#[test]
fn assemble_stops_at_the_first_failing_write() {
    let cases: [(&str, Option<&'static str>, bool, &[&str]); 4] = [
        (
            "close",
            Some("close"),
            false,
            &["generate 0.3.0", "commit release: v0.3.0"],
        ),
        (
            "push",
            None,
            true,
            &["generate 0.3.0", "commit release: v0.3.0", "close #1"],
        ),
        (
            "create",
            Some("create"),
            false,
            &[
                "generate 0.3.0",
                "commit release: v0.3.0",
                "close #1",
                "push HEAD:refs/heads/release/v0.3.0 force=true",
            ],
        ),
        (
            "merge",
            Some("merge"),
            false,
            &[
                "generate 0.3.0",
                "commit release: v0.3.0",
                "close #1",
                "push HEAD:refs/heads/release/v0.3.0 force=true",
                "create pull release: v0.3.0 release/v0.3.0 release",
            ],
        ),
    ];
    for (name, forge_fails, push_fails, want) in cases {
        let log = Log::default();
        let mut git = FakeGit::new(&log);
        git.push_fails = push_fails;
        let mut forge = FakeForge::new(&log);
        forge.fails = forge_fails;
        forge.pulls = vec![Pull {
            number: 1,
            head: "release/v0.2.9".into(),
            cross_repository: false,
        }];
        let registry = FakeRegistry::default();
        let mut workspace = FakeWorkspace::new(&log, &CRATES);
        workspace.version = v("0.2.0");
        let pauses = Pauses::default();
        assert!(
            assemble(&host!(&git, &forge, &registry, &workspace, pauses), "0.3.0").is_err(),
            "{name}"
        );
        assert_eq!(log.entries(), want, "{name}");
    }
}

/// A publishable crate missing its description or license stops `prepare`
/// before anything is packaged.
#[test]
fn prepare_refuses_missing_metadata() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let mut workspace = FakeWorkspace::new(&log, &CRATES);
    workspace.missing_metadata = vec!["spate-kafka".into()];
    let pauses = Pauses::default();
    let err = prepare(
        &host!(&git, &forge, &registry, &workspace, pauses),
        Some(SHA),
    )
    .unwrap_err();
    assert!(err.message.contains("in: spate-kafka"), "{}", err.message);
    assert!(log.entries().is_empty());
}

/// The upload refuses to run without a token and skips when nothing is
/// pending.
#[test]
fn upload_needs_a_token_and_skips_an_empty_set() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    let forge = FakeForge::new(&log);
    let registry = FakeRegistry::with(&CRATES);
    let workspace = FakeWorkspace::new(&log, &CRATES);
    let pauses = Pauses::default();
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    assert!(upload(&host, false, 0, &[]).is_err());
    upload(&host, true, 0, &[]).unwrap();
    assert!(log.entries().is_empty());
}

/// `prepare`'s outputs are appended to the file, after whatever it held.
#[test]
fn the_outputs_append_three_keys() {
    let scratch = Scratch::new("spate-xtask-release-outputs").unwrap();
    let path = scratch.join("output");
    std::fs::write(&path, "earlier=1\n").unwrap();
    let prepared = Prepared {
        version: v("0.3.0"),
        pending: vec!["spate".into()],
        excludes: vec!["spate-core".into(), "spate-kafka".into()],
    };
    append_outputs(&path, &prepared).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "earlier=1\nversion=0.3.0\nexcludes=spate-core spate-kafka\npending=1\n"
    );
}

/// A tag push that fails before reaching origin is retried by the re-run,
/// which tags again.
#[test]
fn a_tag_push_that_never_landed_is_retried() {
    let log = Log::default();
    let mut git = FakeGit::new(&log);
    git.push_fails = true;
    let forge = FakeForge::new(&log);
    let (registry, workspace) = published(&log);
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    assert!(
        finish(
            &host!(&git, &forge, &registry, &workspace, pauses),
            v("0.3.0"),
            SHA,
            Some(&bundle)
        )
        .is_err()
    );
    git.push_fails = false;
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    let tags = log
        .entries()
        .iter()
        .filter(|e| e.starts_with("tag "))
        .count();
    assert_eq!(tags, 2, "{:?}", log.entries());
}

/// A re-run finding the release already made uploads its assets and deploys
/// without creating it again; a failed asset upload stops before the deploy.
#[test]
fn finish_completes_an_existing_release_and_stops_on_a_failed_upload() {
    let log = Log::default();
    let git = FakeGit::new(&log);
    git.remote.borrow_mut().insert("v0.3.0".into(), SHA.into());
    let forge = FakeForge::new(&log);
    *forge.release.borrow_mut() = true;
    let (registry, workspace) = published(&log);
    let (_dir, bundle) = bundle();
    let pauses = Pauses::default();
    finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    )
    .unwrap();
    assert!(
        !log.has("release ") && !log.has("tag "),
        "{:?}",
        log.entries()
    );
    assert!(log.has("dispatch docs"));

    let log = Log::default();
    let mut forge = FakeForge::new(&log);
    forge.fails = Some("upload");
    let err = finish(
        &host!(&git, &forge, &registry, &workspace, pauses),
        v("0.3.0"),
        SHA,
        Some(&bundle),
    );
    assert!(err.is_err());
    assert!(!log.has("dispatch docs"));
}
