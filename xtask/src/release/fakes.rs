//! The fakes the release tests run the sequence and the milestone step over:
//! every collaborator a [`Host`] holds, recording each write in a [`Log`].

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::io::{
    Forge, Git, Index, IndexEntry, Issue, Package, Pull, Registry, ReleaseState, Resolution,
    Workspace,
};
use super::sequence::CYCLONEDX;
use super::version::Version;
use crate::checks::scratch::Scratch;
use crate::run::{Error, Outcome};

pub(crate) const SHA: &str = "1111111111111111111111111111111111111111";
pub(crate) const OTHER: &str = "2222222222222222222222222222222222222222";

pub(crate) fn v(s: &str) -> Version {
    Version::parse(s).unwrap()
}

pub(crate) fn fail(what: &str) -> Error {
    Error::msg(format!("injected: {what}"))
}

/// Every write the fakes saw, in order.
#[derive(Default)]
pub(crate) struct Log(pub(crate) RefCell<Vec<String>>);

impl Log {
    pub(crate) fn push(&self, entry: String) {
        self.0.borrow_mut().push(entry);
    }

    pub(crate) fn entries(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    pub(crate) fn has(&self, prefix: &str) -> bool {
        self.0.borrow().iter().any(|e| e.starts_with(prefix))
    }
}

pub(crate) struct FakeGit<'a> {
    pub(crate) log: &'a Log,
    pub(crate) clean: bool,
    pub(crate) last_tag: String,
    pub(crate) subject: String,
    /// Tags origin holds, by name.
    pub(crate) remote: RefCell<BTreeMap<String, String>>,
    /// A push that reaches origin and then reports failure.
    pub(crate) push_lands_then_fails: bool,
    pub(crate) push_fails: bool,
    /// The commit the checkout is at.
    pub(crate) head: &'static str,
    /// A file whose content `commit_all` records, as the commit holds it.
    pub(crate) watch: Option<PathBuf>,
    pub(crate) committed: RefCell<Option<String>>,
}

impl<'a> FakeGit<'a> {
    pub(crate) fn new(log: &'a Log) -> Self {
        Self {
            log,
            clean: true,
            last_tag: "v0.2.0".to_owned(),
            subject: "release: v0.3.0 (#42)".to_owned(),
            remote: RefCell::default(),
            push_lands_then_fails: false,
            push_fails: false,
            head: SHA,
            watch: None,
            committed: RefCell::default(),
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
        Ok(self.head[..7].to_owned())
    }
    fn commit_all(&self, subject: &str, _body: &str) -> Outcome {
        self.log.push(format!("commit {subject}"));
        if let Some(path) = &self.watch {
            *self.committed.borrow_mut() = Some(std::fs::read_to_string(path).unwrap());
        }
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
pub(crate) struct FakeForge<'a> {
    pub(crate) log: Option<&'a Log>,
    pub(crate) pulls: Vec<Pull>,
    /// `None` until a release exists.
    pub(crate) release: RefCell<Option<ReleaseState>>,
    /// The repository publishes mutable releases.
    pub(crate) mutable: bool,
    /// The state a release reads as after publishing, where it is not the
    /// published one.
    pub(crate) after_publish: Option<ReleaseState>,
    /// `verify_release` calls that fail before one passes.
    pub(crate) verify_release_failures: RefCell<u32>,
    pub(crate) assets: RefCell<Vec<String>>,
    /// Assets an upload left broken: listed by name, so an upload without
    /// `--clobber` is refused, but never reported as uploaded.
    pub(crate) broken: RefCell<Vec<String>>,
    /// Bundle uploads fail.
    pub(crate) refuse_bundle: bool,
    /// The one write that fails: `close`, `create`, `merge` or `upload`.
    pub(crate) fails: Option<&'static str>,
    /// Pull request listings refused, as on a clone with no remote on a GitHub
    /// host, such as one whose only remote is a local path.
    pub(crate) reads_fail: bool,
    /// A file whose attestation does not verify.
    pub(crate) unattested: Option<String>,
    pub(crate) create_release_failures: RefCell<u32>,
    /// The first create lands and then reports failure.
    pub(crate) create_lands_then_fails: RefCell<bool>,
    /// Whether each attestation check was given a bundle.
    pub(crate) bundled: RefCell<Vec<bool>>,
    /// Open `security` issues, by milestone title.
    pub(crate) security: Vec<(&'static str, Issue)>,
    /// The milestones `open_security_issues` was asked about, in order.
    pub(crate) security_queried: RefCell<Vec<String>>,
    /// Security issue listings refused.
    pub(crate) security_reads_fail: bool,
}

impl<'a> FakeForge<'a> {
    pub(crate) fn new(log: &'a Log) -> Self {
        Self {
            log: Some(log),
            ..Self::default()
        }
    }

    pub(crate) fn log(&self, entry: String) {
        self.log.unwrap().push(entry);
    }

    pub(crate) fn check(&self, write: &str) -> Outcome {
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
    fn open_security_issues(&self, milestone: &str) -> Result<Vec<Issue>, Error> {
        self.security_queried
            .borrow_mut()
            .push(milestone.to_owned());
        if self.security_reads_fail {
            return Err(fail("security"));
        }
        Ok(self
            .security
            .iter()
            .filter(|(m, _)| *m == milestone)
            .map(|(_, i)| i.clone())
            .collect())
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
    fn release_state(&self, _tag: &str) -> Result<ReleaseState, Error> {
        Ok(self.release.borrow().unwrap_or(ReleaseState::Missing))
    }
    fn publish_release(&self, tag: &str) -> Outcome {
        self.log(format!("publish {tag}"));
        *self.release.borrow_mut() = Some(self.after_publish.unwrap_or(ReleaseState::Published {
            immutable: !self.mutable,
        }));
        Ok(())
    }
    fn verify_release(&self, tag: &str) -> Outcome {
        if *self.release.borrow() != Some(ReleaseState::Published { immutable: true }) {
            return Err(fail("verify release"));
        }
        let mut left = self.verify_release_failures.borrow_mut();
        if *left > 0 {
            *left -= 1;
            return Err(fail("verify release before the attestation"));
        }
        self.log(format!("verify release {tag}"));
        Ok(())
    }
    fn create_release(&self, tag: &str, _notes: &str) -> Outcome {
        let mut left = self.create_release_failures.borrow_mut();
        if *left > 0 {
            *left -= 1;
            return Err(fail("create release"));
        }
        self.log(format!("draft {tag}"));
        *self.release.borrow_mut() = Some(ReleaseState::Draft);
        if self.create_lands_then_fails.replace(false) {
            return Err(fail("create after landing"));
        }
        Ok(())
    }
    fn upload(&self, tag: &str, files: &[PathBuf], clobber: bool) -> Outcome {
        for f in files {
            let name = f.file_name().unwrap().to_string_lossy().into_owned();
            if self.refuse_bundle && name.ends_with(".intoto.jsonl") {
                return Err(fail("upload bundle"));
            }
            self.check("upload")?;
            // GitHub takes uploads to a draft or a mutable published release.
            if !matches!(
                *self.release.borrow(),
                Some(ReleaseState::Draft | ReleaseState::Published { immutable: false })
            ) {
                return Err(fail("upload to a missing or immutable release"));
            }
            let held = self.assets.borrow().contains(&name) || self.broken.borrow().contains(&name);
            if !clobber && held {
                return Err(fail("asset exists"));
            }
            self.log(format!("upload {tag} {name} clobber={clobber}"));
            self.broken.borrow_mut().retain(|b| *b != name);
            let mut assets = self.assets.borrow_mut();
            if !assets.contains(&name) {
                assets.push(name);
            }
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
    fn verify_attestation(
        &self,
        file: &Path,
        bundle: Option<&Path>,
        signer_workflow: &str,
        predicate_type: &str,
        commit: &str,
    ) -> Outcome {
        self.bundled.borrow_mut().push(bundle.is_some());
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        if self.unattested.as_deref() == Some(name.as_str()) || commit != SHA {
            return Err(fail("verify"));
        }
        let kind = if predicate_type == CYCLONEDX {
            "sbom"
        } else {
            "provenance"
        };
        self.log(format!("verify {kind} {name} by {signer_workflow}"));
        Ok(())
    }
}

/// One published version: its number, its cksum, and the commit it came from.
pub(crate) type Published = (String, String, String);

/// A registry holding, per crate, the versions with their cksum and the commit
/// they were published from.
#[derive(Default)]
pub(crate) struct FakeRegistry {
    pub(crate) crates: RefCell<BTreeMap<String, Vec<Published>>>,
    /// Index reads per crate that still answer without the newest version.
    pub(crate) lag: RefCell<BTreeMap<String, u32>>,
    pub(crate) status: Option<String>,
    /// Downloads serve bytes other than the ones the index lists.
    pub(crate) download_differs: bool,
}

impl FakeRegistry {
    pub(crate) fn with(crates: &[&str]) -> Self {
        let registry = Self::default();
        for c in crates {
            registry.publish(c, "0.2.0", "old", "e");
        }
        registry
    }

    pub(crate) fn publish(&self, krate: &str, vers: &str, cksum: &str, sha: &str) {
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

    fn download(&self, krate: &str, version: Version, dir: &Path) -> Result<PathBuf, Error> {
        let file = dir.join(format!("{krate}-{version}.crate"));
        let body = if self.download_differs {
            format!("other-{krate}")
        } else {
            format!("sum-{krate}")
        };
        std::fs::write(&file, body).unwrap();
        Ok(file)
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

pub(crate) struct FakeWorkspace<'a> {
    pub(crate) log: &'a Log,
    pub(crate) crates: Vec<String>,
    pub(crate) version: Version,
    pub(crate) derived: Version,
    /// Where `package` writes each `.crate`.
    pub(crate) packaged: Scratch,
    pub(crate) registry: Option<&'a FakeRegistry>,
    /// Crates the upload publishes before it fails.
    pub(crate) upload_stops_after: Option<usize>,
    pub(crate) missing_metadata: Vec<String>,
    /// `package_unverified` writes bytes other than the attested ones.
    pub(crate) repackage_differs: bool,
    pub(crate) resolve_failures: RefCell<u32>,
    /// The blog, holding a draft post for 0.3 to begin with.
    pub(crate) blog: Scratch,
}

/// The draft post the 0.3.0 release requires.
pub(crate) const POST_0_3: &str = "---\ntitle: Spate 0.3\nslug: releases/0.3\nauthors: [marcus]\ntags: [release]\ndraft: true\n---\n\nThe opener.\n\n<!-- truncate -->\n\n## A highlight\n";

impl<'a> FakeWorkspace<'a> {
    /// The path of the post file `name` in the blog.
    pub(crate) fn post(&self, name: &str) -> PathBuf {
        self.blog.join(name)
    }

    pub(crate) fn new(log: &'a Log, crates: &[&str]) -> Self {
        Self {
            log,
            crates: crates.iter().map(|c| (*c).to_owned()).collect(),
            version: v("0.3.0"),
            derived: v("0.3.0"),
            packaged: Scratch::new("spate-xtask-release-package").unwrap(),
            registry: None,
            upload_stops_after: None,
            missing_metadata: Vec::new(),
            repackage_differs: false,
            resolve_failures: RefCell::new(0),
            blog: {
                let blog = Scratch::new("spate-xtask-release-blog").unwrap();
                std::fs::write(blog.join("2026-10-11-spate-0-3.md"), POST_0_3).unwrap();
                std::fs::write(blog.join("authors.yml"), "marcus:\n  name: M\n").unwrap();
                blog
            },
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
    fn blog(&self) -> PathBuf {
        self.blog.dir().to_path_buf()
    }
    fn notes(&self, version: Version) -> Result<String, Error> {
        Ok(format!("notes for {version}"))
    }
    fn package(&self, excludes: &[String]) -> Outcome {
        self.log
            .push(format!("package excluding [{}]", excludes.join(" ")));
        for c in self.crates.iter().filter(|c| !excludes.contains(c)) {
            std::fs::write(
                self.packaged.join(&format!("{c}-{}.crate", self.version)),
                format!("sum-{c}"),
            )
            .unwrap();
        }
        Ok(())
    }
    fn package_unverified(&self, excludes: &[String]) -> Outcome {
        self.log
            .push(format!("repackage excluding [{}]", excludes.join(" ")));
        for c in self.crates.iter().filter(|c| !excludes.contains(c)) {
            let body = if self.repackage_differs {
                format!("other-{c}")
            } else {
                format!("sum-{c}")
            };
            std::fs::write(
                self.packaged.join(&format!("{c}-{}.crate", self.version)),
                body,
            )
            .unwrap();
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
            registry.publish(c, "0.3.0", &digest(&format!("sum-{c}")), SHA);
        }
        Ok(())
    }
    fn crate_file(&self, krate: &str, version: Version) -> Option<PathBuf> {
        let file = self.packaged.join(&format!("{krate}-{version}.crate"));
        file.is_file().then_some(file)
    }
    fn sha256(&self, path: &Path) -> Result<String, Error> {
        Ok(digest(&std::fs::read_to_string(path).unwrap()))
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
pub(crate) struct Pauses(pub(crate) RefCell<Vec<Duration>>);

impl Pauses {
    pub(crate) fn of(&self, d: Duration) -> usize {
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

/// A stand-in sha256: the content's bytes in hex, padded to 64 digits.
pub(crate) fn digest(text: &str) -> String {
    let hex: String = text.bytes().map(|b| format!("{b:02x}")).collect();
    format!("{hex:0>64}")
}

pub(crate) use host;
