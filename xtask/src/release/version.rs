//! The workspace version: the rewrite a release applies to every literal that
//! carries it, the check that they agree, and the next version history implies.
//!
//! The version appears in three shapes. `[workspace.package]` carries `X.Y.Z`;
//! the `[workspace.dependencies]` pins carry `=X.Y.Z`; the install snippets in
//! the READMEs and the docs carry `X.Y`, which cargo resolves to the newest
//! `X.Y.z` on the registry.

use std::fmt;
use std::path::Path;

use clap::Subcommand;
use serde::Deserialize;

use crate::run::{self, Error, Outcome, Step};

const MANIFEST: &str = "Cargo.toml";

/// Every file holding an install snippet. A snippet anywhere else fails
/// `check` with instructions to extend this list.
const SNIPPET_FILES: [&str; 4] = [
    "README.md",
    "crates/spate/README.md",
    "docs/user-guide/01-getting-started/01-installation.mdx",
    "docs/user-guide/04-connectors/_securing-kafka.mdx",
];

/// The tracked files `check` scans for snippet-shaped lines.
const SCAN_PATTERNS: [&str; 9] = [
    "*.md", "*.mdx", "*.toml", "*.rs", "*.yml", "*.yaml", "*.ts", "*.tsx", "*.json",
];

#[derive(Subcommand)]
pub(crate) enum VersionCommand {
    /// Print the next version the history since the last tag implies
    Derive,
    /// Every pin and install snippet agrees with the workspace version
    Check,
    /// Every publishable crate carries the description and license the upload requires
    CheckMetadata,
    /// Rewrite the version, the pins, the install snippets and Cargo.lock
    Bump {
        #[arg(value_name = "X.Y.Z")]
        version: String,
    },
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: &VersionCommand) -> Outcome {
    match cmd {
        VersionCommand::Derive => {
            if explain {
                println!("(reads the tags, {MANIFEST} and changelog.d/ at HEAD)");
                return Ok(());
            }
            let (next, reason) = derive(root)?;
            eprintln!("release version: {reason}");
            println!("{next}");
            Ok(())
        }
        VersionCommand::Check => {
            if explain {
                println!("(reads {MANIFEST} and the tracked files for install snippets)");
                return Ok(());
            }
            check(root)
        }
        VersionCommand::CheckMetadata => check_metadata(root, explain),
        VersionCommand::Bump { version } => bump(root, explain, version),
    }
}

// ---------------------------------------------------------------------------
// Version parsing.
// ---------------------------------------------------------------------------

/// A release version, `X.Y.Z`, each component decimal.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Version {
    /// Exactly `X.Y.Z`; a two-component or decorated version is refused.
    pub(crate) fn parse(s: &str) -> Option<Self> {
        let mut parts = s.split('.');
        let v = Self {
            major: component(parts.next()?)?,
            minor: component(parts.next()?)?,
            patch: component(parts.next()?)?,
        };
        parts.next().is_none().then_some(v)
    }

    /// `X.Y`: the requirement an install snippet carries.
    fn minor_of(self) -> String {
        format!("{}.{}", self.major, self.minor)
    }

    fn next(self, bump: Bump) -> Self {
        match bump {
            Bump::Minor => Self {
                major: self.major,
                minor: self.minor + 1,
                patch: 0,
            },
            Bump::Patch => Self {
                patch: self.patch + 1,
                ..self
            },
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bump {
    Minor,
    Patch,
}

/// One decimal component: digits only, and no leading zero.
fn component(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return None;
    }
    s.parse().ok()
}

/// Whether `a < b`, component-wise. `X.Y` reads as `X.Y.0`, so an MSRV pair
/// compares directly.
fn version_lt(a: &str, b: &str) -> Result<bool, Error> {
    Ok(loose(a)? < loose(b)?)
}

fn loose(s: &str) -> Result<(u64, u64, u64), Error> {
    let bad = || Error::msg(format!("'{s}' is not X.Y or X.Y.Z"));
    let parts: Vec<&str> = s.split('.').collect();
    let n = |i: usize| -> Result<u64, Error> {
        parts.get(i).map_or(Ok(0), |p| component(p).ok_or_else(bad))
    };
    if !(2..=3).contains(&parts.len()) {
        return Err(bad());
    }
    Ok((n(0)?, n(1)?, n(2)?))
}

/// The `[workspace.package]` version. Members inherit it, so the manifest
/// carries exactly one line opening `version = "`, and more than one is an
/// error.
pub(crate) fn workspace_version(manifest: &str) -> Result<Version, Error> {
    let lines: Vec<&str> = manifest
        .lines()
        .filter(|l| l.starts_with("version = \""))
        .collect();
    let [line] = lines[..] else {
        return Err(Error::msg(format!(
            "{MANIFEST} carries {} 'version = \"...\"' lines, expected exactly 1",
            lines.len()
        )));
    };
    let value = line
        .strip_prefix("version = \"")
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or_default();
    Version::parse(value).ok_or_else(|| {
        Error::msg(format!(
            "{MANIFEST} workspace version '{value}' is not X.Y.Z"
        ))
    })
}

/// The `rust-version` the manifest declares, or `None` without one.
fn rust_version(manifest: &str) -> Option<&str> {
    manifest.lines().find_map(|l| {
        let rest = l.strip_prefix("rust-version = \"")?;
        Some(&rest[..rest.find('"')?])
    })
}

// ---------------------------------------------------------------------------
// The line grammar. Each matcher takes a position and answers where its match
// ends, or `None`.
// ---------------------------------------------------------------------------

/// POSIX `[[:space:]]`.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

fn spaces(s: &[u8], mut i: usize) -> usize {
    while s.get(i).copied().is_some_and(is_space) {
        i += 1;
    }
    i
}

fn byte(s: &[u8], i: usize, want: u8) -> Option<usize> {
    (s.get(i) == Some(&want)).then_some(i + 1)
}

fn literal(s: &[u8], i: usize, want: &str) -> Option<usize> {
    s.get(i..)?
        .starts_with(want.as_bytes())
        .then_some(i + want.len())
}

fn digits(s: &[u8], i: usize) -> Option<usize> {
    let end = i + s
        .get(i..)?
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    (end > i).then_some(end)
}

/// `[0-9]+\.[0-9]+`.
fn two(s: &[u8], i: usize) -> Option<usize> {
    digits(s, byte(s, digits(s, i)?, b'.')?)
}

/// `[0-9]+\.[0-9]+(\.[0-9]+)?`.
fn two_or_three(s: &[u8], i: usize) -> Option<usize> {
    let end = two(s, i)?;
    Some(byte(s, end, b'.').and_then(|j| digits(s, j)).unwrap_or(end))
}

/// A crate name in the snippet grammar, `spate(-[a-z0-9]+)*`.
fn snippet_crate(s: &[u8], i: usize) -> Option<usize> {
    let mut end = literal(s, i, "spate")?;
    while s.get(end) == Some(&b'-') {
        let run = s[end + 1..]
            .iter()
            .take_while(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            .count();
        if run == 0 {
            break;
        }
        end += 1 + run;
    }
    Some(end)
}

/// A crate name in the pin grammar, `spate-[a-z0-9-]+`, anchored at the start.
fn pin_crate(s: &[u8]) -> Option<usize> {
    let start = literal(s, 0, "spate-")?;
    let run = s[start..]
        .iter()
        .take_while(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || **b == b'-')
        .count();
    (run > 0).then_some(start + run)
}

/// `NAME[[:space:]]*=[[:space:]]*` after a name ending at `i`: the position of
/// the value.
fn assignment(s: &[u8], i: usize) -> Option<usize> {
    Some(spaces(s, byte(s, spaces(s, i), b'=')?))
}

/// `"VALUE"`, answering the value's span.
fn quoted(s: &[u8], i: usize, value: fn(&[u8], usize) -> Option<usize>) -> Option<(usize, usize)> {
    let start = byte(s, i, b'"')?;
    let end = value(s, start)?;
    byte(s, end, b'"').map(|_| (start, end))
}

/// The `version[[:space:]]*=[[:space:]]*"VALUE"` matches lying wholly between
/// `from` and the first `}` after it, each as the span of the key through the
/// closing quote and the span of the value.
fn version_keys(
    s: &[u8],
    from: usize,
    value: fn(&[u8], usize) -> Option<usize>,
) -> Vec<((usize, usize), (usize, usize))> {
    let close = s[from..]
        .iter()
        .position(|b| *b == b'}')
        .map_or(s.len(), |p| from + p);
    let region = &s[..close];
    (from..close)
        .filter_map(|i| {
            let at = assignment(region, literal(region, i, "version")?)?;
            let (vs, ve) = quoted(region, at, value)?;
            Some(((i, ve + 1), (vs, ve)))
        })
        .collect()
}

/// The value position of a line-anchored snippet: leading blanks, the crate,
/// the assignment.
fn anchored_snippet(s: &[u8]) -> Option<usize> {
    assignment(s, snippet_crate(s, spaces(s, 0))?)
}

/// Whether a line looks like an install snippet anywhere on it, with any number
/// of version components.
///
/// Deliberately wider than the rewriters, so `check` reports a snippet they
/// cannot reach. An inline table left open on the line counts, as a snippet
/// wrapped onto several lines, which no line-based rewriter can move.
/// A name preceded by `[A-Za-z0-9_-]`, as in `myspate`, does not match.
fn looks_like_snippet(line: &str) -> bool {
    let s = line.as_bytes();
    (0..s.len()).any(|i| {
        if i > 0 && (s[i - 1].is_ascii_alphanumeric() || matches!(s[i - 1], b'_' | b'-')) {
            return false;
        }
        let Some(at) = snippet_crate(s, i).and_then(|end| assignment(s, end)) else {
            return false;
        };
        if quoted(s, at, two_or_three).is_some() {
            return true;
        }
        let Some(open) = byte(s, at, b'{') else {
            return false;
        };
        !s[open..].contains(&b'}') || !version_keys(s, open, two_or_three).is_empty()
    })
}

/// Whether the rewriters reach a line: line-anchored, two-component version.
/// A snippet that fails this is one the release would silently skip.
fn is_rewritable(line: &str) -> bool {
    let s = line.as_bytes();
    let Some(at) = anchored_snippet(s) else {
        return false;
    };
    quoted(s, at, two).is_some()
        || byte(s, at, b'{').is_some_and(|open| !version_keys(s, open, two).is_empty())
}

/// The version string inside a rewritable snippet line, read from the
/// snippet's own construct and never from a trailing comment.
fn snippet_version(line: &str) -> Option<&str> {
    let s = line.as_bytes();
    let at = anchored_snippet(s)?;
    let (start, end) = match byte(s, at, b'{') {
        Some(open) => version_keys(s, open, two_or_three).last()?.1,
        None => quoted(s, at, two_or_three)?,
    };
    Some(&line[start..end])
}

// ---------------------------------------------------------------------------
// The rewriters, pure functions over the text.
// ---------------------------------------------------------------------------

/// The lines of `text`, each with its terminator.
fn lines_with_ends(text: &str) -> impl Iterator<Item = (&str, &str)> {
    text.split_inclusive('\n').map(|l| {
        let body = l.strip_suffix('\n').unwrap_or(l);
        (body, &l[body.len()..])
    })
}

/// The manifest with the one `[workspace.package]` version line and every
/// `spate-*` pin moved to `new`.
///
/// A pin needs `version` first with an `=` requirement, so the versionless
/// `spate-bench = { path = "bench" }` entry is out of reach. A `spate-*` line
/// carrying an `"=` requirement in any other shape is refused, so a pin the
/// rewrite cannot reach fails the bump.
fn rewrite_manifest(text: &str, new: Version) -> Result<String, Error> {
    let mut out = String::with_capacity(text.len());
    let (mut top, mut pins) = (0, 0);
    for (line, end) in lines_with_ends(text) {
        let s = line.as_bytes();
        if line.starts_with("version = \"") {
            top += 1;
            out.push_str(&replace_first_quoted(line, &new.to_string()));
        } else if let Some(name) = pin_crate(s) {
            let pin = assignment(s, name)
                .and_then(|at| byte(s, at, b'{'))
                .map(|open| spaces(s, open))
                .and_then(|at| literal(s, at, "version"))
                .and_then(|at| assignment(s, at))
                .and_then(|at| literal(s, at, "\"="));
            match pin {
                Some(_) => {
                    pins += 1;
                    out.push_str(&replace_first_pin(line, &new.to_string()));
                }
                None if assignment(s, name).is_some() && line.contains("\"=") => {
                    return Err(Error::msg(format!(
                        "a pin the rewriter cannot reach: {line}"
                    )));
                }
                None => out.push_str(line),
            }
        } else {
            out.push_str(line);
        }
        out.push_str(end);
    }
    if top != 1 {
        return Err(Error::msg(format!(
            "rewrote {top} workspace version lines, expected 1"
        )));
    }
    if pins < 1 {
        return Err(Error::msg("rewrote no spate-* pins"));
    }
    Ok(out)
}

/// The line with its first `"..."` replaced by `"new"`.
fn replace_first_quoted(line: &str, new: &str) -> String {
    let Some(open) = line.find('"') else {
        return line.to_owned();
    };
    let Some(len) = line[open + 1..].find('"') else {
        return line.to_owned();
    };
    let close = open + 1 + len;
    format!("{}\"{new}\"{}", &line[..open], &line[close + 1..])
}

/// The line with its first `"=..."` replaced by `"=new"`.
fn replace_first_pin(line: &str, new: &str) -> String {
    let Some(open) = line.find("\"=") else {
        return line.to_owned();
    };
    let Some(len) = line[open + 2..].find('"') else {
        return line.to_owned();
    };
    let close = open + 2 + len;
    format!("{}\"={new}\"{}", &line[..open], &line[close + 1..])
}

/// The text with every rewritable install snippet moved to `xy`, or `None`
/// when it holds none. Only the version string moves; features, paths and
/// trailing comments stay byte-identical.
fn rewrite_snippets(text: &str, xy: &str) -> Option<String> {
    let mut out = String::with_capacity(text.len());
    let mut hits = 0;
    for (line, end) in lines_with_ends(text) {
        let s = line.as_bytes();
        match anchored_snippet(s) {
            Some(at) => {
                if let Some((start, stop)) = quoted(s, at, two) {
                    hits += 1;
                    out.push_str(&line[..start]);
                    out.push_str(xy);
                    out.push_str(&line[stop..]);
                } else if let Some(&((start, stop), _)) = byte(s, at, b'{')
                    .and_then(|open| version_keys(s, open, two).first().copied())
                    .as_ref()
                {
                    hits += 1;
                    out.push_str(&line[..start]);
                    out.push_str(&format!("version = \"{xy}\""));
                    out.push_str(&line[stop..]);
                } else {
                    out.push_str(line);
                }
            }
            None => out.push_str(line),
        }
        out.push_str(end);
    }
    (hits > 0).then_some(out)
}

// ---------------------------------------------------------------------------
// The modes.
// ---------------------------------------------------------------------------

fn read(root: &Path, rel: &str) -> Result<String, Error> {
    std::fs::read_to_string(root.join(rel)).map_err(|e| Error::msg(format!("{rel}: {e}")))
}

/// Whether `check` leaves a tracked file out of its scan. The changelog and the
/// attribution are generated with the versions they are meant to hold, and an
/// accepted decision record is immutable, so a literal there stays as written.
/// This module's tests hold snippet-shaped fixtures by design.
fn scan_excluded(path: &str) -> bool {
    matches!(
        path,
        "CHANGELOG.md" | "THIRD-PARTY.md" | "xtask/src/release/version/tests.rs"
    ) || path.starts_with("changelog.d/")
        || path.starts_with("docs/adr/")
}

/// The problems `check` finds in the manifest's pins, one diagnostic each.
fn pin_problems(manifest: &str, cur: Version) -> Result<Vec<String>, Error> {
    let pins: Vec<&str> = manifest
        .lines()
        .filter(|l| pin_crate(l.as_bytes()).is_some_and(|n| assignment(l.as_bytes(), n).is_some()))
        .collect();
    if pins.is_empty() {
        return Err(Error::msg(format!(
            "{MANIFEST} carries no spate-* pins; the release has nothing to rewrite"
        )));
    }
    let want = format!("\"={cur}\"");
    Ok(pins
        .into_iter()
        .filter(|l| l.contains("\"=") && !l.contains(&want))
        .map(|l| format!("{MANIFEST} pin does not match the workspace version {cur}:\n  {l}"))
        .collect())
}

/// The problems `check` finds in one file's snippet-shaped lines.
fn snippet_problems(path: &str, text: &str, xy: &str) -> Vec<String> {
    let known = SNIPPET_FILES.contains(&path);
    let mut out = Vec::new();
    for (n, line) in text.split('\n').enumerate() {
        if !looks_like_snippet(line) {
            continue;
        }
        let at = format!("{path}:{}", n + 1);
        if !known {
            out.push(format!(
                "{at} looks like an install snippet, but the file is not\n  \
                 in the rewritten set. Add it to SNIPPET_FILES in xtask/src/release/version.rs,\n  \
                 or reword the line so it carries no literal version."
            ));
        } else if !is_rewritable(line) {
            out.push(format!(
                "{at} is a snippet the rewriters cannot reach:\n  {line}\n  \
                 A snippet is one line and carries a two-component version (\"{xy}\")."
            ));
        } else if let Some(v) = snippet_version(line).filter(|v| *v != xy) {
            out.push(format!(
                "{at} carries {v}, the workspace is at {xy}:\n  {line}"
            ));
        }
    }
    out
}

/// Every literal version agrees with the workspace.
fn check(root: &Path) -> Outcome {
    let cur = workspace_version(&read(root, MANIFEST)?)?;
    let xy = cur.minor_of();
    let mut problems = pin_problems(&read(root, MANIFEST)?, cur)?;

    let listing = run::capture(
        root,
        &Step::new("git", ["ls-files", "--"]).args(SCAN_PATTERNS),
    )?;
    for path in listing.lines().filter(|p| !scan_excluded(p)) {
        let bytes =
            std::fs::read(root.join(path)).map_err(|e| Error::msg(format!("{path}: {e}")))?;
        problems.extend(snippet_problems(
            path,
            &String::from_utf8_lossy(&bytes),
            &xy,
        ));
    }

    // A known file with no snippet means a rewrite, or an edit, destroyed one,
    // and the scan above had nothing to judge.
    for path in SNIPPET_FILES {
        if !read(root, path)?.split('\n').any(looks_like_snippet) {
            problems.push(format!(
                "{path} carries no install snippet, but SNIPPET_FILES says it does"
            ));
        }
    }

    for p in &problems {
        eprintln!("release version: {p}");
    }
    if !problems.is_empty() {
        return Err(Error::msg(format!(
            "{} version literal(s) disagree; see above",
            problems.len()
        )));
    }
    println!("release version: every version literal agrees with {cur}");
    Ok(())
}

/// Every publishable crate carries a description and a license. `cargo
/// publish --dry-run` packages a crate missing either with a warning and exit
/// 0, while the upload rejects it after publishing whatever sorted before it
/// (cargo issue 14249).
fn check_metadata(root: &Path, explain: bool) -> Outcome {
    let step = Step::new(
        "cargo",
        ["metadata", "--no-deps", "--format-version", "1", "--locked"],
    );
    if explain {
        println!("{}", step.display());
        return Ok(());
    }
    let bad = missing_metadata(&run::capture(root, &step)?)?;
    if !bad.is_empty() {
        return Err(Error::msg(format!(
            "missing description or license in: {}",
            bad.join(" ")
        )));
    }
    println!("release version: every publishable crate carries the metadata the upload requires");
    Ok(())
}

/// The publishable packages in `cargo metadata` output missing a description
/// or a license. A package is publishable unless its `publish` is `[]`.
pub(crate) fn missing_metadata(metadata: &str) -> Result<Vec<String>, Error> {
    #[derive(Deserialize)]
    struct Metadata {
        packages: Vec<Package>,
    }
    #[derive(Deserialize)]
    struct Package {
        name: String,
        publish: Option<Vec<String>>,
        description: Option<String>,
        license: Option<String>,
    }

    let parsed: Metadata =
        serde_json::from_str(metadata).map_err(|e| Error::msg(format!("cargo metadata: {e}")))?;
    let blank = |v: &Option<String>| v.as_deref().is_none_or(str::is_empty);
    Ok(parsed
        .packages
        .into_iter()
        .filter(|p| p.publish.as_ref().is_none_or(|allow| !allow.is_empty()))
        .filter(|p| blank(&p.description) || blank(&p.license))
        .map(|p| p.name)
        .collect())
}

/// The next version and the reason for its bump.
///
/// Pre-1.0 a breaking change is a minor bump, since cargo treats `0.x` minors
/// as incompatible. A raised `rust-version` is a minor under the same rule,
/// judged by comparing the two values, so an annotation or a lowering is not a
/// raise.
pub(crate) fn derive(root: &Path) -> Result<(Version, String), Error> {
    let last = crate::checks::semver_checks::last_tag(root)?;
    if last.is_empty() {
        return Err(Error::msg(
            "no vX.Y.Z tag; the first release of a repository is cut by hand",
        ));
    }

    let manifest = read(root, MANIFEST)?;
    let cur = workspace_version(&manifest)?;
    if format!("v{cur}") != last {
        return Err(Error::msg(format!(
            "{MANIFEST} is at {cur} but the last tag is {last}: a release is\n  \
             half-finished. Finish it (re-run the failed publish jobs on its run) before\n  \
             deriving the next version."
        )));
    }

    let count = run::capture(
        root,
        &Step::new("git", ["rev-list", "--no-merges", "--count"]).arg(format!("{last}..HEAD")),
    )?;
    if count.trim() == "0" {
        return Err(Error::msg(format!(
            "no commits since {last}; nothing to release"
        )));
    }

    if crate::checks::changelog::breaking_announced(root)? {
        return Ok((
            cur.next(Bump::Minor),
            "minor bump (a changelog fragment opens with **Breaking:**)".to_owned(),
        ));
    }
    let old = run::capture(
        root,
        &Step::new("git", ["show"]).arg(format!("{last}:{MANIFEST}")),
    )?;
    if let (Some(was), Some(now)) = (rust_version(&old), rust_version(&manifest))
        && !was.is_empty()
        && !now.is_empty()
        && version_lt(was, now)?
    {
        return Ok((
            cur.next(Bump::Minor),
            format!("minor bump (rust-version rose from {was} to {now} since {last})"),
        ));
    }
    Ok((
        cur.next(Bump::Patch),
        format!("patch bump (no breaking change since {last})"),
    ))
}

/// Rewrites every literal to `new`, refreshes the lockfile, then checks.
///
/// Every rewrite is computed before any file is written, so a snippet the
/// rewriter cannot find leaves every file unchanged.
pub(crate) fn bump(root: &Path, explain: bool, new: &str) -> Outcome {
    let update = Step::new("cargo", ["update", "--workspace"]);
    if explain {
        println!("(rewrites {MANIFEST} and {})", SNIPPET_FILES.join(", "));
        println!("{}", update.display());
        println!("(then checks every version literal)");
        return Ok(());
    }

    let new_v = Version::parse(new).ok_or_else(|| Error::msg(format!("'{new}' is not X.Y.Z")))?;
    let manifest = read(root, MANIFEST)?;
    let cur = workspace_version(&manifest)?;
    if new_v == cur {
        return Err(Error::msg(format!("the workspace is already at {cur}")));
    }
    if new_v < cur {
        return Err(Error::msg(format!(
            "{new} is behind the workspace version {cur}, and a published\n  \
             version can never be reused"
        )));
    }

    let mut staged = vec![(
        MANIFEST,
        rewrite_manifest(&manifest, new_v)
            .map_err(|e| Error::msg(format!("{}; nothing was changed", e.message)))?,
    )];
    let xy = new_v.minor_of();
    for path in SNIPPET_FILES {
        if !root.join(path).is_file() {
            return Err(Error::msg(format!(
                "SNIPPET_FILES names '{path}', which does not exist"
            )));
        }
        let text = rewrite_snippets(&read(root, path)?, &xy).ok_or_else(|| {
            Error::msg(format!(
                "no install snippet found in {path}; SNIPPET_FILES says there is one.\n  \
                 Nothing was changed."
            ))
        })?;
        staged.push((path, text));
    }

    replace_all(root, &staged)?;

    // `--workspace` touches only the members' own entries.
    run::run(root, false, &update)?;
    check(root)?;
    println!(
        "release version: bumped {cur} -> {new_v} across {} files",
        staged.len()
    );
    Ok(())
}

/// Replaces each file's contents through a sibling and a rename, keeping its
/// permissions. Every sibling is written before the first rename, so a failed
/// write leaves every target as it was.
fn replace_all(root: &Path, files: &[(&str, String)]) -> Outcome {
    let mut written = Vec::with_capacity(files.len());
    let outcome = files
        .iter()
        .try_for_each(|(rel, text)| {
            let path = root.join(rel);
            let tmp = path.with_file_name(crate::checks::scratch::unique_name(&format!(
                ".{}",
                path.file_name().unwrap_or_default().to_string_lossy()
            )));
            let perms = std::fs::metadata(&path)?.permissions();
            written.push((tmp.clone(), path));
            std::fs::write(&tmp, text)?;
            std::fs::set_permissions(&tmp, perms)
        })
        .and_then(|()| {
            written
                .iter()
                .try_for_each(|(tmp, path)| std::fs::rename(tmp, path))
        });
    if let Err(e) = outcome {
        for (tmp, _) in &written {
            drop(std::fs::remove_file(tmp));
        }
        return Err(Error::msg(format!("rewriting the version literals: {e}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
