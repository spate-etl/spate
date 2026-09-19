//! The changelog-fragment gate and the scaffolder that writes a fragment.
//!
//! A change somebody upgrading would care about carries a file under
//! `changelog.d/`, and `changelog.d/README.md` states the format and the
//! policy. The gate classifies the pull request's title and the branch's own
//! subjects, and demands a fragment for any of them that reaches a crate.
//!
//! The classifier is an ignore list on both axes: an unrecognized type and an
//! unrecognized scope each require a fragment. Stated the other way round
//! ("required iff the scope names a crate") it fails open.

use std::path::Path;

use crate::checks::adr::is_slug;
use crate::checks::scratch::Scratch;
use crate::run::{self, Completed, Error, Outcome, Step, Streams};

/// The prefix on every line this check writes for itself.
const TOOL: &str = "changelog";

/// The directory holding the fragments.
const FRAGMENTS: &str = "changelog.d";

/// The Keep a Changelog six, in the order a release renders them. A breaking
/// change is a `**Breaking:**` marker on one of these.
const TYPES: &[&str] = &[
    "added",
    "changed",
    "deprecated",
    "removed",
    "fixed",
    "security",
];

/// The scopes that do not reach a crate.
///
/// Typed out. The `area:` labels in `.github/labels.yml` also carry
/// `supply-chain`, and a `fix(supply-chain):` closing an advisory is a release
/// note. `bench` covers the unpublished bench harness and the `benches/`
/// targets inside published crates.
const EXEMPT_SCOPES: &[&str] = &["ci", "docs", "examples", "bench", "workspace", "website"];

/// The types whose subjects say nothing user-visible moved. Every other type
/// requires a fragment, `feat`, `fix`, `perf`, `revert` and `build` included.
const INTERNAL_TYPES: &[&str] = &["docs", "test", "chore", "style", "ci", "refactor"];

/// What a new fragment carries until its author writes the entry.
const TEMPLATE: &str = "\
**A short bold lead-in** (`spate-crate`) — what this means for somebody
upgrading, in one to five sentences. Present tense, impersonal. Say what it
means, not what moved; the commit message already says what moved.

Delete this template text and write the entry. If the change is breaking, open
with `**Breaking:**`.
";

/// What the failure prints under the offending subjects.
const GUIDANCE: &str = "
  Add one with:

      cargo xtask changelog new fixed short-description

  and write what the change means for somebody upgrading, not what moved.
  changelog.d/README.md has the format and the conventions.

  If it is not user-visible, say so in the subject. There is no label and
  no opt-out checkbox for this, and .github/labels.yml says why. The exemption
  is derived from the type and scope you write:

      feat(spate-core): ...  ->  refactor(spate-core): ...  nothing user-facing moved
      fix(spate-core): ...   ->  test(spate-core): ...      it only touched tests
      feat(spate-core): ...  ->  feat(docs): ...            it only touched docs

  For a fix to a bug that was never released, put a 'Changelog: none'
  trailer on the commit.

  The pull request title is what lands on main, since this repository squashes
  with the title as the subject, so the title is the one that has to be right.";

/// The fields a pull request run reads, all of them free text somebody else
/// typed.
///
/// They are matched against and never evaluated. Nothing here reaches a shell,
/// a pattern compiler or a format string, because a `pull_request` run executes
/// the pull request's own copy of this code.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct Fields {
    event: String,
    base_sha: String,
    head_sha: String,
    title: String,
    body: String,
}

impl Fields {
    fn from_env() -> Self {
        Self {
            event: var("EVENT_NAME"),
            base_sha: var("BASE_SHA"),
            head_sha: var("HEAD_SHA"),
            title: var("PR_TITLE"),
            body: var("PR_BODY"),
        }
    }
}

/// Where a `Changelog: none` trailer is read from.
///
/// A trailer excuses the message it is written on. In the pull request body
/// that is the whole pull request, because the body is what the squash commit
/// carries; on a commit it is that commit's subject alone.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Source {
    Body,
    Commit(String),
}

/// One subject to classify, the phrase naming where it came from, and the
/// message its excuse would be written on.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Subject {
    text: String,
    origin: String,
    source: Source,
}

/// What the gate compares against.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Mode {
    /// The fragment requirement is not evaluated.
    Structure,
    /// Every subject in `base..head`, where an absent head reads as `HEAD`.
    Require { base: String, head: Option<String> },
}

/// A subject broken into the three fields the classifier reads.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Parsed<'a> {
    kind: &'a str,
    scopes: &'a str,
    bang: bool,
}

/// The gate: classifies every subject this change carries and demands a
/// fragment for each of them that reaches a crate.
pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(classifies this branch's subjects and reads {FRAGMENTS}/)");
        return Ok(());
    }
    gate(
        root,
        &Fields::from_env(),
        std::env::var_os("GITHUB_ACTIONS").is_some(),
        &var("GITHUB_EVENT_NAME"),
    )
}

/// The gate's verdict over one set of fields and one runner state.
fn gate(root: &Path, fields: &Fields, in_actions: bool, github_event: &str) -> Outcome {
    if !root.join(FRAGMENTS).is_dir() {
        return Err(Error::msg(format!(
            "{FRAGMENTS}/ not found. It holds the changelog fragments"
        )));
    }
    if !root.join(FRAGMENTS).join("README.md").is_file() {
        return Err(Error::msg(format!(
            "{FRAGMENTS}/README.md not found. It states the format and, less obviously,\n  \
             is what keeps the directory in git once a release has consumed every fragment."
        )));
    }

    let mode = select(root, fields)?;

    if evaluated_nothing(&mode, in_actions, github_event) {
        return Err(Error::msg(
            "running on a pull request inside GitHub Actions with no EVENT_NAME, so this\n  \
             would have checked nothing and passed.\n\n  \
             The job that runs this has to pass EVENT_NAME, BASE_SHA, HEAD_SHA, PR_TITLE and\n  \
             PR_BODY through `env:`, and its checkout needs `fetch-depth: 0`. See the\n  \
             `changelog` job in .github/workflows/ci.yml.",
        ));
    }

    let Mode::Require { base, head } = mode else {
        println!("{TOOL}: {FRAGMENTS}/ is present with its README; no base to compare against,");
        println!(
            "  so no fragment requirement was evaluated (EVENT_NAME='{}').",
            fields.event
        );
        return Ok(());
    };

    let range = format!("{base}..{}", head.as_deref().unwrap_or("HEAD"));
    let subjects = subjects(root, &range, &fields.title);
    let scratch = Scratch::new("spate-xtask-changelog")?;

    if body_says_none(root, &scratch, &fields.body) {
        println!("{TOOL}: the pull request body carries a 'Changelog: none' trailer, which");
        println!(
            "  is what the squash commit will carry. Taken at its word for this pull request."
        );
        return Ok(());
    }

    let (offenders, excused) = offenders(root, &scratch, &subjects);

    if offenders.is_empty() && excused > 0 {
        println!("{TOOL}: {excused} subject(s) would require a changelog fragment, and each");
        println!("  carries a 'Changelog: none' trailer saying it is not user-visible.");
        return Ok(());
    }
    if offenders.is_empty() {
        println!("{TOOL}: nothing in {range} requires a changelog fragment.");
        return Ok(());
    }

    let (added, empty) = fragments_added(root, &base, head.as_deref());

    if added == 0 && !empty.is_empty() {
        let listed: String = empty.iter().map(|f| format!("    {f}\n")).collect();
        return Err(Error::msg(format!(
            "these fragment(s) were added but are empty:\n\n{listed}  \
             A fragment is the release note. Write what the change means for somebody\n  \
             upgrading. {FRAGMENTS}/README.md has the conventions."
        )));
    }
    if added > 0 {
        println!(
            "{TOOL}: {} subject(s) require a changelog fragment, {added} added.",
            offenders.len()
        );
        return Ok(());
    }

    eprintln!("{TOOL}: these subject(s) say this change is visible to somebody");
    eprintln!("  upgrading, and no fragment was added under {FRAGMENTS}/:");
    eprintln!();
    for line in &offenders {
        eprintln!("{line}");
    }
    eprintln!("{GUIDANCE}");
    Err(Error::status(1))
}

/// Scaffolds a fragment. Under `--explain` the answer is the path this would
/// write, so a type the six do not name, a slug the filename rules reject and a
/// path already taken are refused under the flag as they are without it.
pub(crate) fn new(root: &Path, explain: bool, kind: &str, slug: &str) -> Outcome {
    if kind.is_empty() || slug.is_empty() {
        return Err(Error::msg(format!(
            "usage: cargo xtask changelog new <type> <slug>\n  \
             type is one of: {}",
            TYPES.join(" ")
        )));
    }
    if !in_list(TYPES, kind) {
        return Err(Error::msg(format!(
            "'{kind}' is not a fragment type. The Keep a Changelog six are: {}",
            TYPES.join(" ")
        )));
    }
    if !is_slug(slug) {
        return Err(Error::msg(format!(
            "'{slug}' should be lowercase letters, digits and hyphens, starting and ending\n  \
             with one of the first two. It becomes a filename."
        )));
    }

    let path = format!("{FRAGMENTS}/{slug}.{kind}.md");
    if root.join(&path).exists() {
        return Err(Error::msg(format!("{path} already exists")));
    }
    if explain {
        println!("(writes {path})");
        return Ok(());
    }

    std::fs::create_dir_all(root.join(FRAGMENTS))
        .map_err(|e| Error::msg(format!("{FRAGMENTS}: {e}")))?;
    std::fs::write(root.join(&path), TEMPLATE).map_err(|e| Error::msg(format!("{path}: {e}")))?;
    println!("{TOOL}: wrote {path}");
    println!(
        "  Edit it, then commit it with your change. {FRAGMENTS}/README.md has the conventions."
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The classifier.
// ---------------------------------------------------------------------------

/// Whether this subject requires a changelog fragment.
///
/// Reads the subject line and nothing else. A subject the pattern does not
/// match requires one.
fn needs_entry(subject: &str) -> bool {
    let Some(parsed) = parse_subject(subject) else {
        return true;
    };

    // `!` decides on its own, before either axis.
    if parsed.bang {
        return true;
    }

    // Scope axis: can this reach something somebody depends on?
    let reaches_crate = if parsed.scopes.is_empty() {
        true
    } else {
        scope_list(parsed.scopes)
            .into_iter()
            .any(|scope| !in_list(EXEMPT_SCOPES, trim_space(scope)))
    };
    if !reaches_crate {
        return false;
    }

    // Type axis: would somebody upgrading care?
    !in_list(INTERNAL_TYPES, &parsed.kind.to_ascii_lowercase())
}

/// The type, the scope list and the breaking marker of `type(scope)!: text`.
/// The scope and the marker are optional; the text is required.
fn parse_subject(subject: &str) -> Option<Parsed<'_>> {
    let kind_len = subject.bytes().take_while(u8::is_ascii_alphabetic).count();
    if kind_len == 0 {
        return None;
    }
    let (kind, mut rest) = subject.split_at(kind_len);

    // A scope runs to the first `)`; with none, the whole group is absent and
    // the `:` has to follow the type.
    let mut scopes = "";
    if let Some(open) = rest.strip_prefix('(')
        && let Some(close) = open.find(')')
    {
        scopes = &open[..close];
        rest = &open[close + 1..];
    }

    let bang = rest.starts_with('!');
    if bang {
        rest = &rest[1..];
    }
    rest = rest.strip_prefix(':')?;
    rest = rest.trim_start_matches(is_space);
    (!rest.is_empty()).then_some(Parsed { kind, scopes, bang })
}

/// The scopes of a comma-separated list, where a trailing comma closes the
/// last one and an empty list has no scopes at all.
fn scope_list(scopes: &str) -> Vec<&str> {
    let mut out: Vec<&str> = scopes.split(',').collect();
    if out.last().is_some_and(|last| last.is_empty()) {
        out.pop();
    }
    out
}

/// Whether `item` appears in `list` as a space-delimited run.
fn in_list(list: &[&str], item: &str) -> bool {
    format!(" {} ", list.join(" ")).contains(&format!(" {item} "))
}

/// Trims the ASCII whitespace a field may carry: `feat( spate-core ):` is legal
/// conventional-commits.
fn trim_space(s: &str) -> &str {
    s.trim_matches(is_space)
}

/// The characters POSIX `[[:space:]]` names in the C locale.
fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r')
}

// ---------------------------------------------------------------------------
// What the gate compares against.
// ---------------------------------------------------------------------------

/// Picks the comparison for this event.
///
/// A pull request is compared against the merge base, and a missing merge base
/// is a hard failure: "demand a fragment" is nothing the contributor can act
/// on. It means the checkout lost `fetch-depth: 0`. Every other event proved
/// the requirement on the pull request, so it is structure only.
fn select(root: &Path, fields: &Fields) -> Result<Mode, Error> {
    match fields.event.as_str() {
        "pull_request" => {
            let base = merge_base(root, &fields.base_sha, &fields.head_sha).ok_or_else(|| {
                Error::msg(format!(
                    "no merge base for {}..{}. Does the checkout still set fetch-depth: 0?",
                    or_unknown(&fields.base_sha),
                    or_unknown(&fields.head_sha)
                ))
            })?;
            Ok(Mode::Require {
                base,
                head: (!fields.head_sha.is_empty()).then(|| fields.head_sha.clone()),
            })
        }
        // A laptop. Orient against the obvious upstream so `cargo xtask ci`
        // answers the question before you push, and fall back to structure
        // only.
        "" => Ok(
            laptop_base(root).map_or(Mode::Structure, |base| Mode::Require { base, head: None })
        ),
        _ => Ok(Mode::Structure),
    }
}

/// Whether this run is about to report success having evaluated nothing.
///
/// A job running `cargo xtask tidy changelog` without passing the fields
/// through `env:` calls this with no `EVENT_NAME`, in a shallow checkout, where
/// it takes the laptop arm and reports success. Structure-only must never be
/// the answer on a pull request.
fn evaluated_nothing(mode: &Mode, in_actions: bool, github_event: &str) -> bool {
    *mode == Mode::Structure && in_actions && github_event == "pull_request"
}

/// The first of the obvious upstreams naming a merge base below `HEAD`.
fn laptop_base(root: &Path) -> Option<String> {
    let head = capture(root, &["rev-parse", "HEAD"])?;
    for upstream in ["origin/main", "upstream/main", "main"] {
        let refspec = format!("{upstream}^{{commit}}");
        let Some(candidate) = capture(root, &["rev-parse", "--verify", "--quiet", &refspec]) else {
            continue;
        };
        if let Some(base) = merge_base(root, "HEAD", &candidate)
            && base != head
        {
            return Some(base);
        }
    }
    None
}

fn merge_base(root: &Path, a: &str, b: &str) -> Option<String> {
    capture(root, &["merge-base", a, b])
}

/// One `git` invocation's first line of stdout, or `None` where it failed or
/// answered with nothing.
fn capture(root: &Path, args: &[&str]) -> Option<String> {
    let step = Step::new("git", args.iter().copied());
    let Ok(Completed { code: 0, stdout }) = run::complete(root, &step, Streams::Collect) else {
        return None;
    };
    let first = stdout.split('\n').next().unwrap_or_default();
    (!first.is_empty()).then(|| first.to_owned())
}

/// A sha for a diagnostic, where an empty one reads as unknown.
fn or_unknown(sha: &str) -> &str {
    if sha.is_empty() { "?" } else { sha }
}

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// The subjects.
// ---------------------------------------------------------------------------

/// The union of the pull request title and the branch's own subjects, where the
/// branch can only ever add the requirement.
///
/// The title is authoritative, since this repository squashes with it as the
/// commit subject, and title-only fails open: a pull request titled `chore:
/// tidy up` carrying a `feat(spate-core)` commit would escape. Merges are left
/// out, because `Merge branch 'main' into x` is unparseable and an unparseable
/// subject is not exempt.
fn subjects(root: &Path, range: &str, title: &str) -> Vec<Subject> {
    let mut out: Vec<Subject> = title
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(|line| Subject {
            text: line.to_owned(),
            origin: "pull request title".to_owned(),
            source: Source::Body,
        })
        .collect();

    let step = Step::new("git", ["log", "--no-merges", "--format=%s%x09%h", range]);
    let log = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    for line in log.split('\n').filter(|line| !line.is_empty()) {
        let (text, sha) = line.split_once('\t').unwrap_or((line, ""));
        if text.is_empty() {
            continue;
        }
        out.push(Subject {
            text: text.to_owned(),
            origin: format!("commit {sha}"),
            source: Source::Commit(sha.to_owned()),
        });
    }
    out
}

/// The subjects requiring a fragment with nothing excusing them, and how many
/// a trailer excused.
///
/// A trailer on a commit excuses that commit's subject alone, so the pull
/// request title is never excused here.
fn offenders(root: &Path, scratch: &Scratch, subjects: &[Subject]) -> (Vec<String>, usize) {
    let mut offenders = Vec::new();
    let mut excused = 0;
    for subject in subjects {
        if !needs_entry(&subject.text) {
            continue;
        }
        if let Source::Commit(sha) = &subject.source
            && commit_says_none(root, scratch, sha)
        {
            excused += 1;
            continue;
        }
        offenders.push(offender_line(&subject.text, &subject.origin));
    }
    (offenders, excused)
}

/// One offending subject, padded so the origins line up.
fn offender_line(subject: &str, origin: &str) -> String {
    let pad = " ".repeat(70usize.saturating_sub(subject.len()));
    format!("    {subject}{pad} ({origin})")
}

// ---------------------------------------------------------------------------
// The `Changelog: none` trailer.
// ---------------------------------------------------------------------------

/// Whether the pull request body carries `Changelog: none`, which is the
/// trailer the squash commit will carry.
fn body_says_none(root: &Path, scratch: &Scratch, body: &str) -> bool {
    if body.is_empty() {
        return false;
    }
    says_none(root, scratch, &format!("{body}\n"))
}

/// Whether this commit's own message carries `Changelog: none`.
fn commit_says_none(root: &Path, scratch: &Scratch, sha: &str) -> bool {
    let step = Step::new("git", ["log", "-1", "--format=%B", sha]);
    let message = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    says_none(root, scratch, &message)
}

/// Whether one message carries `Changelog: none`.
///
/// `git interpret-trailers --parse` decides. A body line like `Tests: the two
/// fault-injection knobs ...` starts a sentence, and a substring search would
/// read it as a trailer. One message at a time, because interpret-trailers takes
/// the trailers from the last block of its whole input.
fn says_none(root: &Path, scratch: &Scratch, message: &str) -> bool {
    trailer_says_none(&parse_trailers(root, scratch, message))
}

/// What `git interpret-trailers --parse` makes of a message, empty where it
/// could not be asked.
fn parse_trailers(root: &Path, scratch: &Scratch, message: &str) -> String {
    let path = scratch.join("message");
    if std::fs::write(&path, message).is_err() {
        return String::new();
    }
    let step = Step::new("git", ["interpret-trailers", "--parse"]).arg(path.to_string_lossy());
    match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    }
}

/// Whether a parsed trailer block carries `Changelog: none`, in any casing.
fn trailer_says_none(parsed: &str) -> bool {
    const KEY: &[u8] = b"changelog:";
    parsed.split('\n').any(|line| {
        let bytes = line.as_bytes();
        bytes.len() >= KEY.len()
            && bytes[..KEY.len()].eq_ignore_ascii_case(KEY)
            && trim_space(&line[KEY.len()..]).eq_ignore_ascii_case("none")
    })
}

// ---------------------------------------------------------------------------
// The fragments this change adds.
// ---------------------------------------------------------------------------

/// How many fragments this change adds, and which of the added ones say
/// nothing. A run with no head of its own counts the worktree too, where a
/// fragment written but not yet committed is one this change adds.
fn fragments_added(root: &Path, base: &str, head: Option<&str>) -> (usize, Vec<String>) {
    let (mut added, empty) = added_fragments(root, base, head);
    if head.is_none() {
        added += uncommitted_fragments(root);
    }
    (added, empty)
}

/// The fragments `base..head` adds, counted, with the ones that say nothing
/// named in their place.
///
/// Only an added fragment counts. Editing an existing one is not this change's
/// release note, and counting it would let a typo fix satisfy the gate.
fn added_fragments(root: &Path, base: &str, head: Option<&str>) -> (usize, Vec<String>) {
    let pathspec = format!("{FRAGMENTS}/");
    let step = Step::new(
        "git",
        [
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--name-only",
            "--diff-filter=A",
            base,
            head.unwrap_or("HEAD"),
            "--",
            &pathspec,
        ],
    );
    let listing = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };

    let mut added = 0;
    let mut empty = Vec::new();
    for file in listing.split('\n').filter(|f| !f.is_empty()) {
        if fragment_type(file).is_none() {
            continue;
        }
        // Reads the worktree: locally the file is there, and in CI the
        // checkout is of the head commit anyway.
        if root.join(file).is_file() && !fragment_has_prose(root, file) {
            empty.push(file.to_owned());
            continue;
        }
        added += 1;
    }
    (added, empty)
}

/// How many fragments are written but not yet committed. In CI the worktree is
/// a clean checkout.
fn uncommitted_fragments(root: &Path) -> usize {
    let pathspec = format!("{FRAGMENTS}/");
    let step = Step::new("git", ["status", "--porcelain", "--", &pathspec]);
    let listing = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    listing
        .split('\n')
        .filter(|line| !line.is_empty())
        // New files alone: `A ` staged-added and `??` untracked. A modified
        // fragment is somebody else's note being edited.
        .filter(|line| line.starts_with("A ") || line.starts_with("??"))
        .filter_map(|line| line.get(3..))
        .filter(|file| {
            fragment_type(file).is_some()
                && root.join(file).is_file()
                && fragment_has_prose(root, file)
        })
        .count()
}

/// The type embedded in a fragment filename, or nothing where the name is not
/// a fragment.
///
/// One level exactly: a release globs one level, so accepting a nested path
/// would let the gate pass on a fragment the release cannot see.
fn fragment_type(path: &str) -> Option<&str> {
    if path.matches('/').count() > 1 {
        return None;
    }
    let base = path.rsplit('/').next().unwrap_or(path);
    let stem = base.strip_suffix(".md")?;
    let kind = stem.rsplit_once('.')?.1;
    in_list(TYPES, kind).then_some(kind)
}

/// Whether a fragment says anything. An empty or whitespace-only file would
/// satisfy the gate and ship as an empty bullet.
fn fragment_has_prose(root: &Path, file: &str) -> bool {
    std::fs::read(root.join(file)).is_ok_and(|bytes| has_prose(&bytes))
}

/// A blank outside ASCII is whitespace here, so a fragment holding nothing else
/// is empty for the gate and for the release step that reads it again.
fn has_prose(bytes: &[u8]) -> bool {
    String::from_utf8_lossy(bytes)
        .chars()
        .any(|c| !c.is_whitespace())
}

#[cfg(test)]
mod tests;
