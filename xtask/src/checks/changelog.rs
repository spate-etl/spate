//! The changelog fragments: the gate, the scaffolder that writes one, the
//! release assembly that consumes them and adds the moved root dependency
//! requirements, one version's notes, and whether the release being prepared
//! announces a breaking change.
//!
//! A change somebody upgrading would care about carries a file under
//! `changelog.d/`, and `changelog.d/README.md` states the format and the
//! policy. The gate demands a fragment when the branch changes what a crate
//! ships, unless the pull request says `Changelog: none`. The assembly runs
//! once, at release, and rewrites `CHANGELOG.md` in place.

use std::path::Path;

mod requirements;

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

/// What a fragment opens with when its change breaks something. The release
/// derives a minor bump from it.
const BREAKING: &str = "**Breaking:**";

/// What a new fragment carries until its author writes the entry.
const TEMPLATE: &str = "\
**A short descriptive title** (`spate-crate`)

Start with what happens now, in present tense. Follow with what happened
previously, in past tense, then explain the practical consequence or required
action. For a new feature, include previous limitations only when useful.

Use short sentences and familiar words. Keep exact setting, type and metric
names readers need to find. Avoid idioms, implementation jargon and vague
claims. Include qualifications when they prevent a likely misunderstanding.
Usually write three to five sentences; add paragraphs for migration details.
Check current behavior against source and tests, and previous behavior against
history. Say \"in previous versions\" only for behavior verified in a release.

Delete this template text and write the entry. If the change is breaking, open
with `**Breaking:**`. See changelog.d/README.md for the guide and an example.
";

/// What the failure prints under the shipped paths.
const GUIDANCE: &str = "
  Add one with:

      cargo xtask changelog new fixed short-description

  and write what the change means for somebody upgrading, not what moved.
  changelog.d/README.md has the format and the conventions.

  If nobody upgrading would notice, as with a refactor, a test, a doc comment
  or a fix to a bug that was never released, put this line in the pull
  request body:

      Changelog: none

  A 'Changelog: none' trailer on a commit of the branch does the same before
  the pull request exists.";

/// How many shipped paths a refusal lists before summing up the rest.
const LISTED: usize = 10;

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
    body: String,
}

impl Fields {
    fn from_env() -> Self {
        Self {
            event: var("EVENT_NAME"),
            base_sha: var("BASE_SHA"),
            head_sha: var("HEAD_SHA"),
            body: var("PR_BODY"),
        }
    }
}

/// What the gate compares against.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Mode {
    /// The fragment requirement is not evaluated.
    Structure,
    /// The change `base..head`, where an absent head reads as the worktree.
    Require { base: String, head: Option<String> },
}

/// The gate: demands a fragment when the change touches what a crate ships.
pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads the paths this branch changes and {FRAGMENTS}/)");
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
             The job that runs this has to pass EVENT_NAME, BASE_SHA, HEAD_SHA and PR_BODY\n  \
             through `env:`, and its checkout needs `fetch-depth: 0`. See the `changelog`\n  \
             job in .github/workflows/ci.yml.",
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
    let shipped = shipped_changes(root, &base, head.as_deref());
    if shipped.is_empty() {
        println!("{TOOL}: nothing in {range} changes what a crate ships.");
        return Ok(());
    }

    if body_says_none(&fields.body) {
        println!("{TOOL}: the pull request body says 'Changelog: none'. Taken at its word.");
        return Ok(());
    }
    let scratch = Scratch::new("spate-xtask-changelog")?;
    if let Some(sha) = branch_says_none(root, &scratch, &range) {
        println!("{TOOL}: commit {sha} carries a 'Changelog: none' trailer. Taken at its word.");
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
            "{TOOL}: {} shipped file(s) changed, {added} fragment(s) added.",
            shipped.len()
        );
        return Ok(());
    }

    eprintln!("{TOOL}: this change touches what a crate ships, and no fragment was");
    eprintln!("  added under {FRAGMENTS}/:");
    eprintln!();
    for path in shipped.iter().take(LISTED) {
        eprintln!("    {path}");
    }
    if shipped.len() > LISTED {
        eprintln!("    ... and {} more", shipped.len() - LISTED);
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

/// Whether `item` appears in `list` as a space-delimited run.
fn in_list(list: &[&str], item: &str) -> bool {
    format!(" {} ", list.join(" ")).contains(&format!(" {item} "))
}

/// Trims the ASCII whitespace a field may carry.
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
// What the change ships.
// ---------------------------------------------------------------------------

/// The changed paths that reach somebody depending on a crate, sorted.
///
/// A crate's `src/`, `build.rs` and manifest, and the workspace manifest when
/// its `rust-version` line moved. With no head the worktree is compared, so an
/// uncommitted edit counts.
fn shipped_changes(root: &Path, base: &str, head: Option<&str>) -> Vec<String> {
    let mut step = Step::new("git", ["diff", "--no-ext-diff", "--name-only", base]);
    if let Some(head) = head {
        step = step.arg(head);
    }
    let listing = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    let mut out: Vec<String> = listing
        .split('\n')
        .filter(|path| ships(path))
        .map(str::to_owned)
        .collect();
    if rust_version_moved(root, base, head) {
        out.push("Cargo.toml (rust-version)".to_owned());
    }
    out.sort();
    out
}

/// Whether a path is part of what a published crate ships.
///
/// Every directory under `crates/` is a published crate. Unit tests live
/// under `src/` and count; `tests/`, `benches/` and `examples/` do not ship.
fn ships(path: &str) -> bool {
    let Some((_, inner)) = path
        .strip_prefix("crates/")
        .and_then(|rest| rest.split_once('/'))
    else {
        return false;
    };
    inner.starts_with("src/") || inner == "build.rs" || inner == "Cargo.toml"
}

/// Whether the workspace manifest's `rust-version` line changed.
fn rust_version_moved(root: &Path, base: &str, head: Option<&str>) -> bool {
    let mut step = Step::new("git", ["diff", "--no-ext-diff", "--unified=0", base]);
    if let Some(head) = head {
        step = step.arg(head);
    }
    let diff = match run::complete(root, &step.args(["--", "Cargo.toml"]), Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    diff_moves_rust_version(&diff)
}

/// Whether a unified diff adds or removes a `rust-version` line.
fn diff_moves_rust_version(diff: &str) -> bool {
    diff.split('\n').any(|line| {
        (line.starts_with('+') || line.starts_with('-'))
            && !line.starts_with("+++")
            && !line.starts_with("---")
            && line[1..]
                .trim_start_matches(is_space)
                .starts_with("rust-version")
    })
}

// ---------------------------------------------------------------------------
// `Changelog: none`.
// ---------------------------------------------------------------------------

/// Whether the pull request body has a line reading `Changelog: none`, in any
/// casing.
///
/// Matched per line and not as a git trailer: the body ends in the template's
/// own sections, which would hide a trailer written above them.
fn body_says_none(body: &str) -> bool {
    body.split('\n').map(trim_space).any(says_none_line)
}

/// The first commit in `range` whose message carries a `Changelog: none`
/// trailer.
fn branch_says_none(root: &Path, scratch: &Scratch, range: &str) -> Option<String> {
    let step = Step::new("git", ["log", "--no-merges", "--format=%h", range]);
    let listing = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    listing
        .split('\n')
        .filter(|sha| !sha.is_empty())
        .find(|sha| commit_says_none(root, scratch, sha))
        .map(str::to_owned)
}

/// Whether this commit's own message carries `Changelog: none`.
fn commit_says_none(root: &Path, scratch: &Scratch, sha: &str) -> bool {
    let step = Step::new("git", ["log", "-1", "--format=%B", sha]);
    let message = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout,
        _ => String::new(),
    };
    trailer_says_none(&parse_trailers(root, scratch, &message))
}

/// What `git interpret-trailers --parse` makes of a message, empty where it
/// could not be asked.
///
/// A body line like `Tests: the two fault-injection knobs ...` starts a
/// sentence, and a substring search would read it as a trailer.
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
    parsed.split('\n').any(says_none_line)
}

/// Whether one line is `Changelog:` followed by `none`, in any casing.
fn says_none_line(line: &str) -> bool {
    const KEY: &[u8] = b"changelog:";
    let bytes = line.as_bytes();
    bytes.len() >= KEY.len()
        && bytes[..KEY.len()].eq_ignore_ascii_case(KEY)
        && trim_space(&line[KEY.len()..]).eq_ignore_ascii_case("none")
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

// ---------------------------------------------------------------------------
// The release assembly.
// ---------------------------------------------------------------------------

/// The file a release is assembled into.
const CHANGELOG: &str = "CHANGELOG.md";

/// The repository every derived link points at.
const REPO_URL: &str = "https://github.com/spate-etl/spate";

/// Assembles the fragments and the moved root dependency requirements into a
/// new section of the changelog, in place, and consumes the fragments.
///
/// Everything that can fail happens before anything is written back, so a
/// refusal leaves the tree as it was.
pub(crate) fn build(root: &Path, explain: bool, version: &str) -> Outcome {
    if version.is_empty() {
        return Err(Error::msg("usage: cargo xtask changelog build <version>"));
    }
    if explain {
        println!("(writes ## [{version}] into {CHANGELOG} and consumes {FRAGMENTS}/)");
        return Ok(());
    }

    let path = root.join(CHANGELOG);
    if !path.is_file() {
        return Err(Error::msg(format!("{CHANGELOG} not found")));
    }
    let text =
        std::fs::read_to_string(&path).map_err(|e| Error::msg(format!("{CHANGELOG}: {e}")))?;

    if !records(&text).contains(&"## [Unreleased]") {
        return Err(Error::msg(format!(
            "no '## [Unreleased]' heading in {CHANGELOG}. The new release is inserted below\n  \
             it, so a release that removed it has to put it back, empty, before the next one."
        )));
    }
    if text.contains(&format!("## [{version}]")) {
        return Err(Error::msg(format!(
            "{CHANGELOG} already has a '## [{version}]' section. Pick the next version,\n  \
             or if the previous attempt failed part-way, undo it before running this again."
        )));
    }
    if !unreleased_is_empty(&text) {
        return Err(Error::msg(format!(
            "the '## [Unreleased]' section in {CHANGELOG} is not empty.\n\n  \
             The assembly reads {FRAGMENTS}/, and anything written under that heading by\n  \
             hand would be swept into '## [{version}]' below the link definitions rather than\n  \
             read as part of it. Move it into a fragment, one file per entry, typed by its\n  \
             Keep a Changelog section, and run this again."
        )));
    }

    for file in fragment_names(root) {
        if !fragment_has_prose(root, &file) {
            return Err(Error::msg(format!(
                "{file} is empty. A fragment is the release note: write it, or delete the file."
            )));
        }
    }

    let today = today(root)?;
    let previous = previous_tag(root);
    let range = previous
        .as_ref()
        .map_or_else(|| "HEAD".to_owned(), |tag| format!("{tag}..HEAD"));
    let generated = requirements::entry(root, previous.as_deref())?;
    let block = assemble(
        root,
        &range,
        previous.as_deref(),
        generated.as_deref(),
        &api_pull,
    )?;
    let written = insert(&text, version, &today, &block)?;

    for kind in TYPES {
        for file in fragments_of(root, kind) {
            if !tracked(root, &file) {
                return Err(Error::msg(format!(
                    "{file} is not tracked. Commit it before assembling a release:\n  \
                     a fragment that never reached git is not part of what is being released."
                )));
            }
        }
    }

    std::fs::write(&path, &written).map_err(|e| Error::msg(format!("{CHANGELOG}: {e}")))?;

    for kind in TYPES {
        for file in fragments_of(root, kind) {
            run::run(
                root,
                false,
                &Step::new("git", ["rm", "--quiet", "--force", &file]),
            )?;
        }
    }

    print!("{}", summary(version, &today));
    Ok(())
}

/// What a finished assembly reports.
fn summary(version: &str, today: &str) -> String {
    format!(
        "{TOOL}: wrote ## [{version}] — {today} into {CHANGELOG} and consumed the fragments.\n  Read what it wrote before committing: the assembly is mechanical, the release note is not.\n"
    )
}

/// Prints one version's section on stdout, for the release body. The heading is
/// dropped because the release title already carries the version.
pub(crate) fn notes(root: &Path, explain: bool, version: &str) -> Outcome {
    if version.is_empty() {
        return Err(Error::msg("usage: cargo xtask changelog notes <version>"));
    }
    if explain {
        println!("(prints the ## [{version}] section of {CHANGELOG})");
        return Ok(());
    }
    let path = root.join(CHANGELOG);
    if !path.is_file() {
        return Err(Error::msg(format!("{CHANGELOG} not found")));
    }
    let text =
        std::fs::read_to_string(&path).map_err(|e| Error::msg(format!("{CHANGELOG}: {e}")))?;
    print!("{}", section_notes(&text, version, CHANGELOG)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// One version's section.
// ---------------------------------------------------------------------------

/// What the scan found instead of one section.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Scan {
    Missing,
    Duplicate,
}

/// The body of one release's section: everything between its heading and the
/// next one, the heading itself excluded.
///
/// The slice is self-contained because the assembly writes each section's
/// `[#N]` definitions inside it; the definitions at the foot of the file belong
/// to the headings, and the slice drops those. Every reference the slice uses
/// must be defined inside it, or the release body renders the literal text.
fn section_notes(text: &str, version: &str, file: &str) -> Result<String, Error> {
    let raw = scan(text, &format!("## [{version}] ")).map_err(|what| match what {
        Scan::Missing => Error::msg(format!(
            "no '## [{version}]' section in {file}. The notes read what the assembly wrote,\n  \
             so the release is assembled first."
        )),
        Scan::Duplicate => Error::msg(format!(
            "two '## [{version}]' headings in {file}. A part-finished assembly has to be\n  \
             undone before its section can be read."
        )),
    })?;

    let body = opening_blanks_dropped(&raw);
    if body.is_empty() {
        return Err(Error::msg(format!(
            "the '## [{version}]' section in {file} is empty"
        )));
    }

    let mut used = issue_references(body);
    used.sort_unstable();
    used.dedup();
    for number in used {
        let definition = format!("[#{number}]: ");
        if !body.split('\n').any(|line| line.starts_with(&definition)) {
            return Err(Error::msg(format!(
                "the '## [{version}]' section uses [#{number}] with no definition in the section"
            )));
        }
    }
    Ok(format!("{body}\n"))
}

/// The lines between the heading and the boundary that ends the section.
///
/// The last section is followed by the link foot, so the `[Unreleased]: ` line
/// terminates a section too. Fenced code is opaque, and a hand-edited section
/// may quote a heading-shaped or foot-shaped line inside one. Both fence kinds
/// toggle one state, so a fence of one kind holding the other kind's marker at
/// column zero is not modelled. A second heading for the same version is the
/// part-finished assembly, and the scan refuses it.
fn scan(text: &str, heading: &str) -> Result<String, Scan> {
    let mut out = String::new();
    let (mut fence, mut found, mut in_section) = (false, false, false);
    for line in records(text) {
        if is_fence(line) {
            if in_section {
                out.push_str(line);
                out.push('\n');
            }
            fence = !fence;
            continue;
        }
        if fence {
            if in_section {
                out.push_str(line);
                out.push('\n');
            }
            continue;
        }
        if line.starts_with(heading) {
            if found {
                return Err(Scan::Duplicate);
            }
            found = true;
            in_section = true;
            continue;
        }
        if in_section && (line.starts_with("## ") || line.starts_with("[Unreleased]: ")) {
            in_section = false;
            continue;
        }
        if in_section {
            out.push_str(line);
            out.push('\n');
        }
    }
    if found { Ok(out) } else { Err(Scan::Missing) }
}

/// A fenced-code delimiter: up to three spaces of indent, then either marker.
fn is_fence(line: &str) -> bool {
    let rest = line.trim_start_matches(' ');
    line.len() - rest.len() <= 3 && (rest.starts_with("```") || rest.starts_with("~~~"))
}

/// The slice with its opening blank lines and its trailing newlines dropped. A
/// line carrying a space carries something.
fn opening_blanks_dropped(raw: &str) -> &str {
    let mut body = raw.trim_end_matches('\n');
    while let Some(rest) = body.strip_prefix('\n') {
        body = rest;
    }
    body
}

// ---------------------------------------------------------------------------
// The block one release renders.
// ---------------------------------------------------------------------------

/// Where an entry carrying no reference of its own points.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Reference {
    Pull(String),
    Commit(String),
}

/// How a commit's merged pull request is looked up.
type Lookup<'a> = &'a dyn Fn(&Path, &str) -> Result<Option<String>, Error>;

/// The entries grouped by type in the order the six are declared, the
/// contributors over the range, and the link definitions the entries use.
/// `generated` closes the `changed` group and takes no reference.
fn assemble(
    root: &Path,
    range: &str,
    previous: Option<&str>,
    generated: Option<&str>,
    lookup: Lookup<'_>,
) -> Result<String, Error> {
    let mut block = String::new();
    let mut links: Vec<String> = Vec::new();

    for kind in TYPES {
        let mut open = false;
        for file in fragments_of(root, kind) {
            if !open {
                open_group(&mut block, kind);
                open = true;
            }

            let text = std::fs::read_to_string(root.join(&file))
                .map_err(|e| Error::msg(format!("{file}: {e}")))?;
            let mut body = entry_body(&text);

            // Every `[#N]` in the prose gets a definition regardless, or it
            // renders as literal text. Only a trailing one skips deriving.
            for number in issue_references(&body) {
                links.push(link_line(number));
            }

            if !ends_with_reference(&body) {
                // Each form goes on its own line. A fragment may end in a
                // fenced code block, and CommonMark allows only whitespace after
                // a closing fence, so appending leaves it unclosed and swallows
                // every section below.
                match fragment_reference(root, &file, lookup)? {
                    Some(Reference::Pull(number)) => {
                        links.push(link_line(&number));
                        body = format!("{body}\n([#{number}])");
                    }
                    // An inline link. The definition list holds `[#N]` alone and
                    // sorts on that number.
                    Some(Reference::Commit(sha)) => {
                        let short = sha.get(..7).unwrap_or(&sha);
                        body = format!("{body}\n([`{short}`]({REPO_URL}/commit/{sha}))");
                    }
                    None => {}
                }
            }

            block.push_str(&bullet(&body));
        }
        if *kind == "changed"
            && let Some(body) = generated
        {
            if !open {
                open_group(&mut block, kind);
            }
            block.push_str(&bullet(&entry_body(body)));
        }
    }

    if block.is_empty() {
        return Err(Error::msg(format!(
            "no fragments in {FRAGMENTS}/, so nothing to release.\n  \
             Every user-visible change since {} should have left one; if the release\n  \
             genuinely contains none, write the section by hand and say why in the commit.",
            previous.unwrap_or_default()
        )));
    }

    // Contributors over the whole range, not only the ones who left a fragment.
    let people = contributors(root, range);
    if !people.is_empty() {
        block.push_str("\n### Contributors\n\n");
        for name in people {
            block.push_str(&format!("- {name}\n"));
        }
    }

    if !links.is_empty() {
        block.push('\n');
        for line in sorted_links(links) {
            block.push_str(&line);
            block.push('\n');
        }
    }

    // The insertion line already supplies the separator.
    Ok(format!("{}\n", block.trim_end_matches('\n')))
}

/// Writes the heading that opens one type's group.
fn open_group(block: &mut String, kind: &str) {
    // The leading blank separates this group from the last one. A blank line
    // between list items makes it a *loose* list, and every bullet then renders
    // in its own paragraph.
    if !block.is_empty() {
        block.push('\n');
    }
    block.push_str(&format!("### {}\n\n", sentence_case(kind)));
}

/// The changelog with the new section written below the Unreleased heading and
/// the two link references at the foot rewritten.
fn insert(text: &str, version: &str, today: &str, block: &str) -> Result<String, Error> {
    let mut out = String::new();
    let (mut inserted, mut rewritten) = (false, false);
    for line in records(text) {
        if line == "## [Unreleased]" {
            out.push_str(&format!("{line}\n\n## [{version}] — {today}\n\n{block}"));
            inserted = true;
            continue;
        }
        if line.starts_with("[Unreleased]: ") {
            out.push_str(&format!(
                "[Unreleased]: {REPO_URL}/compare/v{version}...HEAD\n\
                 [{version}]: {REPO_URL}/releases/tag/v{version}\n"
            ));
            rewritten = true;
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    if !inserted {
        return Err(Error::msg("the Unreleased heading vanished mid-write"));
    }
    if !rewritten {
        return Err(Error::msg("no [Unreleased]: link reference to rewrite"));
    }
    Ok(out)
}

/// Whether the Unreleased section holds anything.
///
/// Anything under it would fall through the insertion into the new release, out
/// of section order and dated into a version it was not part of, leaving its own
/// heading empty.
fn unreleased_is_empty(text: &str) -> bool {
    let mut seen = false;
    for line in records(text) {
        if line == "## [Unreleased]" {
            seen = true;
            continue;
        }
        if !seen {
            continue;
        }
        if line.starts_with("## ") {
            break;
        }
        if line.chars().any(|c| !is_space(c)) {
            return false;
        }
    }
    true
}

/// One fragment's prose: trailing whitespace off every line, and no blank line
/// at either end.
fn entry_body(text: &str) -> String {
    let stripped: Vec<&str> = text
        .split('\n')
        .map(|line| line.trim_end_matches(is_space))
        .collect();
    let mut lines = stripped.as_slice();
    while lines.last().is_some_and(|line| line.is_empty()) {
        lines = &lines[..lines.len() - 1];
    }
    while lines.first().is_some_and(|line| line.is_empty()) {
        lines = &lines[1..];
    }
    lines.join("\n")
}

/// The entry as a list item. Blank lines stay blank, since indenting them
/// leaves trailing whitespace and makes the section a *loose* list.
fn bullet(body: &str) -> String {
    let mut out = String::new();
    for (position, line) in body.split('\n').enumerate() {
        if position == 0 {
            out.push_str("- ");
        } else {
            out.push('\n');
            if !line.is_empty() {
                out.push_str("  ");
            }
        }
        out.push_str(line);
    }
    out.push('\n');
    out
}

/// Sentence case for a heading, as Keep a Changelog spells them.
fn sentence_case(word: &str) -> String {
    let mut chars = word.chars();
    chars.next().map_or_else(String::new, |first| {
        format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
    })
}

/// The definition one `[#N]` needs.
fn link_line(number: &str) -> String {
    format!("[#{number}]: {REPO_URL}/pull/{number}")
}

/// The definitions, each one once, ordered by the number they define.
///
/// Whole lines are deduplicated before the numeric order is applied. Doing both
/// at once compares only the numeric key, so `[#031]` and `[#31]` collapse to
/// one and a definition is dropped.
fn sorted_links(mut links: Vec<String>) -> Vec<String> {
    links.sort();
    links.dedup();
    links.sort_by(|a, b| numeric_key(a).cmp(&numeric_key(b)).then_with(|| a.cmp(b)));
    links
}

/// The number read from the field following the first `#`.
fn numeric_key(line: &str) -> u64 {
    let after = line.split_once('#').map_or("", |(_, rest)| rest);
    after
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or_default()
        .parse()
        .unwrap_or(0)
}

/// Every `[#N]` the text carries, in the order they appear, as the digits alone.
fn issue_references(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    while at + 2 < bytes.len() {
        if bytes[at] != b'[' || bytes[at + 1] != b'#' {
            at += 1;
            continue;
        }
        let start = at + 2;
        let mut end = start;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        if end > start && bytes.get(end) == Some(&b']') {
            out.push(&text[start..end]);
            at = end + 1;
        } else {
            at += 1;
        }
    }
    out
}

/// Whether the entry ends in a `([#N])` of its own, which stands in for the
/// derived reference.
///
/// Anchored to the end, so a mid-sentence citation of an earlier pull request
/// is not read as this entry's reference.
fn ends_with_reference(body: &str) -> bool {
    let last = body.rsplit('\n').next().unwrap_or(body);
    let Some(head) = last.trim_end_matches(is_space).strip_suffix("])") else {
        return false;
    };
    let digits = trailing_digits(head);
    digits > 0 && head[..head.len() - digits].ends_with("([#")
}

/// The pull request number GitHub appends to a squash subject. A `(#12)`
/// written mid-subject cites another pull request and is not read, and a subject
/// ending in two is read as the last of them.
fn pr_from_subject(subject: &str) -> Option<&str> {
    let head = subject.strip_suffix(')')?;
    let digits = trailing_digits(head);
    if digits == 0 {
        return None;
    }
    let (before, number) = head.split_at(head.len() - digits);
    before.ends_with("(#").then_some(number)
}

/// How many ASCII digits the text ends in.
fn trailing_digits(text: &str) -> usize {
    text.len() - text.trim_end_matches(|c: char| c.is_ascii_digit()).len()
}

// ---------------------------------------------------------------------------
// What an entry with no reference of its own points at.
// ---------------------------------------------------------------------------

/// The pull request that merged the fragment, or the commit that added it.
///
/// Three sources in order. A squash subject ends in `(#N)`. A rebase merge
/// appends nothing, so the adding commit goes to `lookup` next. A commit that
/// reached the default branch outside a pull request links to itself. A fragment
/// with no history, written but not yet committed, has no reference at all.
fn fragment_reference(
    root: &Path,
    file: &str,
    lookup: Lookup<'_>,
) -> Result<Option<Reference>, Error> {
    let Some(sha) = capture(root, &["log", "--diff-filter=A", "--format=%H", "--", file]) else {
        return Ok(None);
    };
    let subject =
        capture(root, &["log", "--diff-filter=A", "--format=%s", "--", file]).unwrap_or_default();
    if let Some(number) = pr_from_subject(&subject) {
        return Ok(Some(Reference::Pull(number.to_owned())));
    }
    Ok(Some(match lookup(root, &sha)? {
        Some(number) => Reference::Pull(number),
        None => Reference::Commit(sha),
    }))
}

/// The merged pull request the API associates with a commit. Merged ones only,
/// and the first of them, because a commit can also be associated with one that
/// never landed.
///
/// Absent `gh` nothing is asked, and the commit link answers instead.
fn api_pull(root: &Path, sha: &str) -> Result<Option<String>, Error> {
    if !run::on_path("gh") {
        return Ok(None);
    }
    // `run::complete` discards stderr, and the refusal below quotes it.
    let out = std::process::Command::new("gh")
        .args(pulls_query(sha))
        .current_dir(root)
        .output()
        .map_err(|e| Error::msg(format!("gh: {e}")))?;
    classify(
        out.status.success(),
        &String::from_utf8_lossy(&out.stdout),
        &String::from_utf8_lossy(&out.stderr),
        sha,
    )
}

/// The `gh` arguments that ask which pull requests a commit belongs to.
fn pulls_query(sha: &str) -> [String; 4] {
    let slug = REPO_URL
        .strip_prefix("https://github.com/")
        .unwrap_or(REPO_URL);
    [
        "api".to_owned(),
        format!("repos/{slug}/commits/{sha}/pulls"),
        "--jq".to_owned(),
        "map(select(.merged_at)) | first | .number // empty".to_owned(),
    ]
}

/// What one lookup's answer means.
///
/// On an HTTP error `gh api` exits non-zero and prints the response body to
/// stdout, so the answer is used only when the call succeeded and it is a
/// number. A commit the API does not know (assembled locally, never pushed)
/// takes the commit link; any other failure aborts, because a bad token would
/// otherwise turn every derived reference into a commit link with nothing
/// saying so.
fn classify(ok: bool, stdout: &str, stderr: &str, sha: &str) -> Result<Option<String>, Error> {
    let answer = stdout.trim_end_matches('\n');
    let short = sha.get(..12).unwrap_or(sha);
    if ok {
        if answer.is_empty() {
            return Ok(None);
        }
        if !answer.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::msg(format!(
                "the pull-request lookup for {short} answered with something that is\n  \
                 not a number: {answer}"
            )));
        }
        return Ok(Some(answer.to_owned()));
    }
    if answer.contains("No commit found")
        || answer.contains("\"status\": \"422\"")
        || answer.contains("\"status\":\"422\"")
    {
        return Ok(None);
    }
    Err(Error::msg(format!(
        "the pull-request lookup for {short} failed rather than answering:\n  \
         {answer} {}\n  \
         Fix the token or the network and assemble again; falling back to a\n  \
         commit link here would look identical to a commit that has no pull request.",
        stderr.trim_end_matches('\n')
    )))
}

// ---------------------------------------------------------------------------
// What the release reads off the tree.
// ---------------------------------------------------------------------------

/// Every fragment under `changelog.d/`, sorted, as repository-relative paths. A
/// name opening with a dot is not one.
fn fragment_names(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root.join(FRAGMENTS)) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .flatten()
        .map(|entry| format!("{FRAGMENTS}/{}", entry.file_name().to_string_lossy()))
        .filter(|path| {
            !path.starts_with(&format!("{FRAGMENTS}/.")) && fragment_type(path).is_some()
        })
        .collect();
    out.sort();
    out
}

/// The fragments of one type, sorted.
fn fragments_of(root: &Path, kind: &str) -> Vec<String> {
    fragment_names(root)
        .into_iter()
        .filter(|path| fragment_type(path) == Some(kind))
        .collect()
}

/// Whether git has this path in the index.
fn tracked(root: &Path, file: &str) -> bool {
    let step = Step::new("git", ["ls-files", "--error-unmatch", file]);
    matches!(
        run::complete(root, &step, Streams::Discard),
        Ok(Completed { code: 0, .. })
    )
}

/// The newest release tag. `v[0-9]*` keeps a non-release tag such as `vnext`
/// from standing in for one.
fn previous_tag(root: &Path) -> Option<String> {
    capture(root, &["tag", "--list", "v[0-9]*", "--sort=-v:refname"])
}

/// The names on the commits in `range`, bots left out.
fn contributors(root: &Path, range: &str) -> Vec<String> {
    let step = Step::new("git", ["shortlog", "-sn", range]);
    let listing = run::complete(root, &step, Streams::Collect)
        .map(|done| done.stdout)
        .unwrap_or_default();
    listing
        .split('\n')
        .map(shortlog_name)
        .filter(|name| !name.is_empty() && !name.ends_with("[bot]"))
        .map(str::to_owned)
        .collect()
}

/// The name on a `git shortlog -sn` line, where the count is dropped.
fn shortlog_name(line: &str) -> &str {
    let rest = line.trim_start_matches(is_space);
    let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    if digits == 0 {
        return line;
    }
    rest[digits..].trim_start_matches(is_space)
}

/// Today's date in UTC, as the section heading spells it.
fn today(root: &Path) -> Result<String, Error> {
    let out = run::capture(root, &Step::new("date", ["-u", "+%Y-%m-%d"]))?;
    Ok(out.trim_end_matches('\n').to_owned())
}

/// Prints whether the release being prepared announces a breaking change.
pub(crate) fn breaking(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads {FRAGMENTS}/ and {CHANGELOG} at HEAD)");
        return Ok(());
    }
    println!(
        "{}",
        if breaking_announced(root)? {
            "breaking"
        } else {
            "none"
        }
    );
    Ok(())
}

/// Whether the release being prepared announces a breaking change: a fragment
/// at `HEAD` opening with `**Breaking:**`, or, while the workspace version is
/// ahead of the newest tag, that marker inside the version's own section of
/// `CHANGELOG.md`.
///
/// The second arm covers the window between a release merge, which consumes
/// the fragments, and its tag. Reads `HEAD` through git, so an uncommitted
/// fragment does not count.
pub(crate) fn breaking_announced(root: &Path) -> Result<bool, Error> {
    let listing = run::capture(
        root,
        &Step::new("git", ["ls-tree", "--name-only", "HEAD"]).arg(format!("{FRAGMENTS}/")),
    )?;
    for file in listing.split('\n').filter(|f| fragment_type(f).is_some()) {
        let text = run::capture(
            root,
            &Step::new("git", ["show"]).arg(format!("HEAD:{file}")),
        )?;
        if opens_breaking(&text) {
            return Ok(true);
        }
    }

    let manifest = run::capture(root, &Step::new("git", ["show", "HEAD:Cargo.toml"]))?;
    let version = crate::checks::semver_checks::workspace_version(&manifest);
    let Some(tag) = previous_tag(root) else {
        return Ok(false);
    };
    if version.is_empty() || tag == format!("v{version}") {
        return Ok(false);
    }
    let changelog = run::capture(
        root,
        &Step::new("git", ["show"]).arg(format!("HEAD:{CHANGELOG}")),
    )?;
    Ok(scan(&changelog, &format!("## [{version}] "))
        .is_ok_and(|section| section.contains(BREAKING)))
}

/// Whether a fragment's text opens with the breaking marker.
fn opens_breaking(text: &str) -> bool {
    text.trim_start().starts_with(BREAKING)
}

/// The records a line-oriented scan reads. A final newline closes the last
/// record.
fn records(text: &str) -> Vec<&str> {
    let mut out: Vec<&str> = text.split('\n').collect();
    if out.last().is_some_and(|line| line.is_empty()) {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests;
