//! The subject rule, `area: description`, checked on a commit by the
//! commit-msg hook and on a pull request title in CI.
//!
//! The pull request title is what lands on `main`: the repository squashes
//! with the title as the subject and an empty body, and the merge appends
//! ` (#N)`. A title is therefore held to the limit less that suffix.

use std::path::Path;

use crate::run::{self, Completed, Error, Outcome, Step, Streams};

/// The prefix on every line this check writes for itself.
const TOOL: &str = "subject";

/// The areas that are not a crate. A crate's area is its directory name under
/// `crates/` with the `spate-` prefix dropped.
const AREAS: &[&str] = &["workspace", "ci", "docs", "examples", "bench", "website"];

/// The longest subject `git log --oneline` shows without wrapping.
const LIMIT: usize = 72;

/// The subject of a release commit, which the publish job triggers on. Only
/// `release: vX.Y.Z` uses it, so no other title can reach that trigger.
const RELEASE: &str = "release: v";

/// The subjects git writes itself, which a squash merge discards.
const GENERATED: &[&str] = &["fixup! ", "squash! ", "amend! ", "Merge ", "Revert \""];

/// Where a failure points for the rule in full.
const GUIDANCE: &str = "
  A subject names one area and says what the change does:

      kafka: start a partition's fetcher before its lane is handed out
      core: `Clock` moves into spate_core::clock

  The description starts lowercase unless it opens with a `code` reference,
  and has no trailing period. CONTRIBUTING.md has the rule in full.";

/// The areas a subject may name, crates first in directory order.
pub(crate) fn areas(root: &Path) -> Vec<String> {
    let mut crates: Vec<String> = std::fs::read_dir(root.join("crates"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().is_dir())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| !name.starts_with('.'))
                .map(|name| match name.strip_prefix("spate-") {
                    Some(short) => short.to_owned(),
                    None => name,
                })
                .collect()
        })
        .unwrap_or_default();
    crates.sort();
    crates.extend(AREAS.iter().map(|area| (*area).to_owned()));
    crates
}

/// What is wrong with a subject, one line per problem, or nothing.
///
/// `limit` is the longest the subject may be, in characters; `None` waives
/// the length.
fn problems(subject: &str, areas: &[String], limit: Option<usize>) -> Vec<String> {
    let mut out = Vec::new();
    let length = subject.chars().count();
    if let Some(limit) = limit
        && length > limit
    {
        out.push(format!(
            "it is {length} characters, over the limit of {limit}"
        ));
    }

    if let Some(version) = subject.strip_prefix(RELEASE) {
        if !is_version(version) {
            out.push("`release:` is reserved for the release commit, `release: vX.Y.Z`".to_owned());
        }
        return out;
    }

    let Some((area, description)) = subject.split_once(": ") else {
        out.push("it names no area: it has to start with `area: `".to_owned());
        return out;
    };
    if area.contains(['(', ')', '!', ',', ' ']) {
        out.push(format!(
            "`{area}` is not one area: name one, with no type, scope list or `!`"
        ));
    } else if area == "release" {
        out.push("`release:` is reserved for the release commit, `release: vX.Y.Z`".to_owned());
    } else if !areas.iter().any(|known| known == area) {
        out.push(format!(
            "`{area}` is not an area. The areas are: {}",
            areas.join(" ")
        ));
    }

    match description.chars().next() {
        None => out.push("the description after the area is empty".to_owned()),
        Some(c) if c.is_whitespace() => {
            out.push("the description starts with whitespace".to_owned());
        }
        Some(c) if c.is_ascii_uppercase() => out.push(
            "the description starts with a capital: lowercase, unless it opens with a `code` reference"
                .to_owned(),
        ),
        Some(_) => {}
    }
    if description.ends_with('.') {
        out.push("it ends with a period".to_owned());
    }
    out
}

/// Whether `text` is `X.Y.Z` with every component a run of ASCII digits.
fn is_version(text: &str) -> bool {
    let parts: Vec<&str> = text.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// The subject of a commit message as git will record it: the first line that
/// is neither blank nor a comment.
fn subject_of(message: &str, comment: char) -> &str {
    message
        .split('\n')
        .map(|line| line.trim_end_matches('\r'))
        .find(|line| !line.trim().is_empty() && !line.starts_with(comment))
        .unwrap_or_default()
}

/// The character git strips comment lines by. `auto` picks one per message,
/// and `#` is the one it starts from.
fn comment_char(root: &Path) -> char {
    let step = Step::new("git", ["config", "--get", "core.commentChar"]);
    let value = match run::complete(root, &step, Streams::Collect) {
        Ok(Completed { code: 0, stdout }) => stdout.trim_end_matches('\n').to_owned(),
        _ => String::new(),
    };
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => c,
        _ => '#',
    }
}

/// `text` with each control character escaped, and the second `#` of `##[`
/// escaped too. The Actions runner reads a `\r` as a line break, a line
/// opening with `::` is a workflow command, and so is `##[` anywhere in a line.
fn printable(text: &str) -> String {
    let escaped: String = text
        .chars()
        .map(|c| {
            if c.is_control() {
                c.escape_default().to_string()
            } else {
                c.to_string()
            }
        })
        .collect();
    escaped.replace("##[", "#\\u{23}[")
}

/// Prints a refusal and the rule, and fails.
fn refuse(subject: &str, found: &[String]) -> Outcome {
    eprintln!("{TOOL}: {}", printable(subject));
    for problem in found {
        eprintln!("  - {}", printable(problem));
    }
    eprintln!("{GUIDANCE}");
    Err(Error::status(1))
}

/// The commit-msg hook: checks the subject of the message git is about to
/// record. A subject git generates is let through.
pub(crate) fn commit_msg(root: &Path, explain: bool, file: &Path) -> Outcome {
    if explain {
        println!("(reads the subject of {})", file.display());
        return Ok(());
    }
    let message = std::fs::read_to_string(file)
        .map_err(|e| Error::msg(format!("{}: {e}", file.display())))?;
    let subject = subject_of(&message, comment_char(root));
    if subject.is_empty() || GENERATED.iter().any(|prefix| subject.starts_with(prefix)) {
        return Ok(());
    }
    let found = problems(subject, &areas(root), Some(LIMIT));
    if found.is_empty() {
        return Ok(());
    }
    refuse(subject, &found)
}

/// The fields a pull request run reads, all of them free text somebody else
/// typed. They are matched against and never evaluated.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct Fields {
    event: String,
    title: String,
    number: String,
    author: String,
}

impl Fields {
    fn from_env() -> Self {
        Self {
            event: var("EVENT_NAME"),
            title: var("PR_TITLE"),
            number: var("PR_NUMBER"),
            author: var("PR_AUTHOR"),
        }
    }
}

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

/// The gate on a pull request title.
pub(crate) fn check_title(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads PR_TITLE, PR_NUMBER and PR_AUTHOR)");
        return Ok(());
    }
    title_gate(
        root,
        &Fields::from_env(),
        std::env::var_os("GITHUB_ACTIONS").is_some(),
        &var("GITHUB_EVENT_NAME"),
    )
}

/// The gate's verdict over one set of fields and one runner state.
fn title_gate(root: &Path, fields: &Fields, in_actions: bool, github_event: &str) -> Outcome {
    let base_only = github_event == "pull_request_target";
    if fields.event != "pull_request" {
        if in_actions && (github_event == "pull_request" || base_only) {
            return Err(Error::msg(
                "running on a pull request inside GitHub Actions with no EVENT_NAME, so this\n  \
                 would have checked nothing and passed. The job that runs this has to pass\n  \
                 EVENT_NAME, PR_TITLE, PR_NUMBER and PR_AUTHOR through `env:`.",
            ));
        }
        println!(
            "{TOOL}: no pull request title to check (EVENT_NAME='{}').",
            fields.event
        );
        return Ok(());
    }
    if fields.number.is_empty() || !fields.number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::msg(format!(
            "PR_NUMBER is '{}', not a pull request number. The limit counts the ' (#N)'\n  \
             the merge appends, so the job has to pass it through `env:`.",
            fields.number
        )));
    }
    // Dependabot writes its own titles, and a retitle does not survive its
    // next rebase.
    let limit = (fields.author != "dependabot[bot]")
        .then(|| LIMIT - format!(" (#{})", fields.number).len());
    if base_only {
        println!(
            "{TOOL}: the areas are the ones on `main`. A crate this pull request adds\n  \
             is an area in its CI run, which checks the title again."
        );
    }
    let found = problems(&fields.title, &areas(root), limit);
    if found.is_empty() {
        println!("{TOOL}: the pull request title follows the rule.");
        return Ok(());
    }
    refuse(&fields.title, &found)
}

#[cfg(test)]
mod tests;
