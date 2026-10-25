//! Issue bodies laid out as the repository's issue forms: the bug form's
//! fields, the `###` sections GitHub renders a form submission as, and
//! `cargo xtask bug-report`, which writes a bug-form body from answers given
//! on the command line.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use crate::run::{Error, Outcome};

/// The arguments of `cargo xtask bug-report`.
#[derive(clap::Args)]
pub(crate) struct BugReport {
    /// A field's answer, as ID=TEXT, where ID is a field id of
    /// .github/ISSUE_TEMPLATE/2-bug.yml
    #[arg(long = "answer", value_name = "ID=TEXT")]
    answers: Vec<String>,
    /// A field's answer read from a file, as ID=PATH; past 25,000 characters
    /// only the tail is kept. A field given both an --answer and an
    /// --answer-file takes the text first and the file after it
    #[arg(long = "answer-file", value_name = "ID=PATH")]
    answer_files: Vec<String>,
    /// Where the body is written
    #[arg(long, value_name = "PATH")]
    out: PathBuf,
}

/// Writes a body laid out as the bug form to `--out`, with a section for each
/// answered field and none for the rest.
///
/// A field may take one text and one file answer. The text comes first and
/// only the file part is cut to [`MAX_LOG_CHARS`].
///
/// # Errors
///
/// An answer not written as `ID=VALUE`, an id that is no field of the form, a
/// field given two texts or two files, or a file that cannot be read or
/// written.
pub(crate) fn bug_report(root: &Path, explain: bool, args: &BugReport) -> Outcome {
    if explain {
        println!("(renders the body in process)");
        return Ok(());
    }
    let mut answers: Vec<(&str, String)> = Vec::new();
    for arg in &args.answers {
        let (id, text) = split(arg)?;
        answers.push((id, text.to_owned()));
    }
    let mut from_file = Vec::new();
    for arg in &args.answer_files {
        let (id, path) = split(arg)?;
        if from_file.contains(&id) {
            return Err(Error::msg(format!("`{id}` is answered by two files")));
        }
        from_file.push(id);
        let path = root.join(path);
        let bytes = fs::read(&path).map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
        let log = tail(&String::from_utf8_lossy(&bytes), MAX_LOG_CHARS);
        // A text answer to the same field leads, so the cap cannot cut it.
        match answers.iter_mut().find(|(other, _)| *other == id) {
            Some((_, text)) => {
                text.push_str("\n\n");
                text.push_str(&log);
            }
            None => answers.push((id, log)),
        }
    }
    let body = answered(&BUG, &answers)?;
    let out = root.join(&args.out);
    fs::write(&out, body).map_err(|e| Error::msg(format!("{}: {e}", out.display())))
}

fn split(arg: &str) -> Result<(&str, &str), Error> {
    arg.split_once('=')
        .ok_or_else(|| Error::msg(format!("`{arg}` is not written as ID=VALUE")))
}

/// The sections of `fields` that `answers` answers, in the form's order. A
/// field with no answer, or an empty one, is left out.
fn answered(fields: &[Field], answers: &[(&str, String)]) -> Result<String, Error> {
    for (i, (id, _)) in answers.iter().enumerate() {
        if !fields.iter().any(|f| f.id == *id) {
            let ids: Vec<&str> = fields.iter().map(|f| f.id).collect();
            return Err(Error::msg(format!(
                "the form has no field `{id}`; its fields are {}",
                ids.join(", ")
            )));
        }
        if answers[..i].iter().any(|(other, _)| other == id) {
            return Err(Error::msg(format!("`{id}` is answered twice")));
        }
    }
    let mut body = String::new();
    for field in fields {
        if let Some((_, answer)) = answers.iter().find(|(id, _)| *id == field.id)
            && !answer.trim().is_empty()
        {
            section(&mut body, field.label, field.render, answer);
        }
    }
    Ok(body)
}

/// `text` without its ANSI escapes, cut from the front at a line start to
/// `max` characters and marked where it was cut.
fn tail(text: &str, max: usize) -> String {
    let text = strip_ansi(text);
    let count = text.chars().count();
    if count <= max {
        return text;
    }
    let cut: String = text.chars().skip(count - max).collect();
    // A cut just after a newline already opens on a whole line.
    let cut = if text.chars().nth(count - max - 1) == Some('\n') {
        cut.as_str()
    } else {
        cut.split_once('\n').map_or(cut.as_str(), |(_, rest)| rest)
    };
    format!("[earlier lines cut; the run log has the rest]\n{cut}")
}

/// `text` without the CSI escape sequences a colored terminal writes, such
/// as `ESC[1m`.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            // Parameter and intermediate bytes, up to the final byte in `@..=~`.
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// One field of an issue form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Field {
    pub(super) id: &'static str,
    pub(super) label: &'static str,
    /// The form's `render:` language; the body fences the answer in it.
    pub(super) render: Option<&'static str>,
}

pub(super) const fn field(id: &'static str, label: &'static str) -> Field {
    Field {
        id,
        label,
        render: None,
    }
}

pub(super) const fn fenced(id: &'static str, label: &'static str, render: &'static str) -> Field {
    Field {
        id,
        label,
        render: Some(render),
    }
}

/// `.github/ISSUE_TEMPLATE/2-bug.yml`, field by field.
pub(super) const BUG: [Field; 10] = [
    field("components", "Where"),
    field("version", "Version"),
    field("features", "Cargo features"),
    field("toolchain", "Rust version and platform"),
    field("what", "What happened"),
    field("expected", "What you expected instead"),
    fenced("repro", "Reproduction", "rust"),
    field("data", "A sample record"),
    fenced("config", "Configuration", "yaml"),
    fenced("logs", "Output", "shell"),
];

/// The most of a log a body carries. GitHub rejects an issue body over
/// 65,536 characters.
pub(super) const MAX_LOG_CHARS: usize = 25_000;

/// One `###` section; an empty answer reads `_No response_`, as GitHub
/// renders an unanswered field.
pub(super) fn section(body: &mut String, label: &str, render: Option<&str>, answer: &str) {
    let answer = answer.trim_end();
    let _ = write!(body, "### {label}\n\n");
    if answer.is_empty() {
        body.push_str("_No response_\n\n");
        return;
    }
    match render {
        Some(lang) => {
            let fence = fence(answer);
            let _ = write!(body, "{fence}{lang}\n{answer}\n{fence}\n\n");
        }
        None => {
            let _ = write!(body, "{answer}\n\n");
        }
    }
}

/// A backtick fence longer than any backtick run in `text`.
fn fence(text: &str) -> String {
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    "`".repeat(longest.max(2) + 1)
}

#[cfg(test)]
pub(super) mod tests;
