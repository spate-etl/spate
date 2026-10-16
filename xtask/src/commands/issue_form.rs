//! Issue bodies laid out as the repository's issue forms: the bug form's
//! fields, and the `###` sections GitHub renders a form submission as.

use std::fmt::Write as _;

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

/// Every field as a section, in order, each answered by its own entry of
/// `answers`.
pub(super) fn form<const N: usize>(fields: &[Field; N], answers: [String; N]) -> String {
    let mut body = String::new();
    for (field, answer) in fields.iter().zip(answers) {
        section(&mut body, field.label, field.render, &answer);
    }
    body
}

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
