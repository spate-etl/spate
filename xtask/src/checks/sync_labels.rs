//! Applies `.github/labels.yml` to the repository.
//!
//! `gh` rather than a labeling action: the organisation restricts Actions to an
//! allowlist, and a refused one reports `startup_failure` without naming it.

use std::path::Path;

use crate::run::{self, Error, Outcome, Step};

const DEFINITIONS: &str = ".github/labels.yml";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Label {
    name: String,
    color: String,
    description: String,
}

/// Creates a label that is missing and updates the color and description of one
/// that exists. Deletes nothing.
pub(crate) fn sync(root: &Path, explain: bool, dry_run: bool, repo: Option<&str>) -> Outcome {
    let path = root.join(DEFINITIONS);
    let text =
        std::fs::read_to_string(&path).map_err(|e| Error::msg(format!("{DEFINITIONS}: {e}")))?;
    let labels = parse(&text);
    if labels.is_empty() {
        return Err(Error::msg(format!(
            "no labels parsed from {DEFINITIONS}. Has the format changed?"
        )));
    }

    let total = labels.len();
    let mut synced = 0;
    for label in &labels {
        if label.color.is_empty() {
            return Err(Error::msg(format!(
                "'{}' has no color in {DEFINITIONS}",
                label.name
            )));
        }
        if dry_run {
            println!(
                "would sync  {:<28} #{}  {}",
                label.name, label.color, label.description
            );
            continue;
        }
        // `--force` updates color and description when the label exists,
        // repairing one created elsewhere with a default grey and no
        // description.
        let mut step = Step::new(
            "gh",
            [
                "label",
                "create",
                &label.name,
                "--color",
                &label.color,
                "--description",
                &label.description,
                "--force",
            ],
        );
        if let Some(repo) = repo {
            step = step.args(["--repo", repo]);
        }
        run::quiet(root, explain, &step)
            .map_err(|_| Error::msg(format!("failed to sync '{}'", label.name)))?;
        synced += 1;
    }

    if dry_run {
        println!("sync-labels: {total} label(s) would be synced (dry run, nothing changed)");
    } else {
        println!("sync-labels: {synced} of {total} label(s) synced");
    }
    Ok(())
}

/// The quoted, fixed key order is the file's own convention, so a line is
/// recognised by its exact opening and the value runs to the closing quote.
fn parse(text: &str) -> Vec<Label> {
    let mut out: Vec<Label> = Vec::new();
    for line in text.lines() {
        if let Some(name) = value_of(line, "- name: \"") {
            out.push(Label {
                name,
                color: String::new(),
                description: String::new(),
            });
        } else if let (Some(color), Some(last)) = (value_of(line, "  color: \""), out.last_mut()) {
            last.color = color;
        } else if let (Some(description), Some(last)) =
            (value_of(line, "  description: \""), out.last_mut())
        {
            last.description = description;
        }
    }
    out.retain(|l| !l.name.is_empty());
    out
}

/// The text between the first `: "` and the closing quote, for a line opening
/// with `prefix`.
fn value_of(line: &str, prefix: &str) -> Option<String> {
    if !line.starts_with(prefix) {
        return None;
    }
    let after = line.split_once(": \"")?.1;
    let trimmed = after.trim_end();
    Some(trimmed.strip_suffix('"').unwrap_or(trimmed).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(name: &str, color: &str, description: &str) -> Label {
        Label {
            name: name.to_owned(),
            color: color.to_owned(),
            description: description.to_owned(),
        }
    }

    #[test]
    fn an_entry_carries_its_three_fields() {
        let text = "- name: \"area: ci\"\n  color: \"1d76db\"\n  description: \"The pipeline\"\n";
        assert_eq!(
            parse(text),
            vec![label("area: ci", "1d76db", "The pipeline")]
        );
    }

    #[test]
    fn a_colon_inside_a_value_survives() {
        let text = "- name: \"needs: triage\"\n  color: \"ededed\"\n  description: \"a: b\"\n";
        assert_eq!(parse(text), vec![label("needs: triage", "ededed", "a: b")]);
    }

    #[test]
    fn comments_and_blank_lines_are_not_entries() {
        let text =
            "# a comment\n\n- name: \"one\"\n  color: \"fff\"\n  description: \"d\"\n\n# more\n";
        assert_eq!(parse(text), vec![label("one", "fff", "d")]);
    }

    #[test]
    fn a_second_entry_starts_a_new_label() {
        let text = concat!(
            "- name: \"one\"\n  color: \"a\"\n  description: \"x\"\n",
            "- name: \"two\"\n  color: \"b\"\n  description: \"y\"\n"
        );
        assert_eq!(
            parse(text),
            vec![label("one", "a", "x"), label("two", "b", "y")]
        );
    }

    #[test]
    fn a_deeper_indent_is_not_a_field() {
        let text = "- name: \"one\"\n    color: \"a\"\n  description: \"x\"\n";
        assert_eq!(parse(text), vec![label("one", "", "x")]);
    }
}
