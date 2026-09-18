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
    // Ahead of the dry-run branch: a dry run without `gh` would otherwise
    // report a sync that could not have run.
    if !explain && !run::on_path("gh") {
        return Err(Error::msg("gh is not installed"));
    }
    let labels = parse(&text);
    if labels.is_empty() {
        return Err(Error::msg(format!(
            "no labels parsed from {DEFINITIONS}. Has the format changed?"
        )));
    }

    let total = labels.len();
    let mut synced = 0;
    for label in &labels {
        color(label)?;
        if dry_run {
            println!("{}", dry_run_line(label));
            continue;
        }
        run::quiet(root, explain, &create_step(label, repo)).map_err(|e| Error {
            message: format!("failed to sync '{}': {}", label.name, e.message),
            code: e.code,
        })?;
        synced += 1;
    }

    if dry_run {
        println!("sync-labels: {total} label(s) would be synced (dry run, nothing changed)");
    } else {
        println!("sync-labels: {synced} of {total} label(s) synced");
    }
    Ok(())
}

/// The label's color, refusing an entry the file left without one: `gh` would
/// otherwise create the label grey.
fn color(label: &Label) -> Result<&str, Error> {
    if label.color.is_empty() {
        return Err(Error::msg(format!(
            "'{}' has no color in {DEFINITIONS}",
            label.name
        )));
    }
    Ok(&label.color)
}

/// The line a dry run prints for one label, the name padded so the colors
/// align down the column.
fn dry_run_line(label: &Label) -> String {
    format!(
        "would sync  {:<28} #{}  {}",
        label.name, label.color, label.description
    )
}

/// The `gh` invocation that applies one label.
///
/// `--force` updates color and description when the label exists, repairing one
/// created elsewhere with a default grey and no description.
fn create_step(label: &Label, repo: Option<&str>) -> Step<'static> {
    let step = Step::new(
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
    match repo {
        Some(repo) => step.args(["--repo", repo]),
        None => step,
    }
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

    #[test]
    fn an_entry_becomes_a_forcing_create() {
        let step = create_step(&label("area: ci", "1d76db", "The pipeline"), None);
        assert_eq!(
            step.display(),
            "gh label create 'area: ci' --color 1d76db --description 'The pipeline' --force"
        );
    }

    #[test]
    fn a_named_repository_is_appended() {
        let step = create_step(&label("one", "fff", "d"), Some("owner/name"));
        assert_eq!(
            step.display(),
            "gh label create one --color fff --description d --force --repo owner/name"
        );
    }

    #[test]
    fn a_dry_run_line_pads_the_name_to_a_fixed_column() {
        assert_eq!(
            dry_run_line(&label("one", "fff", "d")),
            "would sync  one                          #fff  d"
        );
        assert_eq!(
            dry_run_line(&label(&"x".repeat(30), "fff", "d")),
            format!("would sync  {} #fff  d", "x".repeat(30))
        );
    }

    #[test]
    fn an_entry_with_no_color_is_refused() {
        assert_eq!(
            color(&label("one", "", "d")).unwrap_err().message,
            "'one' has no color in .github/labels.yml"
        );
        assert_eq!(color(&label("one", "fff", "d")).unwrap(), "fff");
    }
}
