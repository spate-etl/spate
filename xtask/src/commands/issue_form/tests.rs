use std::fs;
use std::path::Path;

use super::*;

/// One field of an issue form as the form file declares it.
#[derive(Debug, Default)]
pub(crate) struct FormField {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) render: Option<String>,
    pub(crate) options: Vec<String>,
}

pub(crate) fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

pub(crate) fn form_file(name: &str) -> String {
    fs::read_to_string(repo().join(".github/ISSUE_TEMPLATE").join(name)).unwrap()
}

/// The non-markdown fields of an issue form, in order, read line by line from
/// the layout the forms use: `  - type:` opens an item, `    id:` and
/// `      label:`/`render:` sit at fixed indents, and options are
/// `        - ` items, with a `label: ` prefix on checkboxes.
pub(crate) fn form_fields(yaml: &str) -> Vec<FormField> {
    let unquote = |s: &str| s.trim().trim_matches(|c| c == '"' || c == '\'').to_owned();
    let mut fields: Vec<(String, FormField)> = Vec::new();
    for line in yaml.lines() {
        if let Some(kind) = line.strip_prefix("  - type: ") {
            fields.push((kind.trim().to_owned(), FormField::default()));
            continue;
        }
        let Some((_, field)) = fields.last_mut() else {
            continue;
        };
        if let Some(id) = line.strip_prefix("    id: ") {
            field.id = unquote(id);
        } else if let Some(label) = line.strip_prefix("      label: ") {
            field.label = unquote(label);
        } else if let Some(render) = line.strip_prefix("      render: ") {
            field.render = Some(unquote(render));
        } else if let Some(option) = line.strip_prefix("        - ") {
            let option = option.strip_prefix("label: ").unwrap_or(option);
            field.options.push(unquote(option));
        }
    }
    fields
        .into_iter()
        .filter(|(kind, _)| kind != "markdown")
        .map(|(_, f)| f)
        .collect()
}

pub(crate) fn declared(fields: &[Field]) -> Vec<(String, String, Option<String>)> {
    fields
        .iter()
        .map(|f| {
            (
                f.id.to_owned(),
                f.label.to_owned(),
                f.render.map(str::to_owned),
            )
        })
        .collect()
}

pub(crate) fn read(form: &[FormField]) -> Vec<(String, String, Option<String>)> {
    form.iter()
        .map(|f| (f.id.clone(), f.label.clone(), f.render.clone()))
        .collect()
}

/// The renderer's bug fields are the bug form's fields, with the same ids,
/// labels and render languages, in the same order.
#[test]
fn bug_fields_match_the_bug_form() {
    let form = form_fields(&form_file("2-bug.yml"));
    assert_eq!(read(&form), declared(&BUG));
}

/// A fence around text holding backticks is longer than any run in it.
#[test]
fn fences_outlast_backticks_in_the_answer() {
    assert_eq!(fence("plain"), "```");
    assert_eq!(fence("a ``` b"), "````");
}
