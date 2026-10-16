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

fn answers(pairs: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
    pairs.iter().map(|(id, a)| (*id, (*a).to_owned())).collect()
}

fn headings(body: &str) -> Vec<&str> {
    body.lines()
        .filter_map(|l| l.strip_prefix("### "))
        .collect()
}

/// A body has a section for each answered field, in the form's order, and
/// none for a field left unanswered or answered empty.
#[test]
fn unanswered_fields_are_left_out() {
    let body = answered(
        &BUG,
        &answers(&[
            ("logs", "boom"),
            ("what", "It failed."),
            ("components", "Build, CI, or the examples"),
            ("expected", "  \n"),
        ]),
    )
    .unwrap();
    assert_eq!(headings(&body), ["Where", "What happened", "Output"]);
}

/// Each `render:` field's answer sits in a fence of the form's language, and
/// no other field's answer is fenced.
#[test]
fn render_fields_are_fenced_in_the_forms_language() {
    let form = form_fields(&form_file("2-bug.yml"));
    for field in &form {
        let id = BUG.iter().find(|f| f.id == field.id).unwrap().id;
        let body = answered(&BUG, &answers(&[(id, "a `b` c")])).unwrap();
        let expected = match &field.render {
            Some(lang) => format!("### {}\n\n```{lang}\na `b` c\n```\n\n", field.label),
            None => format!("### {}\n\na `b` c\n\n", field.label),
        };
        assert_eq!(body, expected);
    }
}

/// A file answer keeps its last lines inside the log limit, without ANSI
/// escapes, so two of them still fit GitHub's 65,536-character body limit.
#[test]
fn a_file_answer_keeps_its_tail_inside_the_log_limit() {
    let dir = std::env::temp_dir().join(format!("xtask-bug-report-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let log: String = (0..20_000)
        .map(|i| format!("\u{1b}[1m\u{1b}[31mline {i}\u{1b}[0m\n"))
        .collect();
    fs::write(dir.join("log"), &log).unwrap();
    let out = dir.join("body.md");
    let args = BugReport {
        answers: vec!["components=Build, CI, or the examples".to_owned()],
        answer_files: vec![
            format!("what={}", dir.join("log").display()),
            format!("logs={}", dir.join("log").display()),
        ],
        out: out.clone(),
    };
    bug_report(&dir, false, &args).unwrap();
    let body = fs::read_to_string(&out).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    assert!(body.chars().count() < 65_536, "{}", body.chars().count());
    assert!(body.contains("\nline 19999\n"));
    assert!(!body.contains("\nline 0\n"));
    assert!(!body.contains('\u{1b}'));
    assert!(body.contains("### Output\n\n```shell\n[earlier lines cut"));
}

/// An answer to an id the form does not have, a field answered twice, and an
/// argument with no `=` are refused.
#[test]
fn an_answer_the_form_has_no_field_for_is_refused() {
    assert!(answered(&BUG, &answers(&[("output", "x")])).is_err());
    assert!(answered(&BUG, &answers(&[("what", "x"), ("what", "y")])).is_err());
    assert!(split("what").is_err());
    assert_eq!(split("what=a=b").unwrap(), ("what", "a=b"));
}

/// Every `--answer` and `--answer-file` id a workflow passes is a field of
/// the bug form, so a typo fails here and not on the night a job fails.
#[test]
fn every_answer_a_workflow_gives_is_a_bug_form_field() {
    let ids: Vec<String> = form_fields(&form_file("2-bug.yml"))
        .into_iter()
        .map(|f| f.id)
        .collect();
    let mut seen = 0;
    for entry in fs::read_dir(repo().join(".github/workflows")).unwrap() {
        let path = entry.unwrap().path();
        let text = fs::read_to_string(&path).unwrap();
        for flag in ["--answer ", "--answer-file "] {
            for (at, _) in text.match_indices(flag) {
                let rest = text[at + flag.len()..].trim_start_matches(['$', '"', '\'']);
                let id = rest.split('=').next().unwrap();
                assert!(ids.iter().any(|f| f == id), "{}: {id}", path.display());
                seen += 1;
            }
        }
    }
    assert!(seen > 0, "no workflow passes an answer");
}
