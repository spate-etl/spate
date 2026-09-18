//! Scaffolds an architecture decision record, and holds the set of them
//! internally consistent.
//!
//! Records live one per file in `docs/adr/`, and `docs/adr/_template.md` is
//! both the template and this section's documentation.
//!
//! The gate holds the mechanical half only: numbers unique, statuses from the
//! permitted set, placeholders filled in, every record present in the index.

use std::path::Path;

use crate::run::{Error, Outcome};

const DIR: &str = "docs/adr";
const TEMPLATE: &str = "docs/adr/_template.md";
const INDEX: &str = "docs/adr/README.mdx";

const STATUSES: [&str; 3] = ["accepted", "superseded", "deprecated"];

/// The marker the template leaves behind.
const PLACEHOLDER: &str = "REPLACE-ME";

/// One record on disk: the path the diagnostics name, the number its filename
/// claims, and its text.
struct Record {
    path: String,
    name: String,
    number: String,
    text: String,
}

pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads {DIR}/*.md against {INDEX})");
        return Ok(());
    }

    if !root.join(DIR).is_dir() {
        return Err(Error::msg(format!(
            "{DIR}/ not found. It holds the architecture decision records"
        )));
    }
    if !root.join(TEMPLATE).is_file() {
        return Err(Error::msg(format!(
            "{TEMPLATE} not found. It is the template AND the documentation for\n  \
             this section, so losing it loses the rules rather than just a convenience."
        )));
    }
    if !root.join(INDEX).is_file() {
        return Err(Error::msg(format!(
            "{INDEX} not found. It is the index every record must appear in"
        )));
    }

    let index = read(root, INDEX)?;
    let records = records(root)?;
    let problems = audit(&records, &index);
    for problem in &problems {
        eprintln!("adr: {problem}");
    }

    if records.is_empty() {
        return Err(Error::msg(format!(
            "no records found in {DIR}/. The filename pattern and the tree have diverged,\n  \
             so this gate is now checking nothing and reporting success."
        )));
    }
    if !problems.is_empty() {
        return Err(Error::msg(format!(
            "{} problem(s) across {} record(s)",
            problems.len(),
            records.len()
        )));
    }

    println!(
        "adr: {} record(s), numbers unique, statuses known, all indexed.",
        records.len()
    );
    Ok(())
}

pub(crate) fn new(root: &Path, explain: bool, slug: &str) -> Outcome {
    if slug.is_empty() {
        return Err(Error::msg(
            "usage: cargo xtask adr new <slug>\n  \
             The slug becomes the filename: a present-tense phrase naming the decision,\n  \
             lowercase, hyphenated. 'leader-computed-assignment', not 'coordination-stuff'.",
        ));
    }
    if !is_slug(slug) {
        return Err(Error::msg(format!(
            "'{slug}' should be lowercase letters, digits and hyphens, starting and ending\n  \
             with one of the first two. It becomes a filename."
        )));
    }
    if !root.join(TEMPLATE).is_file() {
        return Err(Error::msg(format!(
            "{TEMPLATE} not found. There is nothing to copy"
        )));
    }

    let (path, text) = scaffold(
        &read(root, TEMPLATE)?,
        &next_number(&paths(root)?),
        slug,
        &today(),
    );
    if root.join(&path).exists() {
        return Err(Error::msg(format!("{path} already exists")));
    }
    if explain {
        println!("(writes {path} from {TEMPLATE})");
        return Ok(());
    }

    std::fs::write(root.join(&path), text).map_err(|e| Error::msg(format!("{path}: {e}")))?;
    println!("adr: wrote {path}");
    println!("  The rules are in the file. Fill in every {PLACEHOLDER}, delete the guidance");
    println!("  comments as you go, and add a row to {INDEX}.");
    Ok(())
}

/// The path a record with this number and slug takes, and the bytes it
/// carries. The number and the date are substituted; every other placeholder
/// is left for the author, and `check` refuses the record until they are gone.
fn scaffold(template: &str, number: &str, slug: &str, today: &str) -> (String, String) {
    let text = template
        .split('\n')
        .map(|line| substitute(line, number, today))
        .collect::<Vec<_>>()
        .join("\n");
    (format!("{DIR}/{number}-{slug}.md"), text)
}

fn substitute(line: &str, number: &str, today: &str) -> String {
    if let Some(rest) = line.strip_prefix("# ADR-NNNN ") {
        return format!("# ADR-{number} {rest}");
    }
    if line == "- **Date:** YYYY-MM-DD" {
        return format!("- **Date:** {today}");
    }
    line.to_owned()
}

/// One past the highest number any record holds, so a withdrawn record still
/// consumes its number.
fn next_number(paths: &[String]) -> String {
    let last = paths
        .last()
        .and_then(|p| record_number(p))
        .and_then(|n| n.parse::<u32>().ok());
    format!("{:04}", last.map_or(1, |n| n + 1))
}

/// The problems a set of records carries, in the order the gate reports them.
fn audit(records: &[Record], index: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let mut previous = "";
    for record in records {
        if record.number == previous {
            problems.push(format!(
                "two records claim number {}. Numbers are never reused",
                record.number
            ));
        }
        previous = &record.number;

        let status = status(&record.text);
        if status.is_empty() {
            problems.push(format!("{} has no '- **Status:** ...' line", record.path));
        } else if !STATUSES.contains(&status) {
            problems.push(format!(
                "{} has status '{status}'; the permitted values are: {}",
                record.path,
                STATUSES.join(" ")
            ));
        }

        if has_placeholder(&record.text) {
            problems.push(format!(
                "{} still contains {PLACEHOLDER}. It was copied but not written",
                record.path
            ));
        }

        // Matching on the filename catches a record listed under the wrong
        // link too.
        if !index.contains(&format!("({})", record.name)) {
            problems.push(format!("{} is not linked from {INDEX}", record.path));
        }
    }
    problems
}

/// Every record in number order. Sorting the filenames is enough because the
/// numbers are zero-padded to a fixed width.
fn paths(root: &Path) -> Result<Vec<String>, Error> {
    let dir = root.join(DIR);
    let mut out = Vec::new();
    let entries =
        std::fs::read_dir(&dir).map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
        let path = format!("{DIR}/{}", entry.file_name().to_string_lossy());
        if record_number(&path).is_some() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn records(root: &Path) -> Result<Vec<Record>, Error> {
    let mut out = Vec::new();
    for path in paths(root)? {
        let number = record_number(&path)
            .ok_or_else(|| Error::msg(format!("{path}: not a record")))?
            .to_owned();
        let name = path.rsplit('/').next().unwrap_or(&path).to_owned();
        let text = read(root, &path)?;
        out.push(Record {
            path,
            name,
            number,
            text,
        });
    }
    Ok(out)
}

/// The four-digit number at the head of a record's filename, or `None` if the
/// name is not a record.
///
/// The pattern is exactly `docs/adr/NNNN-slug.md`. Anything looser and
/// `README.mdx` or an editor backup starts counting as a decision. A deeper
/// path is refused because the gate reads one directory level, so a record
/// under one would pass a check the index and the site both fail.
fn record_number(path: &str) -> Option<&str> {
    if path.matches('/').count() > 2 {
        return None;
    }
    let stem = path
        .rsplit('/')
        .next()
        .unwrap_or(path)
        .strip_suffix(".md")?;
    // Indices 0 to 5 are proved ASCII before anything slices there.
    let bytes = stem.as_bytes();
    if bytes.len() < 6 || !bytes[..4].iter().all(u8::is_ascii_digit) || bytes[4] != b'-' {
        return None;
    }
    if !is_slug(&stem[5..]) {
        return None;
    }
    Some(&stem[..4])
}

/// Lowercase letters, digits and hyphens, starting and ending with one of the
/// first two.
fn is_slug(s: &str) -> bool {
    let ok = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let bytes = s.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(&first), Some(&last)) => {
            ok(first) && ok(last) && bytes.iter().all(|&b| ok(b) || b == b'-')
        }
        _ => false,
    }
}

/// The status named on the record's first `- **Status:**` line, or an empty
/// string where there is no such line or it names no lowercase word.
///
/// The match is anchored to the start of the line. A record may discuss a
/// status in its prose, and a loose match would read that as its own.
fn status(text: &str) -> &str {
    let Some(rest) = text
        .lines()
        .find_map(|line| line.strip_prefix("- **Status:**"))
    else {
        return "";
    };
    let rest = rest.trim_start_matches([' ', '\t', '\u{b}', '\u{c}', '\r']);
    let end = rest
        .bytes()
        .position(|b| !b.is_ascii_lowercase())
        .unwrap_or(rest.len());
    &rest[..end]
}

/// Whether the record still carries an unfilled placeholder.
///
/// Inline code spans are stripped first, so a record naming the marker does
/// not count as carrying one. Placeholders in the template are bare words.
fn has_placeholder(text: &str) -> bool {
    text.lines()
        .any(|line| without_code_spans(line).contains(PLACEHOLDER))
}

/// The line with each backtick pair and its contents removed. An unterminated
/// backtick and everything after it survives.
fn without_code_spans(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(open) = rest.find('`') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('`') else { break };
        out.push_str(&rest[..open]);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

fn read(root: &Path, path: &str) -> Result<String, Error> {
    std::fs::read_to_string(root.join(path)).map_err(|e| Error::msg(format!("{path}: {e}")))
}

/// Today's date in UTC, as `YYYY-MM-DD`.
fn today() -> String {
    let days = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() / 86_400);
    civil_date(i64::from(u32::try_from(days).unwrap_or(0)))
}

/// The civil date of a day count from 1970-01-01, by the era algorithm. The
/// year is shifted to start in March, so the leap day falls at its end.
fn civil_date(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(path: &str, text: &str) -> Record {
        Record {
            path: path.to_owned(),
            name: path.rsplit('/').next().unwrap_or(path).to_owned(),
            number: record_number(path).unwrap().to_owned(),
            text: text.to_owned(),
        }
    }

    const GOOD: &str = "\
# ADR-0001 — x

- **Status:** accepted
- **Date:** 2026-01-01

It leaves ADR-0000 deprecated.
";

    #[test]
    fn the_filename_parser_accepts_records_and_rejects_everything_else() {
        for (path, want) in [
            (
                "docs/adr/0001-record-architecture-decisions-in-adrs.md",
                Some("0001"),
            ),
            ("docs/adr/0042-a-thing.md", Some("0042")),
            // The template, the index and a category file are not records.
            ("docs/adr/_template.md", None),
            ("docs/adr/README.mdx", None),
            ("docs/adr/_category_.json", None),
            // A number that is not four digits would sort wrong.
            ("docs/adr/1-a-thing.md", None),
            ("docs/adr/00001-a-thing.md", None),
            // A slug that is not lowercase-and-hyphens is not a record.
            ("docs/adr/0001-A-Thing.md", None),
            ("docs/adr/0001_a_thing.md", None),
            ("docs/adr/0001-.md", None),
            // Four digits then a hyphen, both required.
            ("docs/adr/note-to-self.md", None),
            ("docs/adr/0001abc.md", None),
            ("docs/adr/0001-a-.md", None),
            ("docs/adr/0001-a.md.md", None),
            // No slug at all.
            ("docs/adr/0001.md", None),
            ("docs/adr/sub/0001-a-thing.md", None),
        ] {
            assert_eq!(record_number(path), want, "{path}");
        }
    }

    #[test]
    fn a_multibyte_filename_is_not_a_record() {
        assert_eq!(record_number("docs/adr/00é1-a.md"), None);
        assert_eq!(record_number("docs/adr/0001-é.md"), None);
    }

    #[test]
    fn the_status_parser_reads_the_metadata_line_and_not_the_prose() {
        assert_eq!(status(GOOD), "accepted");
        assert_eq!(
            status("# x\n\n- **Status:** superseded by [ADR-0009](0009-x.md)\n"),
            "superseded"
        );
        assert_eq!(
            status("# x\n\nThe row reads - **Status:** deprecated there.\n"),
            ""
        );
    }

    /// The first `- **Status:**` line wins even when it names nothing, so a
    /// second one cannot rescue a malformed first.
    #[test]
    fn a_status_the_line_does_not_name_reads_as_absent() {
        assert_eq!(
            status("- **Status:** Accepted\n- **Status:** accepted\n"),
            ""
        );
        assert_eq!(status("- **Status:**\n"), "");
        assert_eq!(status("-  **Status:** accepted\n"), "");
        assert_eq!(status("prose\n"), "");
    }

    #[test]
    fn the_placeholder_check_separates_an_unfilled_record_from_a_mention_of_the_marker() {
        assert!(has_placeholder("Chosen option: \"REPLACE-ME\", because\n"));
        assert!(!has_placeholder(
            "The gate rejects an unfilled `REPLACE-ME` placeholder.\n"
        ));
    }

    /// Code spans are stripped per line, so a backtick opened on one line
    /// hides nothing on the next.
    #[test]
    fn an_unterminated_code_span_hides_nothing_beyond_its_line() {
        assert!(has_placeholder("a `span` and `an open one\nREPLACE-ME\n"));
        assert!(has_placeholder("`open REPLACE-ME\n"));
        assert!(has_placeholder("a `open\nREPLACE-ME\nclose` b\n"));
        assert_eq!(without_code_spans("a `b` c `d` e"), "a  c  e");
        assert_eq!(without_code_spans("a `b` c `d"), "a  c `d");
    }

    #[test]
    fn a_clean_set_has_no_problems() {
        let index = "| [0001](0001-a.md) | x |\n";
        assert!(audit(&[record("docs/adr/0001-a.md", GOOD)], index).is_empty());
    }

    #[test]
    fn two_records_on_one_number_are_caught() {
        let index = "(0001-a.md) (0001-b.md)\n";
        let problems = audit(
            &[
                record("docs/adr/0001-a.md", GOOD),
                record("docs/adr/0001-b.md", GOOD),
            ],
            index,
        );
        assert_eq!(
            problems,
            ["two records claim number 0001. Numbers are never reused"]
        );
    }

    #[test]
    fn an_unknown_status_is_caught() {
        let text = GOOD.replace("accepted", "proposed");
        let problems = audit(&[record("docs/adr/0001-a.md", &text)], "(0001-a.md)");
        assert_eq!(
            problems,
            [
                "docs/adr/0001-a.md has status 'proposed'; the permitted values are: \
                 accepted superseded deprecated"
            ]
        );
    }

    #[test]
    fn a_missing_status_line_is_caught() {
        let problems = audit(&[record("docs/adr/0001-a.md", "# x\n")], "(0001-a.md)");
        assert_eq!(
            problems,
            ["docs/adr/0001-a.md has no '- **Status:** ...' line"]
        );
    }

    #[test]
    fn a_leftover_placeholder_is_caught() {
        let text = format!("{GOOD}\nChosen option: \"REPLACE-ME\"\n");
        let problems = audit(&[record("docs/adr/0001-a.md", &text)], "(0001-a.md)");
        assert_eq!(
            problems,
            ["docs/adr/0001-a.md still contains REPLACE-ME. It was copied but not written"]
        );
    }

    /// The index is matched on the parenthesised filename, so a record listed
    /// under another record's link is unindexed.
    #[test]
    fn a_record_missing_from_the_index_is_caught() {
        for index in ["[0001](0002-b.md)\n", "see 0001-a.md for the decision\n"] {
            let problems = audit(&[record("docs/adr/0001-a.md", GOOD)], index);
            assert_eq!(
                problems,
                ["docs/adr/0001-a.md is not linked from docs/adr/README.mdx"],
                "{index}"
            );
        }
    }

    #[test]
    fn the_next_number_follows_the_highest_that_exists() {
        assert_eq!(next_number(&[]), "0001");
        assert_eq!(next_number(&["docs/adr/0001-a.md".to_owned()]), "0002");
        assert_eq!(next_number(&["docs/adr/0009-a.md".to_owned()]), "0010");
        assert_eq!(next_number(&["docs/adr/9999-a.md".to_owned()]), "10000");
    }

    /// A withdrawn record keeps its number, so allocation follows the last
    /// name in sort order rather than the count.
    #[test]
    fn a_gap_below_the_highest_number_is_not_reused() {
        let paths = [
            "docs/adr/0001-a.md".to_owned(),
            "docs/adr/0003-c.md".to_owned(),
        ];
        assert_eq!(next_number(&paths), "0004");
    }

    const TEMPLATE_TEXT: &str = "\
---
description: \"REPLACE-ME\"
---

# ADR-NNNN — REPLACE-ME short title

- **Status:** accepted
- **Date:** YYYY-MM-DD
- **Supersedes:** —
";

    #[test]
    fn the_scaffolder_substitutes_the_number_and_the_date_and_nothing_else() {
        let (path, text) = scaffold(TEMPLATE_TEXT, "0046", "a-thing", "2026-09-18");
        assert_eq!(path, "docs/adr/0046-a-thing.md");
        assert_eq!(
            text,
            TEMPLATE_TEXT
                .replace("# ADR-NNNN ", "# ADR-0046 ")
                .replace("- **Date:** YYYY-MM-DD", "- **Date:** 2026-09-18")
        );
        assert!(text.contains("description: \"REPLACE-ME\""));
    }

    /// Both substitutions are anchored, so a line quoting either form in prose
    /// is left alone.
    #[test]
    fn the_scaffolder_leaves_an_unanchored_mention_alone() {
        assert_eq!(
            substitute("  # ADR-NNNN — x", "0046", "2026-09-18"),
            "  # ADR-NNNN — x"
        );
        assert_eq!(
            substitute(
                "- **Date:** YYYY-MM-DD (recorded later)",
                "0046",
                "2026-09-18"
            ),
            "- **Date:** YYYY-MM-DD (recorded later)"
        );
        assert_eq!(
            substitute("# ADR-NNNN—x", "0046", "2026-09-18"),
            "# ADR-NNNN—x"
        );
    }

    #[test]
    fn the_scaffolder_keeps_the_templates_trailing_newline() {
        let (_, text) = scaffold("a\nb\n", "0046", "x", "2026-09-18");
        assert_eq!(text, "a\nb\n");
        let (_, text) = scaffold("a\nb", "0046", "x", "2026-09-18");
        assert_eq!(text, "a\nb");
    }

    #[test]
    fn a_slug_is_lowercase_digits_and_interior_hyphens() {
        for good in ["a", "0", "a-b", "a1-b2", "a--b"] {
            assert!(is_slug(good), "{good}");
        }
        for bad in ["", "-a", "a-", "A", "a_b", "a b", "a/b", "é"] {
            assert!(!is_slug(bad), "{bad}");
        }
    }

    #[test]
    fn a_day_count_reads_as_its_civil_date() {
        assert_eq!(civil_date(0), "1970-01-01");
        assert_eq!(civil_date(59), "1970-03-01");
        assert_eq!(civil_date(11_016), "2000-02-29");
        assert_eq!(civil_date(19_723), "2024-01-01");
        assert_eq!(civil_date(20_714), "2026-09-18");
    }

    /// A repository root under the system temporary directory, removed on
    /// drop. The name carries the test's, so a shared process runs them in
    /// parallel without collision.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("spate-xtask-adr-{}-{name}", std::process::id()));
            drop(std::fs::remove_dir_all(&dir));
            std::fs::create_dir_all(dir.join(DIR)).unwrap();
            Self(dir)
        }

        /// A root carrying the template and an empty index.
        fn section(name: &str) -> Self {
            let scratch = Self::new(name);
            scratch.write(TEMPLATE, TEMPLATE_TEXT);
            scratch.write(INDEX, "# Decisions\n");
            scratch
        }

        fn write(&self, path: &str, text: &str) -> &Self {
            std::fs::write(self.0.join(path), text).unwrap();
            self
        }

        /// A record with this filename, indexed, in the shape the template
        /// leaves once its placeholders are filled.
        fn record(&self, name: &str, status: &str) -> &Self {
            let number = &name[..4];
            self.write(
                &format!("{DIR}/{name}.md"),
                &format!("# ADR-{number} — x\n\n- **Status:** {status}\n- **Date:** 2026-01-01\n"),
            );
            let index = std::fs::read_to_string(self.0.join(INDEX)).unwrap();
            self.write(INDEX, &format!("{index}| [{number}]({name}.md) |\n"))
        }

        fn read(&self, path: &str) -> String {
            std::fs::read_to_string(self.0.join(path)).unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }

    fn failure(outcome: Outcome) -> String {
        outcome.unwrap_err().message
    }

    #[test]
    fn a_consistent_section_passes() {
        let scratch = Scratch::section("consistent");
        scratch
            .record("0001-a", "accepted")
            .record("0002-b", "superseded");
        assert!(check(&scratch.0, false).is_ok());
    }

    #[test]
    fn a_section_with_no_records_is_refused() {
        let scratch = Scratch::section("empty");
        assert!(failure(check(&scratch.0, false)).starts_with("no records found in docs/adr/."));
    }

    /// The three preflight files are reported by name, because losing one
    /// leaves the gate with nothing to check.
    #[test]
    fn a_missing_directory_template_or_index_is_reported() {
        let scratch = Scratch::new("preflight");
        std::fs::remove_dir(scratch.0.join(DIR)).unwrap();
        assert_eq!(
            failure(check(&scratch.0, false)),
            "docs/adr/ not found. It holds the architecture decision records"
        );

        let scratch = Scratch::section("preflight-template");
        std::fs::remove_file(scratch.0.join(TEMPLATE)).unwrap();
        assert!(failure(check(&scratch.0, false)).starts_with("docs/adr/_template.md not found."));

        let scratch = Scratch::section("preflight-index");
        std::fs::remove_file(scratch.0.join(INDEX)).unwrap();
        assert_eq!(
            failure(check(&scratch.0, false)),
            "docs/adr/README.mdx not found. It is the index every record must appear in"
        );
    }

    #[test]
    fn the_failure_counts_both_the_problems_and_the_records() {
        let scratch = Scratch::section("counts");
        scratch
            .record("0001-a", "proposed")
            .record("0002-b", "rejected");
        assert_eq!(
            failure(check(&scratch.0, false)),
            "2 problem(s) across 2 record(s)"
        );
    }

    /// A file the filename pattern rejects is not a record, so it neither
    /// counts nor has to appear in the index.
    #[test]
    fn a_file_the_pattern_rejects_is_not_counted() {
        let scratch = Scratch::section("ignored");
        scratch.record("0001-a", "accepted");
        scratch.write(&format!("{DIR}/0002-B.md"), "not a record");
        scratch.write(&format!("{DIR}/notes.txt"), "REPLACE-ME");
        assert!(check(&scratch.0, false).is_ok());
    }

    #[test]
    fn the_scaffolder_writes_the_next_number_and_leaves_the_rest_of_the_template() {
        let scratch = Scratch::section("write");
        scratch.record("0001-a", "accepted");
        new(&scratch.0, false, "a-second-thing").unwrap();
        let written = scratch.read("docs/adr/0002-a-second-thing.md");
        assert!(written.starts_with("---\ndescription: \"REPLACE-ME\"\n"));
        assert!(written.contains("\n# ADR-0002 — REPLACE-ME short title\n"));
        assert!(written.contains(&format!("\n- **Date:** {}\n", today())));
    }

    #[test]
    fn the_scaffolder_refuses_a_slug_the_filename_rules_reject() {
        let scratch = Scratch::section("bad-slug");
        assert!(failure(new(&scratch.0, false, "A-Thing")).starts_with("'A-Thing' should be"));
        assert!(failure(new(&scratch.0, false, "a/b")).starts_with("'a/b' should be"));
        assert!(failure(new(&scratch.0, false, "")).starts_with("usage: cargo xtask adr new"));
        assert_eq!(std::fs::read_dir(scratch.0.join(DIR)).unwrap().count(), 2);
    }

    /// Past 9999 the allocated name is five digits. The filename pattern
    /// rejects that, so the number stops advancing and the write lands on a
    /// file already there.
    #[test]
    fn the_scaffolder_refuses_to_overwrite() {
        let scratch = Scratch::section("collision");
        scratch.record("9999-a", "accepted");
        scratch.write(&format!("{DIR}/10000-b.md"), "held");
        assert_eq!(
            failure(new(&scratch.0, false, "b")),
            "docs/adr/10000-b.md already exists"
        );
        assert_eq!(scratch.read("docs/adr/10000-b.md"), "held");
    }

    #[test]
    fn the_scaffolder_reports_a_missing_template() {
        let scratch = Scratch::section("no-template");
        std::fs::remove_file(scratch.0.join(TEMPLATE)).unwrap();
        assert_eq!(
            failure(new(&scratch.0, false, "a-thing")),
            "docs/adr/_template.md not found. There is nothing to copy"
        );
    }

    /// Ten records created in reverse, because allocation and the duplicate
    /// check both read the listing as ordered.
    #[test]
    fn the_listing_is_sorted_whatever_order_the_directory_reports() {
        let scratch = Scratch::section("order");
        let names: Vec<String> = (1..=10)
            .rev()
            .map(|n| format!("{n:04}-record-{n}"))
            .collect();
        for name in &names {
            scratch.record(name, "accepted");
        }
        let mut want: Vec<String> = names.iter().map(|n| format!("{DIR}/{n}.md")).collect();
        want.sort();
        assert_eq!(paths(&scratch.0).unwrap(), want);
    }

    #[test]
    fn explaining_a_scaffold_writes_nothing() {
        let scratch = Scratch::section("explain");
        new(&scratch.0, true, "a-thing").unwrap();
        assert!(!scratch.0.join(format!("{DIR}/0001-a-thing.md")).exists());
    }
}
