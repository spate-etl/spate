//! Holds the built documentation site to a floor on its file count, and each
//! page its sitemap lists to a description a search result shows in full.

use std::ops::RangeInclusive;
use std::path::{Component, Path, PathBuf};

use crate::run::{Error, Outcome};

/// The build output, relative to the repository root.
const BUILD_DIR: &str = "website/build";

/// A build carrying every page stays well above this.
const FLOOR: usize = 200;

/// The length of a page description, in characters, that a search result
/// shows in full.
pub(crate) const DESCRIPTION: RangeInclusive<usize> = 50..=160;

pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!(
            "(counts the files under {BUILD_DIR}, then reads the description of each page \
             its sitemap.xml lists)"
        );
        return Ok(());
    }
    let build = root.join(BUILD_DIR);
    let count = count_files(&build)?;
    println!("site files: {count}");
    clears_floor(count)?;
    let (pages, problems) = description_problems(&build)?;
    if !problems.is_empty() {
        return Err(Error::msg(format!(
            "{} of {pages} sitemap page(s) without a usable description:\n  {}",
            problems.len(),
            problems.join("\n  ")
        )));
    }
    println!(
        "sitemap pages: {pages}, each described in {} to {} characters",
        DESCRIPTION.start(),
        DESCRIPTION.end()
    );
    Ok(())
}

/// Why `text` is not a description a search result shows in full, or `None`
/// when it is.
pub(crate) fn description_problem(text: Option<&str>) -> Option<String> {
    let Some(text) = text else {
        return Some("no description".to_owned());
    };
    let n = text.chars().count();
    (!DESCRIPTION.contains(&n)).then(|| {
        format!(
            "description is {n} characters; a search result shows {} to {}",
            DESCRIPTION.start(),
            DESCRIPTION.end()
        )
    })
}

/// The number of pages `sitemap.xml` under `build` lists, and one line for each
/// whose description is missing or outside [`DESCRIPTION`].
///
/// A sitemap that is missing or lists nothing is an error, so the check never
/// passes on an empty input. So is a listed page with no file.
fn description_problems(build: &Path) -> Result<(usize, Vec<String>), Error> {
    let path = build.join("sitemap.xml");
    let sitemap = read(&path)?;
    let urls = locs(&sitemap);
    if urls.is_empty() {
        return Err(Error::msg(format!("{}: lists no page", path.display())));
    }
    let mut problems = Vec::new();
    for url in &urls {
        let file = page_file(url)
            .ok_or_else(|| Error::msg(format!("{url}: names a file outside {BUILD_DIR}")))?;
        let html = read(&build.join(&file))?;
        if let Some(problem) = description_problem(description_of(&html).as_deref()) {
            problems.push(format!("{}: {problem}", file.display()));
        }
    }
    Ok((urls.len(), problems))
}

fn read(path: &Path) -> Result<String, Error> {
    std::fs::read_to_string(path).map_err(|e| Error::msg(format!("{}: {e}", path.display())))
}

/// The URLs a sitemap lists, in order.
fn locs(sitemap: &str) -> Vec<&str> {
    sitemap
        .split("<loc>")
        .skip(1)
        .filter_map(|s| s.split_once("</loc>").map(|(url, _)| url.trim()))
        .collect()
}

/// The file under the build that serves `url`, or `None` when its path could
/// name a file outside the build.
///
/// The site builds with `trailingSlash: false`, so a page is `<path>.html` and
/// only the root is `index.html`.
fn page_file(url: &str) -> Option<PathBuf> {
    let path = match url.split_once("://") {
        Some((_, rest)) => rest.find('/').map_or("", |at| &rest[at..]),
        None => url,
    };
    let path = path.trim_start_matches('/');
    let file = if path.is_empty() {
        PathBuf::from("index.html")
    } else {
        PathBuf::from(format!("{path}.html"))
    };
    file.components()
        .all(|c| matches!(c, Component::Normal(_)))
        .then_some(file)
}

/// One attribute of a tag: its name, and its value as written, with the quote
/// it was written in.
struct Attr<'a> {
    name: &'a str,
    value: &'a str,
    quote: Option<char>,
}

/// The attributes of a tag read from just after its name, and the text after
/// its closing `>`.
///
/// A quoted value is read to its closing quote, since the build leaves `>`
/// unescaped inside one.
fn attributes(mut rest: &str) -> (Vec<Attr<'_>>, &str) {
    let mut attrs = Vec::new();
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix('>').or_else(|| rest.strip_prefix("/>")) {
            return (attrs, after);
        }
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, '=' | '>' | '/'))
            .unwrap_or(rest.len());
        if end == 0 {
            // End of input, or a stray `/` or `=`, which are one byte each.
            if rest.is_empty() {
                return (attrs, rest);
            }
            rest = &rest[1..];
            continue;
        }
        let name = &rest[..end];
        rest = &rest[end..];
        let Some(value) = rest.strip_prefix('=') else {
            attrs.push(Attr {
                name,
                value: "",
                quote: None,
            });
            continue;
        };
        let quote = value.chars().next().filter(|c| matches!(c, '"' | '\''));
        let (value, after) = match quote {
            Some(q) => {
                let body = &value[1..];
                let close = body.find(q).unwrap_or(body.len());
                (&body[..close], body.get(close + 1..).unwrap_or(""))
            }
            None => {
                let close = value
                    .find(|c: char| c.is_whitespace() || c == '>')
                    .unwrap_or(value.len());
                value.split_at(close)
            }
        };
        attrs.push(Attr { name, value, quote });
        rest = after;
    }
}

/// The `content` of the page's `<meta name="description">`, or `None` when it
/// has none.
fn description_of(html: &str) -> Option<String> {
    let mut rest = html;
    while let Some(at) = rest.find("<meta") {
        let (attrs, after) = attributes(&rest[at + "<meta".len()..]);
        rest = after;
        if attrs
            .iter()
            .any(|a| a.name == "name" && a.value == "description")
        {
            return attrs.iter().find(|a| a.name == "content").map(decoded);
        }
    }
    None
}

/// An attribute value as the page shows it. The build escapes only the quote
/// the value is written in, as `&quot;` or `&apos;`, and writes every other
/// character as itself.
fn decoded(attr: &Attr<'_>) -> String {
    match attr.quote {
        Some('"') => attr.value.replace("&quot;", "\""),
        Some('\'') => attr.value.replace("&apos;", "'"),
        _ => attr.value.to_owned(),
    }
}

/// Fails unless `count` reaches the floor.
fn clears_floor(count: usize) -> Outcome {
    if count < FLOOR {
        return Err(Error::msg(format!(
            "{count} file(s) under {BUILD_DIR}; a complete site holds at least {FLOOR}"
        )));
    }
    Ok(())
}

/// Counts the regular files under `dir` and every directory below it.
///
/// An unreadable or missing directory is an error, so the count never stands in
/// for a build that produced nothing.
fn count_files(dir: &Path) -> Result<usize, Error> {
    let mut pending = vec![dir.to_path_buf()];
    let mut count = 0;
    while let Some(next) = pending.pop() {
        let at = |e: std::io::Error| Error::msg(format!("{}: {e}", next.display()));
        for entry in std::fs::read_dir(&next).map_err(at)? {
            let entry = entry.map_err(at)?;
            // `file_type` resolves no link, so a symlink is neither counted nor
            // descended into.
            let kind = entry.file_type().map_err(at)?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                count += 1;
            }
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        BUILD_DIR, FLOOR, check, clears_floor, count_files, description_of, description_problem,
        description_problems, page_file,
    };
    use crate::checks::scratch::Scratch;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    /// Writes `n` empty files under `dir`, spread over nested directories.
    fn populate(dir: &Path, n: usize) {
        for i in 0..n {
            let sub = dir.join(format!("d{}", i % 7));
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join(format!("f{i}")), "").unwrap();
        }
    }

    #[test]
    fn files_below_a_directory_are_counted() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let build = scratch.join("build");
        populate(&build, 30);
        assert_eq!(count_files(&build).unwrap(), 30);
    }

    /// An empty directory reads as zero files, and the floor rejects it.
    #[test]
    fn an_empty_build_fails() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let build = scratch.join("build");
        std::fs::create_dir_all(&build).unwrap();
        assert_eq!(count_files(&build).unwrap(), 0);
        assert!(clears_floor(0).is_err());
    }

    /// A missing directory is an error of its own, so no build path yields a
    /// count the floor could pass.
    #[test]
    fn a_missing_build_is_an_error() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let err = count_files(&scratch.join("build")).unwrap_err();
        assert!(err.message.contains("build"), "{}", err.message);
    }

    /// Directories are not files, so a tree of empty directories fails.
    #[test]
    fn a_tree_of_directories_holds_no_files() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let build = scratch.join("build");
        std::fs::create_dir_all(build.join("assets/js/chunks")).unwrap();
        assert_eq!(count_files(&build).unwrap(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_not_counted() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let build = scratch.join("build");
        std::fs::create_dir_all(&build).unwrap();
        std::fs::write(build.join("index.html"), "").unwrap();
        symlink(build.join("index.html"), build.join("link.html")).unwrap();
        symlink(&build, build.join("loop")).unwrap();
        assert_eq!(count_files(&build).unwrap(), 1);
    }

    #[test]
    fn the_floor_is_inclusive() {
        assert!(clears_floor(FLOOR).is_ok());
        assert!(clears_floor(FLOOR - 1).is_err());
    }

    /// The message names both the count and the floor, so a failure says how
    /// far short the build fell.
    #[test]
    fn the_failure_names_the_count_and_the_floor() {
        let message = clears_floor(3).unwrap_err().message;
        assert!(message.contains('3'), "{message}");
        assert!(message.contains(&FLOOR.to_string()), "{message}");
    }

    /// A description of `n` characters.
    fn text(n: usize) -> String {
        "a".repeat(n)
    }

    /// A page head as the build writes it, with the Open Graph copy beside the
    /// description.
    fn page(description: &str) -> String {
        format!(
            "<!doctype html><head><meta charset=UTF-8><meta data-rh=true name=description \
             content=\"{description}\"/><meta data-rh=true property=og:description \
             content=\"{description}\"/></head>"
        )
    }

    #[test]
    fn the_build_form_is_read() {
        assert_eq!(description_of(&page("A page.")).as_deref(), Some("A page."));
    }

    #[test]
    fn a_quoted_name_is_read() {
        let html = r#"<meta name="description" content="A page.">"#;
        assert_eq!(description_of(html).as_deref(), Some("A page."));
    }

    /// Only the tag whose `name` is `description` counts, wherever the Open
    /// Graph copy sits.
    #[test]
    fn the_open_graph_copy_is_not_read() {
        let html = "<meta property=og:description content=\"before\">\
                    <meta name=description content=\"the one\">\
                    <meta property=og:description content=\"after\">";
        assert_eq!(description_of(html).as_deref(), Some("the one"));
        let only_og = "<meta property=og:description content=\"open graph\">";
        assert_eq!(description_of(only_og), None);
    }

    /// Another `name` tag ahead of the description is not read.
    #[test]
    fn another_named_meta_tag_is_not_read() {
        let html = "<meta name=keywords content=\"keywords\">\
                    <meta name=description content=\"the one\">";
        assert_eq!(description_of(html).as_deref(), Some("the one"));
    }

    /// A minifier may drop the space after a quoted value.
    #[test]
    fn an_attribute_can_follow_a_closing_quote() {
        let html = "<meta content=\"the one\"name=description>";
        assert_eq!(description_of(html).as_deref(), Some("the one"));
    }

    #[test]
    fn a_page_without_the_tag_has_none() {
        assert_eq!(description_of("<head><meta charset=UTF-8></head>"), None);
        assert_eq!(description_of("<meta name=description>"), None);
    }

    #[test]
    fn a_single_quoted_value_keeps_its_double_quotes() {
        let html = r#"<meta name=description content='A "quoted" page.'>"#;
        assert_eq!(
            description_of(html).as_deref(),
            Some(r#"A "quoted" page."#)
        );
    }

    #[test]
    fn an_unquoted_value_ends_at_whitespace_or_the_tag() {
        assert_eq!(
            description_of("<meta name=description content=word data-x=1>").as_deref(),
            Some("word")
        );
        assert_eq!(
            description_of("<meta name=description content=word>").as_deref(),
            Some("word")
        );
    }

    /// The build leaves `>` unescaped inside a quoted value.
    #[test]
    fn a_quoted_value_runs_past_a_raw_angle_bracket() {
        let html = r#"<meta name=description content="a > b, and c < d">"#;
        assert_eq!(description_of(html).as_deref(), Some("a > b, and c < d"));
    }

    /// Each escape is one character, decoded once, and only in the quote it
    /// escapes.
    #[test]
    fn the_quote_escape_decodes_once() {
        let read = |html: &str| description_of(html).unwrap();
        assert_eq!(read(r#"<meta name=description content="&quot;">"#), "\"");
        assert_eq!(read("<meta name=description content='&apos;'>"), "'");
        assert_eq!(
            read(r#"<meta name=description content="&amp;quot;">"#),
            "&amp;quot;"
        );
        assert_eq!(read(r#"<meta name=description content="&apos;">"#), "&apos;");
        assert_eq!(read("<meta name=description content='&quot;'>"), "&quot;");
    }

    #[test]
    fn the_bounds_are_inclusive() {
        assert_eq!(description_problem(Some(&text(50))), None);
        assert_eq!(description_problem(Some(&text(160))), None);
        assert_eq!(
            description_problem(Some(&text(49))).as_deref(),
            Some("description is 49 characters; a search result shows 50 to 160")
        );
        assert_eq!(
            description_problem(Some(&text(161))).as_deref(),
            Some("description is 161 characters; a search result shows 50 to 160")
        );
        assert_eq!(description_problem(None).as_deref(), Some("no description"));
    }

    /// 160 em dashes are 480 bytes.
    #[test]
    fn the_length_is_in_characters() {
        assert_eq!(description_problem(Some(&"—".repeat(160))), None);
    }

    #[test]
    fn a_url_maps_to_its_page_file() {
        assert_eq!(page_file("https://host/"), Some(PathBuf::from("index.html")));
        assert_eq!(page_file("https://host"), Some(PathBuf::from("index.html")));
        assert_eq!(page_file("https://host/a/b"), Some(PathBuf::from("a/b.html")));
        assert_eq!(page_file("/a"), Some(PathBuf::from("a.html")));
    }

    #[test]
    fn a_url_cannot_leave_the_build() {
        assert_eq!(page_file("https://host/a/../../etc/passwd"), None);
        assert_eq!(page_file("https://host/./a"), None);
    }

    /// A build under `root`, holding the floor's worth of files, a sitemap
    /// listing `pages`, and each page with its description.
    fn site(root: &Path, pages: &[(&str, &str, &str)]) -> PathBuf {
        let build = root.join(BUILD_DIR);
        populate(&build.join("assets"), FLOOR);
        let mut sitemap = String::from("<?xml version=\"1.0\"?><urlset>");
        for (url, file, description) in pages {
            sitemap.push_str(&format!("<url><loc>{url}</loc></url>"));
            let path = build.join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, page(description)).unwrap();
        }
        sitemap.push_str("</urlset>");
        std::fs::write(build.join("sitemap.xml"), sitemap).unwrap();
        build
    }

    #[test]
    fn a_described_site_passes() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        site(
            scratch.dir(),
            &[
                ("https://host/", "index.html", &text(100)),
                ("https://host/docs/a", "docs/a.html", &text(160)),
            ],
        );
        assert!(check(scratch.dir(), false).is_ok());
    }

    /// Every page out of bounds is named, not only the first.
    #[test]
    fn each_page_out_of_bounds_is_named() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        site(
            scratch.dir(),
            &[
                ("https://host/", "index.html", &text(161)),
                ("https://host/docs/a", "docs/a.html", &text(100)),
                ("https://host/docs/b", "docs/b.html", &text(49)),
            ],
        );
        assert_eq!(
            check(scratch.dir(), false).unwrap_err().message,
            "2 of 3 sitemap page(s) without a usable description:\n  \
             index.html: description is 161 characters; a search result shows 50 to 160\n  \
             docs/b.html: description is 49 characters; a search result shows 50 to 160"
        );
    }

    #[test]
    fn a_missing_sitemap_is_an_error() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        let err = description_problems(scratch.dir()).unwrap_err();
        assert!(err.message.contains("sitemap.xml"), "{}", err.message);
    }

    #[test]
    fn an_empty_sitemap_is_an_error() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        std::fs::write(scratch.join("sitemap.xml"), "<urlset></urlset>").unwrap();
        let err = description_problems(scratch.dir()).unwrap_err();
        assert!(err.message.ends_with("sitemap.xml: lists no page"), "{}", err.message);
    }

    #[test]
    fn a_listed_page_with_no_file_is_an_error() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        std::fs::write(
            scratch.join("sitemap.xml"),
            "<urlset><url><loc>https://host/gone</loc></url></urlset>",
        )
        .unwrap();
        let err = description_problems(scratch.dir()).unwrap_err();
        assert!(err.message.contains("gone.html"), "{}", err.message);
    }

    #[test]
    fn a_listed_path_outside_the_build_is_an_error() {
        let scratch = Scratch::new("xtask-site-check").unwrap();
        std::fs::write(
            scratch.join("sitemap.xml"),
            "<urlset><url><loc>https://host/../secret</loc></url></urlset>",
        )
        .unwrap();
        let err = description_problems(scratch.dir()).unwrap_err();
        assert_eq!(
            err.message,
            format!("https://host/../secret: names a file outside {BUILD_DIR}")
        );
    }
}
