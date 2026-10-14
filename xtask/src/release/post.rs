//! The release post a minor release requires: finding it in the blog by its
//! slug, removing its draft flag in the release commit, and the URL the
//! release body links it at. `website/blog/README.md` states the convention.

use std::path::{Path, PathBuf};

use super::version::Version;
use crate::run::Error;

/// The blog's source directory, relative to the repository root.
pub(crate) const BLOG: &str = "website/blog";

/// The front matter line that keeps a post out of a production build.
const DRAFT: &str = "draft: true";

/// The post files under `blog` whose front matter carries `slug: <slug>`, in
/// path order. A file without front matter is not a post and is skipped.
pub(crate) fn posts_with_slug(blog: &Path, slug: &str) -> Result<Vec<PathBuf>, Error> {
    let mut found = Vec::new();
    let mut dirs = vec![blog.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries =
            std::fs::read_dir(&dir).map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
        for entry in entries {
            let path = entry
                .map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?
                .path();
            // The blog plugin's default `exclude` drops `_`-prefixed files and
            // directories, and its glob skips hidden ones, so the site never
            // builds a post found there.
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(['_', '.']))
            {
                continue;
            }
            if path.is_dir() {
                dirs.push(path);
            } else if path
                .extension()
                .is_some_and(|ext| ext == "md" || ext == "mdx")
            {
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
                if slug_of(&text).as_deref() == Some(slug) {
                    found.push(path);
                }
            }
        }
    }
    found.sort();
    Ok(found)
}

/// The front matter lines of a post: those between an opening `---` on the
/// first line and the next `---`.
fn front_matter(text: &str) -> Option<impl Iterator<Item = &str>> {
    let mut lines = text.lines();
    if lines.next()? != "---" {
        return None;
    }
    let rest: Vec<&str> = lines.collect();
    let end = rest.iter().position(|l| *l == "---")?;
    Some(rest.into_iter().take(end))
}

/// A post's front matter parsed as a YAML mapping, or `None` when it has none
/// or it does not parse.
fn front_matter_map(text: &str) -> Option<serde_yaml::Mapping> {
    let yaml: Vec<&str> = front_matter(text)?.collect();
    serde_yaml::from_str(&yaml.join("\n")).ok()
}

/// The `slug` value of a post's front matter.
fn slug_of(text: &str) -> Option<String> {
    front_matter_map(text)?
        .get("slug")?
        .as_str()
        .map(str::to_owned)
}

/// The post's text with the `draft: true` line removed from its front matter,
/// or `None` when it carries none. Every other byte is kept.
pub(crate) fn undraft(text: &str) -> Option<String> {
    let lines = front_matter(text)?.count();
    let mut offset = 0;
    // The opening `---` and the front matter lines, each with its line ending.
    for line in text.split_inclusive('\n').take(lines + 1) {
        if line.trim_end() == DRAFT {
            return Some(format!(
                "{}{}",
                &text[..offset],
                &text[offset + line.len()..]
            ));
        }
        offset += line.len();
    }
    None
}

/// What can keep the post a draft once [`undraft`] has removed the exact
/// [`DRAFT`] line: a top-level key that is `draft`, the merge key `<<`, or not
/// a plain string, or front matter that does not parse. `None` when nothing
/// does.
fn unremovable_draft(text: &str) -> Option<&'static str> {
    let published = undraft(text).unwrap_or_else(|| text.to_owned());
    match front_matter_map(&published) {
        None => Some("front matter that does not parse as YAML"),
        // The parser keeps `<<` as a literal key and a sequence key as a
        // sequence, while the site build merges the one and reads a
        // one-element sequence key as its string, so either can hide `draft`.
        Some(map)
            if map
                .keys()
                .any(|k| k.as_str().is_none_or(|k| k == "draft" || k == "<<")) =>
        {
            Some(UNREMOVABLE_KEY)
        }
        Some(_) => None,
    }
}

/// The reason [`unremovable_draft`] gives for a front matter key.
const UNREMOVABLE_KEY: &str = "a key the release would not remove: 'draft', \
     a merge key '<<', or a key that is not a plain string";

/// Removes the draft flag from the post at `path`, in place. A post that is
/// already published is left as it is.
pub(crate) fn publish(path: &Path) -> Result<(), Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
    match undraft(&text) {
        Some(published) => {
            std::fs::write(path, published)
                .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
            println!("release: published {}", path.display());
        }
        None => println!("release: {} carries no draft flag", path.display()),
    }
    Ok(())
}

/// The one post `version` requires, or `None` for a patch release.
///
/// # Errors
///
/// When a minor release has no post with its slug, or more than one, or its
/// post's front matter has a key that could keep it a draft after the release
/// commit.
pub(crate) fn required(blog: &Path, version: Version) -> Result<Option<PathBuf>, Error> {
    let Some(slug) = version.post_slug() else {
        return Ok(None);
    };
    let mut posts = posts_with_slug(blog, &slug)?;
    match posts.len() {
        1 => {
            let path = posts.remove(0);
            let text = std::fs::read_to_string(&path)
                .map_err(|e| Error::msg(format!("{}: {e}", path.display())))?;
            if let Some(problem) = unremovable_draft(&text) {
                return Err(Error::msg(format!(
                    "{} has {problem}.\n  \
                     The release removes only the exact line '{DRAFT}'; write the flag that way.",
                    path.display()
                )));
            }
            Ok(Some(path))
        }
        0 => Err(Error::msg(format!(
            "{version} is a minor release and {BLOG}/ has no post with 'slug: {slug}'.\n  \
             Merge the release post first, as a draft; {BLOG}/README.md has the convention."
        ))),
        _ => Err(Error::msg(format!(
            "more than one post in {BLOG}/ has 'slug: {slug}': {}.\n  \
             The release publishes exactly one; remove the others.",
            posts
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The URL the release post for `version` is served at, under the site named
/// by `[workspace.package] homepage` in `manifest`, or `None` for a patch
/// release.
///
/// # Errors
///
/// When the manifest does not parse or names no homepage.
pub(crate) fn url(manifest: &str, version: Version) -> Result<Option<String>, Error> {
    let Some(slug) = version.post_slug() else {
        return Ok(None);
    };
    let table: toml::Table =
        toml::from_str(manifest).map_err(|e| Error::msg(format!("Cargo.toml: {e}")))?;
    let site = table
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("homepage"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| Error::msg("Cargo.toml: no [workspace.package] homepage"))?;
    Ok(Some(format!("{}/blog/{slug}", site.trim_end_matches('/'))))
}

#[cfg(test)]
mod tests {
    use super::*;

    const POST: &str = "---\ntitle: Spate 0.4\nslug: releases/0.4\nauthors: [marcus]\ntags: [release]\ndraft: true\n---\n\nThe opener.\n\n<!-- truncate -->\n\ndraft: true\n";

    /// Removing the flag drops that one front matter line and keeps every other
    /// byte, including a body line that happens to read the same.
    #[test]
    fn undraft_removes_only_the_front_matter_flag() {
        assert_eq!(
            undraft(POST).unwrap(),
            "---\ntitle: Spate 0.4\nslug: releases/0.4\nauthors: [marcus]\ntags: [release]\n---\n\nThe opener.\n\n<!-- truncate -->\n\ndraft: true\n"
        );
        assert_eq!(undraft(&undraft(POST).unwrap()), None);
        assert_eq!(undraft("No front matter.\ndraft: true\n"), None);
    }

    /// Only the exact flag line is removable; a `draft` key in another spelling,
    /// a merge key, a key that is not a plain string, or front matter that does
    /// not parse, is named.
    #[test]
    fn a_draft_the_release_cannot_remove_is_named() {
        assert_eq!(unremovable_draft(POST), None);
        assert_eq!(unremovable_draft("---\nslug: releases/0.4\n---\n"), None);
        for front_matter in ["draft : true", "<<: {title: x}", "[draft]: true", "1: x"] {
            assert_eq!(
                unremovable_draft(&format!("---\n{front_matter}\n---\n")),
                Some(UNREMOVABLE_KEY),
                "{front_matter:?}"
            );
        }
        assert_eq!(
            unremovable_draft("---\ndraft: true\ntitle: [unclosed\n---\n"),
            Some("front matter that does not parse as YAML")
        );
    }

    /// A slug is read from the front matter, quoted or not, and nowhere else.
    #[test]
    fn the_slug_is_read_from_the_front_matter() {
        assert_eq!(slug_of(POST).as_deref(), Some("releases/0.4"));
        assert_eq!(
            slug_of("---\nslug: \"releases/0.4\"\n---\n").as_deref(),
            Some("releases/0.4")
        );
        assert_eq!(
            slug_of("---\nslug: 'releases/0.4'\n---\n").as_deref(),
            Some("releases/0.4")
        );
        assert_eq!(slug_of("# Title\n\nslug: releases/0.4\n"), None);
    }

    /// A minor release's post is served under the manifest's homepage; a patch
    /// release has none.
    #[test]
    fn the_post_url_is_under_the_homepage() {
        let manifest =
            "[workspace.package]\nversion = \"0.3.0\"\nhomepage = \"https://example.test/\"\n";
        let v = |s| Version::parse(s).unwrap();
        assert_eq!(
            url(manifest, v("0.4.0")).unwrap().as_deref(),
            Some("https://example.test/blog/releases/0.4")
        );
        assert_eq!(url(manifest, v("0.4.1")).unwrap(), None);
        assert!(url("[workspace.package]\n", v("0.4.0")).is_err());
    }
}
