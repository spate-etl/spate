//! Holds the built documentation site to a floor on its file count.

use std::path::Path;

use crate::run::{Error, Outcome};

/// The build output, relative to the repository root.
const BUILD_DIR: &str = "website/build";

/// A build carrying every page stays well above this.
const FLOOR: usize = 200;

pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(counts the files under {BUILD_DIR})");
        return Ok(());
    }
    let count = count_files(&root.join(BUILD_DIR))?;
    println!("site files: {count}");
    clears_floor(count)
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
    use std::path::Path;

    use super::{FLOOR, clears_floor, count_files};
    use crate::checks::scratch::Scratch;

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
        std::os::unix::fs::symlink(build.join("index.html"), build.join("link.html")).unwrap();
        std::os::unix::fs::symlink(&build, build.join("loop")).unwrap();
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
}
