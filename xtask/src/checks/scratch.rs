//! A private working directory under the system temporary directory, and path
//! names distinct within a process.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::run::Error;

/// A directory holding a check's working files, removed with its contents on
/// drop.
pub(crate) struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    /// Creates the directory exclusively, and on unix reachable only by its
    /// owner, so no other user can put anything at a path written under it.
    pub(crate) fn new(prefix: &str) -> Result<Self, Error> {
        let dir = std::env::temp_dir().join(unique_name(prefix));
        private_dir(&dir).map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
        Ok(Self { dir })
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn join(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.dir));
    }
}

#[cfg(unix)]
fn private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().mode(0o700).create(path)
}

#[cfg(not(unix))]
fn private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new().create(path)
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// `{prefix}.{pid}.{nanos}.{n}`, where `n` differs on every call in the process.
pub(crate) fn unique_name(prefix: &str) -> String {
    name_at(prefix, now_nanos())
}

fn name_at(prefix: &str, nanos: u128) -> String {
    let n = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}.{}.{nanos}.{n}", std::process::id())
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::name_at;

    /// Two names built at one clock reading differ. Regression for #706.
    #[test]
    fn names_made_at_one_instant_differ() {
        assert_ne!(name_at("p", 0), name_at("p", 0));
    }
}
