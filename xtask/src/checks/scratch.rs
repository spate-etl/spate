//! A working directory under the system temporary directory, and the nonce that
//! keeps two paths made under one process id apart.

use std::path::{Path, PathBuf};

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
        let dir = std::env::temp_dir().join(format!("{prefix}.{}.{}", std::process::id(), nonce()));
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

/// Distinguishes paths made under the same process id.
pub(crate) fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}
