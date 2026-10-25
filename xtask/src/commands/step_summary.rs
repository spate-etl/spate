//! The job summary a command writes on a CI runner, and the fence that keeps
//! outside text inside a code block there.

use std::path::PathBuf;

/// The job summary file. Each write replaces what this run wrote before and
/// keeps what the file held when the run started.
pub(super) struct Summary {
    path: PathBuf,
    from: u64,
}

impl Summary {
    pub(super) fn at(path: PathBuf) -> Self {
        let from = std::fs::metadata(&path).map_or(0, |m| m.len());
        Self { path, from }
    }

    /// Writes `text` after what the file held at the start. A failure is
    /// reported on stderr and is not an error of the run.
    pub(super) fn write(&self, text: &str) {
        use std::io::{Seek, SeekFrom, Write};
        let written = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .and_then(|mut f| {
                f.set_len(self.from)?;
                f.seek(SeekFrom::End(0))?;
                f.write_all(text.as_bytes())
            });
        if let Err(e) = written {
            eprintln!("{}: {e}", self.path.display());
        }
    }
}

/// A backtick fence longer than any backtick run in `text`.
pub(super) fn fence(text: &str) -> String {
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    "`".repeat(longest.max(2) + 1)
}
