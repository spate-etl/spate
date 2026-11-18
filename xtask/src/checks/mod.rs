//! The gates this binary runs in process: the allocation snapshots, the
//! attribution inventory, the decision records, the changelog fragments,
//! labels, docs.rs rustdoc, the instruction-count bench targets, their collected regions, the counted-tier
//! report, the pinned container images, the semver comparison against the
//! published release, the commit subject rule, supported versions, and the
//! built documentation site.

use std::fs;
use std::path::Path;

use crate::run::Error;

pub(crate) mod adr;
pub(crate) mod alloc_snapshot;
pub(crate) mod attribution;
pub(crate) mod changelog;
pub(crate) mod collected_region;
pub(crate) mod container_image;
pub(crate) mod docsrs;
pub(crate) mod gungraun;
pub(crate) mod perf_report;
pub(crate) mod scratch;
pub(crate) mod semver_checks;
pub(crate) mod site_check;
pub(crate) mod subject;
pub(crate) mod supported_versions;
pub(crate) mod sync_labels;

/// The file at `path` as text, with the path in the error.
pub(crate) fn read(path: &Path) -> Result<String, Error> {
    fs::read_to_string(path).map_err(|e| Error::msg(format!("{}: {e}", path.display())))
}
