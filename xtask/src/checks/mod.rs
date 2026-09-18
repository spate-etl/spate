//! The gates this binary runs in process: the attribution artifacts, the
//! decision records, labels, docs.rs rustdoc, the instruction-count bench
//! targets, their collected regions, the counted-tier report, the pinned
//! container images, the semver comparison against the published release, and
//! supported versions.

pub(crate) mod adr;
pub(crate) mod attribution;
pub(crate) mod collected_region;
pub(crate) mod container_image;
pub(crate) mod docsrs;
pub(crate) mod gungraun;
pub(crate) mod perf_report;
pub(crate) mod scratch;
pub(crate) mod semver_checks;
pub(crate) mod supported_versions;
pub(crate) mod sync_labels;
