//! The gates this binary runs in process: the attribution artifacts, the
//! decision records, labels, docs.rs rustdoc, the instruction-count bench
//! targets, the pinned container images, and supported versions.

pub(crate) mod adr;
pub(crate) mod attribution;
pub(crate) mod container_image;
pub(crate) mod docsrs;
pub(crate) mod gungraun;
pub(crate) mod supported_versions;
pub(crate) mod sync_labels;
