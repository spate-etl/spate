//! Test helpers shared across this workspace's crates. Not published: some
//! depend on this repository's layout and its `cargo xtask` task runner.

pub mod alloc;
mod child;
mod corpus;
#[cfg(feature = "elasticmq")]
mod elasticmq;
mod http;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "tls")]
mod tls;

pub use child::{ChildTest, run_in_child};
pub use corpus::{fnv1a, pin};
#[cfg(feature = "elasticmq")]
pub use elasticmq::{ElasticMq, Message, QUERY_PORT};
pub use http::{http, http_with_body, http_with_headers};
#[cfg(feature = "s3")]
pub use s3::{ACCESS_KEY, REGION, S3Store, SECRET_KEY, Store};
#[cfg(feature = "tls")]
pub use tls::{TestCa, native_certs, pem, serve_tls};

use std::path::Path;
use std::process::Command;

/// Runs `cargo xtask container-image` with `args` and returns the image it
/// prints, as `name` and `tag`.
///
/// `ci/<service>/` pins each image by tag and digest, and the task runner is
/// its one parser. testcontainers references an image as `name:tag` only, so
/// the digest is dropped; `--pull` fetches by digest and re-tags locally first.
///
/// # Panics
///
/// Panics when the task runner fails, with its stderr, or prints no tag.
#[must_use]
pub fn container_image(args: &[&str]) -> (String, String) {
    split_reference(&xtask_container_image(args))
}

/// Runs `cargo xtask container-image --selected-lane <service>` and returns the
/// lane the service's suites run against: the one `SPATE_<SERVICE>_LANE`
/// names, or its primary lane.
///
/// # Panics
///
/// Panics when the task runner fails, with its stderr.
#[must_use]
pub fn selected_lane(service: &str) -> String {
    xtask_container_image(&["--selected-lane", service])
}

/// The line `cargo xtask container-image` prints for `args`.
fn xtask_container_image(args: &[&str]) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let out = Command::new("cargo")
        .args(["xtask", "container-image"])
        .args(args)
        .current_dir(&root)
        .output()
        .unwrap_or_else(|e| panic!("run cargo xtask container-image {args:?}: {e}"));
    assert!(
        out.status.success(),
        "cargo xtask container-image {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn split_reference(reference: &str) -> (String, String) {
    let (name, tag) = reference
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("no tag in {reference}"));
    (name.to_owned(), tag.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tag is split at the last colon, so a registry port stays in the name.
    #[test]
    fn split_reference_keeps_a_registry_port_in_the_name() {
        assert_eq!(
            split_reference("registry.local:5000/vendor/db:9.4"),
            ("registry.local:5000/vendor/db".to_owned(), "9.4".to_owned())
        );
    }
}
