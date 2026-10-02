//! Which workspace packages a local diff can affect.

use std::collections::BTreeSet;

use super::classify::{crate_of, is_manifest, is_rust_change};
use super::graph::Graph;

/// What a local run covers.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    /// The diff reaches something shared, or no diff could be read.
    Full,
    /// The packages the diff can break. Empty when it touches no Rust.
    Packages(BTreeSet<String>),
}

/// A path outside `crates/<name>/` that changes Rust, and any manifest, widens
/// the scope to everything: those files reach packages the dependency graph
/// does not name.
pub(crate) fn scope(paths: &[String], graph: &Graph) -> Scope {
    let mut pkgs = BTreeSet::new();
    for path in paths.iter().filter(|p| is_rust_change(p)) {
        if is_manifest(path) {
            return Scope::Full;
        }
        match crate_of(path) {
            Some(name) => pkgs.extend(graph.test_closure_for(name)),
            None => return Scope::Full,
        }
    }
    Scope::Packages(pkgs)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn run(paths: &[&str]) -> Scope {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask/ has a parent");
        let graph = Graph::load(root).expect("the workspace graph loads");
        scope(
            &paths.iter().map(|p| (*p).to_owned()).collect::<Vec<_>>(),
            &graph,
        )
    }

    fn packages(paths: &[&str]) -> BTreeSet<String> {
        match run(paths) {
            Scope::Packages(p) => p,
            Scope::Full => panic!("{paths:?} widened to the whole workspace"),
        }
    }

    /// A change to a leaf crate covers that crate and what depends on it.
    #[test]
    fn a_source_edit_covers_the_crate_and_its_dependents() {
        let pkgs = packages(&["crates/spate-s3/src/lib.rs"]);
        assert!(pkgs.contains("spate-s3"));
        assert!(!pkgs.contains("spate-core"), "a dependency is not affected");
        assert!(
            !pkgs.contains("spate-fuzz"),
            "the fuzz job builds the harness"
        );
    }

    /// Every crate depends on the core, so an edit there covers them all.
    #[test]
    fn a_core_edit_covers_a_dependent() {
        let pkgs = packages(&["crates/spate-core/src/lib.rs"]);
        assert!(pkgs.contains("spate-core") && pkgs.contains("spate-s3"));
    }

    #[test]
    fn documentation_and_changelog_fragments_cover_no_package() {
        assert!(
            packages(&[
                "docs/user-guide/a.md",
                "changelog.d/x.fixed.md",
                "README.md"
            ])
            .is_empty()
        );
    }

    /// The diff widens on a manifest, a lockfile, the shared test crate, tooling
    /// and any Rust outside a crate directory.
    #[test]
    fn a_shared_file_widens_to_everything() {
        for path in [
            "Cargo.lock",
            "Cargo.toml",
            "crates/spate-s3/Cargo.toml",
            "test-support/src/lib.rs",
            "xtask/src/main.rs",
            "scripts/release.sh",
            "rust-toolchain.toml",
            ".config/nextest.toml",
            ".github/workflows/ci.yml",
            "examples/pipeline/src/main.rs",
            "some-new-dir/lib.rs",
        ] {
            assert_eq!(run(&[path]), Scope::Full, "{path}");
        }
    }

    /// One widening path wins over any number of narrow ones.
    #[test]
    fn one_shared_file_among_crate_edits_widens() {
        assert_eq!(
            run(&["crates/spate-s3/src/lib.rs", "Cargo.lock"]),
            Scope::Full
        );
    }
}
