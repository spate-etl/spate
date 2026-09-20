//! Which CI jobs a set of changed paths needs.
//!
//! Classification is an ignore-list: anything not recognised as documentation
//! counts as code. An allow-list fails open, so a new source directory would
//! silently stop being tested.

use std::collections::BTreeSet;

use super::graph::Graph;
use super::outputs::{Lane, Outputs, Shard};

/// The webhook event a run was started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Event {
    PullRequest,
    MergeGroup,
    /// push, schedule and workflow_dispatch: no diff to reason about.
    ForceAll,
}

/// What a run knows about its pull request.
#[derive(Debug, Default)]
pub(crate) struct Context {
    pub(crate) author: String,
    pub(crate) labels: Vec<String>,
}

/// `default` labels the unmodified build. It is not a cargo feature name:
/// packages need not declare a `default` key, and cargo rejects
/// `--features default` when absent.
fn feature_arms_for(pkg: &str) -> Vec<(&'static str, &'static str)> {
    match pkg {
        // `simd` replaces the byte-slice-to-value decoder behind the backend
        // seam: one set of benches over two implementations.
        "spate-json" => vec![("default", ""), ("simd", "simd")],
        _ => vec![("default", "")],
    }
}

/// True when `path` matches a bash `case` glob, where `*` spans `/` as well.
fn glob(path: &str, pattern: &str) -> bool {
    debug_assert!(
        pattern.matches('*').count() <= 1,
        "one `*` per pattern: a second is treated as a literal and silently never matches"
    );
    match pattern.split_once('*') {
        None => path == pattern,
        Some((head, tail)) => {
            path.len() >= head.len() + tail.len() && path.starts_with(head) && path.ends_with(tail)
        }
    }
}

fn any_glob(path: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| glob(path, p))
}

/// The crate a `crates/<name>/...` path belongs to.
fn crate_of(path: &str) -> Option<&str> {
    path.strip_prefix("crates/")?.split('/').next()
}

/// Where a path sits before the code questions are asked.
enum Kind {
    /// Documentation or repo furniture: no job needs it.
    Skip,
    /// Rebuilds the site and needs no Rust build.
    SiteOnly,
    /// Code, and it rebuilds the site too.
    CodeAndSite,
    /// Code.
    Code,
}

/// First match wins, and the order matters.
fn kind_of(path: &str) -> Kind {
    // `crates/*` selects the site as well: a docs page's Rust snippets are
    // regions of files under it, so an edit there can change a rendered page
    // without touching `docs/`. The whole tree, because a `file=` attribute may
    // point anywhere under it and a narrower list goes stale by failing open.
    if glob(path, "crates/*") {
        return Kind::CodeAndSite;
    }
    // Transcludable, so an edit here can change a rendered page. Ahead of the
    // `*.md` arm below, which would otherwise drop the site rebuild.
    if glob(path, "examples/*.md") {
        return Kind::SiteOnly;
    }
    if glob(path, "examples/*") {
        return Kind::CodeAndSite;
    }
    if any_glob(path, &["scripts/*", "bench/*", "xtask/*"]) {
        return Kind::Code;
    }
    // CI definitions decide what every other job does. The toolchain pin
    // reaches every job, and only some of them name a pin.
    if any_glob(
        path,
        &[
            ".github/workflows/*",
            ".github/actions/*",
            ".github/toolchains/*",
        ],
    ) {
        return Kind::Code;
    }
    if glob(path, ".github/*") {
        return Kind::Skip;
    }
    // Ahead of the `*.md` arm, which would otherwise take fuzz/README.md.
    if glob(path, "fuzz/*") {
        return Kind::Code;
    }
    // `.gitmodules` pins the benchmark data the site renders. `.node-version`
    // pins the Node the site build runs on.
    if any_glob(
        path,
        &["docs/*", "website/*", ".gitmodules", ".node-version"],
    ) {
        return Kind::SiteOnly;
    }
    if any_glob(path, &["*.md", "LICENSE", ".gitignore", ".dockerignore"]) {
        return Kind::Skip;
    }
    Kind::Code
}

/// Files that can change whether `cargo package` succeeds or whether the
/// declared floors still resolve. The gate's own apparatus is included, so an
/// edit narrowing the gate is itself gated.
pub(crate) fn is_manifest(path: &str) -> bool {
    any_glob(
        path,
        &[
            "Cargo.toml",
            "Cargo.lock",
            "crates/*/Cargo.toml",
            "bench/Cargo.toml",
            "fuzz/Cargo.toml",
            "xtask/Cargo.toml",
            "scripts/release-version.sh",
            "xtask/*",
            ".github/workflows/ci.yml",
            ".github/toolchains/*",
        ],
    )
}

/// Classify a change. Pure: no git, no environment, no filesystem beyond the
/// graph handed in.
pub(crate) fn classify(
    paths: &[String],
    event: Event,
    ctx: &Context,
    graph: &Graph,
    extra_clickhouse_lanes: &[String],
) -> Outputs {
    let mut out = Outputs::default();
    let mut bench_pkgs: BTreeSet<String> = BTreeSet::new();
    // Held apart from the suite list because the Dependabot deferral empties
    // that one, and an image bump is exempt from it.
    let mut image_suites: BTreeSet<String> = BTreeSet::new();

    if event == Event::ForceAll {
        out.rust = true;
        out.site = true;
        out.fuzz = true;
        out.bench = true;
        out.manifests = true;
        bench_pkgs = graph.all_bench_pkgs().clone();
        out.container_pkgs = graph.all_container_pkgs().clone();
        out.semver_pkgs = graph.all_semver_pkgs().clone();
    } else {
        for path in paths {
            let path = path.as_str();
            out.manifests |= is_manifest(path);
            match kind_of(path) {
                Kind::Skip => continue,
                Kind::SiteOnly => {
                    out.site = true;
                    continue;
                }
                Kind::CodeAndSite => out.site = true,
                Kind::Code => {}
            }
            out.rust = true;

            // Which container suites can this file reach?
            if let Some(name) = crate_of(path) {
                out.container_pkgs.extend(graph.container_suites_for(name));
            }
            // The pinned server image one suite runs against. Booting the
            // others for it proves nothing.
            if glob(path, "ci/clickhouse/*") {
                image_suites.extend(graph.container_suites_for("spate-clickhouse"));
                out.container_pkgs.extend(image_suites.iter().cloned());
            }
            // A dependency, lint or apparatus change moves the whole graph.
            if any_glob(
                path,
                &[
                    "Cargo.lock",
                    "Cargo.toml",
                    "deny.toml",
                    "rust-toolchain.toml",
                    ".cargo/*",
                    ".config/*",
                    ".github/workflows/*",
                    ".github/actions/*",
                    "scripts/*",
                    "xtask/*",
                ],
            ) {
                out.container_pkgs
                    .extend(graph.all_container_pkgs().iter().cloned());
                out.site = true;
            }

            // Whose published API can this file move? A narrower question: a
            // lint or tooling change cannot move a signature.
            if let Some(name) = crate_of(path) {
                out.semver_pkgs.extend(graph.semver_closure_for(name));
            }
            if any_glob(
                path,
                &[
                    "Cargo.lock",
                    "Cargo.toml",
                    ".github/workflows/ci.yml",
                    ".github/actions/*",
                    "xtask/*",
                ],
            ) {
                out.semver_pkgs
                    .extend(graph.all_semver_pkgs().iter().cloned());
            }

            // Which files can move an instruction count? The unit is the whole
            // crate, because codegen is crate-global.
            if let Some(name) = crate_of(path) {
                let selected = graph.bench_pkgs_for(name);
                if !selected.is_empty() {
                    out.bench = true;
                    bench_pkgs.extend(selected);
                }
            }
            // The measuring apparatus: a change to what is discovered, which
            // crates are chosen, or how results are read alters every crate's
            // outcome. Without this, the change rewriting bench selection is
            // the one that never runs them.
            if any_glob(
                path,
                &["xtask/*", ".github/workflows/ci.yml", ".github/actions/*"],
            ) {
                out.bench = true;
                bench_pkgs.extend(graph.all_bench_pkgs().iter().cloned());
            }

            // The harness itself, the fuzz job's apparatus, and the crates the
            // harness depends on.
            if any_glob(
                path,
                &[
                    "fuzz/*",
                    "xtask/*",
                    ".github/workflows/ci.yml",
                    ".github/actions/*",
                    ".github/toolchains/*",
                ],
            ) {
                out.fuzz = true;
            }
            if crate_of(path).is_some_and(|c| graph.is_fuzz_dependency(c)) {
                out.fuzz = true;
            }
        }
    }

    apply_deferrals(&mut out, event, ctx, &image_suites);
    apply_labels(&mut out, &mut bench_pkgs, ctx, graph);

    out.bench_shards = bench_pkgs
        .iter()
        .flat_map(|pkg| {
            feature_arms_for(pkg)
                .into_iter()
                .map(move |(arm, feats)| Shard {
                    package: pkg.clone(),
                    arm: arm.to_string(),
                    cargo_features: feats.to_string(),
                })
        })
        .collect();
    // An empty `include:` fails the workflow, so the boolean and the array
    // have to agree.
    if out.bench_shards.is_empty() {
        out.bench = false;
    }

    if out.container_pkgs.contains("spate-clickhouse") {
        out.clickhouse_lanes = extra_clickhouse_lanes
            .iter()
            .map(|l| Lane { lane: l.clone() })
            .collect();
    }

    out
}

/// A dependency bump touches `Cargo.lock`, which reaches every container
/// suite: one boot of every service per bump and per rebase. These keep the
/// cheap tier and give up the suites, which the push-to-main run re-runs.
///
/// The release pull request is the same trade: its diff is generated artefacts
/// only, and its source tree is byte-identical to the `main` commit that just
/// ran the suites on push.
fn apply_deferrals(
    out: &mut Outputs,
    event: Event,
    ctx: &Context,
    image_suites: &BTreeSet<String>,
) {
    if event != Event::PullRequest {
        return;
    }
    match ctx.author.as_str() {
        "dependabot[bot]" => {
            out.container_pkgs.clear();
            // A bump to a pinned server image changes what the suite runs
            // against, so deferring it defers the only check that would object.
            out.container_pkgs.extend(image_suites.iter().cloned());
        }
        "spate-release[bot]" => {
            out.container_pkgs.clear();
            // The semver gate skips it for the same reason and needs no
            // backstop: the published API is a property of the source tree, and
            // this tree already passed the gate as the `main` commit above. A
            // Dependabot bump is not the same trade, so it is absent there.
            out.semver_pkgs.clear();
        }
        _ => {}
    }
}

/// `ci: docker` and `ci: bench` force a suite on for a change whose paths would
/// not have selected it. They can only add: an override able to clear a
/// selection would make the classifier fail open.
///
/// Applied after the deferrals, so labelling a bot's pull request overrides one
/// for that one bump.
fn apply_labels(
    out: &mut Outputs,
    bench_pkgs: &mut BTreeSet<String>,
    ctx: &Context,
    graph: &Graph,
) {
    let has = |name: &str| ctx.labels.iter().any(|l| l == name);
    if has("ci: docker") {
        out.container_pkgs
            .extend(graph.all_container_pkgs().iter().cloned());
    }
    if has("ci: bench") {
        out.bench = true;
        bench_pkgs.extend(graph.all_bench_pkgs().iter().cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph() -> Graph {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        Graph::load(root).expect("the workspace graph loads")
    }

    fn run(paths: &[&str]) -> Outputs {
        let owned: Vec<String> = paths.iter().map(|p| (*p).to_string()).collect();
        classify(
            &owned,
            Event::PullRequest,
            &Context::default(),
            &graph(),
            &[],
        )
    }

    fn suites(out: &Outputs) -> Vec<&str> {
        out.container_pkgs.iter().map(String::as_str).collect()
    }

    #[test]
    fn documentation_needs_no_build() {
        let out = run(&["docs/INVARIANTS.md", "README.md", ".github/labels.yml"]);
        assert!(!out.rust, "prose compiles nothing");
        assert!(!out.fuzz);
        assert!(out.container_pkgs.is_empty());
    }

    #[test]
    fn a_docs_page_rebuilds_the_site() {
        assert!(run(&["docs/guide.md"]).site);
    }

    #[test]
    fn a_crate_source_rebuilds_the_site() {
        // Docs pages transclude regions of files under `crates/`.
        assert!(run(&["crates/spate-core/src/lib.rs"]).site);
    }

    #[test]
    fn a_nested_readme_is_not_root_prose() {
        // `#![doc = include_str!(...)]` can compile a crate README into the
        // library, so the `*.md` arm must not reach it.
        assert!(run(&["crates/spate/README.md"]).rust);
    }

    #[test]
    fn a_connector_change_selects_its_own_suite() {
        assert_eq!(
            suites(&run(&["crates/spate-kafka/src/lib.rs"])),
            ["spate", "spate-kafka"]
        );
    }

    #[test]
    fn a_core_change_selects_every_suite() {
        let out = run(&["crates/spate-core/src/lib.rs"]);
        assert_eq!(
            suites(&out),
            [
                "spate",
                "spate-clickhouse",
                "spate-coordination",
                "spate-kafka",
                "spate-s3"
            ]
        );
    }

    #[test]
    fn a_lockfile_change_reaches_every_public_signature() {
        let out = run(&["Cargo.lock"]);
        assert_eq!(out.semver_pkgs.len(), graph().all_semver_pkgs().len());
    }

    #[test]
    fn spate_test_moves_no_other_published_api() {
        // Nothing depends on it outside dev-dependencies.
        let out = run(&["crates/spate-test/src/lib.rs"]);
        assert_eq!(
            out.semver_pkgs
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["spate-test"]
        );
    }

    #[test]
    fn documentation_moves_no_published_api() {
        assert!(run(&["docs/INVARIANTS.md"]).semver_pkgs.is_empty());
    }

    #[test]
    fn a_server_image_bump_selects_that_suite_alone() {
        // Booting Kafka and the object store for it proves nothing.
        assert_eq!(
            suites(&run(&["ci/clickhouse/lts/Dockerfile"])),
            ["spate", "spate-clickhouse"]
        );
    }

    #[test]
    fn an_unbenched_crate_runs_no_shards() {
        let out = run(&["crates/spate-test/src/lib.rs"]);
        assert!(!out.bench);
        assert!(
            out.bench_shards.is_empty(),
            "the boolean and the array have to agree"
        );
    }

    #[test]
    fn a_benched_crate_runs_its_own_shards() {
        let out = run(&["crates/spate-kafka/src/lib.rs"]);
        assert!(out.bench);
        assert_eq!(
            out.bench_shards
                .iter()
                .map(|s| s.package.as_str())
                .collect::<Vec<_>>(),
            ["spate-kafka"]
        );
    }

    #[test]
    fn every_arm_names_a_feature_its_package_declares() {
        let g = graph();
        for pkg in g.all_bench_pkgs() {
            let declared = g.features_of(pkg).expect("a workspace member");
            let arms = feature_arms_for(pkg);
            for (label, feats) in &arms {
                for feat in feats.split(',').filter(|f| !f.is_empty()) {
                    assert!(
                        declared.contains(feat),
                        "{pkg}'s `{label}` arm names `{feat}`, which it does not declare"
                    );
                }
            }
            assert_eq!(
                arms.iter().filter(|(_, f)| f.is_empty()).count(),
                1,
                "{pkg} needs exactly one arm building its default features"
            );
            let labels: BTreeSet<&str> = arms.iter().map(|(l, _)| *l).collect();
            assert_eq!(labels.len(), arms.len(), "{pkg} has a duplicate arm label");
        }
    }

    #[test]
    fn at_least_one_crate_is_measured_under_two_arms() {
        let g = graph();
        assert!(
            g.all_bench_pkgs()
                .iter()
                .any(|p| feature_arms_for(p).len() > 1),
            "the second dimension of the counter matrix has no member left"
        );
    }

    #[test]
    fn the_second_arm_gets_its_own_shard() {
        let shards = run(&["crates/spate-json/src/lib.rs"]).bench_shards;
        let arms: Vec<&str> = shards.iter().map(|s| s.arm.as_str()).collect();
        assert_eq!(arms, ["default", "simd"]);
    }

    #[test]
    fn the_apparatus_selects_every_bench() {
        let out = run(&["xtask/src/ci/classify.rs"]);
        assert!(
            out.bench,
            "the change rewriting bench selection must run them"
        );
        assert_eq!(out.bench_shards.len(), graph().all_bench_pkgs().len() + 1);
    }

    /// A fuzz target is a workspace member's source, so it builds the harness
    /// and everything `--workspace` reaches, and moves no manifest.
    #[test]
    fn a_fuzz_target_builds_the_harness_and_the_workspace() {
        let out = run(&["fuzz/fuzz_targets/s3_split_id.rs"]);
        assert!(out.fuzz, "the harness has to build");
        assert!(out.rust, "`--workspace` compiles it");
        assert!(!out.manifests, "a target moves no dependency graph");
        assert!(out.container_pkgs.is_empty());
        assert!(out.semver_pkgs.is_empty());
        assert!(out.bench_shards.is_empty());
    }

    /// The harness resolves from the root lockfile, so its manifest reaches the
    /// gates that read declared floors.
    #[test]
    fn the_fuzz_manifest_is_a_manifest() {
        let out = run(&["fuzz/Cargo.toml"]);
        assert!(out.manifests);
        assert!(out.fuzz);
        assert!(out.rust);
    }

    #[test]
    fn a_fuzz_dependency_builds_the_harness() {
        assert!(run(&["crates/spate-core/src/lib.rs"]).fuzz);
        assert!(!run(&["crates/spate-test/src/lib.rs"]).fuzz);
    }

    #[test]
    fn labels_only_add() {
        let ctx = Context {
            author: String::new(),
            labels: vec!["ci: docker".into()],
        };
        let paths = vec!["docs/a.md".to_string()];
        let out = classify(&paths, Event::PullRequest, &ctx, &graph(), &[]);
        assert_eq!(out.container_pkgs, *graph().all_container_pkgs());
        assert!(
            !out.rust,
            "an override cannot clear the path-derived baseline"
        );
    }

    #[test]
    fn dependabot_defers_the_suites_but_not_an_image_bump() {
        let ctx = Context {
            author: "dependabot[bot]".into(),
            labels: vec![],
        };
        let lock = vec!["Cargo.lock".to_string()];
        assert!(
            classify(&lock, Event::PullRequest, &ctx, &graph(), &[])
                .container_pkgs
                .is_empty()
        );

        let image = vec!["ci/clickhouse/lts/Dockerfile".to_string()];
        let out = classify(&image, Event::PullRequest, &ctx, &graph(), &[]);
        assert_eq!(suites(&out), ["spate", "spate-clickhouse"]);
    }

    #[test]
    fn the_release_pull_request_defers_the_semver_gate_too() {
        let ctx = Context {
            author: "spate-release[bot]".into(),
            labels: vec![],
        };
        let paths = vec!["Cargo.lock".to_string()];
        let out = classify(&paths, Event::PullRequest, &ctx, &graph(), &[]);
        assert!(out.container_pkgs.is_empty());
        assert!(out.semver_pkgs.is_empty());
    }

    #[test]
    fn a_deferral_applies_to_pull_requests_alone() {
        let ctx = Context {
            author: "dependabot[bot]".into(),
            labels: vec![],
        };
        let paths = vec!["Cargo.lock".to_string()];
        let out = classify(&paths, Event::MergeGroup, &ctx, &graph(), &[]);
        assert!(!out.container_pkgs.is_empty());
    }

    #[test]
    fn force_all_selects_everything() {
        let out = classify(&[], Event::ForceAll, &Context::default(), &graph(), &[]);
        assert!(out.rust && out.site && out.fuzz && out.bench && out.manifests);
        assert_eq!(out.container_pkgs, *graph().all_container_pkgs());
        assert_eq!(out.semver_pkgs, *graph().all_semver_pkgs());
    }

    #[test]
    fn clickhouse_lanes_follow_the_suite() {
        let lanes = ["lts-previous".to_string()];
        let paths = vec!["crates/spate-clickhouse/src/lib.rs".to_string()];
        let out = classify(
            &paths,
            Event::PullRequest,
            &Context::default(),
            &graph(),
            &lanes,
        );
        assert_eq!(out.clickhouse_lanes.len(), 1);

        let other = vec!["crates/spate-kafka/src/lib.rs".to_string()];
        let out = classify(
            &other,
            Event::PullRequest,
            &Context::default(),
            &graph(),
            &lanes,
        );
        assert!(out.clickhouse_lanes.is_empty());
    }

    #[test]
    fn a_manifest_change_is_seen() {
        assert!(run(&["crates/spate-s3/Cargo.toml"]).manifests);
        assert!(!run(&["docs/a.md"]).manifests);
    }

    #[test]
    fn a_matrix_entry_is_escaped_rather_than_concatenated() {
        let out = run(&["crates/spate-json/src/lib.rs"]);
        let rendered = out.to_string();
        let line = rendered
            .lines()
            .find(|l| l.starts_with("bench-shards="))
            .unwrap();
        assert_eq!(
            line.lines().count(),
            1,
            "a multi-line value breaks $GITHUB_OUTPUT"
        );
        assert!(line.contains(r#""package":"spate-json""#));
    }
}
