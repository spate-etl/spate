//! Backfill tests against a real S3 API: the store `SPATE_S3_LANE` selects,
//! in a container.
//!
//! This suite covers what the local-filesystem suite cannot: real
//! ListObjectsV2 pagination across pages (>1000 keys) feeding the
//! lister, and HTTP endpoint/option passthrough through the `store`
//! map.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-s3 --test s3_store -- --ignored
//! ```

mod support;

use spate_core::pipeline::ExitState;
use std::time::Duration;
use support::store::Gateway;
use support::{PipelineYaml, captured_rows, launch, lines_bytes, recs, sorted, test_options};

fn config_yaml(gw: &Gateway) -> String {
    PipelineYaml::bucket("s3-store-test", gw).build()
}

/// More than 1000 keys forces ListObjectsV2 pagination; every key must reach
/// the feed exactly as listed across page boundaries.
#[test]
#[ignore = "requires Docker"]
fn paginated_listing_backfills_completely() {
    let gw = Gateway::start("spate-s3-pagination");
    let total = 1200usize;
    let objects: Vec<(String, Vec<u8>)> = (0..total)
        .map(|i| {
            (
                format!("data/obj-{i:05}.ndjson"),
                lines_bytes(&recs(&format!("o{i}"), 2)),
            )
        })
        .collect();
    gw.seed(objects);

    let l = launch(&config_yaml(&gw), test_options());
    let report = l
        .run
        .wait_exit(Duration::from_secs(180))
        .expect("bounded job exits on its own")
        .expect("no start error");
    assert_eq!(report.state, ExitState::Completed);

    let mut expected: Vec<String> = Vec::new();
    for i in 0..total {
        expected.extend(recs(&format!("o{i}"), 2));
    }
    assert_eq!(sorted(captured_rows(&l.script)), sorted(expected));
}
