//! Backfill tests against a real S3 API: a SeaweedFS container.
//!
//! This suite covers what the local-filesystem suite cannot: real
//! ListObjectsV2 pagination across pages (>1000 keys) feeding the
//! planner, and HTTP endpoint/option passthrough through the `store`
//! map. Durable resume across restarts needs a durable coordination
//! store and lives in the NATS-backed suite (`coordinated_nats`).
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-s3 --test seaweedfs_s3 -- --ignored
//! ```

mod support;

use spate_core::pipeline::ExitState;
use std::time::Duration;
use support::seaweed::Gateway;
use support::{captured_rows, launch, lines_bytes, recs, sorted, test_options};

fn config_yaml(gw: &Gateway) -> String {
    format!(
        r#"
pipeline: {{ name: s3-seaweedfs-test, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "s3://{bucket}/data/"
    split_target_bytes: 1MiB
    store:
{opts}
sink: {{ capture: {{}} }}
"#,
        bucket = gw.bucket,
        opts = gw.store_options_yaml(),
    )
}

/// More than 1000 keys forces ListObjectsV2 pagination; ordering must
/// hold across page boundaries for the planner's packing, and with it
/// every split id and composite offset, to be stable.
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
