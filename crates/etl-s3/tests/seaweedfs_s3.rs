//! Backfill tests against a real S3 API: a SeaweedFS container.
//!
//! SeaweedFS (not MinIO, which is no longer open source) runs `weed
//! server -s3`, exposing an S3-compatible gateway on port 8333. These
//! tests cover what the local-filesystem suite cannot: real ListObjectsV2
//! pagination across pages (>1000 keys), HTTP endpoint/option passthrough
//! through the `store` map, and kill-and-resume against a real gateway.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p etl-s3 --test seaweedfs_s3 -- --ignored
//! ```

mod support;

use etl_core::pipeline::ExitState;
use etl_test::wait_until;
use futures_util::StreamExt as _;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use support::{captured_rows, launch, lines_bytes, recs, sorted, test_options};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: &str = "chrislusf/seaweedfs";
const TAG: &str = "3.97";
const S3_PORT: u16 = 8333;

struct Gateway {
    _container: Container<GenericImage>,
    port: u16,
    bucket: String,
    /// One runtime and client for all seeding/assertion I/O — some
    /// helpers are polled in `wait_until` loops, so per-call construction
    /// would build a runtime per iteration.
    rt: tokio::runtime::Runtime,
    client: Arc<dyn ObjectStore>,
}

impl Gateway {
    /// Start SeaweedFS, wait for the S3 API, and create the test bucket.
    fn start(bucket: &str) -> Gateway {
        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(S3_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Starting S3 API Server"))
            .with_cmd(["server", "-s3"])
            .start()
            .expect("start SeaweedFS (is Docker running? first run pulls the image)");
        let port = container
            .get_host_port_ipv4(S3_PORT)
            .expect("mapped S3 port");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let client = build_client(bucket, port);
        let gw = Gateway {
            _container: container,
            port,
            bucket: bucket.to_string(),
            rt,
            client,
        };
        // The gateway logs readiness slightly before the filer finishes
        // registering; retry bucket creation until it sticks.
        wait_until(Duration::from_secs(60), "bucket created", || {
            gw.http_put(&format!("/{bucket}")) == 200
        });
        gw
    }

    /// Minimal anonymous HTTP PUT (SeaweedFS accepts unsigned requests
    /// when no identities are configured); returns the status code.
    fn http_put(&self, path: &str) -> u16 {
        let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.port)) else {
            return 0;
        };
        let req = format!(
            "PUT {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        if stream.write_all(req.as_bytes()).is_err() {
            return 0;
        }
        let mut buf = String::new();
        if stream.read_to_string(&mut buf).is_err() {
            return 0;
        }
        buf.split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn store_options_yaml(&self) -> String {
        format!(
            "      endpoint: \"http://127.0.0.1:{}\"\n      allow_http: \"true\"\n      skip_signature: \"true\"\n      region: \"us-east-1\"",
            self.port
        )
    }

    fn config_yaml(&self, lanes: u32) -> String {
        format!(
            r#"
pipeline: {{ name: s3-seaweedfs-test, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
source:
  s3:
    url: "s3://{bucket}/data/"
    lanes: {lanes}
    checkpoint:
      url: "s3://{bucket}/_etl/manifest.json"
    store:
{opts}
sink: {{ capture: {{}} }}
"#,
            bucket = self.bucket,
            opts = self.store_options_yaml(),
        )
    }

    fn seed(&self, objects: Vec<(String, Vec<u8>)>) {
        let store = &self.client;
        self.rt.block_on(async {
            futures_util::stream::iter(objects.into_iter().map(|(key, body)| {
                let store = Arc::clone(store);
                async move {
                    store
                        .put(&StorePath::from(key.as_str()), PutPayload::from(body))
                        .await
                        .expect("seed object");
                }
            }))
            .buffer_unordered(32)
            .collect::<Vec<()>>()
            .await;
        });
    }

    fn manifest_exists(&self) -> bool {
        self.rt.block_on(async {
            self.client
                .get(&StorePath::from("_etl/manifest.json"))
                .await
                .is_ok()
        })
    }
}

/// An `object_store` client for seeding and assertions.
fn build_client(bucket: &str, port: u16) -> Arc<dyn ObjectStore> {
    let url = url::Url::parse(&format!("s3://{bucket}/")).unwrap();
    let opts = [
        ("endpoint", format!("http://127.0.0.1:{port}")),
        ("allow_http", "true".into()),
        ("skip_signature", "true".into()),
        ("region", "us-east-1".into()),
    ];
    let (store, _) = object_store::parse_url_opts(&url, opts).expect("client builds");
    Arc::from(store)
}

/// More than 1000 keys forces ListObjectsV2 pagination; ordering must
/// hold across page boundaries for the composite offsets to be stable.
#[test]
#[ignore = "requires Docker"]
fn paginated_listing_backfills_completely() {
    let gw = Gateway::start("etl-s3-pagination");
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

    let l = launch(&gw.config_yaml(4), test_options());
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
    assert!(gw.manifest_exists(), "manifest persisted to the bucket");
}

/// Kill mid-backfill, restart, and verify nothing is lost against a real
/// gateway: the resume object is re-opened conditioned on its real ETag
/// and the committed record count is discarded. (Ranged GETs are the
/// *mid-stream retry* path and are covered by the FlakyStore unit tests —
/// no mid-stream failure is injected here.)
#[test]
#[ignore = "requires Docker"]
fn kill_and_resume_loses_nothing() {
    let gw = Gateway::start("etl-s3-resume");
    let objects: Vec<(String, Vec<u8>)> = (0..6)
        .map(|i| {
            (
                format!("data/part-{i}.ndjson"),
                lines_bytes(&recs(&format!("p{i}"), 500)),
            )
        })
        .collect();
    gw.seed(objects);
    let mut expected: Vec<String> = Vec::new();
    for i in 0..6 {
        expected.extend(recs(&format!("p{i}"), 500));
    }

    let l1 = launch(&gw.config_yaml(2), test_options());
    wait_until(Duration::from_secs(60), "first manifest commit", || {
        gw.manifest_exists()
    });
    l1.shutdown.trigger();
    let r1 = l1.run.join().expect("run 1 exits");
    assert_eq!(r1.state, ExitState::Completed);
    let rows1 = captured_rows(&l1.script);

    let l2 = launch(&gw.config_yaml(2), test_options());
    let r2 = l2
        .run
        .wait_exit(Duration::from_secs(120))
        .expect("run 2 completes on its own")
        .expect("no start error");
    assert_eq!(r2.state, ExitState::Completed);

    let mut union: Vec<String> = rows1
        .iter()
        .chain(captured_rows(&l2.script).iter())
        .cloned()
        .collect();
    union.sort();
    union.dedup();
    assert_eq!(union, sorted(expected), "no record lost across the restart");
}

/// Listing drift against a real gateway: deleting a committed key must
/// fail the resume, not replay or skip silently.
#[test]
#[ignore = "requires Docker"]
fn deleting_a_committed_key_fails_the_resume() {
    let gw = Gateway::start("etl-s3-drift");
    gw.seed(
        ["a", "b", "c"]
            .iter()
            .map(|k| (format!("data/{k}.ndjson"), lines_bytes(&recs(k, 3))))
            .collect(),
    );

    let l1 = launch(&gw.config_yaml(1), test_options());
    let r1 = l1.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);

    gw.rt.block_on(async {
        gw.client
            .delete(&StorePath::from("data/b.ndjson"))
            .await
            .expect("delete key");
        // Confirm the listing shrank before relaunching.
        let n = gw.client.list(Some(&StorePath::from("data"))).count().await;
        assert_eq!(n, 2);
    });

    let l2 = launch(&gw.config_yaml(1), test_options());
    let r2 = l2.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    let ExitState::Failed(failure) = r2.state else {
        panic!("drifted listing must fail, got {:?}", r2.state);
    };
    assert!(
        failure.reason.contains("frozen") || failure.reason.contains("listing changed"),
        "actionable drift error: {}",
        failure.reason
    );
}
