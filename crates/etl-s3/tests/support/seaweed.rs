//! A SeaweedFS S3 gateway in a container — the real-S3-API fixture shared
//! by the Docker-gated suites. (SeaweedFS, not MinIO: MinIO is no longer
//! open source.)

use etl_test::wait_until;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: &str = "chrislusf/seaweedfs";
const TAG: &str = "3.97";
const S3_PORT: u16 = 8333;

pub(crate) struct Gateway {
    _container: Container<GenericImage>,
    pub(crate) port: u16,
    pub(crate) bucket: String,
    /// One runtime and client for all seeding/assertion I/O — some
    /// helpers are polled in `wait_until` loops, so per-call construction
    /// would build a runtime per iteration.
    pub(crate) rt: tokio::runtime::Runtime,
    pub(crate) client: Arc<dyn ObjectStore>,
}

impl Gateway {
    /// Start SeaweedFS, wait for the S3 API, and create the test bucket.
    pub(crate) fn start(bucket: &str) -> Gateway {
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

    /// The `store:` passthrough options a source needs to reach this
    /// gateway, as indented YAML map entries.
    pub(crate) fn store_options_yaml(&self) -> String {
        format!(
            "      endpoint: \"http://127.0.0.1:{}\"\n      allow_http: \"true\"\n      skip_signature: \"true\"\n      region: \"us-east-1\"",
            self.port
        )
    }

    pub(crate) fn seed(&self, objects: Vec<(String, Vec<u8>)>) {
        use futures_util::StreamExt as _;
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
