//! A SeaweedFS S3 gateway in a container, holding a run's data set.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use spate_test_support::http;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: &str = "chrislusf/seaweedfs";
const TAG: &str = "3.97";
const S3_PORT: u16 = 8333;
/// How long the gateway may take to accept the bucket after it logs ready.
const BUCKET_DEADLINE: Duration = Duration::from_secs(60);

/// A running gateway with one bucket.
pub struct Gateway {
    container: Container<GenericImage>,
    /// The mapped S3 port on the loopback address.
    pub port: u16,
    /// The bucket.
    pub bucket: String,
    client: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("port", &self.port)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// Starts SeaweedFS and creates `bucket`.
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or the bucket cannot be
    /// created within a minute.
    pub fn start(bucket: &str) -> Result<Gateway, String> {
        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(S3_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Starting S3 API Server"))
            .with_cmd(["server", "-s3"])
            .start()
            .map_err(|e| format!("start SeaweedFS: {e}"))?;
        let port = container
            .get_host_port_ipv4(S3_PORT)
            .map_err(|e| format!("SeaweedFS port: {e}"))?;
        // The gateway logs readiness before the filer registers, so bucket
        // creation is retried. It accepts unsigned requests when no
        // identities are configured.
        let gateway = SocketAddr::from(([127, 0, 0, 1], port));
        let path = format!("/{bucket}");
        let until = Instant::now() + BUCKET_DEADLINE;
        while !http(gateway, "PUT", &path).is_ok_and(|(status, _)| status == 200) {
            if Instant::now() >= until {
                return Err(format!("bucket {bucket} not created within a minute"));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let url = url::Url::parse(&format!("s3://{bucket}/")).map_err(|e| e.to_string())?;
        let (client, _) = object_store::parse_url_opts(
            &url,
            [
                ("endpoint", format!("http://127.0.0.1:{port}")),
                ("allow_http", "true".to_owned()),
                ("skip_signature", "true".to_owned()),
                ("region", "us-east-1".to_owned()),
            ],
        )
        .map_err(|e| e.to_string())?;
        Ok(Gateway {
            container,
            port,
            bucket: bucket.to_owned(),
            client: Arc::from(client),
        })
    }

    /// The endpoint URL a worker reaches the gateway at.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Whether Docker reports the container running.
    ///
    /// # Errors
    ///
    /// Fails when Docker cannot be asked.
    pub fn is_running(&self) -> Result<bool, String> {
        self.container.is_running().map_err(|e| e.to_string())
    }

    /// Lists the bucket's first object, failing when no answer comes within
    /// `cap`.
    ///
    /// # Errors
    ///
    /// Fails when the list fails or does not answer in time.
    pub fn probe(&self, rt: &tokio::runtime::Runtime, cap: Duration) -> Result<(), String> {
        let first =
            rt.block_on(async { tokio::time::timeout(cap, self.client.list(None).next()).await });
        match first {
            Ok(Some(Err(e))) => Err(e.to_string()),
            Ok(_) => Ok(()),
            Err(_) => Err(format!("no answer within {cap:?}")),
        }
    }

    /// Writes each `(key, body)` object.
    ///
    /// # Errors
    ///
    /// Fails when an object cannot be written, with the first error to complete.
    pub fn put_all(
        &self,
        rt: &tokio::runtime::Runtime,
        objects: Vec<(String, Vec<u8>)>,
    ) -> Result<(), String> {
        rt.block_on(async {
            let results: Vec<_> =
                futures_util::stream::iter(objects.into_iter().map(|(key, body)| {
                    let client = Arc::clone(&self.client);
                    async move {
                        client
                            .put(&StorePath::from(key.as_str()), PutPayload::from(body))
                            .await
                            .map(drop)
                            .map_err(|e| format!("put {key}: {e}"))
                    }
                }))
                .buffer_unordered(8)
                .collect()
                .await;
            results.into_iter().collect()
        })
    }
}
