//! A RustFS S3 store in a container, the real-S3-API fixture shared by the
//! Docker-gated suites, with one bucket and a signed client for seeding.

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use spate_test_support::{ACCESS_KEY, REGION, Rustfs, SECRET_KEY};
use std::sync::Arc;

pub(crate) struct Gateway {
    rustfs: Rustfs,
    pub(crate) bucket: String,
    /// One runtime and client for all seeding/assertion I/O. Some helpers
    /// are polled in `wait_until` loops, so per-call construction would
    /// build a runtime per iteration.
    pub(crate) rt: tokio::runtime::Runtime,
    pub(crate) client: Arc<dyn ObjectStore>,
}

impl Gateway {
    /// Start RustFS and create the test bucket.
    pub(crate) fn start(bucket: &str) -> Gateway {
        let rustfs = Rustfs::start(None)
            .expect("start RustFS (is Docker running? first run pulls the image)");
        rustfs.create_bucket(bucket).expect("create the bucket");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let client = build_client(&rustfs, bucket);
        Gateway {
            rustfs,
            bucket: bucket.to_string(),
            rt,
            client,
        }
    }

    /// The `store:` passthrough options a source needs to reach this
    /// store, as indented YAML map entries.
    pub(crate) fn store_options_yaml(&self) -> String {
        format!(
            "      endpoint: \"{}\"\n      allow_http: \"true\"\n      access_key_id: \"{ACCESS_KEY}\"\n      secret_access_key: \"{SECRET_KEY}\"\n      region: \"{REGION}\"",
            self.rustfs.endpoint()
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

/// A signed `object_store` client for seeding and assertions.
fn build_client(rustfs: &Rustfs, bucket: &str) -> Arc<dyn ObjectStore> {
    let store = AmazonS3Builder::new()
        .with_endpoint(rustfs.endpoint())
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .expect("client builds");
    Arc::new(store)
}
