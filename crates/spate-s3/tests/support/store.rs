//! The S3 store `SPATE_S3_LANE` selects, in a container: the real-S3-API
//! fixture shared by the Docker-gated suites, with one bucket and a signed
//! client for seeding.

use object_store::aws::AmazonS3;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use spate_test_support::{S3Store, Store};
use std::sync::Arc;

pub(crate) struct Gateway {
    store: S3Store,
    pub(crate) bucket: String,
    /// One runtime and client for all seeding/assertion I/O. Some helpers
    /// are polled in `wait_until` loops, so per-call construction would
    /// build a runtime per iteration.
    pub(crate) rt: tokio::runtime::Runtime,
    pub(crate) client: Arc<dyn ObjectStore>,
}

impl Gateway {
    /// Start the selected store and create the test bucket.
    pub(crate) fn start(bucket: &str) -> Gateway {
        let store = S3Store::start(None)
            .expect("start the S3 store (is Docker running? first run pulls the image)");
        store.create_bucket(bucket).expect("create the bucket");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let client = build_client(&store, bucket);
        Gateway {
            store,
            bucket: bucket.to_string(),
            rt,
            client,
        }
    }

    /// The store this gateway runs.
    pub(crate) fn store(&self) -> Store {
        self.store.store()
    }

    /// The `store:` passthrough options a source needs to reach this
    /// store, as indented YAML map entries.
    pub(crate) fn store_options_yaml(&self) -> String {
        self.store.store_options_yaml()
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

    /// The bucket's client as its concrete type, for its paginated listing.
    pub(crate) fn s3(&self) -> AmazonS3 {
        self.store
            .client_builder(&self.bucket)
            .build()
            .expect("client builds")
    }

    /// PUT `body` at the raw `key`, which `object_store`'s `Path` cannot
    /// hold, such as a folder marker ending in `/`.
    pub(crate) fn put_raw(&self, key: &str, body: &str) {
        self.store
            .put_raw(&self.bucket, key, body)
            .unwrap_or_else(|e| panic!("PUT {key}: {e}"));
    }
}

/// A signed `object_store` client for seeding and assertions.
fn build_client(store: &S3Store, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(store.client_builder(bucket).build().expect("client builds"))
}
