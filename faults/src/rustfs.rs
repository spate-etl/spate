//! A RustFS S3 store in a container, holding a run's data set.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use spate_test_support::{ACCESS_KEY, REGION, Rustfs, SECRET_KEY};

/// A running store with one bucket.
pub struct Gateway {
    rustfs: Rustfs,
    /// The bucket.
    pub bucket: String,
    client: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("rustfs", &self.rustfs)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// Starts RustFS and creates `bucket`.
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or become ready within a
    /// minute, or the bucket cannot be created.
    ///
    /// # Panics
    ///
    /// Panics as [`spate_test_support::container_image`] does.
    pub fn start(bucket: &str) -> Result<Gateway, String> {
        let rustfs = Rustfs::start(None)?;
        rustfs.create_bucket(bucket)?;
        let client = AmazonS3Builder::new()
            .with_endpoint(rustfs.endpoint())
            .with_allow_http(true)
            .with_bucket_name(bucket)
            .with_region(REGION)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Gateway {
            rustfs,
            bucket: bucket.to_owned(),
            client: Arc::new(client),
        })
    }

    /// The endpoint URL a worker reaches the store at.
    #[must_use]
    pub fn endpoint(&self) -> String {
        self.rustfs.endpoint()
    }

    /// Whether Docker reports the container running.
    ///
    /// # Errors
    ///
    /// Fails when Docker cannot be asked.
    pub fn is_running(&self) -> Result<bool, String> {
        self.rustfs.is_running()
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
