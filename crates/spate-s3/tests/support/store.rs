//! The S3 store `SPATE_S3_LANE` selects, in a container: the real-S3-API
//! fixture shared by the Docker-gated suites, with one bucket and a signed
//! client for seeding.

use object_store::aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer, AwsCredential};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use spate_test_support::{ACCESS_KEY, REGION, S3Store, SECRET_KEY, Store};
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
        format!(
            "      endpoint: \"{}\"\n      allow_http: \"true\"\n      access_key_id: \"{ACCESS_KEY}\"\n      secret_access_key: \"{SECRET_KEY}\"\n      region: \"{REGION}\"",
            self.store.endpoint()
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

    /// The bucket's client as its concrete type, for its paginated listing.
    pub(crate) fn s3(&self) -> AmazonS3 {
        builder(&self.store, &self.bucket)
            .build()
            .expect("client builds")
    }

    /// PUT `body` at the raw `key`, which `object_store`'s `Path` cannot
    /// hold, such as a folder marker ending in `/`. `key` is sent as written,
    /// so it must need no percent-encoding.
    pub(crate) fn put_raw(&self, key: &str, body: &str) {
        let endpoint = self.store.endpoint();
        let path = format!("/{}/{key}", self.bucket);
        let mut request = object_store::client::HttpRequest::new(body.to_owned().into());
        *request.method_mut() = "PUT".parse().unwrap();
        *request.uri_mut() = format!("{endpoint}{path}").parse().unwrap();
        let credential = AwsCredential {
            key_id: ACCESS_KEY.to_owned(),
            secret_key: SECRET_KEY.to_owned(),
            token: None,
        };
        AwsAuthorizer::new(&credential, "s3", REGION)
            .try_authorize(&mut request, None)
            .expect("sign the PUT");
        let headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        // The signature covers `Host`, which the exchange sets to the address.
        let addr = endpoint
            .strip_prefix("http://")
            .and_then(|a| a.parse().ok())
            .expect("the endpoint is http://<address>");
        let (status, reply) =
            spate_test_support::http_with_headers(addr, "PUT", &path, &headers, body)
                .expect("send the PUT");
        assert_eq!(status, 200, "PUT {key}: {reply}");
    }
}

/// A signed `object_store` client for seeding and assertions.
fn build_client(store: &S3Store, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(builder(store, bucket).build().expect("client builds"))
}

fn builder(store: &S3Store, bucket: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_endpoint(store.endpoint())
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
}
