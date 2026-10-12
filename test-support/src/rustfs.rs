//! A RustFS S3 store in a container, with a fixed access key pair, and the
//! signed call that creates a bucket on it.

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use ::http::Method;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::signer::Signer as _;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::{container_image, http};

/// The container port of the S3 API.
pub const S3_PORT: u16 = 9000;

/// The access key id the server accepts.
pub const ACCESS_KEY: &str = "spate-test";

/// The secret access key the server accepts.
pub const SECRET_KEY: &str = "spate-test-secret";

/// The region clients sign for, the server's default.
pub const REGION: &str = "us-east-1";

const READY: Duration = Duration::from_secs(60);

/// A running RustFS container. Dropping it removes the container.
pub struct Rustfs {
    container: Container<GenericImage>,
    api: SocketAddr,
    name: Option<String>,
}

impl fmt::Debug for Rustfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rustfs")
            .field("container", &self.container.id())
            .field("api", &self.api)
            .field("name", &self.name)
            .finish()
    }
}

impl Rustfs {
    /// Starts the image `ci/rustfs/` pins, pulled by digest, and waits until
    /// the server reports itself ready. With `network`, the container joins
    /// it under [`Rustfs::container_name`].
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or is not ready within 60
    /// seconds.
    ///
    /// # Panics
    ///
    /// Panics as [`container_image`] does.
    pub fn start(network: Option<&str>) -> Result<Rustfs, String> {
        let (image, tag) = container_image(&["--pull", "rustfs"]);
        let request = GenericImage::new(image, tag)
            .with_exposed_port(S3_PORT.tcp())
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY);
        let name = network.map(|n| format!("{n}-rustfs"));
        let request = match (network, &name) {
            (Some(network), Some(name)) => request.with_network(network).with_container_name(name),
            _ => request,
        };
        let container = request.start().map_err(|e| format!("start RustFS: {e}"))?;
        let port = container
            .get_host_port_ipv4(S3_PORT)
            .map_err(|e| format!("RustFS port: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], port));
        // The server answers 503 while its storage and IAM start, before any
        // S3 call succeeds.
        let until = Instant::now() + READY;
        loop {
            match http(api, "GET", "/health/ready") {
                Ok((200, _)) => break,
                failure if Instant::now() >= until => {
                    return Err(format!(
                        "RustFS was not ready within {READY:?}: {failure:?}"
                    ));
                }
                _ => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        Ok(Rustfs {
            container,
            api,
            name,
        })
    }

    /// The endpoint URL a client on the host reaches the S3 API at.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.api)
    }

    /// The container's name on the network it joined, when it joined one.
    #[must_use]
    pub fn container_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Whether Docker reports the container running.
    ///
    /// # Errors
    ///
    /// Fails when Docker cannot be asked.
    pub fn is_running(&self) -> Result<bool, String> {
        self.container.is_running().map_err(|e| e.to_string())
    }

    /// Creates `bucket` with a signed `PUT`. The server refuses unsigned
    /// requests.
    ///
    /// # Errors
    ///
    /// Fails when the request cannot be signed or sent, or the server answers
    /// anything but `200`.
    ///
    /// # Panics
    ///
    /// Panics when called from async code on a tokio runtime. Call it through
    /// `tokio::task::spawn_blocking` there.
    pub fn create_bucket(&self, bucket: &str) -> Result<(), String> {
        let store = AmazonS3Builder::new()
            .with_endpoint(self.endpoint())
            .with_allow_http(true)
            .with_bucket_name(bucket)
            .with_region(REGION)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .build()
            .map_err(|e| format!("S3 client for {bucket}: {e}"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        // The empty path presigns the bucket's own URL. Signing with static
        // keys does no I/O.
        let url = runtime
            .block_on(store.signed_url(Method::PUT, &StorePath::default(), READY))
            .map_err(|e| format!("sign PUT {bucket}: {e}"))?;
        let target = format!("{}?{}", url.path(), url.query().unwrap_or_default());
        match http(self.api, "PUT", &target) {
            Ok((200, _)) => Ok(()),
            Ok((status, reply)) => Err(format!("RustFS PUT {bucket}: {status} {reply}")),
            Err(e) => Err(format!("RustFS PUT {bucket}: {e}")),
        }
    }
}
