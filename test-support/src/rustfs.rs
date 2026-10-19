//! A RustFS S3 store in a container, with a fixed access key pair, the
//! signed call that creates a bucket on it, and a variant that publishes
//! the bucket's storage notifications to a Kafka broker.

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use ::http::Method;
use object_store::aws::{AmazonS3Builder, AwsAuthorizer, AwsCredential};
use object_store::client::HttpRequest;
use object_store::path::Path as StorePath;
use object_store::signer::Signer as _;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::{container_image, http, http_with_headers};

/// The container port of the S3 API.
pub const S3_PORT: u16 = 9000;

/// The access key id the server accepts.
pub const ACCESS_KEY: &str = "spate-test";

/// The secret access key the server accepts.
pub const SECRET_KEY: &str = "spate-test-secret";

/// The region clients sign for, the server's default.
pub const REGION: &str = "us-east-1";

const READY: Duration = Duration::from_secs(60);

/// The ARN of the Kafka notification target [`Rustfs::start_notifying`]
/// configures, as RustFS names its `primary` Kafka target.
const KAFKA_TARGET: &str = "arn:rustfs:sqs::primary:kafka";

/// The broker address RustFS reaches from inside the broker's network
/// namespace: the listener the broker advertises to other containers.
const BROKER_FROM_INSIDE: &str = "localhost:9093";

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
        ready(api)?;
        Ok(Rustfs {
            container,
            api,
            name,
        })
    }

    /// Starts the pinned image inside the network namespace of the broker
    /// container `owner_id`, publishing each storage notification of a
    /// bucket configured by [`Rustfs::put_bucket_notification`] to `topic`.
    ///
    /// The container publishes no port of its own: the broker container
    /// must map the S3 port, and `s3_host_port` is the host port it maps it
    /// to. RustFS reaches the broker on the listener it advertises to other
    /// containers, and keeps undelivered notifications in a queue directory.
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or is not ready within 60
    /// seconds.
    ///
    /// # Panics
    ///
    /// Panics as [`container_image`] does.
    pub fn start_notifying(
        owner_id: &str,
        s3_host_port: u16,
        topic: &str,
    ) -> Result<Rustfs, String> {
        let (image, tag) = container_image(&["--pull", "rustfs"]);
        let mode = format!("container:{owner_id}");
        // `with_network` would ask Docker to create a network of this name,
        // which it refuses, so the namespace is set on the host config.
        let request = GenericImage::new(image, tag)
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_env_var("RUSTFS_NOTIFY_ENABLE", "true")
            .with_env_var("RUSTFS_NOTIFY_KAFKA_ENABLE_PRIMARY", "on")
            .with_env_var("RUSTFS_NOTIFY_KAFKA_BROKERS_PRIMARY", BROKER_FROM_INSIDE)
            .with_env_var("RUSTFS_NOTIFY_KAFKA_TOPIC_PRIMARY", topic)
            .with_env_var(
                "RUSTFS_NOTIFY_KAFKA_QUEUE_DIR_PRIMARY",
                "/tmp/rustfs-notify",
            )
            .with_host_config_modifier(move |host| {
                host.network_mode = Some(mode.clone());
                host.publish_all_ports = Some(false);
                host.port_bindings = None;
            });
        let container = request.start().map_err(|e| format!("start RustFS: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], s3_host_port));
        ready(api)?;
        Ok(Rustfs {
            container,
            api,
            name: None,
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

    /// Sends every `s3:ObjectCreated:*` notification of `bucket` to the
    /// Kafka target a container from [`Rustfs::start_notifying`] has.
    ///
    /// # Errors
    ///
    /// Fails when the request cannot be signed or sent, or the server answers
    /// anything but `200`.
    pub fn put_bucket_notification(&self, bucket: &str) -> Result<(), String> {
        let config = format!(
            "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <QueueConfiguration><Id>spate-test</Id><Queue>{KAFKA_TARGET}</Queue>\
             <Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>"
        );
        let path = format!("/{bucket}?notification");
        let mut request: HttpRequest = ::http::Request::builder()
            .method(Method::PUT)
            .uri(format!("http://{}{path}", self.api))
            .body(config.clone().into())
            .map_err(|e| format!("build PUT {path}: {e}"))?;
        let credential = AwsCredential {
            key_id: ACCESS_KEY.to_owned(),
            secret_key: SECRET_KEY.to_owned(),
            token: None,
        };
        AwsAuthorizer::new(&credential, "s3", REGION)
            .try_authorize(&mut request, None)
            .map_err(|e| format!("sign PUT {path}: {e}"))?;
        let headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())))
            .collect();
        match http_with_headers(self.api, "PUT", &path, &headers, &config) {
            Ok((200, _)) => Ok(()),
            Ok((status, reply)) => Err(format!("RustFS PUT {path}: {status} {reply}")),
            Err(e) => Err(format!("RustFS PUT {path}: {e}")),
        }
    }
}

/// Waits until the server at `api` reports itself ready. It answers 503
/// while its storage and IAM start, before any S3 call succeeds.
fn ready(api: SocketAddr) -> Result<(), String> {
    let until = Instant::now() + READY;
    loop {
        match http(api, "GET", "/health/ready") {
            Ok((200, _)) => return Ok(()),
            failure if Instant::now() >= until => {
                return Err(format!(
                    "RustFS was not ready within {READY:?}: {failure:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}
