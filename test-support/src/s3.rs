//! An S3 store in a container, chosen by `SPATE_S3_LANE`: a fixed access key
//! pair, the signed call that creates a bucket, and a variant that publishes
//! a bucket's storage notifications to a Kafka broker.
//!
//! Each lane under `ci/s3/` is a store, and each store has an arm in
//! [`Store`] holding what its startup needs that the S3 API does not carry.

mod garage;
mod rgw;
mod rustfs;

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

use crate::{container_image, http, http_with_headers, selected_lane};

/// The access key id every store accepts.
pub const ACCESS_KEY: &str = "spate-test";

/// The secret access key every store accepts.
pub const SECRET_KEY: &str = "spate-test-secret";

/// The region clients sign for, which every store is configured to accept.
pub const REGION: &str = "us-east-1";

/// The `OpaqueData` of the topic a notifying Ceph RGW publishes through,
/// which RGW copies into each event.
pub const RGW_OPAQUE_DATA: &str = rgw::OPAQUE_DATA;

const READY: Duration = Duration::from_secs(60);

/// The broker address a store reaches from inside the broker's network
/// namespace: the listener the broker advertises to other containers.
const BROKER_FROM_INSIDE: &str = "localhost:9093";

/// The service whose lanes are stores.
const SERVICE: &str = "s3";

/// Why a Garage store refuses the notification calls: Garage implements no
/// bucket notification configuration.
const GARAGE_SENDS_NO_NOTIFICATIONS: &str = "Garage sends no bucket notifications";

/// A store an S3 lane runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    /// RustFS.
    Rustfs,
    /// Ceph RGW.
    Rgw,
    /// Garage.
    Garage,
}

impl Store {
    /// The store a lane directory under `ci/s3/` names.
    ///
    /// # Errors
    ///
    /// Fails, naming the lane, when no store has that lane.
    pub fn from_lane(lane: &str) -> Result<Store, String> {
        match lane {
            "rustfs" => Ok(Store::Rustfs),
            "rgw" => Ok(Store::Rgw),
            "garage" => Ok(Store::Garage),
            other => Err(format!(
                "no store runs S3 lane `{other}`; each ci/s3/ lane needs its arm in `Store`"
            )),
        }
    }

    /// The store the selected lane runs: the one `SPATE_S3_LANE` names, or the
    /// primary lane.
    ///
    /// # Errors
    ///
    /// Fails as [`Store::from_lane`] does.
    ///
    /// # Panics
    ///
    /// Panics as [`selected_lane`] does.
    pub fn selected() -> Result<Store, String> {
        Store::from_lane(&selected_lane(SERVICE))
    }

    /// The lane directory this store runs under `ci/s3/`.
    #[must_use]
    pub fn lane(self) -> &'static str {
        match self {
            Store::Rustfs => "rustfs",
            Store::Rgw => "rgw",
            Store::Garage => "garage",
        }
    }

    /// The container port of the store's S3 API. A broker hosting the store
    /// in its network namespace maps this port before the store starts.
    #[must_use]
    pub fn container_port(self) -> u16 {
        match self {
            Store::Rustfs => rustfs::PORT,
            Store::Rgw => rgw::PORT,
            Store::Garage => garage::PORT,
        }
    }

    /// The container port whose [`Store::ready_path`] reports readiness.
    fn ready_port(self) -> u16 {
        match self {
            Store::Rustfs | Store::Rgw => self.container_port(),
            Store::Garage => garage::ADMIN_PORT,
        }
    }

    fn ready_path(self) -> &'static str {
        match self {
            Store::Rustfs => rustfs::READY_PATH,
            Store::Rgw => rgw::READY_PATH,
            Store::Garage => garage::READY_PATH,
        }
    }
}

impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Store::Rustfs => "RustFS",
            Store::Rgw => "Ceph RGW",
            Store::Garage => "Garage",
        })
    }
}

/// A running S3 store container. Dropping it removes the container.
pub struct S3Store {
    container: Container<GenericImage>,
    store: Store,
    api: SocketAddr,
    name: Option<String>,
    /// The topic a notifying RGW publishes through, once created.
    topic_arn: Option<String>,
}

impl fmt::Debug for S3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Store")
            .field("store", &self.store)
            .field("container", &self.container.id())
            .field("api", &self.api)
            .field("name", &self.name)
            .field("topic_arn", &self.topic_arn)
            .finish()
    }
}

impl S3Store {
    /// Starts the store the selected lane pins, pulled by digest, and waits
    /// until it reports itself ready. With `network`, the container joins it
    /// under [`S3Store::container_name`].
    ///
    /// # Errors
    ///
    /// Fails when the selected lane is pinned but names no store, or the
    /// container does not start or is not ready within 60 seconds.
    ///
    /// # Panics
    ///
    /// Panics when `SPATE_S3_LANE` names a lane with no pinned `Dockerfile`,
    /// as [`Store::selected`] does, and as [`container_image`] does.
    pub fn start(network: Option<&str>) -> Result<S3Store, String> {
        let store = Store::selected()?;
        let mut image = pinned(store).with_exposed_port(store.container_port().tcp());
        if store.ready_port() != store.container_port() {
            image = image.with_exposed_port(store.ready_port().tcp());
        }
        let request = match store {
            Store::Rustfs => rustfs::configure(image),
            Store::Rgw => rgw::configure(image),
            Store::Garage => garage::configure(image),
        };
        let name = network.map(|n| format!("{n}-{}", store.lane()));
        let request = match (network, &name) {
            (Some(network), Some(name)) => request.with_network(network).with_container_name(name),
            _ => request,
        };
        let container = request.start().map_err(|e| format!("start {store}: {e}"))?;
        let port = container
            .get_host_port_ipv4(store.container_port())
            .map_err(|e| format!("{store} port: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], port));
        let ready_port = container
            .get_host_port_ipv4(store.ready_port())
            .map_err(|e| format!("{store} readiness port: {e}"))?;
        ready(store, SocketAddr::from(([127, 0, 0, 1], ready_port)))?;
        Ok(S3Store {
            container,
            store,
            api,
            name,
            topic_arn: None,
        })
    }

    /// Starts the selected store inside the network namespace of the broker
    /// container `owner_id`, publishing each storage notification of a
    /// bucket configured by [`S3Store::put_bucket_notification`] to `topic`.
    ///
    /// The container publishes no port of its own: the broker container
    /// must map [`Store::container_port`], and `s3_host_port` is the host port
    /// it maps it to. The store reaches the broker on the listener it
    /// advertises to other containers.
    ///
    /// # Errors
    ///
    /// Fails when the selected lane is pinned but names no store, the store
    /// sends no notifications, the container does not start or is not ready
    /// within 60 seconds, or a store that takes its Kafka target through its
    /// topic API refuses the topic.
    ///
    /// # Panics
    ///
    /// Panics when `SPATE_S3_LANE` names a lane with no pinned `Dockerfile`,
    /// as [`Store::selected`] does, and as [`container_image`] does.
    pub fn start_notifying(
        owner_id: &str,
        s3_host_port: u16,
        topic: &str,
    ) -> Result<S3Store, String> {
        let store = Store::selected()?;
        let image = pinned(store);
        let mode = format!("container:{owner_id}");
        // `with_network` would ask Docker to create a network of this name,
        // which it refuses, so the namespace is set on the host config.
        let request = match store {
            Store::Rustfs => rustfs::notifying(image, topic),
            Store::Rgw => rgw::notifying(image),
            Store::Garage => return Err(GARAGE_SENDS_NO_NOTIFICATIONS.to_owned()),
        }
        .with_host_config_modifier(move |host| {
            host.network_mode = Some(mode.clone());
            host.publish_all_ports = Some(false);
            host.port_bindings = None;
        });
        let container = request.start().map_err(|e| format!("start {store}: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], s3_host_port));
        ready(store, api)?;
        let mut started = S3Store {
            container,
            store,
            api,
            name: None,
            topic_arn: None,
        };
        match store {
            Store::Rustfs | Store::Garage => {}
            // RGW finds a new persistent topic's queue only on its next
            // 30-second scan, so the topic is created before the caller
            // writes anything.
            Store::Rgw => {
                let body = rgw::create_topic_body(topic, rgw::OPAQUE_DATA);
                let reply = started.signed(
                    Method::POST,
                    "/",
                    &body,
                    "application/x-www-form-urlencoded",
                )?;
                started.topic_arn = Some(rgw::topic_arn(&reply)?);
            }
        }
        Ok(started)
    }

    /// The store this container runs.
    #[must_use]
    pub fn store(&self) -> Store {
        self.store
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

    /// Creates `bucket` with a signed `PUT`. Stores refuse unsigned writes.
    ///
    /// # Errors
    ///
    /// Fails when the request cannot be signed or sent, or the store answers
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
        let named = self.store;
        match http(self.api, "PUT", &target) {
            Ok((200, _)) => Ok(()),
            Ok((status, reply)) => Err(format!("{named} PUT {bucket}: {status} {reply}")),
            Err(e) => Err(format!("{named} PUT {bucket}: {e}")),
        }
    }

    /// Sends every `s3:ObjectCreated:*` notification of `bucket` to the
    /// Kafka target or topic a container from [`S3Store::start_notifying`]
    /// has.
    ///
    /// # Errors
    ///
    /// Fails when the store sends no notifications, the request cannot be
    /// signed or sent, the store answers anything but `200`, or the store
    /// takes its Kafka target through a topic and was started by
    /// [`S3Store::start`], which creates none.
    pub fn put_bucket_notification(&self, bucket: &str) -> Result<(), String> {
        let config = match self.store {
            Store::Rustfs => rustfs::notification_configuration(),
            Store::Rgw => {
                let Some(arn) = &self.topic_arn else {
                    return Err(format!(
                        "{} has no topic for {bucket}: start it with `start_notifying`",
                        self.store
                    ));
                };
                rgw::notification_configuration(arn)
            }
            Store::Garage => return Err(GARAGE_SENDS_NO_NOTIFICATIONS.to_owned()),
        };
        self.signed(
            Method::PUT,
            &format!("/{bucket}?notification"),
            &config,
            "application/xml",
        )
        .map(drop)
    }

    /// Sends `body` to `path` with `method`, signed with the fixed key pair,
    /// and returns the reply.
    ///
    /// # Errors
    ///
    /// Fails when the request cannot be signed or sent, or the store answers
    /// anything but `200`.
    fn signed(
        &self,
        method: Method,
        path: &str,
        body: &str,
        content_type: &str,
    ) -> Result<String, String> {
        let what = format!("{method} {path}");
        let mut request: HttpRequest = ::http::Request::builder()
            .method(method.clone())
            .uri(format!("http://{}{path}", self.api))
            .header(::http::header::CONTENT_TYPE, content_type)
            .body(body.to_owned().into())
            .map_err(|e| format!("build {what}: {e}"))?;
        let credential = AwsCredential {
            key_id: ACCESS_KEY.to_owned(),
            secret_key: SECRET_KEY.to_owned(),
            token: None,
        };
        AwsAuthorizer::new(&credential, "s3", REGION)
            .try_authorize(&mut request, None)
            .map_err(|e| format!("sign {what}: {e}"))?;
        let headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())))
            .collect();
        let named = self.store;
        match http_with_headers(self.api, method.as_str(), path, &headers, body) {
            Ok((200, reply)) => Ok(reply),
            Ok((status, reply)) => Err(format!("{named} {what}: {status} {reply}")),
            Err(e) => Err(format!("{named} {what}: {e}")),
        }
    }
}

/// The image `store`'s lane pins, pulled by digest.
fn pinned(store: Store) -> GenericImage {
    let (image, tag) = container_image(&["--pull", SERVICE, store.lane()]);
    GenericImage::new(image, tag)
}

/// Waits until the store at `api` answers its readiness path with `200`.
fn ready(store: Store, api: SocketAddr) -> Result<(), String> {
    let until = Instant::now() + READY;
    loop {
        match http(api, "GET", store.ready_path()) {
            Ok((200, _)) => return Ok(()),
            failure if Instant::now() >= until => {
                return Err(format!(
                    "{store} was not ready within {READY:?}: {failure:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_named_s3_lane_selects_its_store() {
        assert_eq!(Store::from_lane("rustfs"), Ok(Store::Rustfs));
        assert_eq!(Store::Rustfs.lane(), "rustfs");
        assert_eq!(Store::from_lane("rgw"), Ok(Store::Rgw));
        assert_eq!(Store::Rgw.lane(), "rgw");
        assert_eq!(Store::from_lane("garage"), Ok(Store::Garage));
        assert_eq!(Store::Garage.lane(), "garage");
    }

    /// The ports, readiness path and name Garage serves and reports under.
    /// Readiness is on the admin API, and the other stores report it on
    /// their S3 port.
    #[test]
    fn garage_is_reached_on_its_own_ports_and_readiness_path() {
        assert_eq!(Store::Garage.container_port(), 3900);
        assert_eq!(Store::Garage.ready_port(), 3903);
        assert_eq!(Store::Garage.ready_path(), "/health");
        assert_eq!(Store::Garage.to_string(), "Garage");
        for store in [Store::Rustfs, Store::Rgw] {
            assert_eq!(store.ready_port(), store.container_port(), "{store}");
        }
    }

    /// The config runs one node that accepts requests signed for the
    /// suites' region and binds both APIs on the ports the arm maps.
    #[test]
    fn the_garage_config_is_the_one_the_lane_documents() {
        assert_eq!(
            garage::config(),
            format!(
                "metadata_dir = \"/var/lib/garage/meta\"\n\
                 data_dir = \"/var/lib/garage/data\"\n\
                 replication_factor = 1\n\
                 consistency_mode = \"consistent\"\n\
                 rpc_bind_addr = \"0.0.0.0:3901\"\n\
                 rpc_secret = \"{}\"\n\
                 \n\
                 [s3_api]\n\
                 s3_region = \"us-east-1\"\n\
                 api_bind_addr = \"0.0.0.0:3900\"\n\
                 \n\
                 [admin]\n\
                 api_bind_addr = \"0.0.0.0:3903\"\n",
                "0".repeat(64)
            )
        );
    }

    /// The port, readiness path and name Ceph RGW serves and reports under.
    #[test]
    fn rgw_is_reached_on_its_own_port_and_readiness_path() {
        assert_eq!(Store::Rgw.container_port(), 7480);
        assert_eq!(Store::Rgw.ready_path(), "/");
        assert_eq!(Store::Rgw.to_string(), "Ceph RGW");
    }

    /// The notification configuration points every created object at the
    /// topic it is given.
    #[test]
    fn the_rgw_notification_configuration_names_its_topic() {
        assert_eq!(
            rgw::notification_configuration("arn:aws:sns:default::units"),
            "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <TopicConfiguration><Id>spate-test</Id>\
             <Topic>arn:aws:sns:default::units</Topic>\
             <Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>"
        );
    }

    /// The topic produces to the broker's in-namespace listener, persists
    /// each event until the broker acknowledges it, carries the opaque
    /// marker, and leaves RGW's retry limits at their defaults.
    #[test]
    fn the_rgw_topic_pushes_to_the_broker_persistently() {
        assert_eq!(
            rgw::create_topic_body("units", RGW_OPAQUE_DATA),
            "Action=CreateTopic&Name=units\
             &Attributes.entry.1.key=push-endpoint\
             &Attributes.entry.1.value=kafka%3A%2F%2Flocalhost%3A9093\
             &Attributes.entry.2.key=persistent&Attributes.entry.2.value=true\
             &Attributes.entry.3.key=kafka-ack-level&Attributes.entry.3.value=broker\
             &Attributes.entry.4.key=OpaqueData&Attributes.entry.4.value=spate-test-opaque"
        );
    }

    /// The topic's ARN comes from the `CreateTopic` reply, and a reply that
    /// names none fails carrying the reply.
    #[test]
    fn the_topic_arn_is_read_from_the_create_topic_reply() {
        let reply = "<CreateTopicResponse><CreateTopicResult>\
                     <TopicArn>arn:aws:sns:default::units</TopicArn>\
                     </CreateTopicResult></CreateTopicResponse>";
        assert_eq!(
            rgw::topic_arn(reply),
            Ok("arn:aws:sns:default::units".to_owned())
        );
        for refused in [
            "<Error><Code>InvalidArgument</Code></Error>",
            "<TopicArn></TopicArn>",
        ] {
            let err = rgw::topic_arn(refused).unwrap_err();
            assert!(err.contains(refused), "{err}");
        }
    }

    /// The port, readiness path and name RustFS serves and reports under.
    #[test]
    fn rustfs_is_reached_on_its_own_port_and_readiness_path() {
        assert_eq!(Store::Rustfs.container_port(), 9000);
        assert_eq!(Store::Rustfs.ready_path(), "/health/ready");
        assert_eq!(Store::Rustfs.to_string(), "RustFS");
    }

    /// The notification configuration points every created object at the
    /// Kafka target the notifying container sets up.
    #[test]
    fn the_rustfs_notification_configuration_names_its_kafka_target() {
        assert_eq!(
            rustfs::notification_configuration(),
            "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <QueueConfiguration><Id>spate-test</Id>\
             <Queue>arn:rustfs:sqs::primary:kafka</Queue>\
             <Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>"
        );
    }

    #[test]
    fn an_unknown_s3_lane_is_rejected_naming_it() {
        let err = Store::from_lane("nope").unwrap_err();
        assert!(err.contains("`nope`"), "{err}");
    }

    /// A lane added under `ci/s3/` without its startup code fails here,
    /// without Docker. Only directories are listed; the task runner stays the
    /// one parser of the pins.
    #[test]
    fn every_s3_lane_names_a_known_store() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../ci/s3");
        let lanes: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(!lanes.is_empty(), "no lanes under {}", dir.display());
        for lane in lanes {
            let store = Store::from_lane(&lane).unwrap();
            assert_eq!(store.lane(), lane);
        }
    }
}
