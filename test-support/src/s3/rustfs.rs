//! RustFS: the credential and notification variables it reads, its readiness
//! path, and the Kafka target its notification configuration names.

use testcontainers::{ContainerRequest, GenericImage, ImageExt};

use super::{ACCESS_KEY, BROKER_FROM_INSIDE, SECRET_KEY};

/// The container port of the S3 API.
pub(super) const PORT: u16 = 9000;

/// The path that answers `200` once the server is ready. It answers 503 while
/// its storage and IAM start, before any S3 call succeeds.
pub(super) const READY_PATH: &str = "/health/ready";

/// The ARN of the Kafka notification target [`notifying`] configures, as
/// RustFS names its `primary` Kafka target.
const KAFKA_TARGET: &str = "arn:rustfs:sqs::primary:kafka";

/// The image with the fixed key pair set.
pub(super) fn configure(image: GenericImage) -> ContainerRequest<GenericImage> {
    image
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
}

/// The image with the fixed key pair set and a Kafka target publishing to
/// `topic`. RustFS keeps undelivered notifications in a queue directory.
pub(super) fn notifying(image: GenericImage, topic: &str) -> ContainerRequest<GenericImage> {
    configure(image)
        .with_env_var("RUSTFS_NOTIFY_ENABLE", "true")
        .with_env_var("RUSTFS_NOTIFY_KAFKA_ENABLE_PRIMARY", "on")
        .with_env_var("RUSTFS_NOTIFY_KAFKA_BROKERS_PRIMARY", BROKER_FROM_INSIDE)
        .with_env_var("RUSTFS_NOTIFY_KAFKA_TOPIC_PRIMARY", topic)
        .with_env_var(
            "RUSTFS_NOTIFY_KAFKA_QUEUE_DIR_PRIMARY",
            "/tmp/rustfs-notify",
        )
}

/// The `PUT ?notification` body sending every `s3:ObjectCreated:*` event to
/// the target [`notifying`] configures.
pub(super) fn notification_configuration() -> String {
    format!(
        "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <QueueConfiguration><Id>spate-test</Id><Queue>{KAFKA_TARGET}</Queue>\
         <Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>"
    )
}
