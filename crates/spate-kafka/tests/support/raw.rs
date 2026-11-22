//! The builder for a client a test makes itself, outside every role: a seeding
//! producer, a reading consumer, an admin client, the mock cluster's own client.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::{FromClientConfig, FromClientConfigAndContext};
use rdkafka::error::KafkaResult;

/// A client configuration a test builds itself, outside every role.
#[expect(
    clippy::disallowed_methods,
    reason = "a test client is configured outside every role"
)]
pub(crate) fn raw_config() -> ClientConfig {
    ClientConfig::new()
}

/// Builds a client from a [`raw_config`].
pub(crate) trait Raw {
    fn build<T: FromClientConfig>(&self) -> KafkaResult<T>;

    fn build_with<C, T>(&self, context: C) -> KafkaResult<T>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<C>;
}

impl Raw for ClientConfig {
    #[expect(
        clippy::disallowed_methods,
        reason = "a test client is built outside every role"
    )]
    fn build<T: FromClientConfig>(&self) -> KafkaResult<T> {
        leave_openssl_running();
        self.create()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a test client is built outside every role"
    )]
    fn build_with<C, T>(&self, context: C) -> KafkaResult<T>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<C>,
    {
        leave_openssl_running();
        self.create_with_context(context)
    }
}

/// Leaves OpenSSL's exit handler unregistered, as the crate's own client
/// constructor does. librdkafka seeds its random numbers through OpenSSL in the
/// first client a process builds, and that call registers the handler unless
/// OpenSSL was initialised without it first.
fn leave_openssl_running() {
    #[cfg(feature = "tls")]
    openssl_sys::init();
}
