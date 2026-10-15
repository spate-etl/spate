//! The SDK client the feed talks through: its trust store, proxy, region,
//! credentials, timeouts and retries.

use crate::config::SqsFeedConfig;
use crate::error::SqsError;
use crate::queue::SdkQueue;
use aws_config::environment::region::EnvironmentVariableRegionProvider;
use aws_config::imds::region::ImdsRegionProvider;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::region::ProfileFileRegionProvider;
use aws_config::provider_config::ProviderConfig;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::retry::RetryConfig;
use aws_sdk_sqs::config::timeout::TimeoutConfig;
use aws_sdk_sqs::config::{BehaviorVersion, Region, SharedCredentialsProvider, SharedHttpClient};
use aws_smithy_http_client::ConnectorBuilder;
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore};
use rustls_native_certs::CertificateResult;
use spate_core::config::ConfigError;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The read and operation timeouts of every call but `ReceiveMessage`.
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// `ReceiveMessage` waits up to 20 s for a message before its reply starts.
const RECEIVE_READ_TIMEOUT: Duration = Duration::from_secs(25);
const RECEIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

const MAX_ATTEMPTS: u32 = 3;

/// The client over `config`'s queue. `credentials` replaces the AWS provider
/// chain. `step` names the step under way, for a caller's timeout message.
pub(crate) async fn sdk_queue(
    config: &SqsFeedConfig,
    credentials: Option<SharedCredentialsProvider>,
    step: &std::sync::Mutex<&'static str>,
) -> Result<SdkQueue, SqsError> {
    let set = |s| {
        *step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = s
    };
    set("loading the system trust store");
    let http = http_client(rustls_native_certs::load_native_certs).await?;
    set("resolving the AWS region");
    let region = match config.region.clone().or_else(|| config.region_from_url()) {
        Some(region) => Region::new(region),
        None => chain_region(&http).await?,
    };
    let mut loader = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .http_client(http)
        .region(region)
        .timeout_config(timeouts(READ_TIMEOUT, OPERATION_TIMEOUT))
        .retry_config(RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS));
    if let Some(credentials) = credentials {
        loader = loader.credentials_provider(credentials);
    }
    set("loading the AWS configuration");
    let sdk = loader.load().await;
    let mut builder = aws_sdk_sqs::config::Builder::from(&sdk);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    Ok(SdkQueue {
        client: Client::from_conf(builder.build()),
        queue_url: config.queue_url.clone(),
        receive_timeouts: aws_sdk_sqs::config::Builder::default()
            .timeout_config(timeouts(RECEIVE_READ_TIMEOUT, RECEIVE_OPERATION_TIMEOUT)),
    })
}

/// The client's timeouts with `read` and `operation`.
pub(crate) fn timeouts(read: Duration, operation: Duration) -> TimeoutConfig {
    // No attempt timeout: the SDK loads credentials inside an attempt, and a
    // load that timeout cancels starts over on the next one.
    TimeoutConfig::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(read)
        .operation_timeout(operation)
        .build()
}

/// The region the environment, the shared config file or instance metadata
/// names, looked up over `http`.
///
/// The SDK's default region chain builds its own HTTP client, which reads the
/// system trust store on the calling thread.
async fn chain_region(http: &SharedHttpClient) -> Result<Region, SqsError> {
    let conf = ProviderConfig::default().with_http_client(http.clone());
    let chain = RegionProviderChain::first_try(EnvironmentVariableRegionProvider::new())
        .or_else(
            ProfileFileRegionProvider::builder()
                .configure(&conf)
                .build(),
        )
        .or_else(ImdsRegionProvider::builder().configure(&conf).build());
    chain.region().await.ok_or_else(|| {
        SqsError::Config(ConfigError::Component {
            context: "feed.sqs.region".into(),
            message: "no AWS region: set `region`, use a queue URL on \
                      `sqs.<region>.amazonaws.com`, or set AWS_REGION in the environment"
                .into(),
        })
    })
}

/// An HTTPS client that verifies servers against the certificates `system`
/// yields, or against the bundled Mozilla roots when none of them parse. It
/// goes through the proxies the standard environment variables name.
pub(crate) async fn http_client(
    system: impl FnOnce() -> CertificateResult + Send + 'static,
) -> Result<SharedHttpClient, SqsError> {
    let (roots, fallback) = tokio::task::spawn_blocking(move || trust_roots(system()))
        .await
        .map_err(|e| SqsError::Unavailable(format!("loading the system trust store: {e}")))?;
    if fallback {
        tracing::warn!(
            "the system trust store has no certificates; verifying SQS against the Mozilla \
             root bundle"
        );
    }
    let tls = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(roots))
        .build()
        .map_err(|e| SqsError::Refused(format!("building the SQS TLS context: {e}")))?;
    // The connector the SDK's default client builds, with this trust store.
    Ok(
        aws_smithy_http_client::Builder::new().build_with_connector_fn(
            move |settings, components| {
                let mut connector = ConnectorBuilder::default()
                    .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
                    .tls_context(tls.clone())
                    .proxy_config(ProxyConfig::from_env());
                connector.set_connector_settings(settings.cloned());
                if let Some(components) = components {
                    connector.set_sleep_impl(components.sleep_impl());
                }
                connector.build()
            },
        ),
    )
}

/// The parsable certificates in `loaded` as one PEM bundle, or the Mozilla
/// roots when there are none, and whether it fell back.
///
/// The SDK panics on a certificate it cannot parse, so only those rustls
/// accepts are passed on.
pub(crate) fn trust_roots(loaded: CertificateResult) -> (String, bool) {
    if !loaded.errors.is_empty() {
        let errors = loaded
            .errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        tracing::debug!(%errors, "errors reading the system trust store");
    }
    let mut check = rustls::RootCertStore::empty();
    let mut bundle = String::new();
    for cert in loaded.certs {
        if check.add(cert.clone()).is_ok() {
            push_pem(&mut bundle, &cert);
        }
    }
    let fallback = bundle.is_empty();
    if fallback {
        for cert in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
            push_pem(&mut bundle, cert);
        }
    }
    (bundle, fallback)
}

fn push_pem(out: &mut String, der: &[u8]) {
    use base64::Engine as _;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    out.push_str("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the fallback to the bundled roots when the system store yields
    /// nothing parsable, so an empty store does not panic the SDK.
    #[test]
    fn an_empty_system_store_falls_back_to_the_bundled_roots() {
        let (bundle, fallback) = trust_roots(CertificateResult::default());
        assert!(fallback);
        assert!(bundle.starts_with("-----BEGIN CERTIFICATE-----\n"));
    }
}
