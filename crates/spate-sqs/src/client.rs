//! The SDK client the feed talks through: its trust store, proxy, region,
//! credentials, timeouts and retries.

use crate::config::SqsFeedConfig;
use crate::error::SqsError;
use crate::queue::SdkQueue;
use aws_config::environment::region::EnvironmentVariableRegionProvider;
use aws_config::imds::region::ImdsRegionProvider;
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
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
    let region = match config.region.clone() {
        Some(region) => Some(region),
        None => config.region_from_url().await,
    };
    let region = match region {
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
    first_region(&chain).await
}

/// The region `chain` names, or the error that lists where to set one.
async fn first_region(chain: &impl ProvideRegion) -> Result<Region, SqsError> {
    chain.region().await.ok_or_else(|| {
        SqsError::Config(ConfigError::Component {
            context: "feed.sqs.region".into(),
            message: "no AWS region: set `region`, set AWS_REGION in the environment, or set \
                      `region` in the shared config file's profile"
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
    if let Some(warning) = fallback {
        tracing::warn!("{warning}");
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
/// roots when there are none, with the warning to log when it fell back.
///
/// The SDK panics on a certificate it cannot parse, so only those rustls
/// accepts are passed on.
pub(crate) fn trust_roots(loaded: CertificateResult) -> (String, Option<String>) {
    let errors = loaded
        .errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    let found = loaded.certs.len();
    let mut check = rustls::RootCertStore::empty();
    let mut bundle = String::new();
    let mut parsed = 0;
    for cert in loaded.certs {
        if check.add(cert.clone()).is_ok() {
            push_pem(&mut bundle, &cert);
            parsed += 1;
        }
    }
    if parsed > 0 {
        let refused = found - parsed;
        if refused > 0 || !errors.is_empty() {
            tracing::debug!(refused, %errors, "part of the system trust store did not load");
        }
        return (bundle, None);
    }
    for cert in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        push_pem(&mut bundle, cert);
    }
    let mut warning = if found == 0 {
        "the system trust store has no certificates".to_owned()
    } else {
        format!("none of the {found} certificates in the system trust store parsed")
    };
    warning.push_str("; verifying SQS against the Mozilla root bundle");
    if !errors.is_empty() {
        warning.push_str(&format!(" (errors reading the store: {errors})"));
    }
    (bundle, Some(warning))
}

fn push_pem(out: &mut String, der: &[u8]) {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    let body = STANDARD.encode(der);
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
    use rustls::pki_types::CertificateDer;
    use spate_test_support::{TestCa, native_certs, pem};

    /// Pins the fallback to the bundled roots when the system store yields
    /// nothing parsable, so an empty store does not panic the SDK.
    #[test]
    fn an_empty_system_store_falls_back_to_the_bundled_roots() {
        let (bundle, warning) = trust_roots(CertificateResult::default());
        assert_eq!(
            warning.as_deref(),
            Some(
                "the system trust store has no certificates; verifying SQS against the Mozilla \
                 root bundle"
            )
        );
        assert!(bundle.starts_with("-----BEGIN CERTIFICATE-----\n"));
    }

    /// Pins the warning for a store whose certificates all fail to parse:
    /// it counts them, where an empty store says it has none.
    #[test]
    fn a_store_whose_certificates_do_not_parse_says_so() {
        let unparsable = CertificateDer::from(vec![0u8; 8]);
        let (bundle, warning) = trust_roots(native_certs(vec![unparsable]));
        assert_eq!(
            warning.as_deref(),
            Some(
                "none of the 1 certificates in the system trust store parsed; verifying SQS \
                 against the Mozilla root bundle"
            )
        );
        assert!(bundle.starts_with("-----BEGIN CERTIFICATE-----\n"));
    }

    /// Pins that a store with a parsable certificate is used with no warning.
    #[test]
    fn a_parsable_store_is_used_without_a_warning() {
        let ca = TestCa::new("private");
        let (bundle, warning) = trust_roots(native_certs(vec![ca.der()]));
        assert_eq!(warning, None);
        assert_eq!(bundle, pem("CERTIFICATE", &ca.der()));
    }

    /// A read error as `rustls-native-certs` reports one for an unreadable
    /// bundle file.
    fn read_error(path: &str) -> rustls_native_certs::Error {
        rustls_native_certs::Error {
            context: "failed to read PEM from file",
            kind: rustls_native_certs::ErrorKind::Io {
                inner: std::io::Error::other("permission denied"),
                path: path.into(),
            },
        }
    }

    /// Pins the debug line beside a parsable store: none when the store
    /// loaded whole, and one counting refused certificates and carrying the
    /// read errors otherwise.
    #[test]
    fn a_partly_loaded_store_logs_at_debug() {
        let ca = TestCa::new("private");
        let unparsable = || CertificateDer::from(vec![0u8; 8]);
        let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
            trust_roots(native_certs(vec![ca.der()]));
        });
        assert_eq!(lines, Vec::<String>::new());

        let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
            trust_roots(native_certs(vec![ca.der(), unparsable()]));
        });
        assert_eq!(
            lines,
            [
                "DEBUG spate_sqs::client: part of the system trust store did not load refused=1 \
              errors="
            ]
        );

        let mut loaded = native_certs(vec![ca.der()]);
        loaded.errors.push(read_error("/etc/ssl/a.pem"));
        let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
            trust_roots(loaded);
        });
        assert_eq!(
            lines,
            [
                "DEBUG spate_sqs::client: part of the system trust store did not load refused=0 \
              errors=failed to read PEM from file: permission denied at '/etc/ssl/a.pem'"
            ]
        );
    }

    /// Pins that the fallback warning carries every read error the store
    /// lookup reported, joined by `; `.
    #[test]
    fn the_fallback_warning_carries_the_read_errors() {
        let mut loaded = native_certs(vec![CertificateDer::from(vec![0u8; 8])]);
        loaded.errors.push(read_error("/etc/ssl/a.pem"));
        loaded.errors.push(read_error("/etc/ssl/b.pem"));
        let (_, warning) = trust_roots(loaded);
        assert_eq!(
            warning.as_deref(),
            Some(
                "none of the 1 certificates in the system trust store parsed; verifying SQS \
                 against the Mozilla root bundle (errors reading the store: failed to read PEM \
                 from file: permission denied at '/etc/ssl/a.pem'; failed to read PEM from \
                 file: permission denied at '/etc/ssl/b.pem')"
            )
        );
    }

    /// Pins that building the client logs the fallback warning at warn.
    #[test]
    fn the_fallback_warning_is_logged() {
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(http_client(CertificateResult::default))
                .unwrap();
        });
        assert_eq!(
            lines,
            [
                " WARN spate_sqs::client: the system trust store has no certificates; \
              verifying SQS against the Mozilla root bundle"
            ]
        );
    }

    /// Pins the missing-region error: its context, and a message naming the
    /// three places an operator sets a region.
    #[tokio::test]
    async fn a_missing_region_names_each_way_to_give_one() {
        let Err(SqsError::Config(ConfigError::Component { context, message })) =
            first_region(&None::<Region>).await
        else {
            panic!("expected a config error");
        };
        assert_eq!(context, "feed.sqs.region");
        assert_eq!(
            message,
            "no AWS region: set `region`, set AWS_REGION in the environment, or set `region` in \
             the shared config file's profile"
        );
        let region = first_region(&Some(Region::new("eu-west-2"))).await.unwrap();
        assert_eq!(region.as_ref(), "eu-west-2");
    }
}
