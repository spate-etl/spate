//! The SDK client the feed talks through: its trust store, proxy, region,
//! credentials, timeouts and retries.

use crate::config::SqsFeedConfig;
use crate::config::{BACKEND, NAME};
use crate::credentials::SharedLoad;
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
use aws_types::service_config::ServiceConfigKey;
use spate_core::config::SectionPath;
use spate_core::feed::ConnectError;
use spate_core::tls::{SystemRoots, TrustError, TrustRoots, is_https};
use std::path::PathBuf;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The read and operation timeouts of every call but `ReceiveMessage`.
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// `ReceiveMessage` waits up to 20 s for a message before its reply starts.
const RECEIVE_READ_TIMEOUT: Duration = Duration::from_secs(25);
pub(crate) const RECEIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

const MAX_ATTEMPTS: u32 = 3;

/// The client over `config`'s queue. `credentials` replaces the AWS provider
/// chain. `step` names the step under way, for a caller's timeout message.
pub(crate) async fn sdk_queue(
    config: &SqsFeedConfig,
    credentials: Option<SharedCredentialsProvider>,
    step: &std::sync::Mutex<&'static str>,
) -> Result<SdkQueue, ConnectError> {
    sdk_queue_with(config, credentials, step, SystemRoots::load).await
}

/// [`sdk_queue`], trusting what `system` yields as the system trust store.
async fn sdk_queue_with(
    config: &SqsFeedConfig,
    credentials: Option<SharedCredentialsProvider>,
    step: &std::sync::Mutex<&'static str>,
    system: impl FnOnce() -> SystemRoots + Send + 'static,
) -> Result<SdkQueue, ConnectError> {
    let set = |s| {
        *step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = s
    };
    set("loading the trust roots");
    let roots = trust_roots(system, config.tls.root_ca.clone()).await?;
    let http = http_client(&roots)?;
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
    builder.set_credentials_provider(
        sdk.credentials_provider()
            .map(|p| SharedCredentialsProvider::new(SharedLoad::new(p))),
    );
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    let endpoint = endpoint_called(config, &sdk);
    require_roots(roots, endpoint.as_deref().is_none_or(is_https))?;
    Ok(SdkQueue {
        client: Client::from_conf(builder.build()),
        queue_url: config.queue_url.clone(),
        receive_timeouts: aws_sdk_sqs::config::Builder::default()
            .timeout_config(timeouts(RECEIVE_READ_TIMEOUT, RECEIVE_OPERATION_TIMEOUT)),
        root_ca_key: config.uses_https().then_some("tls.root_ca"),
    })
}

/// The client's timeouts with `read` and `operation`.
pub(crate) fn timeouts(read: Duration, operation: Duration) -> TimeoutConfig {
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
async fn chain_region(http: &SharedHttpClient) -> Result<Region, ConnectError> {
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
async fn first_region(chain: &impl ProvideRegion) -> Result<Region, ConnectError> {
    chain.region().await.ok_or_else(|| {
        ConnectError::Config(SectionPath::feed(BACKEND).invalid(
            "region",
            "no AWS region: set `region`, set AWS_REGION in the environment, or set `region` in \
             the shared config file's profile",
        ))
    })
}

/// The usable certificates `system` yields and every certificate in
/// `root_ca`. Reading them blocks, so it runs off the async workers.
async fn trust_roots(
    system: impl FnOnce() -> SystemRoots + Send + 'static,
    root_ca: Option<PathBuf>,
) -> Result<TrustRoots, ConnectError> {
    #[expect(
        clippy::disallowed_methods,
        reason = "awaited here; the trust-store read ends on its own, and a blocking task cannot be aborted"
    )]
    let loaded =
        tokio::task::spawn_blocking(move || TrustRoots::load(system(), root_ca.as_deref()))
            .await
            .map_err(|e| {
                ConnectError::unavailable(NAME, format!("loading the trust roots: {e}"))
            })?;
    loaded.map_err(root_ca_error)
}

/// `Ok` when `roots` holds a certificate or the feed does not call SQS over
/// TLS, and otherwise the error that names the fixes.
fn require_roots(roots: TrustRoots, https: bool) -> Result<(), ConnectError> {
    if https {
        roots.require_any().map_err(root_ca_error)?;
    }
    Ok(())
}

fn root_ca_error(e: TrustError) -> ConnectError {
    SectionPath::feed(BACKEND)
        .invalid("tls.root_ca", e.to_string())
        .into()
}

/// The endpoint the client calls in place of the region's: `endpoint`, then
/// the one `sdk` names, read as the SQS client config reads it:
/// `AWS_ENDPOINT_URL_SQS`, then `AWS_ENDPOINT_URL`, then the shared config
/// file's SQS and shared `endpoint_url`.
fn endpoint_called(config: &SqsFeedConfig, sdk: &aws_config::SdkConfig) -> Option<String> {
    if let Some(endpoint) = &config.endpoint {
        return Some(endpoint.clone());
    }
    let key = ServiceConfigKey::builder()
        .service_id("SQS")
        .env("AWS_ENDPOINT_URL")
        .profile("endpoint_url")
        .build()
        .expect("every field is set");
    sdk.service_config()
        .and_then(|conf| conf.load_config(key))
        .or_else(|| sdk.endpoint_url().map(str::to_owned))
}

/// An HTTPS client that verifies servers against `roots`, and goes through
/// the proxies the standard environment variables name.
///
/// `roots` may be empty: a call over TLS then fails at the handshake. The SDK
/// panics on a certificate it cannot parse, and [`TrustRoots`] holds only
/// those rustls accepts.
pub(crate) fn http_client(roots: &TrustRoots) -> Result<SharedHttpClient, ConnectError> {
    let tls = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(roots.to_pem()))
        .build()
        .map_err(|e| ConnectError::refused(NAME, format!("building the SQS TLS context: {e}")))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_types::service_config::LoadServiceConfig;
    use spate_core::config::ConfigError;
    use spate_core::tls::CertificateDer;
    use spate_test_support::{TestCa, pem};

    /// The config error loading `system` and `root_ca` for an `https://`
    /// feed fails with, as its context and message.
    async fn refusal(
        system: Vec<CertificateDer<'static>>,
        root_ca: Option<PathBuf>,
    ) -> (String, String) {
        let loaded = trust_roots(move || SystemRoots::from_certs(system), root_ca).await;
        match loaded.and_then(|roots| require_roots(roots, true)) {
            Err(ConnectError::Config(ConfigError::Component { context, message })) => {
                (context, message)
            }
            Err(other) => panic!("expected a config error, got {other}"),
            Ok(()) => panic!("expected a config error, got roots"),
        }
    }

    /// An `https://` feed with no usable system root and no `root_ca` fails
    /// at `tls.root_ca`, with the fixes named.
    #[tokio::test]
    async fn an_https_feed_with_no_trust_root_fails_at_root_ca() {
        let unparsable = CertificateDer::from(vec![0u8; 8]);
        for (system, problem) in [
            (Vec::new(), "the system trust store has no certificates"),
            (
                vec![unparsable],
                "none of the 1 certificates in the system trust store parsed",
            ),
        ] {
            assert_eq!(
                refusal(system, None).await,
                (
                    "feed.sqs.tls.root_ca".to_owned(),
                    format!(
                        "{problem}; install a CA bundle, point SSL_CERT_FILE at one, or set \
                         this key to the issuing CA"
                    )
                )
            );
        }
    }

    /// A feed whose endpoint is not `https://` needs no root, so it builds
    /// its client on an image with no CA bundle.
    #[tokio::test]
    async fn a_feed_off_tls_needs_no_root() {
        let roots = trust_roots(|| SystemRoots::from_certs(Vec::new()), None)
            .await
            .unwrap();
        http_client(&roots).expect("an empty trust store builds");
        require_roots(roots, false).expect("no TLS endpoint, no root needed");
    }

    /// The client trusts the usable system certificates, then `root_ca`'s,
    /// and leaves out a system certificate rustls refuses, on which the SDK
    /// would panic.
    #[tokio::test]
    async fn the_client_takes_only_usable_certificates() {
        let (system, extra) = (TestCa::new("system").der(), TestCa::new("extra"));
        let dir = tempfile::tempdir().unwrap();
        let unparsable = CertificateDer::from(vec![0u8; 8]);
        let store = vec![unparsable, system.clone()];
        let roots = trust_roots(
            move || SystemRoots::from_certs(store),
            Some(extra.write(dir.path())),
        )
        .await
        .unwrap();
        assert_eq!(
            roots.to_pem(),
            pem("CERTIFICATE", &system) + &pem("CERTIFICATE", &extra.der())
        );
        http_client(&roots).expect("only usable certificates reach the SDK");
    }

    /// `root_ca` alone satisfies an `https://` feed on an empty system store.
    #[tokio::test]
    async fn root_ca_satisfies_an_https_feed_on_an_empty_store() {
        let ca = TestCa::new("private");
        let dir = tempfile::tempdir().unwrap();
        let roots = trust_roots(
            || SystemRoots::from_certs(Vec::new()),
            Some(ca.write(dir.path())),
        )
        .await
        .unwrap();
        assert_eq!(roots.to_pem(), pem("CERTIFICATE", &ca.der()));
        require_roots(roots, true).expect("root_ca is a root");
    }

    /// A `root_ca` that is missing, holds no certificate or holds a malformed
    /// one fails at `tls.root_ca` with its path, and does not reach the SDK,
    /// which panics on a certificate it cannot parse.
    #[tokio::test]
    async fn an_unusable_root_ca_fails_at_its_key() {
        let system = TestCa::new("system");
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let malformed = dir.path().join("malformed.pem");
        std::fs::write(&malformed, pem("CERTIFICATE", &[0u8; 8])).unwrap();
        for path in [dir.path().join("missing.pem"), empty, malformed] {
            let (context, message) = refusal(vec![system.der()], Some(path.clone())).await;
            assert_eq!(context, "feed.sqs.tls.root_ca");
            assert!(
                message.starts_with(&format!("`{}`: ", path.display())),
                "{message}"
            );
        }
    }

    /// The check for a usable root reads the endpoint the feed calls: an
    /// `http://` `endpoint` connects on an empty store, and the region's
    /// endpoint, which is `https://`, fails at `tls.root_ca`.
    #[tokio::test]
    async fn the_root_check_follows_the_endpoint_called() {
        let connect = |endpoint: Option<&str>| {
            let config = SqsFeedConfig {
                queue_url: "https://sqs.eu-west-2.amazonaws.com/000000000000/jobs".to_owned(),
                visibility: Duration::from_secs(60),
                region: Some("eu-west-2".to_owned()),
                endpoint: endpoint.map(str::to_owned),
                tls: spate_core::tls::TlsSection::default(),
            };
            async move {
                let step = std::sync::Mutex::new("starting");
                let credentials =
                    aws_sdk_sqs::config::Credentials::new("x", "x", None, None, "test");
                sdk_queue_with(
                    &config,
                    Some(SharedCredentialsProvider::new(credentials)),
                    &step,
                    || SystemRoots::from_certs(Vec::new()),
                )
                .await
                .map(|queue| queue.root_ca_key)
            }
        };
        assert_eq!(connect(Some("http://127.0.0.1:1")).await.unwrap(), None);
        let Err(ConnectError::Config(ConfigError::Component { context, .. })) = connect(None).await
        else {
            panic!("expected a config error");
        };
        assert_eq!(context, "feed.sqs.tls.root_ca");
    }

    /// Service config that answers only the SQS endpoint key, with `sqs`.
    #[derive(Debug)]
    struct SqsEndpoint(Option<&'static str>);

    impl LoadServiceConfig for SqsEndpoint {
        fn load_config(&self, key: ServiceConfigKey<'_>) -> Option<String> {
            let sqs = (key.service_id(), key.env(), key.profile())
                == ("SQS", "AWS_ENDPOINT_URL", "endpoint_url");
            self.0.filter(|_| sqs).map(str::to_owned)
        }
    }

    /// Pins the order the endpoint is read in: `endpoint`, then the SDK
    /// configuration's SQS one, then its shared one.
    #[test]
    fn the_endpoint_called_is_read_in_order() {
        let called = |own: Option<&str>, sqs, shared: Option<&str>| {
            let config = SqsFeedConfig {
                queue_url: "https://sqs.eu-west-2.amazonaws.com/000000000000/jobs".to_owned(),
                visibility: Duration::from_secs(60),
                region: None,
                endpoint: own.map(str::to_owned),
                tls: spate_core::tls::TlsSection::default(),
            };
            let mut builder = aws_config::SdkConfig::builder().service_config(SqsEndpoint(sqs));
            builder.set_endpoint_url(shared.map(str::to_owned));
            endpoint_called(&config, &builder.build())
        };
        assert_eq!(called(None, None, None), None);
        assert_eq!(
            called(None, None, Some("http://shared")).as_deref(),
            Some("http://shared")
        );
        assert_eq!(
            called(None, Some("http://sqs"), Some("https://shared")).as_deref(),
            Some("http://sqs")
        );
        assert_eq!(
            called(
                Some("https://own"),
                Some("http://sqs"),
                Some("http://shared")
            )
            .as_deref(),
            Some("https://own")
        );
    }

    /// Pins the missing-region error: its context, and a message naming the
    /// three places an operator sets a region.
    #[tokio::test]
    async fn a_missing_region_names_each_way_to_give_one() {
        let Err(ConnectError::Config(ConfigError::Component { context, message })) =
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
