//! The `sqs:` body of a `feed:` section, decoded and checked.

use aws_sdk_sqs::config::endpoint::{DefaultResolver, Params, ResolveEndpoint as _};
use serde::Deserialize;
use spate_core::config::{
    ConfigError, DurationBounds, FeedSection, SectionPath, UrlRule, check_duration, redact,
};
use spate_core::tls::{TlsSection, is_https};
use std::fmt;
use std::time::Duration;
use url::Url;

/// The key that names this backend in a `feed:` section.
pub(crate) const COMPONENT_TYPE: &str = "sqs";

/// The backend's name in the text a person reads.
pub(crate) const NAME: &str = "SQS";

/// The component a startup error names, as in "the SQS feed cannot start".
pub(crate) const COMPONENT: &str = "SQS feed";

/// The shortest visibility period: one second per renewal period, and a
/// long-poll wait of at least one second.
const MIN_VISIBILITY: Duration = Duration::from_secs(3);

/// The longest visibility period SQS accepts.
const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 60 * 60);

/// `visibility`, which SQS takes in whole seconds.
const VISIBILITY: DurationBounds = DurationBounds::at_least(MIN_VISIBILITY)
    .at_most(MAX_VISIBILITY)
    .whole_seconds();

/// The shortest settling window: AWS documents a queue's approximate counts
/// as taking at least a minute to settle after its producers stop.
const MIN_SETTLING_WINDOW: Duration = Duration::from_secs(60);

fn default_visibility() -> Duration {
    Duration::from_secs(60)
}

/// The rule `queue_url` and `endpoint` meet. The SDK signs each request with
/// credentials from its provider chain, so a URL's userinfo is never sent.
const URL: UrlRule<'static> =
    UrlRule::new("credentials come from the AWS SDK's default provider chain")
        .schemes(&["http", "https"]);

/// `queue_url` parsed as a URL.
///
/// # Errors
///
/// [`ConfigError::Component`] at `feed.sqs.queue_url` when it is not an
/// `http` or `https` URL, or carries userinfo.
pub(crate) fn queue_url(queue_url: &str) -> Result<Url, ConfigError> {
    URL.check(queue_url)
        .map_err(|e| SectionPath::feed(COMPONENT_TYPE).invalid("queue_url", e.to_string()))
}

/// The keys under `feed: { sqs: … }`: the configuration
/// [`SqsFeed::connect`](crate::SqsFeed::connect) builds the feed from.
///
/// Build one in code with [`new`](Self::new), or from a pipeline's `feed:`
/// section with [`from_section`](Self::from_section).
///
/// ```
/// use spate_sqs::SqsFeedConfig;
/// use std::time::Duration;
///
/// let mut config =
///     SqsFeedConfig::new("https://sqs.eu-west-1.amazonaws.com/123456789012/units");
/// config.visibility = Duration::from_secs(120);
/// config.validate()?;
/// # Ok::<(), spate_core::config::ConfigError>(())
/// ```
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SqsFeedConfig {
    /// The queue's URL, as `GetQueueUrl` returns it. A standard queue: the
    /// feed refuses a FIFO queue.
    pub queue_url: String,
    /// How long a received unit stays hidden from other readers without a
    /// renewal, in whole seconds from 3 s to 12 h. Defaults to 60 s.
    #[serde(default = "default_visibility", with = "humantime_serde")]
    pub visibility: Duration,
    /// The AWS region. Unset: the queue URL's host, then the AWS region
    /// chain.
    #[serde(default)]
    pub region: Option<String>,
    /// An endpoint in place of the region's, such as a local emulator.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Trust settings: `tls.root_ca`, which requires an `https://` endpoint.
    #[serde(default)]
    pub tls: TlsSection,
}

// A hand-written `Debug`, so the URLs print through `redact::url`. The
// destructure lists every field so a new one cannot reach `Debug` unplaced.
impl fmt::Debug for SqsFeedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let SqsFeedConfig {
            queue_url,
            visibility,
            region,
            endpoint,
            tls,
        } = self;
        f.debug_struct("SqsFeedConfig")
            .field("queue_url", &redact::url(queue_url))
            .field("visibility", visibility)
            .field("region", region)
            .field("endpoint", &endpoint.as_deref().map(redact::url))
            .field("tls", tls)
            .finish()
    }
}

/// Refuse the keys beside `sqs:` that the feed cannot honor: an
/// `attempt_cap`, and a `settling_window` under a minute.
///
/// # Errors
///
/// [`ConfigError::Component`] at `feed.sqs` naming the key.
pub(crate) fn check_section_keys(section: &FeedSection) -> Result<(), ConfigError> {
    // Both keys sit beside `sqs:`, so the refusal names the backend that
    // refuses them.
    let at = SectionPath::feed(COMPONENT_TYPE);
    if section.attempt_cap.is_some() {
        return Err(at.error(
            "`attempt_cap` does not apply to `sqs`: SQS counts deliveries through the \
             queue's `maxReceiveCount`",
        ));
    }
    if let Some(window) = section.settling_window {
        check_duration(window, DurationBounds::at_least(MIN_SETTLING_WINDOW)).map_err(|e| {
            at.error(format!(
                "`settling_window` {e}: `sqs` message counts take at least a minute to settle"
            ))
        })?;
    }
    Ok(())
}

impl SqsFeedConfig {
    /// A config for the queue at `queue_url`. Every other field starts at its
    /// YAML default.
    pub fn new(queue_url: impl Into<String>) -> SqsFeedConfig {
        SqsFeedConfig {
            queue_url: queue_url.into(),
            visibility: default_visibility(),
            region: None,
            endpoint: None,
            tls: TlsSection::default(),
        }
    }

    /// Decode and check the `sqs:` body of `section`, and refuse the keys
    /// beside it that the feed cannot honor.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] at `feed` for a section that names another
    /// backend. [`ConfigError::Component`], starting at `feed.sqs`, for an
    /// unknown key, an `attempt_cap`, a `settling_window` under a minute, and
    /// a body [`validate`](Self::validate) refuses.
    pub fn from_section(section: &FeedSection) -> Result<SqsFeedConfig, ConfigError> {
        let config: SqsFeedConfig = section.backend_body(COMPONENT_TYPE, NAME)?;
        check_section_keys(section)?;
        config.validate()?;
        Ok(config)
    }

    /// Check the config's values against each other and against what SQS
    /// accepts.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`], starting at `feed.sqs`, for a
    /// `visibility` outside 3 s to 12 h or not in whole seconds, a
    /// `queue_url` or `endpoint` that is not an `http` or `https` URL or
    /// carries userinfo, a `queue_url` that names a FIFO queue, and a
    /// `tls.root_ca` with an `endpoint` that is not `https://`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let at = SectionPath::feed(COMPONENT_TYPE);
        check_duration(self.visibility, VISIBILITY)
            .map_err(|e| at.invalid("visibility", e.to_string()))?;
        let url = queue_url(&self.queue_url)?;
        if url.path().trim_end_matches('/').ends_with(".fifo") {
            return Err(at.invalid(
                "queue_url",
                "names a FIFO queue; the SQS feed reads standard queues only",
            ));
        }
        if let Some(endpoint) = &self.endpoint {
            URL.check(endpoint)
                .map_err(|e| at.invalid("endpoint", e.to_string()))?;
        }
        if self.tls.root_ca.is_some() && !self.uses_https() {
            return Err(at.invalid("tls.root_ca", "is set, but `endpoint` is not https://"));
        }
        Ok(())
    }

    /// Whether `tls.root_ca` can apply: the region's endpoint is `https://`,
    /// and an `endpoint` in its place sets its own scheme.
    pub(crate) fn uses_https(&self) -> bool {
        self.endpoint.as_deref().is_none_or(is_https)
    }

    /// How often held units are renewed: a third of the visibility period.
    pub(crate) fn renewal_period(&self) -> Duration {
        self.visibility / 3
    }

    /// The region the queue URL's host names: the region whose SQS endpoint,
    /// as the SDK resolves it with or without FIPS and dual-stack, is that
    /// host.
    pub(crate) async fn region_from_url(&self) -> Option<String> {
        let url = Url::parse(&self.queue_url).ok()?;
        let host = url.host_str()?;
        let region = host.split('.').nth(1)?;
        let resolver = DefaultResolver::new();
        for (fips, dual_stack) in [(false, false), (true, false), (false, true), (true, true)] {
            let params = Params::builder()
                .region(region)
                .use_fips(fips)
                .use_dual_stack(dual_stack)
                .build()
                .ok()?;
            let Ok(endpoint) = resolver.resolve_endpoint(&params).await else {
                continue;
            };
            let resolved = Url::parse(endpoint.url()).ok();
            if resolved.as_ref().and_then(Url::host_str) == Some(host) {
                return Some(region.to_owned());
            }
        }
        None
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::num::NonZeroU32;

    fn section(yaml: &str) -> FeedSection {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn parse(yaml: &str) -> Result<SqsFeedConfig, ConfigError> {
        SqsFeedConfig::from_section(&section(yaml))
    }

    /// Pins the defaults and every key read.
    #[tokio::test]
    async fn the_body_is_read_with_its_defaults() {
        let config =
            parse("sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/jobs\" }").unwrap();
        assert_eq!(config.visibility, Duration::from_secs(60));
        assert_eq!(config.renewal_period(), Duration::from_secs(20));
        assert_eq!(config.region, None);
        assert_eq!(config.region_from_url().await.as_deref(), Some("eu-west-2"));

        let config = parse(
            "sqs: { queue_url: \"http://127.0.0.1:9324/000000000000/q\", visibility: 6s, \
             region: us-east-1, endpoint: \"http://127.0.0.1:9324\" }",
        )
        .unwrap();
        assert_eq!(config.visibility, Duration::from_secs(6));
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.region_from_url().await, None);
    }

    /// One region in each partition of the SDK's partition table.
    pub(crate) const PARTITION_REGIONS: [&str; 8] = [
        "eu-west-2",
        "cn-north-1",
        "eusc-de-east-1",
        "us-iso-east-1",
        "us-isob-east-1",
        "eu-isoe-west-1",
        "us-isof-south-1",
        "us-gov-west-1",
    ];

    /// The SQS host the SDK resolves for `region` with `fips` and
    /// `dual_stack`.
    pub(crate) async fn sdk_host(region: &str, fips: bool, dual_stack: bool) -> String {
        let params = Params::builder()
            .region(region)
            .use_fips(fips)
            .use_dual_stack(dual_stack)
            .build()
            .unwrap();
        let endpoint = DefaultResolver::new()
            .resolve_endpoint(&params)
            .await
            .unwrap();
        Url::parse(endpoint.url())
            .unwrap()
            .host_str()
            .unwrap()
            .to_owned()
    }

    async fn region_of(host: &str) -> Option<String> {
        let config = parse(&format!("sqs: {{ queue_url: \"https://{host}/1/q\" }}")).unwrap();
        config.region_from_url().await
    }

    /// Pins that a queue URL's host gives its region exactly when the SDK
    /// resolves that region to the host: every FIPS and dual-stack form in
    /// one region per partition the SDK lists, and none of the near misses.
    #[tokio::test]
    async fn the_region_is_read_from_every_sqs_host_form() {
        for region in PARTITION_REGIONS {
            for (fips, dual_stack) in [(false, false), (true, false), (false, true), (true, true)] {
                let host = sdk_host(region, fips, dual_stack).await;
                assert_eq!(region_of(&host).await.as_deref(), Some(region), "{host}");
            }
        }
        assert_eq!(
            region_of("SQS.EU-WEST-2.AMAZONAWS.COM").await.as_deref(),
            Some("eu-west-2")
        );
        assert_eq!(
            region_of("sqs.eu-west-2.amazonaws.com:8443")
                .await
                .as_deref(),
            Some("eu-west-2")
        );
        for host in [
            "sqs.eu-west-2.example.com",
            "sns.eu-west-2.amazonaws.com",
            "vpce-0abc-xyz.sqs.eu-west-2.vpce.amazonaws.com",
            "sqs.eu-west-2.amazonaws.com.example.com",
            "sqs.eu-west-2.notamazonaws.com",
            "sqs.eu-west-2.amazonaws.com.",
            "eu-west-2.queue.amazonaws.com",
            "sqs.eu_west_2.amazonaws.com",
            "sqs..amazonaws.com",
        ] {
            assert_eq!(region_of(host).await, None, "{host}");
        }
    }

    /// Pins that a region the SDK's partition table does not list is read
    /// when its partition's region pattern matches and the SDK builds that
    /// host for it.
    #[tokio::test]
    async fn an_unlisted_region_in_a_listed_partition_is_read() {
        assert_eq!(
            region_of("sqs.eu-west-9.amazonaws.com").await.as_deref(),
            Some("eu-west-9")
        );
    }

    /// Pins that a host whose region label belongs to another partition than
    /// its suffix gives no region, so calls never go to a host the queue URL
    /// does not name.
    #[tokio::test]
    async fn a_host_mixing_two_partitions_gives_no_region() {
        for region in PARTITION_REGIONS {
            for other in PARTITION_REGIONS {
                let host = sdk_host(region, false, false)
                    .await
                    .replacen(region, other, 1);
                // Two partitions can share a suffix, and the swap then gives
                // the other region's own host.
                let expected = (host == sdk_host(other, false, false).await).then_some(other);
                assert_eq!(region_of(&host).await.as_deref(), expected, "{host}");
            }
        }
    }

    /// Pins that a partition table the SDK loads from
    /// `SMITHY_CLIENT_SDK_CUSTOM_PARTITION` is read, in a child process since
    /// the SDK reads it once per process. Its partition has no FIPS, so the
    /// dual-stack host is read only past a FIPS form the SDK refuses.
    #[test]
    fn a_partition_table_the_sdk_loads_is_read() {
        const NAME: &str = "config::tests::a_partition_table_the_sdk_loads_is_read";
        const VAR: &str = "SMITHY_CLIENT_SDK_CUSTOM_PARTITION";
        if std::env::var_os(VAR).is_some() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            for host in [
                "sqs.test-east-1.example.test",
                "sqs.test-east-1.api.example.test",
            ] {
                let region = rt.block_on(region_of(host));
                assert_eq!(region.as_deref(), Some("test-east-1"), "{host}");
            }
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partitions.json");
        std::fs::write(
            &path,
            r#"{"version":"1.1","partitions":[{"id":"test","regionRegex":"^test\\-\\w+\\-\\d+$",
                "regions":{"test-east-1":{}},"outputs":{"name":"test","dnsSuffix":"example.test",
                "dualStackDnsSuffix":"api.example.test","implicitGlobalRegion":"test-east-1",
                "supportsFIPS":false,"supportsDualStack":true}}]}"#,
        )
        .unwrap();
        spate_test_support::run_in_child(NAME, |child| child.env(VAR, &path));
    }

    /// Pins the visibility range at both ends and the whole-second rule.
    #[test]
    fn visibility_is_whole_seconds_from_3s_to_12h() {
        let with = |v: &str| {
            parse(&format!(
                "sqs: {{ queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", visibility: {v} }}"
            ))
        };
        assert!(with("3s").is_ok());
        assert!(with("12h").is_ok());
        for (bad, found) in [("2s", "2s"), ("43201s", "12h 1s"), ("2500ms", "2s 500ms")] {
            assert_eq!(
                with(bad).unwrap_err().to_string(),
                format!(
                    "feed.sqs.visibility: must be from 3s to 12h in whole seconds; found {found}"
                ),
                "{bad}"
            );
        }
    }

    /// Pins the SQS floor on `settling_window`: `60s` loads, `59s` is
    /// refused naming the key, and a section that sets none loads.
    #[test]
    fn an_sqs_window_below_a_minute_is_refused() {
        let body = "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\" }";
        let mut s = section(body);
        s.settling_window = Some(Duration::from_secs(60));
        SqsFeedConfig::from_section(&s).expect("a minute loads");
        s.settling_window = Some(Duration::from_secs(59));
        let err = SqsFeedConfig::from_section(&s).unwrap_err().to_string();
        assert!(
            err.contains("`settling_window`") && err.contains("59s"),
            "{err}"
        );
        s.settling_window = None;
        SqsFeedConfig::from_section(&s).expect("no window loads");
    }

    fn refusal(result: Result<SqsFeedConfig, ConfigError>) -> (String, String) {
        match result {
            Err(e @ ConfigError::Component { .. }) => {
                let ConfigError::Component { context, .. } = &e else {
                    unreachable!()
                };
                (context.clone(), e.to_string())
            }
            other => panic!("expected a component error, got {other:?}"),
        }
    }

    /// Pins each refusal in the `sqs:` body by its full text, starting at
    /// the key it refuses.
    #[test]
    fn each_rule_names_the_key_it_refuses() {
        let at = |key: &str| format!("feed.sqs.{key}");
        let cases = [
            (
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", visibility: 2s }",
                at("visibility"),
                "feed.sqs.visibility: must be from 3s to 12h in whole seconds; found 2s",
            ),
            (
                "sqs: { queue_url: \"not a url\" }",
                at("queue_url"),
                "feed.sqs.queue_url: is not a URL: relative URL without a base",
            ),
            (
                "sqs: { queue_url: \"ftp://x/1/q\" }",
                at("queue_url"),
                "feed.sqs.queue_url: must be a URL with scheme http or https; found ftp",
            ),
            (
                "sqs: { queue_url: \"https://u:hunter2@sqs.eu-west-2.amazonaws.com/1/q\" }",
                at("queue_url"),
                "feed.sqs.queue_url: carries userinfo; credentials come from the AWS SDK's \
                 default provider chain",
            ),
            (
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q.fifo\" }",
                at("queue_url"),
                "feed.sqs.queue_url: names a FIFO queue; the SQS feed reads standard queues only",
            ),
            (
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", endpoint: \"not a url\" }",
                at("endpoint"),
                "feed.sqs.endpoint: is not a URL: relative URL without a base",
            ),
            (
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", endpoint: \"ftp://localhost:9324\" }",
                at("endpoint"),
                "feed.sqs.endpoint: must be a URL with scheme http or https; found ftp",
            ),
            (
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", endpoint: \"https://u:hunter2@localhost:9324\" }",
                at("endpoint"),
                "feed.sqs.endpoint: carries userinfo; credentials come from the AWS SDK's \
                 default provider chain",
            ),
            (
                "sqs: { queue_url: \"http://127.0.0.1:9324/000000000000/q\", endpoint: \"http://127.0.0.1:9324\", tls: { root_ca: /etc/ca.pem } }",
                at("tls.root_ca"),
                "feed.sqs.tls.root_ca: is set, but `endpoint` is not https://",
            ),
        ];
        for (yaml, context, text) in cases {
            assert_eq!(refusal(parse(yaml)), (context, text.to_owned()), "{yaml}");
        }
    }

    /// `tls.root_ca` loads with the region's endpoint and with an `https://`
    /// one, which are the endpoints the feed calls over TLS.
    #[test]
    fn root_ca_loads_with_an_https_endpoint() {
        let queue = "queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\"";
        for (endpoint, https) in [
            ("", true),
            (", endpoint: \"https://sqs.internal:9324\"", true),
            (", endpoint: \"http://127.0.0.1:9324\"", false),
        ] {
            let plain = parse(&format!("sqs: {{ {queue}{endpoint} }}")).unwrap();
            assert_eq!(plain.uses_https(), https, "{endpoint}");
            assert_eq!(plain.tls.root_ca, None);
            if https {
                let config = parse(&format!(
                    "sqs: {{ {queue}{endpoint}, tls: {{ root_ca: /etc/ca.pem }} }}"
                ))
                .unwrap();
                assert_eq!(
                    config.tls.root_ca.as_deref(),
                    Some(std::path::Path::new("/etc/ca.pem"))
                );
            }
        }
    }

    /// Pins that a config built in code starts at the defaults the YAML
    /// path gives a section that holds only `queue_url`.
    #[test]
    fn new_matches_a_section_holding_only_the_queue_url() {
        let url = "https://sqs.eu-west-2.amazonaws.com/1/jobs";
        let from_section = parse(&format!("sqs: {{ queue_url: \"{url}\" }}")).unwrap();
        assert_eq!(SqsFeedConfig::new(url), from_section);
    }

    /// Pins that `validate` refuses a config built in code with the texts
    /// `from_section` gives the same values.
    #[test]
    fn validate_refuses_a_config_built_in_code_as_from_section_does() {
        let fifo = SqsFeedConfig::new("https://sqs.eu-west-2.amazonaws.com/1/q.fifo");
        let mut short = SqsFeedConfig::new("https://sqs.eu-west-2.amazonaws.com/1/q");
        short.visibility = Duration::from_secs(2);
        let mut plain_ca = SqsFeedConfig::new("http://127.0.0.1:9324/000000000000/q");
        plain_ca.endpoint = Some("http://127.0.0.1:9324".to_owned());
        plain_ca.tls.root_ca = Some("/etc/ca.pem".into());
        let cases = [
            (
                fifo,
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q.fifo\" }",
            ),
            (
                short,
                "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\", visibility: 2s }",
            ),
            (
                plain_ca,
                "sqs: { queue_url: \"http://127.0.0.1:9324/000000000000/q\", endpoint: \"http://127.0.0.1:9324\", tls: { root_ca: /etc/ca.pem } }",
            ),
        ];
        for (config, yaml) in cases {
            assert_eq!(
                refusal(config.validate().map(|()| config.clone())),
                refusal(parse(yaml)),
                "{yaml}"
            );
        }
    }

    /// Pins that `Debug` prints both URLs without their query.
    #[test]
    fn debug_redacts_the_urls() {
        let config = parse(
            "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q?token=hunter2\", \
             endpoint: \"http://127.0.0.1:9324/?token=hunter2\" }",
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(
            debug.contains("https://sqs.eu-west-2.amazonaws.com/1/q?<redacted>"),
            "{debug}"
        );
    }

    /// Pins that the keys beside `sqs:` are refused at the section, since
    /// neither sits under it.
    #[test]
    fn keys_beside_the_body_are_refused_at_the_section() {
        let body = "sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\" }";
        let mut s = section(body);
        s.attempt_cap = NonZeroU32::new(3);
        assert_eq!(
            refusal(SqsFeedConfig::from_section(&s)),
            (
                "feed.sqs".to_owned(),
                "feed.sqs: `attempt_cap` does not apply to `sqs`: SQS counts deliveries through \
                 the queue's `maxReceiveCount`"
                    .to_owned()
            )
        );
        let mut s = section(body);
        s.settling_window = Some(Duration::from_secs(59));
        assert_eq!(
            refusal(SqsFeedConfig::from_section(&s)),
            (
                "feed.sqs".to_owned(),
                "feed.sqs: `settling_window` must be at least 1m; found 59s: `sqs` message \
                 counts take at least a minute to settle"
                    .to_owned()
            )
        );
    }

    /// Pins the refusals: another backend's tag, reported at the section,
    /// `attempt_cap`, an unknown
    /// key, a FIFO queue and a non-HTTP URL.
    #[test]
    fn a_section_the_sqs_feed_cannot_read_is_refused() {
        let err = SqsFeedConfig::from_section(&section("memory: {}"))
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("feed: ") && err.contains("names `memory`"),
            "{err}"
        );

        let mut with_cap =
            section("sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q\" }");
        with_cap.attempt_cap = NonZeroU32::new(3);
        let err = SqsFeedConfig::from_section(&with_cap).unwrap_err();
        assert!(err.to_string().contains("maxReceiveCount"), "{err}");

        let err = parse("sqs: { queue_url: \"https://x/1/q\", visibilty: 6s }").unwrap_err();
        assert!(err.to_string().contains("feed.sqs"), "{err}");

        let err = parse("sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/q.fifo\" }")
            .unwrap_err();
        assert!(err.to_string().contains("FIFO"), "{err}");

        let err = parse("sqs: { queue_url: \"ftp://x/1/q\" }").unwrap_err();
        assert!(err.to_string().contains("http or https"), "{err}");
    }
}
