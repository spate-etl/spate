//! The `sqs:` body of a `feed:` section, decoded and checked.

use aws_sdk_sqs::config::endpoint::{DefaultResolver, Params, ResolveEndpoint as _};
use serde::Deserialize;
use spate_core::config::{ConfigError, FeedSection};
use std::time::Duration;
use url::Url;

/// The key that names this backend in a `feed:` section.
pub(crate) const BACKEND: &str = "sqs";

/// The shortest visibility period: one second per renewal period, and a
/// long-poll wait of at least one second.
const MIN_VISIBILITY: Duration = Duration::from_secs(3);

/// The longest visibility period SQS accepts.
const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 60 * 60);

/// The shortest settling window: AWS documents a queue's approximate counts
/// as taking at least a minute to settle after its producers stop.
const MIN_SETTLING_WINDOW: Duration = Duration::from_secs(60);

fn default_visibility() -> Duration {
    Duration::from_secs(60)
}

/// The keys under `feed: { sqs: … }`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqsFeedConfig {
    /// The queue's URL, as `GetQueueUrl` returns it.
    pub(crate) queue_url: String,
    /// How long a received unit stays hidden from other readers without a
    /// renewal.
    #[serde(default = "default_visibility", with = "humantime_serde")]
    pub(crate) visibility: Duration,
    /// The AWS region. Unset: the queue URL's host, then the AWS region
    /// chain.
    #[serde(default)]
    pub(crate) region: Option<String>,
    /// An endpoint in place of the region's, such as a local emulator.
    #[serde(default)]
    pub(crate) endpoint: Option<String>,
}

impl SqsFeedConfig {
    /// Decode and check the `sqs:` body of `section`.
    pub(crate) fn from_section(section: &FeedSection) -> Result<SqsFeedConfig, ConfigError> {
        let backend = section.backend();
        if backend.type_tag() != BACKEND {
            return Err(error(format!(
                "the SQS feed is built from `feed: {{ {BACKEND}: … }}`, but the section names `{}`",
                backend.type_tag()
            )));
        }
        if section.attempt_cap.is_some() {
            return Err(error(
                "`attempt_cap` does not apply to `sqs`: SQS counts deliveries through the \
                 queue's `maxReceiveCount`"
                    .to_owned(),
            ));
        }
        if let Some(window) = section.settling_window
            && window < MIN_SETTLING_WINDOW
        {
            return Err(error(format!(
                "`settling_window` must be at least 1m on `sqs`, whose message counts take at \
                 least a minute to settle; got {window:?}"
            )));
        }
        let config: SqsFeedConfig = backend.deserialize_into()?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let v = self.visibility;
        if v.subsec_nanos() != 0 || !(MIN_VISIBILITY..=MAX_VISIBILITY).contains(&v) {
            return Err(error(format!(
                "`visibility` must be whole seconds from 3s to 12h; got {v:?}"
            )));
        }
        let url = Url::parse(&self.queue_url)
            .map_err(|e| error(format!("`queue_url` is not a URL: {e}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(error(format!(
                "`queue_url` must be an http or https URL; got scheme `{}`",
                url.scheme()
            )));
        }
        if url.path().trim_end_matches('/').ends_with(".fifo") {
            return Err(error(
                "`queue_url` names a FIFO queue; the SQS feed reads standard queues only"
                    .to_owned(),
            ));
        }
        if let Some(endpoint) = &self.endpoint {
            Url::parse(endpoint).map_err(|e| error(format!("`endpoint` is not a URL: {e}")))?;
        }
        Ok(())
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

fn error(message: String) -> ConfigError {
    ConfigError::Component {
        context: format!("feed.{BACKEND}"),
        message,
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
        for bad in ["2s", "43201s", "2500ms"] {
            let err = with(bad).unwrap_err().to_string();
            assert!(err.contains("whole seconds from 3s to 12h"), "{bad}: {err}");
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

    /// Pins the refusals: another backend's tag, `attempt_cap`, an unknown
    /// key, a FIFO queue and a non-HTTP URL.
    #[test]
    fn a_section_the_sqs_feed_cannot_read_is_refused() {
        let err = SqsFeedConfig::from_section(&section("memory: {}")).unwrap_err();
        assert!(err.to_string().contains("names `memory`"), "{err}");

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
