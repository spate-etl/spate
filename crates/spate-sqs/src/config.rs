//! The `sqs:` body of a `feed:` section, decoded and checked.

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

    /// The region the queue URL's host names, for `sqs.<region>.amazonaws.com`
    /// and its FIPS and dual-stack forms.
    pub(crate) fn region_from_url(&self) -> Option<String> {
        let url = Url::parse(&self.queue_url).ok()?;
        let host = url.host_str()?;
        let mut labels = host.split('.');
        let service = labels.next()?;
        if !matches!(service, "sqs" | "sqs-fips") {
            return None;
        }
        let region = labels.next()?;
        let rest: Vec<&str> = labels.collect();
        let known = matches!(
            rest.as_slice(),
            ["amazonaws", "com"] | ["amazonaws", "com", "cn"] | ["api", "aws"]
        );
        known.then(|| region.to_owned())
    }
}

fn error(message: String) -> ConfigError {
    ConfigError::Component {
        context: format!("feed.{BACKEND}"),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    fn section(yaml: &str) -> FeedSection {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn parse(yaml: &str) -> Result<SqsFeedConfig, ConfigError> {
        SqsFeedConfig::from_section(&section(yaml))
    }

    /// Pins the defaults and every key read.
    #[test]
    fn the_body_is_read_with_its_defaults() {
        let config =
            parse("sqs: { queue_url: \"https://sqs.eu-west-2.amazonaws.com/1/jobs\" }").unwrap();
        assert_eq!(config.visibility, Duration::from_secs(60));
        assert_eq!(config.renewal_period(), Duration::from_secs(20));
        assert_eq!(config.region, None);
        assert_eq!(config.region_from_url().as_deref(), Some("eu-west-2"));

        let config = parse(
            "sqs: { queue_url: \"http://127.0.0.1:9324/000000000000/q\", visibility: 6s, \
             region: us-east-1, endpoint: \"http://127.0.0.1:9324\" }",
        )
        .unwrap();
        assert_eq!(config.visibility, Duration::from_secs(6));
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.region_from_url(), None);
    }

    /// Pins the queue URL hosts the region is read from.
    #[test]
    fn the_region_is_read_from_every_sqs_host_form() {
        for (host, region) in [
            ("sqs.eu-west-2.amazonaws.com", Some("eu-west-2")),
            ("sqs-fips.us-east-1.amazonaws.com", Some("us-east-1")),
            ("sqs.cn-north-1.amazonaws.com.cn", Some("cn-north-1")),
            ("sqs.us-east-2.api.aws", Some("us-east-2")),
            ("sqs.eu-west-2.example.com", None),
        ] {
            let config = parse(&format!("sqs: {{ queue_url: \"https://{host}/1/q\" }}")).unwrap();
            assert_eq!(config.region_from_url().as_deref(), region, "{host}");
        }
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
