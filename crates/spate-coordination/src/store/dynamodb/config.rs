//! [`DynamoDbConfig`] and its validation.

use crate::store::StoreError;
use serde::Deserialize;
use spate_core::config::redact;
use std::fmt;
use std::time::Duration;

/// The shortest lease the store accepts.
const MIN_LEASE: Duration = Duration::from_secs(1);

/// How many poll intervals a lease must span.
const POLLS_PER_LEASE: u32 = 5;

/// Where a job's coordination items live and how often watches list them.
///
/// Construct with [`DynamoDbConfig::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers. Credentials come from the AWS provider chain, never from here.
///
/// `Debug` is safe to log: the endpoint's userinfo and query are redacted.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DynamoDbConfig {
    /// Table name or ARN.
    pub table: String,
    /// Job identity, `[A-Za-z0-9_-]{1,64}`; the partition key prefix.
    pub job: String,
    /// AWS region. Default: the provider chain's.
    #[serde(default)]
    pub region: Option<String>,
    /// Endpoint URL in place of the region's, such as a local emulator's.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Create a pay-per-request table and enable TTL on it when missing.
    #[serde(default)]
    pub create_table: bool,
    /// Time between two listings of a watched prefix. Default 2s.
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
}

// Hand-written: an endpoint URL can carry credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for DynamoDbConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DynamoDbConfig {
            table,
            job,
            region,
            endpoint,
            create_table,
            poll_interval,
        } = self;
        f.debug_struct("DynamoDbConfig")
            .field("table", table)
            .field("job", job)
            .field("region", region)
            .field("endpoint", &endpoint.as_deref().map(redact::url))
            .field("create_table", create_table)
            .field("poll_interval", poll_interval)
            .finish()
    }
}

fn default_poll_interval() -> Duration {
    Duration::from_secs(2)
}

impl DynamoDbConfig {
    /// A config with the provider chain's region, the region's endpoint,
    /// `create_table` off and a 2s poll interval.
    #[must_use]
    pub fn new(table: impl Into<String>, job: impl Into<String>) -> DynamoDbConfig {
        DynamoDbConfig {
            table: table.into(),
            job: job.into(),
            region: None,
            endpoint: None,
            create_table: false,
            poll_interval: default_poll_interval(),
        }
    }

    pub(super) fn validate(&self, lease_ttl: Duration) -> Result<(), StoreError> {
        let fatal = |msg: String| Err(StoreError::Fatal(msg));
        let table = &self.table;
        let is_arn = table.starts_with("arn:") && table.contains(":table/");
        let is_name = (3..=255).contains(&table.len())
            && table
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
        if !is_arn && !is_name {
            return fatal(format!(
                "dynamodb.table must be a table ARN or 3..=255 chars of [A-Za-z0-9_.-], got \
                 {table:?}"
            ));
        }
        let job = &self.job;
        if job.is_empty()
            || job.len() > 64
            || !job
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        {
            return fatal(format!(
                "dynamodb.job must be 1..=64 chars of [A-Za-z0-9_-], got {job:?}"
            ));
        }
        if self.region.as_deref().is_some_and(str::is_empty) {
            return fatal(
                "dynamodb.region must not be empty; remove it to take the AWS provider chain's"
                    .into(),
            );
        }
        // Not echoed: the URL can carry credentials.
        if let Some(endpoint) = &self.endpoint {
            let host = endpoint
                .strip_prefix("http://")
                .or_else(|| endpoint.strip_prefix("https://"))
                .map(|rest| rest.split(['/', '?', '#']).next().unwrap_or(""));
            if host.is_none_or(str::is_empty) {
                return fatal(
                    "dynamodb.endpoint must be an http:// or https:// URL with a host".into(),
                );
            }
        }
        let poll = self.poll_interval;
        if poll.is_zero() || lease_ttl < MIN_LEASE || lease_ttl < poll * POLLS_PER_LEASE {
            return fatal(format!(
                "the DynamoDB store needs lease_duration >= {MIN_LEASE:?} and >= \
                 {POLLS_PER_LEASE} x dynamodb.poll_interval, with a poll interval above zero; \
                 got a lease of {lease_ttl:?} and a poll interval of {poll:?}"
            ));
        }
        Ok(())
    }
}
