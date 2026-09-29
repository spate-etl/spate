//! [`DynamoDbConfig`] and its validation.

use crate::store::StoreError;
use std::time::Duration;

/// The shortest lease the store accepts.
const MIN_LEASE: Duration = Duration::from_secs(1);

/// How many poll intervals a lease must span.
const POLLS_PER_LEASE: u32 = 5;

/// Where a job's coordination items live and how often watches list them.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DynamoDbConfig {
    /// Table name or ARN.
    pub table: String,
    /// Job identity, `[A-Za-z0-9_-]{1,64}`; the partition key prefix.
    pub job: String,
    /// Create a pay-per-request table and enable TTL on it when missing.
    pub create_table: bool,
    /// Time between two listings of a watched prefix.
    pub poll_interval: Duration,
}

impl DynamoDbConfig {
    /// A config with `create_table` off and a 2s poll interval.
    #[must_use]
    pub fn new(table: impl Into<String>, job: impl Into<String>) -> DynamoDbConfig {
        DynamoDbConfig {
            table: table.into(),
            job: job.into(),
            create_table: false,
            poll_interval: Duration::from_secs(2),
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
