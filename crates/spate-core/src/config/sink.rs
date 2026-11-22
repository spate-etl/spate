//! A sink section: one backend key, and the framework keys beside it.

use super::chunk::ChunkSection;
use super::component::{component_error, split_backend};
use super::{ComponentConfig, ConfigError, SectionPath, redact};
use crate::ops::ChunkConfig;
use crate::sink::{BatchConfig, BreakerConfig, InflightConfig, QueueSection, RetryConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;

/// One sink, as it sits under `sink:` or a `sinks:` entry.
///
/// One key names the backend, and its body holds the backend's own keys,
/// decoded by the connector. The framework keys sit beside it and apply
/// whatever the backend:
///
/// - `queue` sizes each shard's chunk queue;
/// - `chunk` sets how the chain terminal seals chunks;
/// - `batch`, `inflight`, `retry` and `breaker` tune the sink's worker pool,
///   each overlaying the connector's base field by field.
///
/// ```yaml
/// sink:
///   queue: { max_bytes: 8MiB }
///   batch: { max_rows: 1000000 }
///   clickhouse:
///     table: events
///     shards: [{ replicas: ["http://ch-0:8123"] }]
/// ```
///
/// A section built in code with [`SinkSection::new`] has no framework keys.
/// Code sets them per sink through
/// [`SinkOptions`](crate::pipeline::SinkOptions).
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub struct SinkSection {
    backend: ComponentConfig,
    /// The framework blocks, stored unparsed so a malformed one surfaces with
    /// its path when it is resolved.
    queue: Option<serde_yaml::Value>,
    chunk: Option<serde_yaml::Value>,
    batch: Option<serde_yaml::Value>,
    inflight: Option<serde_yaml::Value>,
    retry: Option<serde_yaml::Value>,
    breaker: Option<serde_yaml::Value>,
}

// Hand-written: a block can hold an interpolated value. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for SinkSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let SinkSection {
            backend,
            queue,
            chunk,
            batch,
            inflight,
            retry,
            breaker,
        } = self;
        f.debug_struct("SinkSection")
            .field("backend", backend)
            .field("queue", &block(queue))
            .field("chunk", &block(chunk))
            .field("batch", &block(batch))
            .field("inflight", &block(inflight))
            .field("retry", &block(retry))
            .field("breaker", &block(breaker))
            .finish()
    }
}

/// A block's `Debug` form, its keys and shape without its values.
fn block(value: &Option<serde_yaml::Value>) -> Option<impl fmt::Debug + '_> {
    value.as_ref().map(redact::yaml)
}

impl SinkSection {
    /// A section naming `backend`, with no framework keys.
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, SinkSection, YamlValue};
    ///
    /// let sink = SinkSection::new(ComponentConfig::new(
    ///     "memory",
    ///     YamlValue::Mapping(Default::default()),
    /// ));
    /// assert_eq!(sink.backend().type_tag(), "memory");
    /// ```
    #[must_use]
    pub fn new(backend: ComponentConfig) -> SinkSection {
        SinkSection {
            backend,
            queue: None,
            chunk: None,
            batch: None,
            inflight: None,
            retry: None,
            breaker: None,
        }
    }

    /// The backend component (`<backend>: { ... }`), which the connector
    /// decodes.
    #[must_use]
    pub fn backend(&self) -> &ComponentConfig {
        &self.backend
    }

    pub(super) fn set_section(&mut self, section: impl Into<Cow<'static, str>>) {
        self.backend.set_section(section);
    }

    /// The framework keys this section declares, in the order the section
    /// docs list them.
    pub(crate) fn declared(&self) -> Vec<&'static str> {
        [
            ("queue", &self.queue),
            ("chunk", &self.chunk),
            ("batch", &self.batch),
            ("inflight", &self.inflight),
            ("retry", &self.retry),
            ("breaker", &self.breaker),
        ]
        .into_iter()
        .filter_map(|(key, block)| block.as_ref().map(|_| key))
        .collect()
    }

    /// The `chunk:` block resolved, for the sink at `address` (`sink` or
    /// `sinks.<name>`). `Ok(None)` when the section declares none.
    pub(crate) fn chunk(&self, address: &str) -> Result<Option<ChunkConfig>, ConfigError> {
        let Some(value) = &self.chunk else {
            return Ok(None);
        };
        let at = SectionPath::at(address);
        let section: ChunkSection = serde_path_to_error::deserialize(value.clone())
            .map_err(|e| component_error(at.clone(), Some("chunk"), e))?;
        section.resolve(&at).map(Some)
    }

    /// The `queue:` block decoded. Its slot count depends on the chunk size,
    /// so the resolver bounds it.
    pub(crate) fn queue(&self, address: &str) -> Result<Option<QueueSection>, ConfigError> {
        decode(self.queue.as_ref(), address, "queue")
    }

    /// The `batch:` block overlaid on `base`.
    pub(crate) fn batch(
        &self,
        address: &str,
        base: &BatchConfig,
    ) -> Result<Option<BatchConfig>, ConfigError> {
        overlay(self.batch.as_ref(), base, address, "batch")
    }

    /// The `inflight:` block overlaid on `base`.
    pub(crate) fn inflight(
        &self,
        address: &str,
        base: &InflightConfig,
    ) -> Result<Option<InflightConfig>, ConfigError> {
        overlay(self.inflight.as_ref(), base, address, "inflight")
    }

    /// The `retry:` block overlaid on `base`.
    pub(crate) fn retry(
        &self,
        address: &str,
        base: &RetryConfig,
    ) -> Result<Option<RetryConfig>, ConfigError> {
        overlay(self.retry.as_ref(), base, address, "retry")
    }

    /// The `breaker:` block overlaid on `base`.
    pub(crate) fn breaker(
        &self,
        address: &str,
        base: &BreakerConfig,
    ) -> Result<Option<BreakerConfig>, ConfigError> {
        overlay(self.breaker.as_ref(), base, address, "breaker")
    }

    /// Refuse a block that cannot resolve, at load. `chunk` and `queue`
    /// resolve in full; `queue` is bounded against the section's chunk or the
    /// default one. A pool block is decoded over core's default, which
    /// checks its shape. Its rules run at install, on the connector's base.
    pub(crate) fn check(&self, address: &str) -> Result<(), ConfigError> {
        let chunk = self.chunk(address)?.unwrap_or_default();
        if let Some(queue) = self.queue(address)? {
            queue.capacity(chunk.target_bytes, &SectionPath::at(address))?;
        }
        self.batch(address, &BatchConfig::default())?;
        self.inflight(address, &InflightConfig::default())?;
        self.retry(address, &RetryConfig::default())?;
        self.breaker(address, &BreakerConfig::default())?;
        Ok(())
    }
}

/// `block` decoded as `T`, with errors under `<address>.<key>`.
fn decode<T: DeserializeOwned>(
    block: Option<&serde_yaml::Value>,
    address: &str,
    key: &str,
) -> Result<Option<T>, ConfigError> {
    block
        .map(|value| {
            serde_path_to_error::deserialize(value.clone())
                .map_err(|e| component_error(SectionPath::at(address), Some(key), e))
        })
        .transpose()
}

/// `block` overlaid field by field on `base`: the keys it sets replace the
/// base's, and the rest keep the base's values. A bare `key:` keeps the base.
/// The merge goes through `T`'s own serde form, so the field list is declared
/// once, on `T`.
fn overlay<T: Serialize + DeserializeOwned>(
    block: Option<&serde_yaml::Value>,
    base: &T,
    address: &str,
    key: &str,
) -> Result<Option<T>, ConfigError> {
    let Some(block) = block else {
        return Ok(None);
    };
    let merged = match block {
        serde_yaml::Value::Null => serde_yaml::to_value(base),
        serde_yaml::Value::Mapping(fields) => serde_yaml::to_value(base).map(|mut value| {
            if let Some(merged) = value.as_mapping_mut() {
                for (field, v) in fields {
                    merged.insert(field.clone(), v.clone());
                }
            }
            value
        }),
        // Not a mapping: decoding it alone names the type it should be.
        other => Ok(other.clone()),
    }
    .expect("the pool types serialize to a mapping");
    decode(Some(&merged), address, key)
}

/// The framework keys, in the order an error lists them.
const FRAMEWORK_KEYS: [&str; 6] = ["queue", "chunk", "batch", "inflight", "retry", "breaker"];

impl<'de> Deserialize<'de> for SinkSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let keys = serde_yaml::Mapping::deserialize(deserializer)?;
        let ([queue, chunk, batch, inflight, retry, breaker], backend) =
            split_backend::<D::Error, 6>(keys, "a sink section", FRAMEWORK_KEYS, "`memory: {}`")?;
        Ok(SinkSection {
            backend,
            queue,
            chunk,
            batch,
            inflight,
            retry,
            breaker,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::SinkPoolConfig;
    use std::time::Duration;

    /// Two pools at the extremes of each field, one with non-round values.
    fn extreme_bases() -> [SinkPoolConfig; 2] {
        let mut high = SinkPoolConfig::default();
        high.batch.max_rows = u64::MAX;
        high.batch.max_bytes = u64::MAX;
        high.batch.linger = crate::clock::LONGEST_DEADLINE;
        high.inflight.max_per_shard = tokio::sync::Semaphore::MAX_PERMITS;
        high.retry.initial = Duration::new(1, 1);
        high.retry.max = Duration::MAX;
        high.retry.multiplier = 1e9;
        high.retry.jitter = 1.0;
        high.retry.max_attempts = u32::MAX;
        high.breaker.failure_threshold = u32::MAX;
        high.breaker.open_for = BreakerConfig::MAX_OPEN_FOR;
        high.breaker.half_open_probes = u32::MAX;
        let mut odd = SinkPoolConfig::default();
        odd.batch.max_rows = 1;
        odd.batch.max_bytes = 1_000_001;
        odd.batch.linger = Duration::from_nanos(1);
        odd.retry.initial = Duration::from_nanos(7);
        odd.retry.multiplier = 1.000_000_1;
        odd.retry.jitter = 0.0;
        odd.breaker.open_for = Duration::new(3, 999_999_999);
        [high, odd]
    }

    /// An empty or bare block leaves every field of the base as it was, at
    /// the extremes too, so the overlay's serde round trip loses nothing.
    #[test]
    fn an_empty_overlay_keeps_the_base_exactly() {
        for base in extreme_bases() {
            for blocks in ["batch: {}\ninflight: {}\nretry: {}\nbreaker: {}", "batch:\nretry:\n"] {
                let mut keys: serde_yaml::Mapping = serde_yaml::from_str(blocks).unwrap();
                keys.insert("fake".into(), serde_yaml::Value::Null);
                let section: SinkSection =
                    serde_yaml::from_value(serde_yaml::Value::Mapping(keys)).unwrap();
                let got = SinkPoolConfig::new(
                    section.batch("sink", &base.batch).unwrap().unwrap_or(base.batch),
                    section.inflight("sink", &base.inflight).unwrap().unwrap_or(base.inflight),
                    section.retry("sink", &base.retry).unwrap().unwrap_or(base.retry),
                    section.breaker("sink", &base.breaker).unwrap().unwrap_or(base.breaker),
                );
                assert_eq!(got, base, "{blocks}");
            }
        }
    }

    /// `Debug` shows the blocks' keys and none of their values.
    #[test]
    fn debug_never_prints_block_values() {
        let section: SinkSection =
            serde_yaml::from_str("retry: { initial: hunter2 }\nfake: { password: hunter2 }\n")
                .unwrap();
        let printed = format!("{section:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("initial") && printed.contains("password"), "{printed}");
    }
}
