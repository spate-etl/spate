//! The `chunk:` sink sub-section, the wire form of [`ChunkConfig`].
//!
//! [`ChunkConfig`] lives in [`crate::ops`] as a plain runtime tuning struct
//! read on the terminal stage's hot path. This module is its config-layer
//! mirror, keeping serde and the `bytesize` wire form off that runtime type's
//! semver surface. The hot-path `ChunkConfig`/[`ErrorPolicy`] pair must not
//! grow a serde surface; the only convention it shares with
//! [`crate::sink::config`] is the `bytesize` field syntax.
//!
//! The framework peels this block out of the connector body (see
//! [`ComponentConfig`](super::ComponentConfig)) and resolves it into a
//! [`ChunkConfig`] at assembly time, so connectors never see it and need no
//! `chunk` field of their own.

use super::ConfigError;
use crate::error::ErrorPolicy;
use crate::ops::ChunkConfig;
use bytesize::ByteSize;
use serde::{Deserialize, Deserializer};

/// Accept `64KiB`-style sizes on the wire while keeping the field a plain
/// `u64`. Duplicated from [`crate::sink::config`].
fn de_byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    ByteSize::deserialize(d).map(|b| b.as_u64())
}

/// The record-level encoder-failure policy as it appears on the wire
/// (`skip` | `fail`). Keeps serde derives and wire tokens off the
/// `#[non_exhaustive]` runtime type [`ErrorPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EncodePolicyWire {
    Skip,
    Fail,
}

impl From<EncodePolicyWire> for ErrorPolicy {
    fn from(w: EncodePolicyWire) -> Self {
        match w {
            EncodePolicyWire::Skip => ErrorPolicy::Skip,
            EncodePolicyWire::Fail => ErrorPolicy::Fail,
        }
    }
}

/// The `chunk:` block inside a sink body:
/// `{ target_bytes: 512KiB, encode_policy: skip }`. Framework-owned and
/// identical across sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct ChunkSection {
    /// Chunk seal size; accepts `64KiB`-style strings or a plain byte count.
    #[serde(deserialize_with = "de_byte_size")]
    target_bytes: u64,
    /// Record-level encoder-failure policy (`skip` | `fail`).
    encode_policy: EncodePolicyWire,
}

impl Default for ChunkSection {
    fn default() -> Self {
        // Delegate to the runtime default so 64 KiB / Skip live in one place.
        // `ErrorPolicy::default()` is `Fail`, but the chunk encode default is
        // `Skip`. This must not become a field-wise derive.
        let ChunkConfig {
            target_bytes,
            encode_policy,
        } = ChunkConfig::default();
        ChunkSection {
            target_bytes: target_bytes as u64,
            encode_policy: match encode_policy {
                ErrorPolicy::Skip => EncodePolicyWire::Skip,
                ErrorPolicy::Fail => EncodePolicyWire::Fail,
            },
        }
    }
}

impl ChunkSection {
    /// Resolve into the runtime [`ChunkConfig`], validating bounds with
    /// field-named errors (the load-time counterpart to the cold-path
    /// `assert!` in `SinkHandoff::new`).
    pub(crate) fn resolve(&self) -> Result<ChunkConfig, ConfigError> {
        if self.target_bytes == 0 {
            return Err(ConfigError::Validation(
                "chunk.target_bytes must be greater than zero".into(),
            ));
        }
        // `usize` on a 32-bit target is narrower than the `u64` wire value.
        let target_bytes = usize::try_from(self.target_bytes).map_err(|_| {
            ConfigError::Validation(format!(
                "chunk.target_bytes ({}) exceeds the addressable range on this platform",
                self.target_bytes
            ))
        })?;
        Ok(ChunkConfig {
            target_bytes,
            encode_policy: self.encode_policy.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<ChunkSection, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn empty_block_is_the_runtime_default_including_skip() {
        // An omitted/empty `chunk:` must land on 64 KiB AND `Skip`, not
        // `ErrorPolicy::default()` which is `Fail`.
        let cfg = parse("{}").unwrap().resolve().unwrap();
        assert_eq!(cfg.target_bytes, 64 * 1024);
        assert_eq!(cfg.encode_policy, ErrorPolicy::Skip);
    }

    #[test]
    fn partial_block_keeps_the_skip_default() {
        let cfg = parse("target_bytes: 512KiB").unwrap().resolve().unwrap();
        assert_eq!(cfg.target_bytes, 512 * 1024);
        assert_eq!(cfg.encode_policy, ErrorPolicy::Skip);
    }

    #[test]
    fn parses_bytesize_and_encode_policy() {
        let cfg = parse("target_bytes: 128KiB\nencode_policy: fail")
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(cfg.target_bytes, 128 * 1024);
        assert_eq!(cfg.encode_policy, ErrorPolicy::Fail);
        // Plain integers are bytes.
        let cfg = parse("target_bytes: 4096").unwrap().resolve().unwrap();
        assert_eq!(cfg.target_bytes, 4096);
    }

    #[test]
    fn zero_target_is_a_field_named_error() {
        let err = parse("target_bytes: 0B").unwrap().resolve().unwrap_err();
        assert!(err.to_string().contains("chunk.target_bytes"), "{err}");
    }

    #[test]
    fn unknown_key_and_unknown_policy_are_rejected() {
        assert!(parse("bogus: 1").is_err());
        assert!(parse("encode_policy: retry").is_err());
    }
}
