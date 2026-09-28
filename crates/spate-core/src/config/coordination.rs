//! The `coordination:` section: coordinator tuning plus the store that
//! coordinated instances of one job share.

use super::redact;
use super::{ComponentConfig, ConfigError, YamlValue};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::fmt;

/// The pipeline's `coordination:` section, handed to the source through
/// [`Source::configure_coordination`](crate::source::Source::configure_coordination).
///
/// `store:` is a single-key component selecting the backend, with the same
/// shape and error paths as `source:`. Every other key is coordinator tuning,
/// kept opaque here and decoded by the coordination crate through
/// [`deserialize_tuning`](Self::deserialize_tuning).
///
/// ```yaml
/// coordination:
///   instance_id: "${POD_NAME}"
///   lease_duration: 30s
///   store:
///     nats: { servers: ["nats://nats:4222"], job: exports-backfill }
/// ```
#[derive(Clone, PartialEq)]
pub struct CoordinationSection {
    store: ComponentConfig,
    tuning: YamlValue,
}

// Hand-written: both parts hold interpolated values, credentials included.
impl fmt::Debug for CoordinationSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let CoordinationSection { store, tuning } = self;
        f.debug_struct("CoordinationSection")
            .field("store", store)
            .field("tuning", &redact::yaml(tuning))
            .finish()
    }
}

impl CoordinationSection {
    /// The store component (`store: { <backend>: { ... } }`). Its errors carry
    /// paths such as `coordination.store.nats.servers`.
    #[must_use]
    pub fn store(&self) -> &ComponentConfig {
        &self.store
    }

    /// Deserialize the tuning keys (every key but `store`) into the
    /// coordinator's typed config. Errors carry paths such as
    /// `coordination.lease_duration`.
    pub fn deserialize_tuning<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_path_to_error::deserialize(self.tuning.clone()).map_err(|e| {
            let inner = e.path().to_string();
            let context = if inner == "." || inner.is_empty() {
                "coordination".to_owned()
            } else {
                format!("coordination.{inner}")
            };
            ConfigError::Component {
                context,
                message: redact::error_message(&e.into_inner().to_string()).into_owned(),
            }
        })
    }
}

impl<'de> Deserialize<'de> for CoordinationSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let mut tuning = serde_yaml::Mapping::deserialize(deserializer)?;
        let store = tuning
            .remove("store")
            .ok_or_else(|| D::Error::missing_field("store"))?;
        let mut store = ComponentConfig::deserialize(store)
            .map_err(|e| D::Error::custom(format!("store: {e}")))?;
        // The store has no chain terminal, so a `chunk:` peeled from its body
        // would otherwise vanish without an error.
        if store.has_chunk() {
            return Err(D::Error::custom(
                "store: `chunk:` is only valid on a sink section",
            ));
        }
        store.set_section("coordination.store");
        Ok(CoordinationSection {
            store,
            tuning: YamlValue::Mapping(tuning),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Tuning {
        #[serde(default, with = "humantime_serde")]
        lease_duration: Option<Duration>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Store {
        servers: Vec<String>,
    }

    fn parse(yaml: &str) -> Result<CoordinationSection, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn splits_store_from_tuning() {
        let s = parse("lease_duration: 10s\nstore:\n  nats: { servers: [\"nats://n:4222\"] }\n")
            .unwrap();
        assert_eq!(s.store().type_tag(), "nats");
        let store: Store = s.store().deserialize_into().unwrap();
        assert_eq!(store.servers, ["nats://n:4222"]);
        let tuning: Tuning = s.deserialize_tuning().unwrap();
        assert_eq!(tuning.lease_duration, Some(Duration::from_secs(10)));
    }

    #[test]
    fn store_is_required() {
        let err = parse("lease_duration: 10s\n").unwrap_err().to_string();
        assert!(err.contains("missing field `store`"), "{err}");
    }

    #[test]
    fn store_must_select_one_backend() {
        let err = parse("store: { nats: {}, other: {} }\n")
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("store: "), "{err}");
        assert!(err.contains("2 keys"), "{err}");
    }

    #[test]
    fn chunk_under_the_store_is_rejected() {
        let err = parse("store:\n  nats: { chunk: { target_bytes: 1MiB } }\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("chunk"), "{err}");
    }

    #[test]
    fn error_paths_are_rooted_at_coordination() {
        let s = parse("lease_duraton: 10s\nstore:\n  nats: { servrs: [] }\n").unwrap();
        let err = s.deserialize_tuning::<Tuning>().unwrap_err().to_string();
        assert!(err.starts_with("coordination"), "{err}");
        assert!(err.contains("lease_duraton"), "{err}");
        let err = s
            .store()
            .deserialize_into::<Store>()
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("coordination.store.nats"), "{err}");
    }

    /// `Debug` shows keys and none of the values, in both the store and the
    /// tuning.
    #[test]
    fn debug_never_prints_values() {
        let s = parse(
            "instance_id: pod-hunter2\nstore:\n  nats:\n    servers: [\"nats://u:hunter2@n\"]\n",
        )
        .unwrap();
        let printed = format!("{s:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        for visible in ["instance_id", "nats", "servers"] {
            assert!(printed.contains(visible), "{visible}: {printed}");
        }
    }
}
