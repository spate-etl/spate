//! The `feed:` section: the work feed a feed-reading source reads from, and
//! the keys every backend shares.

use super::ComponentConfig;
use serde::Deserialize;
use std::num::{NonZeroU32, NonZeroUsize};

/// The pipeline's `feed:` section, handed to the source through
/// [`Source::configure_feed`](crate::source::Source::configure_feed).
///
/// One key names the backend, with the same single-key shape as `source:`;
/// its body holds the backend's own keys and is decoded by the backend.
/// `units_held` applies whatever the backend; `attempt_cap` applies to the
/// in-process and Kafka feeds, and a backend that cannot honor it refuses
/// it.
///
/// ```yaml
/// feed:
///   memory: {}
///   units_held: 32
/// ```
///
/// Construct one in code with [`FeedSection::new`] and set the optional
/// fields. The struct is `#[non_exhaustive]` so new keys can be added
/// without breaking callers.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct FeedSection {
    backend: ComponentConfig,
    /// Units the source holds and reads at once. `None` leaves the source's
    /// own default.
    pub units_held: Option<NonZeroUsize>,
    /// The delivery on which a failed unit moves to its feed's dead-letter
    /// destination, on the in-process and Kafka feeds. The Kafka feed also
    /// moves a unit whose recorded deliveries have reached it, without
    /// delivering it again. `None` leaves the backend's default. A backend
    /// that cannot honor the key refuses it.
    pub attempt_cap: Option<NonZeroU32>,
}

impl FeedSection {
    /// A section naming `backend`, with every other key unset. Errors from
    /// the backend's body carry paths such as `feed.memory.<key>`.
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, FeedSection, YamlValue};
    /// use std::num::NonZeroUsize;
    ///
    /// let mut feed = FeedSection::new(ComponentConfig::new(
    ///     "memory",
    ///     YamlValue::Mapping(Default::default()),
    /// ));
    /// feed.units_held = NonZeroUsize::new(32);
    ///
    /// assert_eq!(feed.backend().type_tag(), "memory");
    /// ```
    #[must_use]
    pub fn new(mut backend: ComponentConfig) -> FeedSection {
        backend.set_section("feed");
        FeedSection {
            backend,
            units_held: None,
            attempt_cap: None,
        }
    }

    /// The backend component (`<backend>: { ... }`). Its
    /// [`type_tag`](ComponentConfig::type_tag) is the key a feed's
    /// [`backend`](crate::feed::Feed::backend) must return.
    #[must_use]
    pub fn backend(&self) -> &ComponentConfig {
        &self.backend
    }
}

impl<'de> Deserialize<'de> for FeedSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let mut keys = serde_yaml::Mapping::deserialize(deserializer)?;
        let units_held = keys
            .remove("units_held")
            .map(|v| {
                Option::<NonZeroUsize>::deserialize(v)
                    .map_err(|e| D::Error::custom(format!("units_held: {e}")))
            })
            .transpose()?
            .flatten();
        let attempt_cap = keys
            .remove("attempt_cap")
            .map(|v| {
                Option::<NonZeroU32>::deserialize(v)
                    .map_err(|e| D::Error::custom(format!("attempt_cap: {e}")))
            })
            .transpose()?
            .flatten();
        // Every key left names a backend, so a misspelled common key shows up
        // here as a second backend.
        if keys.len() != 1 {
            let found: Vec<String> = keys
                .keys()
                .map(|k| match k.as_str() {
                    Some(name) => format!("`{name}`"),
                    None => "a non-string key".to_owned(),
                })
                .collect();
            return Err(D::Error::custom(if found.is_empty() {
                "`feed:` names no backend; add one, e.g. `memory: {}`".to_owned()
            } else {
                format!(
                    "`feed:` must name exactly one backend beside `units_held` and \
                     `attempt_cap`; found {}",
                    found.join(", ")
                )
            }));
        }
        let mut backend = ComponentConfig::deserialize(serde_yaml::Value::Mapping(keys))
            .map_err(D::Error::custom)?;
        // The backend has no chain terminal, so a `chunk:` peeled from its body
        // would otherwise vanish without an error.
        if backend.has_chunk() {
            return Err(D::Error::custom(format!(
                "{}: `chunk:` is only valid on a sink section",
                backend.type_tag()
            )));
        }
        backend.set_section("feed");
        Ok(FeedSection {
            backend,
            units_held,
            attempt_cap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<FeedSection, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        x: u32,
    }

    /// Pins that the backend is the one key left beside the common keys, and
    /// that both common keys are read.
    #[test]
    fn one_backend_sits_beside_the_common_keys() {
        let s = parse("units_held: 12\nattempt_cap: 3\nmemory: { x: 7 }\n").unwrap();
        assert_eq!(s.backend().type_tag(), "memory");
        assert_eq!(s.units_held, NonZeroUsize::new(12));
        assert_eq!(s.attempt_cap, NonZeroU32::new(3));
        let body: Body = s.backend().deserialize_into().unwrap();
        assert_eq!(body.x, 7);

        let bare = parse("memory: {}\n").unwrap();
        assert_eq!(bare.units_held, None);
        assert_eq!(bare.attempt_cap, None);
    }

    /// Pins the error for a section with no backend key.
    #[test]
    fn a_section_with_no_backend_is_refused() {
        let err = parse("units_held: 4\n").unwrap_err().to_string();
        assert!(err.contains("names no backend"), "{err}");
    }

    /// Pins that two backend keys are refused, naming both.
    #[test]
    fn two_backends_are_refused_naming_both() {
        let err = parse("memory: {}\nother: {}\n").unwrap_err().to_string();
        assert!(err.contains("exactly one backend"), "{err}");
        assert!(err.contains("`memory`") && err.contains("`other`"), "{err}");
    }

    /// Pins that a misspelled common key reads as a second backend, so it is
    /// refused by name and never ignored.
    #[test]
    fn a_misspelled_common_key_is_refused_by_name() {
        let err = parse("memory: {}\nunit_held: 4\n").unwrap_err().to_string();
        assert!(err.contains("`unit_held`"), "{err}");
    }

    /// Pins that `units_held: 0` is refused.
    #[test]
    fn zero_units_held_is_refused() {
        let err = parse("memory: {}\nunits_held: 0\n")
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("units_held: "), "{err}");
    }

    /// Pins that a `chunk:` under the backend is refused.
    #[test]
    fn chunk_under_the_backend_is_refused() {
        let err = parse("memory: { chunk: { target_bytes: 1MiB } }\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("chunk"), "{err}");
    }

    /// Pins that a `chunk:` in a body built in code is refused, as on the YAML path.
    #[test]
    fn chunk_under_a_built_backend_is_refused() {
        let base = "pipeline: { name: demo }\nsource: { memory: {} }\nsink: { memory: {} }\n";
        let mut cfg = crate::config::PipelineConfig::from_str(base).unwrap();
        let body = serde_yaml::from_str("chunk: { target_bytes: 1MiB }").unwrap();
        cfg.feed = Some(FeedSection::new(ComponentConfig::new("memory", body)));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("not `feed`"), "{err}");
    }

    /// Pins that an error in the backend's body carries its path from the
    /// root, `feed.<backend>.<key>`, on the YAML path and from `new`.
    #[test]
    fn backend_errors_are_rooted_at_feed() {
        let s = parse("memory: { x: lots }\n").unwrap();
        let err = s
            .backend()
            .deserialize_into::<Body>()
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("feed.memory.x"), "{err}");

        let body = serde_yaml::from_str("x: lots").unwrap();
        let built = FeedSection::new(ComponentConfig::new("memory", body));
        let err = built
            .backend()
            .deserialize_into::<Body>()
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("feed.memory.x"), "{err}");
    }
}
