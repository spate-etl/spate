//! The `feed:` section: the work feed a feed-reading source reads from, and
//! the keys every backend shares.

use super::{ComponentConfig, redact};
use serde::Deserialize;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

/// The pipeline's `feed:` section, handed to the source through
/// [`Source::configure_feed`](crate::source::Source::configure_feed).
///
/// One key names the backend, with the same single-key shape as `source:`;
/// its body holds the backend's own keys and is decoded by the backend.
/// `units_held` applies whatever the backend; `attempt_cap` applies to the
/// in-process and Kafka feeds, and a backend that cannot honor it refuses
/// it. `location` names a job location for a source that reads one, and
/// `settling_window` is the settling window of a source that ends a job
/// with a location; the in-process feed refuses both.
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
#[derive(Clone, PartialEq)]
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
    /// A job location, an object-store prefix, for a source that reads
    /// one; that source defines what the prefix holds. `None` when the
    /// section names none.
    /// The in-process feed refuses one.
    pub location: Option<String>,
    /// The settling window of a job with a `location`, for a source that ends
    /// one through an [`EndCheck`](crate::feed::EndCheck): how long the feed
    /// must read empty before the job ends. The source's page says what else
    /// it bounds. `None` leaves
    /// [`DEFAULT_SETTLING_WINDOW`](crate::feed::DEFAULT_SETTLING_WINDOW). The
    /// in-process feed refuses one.
    pub settling_window: Option<Duration>,
}

// Hand-written: a location URL can carry credentials in its userinfo or
// query. The destructure lists every field so a new one cannot reach `Debug`
// unredacted.
impl std::fmt::Debug for FeedSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let FeedSection {
            backend,
            units_held,
            attempt_cap,
            location,
            settling_window,
        } = self;
        f.debug_struct("FeedSection")
            .field("backend", backend)
            .field("units_held", units_held)
            .field("attempt_cap", attempt_cap)
            .field("location", &location.as_deref().map(redact::url))
            .field("settling_window", settling_window)
            .finish()
    }
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
            location: None,
            settling_window: None,
        }
    }

    /// The backend component (`<backend>: { ... }`). Its
    /// [`type_tag`](ComponentConfig::type_tag) is the key a feed's
    /// [`backend`](crate::feed::Feed::backend) must return.
    #[must_use]
    pub fn backend(&self) -> &ComponentConfig {
        &self.backend
    }

    /// The section's `settling_window`, or
    /// [`DEFAULT_SETTLING_WINDOW`](crate::feed::DEFAULT_SETTLING_WINDOW) when
    /// it sets none.
    #[must_use]
    pub fn settling_window_or_default(&self) -> Duration {
        self.settling_window
            .unwrap_or(crate::feed::DEFAULT_SETTLING_WINDOW)
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
        let location = keys
            .remove("location")
            .map(|v| {
                Option::<String>::deserialize(v)
                    .map_err(|e| D::Error::custom(format!("location: {e}")))
            })
            .transpose()?
            .flatten();
        let settling_window = keys
            .remove("settling_window")
            .map(|v| {
                humantime_serde::Serde::<Option<Duration>>::deserialize(v)
                    .map(humantime_serde::Serde::into_inner)
                    .map_err(|e| D::Error::custom(format!("settling_window: {e}")))
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
                    "`feed:` must name exactly one backend beside `units_held`, \
                     `attempt_cap`, `location` and `settling_window`; found {}",
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
            location,
            settling_window,
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
    /// that every common key is read.
    #[test]
    fn one_backend_sits_beside_the_common_keys() {
        let s = parse(
            "units_held: 12\nattempt_cap: 3\nlocation: s3://ops/jobs/a/\n\
             settling_window: 2m 30s\nmemory: { x: 7 }\n",
        )
        .unwrap();
        assert_eq!(s.backend().type_tag(), "memory");
        assert_eq!(s.units_held, NonZeroUsize::new(12));
        assert_eq!(s.attempt_cap, NonZeroU32::new(3));
        assert_eq!(s.location.as_deref(), Some("s3://ops/jobs/a/"));
        assert_eq!(s.settling_window, Some(Duration::from_secs(150)));
        let body: Body = s.backend().deserialize_into().unwrap();
        assert_eq!(body.x, 7);

        let bare = parse("memory: {}\nlocation: null\nsettling_window: null\n").unwrap();
        assert_eq!(bare.units_held, None);
        assert_eq!(bare.attempt_cap, None);
        assert_eq!(bare.location, None);
        assert_eq!(bare.settling_window, None);

        let built = FeedSection::new(ComponentConfig::new(
            "memory",
            serde_yaml::Value::Mapping(Default::default()),
        ));
        assert_eq!(built.location, None);
        assert_eq!(built.settling_window, None);
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

        let err = parse("memory: {}\nlocaton: s3://ops/a/\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("`locaton`"), "{err}");
        assert!(err.contains("`location`"), "{err}");

        let err = parse("memory: {}\nsettling_windw: 5m\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("`settling_windw`"), "{err}");
        assert!(err.contains("`settling_window`"), "{err}");
    }

    /// Pins that a `settling_window` that is not a duration is refused under
    /// its own key.
    #[test]
    fn a_settling_window_that_is_not_a_duration_is_refused() {
        for value in ["[1]", "lots", "5"] {
            let err = parse(&format!("memory: {{}}\nsettling_window: {value}\n"))
                .unwrap_err()
                .to_string();
            assert!(err.starts_with("settling_window: "), "{value}: {err}");
        }
    }

    /// Pins that a section with no `settling_window` reads as five minutes,
    /// and one that sets it reads as its value.
    #[test]
    fn an_absent_settling_window_reads_as_five_minutes() {
        let s = parse("sqs: {}\n").unwrap();
        assert_eq!(s.settling_window_or_default(), Duration::from_secs(300));
        let s = parse("sqs: {}\nsettling_window: 90s\n").unwrap();
        assert_eq!(s.settling_window_or_default(), Duration::from_secs(90));
    }

    fn load(feed: &str) -> Result<crate::config::PipelineConfig, String> {
        let yaml = format!(
            "pipeline: {{ name: demo }}\nsource: {{ memory: {{}} }}\nsink: {{ memory: {{}} }}\n\
             feed: {feed}\n"
        );
        crate::config::PipelineConfig::from_str(&yaml).map_err(|e| e.to_string())
    }

    /// Pins that loading a file refuses a zero `settling_window`.
    #[test]
    fn a_zero_settling_window_is_refused() {
        let err = load("{ sqs: {}, settling_window: 0s }").unwrap_err();
        assert!(err.contains("`feed.settling_window`"), "{err}");
    }

    /// Pins the window's floor at one read interval: 20 s loads, 19 s is
    /// refused.
    #[test]
    fn a_settling_window_below_the_floor_is_refused() {
        let cfg = load("{ sqs: {}, settling_window: 20s }").expect("the floor loads");
        assert_eq!(
            cfg.feed.unwrap().settling_window,
            Some(Duration::from_secs(20))
        );
        let err = load("{ sqs: {}, settling_window: 19s 999ms }").unwrap_err();
        assert!(
            err.contains("`feed.settling_window`") && err.contains("20s"),
            "{err}"
        );
    }

    /// Pins the window's ceiling at a day: `24h` loads, a second more and a
    /// value that would overflow a deadline are refused.
    #[test]
    fn a_settling_window_above_a_day_is_refused() {
        load("{ sqs: {}, settling_window: 24h }").expect("a day loads");
        for value in ["24h 1s", "1000000years"] {
            let err = load(&format!("{{ sqs: {{}}, settling_window: {value} }}")).unwrap_err();
            assert!(
                err.contains("`feed.settling_window`") && err.contains("1day"),
                "{value}: {err}"
            );
        }
    }

    /// Pins that loading a file refuses a `settling_window` beside the
    /// in-process feed, naming the rule.
    #[test]
    fn a_memory_feed_with_a_settling_window_is_refused() {
        let err = load("{ memory: {}, settling_window: 1m }").unwrap_err();
        assert!(
            err.contains("`feed.settling_window`") && err.contains("`memory`"),
            "{err}"
        );
    }

    /// Pins that a `location` that is not a string is refused under its own
    /// key.
    #[test]
    fn a_location_that_is_not_a_string_is_refused() {
        let err = parse("memory: {}\nlocation: [1]\n")
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("location: "), "{err}");
    }

    /// Pins that `Debug` shows the location without its userinfo or query.
    #[test]
    fn a_section_debug_redacts_the_location() {
        let s = parse("memory: {}\nlocation: s3://u:secret@ops/jobs/a?x=1\nsettling_window: 1m\n")
            .unwrap();
        let shown = format!("{s:?}");
        assert!(shown.contains("ops/jobs/a"), "{shown}");
        assert!(shown.contains("settling_window: Some(60s)"), "{shown}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(!shown.contains("x=1"), "{shown}");
    }

    /// Pins that loading a file refuses a `location` beside the in-process
    /// feed, naming the rule.
    #[test]
    fn a_memory_feed_with_a_location_is_refused_at_load() {
        let yaml = "pipeline: { name: demo }\nsource: { memory: {} }\nsink: { memory: {} }\n\
                    feed: { memory: {}, location: s3://ops/jobs/a/ }\n";
        let err = crate::config::PipelineConfig::from_str(yaml)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`feed.location`") && err.contains("`memory`"),
            "{err}"
        );
    }

    /// Pins that loading a file keeps a `location` beside a backend other
    /// than the in-process feed.
    #[test]
    fn a_location_beside_a_durable_backend_loads() {
        let yaml = "pipeline: { name: demo }\nsource: { memory: {} }\nsink: { memory: {} }\n\
                    feed: { sqs: {}, location: s3://ops/jobs/a/ }\n";
        let cfg = crate::config::PipelineConfig::from_str(yaml).expect("loads");
        let feed = cfg.feed.expect("feed section");
        assert_eq!(feed.location.as_deref(), Some("s3://ops/jobs/a/"));
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
