//! Configuration of a `DatagenSource`, deserialized
//! from the pipeline's opaque `source: { datagen: ... }` section.
//!
//! Deliberately absent: a raw option passthrough. Every other connector has
//! one because it wraps a client with its own configuration surface, and the
//! deployer needs a way to reach it. This source wraps nothing, so there is
//! no second surface to pass through to, and a map that accepted keys nothing
//! reads would be worse than no map at all.
//!
//! Also deliberately absent: a `rate:` key. The release rate is
//! `partitions × events_per_tick ÷ tick_interval`, which is 400 events/s at
//! the defaults. Expressing it twice is how the two spellings come to
//! disagree.

use serde::Deserialize;
use spate_core::config::{ComponentConfig, ConfigError};
use std::time::Duration;

/// 2026-01-01T00:00:00Z, in milliseconds. The base for the `fixed` clock:
/// a round instant a reader recognizes as synthetic.
pub(crate) const DEFAULT_EPOCH_MS: i64 = 1_767_225_600_000;

/// Most lanes a source will build. Every lane commits a generator, its rings
/// and a batch-sized arena at `open`, so the count is bounded rather than left
/// to a typo in a `u32` field.
const MAX_PARTITIONS: u32 = 1_024;

/// What `encoding: avro` reports when the feature that implements it is off.
/// One string for both the load-time rejection below and the `open`-time one
/// in [`crate::encode`], which answer for the same condition.
pub(crate) const AVRO_FEATURE_OFF: &str = "source.datagen.encoding: avro needs spate-datagen's `avro` feature \
     (the `datagen-avro` feature on the spate facade); it is off in this build";

fn default_partitions() -> u32 {
    4
}

fn default_tick_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_events_per_tick() -> u32 {
    10
}

fn default_epoch_ms() -> i64 {
    DEFAULT_EPOCH_MS
}

/// Which built-in dataset to generate.
///
/// A named dataset, not a schema: see the crate docs for why a `fields:` map
/// is out of scope.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Dataset {
    /// Orders, payments and refunds over a small catalog, the model in
    /// [`crate::storefront`].
    #[default]
    Storefront,
}

/// Wire format of the generated payloads.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Encoding {
    /// One JSON document per payload, internally tagged by `type`.
    #[default]
    Json,
    /// One bare Avro datum per payload, matching
    /// [`EVENT_SCHEMA_JSON`](crate::EVENT_SCHEMA_JSON). Needs this crate's
    /// `avro` feature; without it the value is rejected at load time.
    Avro,
}

/// Where event timestamps come from.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Clock {
    /// Deterministic: `epoch_ms` plus one millisecond per event the lane has
    /// released. Two runs with the same seed produce byte-identical payloads,
    /// which is what lets a test assert on them.
    #[default]
    Fixed,
    /// The host clock, for a demo whose dashboard has a time axis.
    Wall,
}

/// Configuration of a `DatagenSource`.
///
/// Every key has a default, so `source: { datagen: {} }` is a complete
/// section.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DatagenSourceConfig {
    /// The built-in dataset to generate.
    #[serde(default)]
    pub dataset: Dataset,
    /// Wire format of the payloads.
    #[serde(default)]
    pub encoding: Encoding,
    /// How many lanes to run, and therefore how many framework partitions
    /// the pipeline sees. Each lane owns a disjoint slice of the order-id
    /// space and generates independently; no lane ever reads another's
    /// state. At least 1, and at most 1024.
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// Seed of the whole stream. Lane `i` derives its own stream from it, so
    /// changing `partitions` reshuffles which lane mints which order but
    /// leaves each lane reproducible.
    #[serde(default)]
    pub seed: u64,
    /// How often a lane releases a batch. **`0s` means unthrottled**: the
    /// lane generates as fast as the pipeline consumes, which is what a
    /// throughput measurement wants and what a demo does not.
    #[serde(default = "default_tick_interval", with = "humantime_serde")]
    pub tick_interval: Duration,
    /// Events released per lane per tick. Ignored when unthrottled. At
    /// least 1.
    #[serde(default = "default_events_per_tick")]
    pub events_per_tick: u32,
    /// Stop after this many events **across all lanes**, then drain the
    /// pipeline to a clean exit. Absent means the stream never ends.
    #[serde(default)]
    pub count: Option<u64>,
    /// Where event timestamps come from.
    #[serde(default)]
    pub clock: Clock,
    /// Base instant for the `fixed` clock, in milliseconds since the Unix
    /// epoch. Ignored by the `wall` clock.
    #[serde(default = "default_epoch_ms")]
    pub epoch_ms: i64,
}

// Hand-written rather than derived, so the defaults a hand-built config gets
// are the same function calls serde reaches for. A test below asserts the two
// agree; derive would have let them drift.
impl Default for DatagenSourceConfig {
    fn default() -> DatagenSourceConfig {
        DatagenSourceConfig {
            dataset: Dataset::default(),
            encoding: Encoding::default(),
            partitions: default_partitions(),
            seed: 0,
            tick_interval: default_tick_interval(),
            events_per_tick: default_events_per_tick(),
            count: None,
            clock: Clock::default(),
            epoch_ms: default_epoch_ms(),
        }
    }
}

impl DatagenSourceConfig {
    /// Deserialize and validate from the pipeline's opaque component section.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: DatagenSourceConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partitions == 0 {
            return Err(ConfigError::Validation(
                "source.datagen.partitions must be at least 1".into(),
            ));
        }
        if self.partitions > MAX_PARTITIONS {
            return Err(ConfigError::Validation(format!(
                "source.datagen.partitions ({}) is above the {MAX_PARTITIONS} this source \
                 builds: every lane holds its own generator, its rings and an arena sized \
                 to one batch, and all of it is committed at open",
                self.partitions,
            )));
        }
        if self.events_per_tick == 0 {
            return Err(ConfigError::Validation(
                "source.datagen.events_per_tick must be at least 1 (set tick_interval: 0s \
                 to run unthrottled instead)"
                    .into(),
            ));
        }
        if let Some(count) = self.count {
            if count == 0 {
                return Err(ConfigError::Validation(
                    "source.datagen.count must be at least 1; omit it for an unbounded stream"
                        .into(),
                ));
            }
            if count < u64::from(self.partitions) {
                return Err(ConfigError::Validation(format!(
                    "source.datagen.count ({count}) is below source.datagen.partitions ({}): \
                     the total splits as count / partitions = {} events per lane with the \
                     first {} lanes taking one more, so {} lane(s) would be born exhausted \
                     — lower partitions or raise count",
                    self.partitions,
                    count / u64::from(self.partitions),
                    count % u64::from(self.partitions),
                    u64::from(self.partitions) - count,
                )));
            }
        }
        if self.encoding == Encoding::Avro && !cfg!(feature = "avro") {
            return Err(ConfigError::Validation(AVRO_FEATURE_OFF.into()));
        }
        Ok(())
    }

    /// Per-lane event budgets, summing to exactly `count`. Lane `i` takes the
    /// integer share plus one more while `i` is below the remainder, so the
    /// split is deterministic and every event is accounted for exactly once.
    /// `None` when the stream is unbounded.
    pub(crate) fn budgets(&self) -> Option<Vec<u64>> {
        let count = self.count?;
        let partitions = u64::from(self.partitions);
        Some(
            (0..partitions)
                .map(|i| count / partitions + u64::from(i < count % partitions))
                .collect(),
        )
    }

    /// Lane `i`'s seed. The multiplier is the same 64-bit Weyl constant the
    /// generator uses, so adjacent lane indices land far apart in the stream
    /// rather than one step along it.
    pub(crate) fn lane_seed(&self, lane: u32) -> u64 {
        self.seed ^ u64::from(lane).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(body: &str) -> ComponentConfig {
        let yaml = format!("datagen:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("datagen", value["datagen"].clone())
    }

    /// The two default paths, serde's and `Default::default()`, must not be
    /// able to drift. This assertion keeps them one thing.
    #[test]
    fn an_empty_section_deserializes_to_the_default_config() {
        let cfg = DatagenSourceConfig::from_component_config(&section("  {}\n")).unwrap();
        assert_eq!(cfg, DatagenSourceConfig::default());
        assert_eq!(cfg.partitions, 4);
        assert_eq!(cfg.tick_interval, Duration::from_millis(100));
        assert_eq!(cfg.events_per_tick, 10);
        assert_eq!(cfg.epoch_ms, 1_767_225_600_000);
        assert!(cfg.count.is_none());
    }

    #[test]
    fn every_key_parses() {
        let cfg = DatagenSourceConfig::from_component_config(&section(
            "  dataset: storefront\n  encoding: json\n  partitions: 2\n  seed: 99\n  \
             tick_interval: 250ms\n  events_per_tick: 32\n  count: 1000\n  clock: wall\n  \
             epoch_ms: 42\n",
        ))
        .unwrap();
        assert_eq!(cfg.partitions, 2);
        assert_eq!(cfg.seed, 99);
        assert_eq!(cfg.tick_interval, Duration::from_millis(250));
        assert_eq!(cfg.events_per_tick, 32);
        assert_eq!(cfg.count, Some(1000));
        assert_eq!(cfg.clock, Clock::Wall);
        assert_eq!(cfg.epoch_ms, 42);
    }

    #[test]
    fn zero_tick_interval_is_the_unthrottled_spelling_and_is_accepted() {
        let cfg =
            DatagenSourceConfig::from_component_config(&section("  tick_interval: 0s\n")).unwrap();
        assert!(cfg.tick_interval.is_zero());
    }

    #[test]
    fn degenerate_values_are_rejected() {
        for (body, wanted) in [
            ("  partitions: 0\n", "partitions"),
            ("  partitions: 1025\n", "partitions"),
            ("  events_per_tick: 0\n", "events_per_tick"),
            ("  count: 0\n", "count"),
            ("  partitions: 8\n  count: 4\n", "count"),
        ] {
            let err = DatagenSourceConfig::from_component_config(&section(body))
                .expect_err("must reject: {body}");
            assert!(err.to_string().contains(wanted), "{err}");
        }
    }

    /// The message has to say what the arithmetic did, or "count 4 with 8
    /// partitions" reads like an arbitrary refusal.
    #[test]
    fn the_short_count_message_spells_out_the_split() {
        let err =
            DatagenSourceConfig::from_component_config(&section("  partitions: 8\n  count: 4\n"))
                .unwrap_err()
                .to_string();
        assert!(err.contains("count / partitions"), "{err}");
        assert!(err.contains("4 lane(s) would be born exhausted"), "{err}");
    }

    #[test]
    fn unknown_keys_and_unknown_variants_are_rejected() {
        for body in [
            "  rate: 1000\n",
            "  fields:\n    id: int\n",
            "  dataset: auctions\n",
            "  encoding: protobuf\n",
            "  clock: monotonic\n",
        ] {
            assert!(
                DatagenSourceConfig::from_component_config(&section(body)).is_err(),
                "must reject: {body}"
            );
        }
    }

    #[test]
    fn avro_is_accepted_only_when_the_feature_is_on() {
        let parsed = DatagenSourceConfig::from_component_config(&section("  encoding: avro\n"));
        if cfg!(feature = "avro") {
            assert_eq!(parsed.unwrap().encoding, Encoding::Avro);
        } else {
            let err = parsed.unwrap_err().to_string();
            assert!(err.contains("avro"), "{err}");
        }
    }

    #[test]
    fn budgets_sum_to_count_and_differ_by_at_most_one() {
        assert!(DatagenSourceConfig::default().budgets().is_none());
        for (partitions, count) in [(4, 100), (4, 101), (3, 10), (1, 7), (7, 7)] {
            let cfg = DatagenSourceConfig {
                partitions,
                count: Some(count),
                ..DatagenSourceConfig::default()
            };
            cfg.validate().unwrap();
            let budgets = cfg.budgets().unwrap();
            assert_eq!(budgets.len(), partitions as usize);
            assert_eq!(budgets.iter().sum::<u64>(), count, "{partitions}/{count}");
            let (lo, hi) = (
                *budgets.iter().min().unwrap(),
                *budgets.iter().max().unwrap(),
            );
            assert!(hi - lo <= 1, "{budgets:?} is not an even split");
        }
    }

    #[test]
    fn lane_seeds_are_distinct_and_follow_the_configured_seed() {
        let cfg = DatagenSourceConfig {
            seed: 5,
            ..DatagenSourceConfig::default()
        };
        let seeds: Vec<_> = (0..8).map(|i| cfg.lane_seed(i)).collect();
        assert_eq!(seeds[0], 5, "lane 0 is the configured seed itself");
        let unique: std::collections::BTreeSet<_> = seeds.iter().collect();
        assert_eq!(unique.len(), seeds.len(), "lane seeds collide: {seeds:?}");

        let other = DatagenSourceConfig {
            seed: 6,
            ..DatagenSourceConfig::default()
        };
        assert_ne!(cfg.lane_seed(3), other.lane_seed(3));
    }
}
