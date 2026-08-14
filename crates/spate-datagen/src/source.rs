//! The control plane: [`DatagenSource`] hands out its lanes once, publishes
//! the gauges, and decides when a bounded run is finished.
//!
//! There is no I/O anywhere in this crate: the constructors take no runtime
//! handle and nothing here depends on `tokio`. [`DatagenSource::new`] takes a
//! validated config; [`DatagenSource::from_component_config`] reads the
//! pipeline's opaque section.

use crate::config::DatagenSourceConfig;
use crate::encode::Encoder;
use crate::lane::{DatagenLane, LaneParts, Shared, park};
use crate::metrics::DatagenMetrics;
use crate::plan::EventPlan;
use spate_core::checkpoint::AckIssuer;
use spate_core::config::{ComponentConfig, ConfigError};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::framing::FramingContract;
use spate_core::record::PartitionId;
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// What `open` builds and everything afterwards reads.
#[derive(Debug)]
struct OpenState {
    issuer: AckIssuer,
    shared: Arc<Shared>,
    encoder: Arc<Encoder>,
    metrics: Option<DatagenMetrics>,
    /// Per-lane event budgets, summing to `count`. `None` when unbounded.
    budgets: Option<Vec<u64>>,
}

/// Synthetic storefront-event source. See the crate docs for the dataset, the
/// referential-integrity mechanism, and what this source deliberately does not
/// promise.
#[derive(Debug)]
pub struct DatagenSource {
    config: DatagenSourceConfig,
    state: Option<OpenState>,
    /// The entire progress store: a map, in this process, gone at exit.
    watermarks: BTreeMap<PartitionId, i64>,
    handed_out: bool,
    drained: bool,
}

impl DatagenSource {
    /// A source over `config`.
    #[must_use]
    pub fn new(config: DatagenSourceConfig) -> DatagenSource {
        DatagenSource {
            config,
            state: None,
            watermarks: BTreeMap::new(),
            handed_out: false,
            drained: false,
        }
    }

    /// Build from the pipeline's opaque `source: { datagen: ... }` section.
    pub fn from_component_config(section: &ComponentConfig) -> Result<DatagenSource, ConfigError> {
        Ok(DatagenSource::new(
            DatagenSourceConfig::from_component_config(section)?,
        ))
    }

    /// The Avro schema the `avro` encoding writes against, as JSON. The same
    /// string as [`EVENT_SCHEMA_JSON`](crate::EVENT_SCHEMA_JSON).
    #[must_use]
    pub fn avro_schema() -> &'static str {
        crate::EVENT_SCHEMA_JSON
    }

    /// Every watermark this source has been asked to commit. In memory, and
    /// nowhere else; see the crate docs.
    #[must_use]
    pub fn committed(&self) -> &BTreeMap<PartitionId, i64> {
        &self.watermarks
    }

    fn open_state(&mut self) -> Result<&mut OpenState, SourceError> {
        self.state.as_mut().ok_or_else(|| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: "DatagenSource used before open()".into(),
        })
    }
}

impl Source for DatagenSource {
    type Lane = DatagenLane;

    fn component_type(&self) -> &str {
        "datagen"
    }

    fn framing_contract(&self) -> FramingContract {
        // One event per payload, always. The generator writes the frames, so
        // there is nothing for a deserializer to split.
        FramingContract::PerRecord
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        if self.state.is_some() {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "source opened twice".into(),
            });
        }
        // A hand-constructed config has not been through the deserializer,
        // which is where a loaded one is validated.
        self.config.validate().map_err(|e| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: e.to_string(),
        })?;

        tracing::warn!(
            "spate-datagen keeps no durable progress; every run regenerates its stream from \
             the beginning. It is a demo and test source — do not build a production pipeline \
             on it."
        );

        let partitions = self.config.partitions as usize;
        let budgets = self.config.budgets();
        self.state = Some(OpenState {
            issuer: ctx.issuer,
            shared: Arc::new(Shared::new(partitions, budgets.as_deref())),
            encoder: Arc::new(Encoder::new(self.config.encoding)?),
            metrics: ctx.meter.as_ref().map(|meter| {
                DatagenMetrics::new(meter, self.config.partitions, ctx.per_partition_detail)
            }),
            budgets,
        });
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<DatagenLane>, SourceError> {
        if !self.handed_out {
            let config = self.config.clone();
            let state = self.open_state()?;
            let lanes = (0..config.partitions)
                .map(|index| {
                    DatagenLane::new(LaneParts {
                        id: LaneId(index),
                        index: index as usize,
                        issuer: state.issuer.clone(),
                        plan: EventPlan::new(&config, index),
                        encoder: Arc::clone(&state.encoder),
                        counters: state.metrics.as_ref().map(DatagenMetrics::counters),
                        shared: Arc::clone(&state.shared),
                        budget: state
                            .budgets
                            .as_ref()
                            .map_or(u64::MAX, |b| b[index as usize]),
                        tick_interval: config.tick_interval,
                        events_per_tick: config.events_per_tick as usize,
                    })
                })
                .collect();
            // Set once the assignment exists, never before the lookup above:
            // a failure there must leave the hand-out still to be made.
            self.handed_out = true;
            return Ok(SourceEvent::LanesAssigned(lanes));
        }

        let bounded = self.config.count.is_some();
        let state = self.open_state()?;
        // The run-progress gauges are written here and nowhere else;
        // `committed_offset` is written by `commit` below. Both run on the
        // controller thread, so each series has one live writer (INV-10).
        if let Some(metrics) = &state.metrics {
            let remaining = if bounded {
                state
                    .shared
                    .remaining
                    .iter()
                    .map(|r| r.load(Ordering::Acquire))
                    .sum()
            } else {
                // An unbounded stream has no remainder. A sentinel here would
                // read as a real figure.
                0
            };
            let open = state
                .shared
                .open
                .iter()
                .map(|o| o.load(Ordering::Acquire))
                .sum();
            metrics.publish(remaining, open);
        }

        let finished = bounded
            && state
                .shared
                .exhausted
                .iter()
                .all(|e| e.load(Ordering::Acquire));
        if finished {
            // Idempotent by contract, so repeats must not spin the controller.
            if std::mem::replace(&mut self.drained, true) {
                park(timeout);
            }
            return Ok(SourceEvent::Drained);
        }

        // This source never emits `CommitReady`: nothing downstream of a
        // commit here is waiting on it.
        park(timeout);
        Ok(SourceEvent::Idle)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        for &(partition, offset) in watermarks {
            self.watermarks.insert(partition, offset);
        }
        if let Some(metrics) = self.state.as_ref().and_then(|s| s.metrics.as_ref()) {
            for &(partition, offset) in watermarks {
                metrics.set_committed(partition.0, offset);
            }
        }
        Ok(())
    }

    // `flush_commits` keeps the trait's no-op default: there is nothing to
    // flush a map to.

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        set_paused(self.state.as_ref(), lanes, true);
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        set_paused(self.state.as_ref(), lanes, false);
        Ok(())
    }
}

fn set_paused(state: Option<&OpenState>, lanes: &[LaneId], paused: bool) {
    let Some(state) = state else { return };
    for lane in lanes {
        if let Some(flag) = state.shared.paused.get(lane.0 as usize) {
            flag.store(paused, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::StorefrontEvent;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::source::{PayloadBatch, SourceLane};

    const POLL: Duration = Duration::from_millis(1);

    fn config(partitions: u32, count: Option<u64>) -> DatagenSourceConfig {
        DatagenSourceConfig {
            partitions,
            count,
            // Unthrottled: these tests are about the drain, not the cadence.
            tick_interval: Duration::ZERO,
            ..DatagenSourceConfig::default()
        }
    }

    /// Open a source and take its one assignment.
    fn start(config: DatagenSourceConfig) -> (DatagenSource, Checkpointer, Vec<DatagenLane>) {
        let cp = Checkpointer::new();
        let mut source = DatagenSource::new(config);
        source.open(SourceCtx::new(cp.handle())).unwrap();
        let SourceEvent::LanesAssigned(lanes) = source.poll_events(POLL).unwrap() else {
            panic!("the first poll assigns every lane");
        };
        (source, cp, lanes)
    }

    /// Drain a lane to exhaustion, returning every payload it produced as
    /// `(offset, key, decoded event)`.
    fn drain(lane: &mut DatagenLane, max_records: usize) -> Vec<(i64, String, StorefrontEvent)> {
        let mut out = Vec::new();
        // Bounded: a lane that never exhausts would otherwise hang the suite.
        for _ in 0..10_000 {
            let Some(mut batch) = lane.poll(max_records, POLL).unwrap() else {
                break;
            };
            while let Some(payload) = batch.next_payload() {
                out.push((
                    payload.offset,
                    String::from_utf8(payload.key.expect("keyed").to_vec()).unwrap(),
                    serde_json::from_slice(payload.bytes).unwrap(),
                ));
            }
        }
        out
    }

    #[test]
    fn the_assignment_is_one_lane_per_partition_handed_out_once() {
        let (mut source, _cp, lanes) = start(config(3, Some(30)));
        assert_eq!(lanes.len(), 3);
        for (index, lane) in lanes.iter().enumerate() {
            assert_eq!(lane.id(), LaneId(index as u32));
            assert_eq!(lane.partition(), PartitionId(index as u32));
        }
        assert!(
            matches!(source.poll_events(POLL).unwrap(), SourceEvent::Idle),
            "a second poll must not reassign"
        );
    }

    /// The budget split, observed through what the lanes release: exactly
    /// `count` events, and never more than one lane apart.
    #[test]
    fn a_bounded_run_releases_exactly_count_events() {
        for (partitions, count) in [(4u32, 100u64), (4, 101), (3, 10), (1, 9)] {
            let (_source, _cp, mut lanes) = start(config(partitions, Some(count)));
            let per_lane: Vec<usize> = lanes.iter_mut().map(|l| drain(l, 8).len()).collect();
            assert_eq!(
                per_lane.iter().sum::<usize>(),
                count as usize,
                "{partitions} lanes / {count}: {per_lane:?}"
            );
            let (lo, hi) = (
                per_lane.iter().min().unwrap(),
                per_lane.iter().max().unwrap(),
            );
            assert!(hi - lo <= 1, "uneven split {per_lane:?}");
        }
    }

    /// The drain contract: a lane's exhaustion is only declared by a `poll`
    /// that returned `Ok(None)` *after* its final batch was consumed, so
    /// `Drained` cannot be reported while data is still unemitted.
    #[test]
    fn drained_arrives_only_after_every_lane_is_polled_past_its_last_batch() {
        let (mut source, _cp, mut lanes) = start(config(2, Some(20)));

        // Release every event, but stop before the poll that finds nothing.
        let mut released = 0;
        for lane in &mut lanes {
            while let Some(mut batch) = lane.poll(4, POLL).unwrap() {
                while batch.next_payload().is_some() {
                    released += 1;
                }
                if released % 10 == 0 {
                    break;
                }
            }
        }
        assert_eq!(released, 20, "every event was handed over");
        assert!(
            matches!(source.poll_events(POLL).unwrap(), SourceEvent::Idle),
            "no lane has been polled past its last batch yet"
        );

        // The poll that proves the last batch was consumed.
        for lane in &mut lanes {
            assert!(lane.poll(4, POLL).unwrap().is_none());
        }
        assert!(matches!(
            source.poll_events(POLL).unwrap(),
            SourceEvent::Drained
        ));
        assert!(
            matches!(source.poll_events(POLL).unwrap(), SourceEvent::Drained),
            "Drained is idempotent"
        );
    }

    #[test]
    fn an_unbounded_source_never_drains() {
        let (mut source, _cp, mut lanes) = start(config(2, None));
        for lane in &mut lanes {
            assert!(lane.poll(16, POLL).unwrap().is_some());
        }
        for _ in 0..3 {
            assert!(matches!(
                source.poll_events(POLL).unwrap(),
                SourceEvent::Idle
            ));
        }
    }

    /// The routing property: an order, its payment and its refund all carry
    /// the same key, on one partition, at increasing offsets.
    #[test]
    fn payloads_are_keyed_by_order_id_at_monotonic_offsets() {
        let (_source, _cp, mut lanes) = start(config(2, Some(4_000)));
        let payloads = drain(&mut lanes[1], 64);
        assert!(payloads.len() > 100);
        for (index, (offset, key, event)) in payloads.iter().enumerate() {
            assert_eq!(*offset, index as i64, "offsets are dense and monotonic");
            assert_eq!(key, &event.order_id().to_string(), "key is the order id");
            assert_eq!(event.order_id() % 2, 1, "lane 1 owns the odd id slice");
        }
    }

    #[test]
    fn a_paused_lane_yields_nothing_and_resumes_where_it_left_off() {
        let (mut source, _cp, mut lanes) = start(config(1, Some(100)));
        assert!(lanes[0].poll(4, POLL).unwrap().is_some());

        source.pause(&[LaneId(0)]).unwrap();
        assert!(lanes[0].poll(4, POLL).unwrap().is_none());

        source.resume(&[LaneId(0)]).unwrap();
        let mut batch = lanes[0].poll(4, POLL).unwrap().expect("resumed");
        let first = batch.next_payload().expect("a payload").offset;
        assert_eq!(first, 4, "the lane continued rather than restarting");
    }

    /// Watermarks go into a map and nowhere else, and a fresh source over the
    /// same configuration starts from nothing.
    #[test]
    fn commits_are_kept_in_memory_only() {
        let (mut source, _cp, _lanes) = start(config(2, Some(20)));
        source
            .commit(&[(PartitionId(0), 5), (PartitionId(1), 7)])
            .unwrap();
        source.commit(&[(PartitionId(0), 9)]).unwrap();
        assert_eq!(source.committed()[&PartitionId(0)], 9);
        assert_eq!(source.committed()[&PartitionId(1)], 7);

        // A fresh source over the same configuration knows nothing.
        let (restarted, _cp, _lanes) = start(config(2, Some(20)));
        assert!(restarted.committed().is_empty());
    }

    #[test]
    fn the_declared_contracts_are_the_ones_the_runtime_reads() {
        let source = DatagenSource::new(config(1, None));
        assert_eq!(source.component_type(), "datagen");
        assert_eq!(source.framing_contract(), FramingContract::PerRecord);
        assert_eq!(DatagenSource::avro_schema(), crate::EVENT_SCHEMA_JSON);
    }

    #[test]
    fn opening_twice_is_refused() {
        let cp = Checkpointer::new();
        let mut source = DatagenSource::new(config(1, None));
        source.open(SourceCtx::new(cp.handle())).unwrap();
        assert!(source.open(SourceCtx::new(cp.handle())).is_err());
    }

    /// A throttled lane releases its quota and then parks, rather than
    /// draining its whole budget on the first poll.
    #[test]
    fn the_rate_gate_releases_one_quota_per_cadence() {
        let cfg = DatagenSourceConfig {
            partitions: 1,
            count: Some(100),
            tick_interval: Duration::from_millis(50),
            events_per_tick: 3,
            ..DatagenSourceConfig::default()
        };
        let (_source, _cp, mut lanes) = start(cfg);
        let mut batch = lanes[0].poll(64, POLL).unwrap().expect("the first tick");
        let mut released = 0;
        while batch.next_payload().is_some() {
            released += 1;
        }
        assert_eq!(
            released, 3,
            "a tick releases events_per_tick, not the budget"
        );
        drop(batch);
        assert!(
            lanes[0].poll(64, POLL).unwrap().is_none(),
            "the next cadence is not due yet"
        );
    }

    /// A tick's quota survives a `max_records` smaller than itself: it is
    /// spread across polls of the same cadence rather than truncated, so the
    /// documented release rate holds at any `events_per_tick`.
    #[test]
    fn a_quota_larger_than_max_records_is_released_across_polls_of_one_tick() {
        let cfg = DatagenSourceConfig {
            partitions: 1,
            count: Some(1_000),
            tick_interval: Duration::from_secs(60),
            events_per_tick: 50,
            ..DatagenSourceConfig::default()
        };
        let (_source, _cp, mut lanes) = start(cfg);

        // One cadence, polled in batches of 8. The second tick is a minute
        // out, so everything counted here belongs to the first.
        let mut released = 0;
        while let Some(mut batch) = lanes[0].poll(8, POLL).unwrap() {
            while batch.next_payload().is_some() {
                released += 1;
            }
        }
        assert_eq!(released, 50, "the whole quota, not one max_records batch");
    }

    /// `events_remaining` answers for a bounded run from the moment the lanes
    /// exist, before any of them has generated anything, when a scrape would
    /// otherwise read the finished value.
    #[test]
    fn events_remaining_reads_the_budget_before_the_first_fill() {
        let (source, _cp, _lanes) = start(config(4, Some(1_000)));
        let state = source.state.as_ref().expect("opened");
        let remaining: u64 = state
            .shared
            .remaining
            .iter()
            .map(|r| r.load(Ordering::Acquire))
            .sum();
        assert_eq!(remaining, 1_000, "no lane has filled yet");

        // An unbounded stream has no remainder to seed.
        let (unbounded, _cp, _lanes) = start(config(4, None));
        let state = unbounded.state.as_ref().expect("opened");
        assert!(
            state
                .shared
                .remaining
                .iter()
                .all(|r| r.load(Ordering::Acquire) == 0)
        );
    }
}
