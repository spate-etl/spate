//! The control plane: a single consumer whose partitions fan out to lanes.
//!
//! # Rebalance choreography (spike-verified, deferred completion)
//!
//! librdkafka runs rebalance callbacks inside `poll()` on the thread that
//! calls it — here, the runtime controller calling
//! [`Source::poll_events`]. The callback ([`SourceContext::rebalance`])
//! only records an intent and returns without acknowledging, which leaves
//! the rebalance legally in progress until we call `assign`/`unassign`.
//! Completion then happens on the controller thread, interleaved with the
//! runtime's own drain choreography:
//!
//! **Assignment** (all inside one `poll_events` call):
//! 1. `assign(tpl)` — accept the partitions;
//! 2. `pause(tpl)` immediately — no fetch may complete before the split,
//!    so no message can leak onto the main queue;
//! 3. `split_partition_queue` per partition (must be redone after *every*
//!    assign — assign deactivates existing queues) and build lanes;
//! 4. `resume(tpl)` — messages start flowing into the split queues, which
//!    buffer until pipeline threads take the lanes over;
//! 5. return [`SourceEvent::LanesAssigned`].
//!
//! **Revocation** (spans two `poll_events` calls):
//! 1. surface [`SourceEvent::LanesRevoked`] with a [`DrainBarrier`] sized
//!    by lane count (the runtime's drivers arrive once per stopped lane);
//! 2. the runtime stops the lanes, waits for the barrier, drains the
//!    checkpointer, calls [`Source::commit`] + [`Source::flush_commits`] —
//!    the sync commit happens while this member still owns the partitions
//!    (the rebalance is not yet acknowledged, so the group generation is
//!    still valid);
//! 3. the controller loops back into `poll_events`, which sees the pending
//!    completion and calls `unassign()`, letting the rebalance finish.
//!
//! Revoked lanes' queues go silent immediately (fetching stops); dropping
//! a `PartitionQueue` before `unassign` would restore forwarding to the
//! main queue, which is why any message that ever appears on the main
//! queue is defensively rewound with `seek` rather than dropped — its
//! offset would otherwise be committed past without processing.

use crate::config::KafkaSourceConfig;
use crate::context::{Intent, SourceContext};
use crate::lane::KafkaLane;
use crate::metrics::KafkaStatsMetrics;
use etl_core::checkpoint::AckIssuer;
use etl_core::error::{ErrorClass, SourceError};
use etl_core::metrics::SourceMetrics;
use etl_core::record::PartitionId;
use etl_core::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::statistics::Statistics;
use rdkafka::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Kafka source: one consumer-group member per process, partitions split
/// into per-lane queues polled by pipeline threads. Constructed from
/// config ([`KafkaSource::new`]) or a pipeline component section
/// ([`KafkaSource::from_component_config`]).
pub struct KafkaSource {
    config: KafkaSourceConfig,
    consumer: Option<Arc<BaseConsumer<SourceContext>>>,
    issuer: Option<AckIssuer>,
    /// The framework's source-stage handles, shared by the runtime at `open`.
    /// Only consumer lag is published through them — the runtime records
    /// everything else. `None` when the source is driven outside a pipeline.
    metrics: Option<Arc<SourceMetrics>>,
    /// Connector-owned `etl_kafka_source_*` families, resolved from the
    /// runtime-minted Meter at `open`. `None` when the runtime provides no
    /// Meter (e.g. the source is driven outside a pipeline).
    stats_metrics: Option<KafkaStatsMetrics>,
    /// Lanes of the current assignment, by id.
    assignment: HashMap<LaneId, i32>,
    /// Lanes surfaced as revoked but not yet released by `unassign`. The
    /// member still owns these partitions (the rebalance is not acknowledged
    /// until `unassign`), so the post-drain final commit must still store
    /// their offsets — `commit` consults this alongside `assignment`. Cleared
    /// when `unassign` completes the revocation.
    revoking: HashMap<LaneId, i32>,
    next_lane: u32,
    opened_at: Option<Instant>,
    saw_first_assignment: bool,
    /// A revocation was surfaced; `unassign` completes it on the next
    /// `poll_events` call (after the runtime finished drain + commit).
    pending_unassign: bool,
    /// Messages that leaked onto the main queue and were rewound.
    main_queue_rewinds: u64,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topic", &self.config.topic)
            .field("group_id", &self.config.group_id)
            .field("lanes", &self.assignment.len())
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Create a source from validated configuration.
    #[must_use]
    pub fn new(config: KafkaSourceConfig) -> Self {
        KafkaSource {
            config,
            consumer: None,
            issuer: None,
            metrics: None,
            stats_metrics: None,
            assignment: HashMap::new(),
            revoking: HashMap::new(),
            next_lane: 0,
            opened_at: None,
            saw_first_assignment: false,
            pending_unassign: false,
            main_queue_rewinds: 0,
        }
    }

    /// Create a source from the pipeline's opaque `source: { kafka: ... }`
    /// section.
    pub fn from_component_config(
        section: &etl_core::config::ComponentConfig,
    ) -> Result<Self, etl_core::config::ConfigError> {
        Ok(Self::new(KafkaSourceConfig::from_component_config(
            section,
        )?))
    }

    fn consumer(&self) -> Result<&Arc<BaseConsumer<SourceContext>>, SourceError> {
        self.consumer.as_ref().ok_or_else(|| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: "source used before open()".into(),
        })
    }

    fn tpl_for(&self, partitions: impl IntoIterator<Item = i32>) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for p in partitions {
            tpl.add_partition(&self.config.topic, p);
        }
        tpl
    }

    fn lanes_tpl(&self, lanes: &[LaneId]) -> TopicPartitionList {
        self.tpl_for(lanes.iter().filter_map(|l| self.assignment.get(l).copied()))
    }

    /// Partitions still owned (the current assignment), as `PartitionId`s.
    /// Partitions librdkafka reports outside this set belong to another
    /// member now, so their lag is neither published nor left standing.
    fn retained_partition_ids(&self) -> Vec<PartitionId> {
        self.assignment
            .values()
            .filter_map(|p| u32::try_from(*p).ok().map(PartitionId))
            .collect()
    }

    /// Zero and drop the lag series for partitions this member lost in the
    /// rebalance that just completed.
    ///
    /// Called on `Intent::Assign` — once the new assignment is known — and
    /// never on `Intent::Revoke`. Under eager rebalancing a revoke covers
    /// *every* partition, including the ones about to be handed straight
    /// back, so pruning there would zero the whole family on every rebalance
    /// and read as a phantom drain. It would also blank the partitions the
    /// runtime is still draining and committing.
    fn prune_lag_series(&self) {
        if let Some(m) = &self.metrics {
            m.retain_partitions(&self.retained_partition_ids());
        }
    }

    /// Partitions whose offsets this member may still store: the live
    /// assignment plus partitions being revoked but not yet released by
    /// `unassign` (ownership stays valid until the rebalance is acknowledged).
    fn committable_partitions(&self) -> Vec<i32> {
        self.assignment
            .values()
            .chain(self.revoking.values())
            .copied()
            .collect()
    }

    /// Accept an assignment: assign → pause → split → resume → lanes.
    fn accept_assignment(
        &mut self,
        tpl: &TopicPartitionList,
    ) -> Result<Vec<KafkaLane>, SourceError> {
        let consumer = Arc::clone(self.consumer()?);
        let issuer = self.issuer.as_ref().ok_or_else(|| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: "assignment before open()".into(),
        })?;

        consumer.assign(tpl).map_err(fatal("assign"))?;
        // Pause before any fetch can complete: prevents pre-split messages
        // from reaching the main queue (spike-verified choreography).
        consumer.pause(tpl).map_err(fatal("pause new assignment"))?;

        let mut lanes = Vec::new();
        for elem in tpl.elements() {
            let partition = elem.partition();
            let queue = consumer
                .split_partition_queue(&self.config.topic, partition)
                .ok_or_else(|| SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("no queue for assigned partition {partition}"),
                })?;
            let lane_id = LaneId(self.next_lane);
            self.next_lane += 1;
            self.assignment.insert(lane_id, partition);
            lanes.push(KafkaLane::new(
                lane_id,
                PartitionId(u32::try_from(partition).unwrap_or(0)),
                queue,
                issuer.clone(),
            ));
        }
        consumer
            .resume(tpl)
            .map_err(fatal("resume new assignment"))?;
        self.saw_first_assignment = true;
        tracing::info!(
            partitions = lanes.len(),
            topic = %self.config.topic,
            "accepted assignment"
        );
        Ok(lanes)
    }

    /// Feed the latest librdkafka statistics into the framework lag metrics
    /// and the connector-owned `etl_kafka_source_*` families.
    fn publish_stats(&mut self) {
        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        let Some(stats) = consumer.context().stats.lock().expect("stats lock").take() else {
            return;
        };
        if let Some(metrics) = self.metrics.as_ref() {
            publish_lag(
                &stats,
                &self.config.topic,
                &self.retained_partition_ids(),
                metrics,
            );
        }
        if let Some(stats_metrics) = self.stats_metrics.as_mut() {
            stats_metrics.update(&stats, &self.config.topic);
        }
    }
}

/// Translate one statistics snapshot into the framework's per-partition
/// consumer-lag series.
///
/// Free function rather than a method so it is reachable from a unit test:
/// `publish_stats` needs a live consumer, which is why this translation went
/// untested long enough to render a permanent zero.
///
/// librdkafka reports `consumer_lag = -1` while the lag is unknown — before
/// the first commit, and for any partition whose leader has not answered yet
/// (`consumer_lag` is `(hi_offset or ls_offset) - committed_offset`; see the
/// librdkafka `STATISTICS.md`). Those partitions are skipped rather than
/// published as `0`, which would be indistinguishable from "caught up": a
/// maximally backlogged consumer would report no lag and every alert keyed on
/// it would stay green. A partition that has never reported a number is
/// therefore absent from the exposition, and one that reported before keeps
/// its last value.
///
/// `owned` restricts publication to the live assignment. The snapshot carries
/// every partition the client holds metadata for, so without this filter a
/// partition that moved to another member would keep being refreshed here and
/// a `sum` across the family would exceed *this member's* backlog. The filter
/// is one half of that; the other is
/// [`SourceMetrics::retain_partitions`](etl_core::metrics::SourceMetrics::retain_partitions),
/// which zeroes what the member lost — the exporter cannot delete a series,
/// so a partition left alone renders its last value forever.
fn publish_lag(stats: &Statistics, topic: &str, owned: &[PartitionId], metrics: &SourceMetrics) {
    let Some(topic) = stats.topics.get(topic) else {
        return;
    };
    for (pid, p) in &topic.partitions {
        if p.consumer_lag >= 0
            && let Ok(part) = u32::try_from(*pid)
            && owned.contains(&PartitionId(part))
        {
            metrics.set_partition_lag(
                PartitionId(part),
                u64::try_from(p.consumer_lag).unwrap_or(0),
            );
        }
    }
}

fn fatal(what: &'static str) -> impl Fn(rdkafka::error::KafkaError) -> SourceError {
    move |e| SourceError::Client {
        class: ErrorClass::Fatal,
        reason: format!("{what}: {e}"),
    }
}

impl Source for KafkaSource {
    type Lane = KafkaLane;

    fn component_type(&self) -> &str {
        "kafka"
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        if self.consumer.is_some() {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "open() called twice".into(),
            });
        }
        // Enforce the passthrough guard (and the whole denylist) before any
        // client is created — the sink's choke point is `build()`; `open()` is
        // the source's, catching programmatic construction via
        // `KafkaSource::new` that bypasses `from_component_config`'s validation.
        self.config.validate().map_err(|e| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: e.to_string(),
        })?;
        // Resolve the connector-owned metric handles once, before the poll
        // loop — but only when statistics are enabled. With
        // `statistics_interval: 0s` librdkafka never emits a snapshot, so
        // registering the families would leave them frozen at their unset
        // default forever (e.g. `group_healthy 0`, a documented alert
        // signal) — disabling statistics must disable the families with
        // them. `absolute()`-mapped counters are scoped to this consumer's
        // lifetime — sound because open() creates the consumer exactly once
        // (see the `metrics` module docs).
        self.metrics = ctx.stage_metrics.clone();
        self.stats_metrics = if self.config.statistics_interval.is_zero() {
            // Consumer lag is derived from the statistics snapshot and has no
            // other source, so disabling statistics removes a golden signal
            // outright. That is the honest outcome — the series is absent
            // rather than frozen at a `0` that reads as "caught up" — but it
            // must not be silent.
            tracing::warn!(
                topic = %self.config.topic,
                "statistics disabled (statistics_interval: 0s): consumer lag \
                 and the etl_kafka_source_* families will not be published"
            );
            None
        } else {
            ctx.meter
                .as_ref()
                .map(|m| KafkaStatsMetrics::new(m.clone(), ctx.per_partition_detail))
        };
        let consumer: BaseConsumer<SourceContext> = self
            .config
            .client_config()
            .create_with_context(SourceContext::default())
            .map_err(fatal("create consumer"))?;
        consumer
            .subscribe(&[&self.config.topic])
            .map_err(fatal("subscribe"))?;
        self.consumer = Some(Arc::new(consumer));
        self.issuer = Some(ctx.issuer);
        self.opened_at = Some(Instant::now());
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<KafkaLane>, SourceError> {
        // Startup deadline first: with unreachable brokers every poll below
        // surfaces a Retryable transport error and returns early — checked
        // last, this deadline would never fire and a misconfigured pipeline
        // would retry forever instead of failing fast.
        if !self.saw_first_assignment
            && let Some(at) = self.opened_at
            && at.elapsed() > self.config.startup_timeout
        {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "no partition assignment within {:?} (topic {:?}, brokers {:?})",
                    self.config.startup_timeout, self.config.topic, self.config.brokers
                ),
            });
        }

        // Complete a deferred revocation first: the runtime has finished
        // draining and committing by the time it calls poll_events again.
        if self.pending_unassign {
            self.pending_unassign = false;
            let consumer = Arc::clone(self.consumer()?);
            if let Err(e) = consumer.unassign() {
                tracing::warn!(error = %e, "unassign after drained revocation");
            }
            // The revoked partitions are now released; any late commit for
            // them must be refused again.
            self.revoking.clear();
        }

        let consumer = Arc::clone(self.consumer()?);

        // Serve callbacks; with all partitions split and choreographed
        // correctly no message should ever surface here. If one does,
        // rewind so it is refetched through its split queue — dropping it
        // would let the watermark commit past an unprocessed record.
        if let Some(result) = consumer.poll(timeout) {
            match result {
                Ok(msg) => {
                    self.main_queue_rewinds += 1;
                    tracing::warn!(
                        partition = msg.partition(),
                        offset = msg.offset(),
                        total = self.main_queue_rewinds,
                        "message on the main queue; rewinding partition"
                    );
                    let tpl = self.tpl_for([msg.partition()]);
                    let _ = consumer.pause(&tpl);
                    if let Err(e) = consumer.seek(
                        &self.config.topic,
                        msg.partition(),
                        Offset::Offset(msg.offset()),
                        Duration::from_secs(5),
                    ) {
                        tracing::error!(error = %e, "seek for main-queue rewind failed");
                    }
                    let _ = consumer.resume(&tpl);
                }
                Err(e) => {
                    // Permanent broker-side failures (authorization revoked,
                    // deleted topic, unsupported protocol) must fail fast
                    // rather than retry forever behind a green health probe.
                    return Err(SourceError::Client {
                        class: crate::error::classify_poll_error(&e, self.saw_first_assignment),
                        reason: format!("consumer poll: {e}"),
                    });
                }
            }
        }

        self.publish_stats();

        // Rebalance intents recorded by the callback during the poll above
        // (or a previous one).
        let intent = {
            let ctx = self.consumer()?.context().clone();
            let mut intents = ctx.intents.lock().expect("intent lock");
            intents.pop_front()
        };
        if let Some(intent) = intent {
            match intent {
                Intent::Assign(tpl) => {
                    if tpl.count() == 0 {
                        // Empty assignment (no partitions for this member).
                        // The rebalance protocol still MUST be acknowledged:
                        // under deferred completion librdkafka keeps the
                        // rebalance in progress until we call `assign`, even
                        // for an empty set. Skipping it wedges the member —
                        // it can never complete a later rebalance.
                        let consumer = Arc::clone(self.consumer()?);
                        consumer.assign(&tpl).map_err(fatal("assign empty"))?;
                        self.saw_first_assignment = true;
                        self.prune_lag_series();
                        return Ok(SourceEvent::Idle);
                    }
                    let lanes = self.accept_assignment(&tpl)?;
                    self.prune_lag_series();
                    return Ok(SourceEvent::LanesAssigned(lanes));
                }
                Intent::Revoke(tpl) => {
                    // Map revoked partitions back to lane ids.
                    let revoked: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                    let lanes: Vec<LaneId> = self
                        .assignment
                        .iter()
                        .filter(|(_, p)| revoked.contains(p))
                        .map(|(l, _)| *l)
                        .collect();
                    // Move revoked lanes out of the live assignment but keep
                    // them in `revoking`: the member still owns these
                    // partitions until `unassign`, so the runtime's post-drain
                    // final commit must be allowed to store their offsets.
                    // `commit` consults `revoking`; the next `poll_events`
                    // clears it once `unassign` releases the partitions.
                    for lane in &lanes {
                        if let Some(p) = self.assignment.remove(lane) {
                            self.revoking.insert(*lane, p);
                        }
                    }
                    // The lag series are deliberately NOT pruned here. These
                    // partitions are still being drained and committed, and
                    // an eager rebalance revokes everything before handing
                    // most of it back — zeroing now would blank the whole
                    // family for a rebalance that changed nothing. The prune
                    // happens once the new assignment is known, in
                    // `Intent::Assign`.
                    //
                    // Complete with unassign on the next call, after the
                    // runtime drained and committed.
                    self.pending_unassign = true;
                    if lanes.is_empty() {
                        return Ok(SourceEvent::Idle);
                    }
                    let barrier = DrainBarrier::new(lanes.len());
                    return Ok(SourceEvent::LanesRevoked { lanes, barrier });
                }
                Intent::Error(reason) => {
                    return Err(SourceError::Client {
                        class: ErrorClass::Retryable,
                        reason: format!("rebalance error: {reason}"),
                    });
                }
            }
        }

        Ok(SourceEvent::Idle)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        if watermarks.is_empty() {
            return Ok(());
        }
        let consumer = Arc::clone(self.consumer()?);
        // Partitions this member still owns: the live assignment plus any
        // being revoked but not yet released by `unassign`. The revocation
        // choreography drains and commits those partitions while ownership is
        // still valid — filtering them out here would silently drop exactly
        // the offsets the drain produced, replaying that work after the move.
        let owned = self.committable_partitions();
        let mut tpl = TopicPartitionList::new();
        for (p, offset) in watermarks {
            let partition = i32::try_from(p.0).unwrap_or(-1);
            if owned.contains(&partition) {
                tpl.add_partition_offset(&self.config.topic, partition, Offset::Offset(*offset))
                    .map_err(fatal("build offset list"))?;
            } else {
                tracing::debug!(
                    partition = p.0,
                    offset,
                    "skipping store for partition no longer owned"
                );
            }
        }
        if tpl.count() == 0 {
            return Ok(());
        }
        consumer
            .store_offsets(&tpl)
            .map_err(|e| SourceError::Client {
                class: ErrorClass::Retryable,
                reason: format!("store offsets: {e}"),
            })
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        let consumer = Arc::clone(self.consumer()?);
        match consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
            Ok(()) => Ok(()),
            // Nothing stored since the last commit: not an error.
            Err(rdkafka::error::KafkaError::ConsumerCommit(
                rdkafka::error::RDKafkaErrorCode::NoOffset,
            )) => Ok(()),
            Err(e) => Err(SourceError::Client {
                class: ErrorClass::Retryable,
                reason: format!("sync commit: {e}"),
            }),
        }
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let tpl = self.lanes_tpl(lanes);
        if tpl.count() == 0 {
            return Ok(());
        }
        self.consumer()?
            .pause(&tpl)
            .map_err(|e| SourceError::Client {
                class: ErrorClass::Retryable,
                reason: format!("pause: {e}"),
            })
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let tpl = self.lanes_tpl(lanes);
        if tpl.count() == 0 {
            return Ok(());
        }
        self.consumer()?
            .resume(&tpl)
            .map_err(|e| SourceError::Client {
                class: ErrorClass::Retryable,
                reason: format!("resume: {e}"),
            })
    }
}

/// Teardown: consumer close (inside `BaseConsumer::drop`) triggers a final
/// revoke and then polls until the rebalance protocol completes — with the
/// deferred-intent design, nothing would ever complete it and the drop
/// would hang forever. Flip the context to inline-completion mode and
/// settle any revocation that was surfaced but not yet acknowledged.
impl Drop for KafkaSource {
    fn drop(&mut self) {
        if let Some(consumer) = &self.consumer {
            consumer
                .context()
                .closing
                .store(true, std::sync::atomic::Ordering::Release);
            let deferred_revoke = self.pending_unassign
                || consumer
                    .context()
                    .intents
                    .lock()
                    .map(|q| q.iter().any(|i| matches!(i, Intent::Revoke(_))))
                    .unwrap_or(false);
            if deferred_revoke && let Err(e) = consumer.unassign() {
                tracing::warn!(error = %e, "unassign during source teardown failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> KafkaSourceConfig {
        KafkaSourceConfig {
            brokers: "localhost:9092".into(),
            topic: "orders".into(),
            group_id: "test".into(),
            commit_interval: Duration::from_secs(5),
            startup_timeout: Duration::from_secs(30),
            statistics_interval: Duration::ZERO,
            rdkafka: std::collections::BTreeMap::new(),
        }
    }

    /// `open()` runs the TLS/SASL guard before creating the consumer, so a
    /// source built programmatically via `new()` — bypassing
    /// `from_component_config`'s config-load validation — still fails fast with
    /// the actionable message instead of a late librdkafka error. Without the
    /// `tls` feature the guard rejects a security passthrough before any client
    /// (or broker contact); with it the guard is a no-op and the lazily
    /// connecting consumer is created without touching a broker.
    #[test]
    fn open_enforces_tls_guard_on_programmatic_source() {
        use etl_core::checkpoint::Checkpointer;
        let mut config = test_config();
        config
            .rdkafka
            .insert("security.protocol".into(), "ssl".into());
        let mut source = KafkaSource::new(config);
        let cp = Checkpointer::new();
        let result = source.open(SourceCtx::new(cp.handle()));
        if cfg!(feature = "tls") {
            result.expect("tls build: open succeeds");
        } else {
            let err = result.expect_err("non-tls build: open rejects the security config");
            assert!(err.to_string().contains("kafka-tls"), "actionable: {err}");
        }
    }

    /// Reproduces the assignment bookkeeping of a partial revocation: lanes
    /// for the revoked partitions move from `assignment` into `revoking`.
    fn revoke_lanes(source: &mut KafkaSource, revoked: &[i32]) {
        let lanes: Vec<LaneId> = source
            .assignment
            .iter()
            .filter(|(_, p)| revoked.contains(p))
            .map(|(l, _)| *l)
            .collect();
        for lane in &lanes {
            if let Some(p) = source.assignment.remove(lane) {
                source.revoking.insert(*lane, p);
            }
        }
    }

    /// After a revocation the offsets of the partitions being revoked must
    /// still be committable — they are drained and committed while the member
    /// still owns them — while truly unowned partitions stay filtered out.
    #[test]
    fn committable_partitions_include_revoking_until_released() {
        let mut source = KafkaSource::new(test_config());
        for (lane, part) in [(0u32, 0i32), (1, 1), (2, 2), (3, 3)] {
            source.assignment.insert(LaneId(lane), part);
        }

        revoke_lanes(&mut source, &[2, 3]);

        let mut owned = source.committable_partitions();
        owned.sort_unstable();
        assert_eq!(
            owned,
            vec![0, 1, 2, 3],
            "revoked partitions stay committable until unassign releases them"
        );

        // Releasing the revocation (what `unassign` completion does) removes
        // them: a late commit for a released partition is refused.
        source.revoking.clear();
        let mut owned = source.committable_partitions();
        owned.sort_unstable();
        assert_eq!(owned, vec![0, 1]);
    }

    /// The retained set that prunes per-partition metric series must exclude
    /// revoked partitions, so the prune can zero the lag they left behind.
    #[test]
    fn retained_partition_ids_drop_revoked_partitions() {
        let mut source = KafkaSource::new(test_config());
        for (lane, part) in [(0u32, 0i32), (1, 1), (2, 2)] {
            source.assignment.insert(LaneId(lane), part);
        }

        revoke_lanes(&mut source, &[2]);

        let mut kept: Vec<u32> = source
            .retained_partition_ids()
            .iter()
            .map(|p| p.0)
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![0, 1], "revoked partition 2 is not retained");
    }

    mod lag {
        use super::*;
        use etl_core::metrics::ComponentLabels;
        use rdkafka::statistics::{Partition, Topic};
        use std::collections::HashMap;

        const STD: &str = r#"pipeline="orders",component="source",component_type="kafka""#;

        /// Run `f` against a local Prometheus recorder and return the rendered
        /// exposition. Handles must be resolved inside `f`.
        fn render(f: impl FnOnce(&SourceMetrics)) -> String {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || {
                let m = SourceMetrics::new(&ComponentLabels::new("orders", "source", "kafka"));
                f(&m);
            });
            handle.run_upkeep();
            handle.render()
        }

        /// `(partition, consumer_lag)` pairs into a snapshot for `orders`.
        fn stats(parts: &[(i32, i64)]) -> Statistics {
            Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    Topic {
                        topic: "orders".to_owned(),
                        partitions: parts
                            .iter()
                            .map(|&(pid, consumer_lag)| {
                                (
                                    pid,
                                    Partition {
                                        partition: pid,
                                        consumer_lag,
                                        ..Default::default()
                                    },
                                )
                            })
                            .collect(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }
        }

        /// The regression this pins: a maximally backlogged consumer must publish
        /// its backlog, per partition, at full magnitude.
        #[test]
        fn a_large_backlog_publishes_per_partition_lag() {
            let rendered = render(|m| {
                publish_lag(
                    &stats(&[(0, 150_000_000), (1, 90_000_000)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(
                rendered.contains(&format!(
                    r#"etl_source_lag_records{{{STD},partition="0"}} 150000000"#
                )),
                "backlogged partition must report its lag:\n{rendered}"
            );
            assert!(
                rendered.contains(&format!(
                    r#"etl_source_lag_records{{{STD},partition="1"}} 90000000"#
                )),
                "every owned partition gets its own series:\n{rendered}"
            );
        }

        /// There is no aggregate series: readers aggregate in the query layer.
        /// An unlabelled series sharing this family name would make
        /// `sum(etl_source_lag_records)` double-count.
        #[test]
        fn no_unlabelled_aggregate_series_is_published() {
            let rendered = render(|m| {
                publish_lag(
                    &stats(&[(0, 17), (1, 4)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            let unlabelled = rendered
                .lines()
                .filter(|l| l.starts_with("etl_source_lag_records{"))
                .any(|l| !l.contains("partition="));
            assert!(
                !unlabelled,
                "every lag series must carry a partition label:\n{rendered}"
            );
        }

        /// `consumer_lag = -1` means "not measured yet" — before the first
        /// commit, or before the partition leader has answered. Publishing it
        /// as `0` would read as "caught up" on exactly the consumer that is
        /// most behind.
        #[test]
        fn unknown_lag_registers_no_series() {
            let rendered = render(|m| {
                publish_lag(
                    &stats(&[(0, -1), (1, -1)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(
                !rendered.contains("etl_source_lag_records"),
                "an all-unknown snapshot must publish nothing:\n{rendered}"
            );
        }

        /// A mixed snapshot publishes the partitions that have a number and
        /// stays silent about the rest, rather than dragging the unknown ones
        /// to zero.
        #[test]
        fn mixed_snapshot_publishes_only_known_partitions() {
            let rendered = render(|m| {
                publish_lag(
                    &stats(&[(0, 4_200), (1, -1)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(rendered.contains(&format!(
                r#"etl_source_lag_records{{{STD},partition="0"}} 4200"#
            )));
            assert!(
                !rendered.contains(r#"partition="1""#),
                "unknown partition must be absent:\n{rendered}"
            );
        }

        /// Once measured, a partition holds its last value through snapshots
        /// where librdkafka temporarily reports the lag as unknown (a leader
        /// change, say) — dropping to `0` would look like a drain that never
        /// happened.
        #[test]
        fn a_known_partition_holds_its_value_when_lag_goes_unknown() {
            let rendered = render(|m| {
                publish_lag(&stats(&[(0, 5_000)]), "orders", &[PartitionId(0)], m);
                publish_lag(&stats(&[(0, -1)]), "orders", &[PartitionId(0)], m);
            });
            assert!(
                rendered.contains(&format!(
                    r#"etl_source_lag_records{{{STD},partition="0"}} 5000"#
                )),
                "last known value is held:\n{rendered}"
            );
        }

        /// A snapshot for a different topic must not publish anything: the
        /// source owns exactly one topic.
        #[test]
        fn a_snapshot_without_our_topic_publishes_nothing() {
            let rendered = render(|m| {
                publish_lag(&stats(&[(0, 900)]), "other-topic", &[PartitionId(0)], m);
            });
            assert!(
                !rendered.contains("etl_source_lag_records"),
                "wrong topic must publish nothing:\n{rendered}"
            );
        }

        /// A partition that moved to another member must contribute nothing
        /// to this member's total, or every reader that sums across
        /// partitions double-counts it.
        ///
        /// The exporter has no deletion and no idle timeout is configured, so
        /// "contribute nothing" cannot mean "disappear" — the series renders
        /// for the life of the process whatever we do with the handle. It
        /// means `0`, which is the truth for a partition this member no
        /// longer owns. Both halves are asserted: the value is zeroed at the
        /// prune, and the ownership filter keeps later snapshots from
        /// reviving it.
        #[test]
        fn revoked_partitions_zero_out_and_stop_updating() {
            let rendered = render(|m| {
                // Both owned.
                publish_lag(
                    &stats(&[(0, 11), (1, 22)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
                // Partition 1 is revoked: it leaves the owned set and the
                // prune zeroes it. librdkafka keeps reporting it in the
                // snapshot for a while, so the filter has to hold too.
                m.retain_partitions(&[PartitionId(0)]);
                publish_lag(&stats(&[(0, 33), (1, 44)]), "orders", &[PartitionId(0)], m);
            });
            assert!(rendered.contains(&format!(
                r#"etl_source_lag_records{{{STD},partition="0"}} 33"#
            )));
            assert!(
                rendered.contains(&format!(
                    r#"etl_source_lag_records{{{STD},partition="1"}} 0"#
                )),
                "revoked partition must be zeroed, not left at its last lag:\n{rendered}"
            );
            assert!(
                !rendered.contains(r#"partition="1"} 44"#),
                "revoked partition must not resume updating:\n{rendered}"
            );
        }
    }
}
