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
use etl_core::checkpoint::AckIssuer;
use etl_core::error::{ErrorClass, SourceError};
use etl_core::metrics::SourceMetrics;
use etl_core::record::PartitionId;
use etl_core::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
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
    metrics: Option<SourceMetrics>,
    /// Lanes of the current assignment, by id.
    assignment: HashMap<LaneId, i32>,
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
            assignment: HashMap::new(),
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

    /// Attach pre-registered source metrics (consumer lag, rebalances).
    /// Optional; without it the source only logs.
    #[must_use]
    pub fn with_metrics(mut self, metrics: SourceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
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

    /// Feed the latest librdkafka statistics into the lag metrics.
    fn publish_stats(&mut self) {
        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        let Some(stats) = consumer.context().stats.lock().expect("stats lock").take() else {
            return;
        };
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        let mut max_lag: u64 = 0;
        if let Some(topic) = stats.topics.get(&self.config.topic) {
            for (pid, p) in &topic.partitions {
                if p.consumer_lag >= 0
                    && let Ok(part) = u32::try_from(*pid)
                {
                    let lag = u64::try_from(p.consumer_lag).unwrap_or(0);
                    max_lag = max_lag.max(lag);
                    metrics.set_partition_lag(PartitionId(part), lag);
                }
            }
        }
        metrics.set_lag_max(max_lag);
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

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        if self.consumer.is_some() {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "open() called twice".into(),
            });
        }
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
                    return Err(SourceError::Client {
                        class: ErrorClass::Retryable,
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
                    if let Some(m) = &self.metrics {
                        m.rebalance_assigned();
                        m.set_lanes_active(tpl.count());
                    }
                    if tpl.count() == 0 {
                        // Empty assignment (no partitions for this member).
                        self.saw_first_assignment = true;
                        return Ok(SourceEvent::Idle);
                    }
                    let lanes = self.accept_assignment(&tpl)?;
                    return Ok(SourceEvent::LanesAssigned(lanes));
                }
                Intent::Revoke(tpl) => {
                    if let Some(m) = &self.metrics {
                        m.rebalance_revoked();
                    }
                    // Map revoked partitions back to lane ids.
                    let revoked: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                    let lanes: Vec<LaneId> = self
                        .assignment
                        .iter()
                        .filter(|(_, p)| revoked.contains(p))
                        .map(|(l, _)| *l)
                        .collect();
                    for lane in &lanes {
                        self.assignment.remove(lane);
                    }
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
        let assigned: Vec<i32> = self.assignment.values().copied().collect();
        let mut tpl = TopicPartitionList::new();
        for (p, offset) in watermarks {
            let partition = i32::try_from(p.0).unwrap_or(-1);
            if assigned.contains(&partition) {
                tpl.add_partition_offset(&self.config.topic, partition, Offset::Offset(*offset))
                    .map_err(fatal("build offset list"))?;
            } else {
                tracing::debug!(
                    partition = p.0,
                    offset,
                    "skipping store for partition no longer assigned"
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
