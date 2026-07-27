//! In-memory source mock with a scripting handle.
//!
//! [`MemorySource`] implements [`Source`]; the paired [`SourceHandle`]
//! scripts the control plane (assignments, revocations, pushed payloads)
//! and observes everything the source is asked to do (commits, pauses,
//! flushes). The mock is deliberately strict: misuse that would indicate a
//! runtime bug — pausing an unassigned lane, revoking an unknown lane,
//! duplicate lane ids — panics with a clear message.

use spate_core::checkpoint::AckIssuer;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{
    DrainBarrier, LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane,
};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// One queued payload with its pre-assigned offset.
#[derive(Debug)]
struct PayloadData {
    key: Option<Vec<u8>>,
    bytes: Vec<u8>,
    offset: i64,
    timestamp_ms: i64,
}

#[derive(Debug, Default)]
struct PartitionQueue {
    queue: VecDeque<PayloadData>,
    next_offset: i64,
}

#[derive(Debug)]
enum ScriptEvent {
    Assign(Vec<(LaneId, PartitionId)>),
    Revoke {
        lanes: Vec<LaneId>,
        barrier: DrainBarrier,
    },
}

#[derive(Debug, Default)]
struct Shared {
    partitions: HashMap<PartitionId, PartitionQueue>,
    active: HashMap<LaneId, PartitionId>,
    paused: BTreeSet<LaneId>,
    committed: Vec<(PartitionId, i64)>,
    flush_commits_calls: usize,
    open: bool,
    events: VecDeque<ScriptEvent>,
}

#[derive(Debug, Default)]
struct SharedSync {
    state: Mutex<Shared>,
    /// Notified on push and resume — wakes waiting lane polls.
    data_cv: Condvar,
    /// Notified on assign/revoke — wakes a waiting `poll_events`.
    event_cv: Condvar,
    /// Notified on commit — wakes a waiting `wait_committed`.
    commit_cv: Condvar,
}

impl SharedSync {
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.state.lock().expect("spate-test source state poisoned")
    }
}

/// Build a paired in-memory source and its scripting handle.
#[must_use]
pub fn memory_source() -> (MemorySource, SourceHandle) {
    let shared = Arc::new(SharedSync::default());
    (
        MemorySource {
            shared: Arc::clone(&shared),
            issuer: None,
        },
        SourceHandle { shared },
    )
}

/// Scripts and observes a [`MemorySource`]. Cloneable; all clones address
/// the same source.
#[derive(Clone, Debug)]
pub struct SourceHandle {
    shared: Arc<SharedSync>,
}

impl SourceHandle {
    /// Queue an assignment event: the source's next
    /// [`poll_events`](Source::poll_events) returns
    /// [`SourceEvent::LanesAssigned`] with one [`MemoryLane`] per pair.
    ///
    /// # Panics
    ///
    /// Panics on duplicate lane ids (within the call or against currently
    /// active lanes) or on a partition that is already served by an active
    /// lane — both would be runtime bugs in a real deployment.
    pub fn assign_lanes(&self, lanes: &[(LaneId, PartitionId)]) {
        let mut st = self.shared.lock();
        for &(lane, partition) in lanes {
            assert!(
                !st.active.contains_key(&lane),
                "assign_lanes: lane {lane:?} is already active"
            );
            assert!(
                !st.active.values().any(|&p| p == partition),
                "assign_lanes: partition {partition:?} is already served by an active lane"
            );
            st.active.insert(lane, partition);
            st.partitions.entry(partition).or_default();
        }
        let seen: BTreeSet<_> = lanes.iter().map(|&(l, _)| l).collect();
        assert_eq!(seen.len(), lanes.len(), "assign_lanes: duplicate lane ids");
        st.events.push_back(ScriptEvent::Assign(lanes.to_vec()));
        drop(st);
        self.shared.event_cv.notify_all();
    }

    /// Queue a revocation event for `lanes` and return a probe over its
    /// [`DrainBarrier`]. The barrier expects **one arrival per revoked
    /// lane** (a pipeline thread owning several of them arrives once per
    /// lane it stops).
    ///
    /// # Panics
    ///
    /// Panics if any lane is not currently active.
    pub fn revoke_lanes(&self, lanes: &[LaneId]) -> DrainBarrierProbe {
        let barrier = DrainBarrier::new(lanes.len());
        let mut st = self.shared.lock();
        for lane in lanes {
            assert!(
                st.active.remove(lane).is_some(),
                "revoke_lanes: lane {lane:?} is not active"
            );
            st.paused.remove(lane);
        }
        st.events.push_back(ScriptEvent::Revoke {
            lanes: lanes.to_vec(),
            barrier: barrier.clone(),
        });
        drop(st);
        self.shared.event_cv.notify_all();
        DrainBarrierProbe { barrier }
    }

    /// Queue one payload on `partition` with `timestamp_ms` equal to the
    /// assigned offset. Returns that offset. Pushing to a partition that
    /// has no active lane yet is allowed — payloads wait.
    pub fn push(&self, partition: PartitionId, key: Option<&[u8]>, payload: &[u8]) -> i64 {
        self.push_at(partition, key, payload, None)
    }

    /// [`push`](Self::push) with an explicit event timestamp.
    pub fn push_at(
        &self,
        partition: PartitionId,
        key: Option<&[u8]>,
        payload: &[u8],
        timestamp_ms: Option<i64>,
    ) -> i64 {
        let mut st = self.shared.lock();
        let q = st.partitions.entry(partition).or_default();
        let offset = q.next_offset;
        q.next_offset += 1;
        q.queue.push_back(PayloadData {
            key: key.map(<[u8]>::to_vec),
            bytes: payload.to_vec(),
            offset,
            timestamp_ms: timestamp_ms.unwrap_or(offset),
        });
        drop(st);
        self.shared.data_cv.notify_all();
        offset
    }

    /// Queue several keyless payloads; returns their offsets.
    pub fn push_many<I, P>(&self, partition: PartitionId, payloads: I) -> Vec<i64>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        payloads
            .into_iter()
            .map(|p| self.push(partition, None, p.as_ref()))
            .collect()
    }

    /// Every watermark ever passed to [`Source::commit`], in call order
    /// (flattened).
    #[must_use]
    pub fn committed(&self) -> Vec<(PartitionId, i64)> {
        self.shared.lock().committed.clone()
    }

    /// The most recent committed position for `partition`, if any.
    #[must_use]
    pub fn last_committed(&self, partition: PartitionId) -> Option<i64> {
        self.shared
            .lock()
            .committed
            .iter()
            .rev()
            .find(|&&(p, _)| p == partition)
            .map(|&(_, o)| o)
    }

    /// Block until `partition`'s most recent committed offset reaches at least
    /// `offset`, or `timeout` elapses; returns whether the target was met.
    ///
    /// The blocking counterpart to [`last_committed`](Self::last_committed):
    /// waking on each [`Source::commit`] rather than polling. Pass the
    /// one-past-last watermark (e.g. `last_offset + 1`) to wait for every
    /// pushed record to be durably committed.
    #[must_use]
    pub fn wait_committed(&self, partition: PartitionId, offset: i64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut st = self.shared.lock();
        loop {
            let reached = st
                .committed
                .iter()
                .rev()
                .find(|&&(p, _)| p == partition)
                .is_some_and(|&(_, o)| o >= offset);
            if reached {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .shared
                .commit_cv
                .wait_timeout(st, deadline - now)
                .expect("spate-test source state poisoned");
            st = guard;
        }
    }

    /// Lanes currently paused via [`Source::pause`], ascending.
    #[must_use]
    pub fn paused_lanes(&self) -> Vec<LaneId> {
        self.shared.lock().paused.iter().copied().collect()
    }

    /// How many times [`Source::flush_commits`] was called.
    #[must_use]
    pub fn flush_commits_calls(&self) -> usize {
        self.shared.lock().flush_commits_calls
    }

    /// Whether [`Source::open`] has been called.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.shared.lock().open
    }
}

/// Observation side of a revocation's [`DrainBarrier`].
#[derive(Clone, Debug)]
pub struct DrainBarrierProbe {
    barrier: DrainBarrier,
}

impl DrainBarrierProbe {
    /// Whether every party has arrived.
    #[must_use]
    pub fn completed(&self) -> bool {
        self.barrier.remaining() == 0
    }

    /// Parties still outstanding.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.barrier.remaining()
    }

    /// Block until the drain completes or `timeout` elapses; returns
    /// whether it completed.
    #[must_use]
    pub fn wait(&self, timeout: Duration) -> bool {
        self.barrier.wait(timeout)
    }
}

/// In-memory [`Source`] driven entirely by its [`SourceHandle`].
#[derive(Debug)]
pub struct MemorySource {
    shared: Arc<SharedSync>,
    issuer: Option<AckIssuer>,
}

impl Source for MemorySource {
    type Lane = MemoryLane;

    /// Scopes the source's own metric families under `spate_memory_source_*`.
    fn component_type(&self) -> &str {
        "memory"
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        // Exercise the connector-metrics seam: a family under the source's
        // `spate_memory_source_*` namespace, resolved once at open.
        if let Some(meter) = &ctx.meter {
            meter.counter("opens_total", &[]).increment(1);
        }
        self.issuer = Some(ctx.issuer);
        self.shared.lock().open = true;
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<MemoryLane>, SourceError> {
        let issuer = self.issuer.as_ref().ok_or_else(|| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: "poll_events called before open()".to_owned(),
        })?;

        let deadline = Instant::now() + timeout;
        let mut st = self.shared.lock();
        let event = loop {
            if let Some(event) = st.events.pop_front() {
                break event;
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(SourceEvent::Idle);
            }
            let (guard, _) = self
                .shared
                .event_cv
                .wait_timeout(st, deadline - now)
                .expect("spate-test source state poisoned");
            st = guard;
        };
        drop(st);

        Ok(match event {
            ScriptEvent::Assign(pairs) => SourceEvent::LanesAssigned(
                pairs
                    .into_iter()
                    .map(|(id, partition)| MemoryLane {
                        id,
                        partition,
                        shared: Arc::clone(&self.shared),
                        issuer: issuer.clone(),
                        buf: Vec::new(),
                    })
                    .collect(),
            ),
            ScriptEvent::Revoke { lanes, barrier } => SourceEvent::LanesRevoked { lanes, barrier },
        })
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        self.shared.lock().committed.extend_from_slice(watermarks);
        self.shared.commit_cv.notify_all();
        Ok(())
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        self.shared.lock().flush_commits_calls += 1;
        Ok(())
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let mut st = self.shared.lock();
        for lane in lanes {
            assert!(
                st.active.contains_key(lane),
                "pause: lane {lane:?} is not active — runtime bug"
            );
            st.paused.insert(*lane);
        }
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let mut st = self.shared.lock();
        for lane in lanes {
            assert!(
                st.active.contains_key(lane),
                "resume: lane {lane:?} is not active — runtime bug"
            );
            st.paused.remove(lane);
        }
        drop(st);
        self.shared.data_cv.notify_all();
        Ok(())
    }
}

/// Data-plane lane of a [`MemorySource`]: drains pushed payloads for one
/// partition. Polls block (up to the timeout) while the lane is paused or
/// its queue is empty, waking on push and resume.
#[derive(Debug)]
pub struct MemoryLane {
    id: LaneId,
    partition: PartitionId,
    shared: Arc<SharedSync>,
    issuer: AckIssuer,
    buf: Vec<PayloadData>,
}

impl SourceLane for MemoryLane {
    type Batch<'a> = MemoryBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<MemoryBatch<'_>>, SourceError> {
        if max_records == 0 {
            return Ok(None);
        }
        let deadline = Instant::now() + timeout;
        {
            let mut st = self.shared.lock();
            self.buf = loop {
                let available = !st.paused.contains(&self.id)
                    && st
                        .partitions
                        .get(&self.partition)
                        .is_some_and(|q| !q.queue.is_empty());
                if available {
                    let q = st
                        .partitions
                        .get_mut(&self.partition)
                        .expect("partition queue exists");
                    let n = q.queue.len().min(max_records);
                    break q.queue.drain(..n).collect();
                }
                let now = Instant::now();
                if now >= deadline {
                    return Ok(None);
                }
                let (guard, _) = self
                    .shared
                    .data_cv
                    .wait_timeout(st, deadline - now)
                    .expect("spate-test source state poisoned");
                st = guard;
            };
        }
        let last_offset = self.buf.last().expect("non-empty batch").offset;
        let ack = self.issuer.issue(self.partition, last_offset);
        Ok(Some(MemoryBatch {
            items: &self.buf,
            next: 0,
            partition: self.partition,
            ack,
        }))
    }
}

/// One poll's payloads, borrowing the lane's buffer.
#[derive(Debug)]
pub struct MemoryBatch<'a> {
    items: &'a [PayloadData],
    next: usize,
    partition: PartitionId,
    ack: spate_core::checkpoint::AckRef,
}

impl<'a> PayloadBatch<'a> for MemoryBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let item = self.items.get(self.next)?;
        self.next += 1;
        Some(RawPayload {
            bytes: &item.bytes,
            key: item.key.as_deref(),
            partition: self.partition,
            offset: item.offset,
            timestamp_ms: item.timestamp_ms,
        })
    }

    fn ack(&self) -> &spate_core::checkpoint::AckRef {
        &self.ack
    }
}
