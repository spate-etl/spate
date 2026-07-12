//! Sink-side mocks: a scriptable [`ShardWriter`] that captures every write,
//! and a length-prefixed [`RowEncoder`] whose frames tests can decode.

use bytes::{BufMut, Bytes, BytesMut};
use etl_core::deser::Owned;
use etl_core::error::{ErrorClass, SinkError};
use etl_core::metrics::Meter;
use etl_core::record::Record;
use etl_core::sink::{
    RowEncoder, SealedBatch, ShardWriter, SinkBundle, SinkParts, SinkPoolConfig, endpoint_probe,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// A replica endpoint tag — [`CaptureWriter`]'s
/// [`Endpoint`](ShardWriter::Endpoint).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaTag {
    /// Shard index.
    pub shard: usize,
    /// Replica index within the shard.
    pub replica: usize,
}

/// How a scripted write resolves.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScriptedResult {
    /// The write succeeds (the durable-ack point).
    Ok,
    /// The write fails with [`ErrorClass::Retryable`].
    Retryable(String),
    /// The write fails with [`ErrorClass::Fatal`].
    Fatal(String),
}

/// One scripted write outcome: an optional delay, then a result.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteOutcome {
    /// Sleep this long (tokio time — controllable with `test-util`) before
    /// resolving.
    pub delay: Option<Duration>,
    /// How the write resolves.
    pub result: ScriptedResult,
}

impl WriteOutcome {
    /// A successful write.
    #[must_use]
    pub fn ok() -> Self {
        WriteOutcome {
            delay: None,
            result: ScriptedResult::Ok,
        }
    }

    /// A retryable failure.
    #[must_use]
    pub fn retryable(reason: impl Into<String>) -> Self {
        WriteOutcome {
            delay: None,
            result: ScriptedResult::Retryable(reason.into()),
        }
    }

    /// A fatal failure.
    #[must_use]
    pub fn fatal(reason: impl Into<String>) -> Self {
        WriteOutcome {
            delay: None,
            result: ScriptedResult::Fatal(reason.into()),
        }
    }

    /// Resolve only after `delay` of tokio time.
    #[must_use]
    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }
}

/// One recorded [`ShardWriter::write_batch`] attempt.
#[derive(Clone, Debug)]
pub struct CapturedWrite {
    /// Target shard.
    pub shard: usize,
    /// Target replica.
    pub replica: usize,
    /// `SealedBatch::rows`.
    pub rows: u64,
    /// `SealedBatch::bytes`.
    pub bytes: u64,
    /// `SealedBatch::dedup_token`.
    pub dedup_token: String,
    /// All frames concatenated (decode with [`decode_rows`](crate::decode_rows)
    /// when produced by [`TestEncoder`]).
    pub payload: Vec<u8>,
    /// How the scripted attempt resolved.
    pub result: ScriptedResult,
}

#[derive(Debug, Default)]
struct SinkState {
    global: VecDeque<WriteOutcome>,
    per_endpoint: HashMap<(usize, usize), VecDeque<WriteOutcome>>,
    probe_failures: HashMap<(usize, usize), String>,
    writes: Vec<CapturedWrite>,
}

/// Build a paired scriptable writer and its handle.
#[must_use]
pub fn capture_writer() -> (CaptureWriter, SinkScript) {
    let state = Arc::new(Mutex::new(SinkState::default()));
    (
        CaptureWriter {
            state: Arc::clone(&state),
        },
        SinkScript { state },
    )
}

/// Scripts outcomes for a [`CaptureWriter`] and reads back its captures.
/// Cloneable; all clones address the same writer.
#[derive(Clone, Debug)]
pub struct SinkScript {
    state: Arc<Mutex<SinkState>>,
}

impl SinkScript {
    fn lock(&self) -> MutexGuard<'_, SinkState> {
        self.state.lock().expect("etl-test sink state poisoned")
    }

    /// Queue an outcome consumed by the next otherwise-unscripted write to
    /// **any** endpoint (FIFO). Endpoint-specific scripts take precedence.
    pub fn enqueue_global(&self, outcome: WriteOutcome) {
        self.lock().global.push_back(outcome);
    }

    /// Queue an outcome for the next write to `(shard, replica)` (FIFO).
    pub fn enqueue_for(&self, shard: usize, replica: usize, outcome: WriteOutcome) {
        self.lock()
            .per_endpoint
            .entry((shard, replica))
            .or_default()
            .push_back(outcome);
    }

    /// Make [`ShardWriter::probe`] of `(shard, replica)` fail with a
    /// retryable error until [`heal_probe`](Self::heal_probe).
    pub fn fail_probe(&self, shard: usize, replica: usize, reason: impl Into<String>) {
        self.lock()
            .probe_failures
            .insert((shard, replica), reason.into());
    }

    /// Let `(shard, replica)`'s probe succeed again.
    pub fn heal_probe(&self, shard: usize, replica: usize) {
        self.lock().probe_failures.remove(&(shard, replica));
    }

    /// Every write attempt so far, in order.
    #[must_use]
    pub fn writes(&self) -> Vec<CapturedWrite> {
        self.lock().writes.clone()
    }
}

/// A [`ShardWriter`] that records every attempt and resolves it according
/// to the paired [`SinkScript`] (default: success). Unscripted writes
/// succeed, so happy-path tests need no scripting at all.
#[derive(Clone, Debug)]
pub struct CaptureWriter {
    state: Arc<Mutex<SinkState>>,
}

impl ShardWriter for CaptureWriter {
    type Endpoint = ReplicaTag;

    fn attach_metrics(&mut self, meter: Option<Meter>) {
        // Exercise the connector-metrics seam: a family under the sink's
        // `etl_<component_type>_sink_*` namespace (e.g. `etl_capture_sink_*`).
        if let Some(meter) = meter {
            meter.counter("attaches_total", &[]).increment(1);
        }
    }

    fn write_batch(
        &self,
        endpoint: &ReplicaTag,
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let state = Arc::clone(&self.state);
        let endpoint = *endpoint;
        let rows = batch.rows;
        let bytes = batch.bytes;
        let dedup_token = batch.dedup_token.clone();
        let payload: Vec<u8> = batch
            .frames
            .iter()
            .flat_map(|f: &Bytes| f.iter().copied())
            .collect();
        async move {
            let outcome = {
                let mut st = state.lock().expect("etl-test sink state poisoned");
                let outcome = st
                    .per_endpoint
                    .get_mut(&(endpoint.shard, endpoint.replica))
                    .and_then(VecDeque::pop_front)
                    .or_else(|| st.global.pop_front())
                    .unwrap_or_else(WriteOutcome::ok);
                st.writes.push(CapturedWrite {
                    shard: endpoint.shard,
                    replica: endpoint.replica,
                    rows,
                    bytes,
                    dedup_token,
                    payload,
                    result: outcome.result.clone(),
                });
                outcome
            };
            if let Some(delay) = outcome.delay {
                tokio::time::sleep(delay).await;
            }
            match outcome.result {
                ScriptedResult::Ok => Ok(()),
                ScriptedResult::Retryable(reason) => Err(SinkError::Client {
                    class: ErrorClass::Retryable,
                    reason,
                }),
                ScriptedResult::Fatal(reason) => Err(SinkError::Client {
                    class: ErrorClass::Fatal,
                    reason,
                }),
            }
        }
    }

    fn probe(&self, endpoint: &ReplicaTag) -> impl Future<Output = Result<(), SinkError>> + Send {
        let state = Arc::clone(&self.state);
        let endpoint = *endpoint;
        async move {
            let failure = state
                .lock()
                .expect("etl-test sink state poisoned")
                .probe_failures
                .get(&(endpoint.shard, endpoint.replica))
                .cloned();
            match failure {
                None => Ok(()),
                Some(reason) => Err(SinkError::Client {
                    class: ErrorClass::Retryable,
                    reason,
                }),
            }
        }
    }
}

/// Build a ready-to-run capturing sink: a `shards × replicas` in-memory
/// topology over a [`CaptureWriter`], with its scripting handle.
///
/// The returned [`CaptureSink`] implements
/// [`SinkBundle`](etl_core::sink::SinkBundle), so it drops straight into
/// `Pipeline::sink` — the first-class mock path for testing whole
/// assemblies. Unscripted writes succeed; drive failures and probe health
/// through the [`SinkScript`].
#[must_use]
pub fn capture_sink(shards: usize, replicas: usize) -> (CaptureSink, SinkScript) {
    assert!(shards > 0, "capture_sink needs at least one shard");
    assert!(replicas > 0, "capture_sink needs at least one replica");
    let (writer, script) = capture_writer();
    (
        CaptureSink {
            writer,
            shards,
            replicas,
            pool: SinkPoolConfig::default(),
        },
        script,
    )
}

/// A capturing sink bundle for whole-pipeline tests — see [`capture_sink`].
#[derive(Clone, Debug)]
pub struct CaptureSink {
    writer: CaptureWriter,
    shards: usize,
    replicas: usize,
    pool: SinkPoolConfig,
}

impl CaptureSink {
    /// Override the pool tuning (tests usually shrink `linger`).
    #[must_use]
    pub fn with_pool_config(mut self, pool: SinkPoolConfig) -> Self {
        self.pool = pool;
        self
    }
}

impl SinkBundle for CaptureSink {
    type Writer = CaptureWriter;

    fn into_parts(self) -> SinkParts<CaptureWriter> {
        let endpoints: Vec<Vec<ReplicaTag>> = (0..self.shards)
            .map(|shard| {
                (0..self.replicas)
                    .map(|replica| ReplicaTag { shard, replica })
                    .collect()
            })
            .collect();
        // A mock scripts probe health directly (via `SinkScript`), so this
        // reuses the capture writer rather than an independent client set —
        // the separate-probe-clients rule matters for real connectors with a
        // live pool, not an in-memory fake.
        let probe = endpoint_probe(self.writer.clone(), Arc::new(endpoints.clone()));
        SinkParts::new(self.writer, endpoints, self.pool)
            .with_component_type("capture")
            .with_probe(probe)
    }
}

/// [`RowEncoder`] writing each row as `u32` little-endian length + bytes.
/// Decode captured frames with [`decode_rows`](crate::decode_rows).
#[derive(Clone, Copy, Debug, Default)]
pub struct TestEncoder;

impl RowEncoder<Owned<Vec<u8>>> for TestEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        let len = u32::try_from(rec.payload.len()).map_err(|_| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: "row longer than u32::MAX".to_owned(),
        })?;
        buf.put_u32_le(len);
        buf.extend_from_slice(&rec.payload);
        Ok(())
    }
}

/// Decode a frame produced by [`TestEncoder`] back into rows.
///
/// # Panics
///
/// Panics on truncated input — the frame did not come from [`TestEncoder`].
#[must_use]
pub fn decode_rows(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    while !bytes.is_empty() {
        assert!(bytes.len() >= 4, "decode_rows: truncated length prefix");
        let len = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")) as usize;
        bytes = &bytes[4..];
        assert!(bytes.len() >= len, "decode_rows: truncated row body");
        rows.push(bytes[..len].to_vec());
        bytes = &bytes[len..];
    }
    rows
}
