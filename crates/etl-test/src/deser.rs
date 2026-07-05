//! Deserializer mocks and a collecting emitter.

use etl_core::checkpoint::AckRef;
use etl_core::deser::{Deserializer, EmitRecord, Owned};
use etl_core::error::DeserError;
use etl_core::record::{Flow, RawPayload, Record};

#[derive(Clone, Debug)]
enum Mode {
    Passthrough,
    SplitOn(u8),
    FailOnPrefix(Vec<u8>),
}

/// A scriptable [`Deserializer`] over owned byte-vector records.
///
/// Modes:
/// - [`passthrough`](Self::passthrough): one record per payload;
/// - [`split_on`](Self::split_on): one record per delimiter-separated
///   segment — exercises 1→N fan-out (`b"a,b"` → `["a", "b"]`; empty
///   segments are emitted, so `b"a,,b"` yields three records);
/// - [`fail_on_prefix`](Self::fail_on_prefix): payloads starting with the
///   marker fail with [`DeserError::Malformed`], everything else passes
///   through — exercises `Skip`/`Fail` error policies.
///
/// Emission honors [`Flow::Blocked`]: the deserializer stops emitting for
/// the current payload, as the seam contract requires.
#[derive(Clone, Debug)]
pub struct TestDeserializer {
    mode: Mode,
}

impl TestDeserializer {
    /// One record per payload.
    #[must_use]
    pub fn passthrough() -> Self {
        TestDeserializer {
            mode: Mode::Passthrough,
        }
    }

    /// One record per `delimiter`-separated segment of the payload.
    #[must_use]
    pub fn split_on(delimiter: u8) -> Self {
        TestDeserializer {
            mode: Mode::SplitOn(delimiter),
        }
    }

    /// Fail payloads starting with `marker`; pass the rest through.
    #[must_use]
    pub fn fail_on_prefix(marker: impl Into<Vec<u8>>) -> Self {
        TestDeserializer {
            mode: Mode::FailOnPrefix(marker.into()),
        }
    }
}

impl Deserializer<Owned<Vec<u8>>> for TestDeserializer {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, Vec<u8>>,
    ) -> Result<(), DeserError> {
        let meta = raw.meta();
        let mut emit = |payload: Vec<u8>| {
            out.emit(Record {
                payload,
                meta,
                ack: ack.clone(),
            })
        };
        match &self.mode {
            Mode::Passthrough => {
                let _ = emit(raw.bytes.to_vec());
            }
            Mode::SplitOn(delim) => {
                for segment in raw.bytes.split(|b| b == delim) {
                    if emit(segment.to_vec()) == Flow::Blocked {
                        break;
                    }
                }
            }
            Mode::FailOnPrefix(marker) => {
                if raw.bytes.starts_with(marker) {
                    return Err(DeserError::Malformed {
                        reason: format!("payload starts with test marker {marker:?}"),
                    });
                }
                let _ = emit(raw.bytes.to_vec());
            }
        }
        Ok(())
    }
}

/// An [`EmitRecord`] implementation that collects records into a `Vec`,
/// optionally reporting [`Flow::Blocked`] once a capacity is reached
/// (rejected records are counted, not stored) — for testing blocked-path
/// handling.
#[derive(Debug, Default)]
pub struct EmitCollector<T> {
    /// Records accepted so far.
    pub records: Vec<Record<T>>,
    /// Records rejected after the block threshold was reached.
    pub rejected: usize,
    block_after: Option<usize>,
}

impl<T> EmitCollector<T> {
    /// A collector that always accepts.
    #[must_use]
    pub fn new() -> Self {
        EmitCollector {
            records: Vec::new(),
            rejected: 0,
            block_after: None,
        }
    }

    /// A collector that accepts `n` records, then rejects with
    /// [`Flow::Blocked`].
    #[must_use]
    pub fn blocking_after(n: usize) -> Self {
        EmitCollector {
            records: Vec::new(),
            rejected: 0,
            block_after: Some(n),
        }
    }

    /// The collected payloads, cloned out.
    #[must_use]
    pub fn payloads(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.records.iter().map(|r| r.payload.clone()).collect()
    }
}

impl<'buf, T> EmitRecord<'buf, T> for EmitCollector<T> {
    fn emit(&mut self, rec: Record<T>) -> Flow {
        if let Some(cap) = self.block_after
            && self.records.len() >= cap
        {
            self.rejected += 1;
            return Flow::Blocked;
        }
        self.records.push(rec);
        Flow::Continue
    }
}
