//! Deserializer mocks and a collecting emitter.

use crate::wait_for_value;
use spate_core::checkpoint::AckRef;
use spate_core::config::ConfigError;
use spate_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate_core::error::{DeserError, DeserErrorKind};
use spate_core::framing::FramingContract;
use spate_core::record::{Flow, RawPayload, Record};
use std::time::Duration;

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
///   segment, exercising 1→N fan-out (`b"a,b"` → `["a", "b"]`; empty
///   segments are emitted, so `b"a,,b"` yields three records). It refuses a
///   source that frames one record per payload, so a pipeline over such a
///   source fails at start;
/// - [`fail_on_prefix`](Self::fail_on_prefix): payloads starting with the
///   marker fail with
///   [`DeserErrorKind::Malformed`](spate_core::error::DeserErrorKind::Malformed),
///   everything else passes through, exercising `Skip`/`Fail` error policies.
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

    /// One record per `delimiter`-separated segment of the payload. A
    /// pipeline whose source reports
    /// [`FramingContract::PerRecord`] refuses to start with it.
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
                    return Err(DeserError::malformed(format!(
                        "payload starts with test marker {marker:?}"
                    )));
                }
                let _ = emit(raw.bytes.to_vec());
            }
        }
        Ok(())
    }

    fn check_source_framing(&self, source: FramingContract) -> Result<(), ConfigError> {
        match (&self.mode, source) {
            (Mode::SplitOn(_), FramingContract::PerRecord) => Err(ConfigError::Component {
                context: "deserializer".to_owned(),
                message: "TestDeserializer::split_on splits each payload into records, but the \
                          source already frames one record per payload; use \
                          TestDeserializer::passthrough"
                    .to_owned(),
            }),
            _ => Ok(()),
        }
    }
}

/// An [`EmitRecord`] implementation that collects records into a `Vec`,
/// optionally reporting [`Flow::Blocked`] once a capacity is reached
/// (rejected records are counted, not stored), for testing blocked-path
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

/// Replays `raw` through `deser` until it answers anything but
/// [`DeserErrorKind::NotReady`](spate_core::error::DeserErrorKind::NotReady),
/// as the pipeline's driver replays a payload, and returns that answer.
///
/// `between` runs before each replay. A test uses it to move the clock the
/// deserializer reads, or to drive the runtime its fetch runs on.
///
/// `timeout` catches a hang and never decides a result: pass
/// [`HANG`](crate::HANG). The wait blocks the calling thread, so async code
/// calls this from `spawn_blocking` or a thread of its own.
///
/// # Errors
///
/// The first error `deser` returns that is not `NotReady`.
///
/// # Panics
///
/// When `timeout` passes while `deser` still answers `NotReady`, saying the
/// test hung and carrying the last answer.
///
/// # Examples
///
/// ```
/// use spate_test::{EmitCollector, HANG, TestDeserializer, deserialize_until_ready};
///
/// let mut deser = TestDeserializer::passthrough();
/// let mut out = EmitCollector::new();
/// let raw = spate_test::raw_payload(b"row");
/// deserialize_until_ready(HANG, &mut deser, &raw, &spate_test::test_ack(), &mut out, || {})
///     .expect("decodes");
/// assert_eq!(out.payloads(), vec![b"row".to_vec()]);
/// ```
pub fn deserialize_until_ready<F, O>(
    timeout: Duration,
    deser: &mut dyn Deserializer<F>,
    raw: &RawPayload<'_>,
    ack: &AckRef,
    out: &mut O,
    mut between: impl FnMut(),
) -> Result<(), DeserError>
where
    F: RecFamily,
    O: for<'buf> EmitRecord<'buf, F::Rec<'buf>>,
{
    let mut replay = false;
    wait_for_value(
        timeout,
        "an answer other than NotReady",
        || {
            if replay {
                between();
            }
            replay = true;
            deser.deserialize(raw, ack, out)
        },
        |answer| {
            !answer
                .as_ref()
                .is_err_and(|e| e.kind() == DeserErrorKind::NotReady)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HANG, raw_payload, test_ack};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// What [`Waiting`] answers once it is ready.
    #[derive(Clone, Copy)]
    enum Answer {
        Record,
        Fatal,
    }

    /// Answers `NotReady` until `ready` reaches `after`, then `answer`, and
    /// counts its calls. A test's `between` moves `ready`, so the answer
    /// depends on `between` running before the replay.
    struct Waiting {
        ready: Arc<AtomicUsize>,
        after: usize,
        answer: Answer,
        calls: usize,
    }

    impl Waiting {
        fn new(after: usize, answer: Answer) -> (Self, Arc<AtomicUsize>) {
            let ready = Arc::new(AtomicUsize::new(0));
            let deser = Waiting {
                ready: Arc::clone(&ready),
                after,
                answer,
                calls: 0,
            };
            (deser, ready)
        }
    }

    impl Deserializer<Owned<Vec<u8>>> for Waiting {
        fn deserialize<'buf>(
            &mut self,
            raw: &RawPayload<'buf>,
            ack: &AckRef,
            out: &mut dyn EmitRecord<'buf, Vec<u8>>,
        ) -> Result<(), DeserError> {
            self.calls += 1;
            if self.ready.load(Ordering::SeqCst) < self.after {
                return Err(DeserError::not_ready("waiting on the test"));
            }
            match self.answer {
                Answer::Record => {
                    let _ = out.emit(Record {
                        payload: raw.bytes.to_vec(),
                        meta: raw.meta(),
                        ack: ack.clone(),
                    });
                    Ok(())
                }
                Answer::Fatal => Err(DeserError::fatal("rejected")),
            }
        }
    }

    /// Each `NotReady` is followed by one `between` and one replay, and the
    /// first answer that is not `NotReady` is returned.
    #[test]
    fn replays_after_each_not_ready_until_the_deserializer_answers() {
        let (mut deser, ready) = Waiting::new(3, Answer::Record);
        let mut out = EmitCollector::new();
        let result = deserialize_until_ready(
            HANG,
            &mut deser,
            &raw_payload(b"row"),
            &test_ack(),
            &mut out,
            || {
                ready.fetch_add(1, Ordering::SeqCst);
            },
        );
        result.expect("the record decodes");
        assert_eq!(out.payloads(), vec![b"row".to_vec()]);
        assert_eq!(ready.load(Ordering::SeqCst), 3, "between runs");
        assert_eq!(deser.calls, 4, "deserialize calls");
    }

    /// An error other than `NotReady` is returned at once, with no replay.
    #[test]
    fn returns_another_error_without_a_replay() {
        let (mut deser, ready) = Waiting::new(0, Answer::Fatal);
        let mut out = EmitCollector::<Vec<u8>>::new();
        let result = deserialize_until_ready(
            HANG,
            &mut deser,
            &raw_payload(b"row"),
            &test_ack(),
            &mut out,
            || {
                ready.fetch_add(1, Ordering::SeqCst);
            },
        );
        assert!(
            result
                .as_ref()
                .is_err_and(|e| e.kind() == DeserErrorKind::Fatal && e.reason() == "rejected"),
            "{result:?}"
        );
        assert_eq!(ready.load(Ordering::SeqCst), 0, "between runs");
        assert_eq!(deser.calls, 1, "deserialize calls");
    }

    /// A deserializer that never gets ready makes the wait say the test hung.
    #[test]
    #[should_panic(expected = "hung after 0ns waiting for: an answer other than NotReady")]
    fn a_deserializer_that_never_gets_ready_hangs() {
        let (mut deser, _ready) = Waiting::new(1, Answer::Record);
        let mut out = EmitCollector::new();
        let _ = deserialize_until_ready(
            Duration::ZERO,
            &mut deser,
            &raw_payload(b"row"),
            &test_ack(),
            &mut out,
            || {},
        );
    }
}
