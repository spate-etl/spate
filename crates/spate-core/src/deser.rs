//! Deserialization contract: one borrowed payload in, 0..N records out,
//! push-style.
//!
//! The [`RecFamily`] lifetime→type family lets a lifetime-parameterized
//! record type cross generic and dyn boundaries (ADR-0013). Deserializers
//! for borrowing record types implement `Deserializer<F>` for a family `F`
//! whose `Rec<'buf>` borrows the payload buffer; owned record types use
//! the provided [`Owned`] family.

use crate::checkpoint::AckRef;
use crate::error::DeserError;
use crate::metrics::Meter;
use crate::metrics::names::DESERIALIZER_DEFAULT;
use crate::record::{Flow, RawPayload, Record};

/// Maps a payload-buffer lifetime to the deserialized record type.
///
/// The family tag itself is `'static`, a type-level function that is never
/// instantiated. Chains stay nameable and `Send` while the records they
/// process borrow ephemeral buffers.
pub trait RecFamily: 'static {
    /// The record type produced for payloads borrowing `'buf`.
    type Rec<'buf>: Send;
}

/// Family for record types that do not borrow (owned payloads).
#[derive(Debug)]
pub struct Owned<T>(std::marker::PhantomData<fn() -> T>);

impl<T: Send + 'static> RecFamily for Owned<T> {
    type Rec<'buf> = T;
}

/// Push-style receiver for deserialized records (0..N per payload).
pub trait EmitRecord<'buf, T> {
    /// Hand one record to the chain. A [`Flow::Blocked`] return means
    /// downstream is full; the deserializer stops emitting and the driver
    /// retries the batch from the current payload cursor.
    fn emit(&mut self, rec: Record<T>) -> Flow;
}

/// One borrowed payload in, 0..N records out through `out`.
///
/// Dyn-compatible for a concrete family (the only generic on the method is
/// a lifetime). Zero emissions is valid (tombstones, empty envelopes). An
/// error is subject to the stage's `ErrorPolicy`, except
/// [`DeserError::NotReady`], which holds the batch for a replay, and
/// [`DeserError::Fatal`], which stops the pipeline.
pub trait Deserializer<F: RecFamily>: Send {
    /// Decode `raw`, emitting each resulting record with the payload's
    /// metadata and a clone of `ack`.
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, F::Rec<'buf>>,
    ) -> Result<(), DeserError>;

    /// The implementation label (`"json"`, `"avro"`) on this deserializer's
    /// stage series and on the `Meter` it receives, which is scoped
    /// `spate_<component_type>_deser_*`. The default,
    /// [`DESERIALIZER_DEFAULT`](crate::metrics::names::DESERIALIZER_DEFAULT),
    /// receives no `Meter`.
    ///
    /// A deserializer that wraps another returns the inner one's type.
    fn component_type(&self) -> &str {
        DESERIALIZER_DEFAULT
    }

    /// Receive the [`DeserMetricsCtx`] for this deserializer's own metric
    /// families. A chain's
    /// [`with_metrics`](crate::ops::ChainBuilder::with_metrics) calls it once
    /// per chain; resolve handles here, never on the per-record path. The
    /// default ignores it.
    ///
    /// A deserializer that wraps another passes it on.
    fn attach_metrics(&mut self, _cx: DeserMetricsCtx) {}
}

/// What a chain hands its deserializer for the deserializer's own metric
/// families, through [`Deserializer::attach_metrics`].
#[derive(Debug)]
#[non_exhaustive]
pub struct DeserMetricsCtx {
    /// Scoped `spate_<component_type>_deser_*` and pre-labeled with the
    /// pipeline, the chain's `<component>.deserializer` and the deserializer's
    /// [`component_type`](Deserializer::component_type). `None` when that type
    /// cannot scope a family: the default `"deserializer"` or a reserved root
    /// opts out silently, and a malformed value is logged and also yields
    /// `None`.
    pub meter: Option<Meter>,
}

impl DeserMetricsCtx {
    /// Context carrying `meter`. Deserializers outside this crate construct it
    /// here, as the struct is `#[non_exhaustive]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::deser::DeserMetricsCtx;
    /// use spate_core::metrics::{Meter, render_local};
    ///
    /// let meter = Meter::with_namespace("probe", "orders", "main.deserializer", "probe");
    /// let cx = DeserMetricsCtx::new(Some(meter));
    /// let meter = cx.meter.expect("a scoped Meter");
    /// let text = render_local(|| meter.counter("drops_total", &[]).increment(1));
    /// assert!(text.lines().any(|l| {
    ///     l == r#"spate_probe_drops_total{pipeline="orders",component="main.deserializer",component_type="probe"} 1"#
    /// }));
    /// ```
    #[must_use]
    pub fn new(meter: Option<Meter>) -> Self {
        DeserMetricsCtx { meter }
    }
}

/// Passthrough deserializer: yields the raw bytes as owned `Vec<u8>`
/// records. Useful for byte-oriented pipelines and tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesPassthrough;

impl Deserializer<Owned<Vec<u8>>> for BytesPassthrough {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, Vec<u8>>,
    ) -> Result<(), DeserError> {
        let _ = out.emit(Record {
            payload: raw.bytes.to_vec(),
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::record::PartitionId;

    struct Sink(Vec<Vec<u8>>);
    impl EmitRecord<'_, Vec<u8>> for Sink {
        fn emit(&mut self, rec: Record<Vec<u8>>) -> Flow {
            self.0.push(rec.payload);
            Flow::Continue
        }
    }

    #[test]
    fn passthrough_emits_one_owned_record() {
        let (ack, _rx) = AckRef::test_pair();
        let raw = RawPayload {
            bytes: b"abc",
            key: None,
            partition: PartitionId(0),
            offset: 1,
            timestamp_ms: 2,
        };
        let mut sink = Sink(Vec::new());
        BytesPassthrough.deserialize(&raw, &ack, &mut sink).unwrap();
        assert_eq!(sink.0, vec![b"abc".to_vec()]);
    }
}
