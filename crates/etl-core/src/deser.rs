//! Deserialization contract: one borrowed payload in, 0..N records out,
//! push-style.
//!
//! The [`RecFamily`] lifetime→type family is what lets a
//! lifetime-parameterized record type cross generic and dyn boundaries
//! (validated by the seam prototype — see `docs/DESIGN.md` § Frozen v1
//! contracts). Deserializers for borrowing record types implement
//! `Deserializer<F>` for a family `F` whose `Rec<'buf>` borrows the payload
//! buffer; owned record types use the provided [`Owned`] family.

use crate::checkpoint::AckRef;
use crate::error::DeserError;
use crate::record::{Flow, RawPayload, Record};

/// Maps a payload-buffer lifetime to the deserialized record type.
///
/// The family tag itself is `'static` — it is a type-level function, never
/// instantiated — which is what keeps chains nameable and `Send` while the
/// records they process borrow ephemeral buffers.
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
/// a lifetime). Zero emissions is valid (tombstones, empty envelopes);
/// errors are subject to the stage's `ErrorPolicy`.
pub trait Deserializer<F: RecFamily>: Send {
    /// Decode `raw`, emitting each resulting record with the payload's
    /// metadata and a clone of `ack`.
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, F::Rec<'buf>>,
    ) -> Result<(), DeserError>;
}

/// Passthrough deserializer: yields the raw bytes as owned `Vec<u8>`
/// records. Useful for byte-oriented pipelines and tests.
#[derive(Debug, Default)]
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
