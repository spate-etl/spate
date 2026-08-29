#![cfg_attr(docsrs, feature(doc_cfg))]
// `loom` is never set in a published build, so its cfg carries no badge.
#![cfg_attr(docsrs, doc(auto_cfg(hide(loom))))]
//! Testing utilities for the Spate framework.
//!
//! Every mock pairs with a scripting/observation handle: [`MemorySource`] +
//! [`SourceHandle`] for the source side, [`CaptureWriter`] + [`SinkScript`]
//! for the sink side, plus [`TestDeserializer`], [`TestEncoder`], and
//! [`EmitCollector`] to exercise the stages in between. All are deterministic
//! and need no external infrastructure.
//!
//! # Example: source → deserialize → encode → acknowledge → commit
//!
//! The full life of a record, as a pipeline thread drives it:
//!
//! ```
//! use spate_core::checkpoint::Checkpointer;
//! use spate_core::deser::Deserializer;
//! use spate_core::record::PartitionId;
//! use spate_core::sink::RowEncoder;
//! use spate_core::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
//! use spate_test::{EmitCollector, TestDeserializer, TestEncoder, memory_source};
//! use std::time::Duration;
//!
//! // Wire a real checkpointer to the in-memory source.
//! let mut cp = Checkpointer::new();
//! let (mut source, handle) = memory_source();
//! source.open(SourceCtx::new(cp.handle())).unwrap();
//!
//! let p = PartitionId(0);
//! cp.begin_epoch(&[p], 1);
//! handle.assign_lanes(&[(LaneId(0), p)]);
//! let SourceEvent::LanesAssigned(mut lanes) =
//!     source.poll_events(Duration::from_millis(10)).unwrap()
//! else {
//!     panic!("expected an assignment");
//! };
//!
//! // Produce, then poll → deserialize → encode.
//! handle.push(p, Some(b"key"), b"hello");
//! let mut batch = lanes[0].poll(512, Duration::from_millis(100)).unwrap().unwrap();
//! let ack = batch.ack().clone();
//! let mut out = EmitCollector::new();
//! let mut deser = TestDeserializer::passthrough();
//! while let Some(raw) = batch.next_payload() {
//!     deser.deserialize(&raw, &ack, &mut out).unwrap();
//! }
//! let mut frame = bytes::BytesMut::new();
//! for rec in &out.records {
//!     TestEncoder.encode(rec, &mut frame).unwrap();
//! }
//! assert_eq!(spate_test::decode_rows(&frame), vec![b"hello".to_vec()]);
//!
//! // Dropping every record (and the batch) resolves the acknowledgement;
//! // the watermark advances; the source records the commit.
//! drop(out);
//! drop(batch);
//! drop(ack);
//! cp.drain();
//! let watermarks = cp.take_watermarks();
//! assert_eq!(watermarks, vec![(p, 1)]);
//! source.commit(&watermarks).unwrap();
//! assert_eq!(handle.last_committed(p), Some(1));
//! ```
//!
//! For failure-path testing, script the sink
//! ([`SinkScript::enqueue_for`]) and the deserializer
//! ([`TestDeserializer::fail_on_prefix`]); for property tests, enable the
//! `proptest` feature and use [`strategies`].

mod coordination;
mod deser;
mod run;
mod sink;
mod source;

#[cfg(feature = "proptest")]
pub mod strategies;

pub use coordination::{CoordinatorScript, ScriptedCoordinator, scripted_coordinator};
pub use deser::{EmitCollector, TestDeserializer};
pub use run::{PipelineRun, wait_until};
pub use sink::{
    CaptureSink, CaptureWriter, CapturedWrite, ReplicaTag, ScriptedResult, SinkScript, TestEncoder,
    WriteOutcome, capture_sink, capture_writer, decode_rows,
};
pub use source::{
    DrainBarrierProbe, MemoryBatch, MemoryLane, MemorySource, SourceHandle, memory_source,
};

/// Re-export of the framework's byte-passthrough deserializer.
pub use spate_core::deser::BytesPassthrough;
