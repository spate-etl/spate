//! Measurable entry points into work this crate does not otherwise expose.
//!
//! The sink's writer prepares every message of a sealed batch inside an async
//! `write_batch`, before it hands the first one to librdkafka. Under a runtime
//! an instruction count depends on how the scheduler interleaved polls, so the
//! counted tier cannot enter that function. The preparation itself is
//! synchronous and needs no broker, so it is exported here.
//!
//! The seams follow the rules `spate-s3`'s `bench_seams` module states: behind
//! the off-by-default `testing` feature and `#[doc(hidden)]`, functions only and
//! never this crate's own types, and one whole unit of a stage's work per
//! function.

use crate::sink::{message_headers, parse_messages};
use bytes::Bytes;
use rdkafka::message::Headers as _;
use spate_core::error::SinkError;
use std::hint::black_box;

/// Prepare a sealed batch's frames as the sink's writer does before it sends
/// anything: parse every message, check the count against `rows`, and build
/// each message's librdkafka header list. Returns the number of messages and
/// the number of headers built.
///
/// # Errors
///
/// The fatal error the writer returns for a corrupt frame or a row count the
/// frames do not match.
pub fn prepare_batch(frames: &[Bytes], rows: u64) -> Result<(usize, usize), SinkError> {
    let messages = parse_messages(frames, rows)?;
    let mut headers = 0;
    for message in &messages {
        if let Some(list) = message_headers(message) {
            headers += list.count();
            black_box(list);
        }
    }
    Ok((messages.len(), headers))
}
