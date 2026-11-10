//! Entry points the benches in this workspace need into work this crate does
//! not otherwise expose.
//!
//! Benches in this workspace drive their crate's public API. This module is
//! an exception.
//!
//! This crate's public entry point is [`S3Source`](crate::S3Source), which is
//! asynchronous and driven by object-store I/O. Both properties stop an
//! instruction count being deterministic: under a runtime the number becomes
//! a function of how the scheduler interleaved polls. Record framing is pure,
//! synchronous, on the per-object path and so countable, but reaching it means
//! reaching past the async surface.
//!
//! The rules a future seam has to honor:
//!
//! - It is behind the off-by-default `testing` feature and `#[doc(hidden)]`,
//!   so it is not part of this crate's semver surface and no consumer of the
//!   `spate` facade can see it.
//! - It exports **functions and the aliases they need, never this crate's own
//!   types**. `ObjectFramer`, `ObjectEntry` and the rest stay private and free
//!   to change; only the shape of the work is fixed. `MakeFramer` is an alias
//!   over `std` and `spate-core` types, which a caller must be able to name to
//!   supply a framer at all.
//! - Each function is one whole unit of the work a stage does (an object
//!   run) rather than one internal call. Attribution below that level
//!   comes from the callgrind profile the bench already writes.

use crate::config::Compression;
use crate::framer::{Codec, ObjectFramer};
use crate::split::SplitRange;
use crate::trim::RangeTrim;
use bytes::Bytes;
use spate_core::framing::RecordFramer;
use std::io;
use std::sync::Arc;

/// Builds a fresh record framer, as the source does per object.
pub type MakeFramer = Arc<dyn Fn() -> Box<dyn RecordFramer> + Send + Sync>;

/// Frame a run of objects into records through one framer, returning how
/// many records they produced in total.
///
/// Mirrors the lane, which builds a single `ObjectFramer` and cycles
/// `begin_object`/`finish_object` over the members of its split, so a run of
/// several objects here measures the per-object codec resolution and state
/// reset a multi-member split pays, not the cost of constructing a framer
/// per object. One object is a one-element slice.
///
/// Each object is `(key, chunks)`: the key because the codec is resolved from
/// it per object, and the chunks already split the way a fetcher would
/// deliver them, already compressed if `compression` says so, since
/// compressing here would count the compressor rather than the decompressor.
///
/// The framer's record sequence is a pure function of the bytes it is given,
/// so a chunk list starting part-way through a record frames that partial
/// line as a record. [`frame_range`] is the entry a byte-range split takes.
///
/// # Errors
///
/// Whatever the decompressor or the framer reports: a truncated stream, a
/// corrupt frame, or a record over the framer's cap.
pub fn frame_objects(
    compression: Compression,
    make_framer: MakeFramer,
    objects: &[(String, Vec<Bytes>)],
) -> io::Result<usize> {
    let mut framer = ObjectFramer::new(make_framer);
    let mut records = 0;
    for (key, chunks) in objects {
        framer.begin_object(Codec::resolve(compression, key))?;
        for chunk in chunks {
            // A refcount bump, as the lane's own hand-off is.
            framer.push_chunk(chunk.clone())?;
            // Drained per decode step, because the lane drains at the top of
            // every poll-loop iteration rather than at end of object. Draining
            // only at the end would hold a whole object's records live at
            // once and report a DHAT peak the source never reaches. DHAT is
            // attached to this bench, so that shape is part of what it
            // publishes.
            loop {
                while framer.pop_record().is_some() {
                    records += 1;
                }
                if !framer.has_pending_input() {
                    break;
                }
                framer.pump()?;
            }
        }
        framer.finish_object()?;
        // The tail: `finish_object` completes an unterminated final record.
        // A record never spans an object boundary, so the queue is empty
        // before the next `begin_object`.
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    Ok(records)
}

/// Read the byte range `[start, end)` of one uncompressed object as a
/// byte-range split does, returning how many records it owns.
///
/// The object is fetched in the windows the fetcher schedules for
/// `range_bytes` and `chunk_bytes`, each copied out of `object` as a GET would
/// deliver it. The owned bytes are trimmed out of each window and framed in
/// `chunk_bytes` pieces through one framer.
///
/// # Errors
///
/// Whatever the framer reports, such as a record over its cap.
///
/// # Panics
///
/// If `start >= end` or `end` is past the object.
pub fn frame_range(
    make_framer: MakeFramer,
    object: &[u8],
    start: u64,
    end: u64,
    delimiter: u8,
    range_bytes: u64,
    chunk_bytes: usize,
) -> io::Result<usize> {
    let size = object.len() as u64;
    assert!(
        start < end && end <= size,
        "[{start}, {end}) is not a range of the object"
    );
    let mut trim = RangeTrim::new(SplitRange::new(start, end, delimiter), size);
    let mut framer = ObjectFramer::new(make_framer);
    let mut records = 0;
    framer.begin_object(Codec::Plain)?;
    let mut pos = trim.first_byte();
    while let Some(window_end) = trim.next_window(pos, range_bytes, chunk_bytes as u64) {
        let window = Bytes::copy_from_slice(&object[pos as usize..window_end as usize]);
        let mut owned = trim.feed(pos, window);
        while !owned.is_empty() {
            let chunk = owned.split_to(owned.len().min(chunk_bytes));
            framer.push_chunk(chunk)?;
            while framer.pop_record().is_some() {
                records += 1;
            }
            debug_assert!(
                !framer.has_pending_input(),
                "a plain chunk decodes in one step"
            );
        }
        pos = window_end;
    }
    framer.finish_object()?;
    while framer.pop_record().is_some() {
        records += 1;
    }
    Ok(records)
}

/// The member count of each unit the lister packs `objects` into at a split
/// target of `target_bytes`, in listing order.
///
/// Each object is `(key, size)` and carries an ETag, as a listed object does.
/// The compression is resolved from each key and the framer cuts on a newline,
/// as the NDJSON framer does. An object cut into ranges yields one unit per
/// range, each with one member.
///
/// For a bench whose cases divide work over pipeline threads: a thread takes
/// whole units, so the case's setup can check the share is even against the
/// packing the lister does rather than a copy of it.
///
/// # Panics
///
/// Panics when a unit's descriptor cannot be built, which no listed object
/// causes.
#[must_use]
pub fn pack_members(objects: &[(&str, u64)], target_bytes: u64) -> Vec<usize> {
    let mut packer = crate::unit::Packer::new(crate::split::Packing {
        target_bytes,
        compression: Compression::Auto,
        delimiter: Some(b'\n'),
    });
    let mut members = Vec::new();
    for (key, size) in objects {
        let entry = crate::fetch::ObjectEntry {
            key: (*key).to_owned(),
            size: *size,
            etag: Some("\"etag\"".to_owned()),
            last_modified_ms: 0,
        };
        for minted in packer.push(entry) {
            members.push(minted.expect("a listed object mints its unit").objects);
        }
    }
    if let Some(minted) = packer.close() {
        members.push(minted.expect("a listed object mints its unit").objects);
    }
    members
}

#[cfg(test)]
mod tests {
    use super::pack_members;

    /// Pins that objects pack until the next would pass the target.
    #[test]
    fn objects_pack_until_the_next_would_pass_the_target() {
        let objects = [("a.ndjson", 10), ("b.ndjson", 10), ("c.ndjson", 10)];
        assert_eq!(pack_members(&objects, 25), vec![2, 1]);
    }

    /// Pins that a unit closes at sixteen members whatever room it has left.
    #[test]
    fn a_unit_closes_at_sixteen_members() {
        let keys: Vec<String> = (0..17).map(|i| format!("o{i:02}.ndjson.zst")).collect();
        let objects: Vec<(&str, u64)> = keys.iter().map(|k| (k.as_str(), 1)).collect();
        assert_eq!(pack_members(&objects, 1 << 20), vec![16, 1]);
    }

    /// Pins that an uncompressed object over the target is cut into ranges,
    /// one single-member unit each, and a zstd one is read whole.
    #[test]
    fn only_an_uncompressed_object_over_the_target_is_cut() {
        let cut = pack_members(&[("big.ndjson", 4 << 20)], 1 << 20);
        assert!(cut.len() > 1 && cut.iter().all(|m| *m == 1), "{cut:?}");
        assert_eq!(
            pack_members(&[("big.ndjson.zst", 4 << 20)], 1 << 20),
            vec![1]
        );
    }
}
