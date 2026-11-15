//! Work units of the feed path and their parts: a unit a lister publishes
//! names one object, one byte range of one object, or up to
//! [`PACK_MEMBERS`] small objects read whole on one lane, and a storage
//! notification, a `Records[]` message or an EventBridge event, is one unit
//! with one part per non-empty created object.
//! [`Packer`] packs a listing's small objects into shared units.
//!
//! [`parts_of`] decides whether an object is read whole or cut, by the rule
//! the lister applies to a listing ([`Packing::delimiter_for`] and
//! [`tile`]), and [`units`] mints one feed unit per part. The reader cuts a
//! whole-object unit through the same [`Cursor`], so a unit's ranges are the
//! same whichever side cuts them. [`decode`] reads a delivery's body, a
//! descriptor or a notification, into the parts the reader opens, and
//! [`re_enqueue_body`] writes the notification a worker publishes for a key
//! overwritten while it was read.

use crate::fetch::{ObjectEntry, Overwrite};
use crate::framer::Codec;
use crate::listing::addressable;
use crate::split::{
    DescriptorObject, MAX_UNIT_MEMBERS, PACK_ETAG_MAX_BYTES, PACK_KEY_MAX_BYTES, PACK_MEMBERS,
    Packing, SplitDescriptor, SplitError, SplitRange, split_id_for, split_id_for_range, tile,
};
use object_store::path::Path;
use serde::Deserialize;
use spate_core::feed::Unit;
use std::iter::Peekable;
use std::sync::Arc;

/// Where a part's unit came from, which decides how a failed read of it is
/// handled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Origin {
    /// A descriptor a lister published.
    Lister,
    /// A storage notification, or a unit a worker re-enqueued after an
    /// overwrite.
    Notification {
        /// The object version the notification names. `None` reads the key.
        version: Option<String>,
        /// How many times the key's unit has been re-enqueued after an
        /// overwrite.
        re_enqueues: u32,
    },
}

/// One part of a unit: its objects read whole, or one byte range of one object.
pub(crate) struct Part {
    /// The objects the part reads, shared by every part of the unit.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The byte range read, or `None` for the whole object.
    pub(crate) range: Option<SplitRange>,
    /// Where the part's unit came from.
    pub(crate) origin: Origin,
}

/// The parts of a unit's objects, yielded lazily so an object cut into many
/// ranges holds one range at a time.
pub(crate) struct Cursor {
    /// The objects every part clones.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The ranges still to yield, or `None` when the object is read whole.
    ranges: Option<Peekable<Box<dyn Iterator<Item = SplitRange> + Send>>>,
    /// The whole part is still to yield.
    whole: bool,
    /// Where the unit came from, given to every part.
    origin: Origin,
}

impl Cursor {
    /// A cursor yielding `object` whole, once.
    fn whole(object: Arc<Vec<ObjectEntry>>, origin: Origin) -> Cursor {
        Cursor {
            object,
            ranges: None,
            whole: true,
            origin,
        }
    }

    /// A cursor yielding the one byte `range` of `object`.
    fn range(object: Arc<Vec<ObjectEntry>>, range: SplitRange, origin: Origin) -> Cursor {
        Cursor::ranges(object, Box::new(std::iter::once(range)), origin)
    }

    fn ranges(
        object: Arc<Vec<ObjectEntry>>,
        ranges: Box<dyn Iterator<Item = SplitRange> + Send>,
        origin: Origin,
    ) -> Cursor {
        Cursor {
            object,
            ranges: Some(ranges.peekable()),
            whole: false,
            origin,
        }
    }

    /// The key of the cursor's first object, its only one unless a
    /// descriptor names several.
    pub(crate) fn key(&self) -> &str {
        &self.object[0].key
    }

    /// Whether every part has been yielded.
    pub(crate) fn is_exhausted(&mut self) -> bool {
        match &mut self.ranges {
            Some(ranges) => ranges.peek().is_none(),
            None => !self.whole,
        }
    }
}

impl Iterator for Cursor {
    type Item = Part;

    fn next(&mut self) -> Option<Part> {
        let range = match &mut self.ranges {
            Some(ranges) => Some(ranges.next()?),
            None => {
                if !std::mem::take(&mut self.whole) {
                    return None;
                }
                None
            }
        };
        Some(Part {
            object: Arc::clone(&self.object),
            range,
            origin: self.origin.clone(),
        })
    }
}

/// The parts of the one object in `object`, a notification's object or a
/// one-member descriptor's: the ranges [`tile`] cuts when
/// [`Packing::delimiter_for`] accepts the object and its key is
/// [`addressable`], and the whole object otherwise. Every part carries
/// `origin`.
pub(crate) fn parts_of(object: Arc<Vec<ObjectEntry>>, packing: Packing, origin: Origin) -> Cursor {
    debug_assert_eq!(object.len(), 1, "a part names one object");
    let entry = &object[0];
    match packing
        .delimiter_for(entry)
        .filter(|_| addressable(&entry.key))
    {
        Some(delimiter) => {
            let ranges = tile(entry.size, packing.target_bytes, delimiter);
            Cursor::ranges(object, Box::new(ranges), origin)
        }
        None => Cursor::whole(object, origin),
    }
}

/// The feed units of one listed object, one per part, minted as the
/// iterator is drawn, so an object cut into many ranges holds one at a time.
///
/// A whole part's id is [`split_id_for`] over its key and ETag, and a range
/// part's is [`split_id_for_range`], so an id depends only on the key, the
/// ETag, the range and the range configuration.
///
/// # Errors
///
/// An item is an error when a descriptor or an id cannot be built, which no
/// listed object causes.
pub(crate) fn units(
    entry: ObjectEntry,
    packing: Packing,
) -> impl Iterator<Item = Result<Unit, SplitError>> {
    let object = DescriptorObject {
        key: entry.key.clone(),
        size: entry.size,
        etag: entry.etag.clone(),
        last_modified_ms: entry.last_modified_ms,
    };
    parts_of(Arc::new(vec![entry]), packing, Origin::Lister).map(move |part| {
        let (id, descriptor) = match part.range {
            None => (
                split_id_for([(object.key.as_str(), object.etag.as_deref())])?,
                SplitDescriptor::new(vec![object.clone()]),
            ),
            Some(range) => (
                split_id_for_range(
                    &object.key,
                    object
                        .etag
                        .as_deref()
                        .expect("an object is cut only when it has an ETag"),
                    range,
                )?,
                SplitDescriptor::with_range(object.clone(), range),
            ),
        };
        Ok(Unit::new(id, descriptor.encode()?))
    })
}

/// A unit the lister publishes, with the first object it names and how many
/// it names.
#[derive(Debug)]
pub(crate) struct Minted {
    pub(crate) unit: Unit,
    pub(crate) first_key: String,
    pub(crate) objects: usize,
    /// The objects this unit is the first to name: its members, or one for
    /// an object's first range and none for its later ones.
    pub(crate) listed: usize,
}

/// Packs listed objects read whole into shared units, in listing order, with
/// one unit open at a time.
///
/// An object shares a unit when it is read whole, its key is
/// [`addressable`] and at most [`PACK_KEY_MAX_BYTES`], and its ETag is absent
/// or can be sent in `If-Match`. The open unit closes before an object whose
/// bytes would take it past the split target, and once it holds
/// [`PACK_MEMBERS`] objects. Any other object closes the open unit and is
/// minted by [`units`] alone. Which objects share a unit depends only on the
/// sequence of keys, sizes and ETags pushed, so the same listing packs the
/// same way.
pub(crate) struct Packer {
    packing: Packing,
    open: Vec<ObjectEntry>,
    open_bytes: u64,
}

impl Packer {
    pub(crate) fn new(packing: Packing) -> Packer {
        Packer {
            packing,
            open: Vec::new(),
            open_bytes: 0,
        }
    }

    /// Add `entry` and return the units it closes or mints, in listing
    /// order. A cut object's ranges are minted as the iterator is drawn.
    ///
    /// # Errors
    ///
    /// An item is an error when a descriptor or an id cannot be built, which
    /// no listed object causes.
    pub(crate) fn push(
        &mut self,
        entry: ObjectEntry,
    ) -> impl Iterator<Item = Result<Minted, SplitError>> + use<> {
        let shared = self.shares(&entry);
        let before =
            if !shared || self.open_bytes.saturating_add(entry.size) > self.packing.target_bytes {
                self.close()
            } else {
                None
            };
        let (alone, after) = if shared {
            self.open_bytes = self.open_bytes.saturating_add(entry.size);
            self.open.push(entry);
            let full = if self.open.len() == PACK_MEMBERS {
                self.close()
            } else {
                None
            };
            (None, full)
        } else {
            (Some(entry), None)
        };
        let packing = self.packing;
        before.into_iter().chain(after).chain(
            alone
                .into_iter()
                .flat_map(move |entry| alone_units(entry, packing)),
        )
    }

    /// Mint the open unit, or `None` when no object is waiting in it.
    ///
    /// # Errors
    ///
    /// An error when a descriptor or an id cannot be built, which no listed
    /// object causes.
    pub(crate) fn close(&mut self) -> Option<Result<Minted, SplitError>> {
        if self.open.is_empty() {
            return None;
        }
        let mut members = std::mem::take(&mut self.open);
        self.open_bytes = 0;
        if members.len() == 1 {
            // Minted as `units` mints it, so its id and body are an unpacked
            // object's.
            let entry = members.pop().expect("one member");
            return alone_units(entry, self.packing).next();
        }
        Some(packed_unit(&members))
    }

    fn shares(&self, entry: &ObjectEntry) -> bool {
        self.packing.delimiter_for(entry).is_none()
            && addressable(&entry.key)
            && entry.key.len() <= PACK_KEY_MAX_BYTES
            && entry
                .etag
                .as_deref()
                .is_none_or(|etag| etag.len() <= PACK_ETAG_MAX_BYTES && sendable(etag))
    }
}

/// [`units`] over one object, each with that object as its first.
pub(crate) fn alone_units(
    entry: ObjectEntry,
    packing: Packing,
) -> impl Iterator<Item = Result<Minted, SplitError>> {
    let first_key = entry.key.clone();
    units(entry, packing).enumerate().map(move |(i, unit)| {
        unit.map(|unit| Minted {
            unit,
            first_key: first_key.clone(),
            objects: 1,
            listed: usize::from(i == 0),
        })
    })
}

/// One unit naming `members` whole, in order.
fn packed_unit(members: &[ObjectEntry]) -> Result<Minted, SplitError> {
    let id = split_id_for(members.iter().map(|e| (e.key.as_str(), e.etag.as_deref())))?;
    let descriptor = SplitDescriptor::new(
        members
            .iter()
            .map(|e| DescriptorObject {
                key: e.key.clone(),
                size: e.size,
                etag: e.etag.clone(),
                last_modified_ms: e.last_modified_ms,
            })
            .collect(),
    );
    Ok(Minted {
        unit: Unit::new(id, descriptor.encode()?),
        first_key: members[0].key.clone(),
        objects: members.len(),
        listed: members.len(),
    })
}

/// Where the objects a notification names must be for the source to read
/// them.
#[derive(Clone, Debug)]
pub(crate) struct Scope {
    /// The source's bucket. `None` matches no record.
    pub(crate) bucket: Option<String>,
    /// The source's prefix within the bucket; empty for the whole bucket.
    pub(crate) prefix: Path,
}

/// What the reader does with one delivery.
pub(crate) enum Decoded {
    /// Read these parts, then acknowledge the delivery once.
    Read(Vec<Cursor>),
    /// Acknowledge the delivery with nothing to read, for the reason given.
    Acknowledge(&'static str),
    /// Fail the delivery back to the feed, for the reason given.
    Fail(String),
}

/// Read a delivery's `body`: a [`SplitDescriptor`] a lister published, or a
/// storage notification.
///
/// A notification is an S3 `Records[]` message, the same records inside the
/// `{"EventName","Key","Records"}` envelope, an S3 event EventBridge
/// delivers, or the S3 test event, which is acknowledged. A body with a
/// `Records` array is read as records whatever else it carries.
///
/// A record with no `eventName`, or one that is not a string, fails the whole
/// message. Each record whose `eventName` is not `ObjectCreated:*`, with or
/// without an `s3:` prefix, is dropped whatever its bucket. A created record
/// that does not deserialise, or with a key that does not decode, fails the
/// whole message. Its key is read as written when the record's `eventSource`
/// is exactly `ceph:s3`, and is otherwise form-decoded (`+` is a space, then
/// percent-decoding as UTF-8), so only a form-decoded key can fail to decode.
/// An absent or JSON null `eventSource` is form-decoded. A record whose
/// `eventSource` is any other value that is not a string fails the whole
/// message. A record's `spateReEnqueues`, which only a unit
/// [`re_enqueue_body`] wrote carries, is how many times its key was
/// re-enqueued; absent or null is 0, and any value other than a `u32` fails
/// the whole message.
///
/// An EventBridge event is a body whose `source` is the string `aws.s3`. Its
/// `detail-type` must be a string. An `Object Created` event names one
/// object, read from `detail.bucket.name` and `detail.object`, with its key
/// as written, its ETag from `etag`, its version from `version-id`, its event
/// time from the top-level `time`, and a re-enqueue count of 0. It fails
/// unless its `detail.event-version` is a string of major version 1: `1.`
/// followed by one or more digits. An event of a type in
/// [`EVENTBRIDGE_ACKNOWLEDGED`] is acknowledged whatever its bucket, and any
/// other type fails. An `Object Created` event that does not deserialise,
/// a field of the wrong type included, fails.
///
/// The object a created record or an `Object Created` event names fails its
/// message when it is in another bucket, has a key outside `scope`'s prefix,
/// or has no size. An object of size 0 is then dropped. The rest fail the
/// message when [`addressable`] refuses the key, when the ETag cannot be an
/// `If-Match` value, or when the version is longer than
/// [`MAX_VERSION_BYTES`] or holds a space or a control byte. Otherwise the
/// object becomes one part with its ETag quoted. A version that is a string
/// other than `""` and `"null"` names the version the part reads; absent,
/// JSON null, `""` and `"null"` read the key. A message with no part left is
/// acknowledged.
pub(crate) fn decode(body: &[u8], scope: &Scope, packing: Packing) -> Decoded {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Decoded::Fail(
            "the unit's body is neither a descriptor nor a storage notification".to_owned(),
        );
    };
    if value.get("v").is_some() {
        return match descriptor_parts(body, packing) {
            Ok(cursor) => Decoded::Read(vec![cursor]),
            Err(reason) => Decoded::Fail(reason),
        };
    }
    if value.get("Event").and_then(serde_json::Value::as_str) == Some("s3:TestEvent") {
        return Decoded::Acknowledge("a storage test event");
    }
    let Some(records) = value.get("Records").and_then(serde_json::Value::as_array) else {
        if value.get("source").and_then(serde_json::Value::as_str) == Some(EVENTBRIDGE_SOURCE) {
            return decode_event(&value, scope, packing);
        }
        return Decoded::Fail(
            "the unit's body is neither a descriptor nor a storage notification".to_owned(),
        );
    };
    let mut parts = Vec::with_capacity(records.len());
    for (i, record) in records.iter().enumerate() {
        match record_part(record, scope, packing) {
            Ok(Some(cursor)) => parts.push(cursor),
            Ok(None) => {}
            Err(reason) => return Decoded::Fail(format!("notification record {i}: {reason}")),
        }
    }
    if parts.is_empty() {
        Decoded::Acknowledge(NOTHING_TO_READ)
    } else {
        Decoded::Read(parts)
    }
}

/// The longest version id a notification may name.
pub(crate) const MAX_VERSION_BYTES: usize = 1024;

/// The `eventSource` of a record Ceph RGW writes, whose key is the object's
/// name unencoded.
const CEPH_SOURCE: &str = "ceph:s3";

/// The field of a re-enqueued unit's record that counts its re-enqueues.
const RE_ENQUEUES: &str = "spateReEnqueues";

/// Why a notification that names no object to read is acknowledged.
const NOTHING_TO_READ: &str = "a notification that names no object to read";

/// The `source` of an S3 event EventBridge delivers.
const EVENTBRIDGE_SOURCE: &str = "aws.s3";

/// The `detail-type` of an EventBridge event for a created object.
const OBJECT_CREATED: &str = "Object Created";

/// The other `detail-type`s S3 sends to EventBridge, as AWS maps its event
/// types. Each names no new object content, so the source acknowledges it
/// with nothing to read.
pub(crate) const EVENTBRIDGE_ACKNOWLEDGED: [&str; 12] = [
    "Object Deleted",
    "Object Restore Initiated",
    "Object Restore Completed",
    "Object Restore Expired",
    "Object Storage Class Changed",
    "Object Access Tier Changed",
    "Object Tags Added",
    "Object Tags Deleted",
    "Object ACL Updated",
    "Object Annotation Created",
    "Object Annotation Removed",
    "Object Retention Updated",
];

/// The fields of a notification record the source reads.
#[derive(Deserialize)]
struct Record {
    #[serde(rename = "eventSource", default)]
    event_source: Option<String>,
    #[serde(rename = "eventTime", default)]
    event_time: Option<String>,
    #[serde(rename = "spateReEnqueues", default)]
    re_enqueues: Option<u32>,
    s3: RecordS3,
}

#[derive(Deserialize)]
struct RecordS3 {
    bucket: RecordBucket,
    object: RecordObject,
}

#[derive(Deserialize)]
struct RecordBucket {
    name: String,
}

#[derive(Deserialize)]
struct RecordObject {
    key: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(rename = "eTag", default)]
    etag: Option<String>,
    #[serde(rename = "versionId", default)]
    version_id: Option<String>,
}

/// The fields of an EventBridge `Object Created` event the source reads.
#[derive(Deserialize)]
struct Event {
    #[serde(default)]
    time: Option<String>,
    detail: EventDetail,
}

#[derive(Deserialize)]
struct EventDetail {
    bucket: RecordBucket,
    object: EventObject,
}

#[derive(Deserialize)]
struct EventObject {
    key: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(rename = "version-id", default)]
    version_id: Option<String>,
}

/// The object a notification names, with its key as the read uses it.
struct Announced {
    bucket: String,
    key: String,
    size: Option<u64>,
    etag: Option<String>,
    version_id: Option<String>,
    event_time: Option<String>,
    re_enqueues: u32,
}

/// The part one notification record names, `None` for a record dropped
/// without one, or why the record fails its message.
fn record_part(
    record: &serde_json::Value,
    scope: &Scope,
    packing: Packing,
) -> Result<Option<Cursor>, String> {
    let Some(event) = record.get("eventName").and_then(serde_json::Value::as_str) else {
        return Err("the record's `eventName` is missing or not a string".to_owned());
    };
    if !event
        .strip_prefix("s3:")
        .unwrap_or(event)
        .starts_with("ObjectCreated:")
    {
        return Ok(None);
    }
    let record =
        Record::deserialize(record).map_err(|e| format!("the record is unreadable: {e}"))?;
    // A re-enqueued unit's record carries no `eventSource`, so the key
    // `re_enqueue_body` form-encoded is decoded whatever store announced it.
    let key = if record.event_source.as_deref() == Some(CEPH_SOURCE) {
        record.s3.object.key
    } else {
        form_decoded(&record.s3.object.key)?
    };
    announced_part(
        Announced {
            bucket: record.s3.bucket.name,
            key,
            size: record.s3.object.size,
            etag: record.s3.object.etag,
            version_id: record.s3.object.version_id,
            event_time: record.event_time,
            re_enqueues: record.re_enqueues.unwrap_or(0),
        },
        scope,
        packing,
    )
}

/// What the source does with an EventBridge event, `value`, whose `source`
/// is [`EVENTBRIDGE_SOURCE`].
fn decode_event(value: &serde_json::Value, scope: &Scope, packing: Packing) -> Decoded {
    let fail = |reason: String| Decoded::Fail(format!("EventBridge event: {reason}"));
    let Some(detail_type) = value.get("detail-type").and_then(serde_json::Value::as_str) else {
        return fail("the event's `detail-type` is missing or not a string".to_owned());
    };
    if detail_type != OBJECT_CREATED {
        if EVENTBRIDGE_ACKNOWLEDGED.contains(&detail_type) {
            return Decoded::Acknowledge("an EventBridge event that creates no object");
        }
        return fail(format!(
            "the detail type \"{detail_type}\" is not one this source reads"
        ));
    }
    match event_part(value, scope, packing) {
        Ok(Some(cursor)) => Decoded::Read(vec![cursor]),
        Ok(None) => Decoded::Acknowledge(NOTHING_TO_READ),
        Err(reason) => fail(reason),
    }
}

/// The part an `Object Created` event names, `None` for an empty object, or
/// why the event fails.
fn event_part(
    value: &serde_json::Value,
    scope: &Scope,
    packing: Packing,
) -> Result<Option<Cursor>, String> {
    // AWS changes the major version for a change that is not backward
    // compatible, which could move a field this source reads as optional.
    let version = value
        .pointer("/detail/event-version")
        .and_then(serde_json::Value::as_str);
    if !version.is_some_and(|v| {
        v.strip_prefix("1.")
            .is_some_and(|minor| !minor.is_empty() && minor.bytes().all(|b| b.is_ascii_digit()))
    }) {
        return Err("the event's `detail.event-version` is not a version 1 string".to_owned());
    }
    let event = Event::deserialize(value).map_err(|e| format!("the event is unreadable: {e}"))?;
    announced_part(
        Announced {
            bucket: event.detail.bucket.name,
            key: event.detail.object.key,
            size: event.detail.object.size,
            etag: event.detail.object.etag,
            version_id: event.detail.object.version_id,
            event_time: event.time,
            re_enqueues: 0,
        },
        scope,
        packing,
    )
}

/// The part an announced object names, `None` for an empty object, or why
/// its notification fails.
fn announced_part(
    object: Announced,
    scope: &Scope,
    packing: Packing,
) -> Result<Option<Cursor>, String> {
    let Announced {
        bucket,
        key,
        size,
        etag,
        version_id,
        event_time,
        re_enqueues,
    } = object;
    if scope.bucket.as_deref() != Some(bucket.as_str()) {
        return Err(format!(
            "the object is in the bucket \"{bucket}\", which this source does not read"
        ));
    }
    let prefix = scope.prefix.as_ref();
    if !prefix.is_empty()
        && !key
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
    {
        return Err(format!(
            "the key \"{key}\" is outside the source's prefix \"{prefix}/\""
        ));
    }
    let Some(size) = size else {
        return Err(format!("the object \"{key}\" has no size"));
    };
    // An empty object has no records, and is read without a request.
    if size == 0 {
        return Ok(None);
    }
    if !addressable(&key) {
        return Err(format!(
            "the key \"{key}\" cannot be read under its own name through `object_store`"
        ));
    }
    if etag.as_deref().is_some_and(|etag| !sendable(etag)) {
        return Err(format!(
            "the object \"{key}\" has an ETag that cannot be sent in `If-Match`"
        ));
    }
    let version = match version_id {
        // Ceph RGW writes `""` for a bucket without versioning, and `"null"`
        // names an object stored without a version id.
        Some(version) if version.is_empty() || version == "null" => None,
        Some(version) if version.len() > MAX_VERSION_BYTES => {
            return Err(format!(
                "the object \"{key}\" has a version id longer than {MAX_VERSION_BYTES} bytes"
            ));
        }
        // A space is signed as `%20` but sent as `+` in the query, so the
        // store would refuse the request as unauthorised and fail the
        // pipeline. A control byte cannot be a version either.
        Some(version) if version.bytes().any(|b| b == b' ' || b.is_ascii_control()) => {
            return Err(format!(
                "the object \"{key}\" has a version id holding a space or a control byte"
            ));
        }
        version => version,
    };
    let last_modified_ms = event_time
        .as_deref()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map_or(0, |t| t.timestamp_millis());
    let entry = ObjectEntry {
        key,
        size,
        etag: etag.map(quoted),
        last_modified_ms,
    };
    let origin = Origin::Notification {
        version,
        re_enqueues,
    };
    Ok(Some(parts_of(Arc::new(vec![entry]), packing, origin)))
}

/// The body of the unit a worker publishes for `key` in `bucket` after the
/// key was overwritten while it was read: one `ObjectCreated:Put` record at
/// the content `overwrite` names, counting `re_enqueues`.
///
/// [`decode`] reads it as a notification: its key form-encoded under no
/// `eventSource`, its ETag quoted when the store gave one, and no
/// `versionId`. The body is ASCII
/// when the ETag is, as one read from a header always is.
pub(crate) fn re_enqueue_body(bucket: &str, overwrite: &Overwrite, re_enqueues: u32) -> Vec<u8> {
    let mut object = serde_json::json!({
        "key": form_encoded(&overwrite.key),
        "size": overwrite.size,
    });
    if let Some(etag) = &overwrite.etag {
        object["eTag"] = etag.as_str().into();
    }
    let mut record = serde_json::json!({
        "eventName": "ObjectCreated:Put",
        RE_ENQUEUES: re_enqueues,
        "s3": { "bucket": { "name": bucket }, "object": object },
    });
    if let Some(time) = chrono::DateTime::from_timestamp_millis(overwrite.last_modified_ms) {
        record["eventTime"] = time
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            .into();
    }
    let body = serde_json::json!({ "Records": [record] });
    serde_json::to_vec(&body).expect("a JSON value always serialises")
}

/// `key` encoded as `application/x-www-form-urlencoded`, the inverse of
/// [`form_decoded`]: every byte other than an ASCII letter, a digit, `-`,
/// `.`, `_`, `~` and `/` is written `%HH`.
fn form_encoded(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `etag` in the quoted form a listing reports and `If-Match` compares,
/// adding the quotes a notification leaves out.
pub(crate) fn quoted(etag: String) -> String {
    if etag.len() >= 2 && etag.starts_with('"') && etag.ends_with('"') {
        etag
    } else {
        format!("\"{etag}\"")
    }
}

/// Whether `etag` can be an `If-Match` header value: `http` refuses a
/// control byte other than a tab, and `object_store` panics building a
/// request with a refused header.
fn sendable(etag: &str) -> bool {
    etag.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f))
}

/// `key` decoded as `application/x-www-form-urlencoded`: `+` is a space, and
/// each `%HH` is a byte, the bytes read as UTF-8.
fn form_decoded(key: &str) -> Result<String, String> {
    let hex = |b: u8| char::from(b).to_digit(16);
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let byte = match (
                    bytes.get(i + 1).copied().and_then(hex),
                    bytes.get(i + 2).copied().and_then(hex),
                ) {
                    (Some(high), Some(low)) => (high * 16 + low) as u8,
                    _ => return Err(format!("the key \"{key}\" has a malformed `%` escape")),
                };
                out.push(byte);
                i += 2;
            }
            b => out.push(b),
        }
        // Decoding never makes the key longer. The check turns a loop that
        // stops advancing `i`, as a cargo-mutants mutant of either step does,
        // into a panic before it exhausts the test host's memory.
        debug_assert!(out.len() <= bytes.len());
        i += 1;
    }
    String::from_utf8(out).map_err(|_| format!("the key \"{key}\" does not decode as UTF-8"))
}

/// The parts of the objects a descriptor body names, or why the unit cannot
/// be read.
///
/// One member is read as [`parts_of`] cuts it, or as the byte range the
/// descriptor names. Two or more are one whole part, read member by member
/// on one lane. A unit naming no member, more than [`MAX_UNIT_MEMBERS`], or a
/// member whose ETag cannot be sent in `If-Match` is refused, and so is a
/// ranged unit whose delimiter is not the framer's or whose object is
/// compressed.
fn descriptor_parts(body: &[u8], packing: Packing) -> Result<Cursor, String> {
    let descriptor = SplitDescriptor::decode(body)
        .map_err(|e| format!("the unit's body is not a descriptor: {}", e.reason))?;
    let entries = descriptor.to_entries();
    if entries.is_empty() || entries.len() > MAX_UNIT_MEMBERS {
        return Err(format!(
            "the unit names {} objects; this source reads 1 to {MAX_UNIT_MEMBERS} per unit",
            entries.len()
        ));
    }
    if let Some(object) = entries
        .iter()
        .find(|e| e.etag.as_deref().is_some_and(|etag| !sendable(etag)))
    {
        return Err(format!(
            "the unit's ETag for \"{}\" cannot be sent in `If-Match`",
            object.key
        ));
    }
    let object = match <[ObjectEntry; 1]>::try_from(entries) {
        Ok([object]) => object,
        // `SplitDescriptor::decode` refuses a range on more than one member,
        // so these are read whole.
        Err(entries) => return Ok(Cursor::whole(Arc::new(entries), Origin::Lister)),
    };
    let Some(range) = descriptor.range else {
        return Ok(parts_of(Arc::new(vec![object]), packing, Origin::Lister));
    };
    if packing.delimiter != Some(range.delimiter) {
        return Err(format!(
            "the unit splits its object on byte {:#04x}, but the source's framer {}",
            range.delimiter,
            match packing.delimiter {
                Some(d) => format!("resyncs on byte {d:#04x}"),
                None => "declares no resync delimiter".to_owned(),
            }
        ));
    }
    let codec = Codec::resolve(packing.compression, &object.key);
    if codec != Codec::Plain {
        return Err(format!(
            "the unit names a byte range of \"{}\", which the source decodes as {}; a byte \
             range needs an uncompressed object",
            object.key,
            format!("{codec:?}").to_lowercase()
        ));
    }
    Ok(Cursor::range(Arc::new(vec![object]), range, Origin::Lister))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::MAX_SUBDIVIDABLE_BYTES;
    use proptest::prelude::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    const EXTENSIONS: [&str; 6] = ["", ".ndjson", ".gz", ".gzip", ".zst", ".zstd"];

    /// Key shapes: one `Path::parse` keeps, three it rejects and two it
    /// rewrites.
    fn key(i: usize, shape: u8, ext: &str) -> (String, bool) {
        match shape {
            0 => (format!("a/k{i}{ext}"), false),
            1 => (format!("a//k{i}{ext}"), true),
            2 => (format!("a/../k{i}{ext}"), true),
            3 => (format!("a/k\u{1}{i}{ext}"), true),
            4 => (format!("/a/k{i}{ext}"), true),
            _ => (format!("a/k{i}{ext}/"), true),
        }
    }

    fn compression(n: u8) -> Compression {
        match n {
            0 => Compression::Auto,
            1 => Compression::None,
            2 => Compression::Gzip,
            _ => Compression::Zstd,
        }
    }

    /// Whether the object is read uncompressed, decided without
    /// `Codec::resolve`.
    fn plain(compression: Compression, key: &str) -> bool {
        match compression {
            Compression::None => true,
            Compression::Auto => ![".gz", ".gzip", ".zst", ".zstd"]
                .iter()
                .any(|ext| key.ends_with(ext)),
            _ => false,
        }
    }

    fn size(target: u64, shape: u8, random: u64) -> u64 {
        match shape {
            0 => target,
            1 => target + 1,
            2 if target >= GIB => MAX_SUBDIVIDABLE_BYTES - 1,
            3 if target >= GIB => MAX_SUBDIVIDABLE_BYTES,
            4 if target >= GIB => MAX_SUBDIVIDABLE_BYTES + 1,
            5 => 0,
            _ => random % (4 * target),
        }
    }

    fn target() -> impl Strategy<Value = u64> {
        prop_oneof![MIB..=64 * MIB, 16 * GIB..=64 * GIB]
    }

    /// Pins that an object is cut lazily. At a one-byte target the largest
    /// object the cut accepts has more ranges than memory can hold at once,
    /// and its cursor still yields the first two.
    #[test]
    fn the_cut_yields_one_range_at_a_time() {
        let packing = Packing {
            target_bytes: 1,
            compression: Compression::None,
            delimiter: Some(b'\n'),
        };
        let entry = ObjectEntry {
            key: "a.ndjson".into(),
            size: MAX_SUBDIVIDABLE_BYTES,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let mut cursor = parts_of(Arc::new(vec![entry]), packing, Origin::Lister);
        let first: Vec<(u64, u64)> = cursor
            .by_ref()
            .take(2)
            .map(|part| {
                let range = part.range.expect("the object is cut");
                (range.start, range.end)
            })
            .collect();
        assert_eq!(first, [(0, 1), (1, 2)]);
        assert!(!cursor.is_exhausted());
    }

    /// Pins that the cut marks each range with the framer's delimiter.
    #[test]
    fn the_cut_marks_each_range_with_the_framers_delimiter() {
        let packing = Packing {
            target_bytes: MIB,
            compression: Compression::None,
            delimiter: Some(b'\r'),
        };
        let entry = ObjectEntry {
            key: "a.csv".into(),
            size: 3 * MIB,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let delimiters: Vec<u8> = parts_of(Arc::new(vec![entry]), packing, Origin::Lister)
            .map(|part| part.range.expect("the object is cut").delimiter)
            .collect();
        assert_eq!(delimiters, [b'\r'; 3]);
    }

    /// The scope of a source over `s3://bucket/data/`.
    fn scope() -> Scope {
        Scope {
            bucket: Some("bucket".to_owned()),
            prefix: Path::from("data"),
        }
    }

    /// The scope of a source over the whole of `s3://bucket/`.
    fn whole_bucket() -> Scope {
        Scope {
            bucket: Some("bucket".to_owned()),
            prefix: Path::from(""),
        }
    }

    /// The packing of a source whose range target is 100 bytes.
    fn packing() -> Packing {
        Packing {
            target_bytes: 100,
            compression: Compression::Auto,
            delimiter: Some(b'\n'),
        }
    }

    /// One notification record, as AWS writes it.
    fn record(event: &str, bucket: &str, key: &str, size: Option<u64>) -> serde_json::Value {
        serde_json::json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventTime": "2026-10-19T12:00:00.000Z",
            "eventName": event,
            "s3": {
                "bucket": { "name": bucket },
                "object": { "key": key, "size": size, "eTag": "0123abcd", "sequencer": "0A1B" },
            },
        })
    }

    /// A created record for `key` in the source's bucket.
    fn created(key: &str, size: u64) -> serde_json::Value {
        record("ObjectCreated:Put", "bucket", key, Some(size))
    }

    fn message(records: &[serde_json::Value]) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "Records": records })).unwrap()
    }

    /// One part as `(key, etag, range)`.
    type Read = (String, Option<String>, Option<(u64, u64)>);

    /// Every part of every cursor `decoded` reads.
    fn read(decoded: Decoded) -> Vec<Read> {
        match decoded {
            Decoded::Read(cursors) => cursors
                .into_iter()
                .flatten()
                .map(|part| {
                    let entry = &part.object[0];
                    (
                        entry.key.clone(),
                        entry.etag.clone(),
                        part.range.map(|r| (r.start, r.end)),
                    )
                })
                .collect(),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    fn keys(decoded: Decoded) -> Vec<String> {
        read(decoded).into_iter().map(|(key, _, _)| key).collect()
    }

    fn failed(decoded: Decoded) -> String {
        match decoded {
            Decoded::Fail(reason) => reason,
            Decoded::Read(cursors) => panic!("read {} parts", cursors.len()),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
        }
    }

    fn acknowledged(decoded: Decoded) -> &'static str {
        match decoded {
            Decoded::Acknowledge(reason) => reason,
            Decoded::Read(cursors) => panic!("read {} parts", cursors.len()),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    fn decoded(records: &[serde_json::Value]) -> Decoded {
        decode(&message(records), &scope(), packing())
    }

    /// Pins the key decoding: `+` is a space, `%2F` and a raw `/` give the
    /// same key, a percent-encoded UTF-8 sequence decodes, and a malformed
    /// escape or bytes that are not UTF-8 fail the message.
    #[test]
    fn a_notification_key_is_form_decoded() {
        for key in [
            "data/dir%2Fhello+world.ndjson",
            "data/dir/hello+world.ndjson",
            "data%2Fdir%2Fhello%20world.ndjson",
        ] {
            assert_eq!(
                keys(decoded(&[created(key, 10)])),
                ["data/dir/hello world.ndjson"],
                "{key}"
            );
        }
        assert_eq!(
            keys(decoded(&[created("data/caf%C3%A9+%2B.ndjson", 10)])),
            ["data/café +.ndjson"]
        );
        for key in [
            "data/a%2",
            "data/a%zz.ndjson",
            "data/a%+1.ndjson",
            "data/a%",
        ] {
            let reason = failed(decoded(&[created(key, 10)]));
            assert!(reason.contains("malformed `%` escape"), "{key}: {reason}");
        }
        let reason = failed(decoded(&[created("data/%FF.ndjson", 10)]));
        assert!(reason.contains("does not decode as UTF-8"), "{reason}");
    }

    /// `record` with its `eventSource` set to `source`, or removed for
    /// `None`.
    fn from_source(mut record: serde_json::Value, source: Option<&str>) -> serde_json::Value {
        match source {
            Some(source) => record["eventSource"] = source.into(),
            None => {
                record
                    .as_object_mut()
                    .expect("a record is an object")
                    .remove("eventSource");
            }
        }
        record
    }

    /// A created record for `key` in the source's bucket, as Ceph RGW writes
    /// it.
    fn ceph(key: &str, size: u64) -> serde_json::Value {
        from_source(created(key, size), Some("ceph:s3"))
    }

    /// Pins that a record whose `eventSource` is exactly `ceph:s3` is read at
    /// its key as written, that any other source, none or JSON null
    /// form-decodes, and that any other `eventSource` that is not a string
    /// fails the message.
    #[test]
    fn a_ceph_notification_key_is_read_as_written() {
        for key in [
            "data/a+b c%25.ndjson",
            "data/a%2Fb.ndjson",
            "data/a%41.ndjson",
            "data/a%zz.ndjson",
            "data/c%",
            "data/é x.ndjson",
        ] {
            assert_eq!(keys(decoded(&[ceph(key, 10)])), [key], "{key}");
        }
        for source in [
            Some("aws:s3"),
            Some("rustfs:s3"),
            None,
            Some("Ceph:s3"),
            Some("ceph:s3 "),
            Some("ceph"),
        ] {
            let record = |key| from_source(created(key, 10), source);
            assert_eq!(
                keys(decoded(&[record("data/a+b c%25.ndjson")])),
                ["data/a b c%.ndjson"],
                "{source:?}"
            );
            assert_eq!(
                keys(decoded(&[record("data/a%41.ndjson")])),
                ["data/aA.ndjson"],
                "{source:?}"
            );
            let reason = failed(decoded(&[record("data/a%zz.ndjson")]));
            assert!(
                reason.contains("malformed `%` escape"),
                "{source:?}: {reason}"
            );
        }
        let mut null = created("data/a+b.ndjson", 10);
        null["eventSource"] = serde_json::Value::Null;
        assert_eq!(keys(decoded(&[null])), ["data/a b.ndjson"]);
        let mut numbered = created("data/a.ndjson", 10);
        numbered["eventSource"] = 1.into();
        let reason = failed(decoded(&[numbered]));
        assert!(reason.contains("the record is unreadable"), "{reason}");
    }

    /// Pins that a key read as written meets the prefix, size and path
    /// checks that follow decoding, and that each record of one message
    /// takes the rule its own `eventSource` names.
    #[test]
    fn a_ceph_key_meets_the_checks_that_follow_decoding() {
        let reason = failed(decoded(&[ceph("data%2Fx.ndjson", 10)]));
        assert!(
            reason.contains("outside the source's prefix \"data/\""),
            "{reason}"
        );
        for key in ["data//a.ndjson", "data/../x"] {
            let reason = failed(decoded(&[ceph(key, 10)]));
            assert!(
                reason.contains("cannot be read under its own name"),
                "{key}: {reason}"
            );
        }
        let body = message(&[ceph("a/%2Fb.ndjson", 10)]);
        assert_eq!(
            keys(decode(&body, &whole_bucket(), packing())),
            ["a/%2Fb.ndjson"]
        );
        acknowledged(decoded(&[ceph("data/a%zz", 0)]));
        assert_eq!(
            keys(decoded(&[
                ceph("data/a+b.ndjson", 10),
                created("data/a+b.ndjson", 10)
            ])),
            ["data/a+b.ndjson", "data/a b.ndjson"]
        );
    }

    /// Pins that a unit re-enqueued for an object Ceph RGW announced decodes
    /// to the key RGW wrote, though its body names no `eventSource`.
    #[test]
    fn a_re_enqueued_ceph_unit_reads_the_key_rgw_announced() {
        let key = "data/a+b%2Fc d%25.ndjson";
        let Decoded::Read(cursors) = decoded(&[ceph(key, 10)]) else {
            panic!("not read");
        };
        let parts: Vec<Part> = cursors.into_iter().flatten().collect();
        let entry = &parts[0].object[0];
        assert_eq!(entry.key, key);
        let overwrite = Overwrite {
            key: entry.key.clone(),
            etag: entry.etag.clone(),
            size: entry.size,
            last_modified_ms: entry.last_modified_ms,
        };
        let body = re_enqueue_body("bucket", &overwrite, 1);
        assert_eq!(keys(decode(&body, &scope(), packing())), [key]);
    }

    /// An EventBridge event of `detail_type` for `key` in `bucket`, shaped as
    /// AWS documents its `Object Created` example.
    fn event(detail_type: &str, bucket: &str, key: &str, size: Option<u64>) -> serde_json::Value {
        serde_json::json!({
            "version": "0",
            "id": "17793124-05d4-b198-2fde-7ededc63b103",
            "detail-type": detail_type,
            "source": "aws.s3",
            "account": "111122223333",
            "time": "2021-11-12T00:00:00Z",
            "region": "eu-west-2",
            "resources": [format!("arn:aws:s3:::{bucket}")],
            "detail": {
                "version": "0",
                "event-version": "1.2",
                "bucket": { "name": bucket },
                "object": {
                    "key": key,
                    "size": size,
                    "etag": "b1946ac92492d2347c6235b4d2611184",
                    "version-id": "IYV3p45BT0ac8hjHg1houSdS1a.Mro8e",
                    "sequencer": "617f08299329d189",
                },
                "request-id": "N4N7GDK58NMKJ12R",
                "requester": "123456789012",
                "source-ip-address": "192.0.2.1",
                "reason": "PutObject",
            },
        })
    }

    /// An `Object Created` event for `key` in the source's bucket.
    fn object_created(key: &str, size: u64) -> serde_json::Value {
        event("Object Created", "bucket", key, Some(size))
    }

    /// `event` decoded by a source over `scope`.
    fn decoded_event(event: &serde_json::Value, scope: &Scope) -> Decoded {
        decode(&serde_json::to_vec(event).unwrap(), scope, packing())
    }

    /// The reason `event` fails for a source over `s3://bucket/data/`, which
    /// must name the EventBridge event.
    fn event_failed(event: &serde_json::Value) -> String {
        let reason = failed(decoded_event(event, &scope()));
        assert!(reason.starts_with("EventBridge event: "), "{reason}");
        reason
    }

    /// One part as `(key, etag, version, re_enqueues, last_modified_ms)`.
    type EventPart = (String, Option<String>, Option<String>, u32, i64);

    /// Every part `decoded` reads, as [`EventPart`].
    fn announced(decoded: Decoded) -> Vec<EventPart> {
        match decoded {
            Decoded::Read(cursors) => cursors
                .into_iter()
                .flatten()
                .map(|part| {
                    let entry = &part.object[0];
                    let Origin::Notification {
                        version,
                        re_enqueues,
                    } = part.origin
                    else {
                        panic!("{:?} is not a notification part", part.origin);
                    };
                    (
                        entry.key.clone(),
                        entry.etag.clone(),
                        version,
                        re_enqueues,
                        entry.last_modified_ms,
                    )
                })
                .collect(),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    /// Pins that an `Object Created` event is read at its key as written,
    /// with the event's ETag quoted, its `version-id`, its `time` and a
    /// re-enqueue count of 0, even when the event carries one. The keys are
    /// those recorded from AWS S3 in eu-west-2, which EventBridge sent as
    /// written and the direct notification sent form-encoded.
    #[test]
    fn an_eventbridge_object_created_event_reads_its_key_as_written() {
        for key in [
            "plain.txt",
            "a b.txt",
            "a+b.txt",
            "100%.txt",
            "café.txt",
            "q?x=1&y=2#f.txt",
            "dir/sub dir/k~'(x).txt",
        ] {
            assert_eq!(
                announced(decoded_event(&object_created(key, 10), &whole_bucket())),
                [(
                    key.to_owned(),
                    Some("\"b1946ac92492d2347c6235b4d2611184\"".to_owned()),
                    Some("IYV3p45BT0ac8hjHg1houSdS1a.Mro8e".to_owned()),
                    0,
                    1_636_675_200_000,
                )],
                "{key}"
            );
        }
        let mut counted = object_created("data/a.ndjson", 10);
        counted["spateReEnqueues"] = 5.into();
        counted["detail"]["spateReEnqueues"] = 5.into();
        counted["detail"]["object"]["spateReEnqueues"] = 5.into();
        let parts = announced(decoded_event(&counted, &scope()));
        assert_eq!(parts[0].3, 0, "{parts:?}");
    }

    /// Pins that an event's object meets the checks a record's object does:
    /// bucket, prefix on the key as written, size, `object_store`'s path
    /// rules, the ETag's header rule and the version rules, each failing
    /// with a reason that names the event; and that an empty object is
    /// acknowledged.
    #[test]
    fn an_eventbridge_event_meets_the_notification_checks() {
        let reason = event_failed(&event(
            "Object Created",
            "elsewhere",
            "data/a.ndjson",
            Some(10),
        ));
        assert!(reason.contains("the bucket \"elsewhere\""), "{reason}");
        for key in ["data%2Fx.ndjson", "database/x", "other/a.ndjson"] {
            let reason = event_failed(&object_created(key, 10));
            assert!(
                reason.contains("outside the source's prefix \"data/\""),
                "{key}: {reason}"
            );
        }
        for key in ["data/../x", "data//a", "data/a/"] {
            let reason = event_failed(&object_created(key, 10));
            assert!(
                reason.contains("cannot be read under its own name"),
                "{key}: {reason}"
            );
        }
        let reason = event_failed(&event("Object Created", "bucket", "data/a", None));
        assert!(reason.contains("has no size"), "{reason}");
        let mut text_size = object_created("data/a.ndjson", 10);
        text_size["detail"]["object"]["size"] = "10".into();
        assert!(event_failed(&text_size).contains("the event is unreadable"));
        let mut etag = object_created("data/a.ndjson", 10);
        etag["detail"]["object"]["etag"] = "a\u{1}b".into();
        assert!(event_failed(&etag).contains("`If-Match`"));
        for refused in [
            serde_json::json!(7),
            "a b".into(),
            "v".repeat(MAX_VERSION_BYTES + 1).into(),
        ] {
            let mut version = object_created("data/a.ndjson", 10);
            version["detail"]["object"]["version-id"] = refused.clone();
            event_failed(&version);
        }
        for absent in ["", "null"] {
            let mut version = object_created("data/a.ndjson", 10);
            version["detail"]["object"]["version-id"] = absent.into();
            let parts = announced(decoded_event(&version, &scope()));
            assert_eq!(parts[0].2, None, "{absent:?}");
        }
        acknowledged(decoded_event(
            &object_created("data/empty.ndjson", 0),
            &scope(),
        ));
    }

    /// Pins that an event with no `time`, or one that does not parse as
    /// RFC 3339, gives its records event time 0.
    #[test]
    fn an_eventbridge_event_time_that_is_absent_or_unparsed_is_zero() {
        let mut absent = object_created("data/a.ndjson", 10);
        absent
            .as_object_mut()
            .expect("an event is an object")
            .remove("time");
        let mut unparsed = object_created("data/a.ndjson", 10);
        unparsed["time"] = "yesterday".into();
        for event in [absent, unparsed] {
            let parts = announced(decoded_event(&event, &scope()));
            assert_eq!(parts[0].4, 0, "{event}");
        }
    }

    /// Pins the event-version rule: `1.` followed by one or more digits is
    /// read, and anything else, absent or not a string included, fails.
    #[test]
    fn an_eventbridge_event_version_other_than_major_1_fails() {
        let with_version = |version: Option<serde_json::Value>| {
            let mut event = object_created("data/a.ndjson", 10);
            match version {
                Some(version) => event["detail"]["event-version"] = version,
                None => {
                    event["detail"]
                        .as_object_mut()
                        .expect("a detail is an object")
                        .remove("event-version");
                }
            }
            event
        };
        for read in ["1.0", "1.2", "1.10"] {
            let parts = announced(decoded_event(&with_version(Some(read.into())), &scope()));
            assert_eq!(parts.len(), 1, "{read}");
        }
        for refused in [
            Some("2.0".into()),
            Some("0.9".into()),
            Some("1".into()),
            Some("1.".into()),
            Some("1.x".into()),
            Some("1.2x".into()),
            Some("1.2.3".into()),
            Some("01.2".into()),
            Some("+1.2".into()),
            Some("".into()),
            Some(serde_json::json!(1.2)),
            None,
        ] {
            let reason = event_failed(&with_version(refused.clone()));
            assert!(reason.contains("event-version"), "{refused:?}: {reason}");
        }
    }

    /// Pins the `detail-type` rule: each listed type is acknowledged for any
    /// bucket and with no object, any other type fails naming it, a missing
    /// or numeric type fails, and a `source` other than exactly `aws.s3` is
    /// no notification.
    #[test]
    fn an_eventbridge_event_detail_type_is_acknowledged_only_from_the_list() {
        for detail_type in EVENTBRIDGE_ACKNOWLEDGED {
            let mut event = event(detail_type, "elsewhere", "other/a.ndjson", None);
            event["detail"]
                .as_object_mut()
                .expect("a detail is an object")
                .remove("object");
            acknowledged(decoded_event(&event, &scope()));
        }
        for detail_type in [
            "AWS API Call via CloudTrail",
            "Async Copy Completion",
            "object created",
            "Object Created ",
            "anything",
        ] {
            let reason = event_failed(&event(detail_type, "bucket", "data/a.ndjson", Some(10)));
            assert!(
                reason.contains(&format!("\"{detail_type}\"")),
                "{detail_type}: {reason}"
            );
        }
        let mut missing = object_created("data/a.ndjson", 10);
        missing
            .as_object_mut()
            .expect("an event is an object")
            .remove("detail-type");
        let mut numeric = object_created("data/a.ndjson", 10);
        numeric["detail-type"] = 1.into();
        for event in [missing, numeric] {
            assert!(event_failed(&event).contains("`detail-type`"));
        }
        for source in ["aws.S3", "aws.ec2", "aws.s3 "] {
            let mut event = object_created("data/a.ndjson", 10);
            event["source"] = source.into();
            let reason = failed(decoded_event(&event, &scope()));
            assert!(
                reason.contains("neither a descriptor nor a storage notification"),
                "{source}: {reason}"
            );
        }
    }

    /// Pins the order of the shapes: a body that carries an EventBridge
    /// event's fields beside a descriptor, the test event or a `Records`
    /// array is read as that shape.
    #[test]
    fn an_event_source_beside_another_shape_keeps_that_shape() {
        let beside = |body: serde_json::Value| {
            let mut body = body;
            let event = object_created("data/event.ndjson", 10);
            for field in ["source", "detail-type", "detail", "time"] {
                body[field] = event[field].clone();
            }
            serde_json::to_vec(&body).unwrap()
        };
        let records = beside(serde_json::json!({ "Records": [created("data/a+b.ndjson", 10)] }));
        assert_eq!(
            keys(decode(&records, &scope(), packing())),
            ["data/a b.ndjson"]
        );
        let object = DescriptorObject {
            key: "data/listed.ndjson".into(),
            size: 10,
            etag: None,
            last_modified_ms: 1,
        };
        let descriptor: serde_json::Value =
            serde_json::from_slice(&SplitDescriptor::new(vec![object]).encode().unwrap()).unwrap();
        assert_eq!(
            keys(decode(&beside(descriptor), &scope(), packing())),
            ["data/listed.ndjson"]
        );
        let test_event = beside(serde_json::json!({
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Bucket": "bucket",
        }));
        assert_eq!(
            acknowledged(decode(&test_event, &scope(), packing())),
            "a storage test event"
        );
    }

    /// Pins that a unit re-enqueued for an object an EventBridge event
    /// announced decodes to the key the event carried as written.
    #[test]
    fn a_re_enqueued_eventbridge_unit_reads_the_key_the_event_announced() {
        let key = "data/a+b%2Fc d%25.ndjson";
        let Decoded::Read(cursors) = decoded_event(&object_created(key, 10), &scope()) else {
            panic!("not read");
        };
        let parts: Vec<Part> = cursors.into_iter().flatten().collect();
        let entry = &parts[0].object[0];
        assert_eq!(entry.key, key);
        let overwrite = Overwrite {
            key: entry.key.clone(),
            etag: entry.etag.clone(),
            size: entry.size,
            last_modified_ms: entry.last_modified_ms,
        };
        let body = re_enqueue_body("bucket", &overwrite, 1);
        assert_eq!(keys(decode(&body, &scope(), packing())), [key]);
    }

    /// Pins that a created record is read whether or not its `eventName`
    /// carries the `s3:` prefix, and that the envelope RustFS sends is read
    /// as the bare `Records[]` message is.
    #[test]
    fn an_event_name_with_or_without_the_s3_prefix_is_accepted() {
        for event in [
            "s3:ObjectCreated:Put",
            "ObjectCreated:Copy",
            "ObjectCreated:CompleteMultipartUpload",
        ] {
            let message = [record(event, "bucket", "data/a.ndjson", Some(10))];
            assert_eq!(keys(decoded(&message)), ["data/a.ndjson"], "{event}");
        }
        let envelope = serde_json::json!({
            "EventName": "s3:ObjectCreated:Put",
            "Key": "bucket/data/a.ndjson",
            "Records": [record("s3:ObjectCreated:Put", "bucket", "data%2Fa.ndjson", Some(10))],
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins the scope check: a record for another bucket, or for a key
    /// outside `prefix/` (one that only shares the prefix's characters
    /// included), fails the whole message, as does any record when the
    /// source names no bucket; a whole-bucket source reads any key.
    #[test]
    fn a_record_for_another_bucket_or_prefix_is_dead_lettered() {
        let other = record("ObjectCreated:Put", "elsewhere", "data/a.ndjson", Some(10));
        let reason = failed(decoded(&[created("data/a.ndjson", 10), other]));
        assert!(reason.contains("notification record 1"), "{reason}");
        assert!(reason.contains("the bucket \"elsewhere\""), "{reason}");
        for key in ["other/a.ndjson", "database/a.ndjson", "data"] {
            let reason = failed(decoded(&[created(key, 10)]));
            assert!(
                reason.contains("outside the source's prefix \"data/\""),
                "{key}: {reason}"
            );
        }
        let nameless = Scope {
            bucket: None,
            prefix: Path::from("data"),
        };
        let reason = failed(decode(
            &message(&[created("data/a.ndjson", 10)]),
            &nameless,
            packing(),
        ));
        assert!(
            reason.contains("which this source does not read"),
            "{reason}"
        );
        let body = message(&[created("any/where.ndjson", 10)]);
        assert_eq!(
            keys(decode(&body, &whole_bucket(), packing())),
            ["any/where.ndjson"]
        );
    }

    /// Pins that a created object of size 0 is acknowledged with no part,
    /// even under a key `object_store` cannot address, but not under a key
    /// that does not decode, for another bucket or outside the prefix, and
    /// that an object of one byte is read.
    #[test]
    fn a_size_zero_record_is_acknowledged_without_a_unit() {
        for key in ["data/empty.ndjson", "data//empty.ndjson", "data/folder/"] {
            acknowledged(decoded(&[created(key, 0)]));
        }
        let reason = failed(decoded(&[created("data/a%zz", 0)]));
        assert!(reason.contains("malformed `%` escape"), "{reason}");
        let reason = failed(decoded(&[record(
            "ObjectCreated:Put",
            "elsewhere",
            "data/empty.ndjson",
            Some(0),
        )]));
        assert!(reason.contains("the bucket \"elsewhere\""), "{reason}");
        let reason = failed(decoded(&[created("other/empty.ndjson", 0)]));
        assert!(reason.contains("outside the source's prefix"), "{reason}");
        assert_eq!(
            keys(decoded(&[created("data/one.ndjson", 1)])),
            ["data/one.ndjson"]
        );
        let reason = failed(decoded(&[record(
            "ObjectCreated:Put",
            "bucket",
            "data/a",
            None,
        )]));
        assert!(reason.contains("has no size"), "{reason}");
    }

    /// Pins that the S3 test event and a record that creates nothing, for
    /// any bucket, are acknowledged with no part. A name creates only when
    /// `ObjectCreated:` opens it after at most one `s3:`, so one that only
    /// begins with `ObjectCreated`, or holds `ObjectCreated:` after anything
    /// other than one `s3:`, creates nothing.
    #[test]
    fn a_test_event_and_a_name_that_creates_nothing_are_acknowledged_without_a_unit() {
        let test_event = serde_json::json!({
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": "2026-10-19T12:00:00.000Z",
            "Bucket": "bucket",
            "RequestId": "5582815E1AEA5ADF",
            "HostId": "8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE",
        });
        acknowledged(decode(
            &serde_json::to_vec(&test_event).unwrap(),
            &scope(),
            packing(),
        ));
        for event in [
            "ObjectRemoved:Delete",
            "s3:ObjectRemoved:DeleteMarkerCreated",
            "ObjectCreatedButNot",
            "xObjectCreated:Put",
            "s3:s3:ObjectCreated:Put",
            "ObjectRemoved:ObjectCreated:Put",
        ] {
            acknowledged(decoded(&[record(
                event,
                "elsewhere",
                "other/a.ndjson",
                None,
            )]));
        }
        acknowledged(decoded(&[]));
    }

    /// Pins that a key `Path::parse` rejects or rewrites fails the message,
    /// including one whose encoded `/` decodes to an empty segment.
    #[test]
    fn a_key_path_parse_rejects_or_rewrites_is_dead_lettered() {
        for key in [
            "a//b.ndjson",
            "/a.ndjson",
            "a/",
            "a/%2Fb.ndjson",
            "a/./b.ndjson",
        ] {
            let body = message(&[created(key, 10)]);
            let reason = failed(decode(&body, &whole_bucket(), packing()));
            assert!(
                reason.contains("cannot be read under its own name"),
                "{key}: {reason}"
            );
        }
    }

    /// Pins that a body that is neither a descriptor nor a notification, a
    /// body with a descriptor's `v` that does not decode as one, or a created
    /// record the source cannot read, fails the message, and that a
    /// descriptor still reads.
    #[test]
    fn an_unknown_notification_shape_is_dead_lettered() {
        for body in [
            b"not json".to_vec(),
            b"{}".to_vec(),
            br#"{"Records":{}}"#.to_vec(),
            br#"["Records"]"#.to_vec(),
        ] {
            let reason = failed(decode(&body, &scope(), packing()));
            assert!(
                reason.contains("neither a descriptor nor a storage notification"),
                "{reason}"
            );
        }
        let no_s3 = serde_json::json!({ "eventName": "ObjectCreated:Put" });
        let no_name = serde_json::json!({ "s3": { "bucket": { "name": "bucket" } } });
        let mut negative = created("data/a.ndjson", 10);
        negative["s3"]["object"]["size"] = serde_json::json!(-1);
        let mut fractional = created("data/a.ndjson", 10);
        fractional["s3"]["object"]["size"] = serde_json::json!(1.5);
        for record in [no_s3, no_name, negative, fractional] {
            failed(decoded(&[record]));
        }
        for body in [br#"{"v":1,"objects":[]}"#.to_vec(), br#"{"v":2}"#.to_vec()] {
            let reason = failed(decode(&body, &scope(), packing()));
            assert!(reason.contains("not a descriptor"), "{reason}");
        }
        let object = DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 10,
            etag: None,
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins that a notification's ETag is sent quoted, as a listing reports
    /// it, that a quoted one is kept, the empty `""` included, that one with
    /// only a leading quote is wrapped, and that an absent one stays absent.
    #[test]
    fn an_unquoted_event_etag_is_sent_quoted() {
        let etag = |value: serde_json::Value| {
            let mut created = created("data/a.ndjson", 10);
            created["s3"]["object"]["eTag"] = value;
            read(decoded(&[created])).remove(0).1
        };
        assert_eq!(etag(serde_json::json!("abc")), Some("\"abc\"".to_owned()));
        assert_eq!(
            etag(serde_json::json!("\"abc\"")),
            Some("\"abc\"".to_owned())
        );
        assert_eq!(etag(serde_json::json!("\"")), Some("\"\"\"".to_owned()));
        assert_eq!(
            etag(serde_json::json!("\"abc")),
            Some("\"\"abc\"".to_owned())
        );
        assert_eq!(etag(serde_json::json!("\"\"")), Some("\"\"".to_owned()));
        assert_eq!(etag(serde_json::Value::Null), None);
    }

    /// Pins that an ETag `If-Match` cannot carry fails the message, from a
    /// notification or a descriptor, and that a tab, a space, `~` and a
    /// non-ASCII character are sent.
    #[test]
    fn an_etag_that_cannot_be_a_header_value_is_dead_lettered() {
        let with_etag = |etag: &str| {
            let mut created = created("data/a.ndjson", 10);
            created["s3"]["object"]["eTag"] = serde_json::json!(etag);
            decoded(&[created])
        };
        for etag in ["abc\u{1}", "abc\u{1f}", "abc\u{7f}", "a\nb"] {
            let reason = failed(with_etag(etag));
            assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        }
        for etag in ["a\tb", "a b", "a~b", "a\u{e9}b"] {
            assert_eq!(keys(with_etag(etag)), ["data/a.ndjson"], "{etag:?}");
        }
        let object = |etag: &str| DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 10,
            etag: Some(etag.to_owned()),
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object("\"abc\u{1}\"")])
            .encode()
            .unwrap();
        let reason = failed(decode(&body, &scope(), packing()));
        assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        let body = SplitDescriptor::new(vec![object("\"abc\"")])
            .encode()
            .unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    fn member(key: &str, size: u64, etag: &str) -> DescriptorObject {
        DescriptorObject {
            key: key.into(),
            size,
            etag: Some(etag.to_owned()),
            last_modified_ms: 1,
        }
    }

    /// Pins that a descriptor of two members is one whole part naming both
    /// in order, with no member cut, though the first qualifies for cutting.
    #[test]
    fn a_multi_member_descriptor_is_one_whole_part() {
        let body = SplitDescriptor::new(vec![
            member("data/big.ndjson", 3 * packing().target_bytes, "\"a\""),
            member("data/small.ndjson", 10, "\"b\""),
        ])
        .encode()
        .unwrap();
        let Decoded::Read(cursors) = decode(&body, &scope(), packing()) else {
            panic!("the unit is read");
        };
        let parts: Vec<Part> = cursors.into_iter().flatten().collect();
        assert_eq!(parts.len(), 1);
        assert!(parts[0].range.is_none());
        let keys: Vec<&str> = parts[0].object.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(keys, ["data/big.ndjson", "data/small.ndjson"]);
    }

    /// Pins that every member's ETag is checked for `If-Match`, and the
    /// refusal names the member that fails it.
    #[test]
    fn a_descriptor_with_an_unsendable_second_etag_fails() {
        let body = SplitDescriptor::new(vec![
            member("data/a.ndjson", 10, "\"abc\""),
            member("data/b.ndjson", 10, "\"abc\u{1}\""),
        ])
        .encode()
        .unwrap();
        let reason = failed(decode(&body, &scope(), packing()));
        assert!(
            reason.contains("\"data/b.ndjson\" cannot be sent in `If-Match`"),
            "{reason}"
        );
    }

    /// Pins that a descriptor naming no member, or more than
    /// `MAX_UNIT_MEMBERS`, fails, and that one at the bound is read.
    #[test]
    fn a_descriptor_of_no_members_or_past_the_bound_fails() {
        let body = |n: usize| {
            SplitDescriptor::new(
                (0..n)
                    .map(|i| member(&format!("data/{i}.ndjson"), 10, "\"e\""))
                    .collect(),
            )
            .encode()
            .unwrap()
        };
        for n in [0, MAX_UNIT_MEMBERS + 1] {
            let reason = failed(decode(&body(n), &scope(), packing()));
            assert!(
                reason.contains(&format!("names {n} objects")),
                "{n}: {reason}"
            );
        }
        assert_eq!(
            keys(decode(&body(MAX_UNIT_MEMBERS), &scope(), packing())).len(),
            1,
            "one whole part"
        );
    }

    /// Pins that a created object of size 0 is dropped before its ETag is
    /// checked: an ETag `If-Match` cannot carry on an empty object leaves the
    /// message's other records to be read.
    #[test]
    fn an_empty_object_with_an_unsendable_etag_is_dropped() {
        let mut empty = created("data/empty.ndjson", 0);
        empty["s3"]["object"]["eTag"] = serde_json::json!("abc\u{1}");
        assert_eq!(
            keys(decoded(&[empty, created("data/a.ndjson", 10)])),
            ["data/a.ndjson"]
        );
    }

    /// Pins that the ETag check covers a descriptor naming a byte range, as
    /// it does a whole-object descriptor.
    #[test]
    fn a_ranged_descriptor_with_an_unsendable_etag_fails() {
        let object = |etag: &str| DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 300,
            etag: Some(etag.to_owned()),
            last_modified_ms: 1,
        };
        let range = SplitRange::new(0, 100, b'\n');
        let body = SplitDescriptor::with_range(object("\"abc\u{1}\""), range)
            .encode()
            .unwrap();
        let reason = failed(decode(&body, &scope(), packing()));
        assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        let body = SplitDescriptor::with_range(object("\"abc\""), range)
            .encode()
            .unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins that a record with no `eventName`, or one that is not a string,
    /// fails its message even when its `s3` fields name an object to read.
    #[test]
    fn a_record_without_an_event_name_fails_its_message() {
        for name in [None, Some(serde_json::json!(1))] {
            let mut record = created("data/a.ndjson", 10);
            match name {
                None => {
                    record.as_object_mut().unwrap().remove("eventName");
                }
                Some(value) => record["eventName"] = value,
            }
            let reason = failed(decoded(&[created("data/b.ndjson", 10), record]));
            assert!(reason.contains("notification record 1"), "{reason}");
            assert!(
                reason.contains("`eventName` is missing or not a string"),
                "{reason}"
            );
        }
    }

    /// Pins that a dropped record leaves the records after it to be read: a
    /// removal, an event that creates nothing and an empty object ahead of a
    /// created object each give the message that object's part alone.
    #[test]
    fn a_dropped_record_leaves_the_records_after_it() {
        for dropped in [
            record("ObjectRemoved:Delete", "bucket", "data/gone.ndjson", None),
            record(
                "ObjectRestore:Completed",
                "elsewhere",
                "other/a.ndjson",
                Some(10),
            ),
            created("data/empty.ndjson", 0),
        ] {
            assert_eq!(
                keys(decoded(&[dropped, created("data/a.ndjson", 10)])),
                ["data/a.ndjson"]
            );
        }
    }

    /// Pins that a message's created records give one part each, in record
    /// order, that a qualifying object is cut into ranges, and that
    /// `eventTime` becomes the object's time, 0 when absent or unreadable.
    #[test]
    fn a_message_with_several_records_gives_one_part_each() {
        let parts = read(decoded(&[
            created("data/small.ndjson", 10),
            created("data/large.ndjson", 300),
        ]));
        assert_eq!(
            parts,
            [
                (
                    "data/small.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    None
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((0, 100))
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((100, 200))
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((200, 300))
                ),
            ]
        );
        let Decoded::Read(cursors) = decoded(&[created("data/a.ndjson", 10)]) else {
            panic!("not read");
        };
        let expected = chrono::DateTime::parse_from_rfc3339("2026-10-19T12:00:00.000Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(cursors[0].object[0].last_modified_ms, expected);
        for time in [serde_json::Value::Null, serde_json::json!("yesterday")] {
            let mut record = created("data/a.ndjson", 10);
            record["eventTime"] = time;
            let Decoded::Read(cursors) = decoded(&[record]) else {
                panic!("not read");
            };
            assert_eq!(cursors[0].object[0].last_modified_ms, 0);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Pins that every listed object yields one whole unit, or ranges
        /// that tile `[0, size)` with each between half the target and the
        /// target, and that each id is the one `split_id_for` or
        /// `split_id_for_range` mints, the same on every call and different
        /// under another ETag.
        #[test]
        fn prop_units_tile_the_listing_one_object_or_range_each(
            objects in proptest::collection::vec(
                (0u8..7, any::<u64>(), 0usize..6, 0u8..6, any::<bool>()),
                0..12,
            ),
            target in target(),
            codec in 0u8..4,
            delimiter in proptest::option::of(Just(b'\n')),
        ) {
            let compression = compression(codec);
            let packing = Packing { target_bytes: target, compression, delimiter };
            let listing: Vec<(ObjectEntry, bool)> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size_shape, random, ext, key_shape, etag))| {
                    let (key, unaddressable) = key(i, key_shape, EXTENSIONS[ext]);
                    let entry = ObjectEntry {
                        etag: etag.then(|| format!("\"etag-{i}\"")),
                        key,
                        size: size(target, size_shape, random),
                        last_modified_ms: 1,
                    };
                    (entry, unaddressable)
                })
                .collect();
            let entries: Vec<ObjectEntry> = listing.iter().map(|(e, _)| e.clone()).collect();
            let units_of = |entries: &[ObjectEntry]| {
                entries
                    .iter()
                    .cloned()
                    .flat_map(|e| units(e, packing))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            };
            let got = units_of(&entries);
            let again = units_of(&entries);
            prop_assert_eq!(&got, &again, "units differ between two calls");

            let mut seen = 0;
            for (entry, unaddressable) in &listing {
                let mine: Vec<(String, SplitDescriptor)> = got
                    .iter()
                    .map(|u| (u.id.clone(), SplitDescriptor::decode(&u.body).unwrap()))
                    .filter(|(_, d)| d.objects[0].key == entry.key)
                    .collect();
                seen += mine.len();
                for (_, d) in &mine {
                    prop_assert_eq!(d.objects.len(), 1);
                    prop_assert_eq!(&d.to_entries()[0], entry);
                }
                let cut = delimiter.is_some()
                    && entry.size > target
                    && entry.size <= MAX_SUBDIVIDABLE_BYTES
                    && entry.etag.is_some()
                    && plain(compression, &entry.key)
                    && !unaddressable;
                if cut {
                    let mut next = 0;
                    for (id, d) in &mine {
                        let range = d.range.expect("a cut object yields only ranges");
                        prop_assert_eq!(range.start, next);
                        let len = range.end - range.start;
                        prop_assert!(target / 2 <= len && len <= target, "{:?}", range);
                        prop_assert_eq!(Some(range.delimiter), delimiter);
                        let want = split_id_for_range(
                            &entry.key,
                            entry.etag.as_deref().unwrap(),
                            range,
                        )
                        .unwrap();
                        prop_assert_eq!(id, &want);
                        next = range.end;
                    }
                    prop_assert_eq!(next, entry.size, "the ranges do not tile the object");
                } else {
                    prop_assert_eq!(mine.len(), 1, "{:?} is not one whole unit", entry);
                    let (id, d) = &mine[0];
                    prop_assert_eq!(d.range, None);
                    let want =
                        split_id_for([(entry.key.as_str(), entry.etag.as_deref())]).unwrap();
                    prop_assert_eq!(id, &want);
                }
            }
            prop_assert_eq!(seen, got.len(), "a unit names no listed object");

            let retagged: Vec<ObjectEntry> = entries
                .iter()
                .map(|e| ObjectEntry {
                    etag: e.etag.as_ref().map(|t| format!("{t}-2")),
                    ..e.clone()
                })
                .collect();
            let other = units_of(&retagged);
            for (a, b) in got.iter().zip(&other) {
                let tagged = SplitDescriptor::decode(&a.body).unwrap().objects[0].etag.is_some();
                if tagged {
                    prop_assert_ne!(&a.id, &b.id, "the id ignores the ETag");
                }
            }
        }
    }

    /// Every unit `Packer` mints over `entries`, pushed in order and closed.
    fn pack(entries: &[ObjectEntry], packing: Packing) -> Vec<Minted> {
        let mut packer = Packer::new(packing);
        let mut out: Vec<Minted> = Vec::new();
        for entry in entries {
            for minted in packer.push(entry.clone()) {
                out.push(minted.unwrap());
            }
        }
        out.extend(packer.close().map(Result::unwrap));
        out
    }

    fn member_keys(minted: &[Minted]) -> Vec<Vec<String>> {
        minted
            .iter()
            .map(|m| {
                SplitDescriptor::decode(&m.unit.body)
                    .unwrap()
                    .objects
                    .into_iter()
                    .map(|o| o.key)
                    .collect()
            })
            .collect()
    }

    fn entry_tagged(key: &str, etag: Option<&str>) -> ObjectEntry {
        ObjectEntry {
            key: key.into(),
            size: 10,
            etag: etag.map(str::to_owned),
            last_modified_ms: 1,
        }
    }

    /// Pins that an object whose ETag `If-Match` cannot carry is a unit of
    /// its own between objects that would otherwise share one.
    #[test]
    fn an_object_whose_etag_cannot_be_sent_is_packed_alone() {
        let entries = [
            entry_tagged("a/x", Some("\"x\"")),
            entry_tagged("a/bad", Some("\"a\u{1}b\"")),
            entry_tagged("a/y", Some("\"y\"")),
        ];
        assert_eq!(
            member_keys(&pack(&entries, packing())),
            [["a/x"], ["a/bad"], ["a/y"]]
        );
    }

    /// Pins that a small object with no ETag shares a unit with the objects
    /// listed beside it.
    #[test]
    fn a_small_object_without_an_etag_shares_a_unit() {
        let entries = [
            entry_tagged("a/x", Some("\"x\"")),
            entry_tagged("a/none", None),
            entry_tagged("a/y", Some("\"y\"")),
        ];
        let minted = pack(&entries, packing());
        assert_eq!(member_keys(&minted), [["a/x", "a/none", "a/y"]]);
        assert_eq!(minted[0].first_key, "a/x");
        assert_eq!(minted[0].objects, 3);
    }

    /// Pins that an object whose listed size takes the open unit past the
    /// split target closes that unit, for any size the listing reports.
    #[test]
    fn a_listed_size_near_u64_max_closes_the_open_unit() {
        let mut big = entry_tagged("a/big", Some("\"b\""));
        big.size = u64::MAX - 5;
        let minted = pack(&[entry_tagged("a/x", Some("\"x\"")), big], packing());
        assert_eq!(member_keys(&minted), [["a/x"], ["a/big"]]);
    }

    /// Pins that sixteen members at the key and ETag caps, every byte one
    /// JSON escapes to two, encode under the packed body bound.
    #[test]
    fn a_unit_at_every_packing_cap_stays_under_the_body_bound() {
        let etag = format!("\"{}\"", "\"".repeat(crate::split::PACK_ETAG_MAX_BYTES - 2));
        let entries: Vec<ObjectEntry> = (0..PACK_MEMBERS)
            .map(|i| {
                let key = format!("a/{i:02}{}", "\"".repeat(PACK_KEY_MAX_BYTES - 4));
                entry_tagged(&key, Some(&etag))
            })
            .collect();
        let minted = pack(
            &entries,
            Packing {
                target_bytes: MIB,
                ..packing()
            },
        );
        assert_eq!(minted.len(), 1, "one unit of {PACK_MEMBERS}");
        let body = minted[0].unit.body.len() + minted[0].unit.id.len();
        assert!(
            body <= crate::split::PACKED_BODY_MAX_BYTES,
            "{body} bytes with the id"
        );
    }

    /// Pins that an object whose ETag is longer than the packing cap is a
    /// unit of its own, so a packed body stays under its bound whatever
    /// ETags the listing reports.
    #[test]
    fn a_packed_body_stays_under_its_bound_with_long_etags() {
        let long = format!("\"{}\"", "e".repeat(70_000));
        let entries: Vec<ObjectEntry> = (0..PACK_MEMBERS)
            .map(|i| entry_tagged(&format!("a/{i:02}"), Some(&long)))
            .chain([
                entry_tagged("a/x", Some("\"x\"")),
                entry_tagged("a/y", Some("\"y\"")),
            ])
            .collect();
        let minted = pack(&entries, packing());
        for m in &minted {
            if m.objects > 1 {
                assert!(m.unit.body.len() <= crate::split::PACKED_BODY_MAX_BYTES);
            }
        }
        let sizes: Vec<usize> = minted.iter().map(|m| m.objects).collect();
        let mut want = vec![1; PACK_MEMBERS];
        want.push(2);
        assert_eq!(sizes, want);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Pins the packing rule against a restatement of it: every listed
        /// object is in exactly one unit, whole or in ranges tiling it; a
        /// unit of several objects holds only objects read whole with an
        /// addressable key of at most 1,024 bytes and a sendable ETag,
        /// consecutive in listing order, at most sixteen and at most the
        /// target in bytes, under the id of its member set; and two runs
        /// mint the same units.
        #[test]
        fn prop_packed_units_tile_the_listing(
            objects in proptest::collection::vec(
                (0u8..7, any::<u64>(), 0usize..6, 0u8..6, 0u8..3, any::<bool>()),
                0..40,
            ),
            target in target(),
            codec in 0u8..4,
            delimiter in proptest::option::of(Just(b'\n')),
        ) {
            let compression = compression(codec);
            let packing = Packing { target_bytes: target, compression, delimiter };
            let listing: Vec<(ObjectEntry, bool)> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size_shape, random, ext, key_shape, etag, long))| {
                    let (mut key, unaddressable) = key(i, key_shape, EXTENSIONS[ext]);
                    // Only an addressable key, so the padding keeps its shape.
                    if long && key_shape == 0 {
                        key = format!("{}{key}", "x".repeat(1025 - key.len()));
                    }
                    let etag = match etag {
                        0 => None,
                        1 => Some(format!("\"etag-{i}\"")),
                        _ => Some(format!("\"bad\u{1}{i}\"")),
                    };
                    let entry = ObjectEntry {
                        key,
                        size: size(target, size_shape, random),
                        etag,
                        last_modified_ms: 1,
                    };
                    (entry, unaddressable)
                })
                .collect();
            let entries: Vec<ObjectEntry> = listing.iter().map(|(e, _)| e.clone()).collect();
            let got = pack(&entries, packing);
            let again = pack(&entries, packing);
            let units: Vec<&Unit> = got.iter().map(|m| &m.unit).collect();
            prop_assert_eq!(units, again.iter().map(|m| &m.unit).collect::<Vec<_>>());

            // The rule, restated: which objects are cut, and which share.
            let cut = |(e, unaddressable): &(ObjectEntry, bool)| {
                delimiter.is_some()
                    && e.size > target
                    && e.size <= MAX_SUBDIVIDABLE_BYTES
                    && e.etag.is_some()
                    && plain(compression, &e.key)
                    && !unaddressable
            };
            let shares = |item: &(ObjectEntry, bool)| {
                let (e, unaddressable) = item;
                !cut(item)
                    && !unaddressable
                    && e.key.len() <= 1024
                    && !e.etag.as_deref().is_some_and(|t| t.contains('\u{1}') || t.len() > 256)
            };
            // The expected groups: (keys, whether the group is one cut object).
            let mut want: Vec<(Vec<String>, bool)> = Vec::new();
            let mut open: Vec<String> = Vec::new();
            let mut open_bytes = 0u64;
            for item in &listing {
                let (e, _) = item;
                if shares(item) {
                    if !open.is_empty() && open_bytes + e.size > target {
                        want.push((std::mem::take(&mut open), false));
                        open_bytes = 0;
                    }
                    open.push(e.key.clone());
                    open_bytes += e.size;
                    if open.len() == 16 {
                        want.push((std::mem::take(&mut open), false));
                        open_bytes = 0;
                    }
                } else {
                    if !open.is_empty() {
                        want.push((std::mem::take(&mut open), false));
                        open_bytes = 0;
                    }
                    want.push((vec![e.key.clone()], cut(item)));
                }
            }
            if !open.is_empty() {
                want.push((open, false));
            }

            let mut at = 0;
            for (keys, is_cut) in &want {
                if *is_cut {
                    let entry = &entries.iter().find(|e| e.key == keys[0]).unwrap();
                    let mut next = 0;
                    while next < entry.size {
                        let d = SplitDescriptor::decode(&got[at].unit.body).unwrap();
                        let range = d.range.expect("a cut object yields ranges");
                        prop_assert_eq!(&d.objects[0].key, &keys[0]);
                        prop_assert_eq!(range.start, next);
                        next = range.end;
                        at += 1;
                    }
                    continue;
                }
                let minted = &got[at];
                let d = SplitDescriptor::decode(&minted.unit.body).unwrap();
                let got_keys: Vec<&String> = d.objects.iter().map(|o| &o.key).collect();
                prop_assert_eq!(got_keys, keys.iter().collect::<Vec<_>>());
                prop_assert_eq!(d.range, None);
                prop_assert!(keys.len() <= PACK_MEMBERS);
                if keys.len() > 1 {
                    let bytes: u64 = d.objects.iter().map(|o| o.size).sum();
                    prop_assert!(bytes <= target, "{} bytes over a {} target", bytes, target);
                }
                let id = split_id_for(
                    d.objects.iter().map(|o| (o.key.as_str(), o.etag.as_deref())),
                )
                .unwrap();
                prop_assert_eq!(&minted.unit.id, &id);
                prop_assert_eq!(&minted.first_key, &keys[0]);
                prop_assert_eq!(minted.objects, keys.len());
                at += 1;
            }
            prop_assert_eq!(at, got.len(), "a unit the rule does not account for");
        }
    }

    /// The origin of every part `decoded` reads.
    fn origins(decoded: Decoded) -> Vec<Origin> {
        match decoded {
            Decoded::Read(cursors) => cursors.into_iter().flatten().map(|p| p.origin).collect(),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    fn with_field(field: &str, value: serde_json::Value) -> serde_json::Value {
        let mut record = created("data/a.ndjson", 10);
        match field {
            "versionId" => record["s3"]["object"]["versionId"] = value,
            _ => record[field] = value,
        }
        record
    }

    /// Pins the `versionId` rule: a string other than `""` and `"null"`
    /// names the version read; absent, JSON null, `""` and `"null"` read the
    /// key; a number, a space, a control byte or a string over the bound
    /// fails the message.
    #[test]
    fn a_version_id_decodes_by_the_notification_rule() {
        let version = |origins: Vec<Origin>| match &origins[..] {
            [Origin::Notification { version, .. }] => version.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            version(origins(decoded(&[with_field(
                "versionId",
                "3HL4kqtJ".into()
            )]))),
            Some("3HL4kqtJ".to_owned())
        );
        assert_eq!(
            version(origins(decoded(&[created("data/a.ndjson", 10)]))),
            None
        );
        for absent in [serde_json::Value::Null, "".into(), "null".into()] {
            let record = with_field("versionId", absent.clone());
            assert_eq!(version(origins(decoded(&[record]))), None, "{absent}");
        }
        let longest = "v".repeat(MAX_VERSION_BYTES);
        assert_eq!(
            version(origins(decoded(&[with_field(
                "versionId",
                longest.clone().into()
            )]))),
            Some(longest)
        );
        for refused in [
            serde_json::json!(7),
            "a b".into(),
            "a\u{1}b".into(),
            "a\u{7f}b".into(),
            "v".repeat(MAX_VERSION_BYTES + 1).into(),
        ] {
            let reason = failed(decoded(&[with_field("versionId", refused.clone())]));
            assert!(
                reason.starts_with("notification record 0: "),
                "{refused}: {reason}"
            );
        }
    }

    /// Pins the re-enqueue count: absent and null are 0, a `u32` is read as
    /// given, and any other value fails the message.
    #[test]
    fn a_re_enqueue_count_decodes_by_the_notification_rule() {
        let count = |origins: Vec<Origin>| match &origins[..] {
            [Origin::Notification { re_enqueues, .. }] => *re_enqueues,
            other => panic!("{other:?}"),
        };
        assert_eq!(count(origins(decoded(&[created("data/a.ndjson", 10)]))), 0);
        assert_eq!(
            count(origins(decoded(&[with_field(
                RE_ENQUEUES,
                serde_json::Value::Null
            )]))),
            0
        );
        for n in [0, 7, u32::MAX] {
            assert_eq!(
                count(origins(decoded(&[with_field(RE_ENQUEUES, n.into())]))),
                n
            );
        }
        for refused in [
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("3"),
            serde_json::json!(4_294_967_296_u64),
        ] {
            let reason = failed(decoded(&[with_field(RE_ENQUEUES, refused.clone())]));
            assert!(
                reason.contains("the record is unreadable"),
                "{refused}: {reason}"
            );
        }
    }

    /// Pins that a re-enqueued unit's body decodes to a notification part
    /// for the key at the HEAD's ETag and size, with the count it was given
    /// and the HEAD's time, whatever bytes the key holds, and that the body
    /// is printable ASCII.
    #[test]
    fn a_re_enqueued_unit_decodes_to_the_key_and_the_heads_etag_and_size() {
        for key in [
            "data/a b.ndjson",
            "data/a+b.ndjson",
            "data/a%b.ndjson",
            "data/é.ndjson",
            "data/\u{FFFE}.ndjson",
            "data/a~b-c_d.e",
        ] {
            let overwrite = Overwrite {
                key: key.to_owned(),
                etag: Some("\"0123abcd\"".to_owned()),
                size: 1234,
                last_modified_ms: 1_760_000_000_123,
            };
            let body = re_enqueue_body("bucket", &overwrite, 4);
            assert!(
                body.iter().all(|&b| (0x20..0x7f).contains(&b)),
                "{key}: {}",
                String::from_utf8_lossy(&body)
            );
            let decoded = decode(&body, &scope(), packing());
            let Decoded::Read(cursors) = decoded else {
                panic!("{key}: not read");
            };
            let parts: Vec<Part> = cursors.into_iter().flatten().collect();
            let entry = &parts[0].object[0];
            assert_eq!(entry.key, key);
            assert_eq!(entry.etag.as_deref(), Some("\"0123abcd\""));
            assert_eq!(entry.size, 1234);
            assert_eq!(entry.last_modified_ms, 1_760_000_000_123);
            assert_eq!(
                parts[0].origin,
                Origin::Notification {
                    version: None,
                    re_enqueues: 4
                },
                "{key}"
            );
        }
    }

    /// Pins that a re-enqueued unit for a key the HEAD reported without an
    /// ETag decodes to a part that reads unpinned.
    #[test]
    fn a_re_enqueued_unit_without_an_etag_reads_unpinned() {
        let overwrite = Overwrite {
            key: "data/a.ndjson".to_owned(),
            etag: None,
            size: 10,
            last_modified_ms: 0,
        };
        let body = re_enqueue_body("bucket", &overwrite, 1);
        assert_eq!(
            read(decode(&body, &scope(), packing())),
            [("data/a.ndjson".to_owned(), None, None)]
        );
    }
}
