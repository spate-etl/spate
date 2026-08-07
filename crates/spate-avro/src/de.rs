//! The single-pass Avro datum decoder: a [`serde::Deserializer`] driven by
//! the *writer* [`Schema`] directly over the datum bytes.
//!
//! This is the decode backend behind [`crate::AvroDatumDeserializer`]. It
//! never materialises `apache_avro::types::Value` — the schema decides which
//! visitor method fires for each wire position, reproducing the visitor
//! calls `apache_avro::from_value` would make for the `Value` that
//! `from_avro_datum` would have built. String and bytes contents are handed
//! to the visitor **borrowed from the payload buffer** (`'de`), which is
//! what makes borrowed record families decode without copying.
//!
//! # Parity with the two-pass path
//!
//! The dispatch table mirrors `apache-avro` 0.21's `decode_internal` +
//! `from_value` pair, with these deliberate differences (each one is pinned
//! by a test in this module or in `tests/datum_parity.rs`):
//!
//! - **Strict truncation.** `decode_internal` maps EOF to `Value::Null` in
//!   three places (a truncated boolean, a truncated string body, a truncated
//!   union index), silently decoding a truncated trailing `Option` field as
//!   `None`. Here any truncation is an error.
//! - **Uniform supersets.** `from_value`'s per-method type acceptance is
//!   uneven (`deserialize_str` rejects an enum value that
//!   `deserialize_string` accepts; `deserialize_map` refuses a union that
//!   `deserialize_struct` unwraps). Here every method unwraps a union
//!   branch, accepts the enum-symbol string, and feeds a string position
//!   into a unit enum by variant name — a strict superset: anything the
//!   two-pass path decodes, this decodes identically, and some target
//!   shapes it rejects also work.
//! - **Enum symbols and record field names are transient** (`visit_str`),
//!   never `'de`-borrowed — they live in the schema, not the payload. A
//!   `&'de str` target works for string *contents* only.
//! - **A Rust enum target over a record schema** is an error; `from_value`
//!   has a legacy arm keying on a first field named `"type"`, which is not
//!   replicated.
//! - **Skipped fields are structurally validated only.** A field the target
//!   type ignores is skipped by wire structure (lengths, block counts,
//!   union and enum indexes are still bounds-checked) without validating
//!   string contents as UTF-8, so a payload whose *ignored* fields hold
//!   invalid UTF-8 decodes here and errors in the two-pass path. A skipped
//!   size-prefixed array/map block is trusted at its declared byte size,
//!   where the decode paths (both of them) walk the items and ignore the
//!   size — a corrupt payload whose size field lies about the block can
//!   therefore skip differently than it decodes.
//! - **Collections carry a per-datum item budget** of
//!   `max(payload length, 65 536)` claimed items, charged as each block
//!   opens. A legitimate item costs at least one wire byte except for
//!   zero-width shapes (an array of `null`s or of empty records), where a
//!   hostile block count would otherwise drive an unbounded walk;
//!   apache-avro instead caps each single block at 512 Mi items and walks
//!   it.
//! - **Two accepting-direction corners.** A zero-length decimal passes its
//!   (empty) wire bytes through where `Decimal::to_vec` errors, and a
//!   bytes-shaped target over a string position receives the raw bytes
//!   without the UTF-8 validation `decode_internal` applies to every
//!   string.
//! - `is_human_readable` is hardcoded `false` — apache-avro's matching
//!   global (`set_serde_human_readable`) cannot be read from outside the
//!   crate, and this workspace never sets it.
//!
//! `Duration` and `BigDecimal` schemas are rejected when the decode spec is
//! compiled (see [`compile_spec`]), so the dispatch here treats them as
//! unreachable-but-fallible.
//!
//! # Malformed-input safety
//!
//! Every length and block count read from the wire is checked against the
//! *remaining buffer* before any slice or allocation; collection
//! `size_hint`s are capped the same way, so a hostile count cannot drive a
//! large `Vec::with_capacity`, and the per-datum item budget above bounds
//! the collection walk itself. Varints are capped at 10 bytes. Recursion is
//! schema-and-data driven (nested records, arrays, unions), so a depth
//! guard of [`MAX_DEPTH`] bounds it; nothing here panics or loops
//! unboundedly on any input.

use apache_avro::Schema;
use apache_avro::schema::{DecimalSchema, RecordSchema, UnionSchema};
use serde::de::{self, DeserializeSeed, Visitor};
use std::collections::HashMap;
use std::fmt;

/// Named types (record/enum/fixed) keyed by namespace (`""` for none),
/// then by simple name, cloned out of the schema once at compile time so a
/// `Schema::Ref` resolves with two borrowed-key lookups — no per-datum
/// `ResolvedSchema` allocation, and no rendered-fullname `String` per
/// occurrence.
pub(crate) type Names = HashMap<String, HashMap<String, Schema>>;

/// Recursion bound for the schema/data walk. apache-avro 0.21 has no
/// decode-side depth guard at all; 128 comfortably covers real schemas
/// while keeping a recursive-schema depth bomb from overflowing the stack.
const MAX_DEPTH: u16 = 128;

/// Floor of the per-datum collection-item budget (see the module docs): a
/// datum's collections may claim at most `max(payload length, this)` items
/// in total, so zero-width items cannot drive an unbounded walk.
const MIN_ITEMS_BUDGET: u64 = 1 << 16;

/// The depth-limit check every walk entry shares.
fn check_depth(depth: u16) -> Result<(), DatumError> {
    if depth >= MAX_DEPTH {
        return Err(DatumError(format!(
            "datum nesting exceeds the depth limit of {MAX_DEPTH}"
        )));
    }
    Ok(())
}

/// Decode error: a pre-rendered message, converted to
/// `DeserError::Malformed` at the deserializer boundary.
#[derive(Debug)]
pub(crate) struct DatumError(pub(crate) String);

impl fmt::Display for DatumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DatumError {}

impl de::Error for DatumError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        DatumError(msg.to_string())
    }
}

/// Bounds-checked reader over the datum slice. All primitives error on
/// truncation — never a silent `Null` (see the module docs).
#[derive(Debug)]
pub(crate) struct Cursor<'de> {
    buf: &'de [u8],
    items_budget: u64,
}

impl<'de> Cursor<'de> {
    pub(crate) fn new(buf: &'de [u8]) -> Self {
        Cursor {
            items_budget: u64::try_from(buf.len())
                .unwrap_or(u64::MAX)
                .max(MIN_ITEMS_BUDGET),
            buf,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.buf.len()
    }

    /// Charge `count` claimed collection items against the per-datum
    /// budget (see the module docs), erroring before any walk happens.
    #[inline]
    fn charge_items(&mut self, count: u64) -> Result<(), DatumError> {
        match self.items_budget.checked_sub(count) {
            Some(rest) => {
                self.items_budget = rest;
                Ok(())
            }
            None => Err(DatumError(format!(
                "block claims {count} items, over the datum's remaining item budget of {}",
                self.items_budget
            ))),
        }
    }

    #[inline]
    fn take(&mut self, n: usize) -> Result<&'de [u8], DatumError> {
        if n > self.buf.len() {
            return Err(DatumError(format!(
                "datum truncated: needed {n} bytes, {} left",
                self.buf.len()
            )));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    #[inline]
    fn byte(&mut self) -> Result<u8, DatumError> {
        match self.buf.split_first() {
            Some((b, tail)) => {
                self.buf = tail;
                Ok(*b)
            }
            None => Err(DatumError("datum truncated: needed 1 byte, 0 left".into())),
        }
    }

    /// Base-128 little-endian varint, at most 10 bytes (the apache-avro
    /// bound: `j > 9` is an overflow).
    #[inline]
    fn var_u64(&mut self) -> Result<u64, DatumError> {
        let mut value = 0u64;
        for shift in 0..10u32 {
            let b = self.byte()?;
            value |= u64::from(b & 0x7F) << (shift * 7);
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DatumError("varint longer than 10 bytes".into()))
    }

    #[inline]
    fn zag_i64(&mut self) -> Result<i64, DatumError> {
        let z = self.var_u64()?;
        #[expect(
            clippy::cast_possible_wrap,
            reason = "zigzag decode: the wrap is the definition"
        )]
        Ok(if z & 0x1 == 0 {
            (z >> 1) as i64
        } else {
            !(z >> 1) as i64
        })
    }

    #[inline]
    fn zag_i32(&mut self) -> Result<i32, DatumError> {
        let v = self.zag_i64()?;
        i32::try_from(v).map_err(|_| DatumError(format!("int value {v} does not fit in an i32")))
    }

    /// A non-negative length prefix, bounds-checked against the remaining
    /// buffer before anything is sliced or allocated.
    #[inline]
    fn len_prefixed(&mut self) -> Result<&'de [u8], DatumError> {
        let len = self.zag_i64()?;
        let len = usize::try_from(len)
            .map_err(|_| DatumError(format!("negative length prefix: {len}")))?;
        self.take(len)
    }

    #[inline]
    fn f32(&mut self) -> Result<f32, DatumError> {
        let b = self.take(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    #[inline]
    fn f64(&mut self) -> Result<f64, DatumError> {
        let b = self.take(8)?;
        Ok(f64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    #[inline]
    fn bool(&mut self) -> Result<bool, DatumError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(DatumError(format!("invalid boolean byte {b:#04x}"))),
        }
    }

    #[inline]
    fn utf8(&mut self) -> Result<&'de str, DatumError> {
        let bytes = self.len_prefixed()?;
        std::str::from_utf8(bytes)
            .map_err(|e| DatumError(format!("string is not valid UTF-8: {e}")))
    }
}

/// Compile the datum-decode spec for a schema: the named-type index the
/// walk needs, or the pre-rendered reason this schema cannot datum-decode.
///
/// `Duration` and `BigDecimal` are rejected here — `from_value` cannot
/// deserialize either (no `deserialize_any` arm), so instead of a confusing
/// per-record type error the schema is refused up front: a build error for
/// fixed schemas, `SchemaUnavailable` per record for registry ids.
pub(crate) fn compile_spec(schema: &Schema) -> Result<Names, String> {
    let resolved = apache_avro::schema::ResolvedSchema::try_from(schema)
        .map_err(|e| format!("schema references cannot be resolved: {e}"))?;
    let mut names = Names::new();
    for (name, s) in resolved.get_names() {
        names
            .entry(name.namespace.clone().unwrap_or_default())
            .or_default()
            .insert(name.name.clone(), (*s).clone());
    }
    scan_supported(schema)?;
    for named in names.values().flat_map(HashMap::values) {
        scan_supported(named)?;
    }
    Ok(names)
}

/// Reject schema nodes the single-pass path does not decode. Named types
/// are scanned once via the names index (a `Ref` is a leaf here), so a
/// recursive schema terminates.
fn scan_supported(schema: &Schema) -> Result<(), String> {
    match schema {
        Schema::Duration | Schema::BigDecimal => Err(format!(
            "the datum deserializer does not support {schema:?} \
             (use build_value or build_serde for this schema)"
        )),
        Schema::Record(rec) => {
            for field in &rec.fields {
                scan_supported(&field.schema)?;
            }
            Ok(())
        }
        Schema::Array(a) => scan_supported(a.items.as_ref()),
        Schema::Map(m) => scan_supported(m.types.as_ref()),
        Schema::Union(u) => {
            for variant in u.variants() {
                scan_supported(variant)?;
            }
            Ok(())
        }
        Schema::Decimal(DecimalSchema { inner, .. }) => scan_supported(inner),
        _ => Ok(()),
    }
}

/// Decode one datum into `T`, driven by `schema`. The caller guarantees the
/// schema passed [`compile_spec`] (no `Duration`/`BigDecimal`).
/// Trailing bytes after the datum are ignored, as `from_avro_datum` does.
pub(crate) fn decode_datum<'de, T>(
    schema: &Schema,
    names: &Names,
    datum: &'de [u8],
) -> Result<T, DatumError>
where
    T: serde::Deserialize<'de>,
{
    let mut cur = Cursor::new(datum);
    T::deserialize(DatumDeserializer {
        cur: &mut cur,
        schema,
        names,
        enclosing: None,
        depth: 0,
    })
}

/// The schema-driven deserializer for one wire position.
///
/// `enclosing` is the namespace context threaded exactly as
/// `decode_internal` threads `enclosing_namespace`, but borrowed — apache's
/// `fully_qualified_name` clones two `String`s per record instance, which
/// on an array of records is a per-row cost.
pub(crate) struct DatumDeserializer<'a, 'de> {
    cur: &'a mut Cursor<'de>,
    schema: &'a Schema,
    names: &'a Names,
    enclosing: Option<&'a str>,
    depth: u16,
}

impl fmt::Debug for DatumDeserializer<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatumDeserializer")
            .field("depth", &self.depth)
            .finish_non_exhaustive()
    }
}

/// Resolve a possible `Schema::Ref` to its named target (one hop — a
/// named type is never itself a `Ref`), yielding the node and the
/// namespace context its body decodes under, as `decode_internal` does.
fn resolve_schema<'x>(
    schema: &'x Schema,
    names: &'x Names,
    enclosing: Option<&'x str>,
) -> Result<(&'x Schema, Option<&'x str>), DatumError> {
    match schema {
        Schema::Ref { name } => {
            let target =
                lookup_named(names, name, enclosing).ok_or_else(|| missing_ref(name, enclosing))?;
            Ok((target, name.namespace.as_deref().or(enclosing)))
        }
        other => Ok((other, enclosing)),
    }
}

/// Resolve a `Ref`'s target the way `Name::fully_qualified_name` does —
/// the ref's own namespace, else the enclosing one — with two borrowed-key
/// lookups: no per-occurrence allocation.
fn lookup_named<'x>(
    names: &'x Names,
    name: &apache_avro::schema::Name,
    enclosing: Option<&str>,
) -> Option<&'x Schema> {
    let ns = name.namespace.as_deref().or(enclosing).unwrap_or_default();
    names.get(ns)?.get(name.name.as_str())
}

/// The unresolved-`Ref` error, rendered on the failure path only.
fn missing_ref(name: &apache_avro::schema::Name, enclosing: Option<&str>) -> DatumError {
    DatumError(match name.namespace.as_deref().or(enclosing) {
        Some(ns) => format!("schema reference `{ns}.{}` has no definition", name.name),
        None => format!("schema reference `{}` has no definition", name.name),
    })
}

impl<'a, 'de> DatumDeserializer<'a, 'de> {
    /// A child deserializer one level deeper, at `schema`, with the record
    /// namespace context `enclosing`.
    fn child<'b>(
        &'b mut self,
        schema: &'b Schema,
        enclosing: Option<&'b str>,
    ) -> Result<DatumDeserializer<'b, 'de>, DatumError> {
        let depth = self.deeper()?;
        Ok(DatumDeserializer {
            cur: self.cur,
            schema,
            names: self.names,
            enclosing,
            depth,
        })
    }

    /// See [`resolve_schema`].
    fn resolved(&self) -> Result<(&'a Schema, Option<&'a str>), DatumError> {
        resolve_schema(self.schema, self.names, self.enclosing)
    }

    /// One nesting level down, or the depth-limit error.
    fn deeper(&self) -> Result<u16, DatumError> {
        check_depth(self.depth)?;
        Ok(self.depth + 1)
    }

    /// Read a union's branch index and return the selected branch schema.
    fn union_branch(
        cur: &mut Cursor<'de>,
        union: &'a UnionSchema,
    ) -> Result<(usize, &'a Schema), DatumError> {
        let index = cur.zag_i64()?;
        let idx = usize::try_from(index)
            .map_err(|_| DatumError(format!("negative union index {index}")))?;
        let variants = union.variants();
        variants.get(idx).map(|s| (idx, s)).ok_or_else(|| {
            DatumError(format!(
                "union index {idx} out of range for {} variants",
                variants.len()
            ))
        })
    }

    /// Read an enum's symbol index and return the symbol.
    fn enum_symbol(cur: &mut Cursor<'de>, symbols: &'a [String]) -> Result<&'a str, DatumError> {
        let raw = cur.zag_i32()?;
        let idx =
            usize::try_from(raw).map_err(|_| DatumError(format!("negative enum index {raw}")))?;
        symbols.get(idx).map(String::as_str).ok_or_else(|| {
            DatumError(format!(
                "enum index {idx} out of range for {} symbols",
                symbols.len()
            ))
        })
    }

    /// Decode a uuid position, mirroring apache-avro's dual
    /// representation: a 16-byte payload is raw uuid bytes, anything else
    /// is parsed as text. Callers render the canonical hyphenated form
    /// into a stack buffer — no heap `String` per field.
    fn uuid_value(cur: &mut Cursor<'de>) -> Result<apache_avro::Uuid, DatumError> {
        let bytes = cur.len_prefixed()?;
        if bytes.len() == 16 {
            apache_avro::Uuid::from_slice(bytes)
                .map_err(|e| DatumError(format!("invalid uuid bytes: {e}")))
        } else {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| DatumError(format!("uuid is not valid UTF-8: {e}")))?;
            apache_avro::Uuid::parse_str(s)
                .map_err(|e| DatumError(format!("invalid uuid string: {e}")))
        }
    }

    /// Decode a decimal position (bytes- or fixed-backed) to its wire
    /// bytes, unchanged — `from_value` emits `Decimal::to_vec()`, which
    /// sign-extends the value back to its original wire length, i.e. the
    /// bytes as written.
    fn decimal_bytes(cur: &mut Cursor<'de>, inner: &Schema) -> Result<&'de [u8], DatumError> {
        match inner {
            Schema::Bytes => cur.len_prefixed(),
            Schema::Fixed(f) => cur.take(f.size),
            other => Err(DatumError(format!(
                "decimal inner schema must be bytes or fixed, got {other:?}"
            ))),
        }
    }
}

/// Structurally skip one datum position the target type ignores. Lengths,
/// block counts, union and enum indexes stay bounds-checked; string
/// contents are not UTF-8-validated (see the module docs).
fn skip_datum(
    cur: &mut Cursor<'_>,
    schema: &Schema,
    names: &Names,
    enclosing: Option<&str>,
    depth: u16,
) -> Result<(), DatumError> {
    check_depth(depth)?;
    match schema {
        Schema::Null => Ok(()),
        Schema::Boolean => cur.bool().map(drop),
        Schema::Int | Schema::Date | Schema::TimeMillis => cur.zag_i32().map(drop),
        Schema::Enum(e) => DatumDeserializer::enum_symbol(cur, &e.symbols).map(drop),
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => cur.zag_i64().map(drop),
        Schema::Float => cur.take(4).map(drop),
        Schema::Double => cur.take(8).map(drop),
        Schema::Bytes | Schema::String | Schema::Uuid => cur.len_prefixed().map(drop),
        Schema::Fixed(f) => cur.take(f.size).map(drop),
        Schema::Decimal(DecimalSchema { inner, .. }) => {
            skip_datum(cur, inner, names, enclosing, depth + 1)
        }
        Schema::Record(rec) => {
            let ns = rec.name.namespace.as_deref().or(enclosing);
            for field in &rec.fields {
                skip_datum(cur, &field.schema, names, ns, depth + 1)?;
            }
            Ok(())
        }
        Schema::Array(inner) => loop {
            let count = cur.zag_i64()?;
            match count.cmp(&0) {
                std::cmp::Ordering::Equal => break Ok(()),
                std::cmp::Ordering::Less => {
                    // A negative count is followed by the block's byte
                    // size — the wire format's fast-skip path.
                    let size = cur.zag_i64()?;
                    let size = usize::try_from(size)
                        .map_err(|_| DatumError(format!("negative block size {size}")))?;
                    cur.take(size)?;
                }
                std::cmp::Ordering::Greater => {
                    #[expect(clippy::cast_sign_loss, reason = "the Greater arm: count is positive")]
                    {
                        cur.charge_items(count as u64)?;
                    }
                    for _ in 0..count {
                        skip_datum(cur, inner.items.as_ref(), names, enclosing, depth + 1)?;
                    }
                }
            }
        },
        Schema::Map(inner) => loop {
            let count = cur.zag_i64()?;
            match count.cmp(&0) {
                std::cmp::Ordering::Equal => break Ok(()),
                std::cmp::Ordering::Less => {
                    let size = cur.zag_i64()?;
                    let size = usize::try_from(size)
                        .map_err(|_| DatumError(format!("negative block size {size}")))?;
                    cur.take(size)?;
                }
                std::cmp::Ordering::Greater => {
                    #[expect(clippy::cast_sign_loss, reason = "the Greater arm: count is positive")]
                    {
                        cur.charge_items(count as u64)?;
                    }
                    for _ in 0..count {
                        cur.len_prefixed()?;
                        skip_datum(cur, inner.types.as_ref(), names, enclosing, depth + 1)?;
                    }
                }
            }
        },
        Schema::Union(u) => {
            let (_, branch) = DatumDeserializer::union_branch(cur, u)?;
            skip_datum(cur, branch, names, enclosing, depth + 1)
        }
        Schema::Ref { name } => {
            let target =
                lookup_named(names, name, enclosing).ok_or_else(|| missing_ref(name, enclosing))?;
            skip_datum(
                cur,
                target,
                names,
                name.namespace.as_deref().or(enclosing),
                depth + 1,
            )
        }
        Schema::Duration | Schema::BigDecimal => Err(DatumError(format!(
            "unsupported schema {schema:?} survived spec compilation"
        ))),
    }
}

/// Iterates a record's fields in schema order as a serde map.
struct RecordAccess<'a, 'de> {
    cur: &'a mut Cursor<'de>,
    fields: std::slice::Iter<'a, apache_avro::schema::RecordField>,
    pending: Option<&'a apache_avro::schema::RecordField>,
    names: &'a Names,
    enclosing: Option<&'a str>,
    depth: u16,
}

impl<'a, 'de> RecordAccess<'a, 'de> {
    fn new(
        cur: &'a mut Cursor<'de>,
        rec: &'a RecordSchema,
        names: &'a Names,
        enclosing: Option<&'a str>,
        depth: u16,
    ) -> Self {
        RecordAccess {
            cur,
            fields: rec.fields.iter(),
            pending: None,
            names,
            enclosing: rec.name.namespace.as_deref().or(enclosing),
            depth,
        }
    }

    /// An empty record view: the union-null-into-struct case.
    fn empty(cur: &'a mut Cursor<'de>, names: &'a Names, depth: u16) -> Self {
        RecordAccess {
            cur,
            fields: [].iter(),
            pending: None,
            names,
            enclosing: None,
            depth,
        }
    }
}

impl<'de> de::MapAccess<'de> for RecordAccess<'_, 'de> {
    type Error = DatumError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.fields.next() {
            Some(field) => {
                self.pending = Some(field);
                seed.deserialize(de::value::StrDeserializer::new(field.name.as_str()))
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let field = self
            .pending
            .take()
            .ok_or_else(|| DatumError("next_value_seed called before next_key_seed".into()))?;
        seed.deserialize(DatumDeserializer {
            cur: self.cur,
            schema: &field.schema,
            names: self.names,
            enclosing: self.enclosing,
            depth: self.depth,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.fields.len())
    }
}

/// Iterates a block-encoded array as a serde sequence.
struct BlockSeqAccess<'a, 'de> {
    cur: &'a mut Cursor<'de>,
    item: &'a Schema,
    names: &'a Names,
    enclosing: Option<&'a str>,
    depth: u16,
    remaining: u64,
    done: bool,
}

impl<'a, 'de> BlockSeqAccess<'a, 'de> {
    fn new(
        cur: &'a mut Cursor<'de>,
        item: &'a Schema,
        names: &'a Names,
        enclosing: Option<&'a str>,
        depth: u16,
    ) -> Self {
        BlockSeqAccess {
            cur,
            item,
            names,
            enclosing,
            depth,
            remaining: 0,
            done: false,
        }
    }

    /// An exhausted sequence: the union-null-into-seq case.
    fn empty(cur: &'a mut Cursor<'de>, item: &'a Schema, names: &'a Names, depth: u16) -> Self {
        BlockSeqAccess {
            cur,
            item,
            names,
            enclosing: None,
            depth,
            remaining: 0,
            done: true,
        }
    }

    /// Advance to the next block header when the current block is drained:
    /// `count > 0` opens a block, `count < 0` opens a block of `-count`
    /// items prefixed with its byte size (read and discarded, as
    /// `decode_seq_len` does), `0` terminates the collection.
    fn refill(
        cur: &mut Cursor<'de>,
        done: &mut bool,
        remaining: &mut u64,
    ) -> Result<(), DatumError> {
        let count = cur.zag_i64()?;
        match count.cmp(&0) {
            std::cmp::Ordering::Equal => *done = true,
            std::cmp::Ordering::Less => {
                let _size = cur.zag_i64()?;
                let count = count
                    .checked_neg()
                    .ok_or_else(|| DatumError("block count overflow".into()))?;
                #[expect(
                    clippy::cast_sign_loss,
                    reason = "checked_neg of a negative is positive"
                )]
                {
                    *remaining = count as u64;
                }
            }
            std::cmp::Ordering::Greater => {
                #[expect(clippy::cast_sign_loss, reason = "the Greater arm: count is positive")]
                {
                    *remaining = count as u64;
                }
            }
        }
        cur.charge_items(*remaining)
    }
}

impl<'de> de::SeqAccess<'de> for BlockSeqAccess<'_, 'de> {
    type Error = DatumError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        while self.remaining == 0 {
            if self.done {
                return Ok(None);
            }
            Self::refill(self.cur, &mut self.done, &mut self.remaining)?;
        }
        self.remaining -= 1;
        seed.deserialize(DatumDeserializer {
            cur: self.cur,
            schema: self.item,
            names: self.names,
            enclosing: self.enclosing,
            depth: self.depth,
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        // Capped by the remaining buffer so a hostile block count cannot
        // drive a large reservation: every claimed item costs at least the
        // bytes still present (zero-width items are capped too).
        usize::try_from(self.remaining)
            .ok()
            .map(|r| r.min(self.cur.remaining()))
    }
}

/// Iterates a block-encoded map as a serde map; keys are wire strings and
/// borrow the payload.
struct BlockMapAccess<'a, 'de> {
    cur: &'a mut Cursor<'de>,
    value: &'a Schema,
    names: &'a Names,
    enclosing: Option<&'a str>,
    depth: u16,
    remaining: u64,
    done: bool,
}

impl<'de> de::MapAccess<'de> for BlockMapAccess<'_, 'de> {
    type Error = DatumError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        while self.remaining == 0 {
            if self.done {
                return Ok(None);
            }
            BlockSeqAccess::refill(self.cur, &mut self.done, &mut self.remaining)?;
        }
        self.remaining -= 1;
        let key = self.cur.utf8()?;
        seed.deserialize(de::value::BorrowedStrDeserializer::new(key))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(DatumDeserializer {
            cur: self.cur,
            schema: self.value,
            names: self.names,
            enclosing: self.enclosing,
            depth: self.depth,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        usize::try_from(self.remaining)
            .ok()
            .map(|r| r.min(self.cur.remaining()))
    }
}

/// Enum access for a union position: the Rust variant is selected
/// **positionally** by the wire branch index, as `from_value` does.
struct UnionAccess<'a, 'de> {
    de: DatumDeserializer<'a, 'de>,
    variant: &'static str,
}

impl<'de> de::EnumAccess<'de> for UnionAccess<'_, 'de> {
    type Error = DatumError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let name = seed.deserialize(de::value::StrDeserializer::new(self.variant))?;
        Ok((name, self))
    }
}

impl<'de> de::VariantAccess<'de> for UnionAccess<'_, 'de> {
    type Error = DatumError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.de.resolved()?.0 {
            Schema::Null => Ok(()),
            other => Err(DatumError(format!(
                "expected a null union branch for a unit variant, got {other:?}"
            ))),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_struct(self.de, "", fields, visitor)
    }
}

/// Enum access for an Avro `enum` position: the Rust variant is matched
/// **by symbol name**, and only unit variants are legal.
struct SymbolAccess<'a> {
    symbol: &'a str,
}

impl<'de> de::EnumAccess<'de> for SymbolAccess<'_> {
    type Error = DatumError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let name = seed.deserialize(de::value::StrDeserializer::new(self.symbol))?;
        Ok((name, self))
    }
}

impl<'de> de::VariantAccess<'de> for SymbolAccess<'_> {
    type Error = DatumError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        Err(DatumError("an avro enum only maps to unit variants".into()))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DatumError("an avro enum only maps to unit variants".into()))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DatumError("an avro enum only maps to unit variants".into()))
    }
}

impl<'de> de::Deserializer<'de> for DatumDeserializer<'_, 'de> {
    type Error = DatumError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Null => visitor.visit_unit(),
            Schema::Boolean => visitor.visit_bool(self.cur.bool()?),
            Schema::Int | Schema::Date | Schema::TimeMillis => {
                visitor.visit_i32(self.cur.zag_i32()?)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => visitor.visit_i64(self.cur.zag_i64()?),
            Schema::Float => visitor.visit_f32(self.cur.f32()?),
            Schema::Double => visitor.visit_f64(self.cur.f64()?),
            Schema::String => visitor.visit_borrowed_str(self.cur.utf8()?),
            Schema::Bytes => visitor.visit_borrowed_bytes(self.cur.len_prefixed()?),
            Schema::Fixed(f) => visitor.visit_borrowed_bytes(self.cur.take(f.size)?),
            Schema::Enum(e) => visitor.visit_str(Self::enum_symbol(self.cur, &e.symbols)?),
            Schema::Uuid => {
                let uuid = Self::uuid_value(self.cur)?;
                let mut buf = apache_avro::Uuid::encode_buffer();
                visitor.visit_str(uuid.hyphenated().encode_lower(&mut buf))
            }
            Schema::Decimal(DecimalSchema { inner, .. }) => {
                visitor.visit_borrowed_bytes(Self::decimal_bytes(self.cur, inner)?)
            }
            Schema::Record(rec) => {
                let depth = self.deeper()?;
                visitor.visit_map(RecordAccess::new(
                    self.cur, rec, self.names, enclosing, depth,
                ))
            }
            Schema::Array(a) => {
                let depth = self.deeper()?;
                visitor.visit_seq(BlockSeqAccess::new(
                    self.cur,
                    a.items.as_ref(),
                    self.names,
                    enclosing,
                    depth,
                ))
            }
            Schema::Map(m) => {
                let depth = self.deeper()?;
                visitor.visit_map(BlockMapAccess {
                    cur: self.cur,
                    value: m.types.as_ref(),
                    names: self.names,
                    enclosing,
                    depth,
                    remaining: 0,
                    done: false,
                })
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                self.child(branch, enclosing)?.deserialize_any(visitor)
            }
            Schema::Duration | Schema::BigDecimal => Err(DatumError(format!(
                "unsupported schema {schema:?} survived spec compilation"
            ))),
            Schema::Ref { .. } => unreachable!("resolved() strips Ref"),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DatumError("avro does not support char".into()))
    }

    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::String => visitor.visit_borrowed_str(self.cur.utf8()?),
            Schema::Bytes => {
                let bytes = self.cur.len_prefixed()?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| DatumError(format!("bytes are not valid UTF-8: {e}")))?;
                visitor.visit_borrowed_str(s)
            }
            Schema::Fixed(f) => {
                let bytes = self.cur.take(f.size)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| DatumError(format!("fixed bytes are not valid UTF-8: {e}")))?;
                visitor.visit_borrowed_str(s)
            }
            Schema::Enum(e) => visitor.visit_str(Self::enum_symbol(self.cur, &e.symbols)?),
            Schema::Uuid => {
                let uuid = Self::uuid_value(self.cur)?;
                let mut buf = apache_avro::Uuid::encode_buffer();
                visitor.visit_str(uuid.hyphenated().encode_lower(&mut buf))
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                self.child(branch, enclosing)?.deserialize_str(visitor)
            }
            other => Err(DatumError(format!(
                "expected a string-shaped schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::String | Schema::Bytes => {
                visitor.visit_borrowed_bytes(self.cur.len_prefixed()?)
            }
            Schema::Fixed(f) => visitor.visit_borrowed_bytes(self.cur.take(f.size)?),
            Schema::Uuid => visitor.visit_bytes(Self::uuid_value(self.cur)?.as_bytes()),
            Schema::Decimal(DecimalSchema { inner, .. }) => {
                visitor.visit_borrowed_bytes(Self::decimal_bytes(self.cur, inner)?)
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                self.child(branch, enclosing)?.deserialize_bytes(visitor)
            }
            other => Err(DatumError(format!(
                "expected a bytes-shaped schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                if matches!(branch, Schema::Null) {
                    visitor.visit_none()
                } else {
                    visitor.visit_some(self.child(branch, enclosing)?)
                }
            }
            other => Err(DatumError(format!(
                "expected a union schema for an Option, got {other:?}"
            ))),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Null => visitor.visit_unit(),
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                match resolve_schema(branch, self.names, enclosing)?.0 {
                    Schema::Null => visitor.visit_unit(),
                    other => Err(DatumError(format!(
                        "expected a null union branch, got {other:?}"
                    ))),
                }
            }
            other => Err(DatumError(format!(
                "expected a null-shaped schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Array(a) => {
                let depth = self.deeper()?;
                visitor.visit_seq(BlockSeqAccess::new(
                    self.cur,
                    a.items.as_ref(),
                    self.names,
                    enclosing,
                    depth,
                ))
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                match branch {
                    // A null branch reads as an empty sequence, as
                    // `from_value` maps `Union(null)` for a seq target.
                    Schema::Null => visitor.visit_seq(BlockSeqAccess::empty(
                        self.cur,
                        &Schema::Null,
                        self.names,
                        self.depth,
                    )),
                    _ => self.child(branch, enclosing)?.deserialize_seq(visitor),
                }
            }
            other => Err(DatumError(format!(
                "expected an array schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Map(m) => {
                let depth = self.deeper()?;
                visitor.visit_map(BlockMapAccess {
                    cur: self.cur,
                    value: m.types.as_ref(),
                    names: self.names,
                    enclosing,
                    depth,
                    remaining: 0,
                    done: false,
                })
            }
            Schema::Record(rec) => {
                let depth = self.deeper()?;
                visitor.visit_map(RecordAccess::new(
                    self.cur, rec, self.names, enclosing, depth,
                ))
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                self.child(branch, enclosing)?.deserialize_map(visitor)
            }
            other => Err(DatumError(format!(
                "expected a map or record schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Record(rec) => {
                let depth = self.deeper()?;
                visitor.visit_map(RecordAccess::new(
                    self.cur, rec, self.names, enclosing, depth,
                ))
            }
            Schema::Union(u) => {
                let (_, branch) = Self::union_branch(self.cur, u)?;
                let depth = self.deeper()?;
                match resolve_schema(branch, self.names, enclosing)? {
                    // A null branch reads as a record with no fields, as
                    // `from_value` maps `Union(null)` for a struct target.
                    (Schema::Null, _) => {
                        visitor.visit_map(RecordAccess::empty(self.cur, self.names, depth))
                    }
                    (Schema::Record(rec), ns) => {
                        visitor.visit_map(RecordAccess::new(self.cur, rec, self.names, ns, depth))
                    }
                    (other, _) => Err(DatumError(format!(
                        "expected a record or null union branch, got {other:?}"
                    ))),
                }
            }
            other => Err(DatumError(format!(
                "expected a record schema, got {other:?}"
            ))),
        }
    }

    fn deserialize_enum<V>(
        mut self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (schema, enclosing) = self.resolved()?;
        match schema {
            Schema::Union(u) => {
                let (idx, branch) = Self::union_branch(self.cur, u)?;
                let variant = variants.get(idx).ok_or_else(|| {
                    DatumError(format!(
                        "union branch {idx} out of range for an enum with {} variants",
                        variants.len()
                    ))
                })?;
                let de = self.child(branch, enclosing)?;
                visitor.visit_enum(UnionAccess { de, variant })
            }
            Schema::Enum(e) => {
                let symbol = Self::enum_symbol(self.cur, &e.symbols)?;
                visitor.visit_enum(SymbolAccess { symbol })
            }
            // `from_value` also feeds a plain string value into a unit
            // enum (`EnumUnitDeserializer`, matched by variant name);
            // mirrored so the acceptance table stays a superset.
            Schema::String => {
                let symbol = self.cur.utf8()?;
                visitor.visit_enum(SymbolAccess { symbol })
            }
            other => Err(DatumError(format!(
                "expected a union, enum, or string schema for a Rust enum, got {other:?}"
            ))),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        skip_datum(
            self.cur,
            self.schema,
            self.names,
            self.enclosing,
            self.depth,
        )?;
        visitor.visit_unit()
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    /// Zigzag-varint encode, the test-side twin of `Cursor::zag_i64`.
    fn zig(n: i64) -> Vec<u8> {
        #[expect(
            clippy::cast_sign_loss,
            reason = "zigzag encode: the wrap is the definition"
        )]
        let mut z = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            if z <= 0x7F {
                out.push((z & 0x7F) as u8);
                break;
            }
            out.push(0x80 | (z & 0x7F) as u8);
            z >>= 7;
        }
        out
    }

    fn avro_str(s: &str) -> Vec<u8> {
        let mut out = zig(s.len() as i64);
        out.extend_from_slice(s.as_bytes());
        out
    }

    fn schema(json: &str) -> Schema {
        Schema::parse_str(json).unwrap()
    }

    fn decode<'de, T: Deserialize<'de>>(sch: &Schema, datum: &'de [u8]) -> Result<T, DatumError> {
        let names = compile_spec(sch).map_err(DatumError)?;
        // The borrow of `names` must outlive the call, so leak per test —
        // tests only.
        let names: &'static Names = Box::leak(Box::new(names));
        decode_datum(sch, names, datum)
    }

    const LONG_ARRAY: &str = r#"{"type":"array","items":"long"}"#;

    #[test]
    fn multi_block_array_concatenates() {
        // [2 items: 1, 2][2 items: 3, 4][terminator]
        let mut datum = zig(2);
        datum.extend(zig(1));
        datum.extend(zig(2));
        datum.extend(zig(2));
        datum.extend(zig(3));
        datum.extend(zig(4));
        datum.extend(zig(0));
        let got: Vec<i64> = decode(&schema(LONG_ARRAY), &datum).unwrap();
        assert_eq!(got, vec![1, 2, 3, 4]);
    }

    #[test]
    fn negative_block_count_carries_byte_size() {
        // [-2 items, size 2 bytes: 5, 6][terminator]
        let mut datum = zig(-2);
        datum.extend(zig(2)); // byte size of the block
        datum.extend(zig(5));
        datum.extend(zig(6));
        datum.extend(zig(0));
        let got: Vec<i64> = decode(&schema(LONG_ARRAY), &datum).unwrap();
        assert_eq!(got, vec![5, 6]);
    }

    #[test]
    fn empty_array_is_just_a_terminator() {
        let got: Vec<i64> = decode(&schema(LONG_ARRAY), &zig(0)).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn missing_block_terminator_is_an_error() {
        let mut datum = zig(1);
        datum.extend(zig(7));
        // no terminator
        let err = decode::<Vec<i64>>(&schema(LONG_ARRAY), &datum).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    const NULLABLE_LONG: &str = r#"["null","long"]"#;

    #[test]
    fn union_index_out_of_range_is_an_error() {
        let err = decode::<Option<i64>>(&schema(NULLABLE_LONG), &zig(2)).unwrap_err();
        assert!(err.0.contains("union index 2 out of range"), "{err}");
        let err = decode::<Option<i64>>(&schema(NULLABLE_LONG), &zig(-1)).unwrap_err();
        assert!(err.0.contains("negative union index"), "{err}");
    }

    #[test]
    fn truncated_union_index_is_an_error_not_none() {
        // apache-avro's decode_internal maps EOF at a union index to
        // Union(0, Null); the single-pass path is deliberately strict.
        let err = decode::<Option<i64>>(&schema(NULLABLE_LONG), &[]).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn truncated_string_body_is_an_error_not_null() {
        let datum = zig(5); // claims 5 bytes, provides none
        let err = decode::<String>(&schema(r#""string""#), &datum).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn truncated_varint_is_an_error() {
        let err = decode::<i64>(&schema(r#""long""#), &[0x80]).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn truncated_double_is_an_error() {
        let err = decode::<f64>(&schema(r#""double""#), &[0, 0, 0]).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn absurd_length_prefix_fails_without_allocating() {
        let datum = zig(1 << 60);
        let err = decode::<String>(&schema(r#""string""#), &datum).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn negative_length_prefix_is_an_error() {
        let err = decode::<String>(&schema(r#""string""#), &zig(-3)).unwrap_err();
        assert!(err.0.contains("negative length prefix"), "{err}");
    }

    #[test]
    fn invalid_utf8_in_string_is_an_error() {
        let mut datum = zig(2);
        datum.extend_from_slice(&[0xFF, 0xFE]);
        let err = decode::<String>(&schema(r#""string""#), &datum).unwrap_err();
        assert!(err.0.contains("UTF-8"), "{err}");
    }

    #[test]
    fn invalid_utf8_in_map_key_is_an_error() {
        let mut datum = zig(1);
        datum.extend(zig(2));
        datum.extend_from_slice(&[0xFF, 0xFE]);
        datum.extend(zig(9));
        datum.extend(zig(0));
        let err = decode::<std::collections::HashMap<String, i64>>(
            &schema(r#"{"type":"map","values":"long"}"#),
            &datum,
        )
        .unwrap_err();
        assert!(err.0.contains("UTF-8"), "{err}");
    }

    #[test]
    fn multi_block_map_decodes() {
        let mut datum = zig(1);
        datum.extend(avro_str("a"));
        datum.extend(zig(1));
        datum.extend(zig(1));
        datum.extend(avro_str("b"));
        datum.extend(zig(2));
        datum.extend(zig(0));
        let got: std::collections::HashMap<String, i64> =
            decode(&schema(r#"{"type":"map","values":"long"}"#), &datum).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got["a"], 1);
        assert_eq!(got["b"], 2);
    }

    #[test]
    fn bool_byte_out_of_range_is_an_error() {
        let err = decode::<bool>(&schema(r#""boolean""#), &[2]).unwrap_err();
        assert!(err.0.contains("invalid boolean byte"), "{err}");
    }

    #[test]
    fn enum_index_out_of_range_is_an_error() {
        let sch = schema(r#"{"type":"enum","name":"E","symbols":["A","B"]}"#);
        let err = decode::<String>(&sch, &zig(5)).unwrap_err();
        assert!(err.0.contains("enum index 5 out of range"), "{err}");
    }

    #[test]
    fn enum_decodes_by_symbol_name() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum E {
            A,
            B,
        }
        let sch = schema(r#"{"type":"enum","name":"E","symbols":["A","B"]}"#);
        assert_eq!(decode::<E>(&sch, &zig(1)).unwrap(), E::B);
        // And as a plain string target.
        assert_eq!(decode::<String>(&sch, &zig(0)).unwrap(), "A");
    }

    #[test]
    fn int_varint_overflowing_i32_is_an_error() {
        let err = decode::<i32>(&schema(r#""int""#), &zig(i64::from(i32::MAX) + 1)).unwrap_err();
        assert!(err.0.contains("does not fit in an i32"), "{err}");
    }

    #[test]
    fn trailing_bytes_are_ignored() {
        let mut datum = zig(42);
        datum.extend_from_slice(&[1, 2, 3]);
        let got: i64 = decode(&schema(r#""long""#), &datum).unwrap();
        assert_eq!(got, 42);
    }

    #[test]
    fn recursive_depth_bomb_errors_instead_of_overflowing() {
        // A LongList cons chain deeper than MAX_DEPTH: each level is
        // union-index 1 (the record branch), a value, and a nested tail.
        const LIST: &str = r#"{"type":"record","name":"LongList","fields":[
            {"name":"value","type":"long"},
            {"name":"next","type":["null","LongList"]}]}"#;
        #[derive(Debug, Deserialize)]
        #[expect(dead_code, reason = "shape only")]
        struct LongList {
            value: i64,
            next: Option<Box<LongList>>,
        }
        let mut datum = Vec::new();
        for _ in 0..300 {
            datum.extend(zig(7)); // value
            datum.extend(zig(1)); // next: the LongList branch
        }
        datum.extend(zig(7));
        datum.extend(zig(0)); // final next: null
        let err = decode::<LongList>(&schema(LIST), &datum).unwrap_err();
        assert!(err.0.contains("depth limit"), "{err}");
    }

    #[test]
    fn recursive_schema_within_the_limit_decodes() {
        const LIST: &str = r#"{"type":"record","name":"LongList","fields":[
            {"name":"value","type":"long"},
            {"name":"next","type":["null","LongList"]}]}"#;
        #[derive(Debug, Deserialize)]
        struct LongList {
            value: i64,
            next: Option<Box<LongList>>,
        }
        let mut datum = Vec::new();
        datum.extend(zig(1));
        datum.extend(zig(1));
        datum.extend(zig(2));
        datum.extend(zig(0));
        let got: LongList = decode(&schema(LIST), &datum).unwrap();
        assert_eq!(got.value, 1);
        let next = got.next.unwrap();
        assert_eq!(next.value, 2);
        assert!(next.next.is_none());
    }

    #[test]
    fn ignored_fields_are_skipped_structurally() {
        // The struct omits leading, middle, and trailing fields, including
        // an array with a negative-count block and a nested record.
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"skip_head","type":{"type":"array","items":"string"}},
            {"name":"keep_a","type":"long"},
            {"name":"skip_mid","type":{"type":"record","name":"Inner","fields":[
                {"name":"x","type":"double"},
                {"name":"y","type":["null","string"]}]}},
            {"name":"keep_b","type":"string"},
            {"name":"skip_tail","type":"boolean"}]}"#;
        #[derive(Debug, Deserialize, PartialEq)]
        struct R {
            keep_a: i64,
            keep_b: String,
        }
        let mut datum = Vec::new();
        // skip_head: a negative-count block of 2 strings, then terminator
        let block: Vec<u8> = [avro_str("aa"), avro_str("bb")].concat();
        datum.extend(zig(-2));
        datum.extend(zig(block.len() as i64));
        datum.extend(&block);
        datum.extend(zig(0));
        // keep_a
        datum.extend(zig(41));
        // skip_mid: double + union(string)
        datum.extend_from_slice(&1.5f64.to_le_bytes());
        datum.extend(zig(1));
        datum.extend(avro_str("ignored"));
        // keep_b
        datum.extend(avro_str("kept"));
        // skip_tail
        datum.push(1);
        let got: R = decode(&schema(SCH), &datum).unwrap();
        assert_eq!(
            got,
            R {
                keep_a: 41,
                keep_b: "kept".into()
            }
        );
    }

    #[test]
    fn skipped_negative_block_with_bad_size_is_an_error() {
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"skip","type":{"type":"array","items":"long"}},
            {"name":"keep","type":"long"}]}"#;
        #[derive(Debug, Deserialize)]
        #[expect(dead_code, reason = "shape only")]
        struct R {
            keep: i64,
        }
        let mut datum = zig(-1);
        datum.extend(zig(1 << 40)); // block byte size far beyond the buffer
        datum.extend(zig(9));
        let err = decode::<R>(&schema(SCH), &datum).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn borrowed_str_points_into_the_datum() {
        #[derive(Debug, Deserialize)]
        struct R<'a> {
            s: &'a str,
        }
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"s","type":"string"}]}"#;
        let datum = avro_str("borrowed");
        let sch = schema(SCH);
        let names = Names::new();
        let got: R<'_> = decode_datum(&sch, &names, &datum).unwrap();
        let datum_range = datum.as_ptr() as usize..datum.as_ptr() as usize + datum.len();
        assert!(datum_range.contains(&(got.s.as_ptr() as usize)));
        assert_eq!(got.s, "borrowed");
    }

    #[test]
    fn named_ref_resolves_through_the_enclosing_namespace() {
        // `Point` is declared inside namespace `geo` and referenced by its
        // short name from a sibling field — resolution must thread the
        // enclosing namespace exactly as decode_internal does.
        const SCH: &str = r#"{"type":"record","name":"Pair","namespace":"geo","fields":[
            {"name":"a","type":{"type":"record","name":"Point","fields":[
                {"name":"x","type":"long"}]}},
            {"name":"b","type":"Point"}]}"#;
        #[derive(Debug, Deserialize, PartialEq)]
        struct Point {
            x: i64,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Pair {
            a: Point,
            b: Point,
        }
        let mut datum = zig(3);
        datum.extend(zig(4));
        let got: Pair = decode(&schema(SCH), &datum).unwrap();
        assert_eq!(
            got,
            Pair {
                a: Point { x: 3 },
                b: Point { x: 4 }
            }
        );
    }

    #[test]
    fn duration_and_bigdecimal_are_rejected_at_spec_compile() {
        let dur = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"d","type":{"type":"fixed","name":"F","size":12,"logicalType":"duration"}}]}"#,
        );
        let err = compile_spec(&dur).unwrap_err();
        assert!(err.contains("does not support"), "{err}");
        let big = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"d","type":{"type":"bytes","logicalType":"big-decimal"}}]}"#,
        );
        let err = compile_spec(&big).unwrap_err();
        assert!(err.contains("does not support"), "{err}");
    }

    #[test]
    fn hostile_count_over_zero_width_items_is_bounded() {
        // Null items are zero-width: only the item budget bounds the walk.
        const SCH: &str = r#"{"type":"array","items":"null"}"#;
        let err = decode::<Vec<()>>(&schema(SCH), &zig(i64::MAX)).unwrap_err();
        assert!(err.0.contains("item budget"), "{err}");
        // The same count in a *skipped* field.
        const REC: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"skip","type":{"type":"array","items":"null"}},
            {"name":"keep","type":"long"}]}"#;
        #[derive(Debug, Deserialize)]
        #[expect(dead_code, reason = "shape only")]
        struct R {
            keep: i64,
        }
        let err = decode::<R>(&schema(REC), &zig(i64::MAX)).unwrap_err();
        assert!(err.0.contains("item budget"), "{err}");
        // A run of blocks cannot ratchet past the budget either.
        let mut blocks = Vec::new();
        for _ in 0..64 {
            blocks.extend(zig(2048));
        }
        let err = decode::<Vec<()>>(&schema(SCH), &blocks).unwrap_err();
        assert!(err.0.contains("item budget"), "{err}");
        // Within the budget, zero-width items decode.
        let mut ok = zig(1000);
        ok.extend(zig(0));
        let got: Vec<()> = decode(&schema(SCH), &ok).unwrap();
        assert_eq!(got.len(), 1000);
    }

    #[test]
    fn skipped_enum_index_is_bounds_checked() {
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"skip","type":{"type":"enum","name":"E","symbols":["A","B"]}},
            {"name":"keep","type":"long"}]}"#;
        #[derive(Debug, Deserialize, PartialEq)]
        struct R {
            keep: i64,
        }
        let mut datum = zig(999);
        datum.extend(zig(7));
        let err = decode::<R>(&schema(SCH), &datum).unwrap_err();
        assert!(err.0.contains("enum index 999 out of range"), "{err}");
        let mut datum = zig(1);
        datum.extend(zig(7));
        assert_eq!(decode::<R>(&schema(SCH), &datum).unwrap(), R { keep: 7 });
    }

    #[test]
    fn string_position_decodes_a_unit_enum() {
        // The `from_value` string-into-unit-enum arm, mirrored.
        #[derive(Debug, Deserialize, PartialEq)]
        enum E {
            A,
            B,
        }
        assert_eq!(
            decode::<E>(&schema(r#""string""#), &avro_str("B")).unwrap(),
            E::B
        );
        let err = decode::<E>(&schema(r#""string""#), &avro_str("C")).unwrap_err();
        assert!(err.0.contains("unknown variant"), "{err}");
        let _ = E::A;
    }

    #[test]
    fn truncated_boolean_is_an_error_not_null() {
        // The third lenient-EOF site in decode_internal; the union index
        // and the string body are pinned above.
        let err = decode::<bool>(&schema(r#""boolean""#), &[]).unwrap_err();
        assert!(err.0.contains("truncated"), "{err}");
    }

    #[test]
    fn rust_enum_over_a_record_schema_is_an_error() {
        // `from_value`'s legacy record-with-a-`"type"`-field arm is not
        // replicated (see the module docs).
        #[derive(Debug, Deserialize)]
        enum E {
            A,
        }
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"type","type":"string"}]}"#;
        let err = decode::<E>(&schema(SCH), &avro_str("A")).unwrap_err();
        assert!(err.0.contains("expected a union, enum, or string"), "{err}");
    }

    #[test]
    fn enum_symbols_are_never_borrowed() {
        // Symbols live in the schema, not the payload: a `&'de str` target
        // over an enum position must fail rather than dangle.
        let sch = schema(r#"{"type":"enum","name":"E","symbols":["A"]}"#);
        let err = decode::<&str>(&sch, &zig(0)).unwrap_err();
        assert!(err.0.contains("borrowed string"), "{err}");
    }

    #[test]
    fn is_human_readable_is_false() {
        struct Probe(bool);
        impl<'de> Deserialize<'de> for Probe {
            fn deserialize<D>(d: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let hr = d.is_human_readable();
                serde::de::IgnoredAny::deserialize(d)?;
                Ok(Probe(hr))
            }
        }
        let got: Probe = decode(&schema(r#""long""#), &zig(1)).unwrap();
        assert!(!got.0);
    }

    /// A bytes-visiting target for positions that only surface through
    /// `deserialize_bytes` (decimal wire bytes, string-as-bytes).
    #[derive(Debug, PartialEq)]
    struct RawBytes(Vec<u8>);
    impl<'de> Deserialize<'de> for RawBytes {
        fn deserialize<D>(d: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct V;
            impl Visitor<'_> for V {
                type Value = RawBytes;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("bytes")
                }
                fn visit_bytes<E>(self, v: &[u8]) -> Result<RawBytes, E> {
                    Ok(RawBytes(v.to_vec()))
                }
            }
            d.deserialize_bytes(V)
        }
    }

    #[test]
    fn zero_length_decimal_passes_through() {
        // Accepting-direction corner: `Decimal::to_vec` cannot render a
        // zero-length value, so the two-pass path errors where the wire
        // bytes here pass through unchanged (see the module docs).
        const SCH: &str = r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#;
        let got: RawBytes = decode(&schema(SCH), &zig(0)).unwrap();
        assert_eq!(got, RawBytes(Vec::new()));
    }

    #[test]
    fn bytes_target_over_a_string_position_skips_utf8_validation() {
        // Accepting-direction corner: decode_internal UTF-8-validates
        // every string; a bytes target here receives the raw bytes.
        let mut datum = zig(2);
        datum.extend_from_slice(&[0xFF, 0xFE]);
        let got: RawBytes = decode(&schema(r#""string""#), &datum).unwrap();
        assert_eq!(got, RawBytes(vec![0xFF, 0xFE]));
    }

    #[test]
    fn skipped_negative_block_trusts_its_size() {
        // Documented corner: the fast skip honors the declared byte size;
        // the decode paths walk the items and ignore it. A payload whose
        // size field lies therefore skips differently than it decodes.
        const SCH: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"arr","type":{"type":"array","items":"long"}},
            {"name":"keep","type":"long"}]}"#;
        #[derive(Debug, Deserialize)]
        struct Full {
            arr: Vec<i64>,
            keep: i64,
        }
        #[derive(Debug, Deserialize)]
        struct Skipping {
            keep: i64,
        }
        // One size-prefixed block whose declared size (2) covers a filler
        // byte beyond its single 1-byte item.
        let datum: Vec<u8> = vec![0x01, 0x04, 0x00, 0x00, 0x00, 0x02, 0x04];
        let full: Full = decode(&schema(SCH), &datum).unwrap();
        assert_eq!((full.arr, full.keep), (vec![0], 0));
        let skipping: Skipping = decode(&schema(SCH), &datum).unwrap();
        assert_eq!(skipping.keep, 1);
    }
}
