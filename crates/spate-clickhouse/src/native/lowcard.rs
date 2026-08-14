//! `LowCardinality(String)` and `LowCardinality(Nullable(String))` columns:
//! a fresh per-block dictionary of distinct values plus a width-selected
//! index per row.
//!
//! First-cut scope is a `String` (optionally `Nullable(String)`) inner —
//! what real ClickHouse schemas overwhelmingly use `LowCardinality` for.
//! Other inners fail fatally at build.
//!
//! Per-block layout (written by [`LowCard::write_prefix`] then
//! [`LowCard::write_data`]):
//! ```text
//! prefix:  Int64 LE = 1                       (serialization version)
//! data:    UInt64 LE  0x600 | key_width_code  (HasAdditionalKeys|NeedUpdateDictionary)
//!          UInt64 LE  dict_size
//!          <dict entries: [VarUInt len][bytes] each>
//!          UInt64 LE  keys_count
//!          keys       keys_count × (1<<code) bytes, LE
//! ```
//! Reserved slots: a non-nullable dictionary reserves index 0 for the inner
//! default (`""`); a nullable one reserves index 0 = NULL and index 1 =
//! default, with null-ness carried by a key of 0 (no separate null-map).

use super::leaf::put_string;
use crate::schema::typeparse::ChType;
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;

// `map` interns per row, so it uses foldhash rather than std's SipHash. Two
// constraints keep that safe:
//   1. Wire bytes are hasher-independent. Dictionary order is first-seen
//      append order into `dict`, so the encoded block is byte-identical
//      regardless of hash function.
//   2. HashDoS exposure is bounded. The dict is per-block and reset each block
//      (entries ≤ rows per block), and foldhash's RandomState seeds per
//      instance, so the worst case is a bounded per-block probe cost rather
//      than unbounded amplification. SipHash is therefore not required here.
//      A hand-rolled FxHash fails on collision quality (trailing-digit
//      keys), which foldhash's avalanche fixes.
type DictMap = HashMap<Vec<u8>, u64, foldhash::fast::RandomState>;

pub(crate) struct LowCard {
    nullable: bool,
    /// Dictionary-entry raw bytes → assigned index.
    map: DictMap,
    /// Accumulated dictionary entries, each `[VarUInt len][bytes]`.
    dict: BytesMut,
    dict_count: u64,
    /// One dictionary index per appended element (flattened for `Array(LC)`).
    keys: Vec<u64>,
}

impl LowCard {
    pub(crate) fn build(inner: &ChType) -> Result<LowCard, String> {
        let nullable = match inner {
            ChType::Named(n) if n == "String" => false,
            ChType::Nullable(b) if matches!(&**b, ChType::Named(n) if n == "String") => true,
            other => {
                return Err(format!(
                    "LowCardinality({other:?}) — only LowCardinality(String) \
                     and LowCardinality(Nullable(String)) are supported"
                ));
            }
        };
        let mut lc = LowCard {
            nullable,
            map: DictMap::default(),
            dict: BytesMut::new(),
            dict_count: 0,
            keys: Vec::new(),
        };
        lc.seed();
        Ok(lc)
    }

    /// Seed the reserved dictionary slots for a fresh block.
    fn seed(&mut self) {
        if self.nullable {
            // [0] = NULL placeholder (default bytes), [1] = default; real
            // values start at 2. Null-ness is a key of 0.
            put_string(&mut self.dict, b"");
            put_string(&mut self.dict, b"");
            self.dict_count = 2;
            self.map.insert(Vec::new(), 1);
        } else {
            // [0] = default (""); real values start at 1.
            put_string(&mut self.dict, b"");
            self.dict_count = 1;
            self.map.insert(Vec::new(), 0);
        }
    }

    /// Intern a string value and push its key.
    #[inline]
    pub(crate) fn intern(&mut self, value: &[u8]) {
        let idx = match self.map.get(value) {
            Some(&idx) => idx,
            None => {
                let idx = self.dict_count;
                put_string(&mut self.dict, value);
                self.dict_count += 1;
                self.map.insert(value.to_vec(), idx);
                idx
            }
        };
        self.keys.push(idx);
    }

    /// Push a NULL (only valid for a nullable dictionary): key 0.
    pub(crate) fn push_null(&mut self) {
        self.keys.push(0);
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Append one default entry (key 0 = default/NULL slot).
    pub(crate) fn push_default(&mut self) {
        self.keys.push(0);
    }

    pub(crate) fn write_prefix(&self, out: &mut BytesMut) {
        // "sharedDictionariesWithAdditionalKeys", the only defined version.
        out.put_i64_le(1);
    }

    pub(crate) fn write_data(&self, out: &mut BytesMut) {
        // An empty key run (e.g. an Array(LowCardinality) block whose arrays
        // are all empty) writes nothing after the state prefix.
        if self.keys.is_empty() {
            return;
        }
        let code = key_width_code(self.dict_count);
        // HasAdditionalKeys (0x200) | NeedUpdateDictionary (0x400) | width.
        out.put_u64_le(0x600 | u64::from(code));
        out.put_u64_le(self.dict_count);
        out.put_slice(&self.dict);
        out.put_u64_le(self.keys.len() as u64);
        // Key width is uniform across the block (fixed by the final dict
        // size), so branch once rather than per key.
        match code {
            0 => {
                for &k in &self.keys {
                    out.put_u8(k as u8);
                }
            }
            1 => {
                for &k in &self.keys {
                    out.put_u16_le(k as u16);
                }
            }
            2 => {
                for &k in &self.keys {
                    out.put_u32_le(k as u32);
                }
            }
            _ => {
                for &k in &self.keys {
                    out.put_u64_le(k);
                }
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        self.map.clear();
        self.dict.clear();
        self.keys.clear();
        self.dict_count = 0;
        self.seed();
    }

    pub(crate) fn byte_len(&self) -> usize {
        self.dict.len() + self.keys.len() * 8
    }
}

/// The smallest key-width code that indexes `dict_size` entries:
/// 0=UInt8, 1=UInt16, 2=UInt32, 3=UInt64.
fn key_width_code(dict_size: u64) -> u8 {
    if dict_size <= (1 << 8) {
        0
    } else if dict_size <= (1 << 16) {
        1
    } else if dict_size <= (1 << 32) {
        2
    } else {
        3
    }
}
