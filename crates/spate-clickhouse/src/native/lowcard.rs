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

use super::NativeError;
use super::leaf::put_string;
use crate::schema::typeparse::ChType;
use bytes::{BufMut, BytesMut};
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use spate_core::hash::QualityState;
use std::hash::BuildHasher;

// `table` is keyed by row values a producer chooses, so it takes the quality
// hasher.
//   1. Wire bytes are hasher-independent. Dictionary order is first-seen
//      append order into `dict`, so the encoded block is byte-identical
//      regardless of hash function.
//   2. The dict is reset each block (entries ≤ values interned in the block
//      plus the reserved slots) and keeps one per-instance seed for the life
//      of the `LowCard`, so a crafted block costs at most a bounded per-block
//      probe count.

pub(crate) struct LowCard {
    nullable: bool,
    /// One slot per dictionary entry except a nullable dictionary's NULL
    /// placeholder. A slot points at its bytes in `dict`, so a new entry is
    /// not copied a second time.
    table: HashTable<Slot>,
    state: QualityState,
    /// Accumulated dictionary entries, each `[VarUInt len][bytes]`.
    dict: BytesMut,
    /// At most 2^32, the most entries a `u32` key indexes.
    dict_count: u64,
    /// One dictionary index per appended element (flattened for `Array(LC)`).
    keys: Vec<u32>,
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
            table: HashTable::new(),
            state: QualityState::default(),
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
            self.dict_count = 1;
        } else {
            // [0] = default (""); real values start at 1.
            self.dict_count = 0;
        }
        self.index_of(b"")
            .expect("a fresh dictionary has room for its default entry");
    }

    /// Intern a string value and push its key.
    ///
    /// Refuses a new value once the block's dictionary holds 2^32 entries,
    /// and leaves the dictionary and keys as they were.
    #[inline]
    pub(crate) fn intern(&mut self, value: &[u8]) -> Result<(), NativeError> {
        let idx = self.index_of(value)?;
        self.keys.push(idx);
        Ok(())
    }

    /// The dictionary index of `value`, appending it as a new entry if the
    /// block has not seen it.
    ///
    /// Refuses a new value once the dictionary holds 2^32 entries, and leaves
    /// the dictionary as it was.
    #[inline]
    fn index_of(&mut self, value: &[u8]) -> Result<u32, NativeError> {
        let hash = self.state.hash_one(value);
        let dict = &self.dict;
        match self
            .table
            .entry(hash, |s| &dict[s.start..s.end] == value, |s| s.hash)
        {
            Entry::Occupied(slot) => Ok(slot.get().idx),
            Entry::Vacant(slot) => {
                let idx = u32::try_from(self.dict_count)
                    .map_err(|_| NativeError::LowCardinalityDictionaryFull)?;
                put_string(&mut self.dict, value);
                let end = self.dict.len();
                slot.insert(Slot {
                    start: end - value.len(),
                    end,
                    idx,
                    hash,
                });
                self.dict_count += 1;
                Ok(idx)
            }
        }
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
        // Grow once and narrow into the new tail: a put per key checks
        // capacity each time and keeps the loop from vectorising.
        let start = out.len();
        out.resize(start + (self.keys.len() << code), 0);
        let tail = &mut out[start..];
        match code {
            0 => {
                for (d, &k) in tail.iter_mut().zip(&self.keys) {
                    *d = k as u8;
                }
            }
            1 => {
                for (d, &k) in tail.as_chunks_mut::<2>().0.iter_mut().zip(&self.keys) {
                    *d = (k as u16).to_le_bytes();
                }
            }
            _ => {
                for (d, &k) in tail.as_chunks_mut::<4>().0.iter_mut().zip(&self.keys) {
                    *d = k.to_le_bytes();
                }
            }
        }
    }

    /// Sets the entry count as if the block had interned that many values,
    /// so a test reaches the dictionary limit without 2^32 of them.
    #[cfg(test)]
    pub(super) fn set_dict_count(&mut self, n: u64) {
        self.dict_count = n;
    }

    pub(crate) fn reset(&mut self) {
        self.table.clear();
        self.dict.clear();
        self.keys.clear();
        self.seed();
    }

    /// The bytes [`write_prefix`](Self::write_prefix) and
    /// [`write_data`](Self::write_data) emit for the buffered block, exactly.
    pub(crate) fn byte_len(&self) -> usize {
        let prefix = 8;
        if self.keys.is_empty() {
            return prefix;
        }
        // Flags, dictionary size and key count are a UInt64 each.
        prefix + 24 + self.dict.len() + (self.keys.len() << key_width_code(self.dict_count))
    }
}

/// A dictionary entry's bytes in `dict` and its index. The hash is kept so
/// that growing the table reads no `dict` bytes.
#[derive(Clone, Copy)]
struct Slot {
    start: usize,
    end: usize,
    idx: u32,
    hash: u64,
}

/// The smallest key-width code that indexes `dict_size` entries:
/// 0=UInt8, 1=UInt16, 2=UInt32. [`LowCard::intern`] keeps `dict_size` at or
/// below 2^32, so UInt64 keys are never needed.
fn key_width_code(dict_size: u64) -> u8 {
    if dict_size <= (1 << 8) {
        0
    } else if dict_size <= (1 << 16) {
        1
    } else {
        2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `reset` clears the dictionary for the next block and keeps the hasher's
    /// per-instance seed.
    #[test]
    fn reset_keeps_the_dictionary_seed() {
        let mut lc = LowCard::build(&ChType::Named("String".into())).expect("builds");
        for i in 0..64 {
            lc.intern(format!("v{i}").as_bytes()).expect("interns");
        }
        let before = lc.state.hash_one(b"probe".as_slice());
        lc.reset();
        assert_eq!(lc.table.len(), 1, "only the reserved default slot remains");
        assert_eq!(lc.state.hash_one(b"probe".as_slice()), before);
    }

    /// The key bytes `write_data` emits for a block whose dictionary holds
    /// `distinct` entries, read back at the width its flags declare.
    fn written_keys(distinct: usize) -> (u64, Vec<u64>, Vec<u64>) {
        let mut lc = LowCard::build(&ChType::Named("String".into())).expect("builds");
        // Each value once, then the first and last again, then the default.
        for i in 0..distinct {
            lc.intern(format!("v{i}").as_bytes()).expect("interns");
        }
        lc.intern(b"v0").expect("interns");
        lc.intern(format!("v{}", distinct - 1).as_bytes())
            .expect("interns");
        lc.push_default();
        let want = lc.keys.iter().map(|&k| u64::from(k)).collect::<Vec<u64>>();
        let mut out = BytesMut::new();
        lc.write_data(&mut out);
        assert_eq!(
            out.len() + 8,
            lc.byte_len(),
            "byte_len counts the written bytes"
        );
        let flags = u64::from_le_bytes(out[..8].try_into().unwrap());
        let width = 1usize << (flags & 0xff);
        let keys_at = out.len() - want.len() * width;
        let count = u64::from_le_bytes(out[keys_at - 8..keys_at].try_into().unwrap());
        assert_eq!(count, want.len() as u64);
        let got = out[keys_at..]
            .chunks_exact(width)
            .map(|c| {
                let mut b = [0u8; 8];
                b[..width].copy_from_slice(c);
                u64::from_le_bytes(b)
            })
            .collect();
        (flags, want, got)
    }

    /// Keys narrow to one, two and four little-endian bytes as the
    /// dictionary crosses 256 and 65,536 entries.
    #[test]
    fn keys_narrow_to_the_dictionary_width() {
        for (distinct, code) in [(255, 0), (256, 1), (65_535, 1), (65_536, 2)] {
            let (flags, want, got) = written_keys(distinct);
            assert_eq!(flags, 0x600 | code, "{distinct} values");
            assert_eq!(got, want, "{distinct} values");
            assert_eq!(*want.last().unwrap(), 0);
            assert_eq!(want[want.len() - 2], distinct as u64);
        }
    }

    /// A new value takes key `u32::MAX`, the next new one is refused with the
    /// block left as it was, and an interned value still takes its key.
    #[test]
    fn a_full_dictionary_refuses_a_new_value() {
        let mut lc = LowCard::build(&ChType::Named("String".into())).expect("builds");
        lc.dict_count = u64::from(u32::MAX);
        lc.intern(b"last").expect("key u32::MAX fits");
        assert_eq!(lc.keys, [u32::MAX]);
        assert_eq!(lc.dict_count, 1 << 32);

        let dict = lc.dict.clone();
        assert!(matches!(
            lc.intern(b"one more"),
            Err(NativeError::LowCardinalityDictionaryFull)
        ));
        assert_eq!(lc.dict, dict);
        assert_eq!(lc.keys, [u32::MAX]);
        assert_eq!(lc.dict_count, 1 << 32);
        assert_eq!(lc.table.len(), 2);

        lc.intern(b"last")
            .expect("an interned value needs no new entry");
        assert_eq!(lc.keys, [u32::MAX, u32::MAX]);
    }
}
