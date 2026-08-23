//! RowBinary row encoding against a reference reader.
//!
//! RowBinary carries no column names, so a row's bytes are readable only by
//! the layout the `spate_clickhouse::rowbinary` module documents. The target
//! serializes a run of arbitrary rows into one buffer and reads them back with
//! a reader written from that documented layout, asserting each field round
//! trips and that the run consumes the buffer exactly. A width that drifts,
//! an endianness that flips, or a `Nullable` prefix that inverts moves the
//! reader off the row and fails here.

#![no_main]

use arbitrary::Arbitrary;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use serde::Serialize;
use spate_clickhouse::serialize_row;

/// One row covering the encodings the layout distinguishes: fixed-width
/// integers and floats, a length-prefixed string, a `Nullable` prefix, a
/// counted array, an untagged tuple, and a fixed-width byte array.
#[derive(Arbitrary, Debug, Serialize)]
struct Row {
    id: u64,
    delta: i32,
    flag: bool,
    ratio: f64,
    name: String,
    note: Option<i64>,
    tags: Vec<String>,
    pair: (i8, u16),
    fixed: [u8; 4],
}

/// A cursor over encoded rows, reading the layout the rowbinary module
/// documents. Every read panics when the buffer holds too few bytes.
struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        assert!(
            self.rest.len() >= n,
            "the row ran {n} bytes past its buffer"
        );
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        head
    }

    fn byte(&mut self) -> u8 {
        self.take(1)[0]
    }

    fn array<const N: usize>(&mut self) -> [u8; N] {
        self.take(N).try_into().expect("length taken")
    }

    /// The LEB128 length prefix a string or an array carries.
    fn leb128(&mut self) -> u64 {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.byte();
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
            assert!(shift < 64, "a length prefix ran past 64 bits");
        }
    }

    fn string(&mut self) -> String {
        let len = usize::try_from(self.leb128()).expect("a length that fits the buffer");
        String::from_utf8(self.take(len).to_vec()).expect("strings encode as UTF-8")
    }

    /// Read one row. The field initializers run in the order written, which
    /// is the order the encoder writes them in; do not reorder them.
    fn row(&mut self) -> Row {
        Row {
            id: u64::from_le_bytes(self.array()),
            delta: i32::from_le_bytes(self.array()),
            flag: match self.byte() {
                0 => false,
                1 => true,
                other => panic!("`{other}` is not a bool"),
            },
            ratio: f64::from_le_bytes(self.array()),
            name: self.string(),
            note: match self.byte() {
                0 => Some(i64::from_le_bytes(self.array())),
                1 => None,
                other => panic!("`{other}` is not a Nullable prefix"),
            },
            tags: {
                let count = usize::try_from(self.leb128()).expect("a count that fits the buffer");
                (0..count).map(|_| self.string()).collect()
            },
            pair: (
                i8::from_le_bytes(self.array()),
                u16::from_le_bytes(self.array()),
            ),
            fixed: self.array(),
        }
    }
}

fuzz_target!(|rows: Vec<Row>| {
    let mut buf = BytesMut::new();
    for row in &rows {
        serialize_row(row, &mut buf).expect("every field of Row has a RowBinary encoding");
    }

    let mut reader = Reader { rest: &buf };
    for row in &rows {
        let read = reader.row();
        assert_eq!(read.id, row.id);
        assert_eq!(read.delta, row.delta);
        assert_eq!(read.flag, row.flag);
        assert_eq!(read.ratio.to_bits(), row.ratio.to_bits());
        assert_eq!(read.name, row.name);
        assert_eq!(read.note, row.note);
        assert_eq!(read.tags, row.tags);
        assert_eq!(read.pair, row.pair);
        assert_eq!(read.fixed, row.fixed);
    }
    assert!(
        reader.rest.is_empty(),
        "{} bytes remain after {} rows",
        reader.rest.len(),
        rows.len()
    );
});
