//! Byte-unit tests for the Native encoder: hand-computed block bytes for
//! each mechanism (header, fixed-width, String, Nullable null-map, Array
//! offsets, LowCardinality dictionary). These run without Docker; the
//! server round-trip (`tests/container.rs`) is the ground-truth oracle.

use super::{NativeEncoder, NativeSchema};
use etl_core::deser::Owned;
use serde::Serialize;
use std::sync::Arc;

fn schema(cols: &[(&str, &str)]) -> Arc<NativeSchema> {
    NativeSchema::from_columns(cols).expect("schema builds")
}

fn block<R: Serialize>(cols: &[(&str, &str)], rows: &[R]) -> Vec<u8> {
    NativeEncoder::<()>::block_of(schema(cols), rows).expect("encode")
}

/// `[VarUInt cols][VarUInt rows]` then per column `[name][type]`.
fn header(cols: u64, rows: u64) -> Vec<u8> {
    vec![cols as u8, rows as u8]
}

fn string_bytes(s: &str) -> Vec<u8> {
    let mut v = vec![s.len() as u8];
    v.extend_from_slice(s.as_bytes());
    v
}

#[test]
fn single_uint64_column_two_rows() {
    #[derive(Serialize)]
    struct R {
        x: u64,
    }
    let got = block(&[("x", "UInt64")], &[R { x: 1 }, R { x: 2 }]);

    let mut want = header(1, 2);
    want.extend(string_bytes("x")); // name
    want.extend(string_bytes("UInt64")); // type
    want.extend_from_slice(&1u64.to_le_bytes());
    want.extend_from_slice(&2u64.to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn nullable_string_writes_nullmap_then_dense_values() {
    #[derive(Serialize)]
    struct R {
        s: Option<String>,
    }
    let got = block(
        &[("s", "Nullable(String)")],
        &[
            R {
                s: Some("ab".into()),
            },
            R { s: None },
        ],
    );

    let mut want = header(1, 2);
    want.extend(string_bytes("s"));
    want.extend(string_bytes("Nullable(String)"));
    // null-map: present, NULL.
    want.extend_from_slice(&[0, 1]);
    // dense value stream: "ab", then the empty-string placeholder at NULL.
    want.extend(string_bytes("ab"));
    want.extend(string_bytes(""));
    assert_eq!(got, want);
}

#[test]
fn array_uint32_writes_cumulative_u64_offsets_then_flattened() {
    #[derive(Serialize)]
    struct R {
        a: Vec<u32>,
    }
    let got = block(
        &[("a", "Array(UInt32)")],
        &[R { a: vec![7, 8] }, R { a: vec![] }, R { a: vec![9] }],
    );

    let mut want = header(1, 3);
    want.extend(string_bytes("a"));
    want.extend(string_bytes("Array(UInt32)"));
    // cumulative end offsets: 2, 2, 3.
    want.extend_from_slice(&2u64.to_le_bytes());
    want.extend_from_slice(&2u64.to_le_bytes());
    want.extend_from_slice(&3u64.to_le_bytes());
    // flattened values.
    want.extend_from_slice(&7u32.to_le_bytes());
    want.extend_from_slice(&8u32.to_le_bytes());
    want.extend_from_slice(&9u32.to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn lowcardinality_string_dictionary_and_keys() {
    #[derive(Serialize)]
    struct R {
        c: String,
    }
    let got = block(
        &[("c", "LowCardinality(String)")],
        &[
            R { c: "a".into() },
            R { c: "b".into() },
            R { c: "a".into() },
        ],
    );

    let mut want = header(1, 3);
    want.extend(string_bytes("c"));
    want.extend(string_bytes("LowCardinality(String)"));
    // prefix: serialization version = 1 (Int64 LE).
    want.extend_from_slice(&1i64.to_le_bytes());
    // metadata: HasAdditionalKeys | NeedUpdateDictionary | UInt8 keys.
    want.extend_from_slice(&0x600u64.to_le_bytes());
    // dict_size: reserved "" + "a" + "b" = 3.
    want.extend_from_slice(&3u64.to_le_bytes());
    // dict entries.
    want.extend(string_bytes("")); // slot 0 (default)
    want.extend(string_bytes("a")); // slot 1
    want.extend(string_bytes("b")); // slot 2
    // keys_count then u8 keys.
    want.extend_from_slice(&3u64.to_le_bytes());
    want.extend_from_slice(&[1, 2, 1]);
    assert_eq!(got, want);
}

#[test]
fn lowcardinality_nullable_uses_key_zero_for_null() {
    #[derive(Serialize)]
    struct R {
        c: Option<String>,
    }
    let got = block(
        &[("c", "LowCardinality(Nullable(String))")],
        &[
            R {
                c: Some("x".into()),
            },
            R { c: None },
        ],
    );

    let mut want = header(1, 2);
    want.extend(string_bytes("c"));
    want.extend(string_bytes("LowCardinality(Nullable(String))"));
    want.extend_from_slice(&1i64.to_le_bytes()); // version
    want.extend_from_slice(&0x600u64.to_le_bytes()); // metadata
    // dict_size: reserved [0]=NULL, [1]=default, plus "x" = 3.
    want.extend_from_slice(&3u64.to_le_bytes());
    want.extend(string_bytes("")); // slot 0 NULL placeholder
    want.extend(string_bytes("")); // slot 1 default
    want.extend(string_bytes("x")); // slot 2
    want.extend_from_slice(&2u64.to_le_bytes()); // keys_count
    // "x" -> key 2, NULL -> key 0.
    want.extend_from_slice(&[2, 0]);
    assert_eq!(got, want);
}

#[test]
fn multi_column_row_interleaves_into_separate_column_streams() {
    #[derive(Serialize)]
    struct R {
        id: u32,
        name: String,
    }
    let got = block(
        &[("id", "UInt32"), ("name", "String")],
        &[
            R {
                id: 1,
                name: "a".into(),
            },
            R {
                id: 2,
                name: "bb".into(),
            },
        ],
    );

    let mut want = header(2, 2);
    // Column 0: all ids together.
    want.extend(string_bytes("id"));
    want.extend(string_bytes("UInt32"));
    want.extend_from_slice(&1u32.to_le_bytes());
    want.extend_from_slice(&2u32.to_le_bytes());
    // Column 1: all names together.
    want.extend(string_bytes("name"));
    want.extend(string_bytes("String"));
    want.extend(string_bytes("a"));
    want.extend(string_bytes("bb"));
    assert_eq!(got, want);
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_is_two_le_u64_halves() {
    #[derive(Serialize)]
    struct R {
        #[serde(with = "crate::serde::uuid")]
        id: uuid::Uuid,
    }
    let id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    let got = block(&[("id", "UUID")], &[R { id }]);

    let (hi, lo) = id.as_u64_pair();
    let mut want = header(1, 1);
    want.extend(string_bytes("id"));
    want.extend(string_bytes("UUID"));
    want.extend_from_slice(&hi.to_le_bytes());
    want.extend_from_slice(&lo.to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn ipv6_is_sixteen_network_order_octets() {
    #[derive(Serialize)]
    struct R {
        ip: std::net::Ipv6Addr,
    }
    let ip = std::net::Ipv6Addr::LOCALHOST; // ::1
    let got = block(&[("ip", "IPv6")], &[R { ip }]);

    let mut want = header(1, 1);
    want.extend(string_bytes("ip"));
    want.extend(string_bytes("IPv6"));
    want.extend_from_slice(&ip.octets());
    assert_eq!(got, want);
}

#[test]
fn decimal_writes_the_backing_le_integer() {
    use crate::types::Decimal64;
    #[derive(Serialize)]
    struct R {
        d: Decimal64<4>,
    }
    let got = block(
        &[("d", "Decimal(18, 4)")],
        &[R {
            d: Decimal64(-15_000),
        }],
    );

    let mut want = header(1, 1);
    want.extend(string_bytes("d"));
    want.extend(string_bytes("Decimal(18, 4)"));
    want.extend_from_slice(&(-15_000i64).to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn map_writes_offsets_then_separate_key_and_value_streams() {
    #[derive(Serialize)]
    struct R {
        m: std::collections::BTreeMap<String, u32>,
    }
    let mut m = std::collections::BTreeMap::new();
    m.insert("a".to_string(), 10u32);
    m.insert("b".to_string(), 20u32);
    let got = block(&[("m", "Map(String, UInt32)")], &[R { m }]);

    let mut want = header(1, 1);
    want.extend(string_bytes("m"));
    want.extend(string_bytes("Map(String, UInt32)"));
    // offsets: 2 pairs after row 0.
    want.extend_from_slice(&2u64.to_le_bytes());
    // keys stream (BTreeMap orders a,b), then values stream.
    want.extend(string_bytes("a"));
    want.extend(string_bytes("b"));
    want.extend_from_slice(&10u32.to_le_bytes());
    want.extend_from_slice(&20u32.to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn tuple_point_writes_one_stream_per_element() {
    #[derive(Serialize)]
    struct R {
        p: (f64, f64),
    }
    let got = block(
        &[("p", "Point")],
        &[R { p: (1.0, 2.0) }, R { p: (3.0, 4.0) }],
    );

    let mut want = header(1, 2);
    want.extend(string_bytes("p"));
    want.extend(string_bytes("Point"));
    // element 0 stream (all x), then element 1 stream (all y).
    want.extend_from_slice(&1.0f64.to_le_bytes());
    want.extend_from_slice(&3.0f64.to_le_bytes());
    want.extend_from_slice(&2.0f64.to_le_bytes());
    want.extend_from_slice(&4.0f64.to_le_bytes());
    assert_eq!(got, want);
}

#[test]
fn array_of_lowcardinality_puts_dictionary_prefix_before_offsets() {
    #[derive(Serialize)]
    struct R {
        tags: Vec<String>,
    }
    let got = block(
        &[("tags", "Array(LowCardinality(String))")],
        &[R {
            tags: vec!["a".into(), "b".into()],
        }],
    );

    // The composite prefix-ordering rule: the inner LowCardinality state
    // prefix (Int64 = 1) must precede the array's cumulative offsets. Exact
    // dictionary bytes are validated against a live server; here we pin the
    // ordering that is the trickiest correctness point.
    let head = 2 + string_bytes("tags").len() + string_bytes("Array(LowCardinality(String))").len();
    assert_eq!(
        &got[head..head + 8],
        &1i64.to_le_bytes(),
        "LowCardinality version prefix must come before the array offsets"
    );
    assert_eq!(
        &got[head + 8..head + 16],
        &2u64.to_le_bytes(),
        "array offset (2 elements) follows the LC prefix"
    );
}

#[test]
fn unsupported_column_type_fails_at_build() {
    let err = NativeSchema::from_columns(&[("v", "Variant(Int64, String)")]);
    assert!(err.is_err(), "Variant must be rejected at build");
}

#[test]
fn zero_rows_produce_no_bytes() {
    #[derive(Serialize)]
    struct R {
        x: u64,
    }
    let got = block::<R>(&[("x", "UInt64")], &[]);
    assert!(got.is_empty(), "a zero-row chunk emits nothing");
}

// The `encode` path (not `block_of`) runs the first-record struct check:
// field names/order always, type classes when the schema carries `full`.
mod first_record_check {
    use super::*;
    use crate::config::SchemaValidation;
    use crate::schema::RowSchema;
    use crate::types::DateTime64Millis;
    use bytes::BytesMut;
    use etl_core::checkpoint::AckRef;
    use etl_core::error::{ErrorClass, SinkError};
    use etl_core::record::{PartitionId, Record, RecordMeta};
    use etl_core::sink::RowEncoder;

    fn record<T>(payload: T) -> Record<T> {
        let (ack, rx) = AckRef::test_pair();
        std::mem::forget(rx);
        Record {
            payload,
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: 0,
                event_time_ms: 0,
                key_hash: None,
            },
            ack,
        }
    }

    /// A schema as `native_schema()` would build it from a live table
    /// fetched under `validate_schema: full`.
    fn full_schema(cols: &[(&str, &str)]) -> Arc<NativeSchema> {
        let expected = RowSchema {
            mode: SchemaValidation::Full,
            table: "`t`".into(),
            columns: cols
                .iter()
                .map(|(n, t)| {
                    (
                        (*n).to_string(),
                        crate::schema::typeparse::parse(t),
                        (*t).to_string(),
                    )
                })
                .collect(),
        };
        NativeSchema::from_row_schema(&expected).expect("schema builds")
    }

    fn fatal_reason(err: SinkError) -> String {
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, ErrorClass::Fatal, "{reason}");
                reason
            }
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn full_mode_rejects_a_wrapper_scale_mismatch() {
        // The struct declares milli scale via the wire wrapper; the table
        // column is micro precision. Without this check every timestamp
        // would land ~1000x too small (1970-era) — the raw Int64 layout
        // cannot notice.
        #[derive(Serialize)]
        struct R {
            ts: DateTime64Millis,
        }
        let mut enc = NativeEncoder::<Owned<R>>::new(full_schema(&[("ts", "DateTime64(6)")]));
        let err = enc
            .encode(
                &record(R {
                    ts: DateTime64Millis(1_700_000_000_000),
                }),
                &mut BytesMut::new(),
            )
            .expect_err("scale mismatch must be rejected at the first record");
        let reason = fatal_reason(err);
        assert!(
            reason.contains("DateTime64Millis") && reason.contains("DateTime64(6)"),
            "{reason}"
        );
    }

    #[test]
    fn full_mode_accepts_a_matching_wrapper_scale_and_encodes_raw_int64() {
        #[derive(Serialize)]
        struct R {
            ts: DateTime64Millis,
        }
        let mut enc = NativeEncoder::<Owned<R>>::new(full_schema(&[("ts", "DateTime64(3)")]));
        let mut buf = BytesMut::new();
        enc.encode(
            &record(R {
                ts: DateTime64Millis(1_700_000_000_000),
            }),
            &mut buf,
        )
        .expect("matching scale encodes");
        enc.finish_chunk(&mut buf).expect("finish");
        // The wrapper is wire-transparent: the column data is the raw LE i64.
        assert!(
            buf.ends_with(&1_700_000_000_000i64.to_le_bytes()),
            "wrapper must encode as the raw little-endian Int64"
        );
    }

    #[test]
    fn full_mode_rejects_a_class_incompatible_plain_field() {
        // Not just wrappers: `full` brings the whole class matrix to the
        // Native path (a u64 field cannot feed an Int64 column).
        #[derive(Serialize)]
        struct R {
            n: u64,
        }
        let mut enc = NativeEncoder::<Owned<R>>::new(full_schema(&[("n", "Int64")]));
        let err = enc
            .encode(&record(R { n: 1 }), &mut BytesMut::new())
            .expect_err("class mismatch must be rejected");
        let reason = fatal_reason(err);
        assert!(reason.contains("not compatible"), "{reason}");
    }

    #[test]
    fn full_mode_cannot_check_an_undeclared_scale() {
        // A plain i64 declares no scale, so `full` has nothing to compare:
        // the docs tell users to declare intent through the wrappers.
        #[derive(Serialize)]
        struct R {
            ts: i64,
        }
        let mut enc = NativeEncoder::<Owned<R>>::new(full_schema(&[("ts", "DateTime64(6)")]));
        enc.encode(
            &record(R {
                ts: 1_700_000_000_000,
            }),
            &mut BytesMut::new(),
        )
        .expect("an undeclared scale is not checkable and must pass");
    }

    #[test]
    fn static_schemas_check_names_only() {
        // `from_columns` has no fetched truth: the wrapper mismatch that
        // `full` rejects passes here (names-level check only).
        #[derive(Serialize)]
        struct R {
            ts: DateTime64Millis,
        }
        let mut enc = NativeEncoder::<Owned<R>>::new(schema(&[("ts", "DateTime64(6)")]));
        enc.encode(
            &record(R {
                ts: DateTime64Millis(1),
            }),
            &mut BytesMut::new(),
        )
        .expect("static schemas stay at the name-level check");
    }

    #[test]
    fn rejects_a_same_type_field_column_swap() {
        // Struct declares b-then-a; columns are a-then-b (both UInt32). A
        // positional encoder without the name check would silently mis-column.
        #[derive(Serialize)]
        struct Swapped {
            b: u32,
            a: u32,
        }
        let mut enc =
            NativeEncoder::<Owned<Swapped>>::new(schema(&[("a", "UInt32"), ("b", "UInt32")]));
        let err = enc
            .encode(&record(Swapped { b: 1, a: 2 }), &mut BytesMut::new())
            .expect_err("field-name swap must be rejected");
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, ErrorClass::Fatal);
                assert!(reason.contains("`b`") && reason.contains("`a`"), "{reason}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn accepts_matching_field_names_and_encodes() {
        #[derive(Serialize)]
        struct Ok2 {
            a: u32,
            b: u32,
        }
        let mut enc = NativeEncoder::<Owned<Ok2>>::new(schema(&[("a", "UInt32"), ("b", "UInt32")]));
        let mut buf = BytesMut::new();
        enc.encode(&record(Ok2 { a: 1, b: 2 }), &mut buf)
            .expect("matching names encode");
        enc.finish_chunk(&mut buf).expect("finish");
        assert!(!buf.is_empty(), "a matching row produces a block");
    }

    #[test]
    fn borrowed_rows_encode_identically_to_owned() {
        // A borrowed family produces the same block bytes as its owned
        // equivalent: the Native encoder only needs `Serialize` on the row.
        #[derive(Serialize)]
        struct RowOwned {
            id: u32,
            s: String,
        }
        #[derive(Serialize)]
        struct RowRef<'a> {
            id: u32,
            s: &'a str,
        }
        struct RowRefFam;
        impl etl_core::deser::RecFamily for RowRefFam {
            type Rec<'buf> = RowRef<'buf>;
        }
        let cols = &[("id", "UInt32"), ("s", "String")];

        let mut owned_enc = NativeEncoder::<Owned<RowOwned>>::new(schema(cols));
        let mut owned_buf = BytesMut::new();
        owned_enc
            .encode(
                &record(RowOwned {
                    id: 9,
                    s: "zero".into(),
                }),
                &mut owned_buf,
            )
            .expect("owned encodes");
        owned_enc.finish_chunk(&mut owned_buf).expect("finish");

        let text = String::from("zero");
        let mut ref_enc = NativeEncoder::<RowRefFam>::new(schema(cols));
        let mut ref_buf = BytesMut::new();
        ref_enc
            .encode(&record(RowRef { id: 9, s: &text }), &mut ref_buf)
            .expect("borrowed encodes");
        ref_enc.finish_chunk(&mut ref_buf).expect("finish");

        assert_eq!(owned_buf, ref_buf);
    }
}
