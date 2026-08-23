//! Confluent wire-format header parsing over arbitrary payload bytes.
//!
//! A Kafka message body reaches `parse_confluent` exactly as the broker
//! delivered it. The target checks the parse against a reference oracle: the
//! payload parses when it is at least 5 bytes long and opens with the `0x00`
//! magic byte, the schema id is the big-endian `u32` at bytes 1..5, and the
//! datum is the rest of the payload.

#![no_main]

use libfuzzer_sys::fuzz_target;
use spate_avro::parse_confluent;

fuzz_target!(|payload: &[u8]| {
    let framed = payload.len() >= 5 && payload[0] == 0x00;
    match parse_confluent(payload) {
        Ok((schema_id, datum)) => {
            assert!(
                framed,
                "parsed a payload the framing rejects: {payload:02x?}"
            );
            assert_eq!(
                schema_id,
                u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]),
                "schema id is the big-endian u32 at bytes 1..5"
            );
            assert_eq!(datum, &payload[5..], "the datum is the bytes after byte 5");
        }
        Err(_) => assert!(
            !framed,
            "rejected a Confluent-framed payload: {payload:02x?}"
        ),
    }
});
