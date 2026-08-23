//! Avro single-object encoding header parsing over arbitrary payload bytes.
//!
//! A Kafka message body reaches `parse_single_object` exactly as the broker
//! delivered it. The target checks the parse against a reference oracle: the
//! payload parses when it is at least 10 bytes long and opens with the
//! `0xC3 0x01` magic, the fingerprint is the little-endian `u64` at bytes
//! 2..10, and the datum is the rest of the payload.

#![no_main]

use libfuzzer_sys::fuzz_target;
use spate_avro::parse_single_object;

fuzz_target!(|payload: &[u8]| {
    let framed = payload.len() >= 10 && payload[0] == 0xC3 && payload[1] == 0x01;
    match parse_single_object(payload) {
        Ok((fingerprint, datum)) => {
            assert!(
                framed,
                "parsed a payload the framing rejects: {payload:02x?}"
            );
            let bytes: [u8; 8] = payload[2..10].try_into().expect("length checked");
            assert_eq!(
                fingerprint,
                u64::from_le_bytes(bytes),
                "the fingerprint is the little-endian u64 at bytes 2..10"
            );
            assert_eq!(
                datum,
                &payload[10..],
                "the datum is the bytes after byte 10"
            );
        }
        Err(_) => assert!(
            !framed,
            "rejected a single-object-framed payload: {payload:02x?}"
        ),
    }
});
