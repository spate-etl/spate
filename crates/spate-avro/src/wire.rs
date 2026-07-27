//! Avro payload framings.
//!
//! Kafka messages carry bare Avro datums wrapped in one of three framings:
//!
//! - **Confluent wire format** (the default): `0x00` magic byte, a 4-byte
//!   big-endian schema-registry id, then the datum.
//! - **Single-object encoding** (Avro spec): `0xC3 0x01` magic, the
//!   8-byte little-endian Rabin fingerprint of the writer schema, then the
//!   datum.
//! - **Raw**: the whole payload is the datum; the writer schema is fixed
//!   by configuration.

use spate_core::error::DeserError;

/// Confluent framing: magic `0x00` + `u32` big-endian schema id + datum.
///
/// Returns the schema id and the datum slice.
pub fn parse_confluent(payload: &[u8]) -> Result<(u32, &[u8]), DeserError> {
    if let Some(&magic) = payload.first()
        && magic != 0x00
    {
        return Err(DeserError::Malformed {
            reason: format!("not Confluent wire format: magic byte {magic:#04x}, expected 0x00"),
        });
    }
    if payload.len() < 5 {
        return Err(DeserError::Malformed {
            reason: format!(
                "truncated Confluent header: {} bytes, need at least 5",
                payload.len()
            ),
        });
    }
    let schema_id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
    Ok((schema_id, &payload[5..]))
}

/// Single-object framing: magic `0xC3 0x01` + `u64` little-endian Rabin
/// fingerprint + datum.
///
/// Returns the fingerprint and the datum slice.
pub fn parse_single_object(payload: &[u8]) -> Result<(u64, &[u8]), DeserError> {
    if payload.len() < 10 {
        return Err(DeserError::Malformed {
            reason: format!(
                "truncated single-object header: {} bytes, need at least 10",
                payload.len()
            ),
        });
    }
    if payload[0] != 0xC3 || payload[1] != 0x01 {
        return Err(DeserError::Malformed {
            reason: format!(
                "not Avro single-object encoding: magic {:#04x} {:#04x}, expected 0xc3 0x01",
                payload[0], payload[1]
            ),
        });
    }
    let fingerprint = u64::from_le_bytes(payload[2..10].try_into().expect("length checked"));
    Ok((fingerprint, &payload[10..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confluent_parses_id_and_datum() {
        let payload = [0x00, 0x00, 0x00, 0x01, 0x2A, 0xDE, 0xAD];
        let (id, datum) = parse_confluent(&payload).unwrap();
        assert_eq!(id, 298);
        assert_eq!(datum, &[0xDE, 0xAD]);
    }

    #[test]
    fn confluent_empty_datum_is_valid() {
        let payload = [0x00, 0, 0, 0, 7];
        let (id, datum) = parse_confluent(&payload).unwrap();
        assert_eq!(id, 7);
        assert!(datum.is_empty());
    }

    #[test]
    fn confluent_rejects_wrong_magic() {
        let err = parse_confluent(&[0x01, 0, 0, 0, 1, 2]).unwrap_err();
        assert!(err.to_string().contains("magic byte 0x01"), "{err}");
    }

    #[test]
    fn confluent_rejects_truncation() {
        for len in 0..5 {
            let payload = vec![0x00; len];
            let err = parse_confluent(&payload).unwrap_err();
            assert!(err.to_string().contains("truncated"), "{err}");
        }
    }

    #[test]
    fn single_object_parses_fingerprint_and_datum() {
        let mut payload = vec![0xC3, 0x01];
        payload.extend_from_slice(&0x0123_4567_89AB_CDEFu64.to_le_bytes());
        payload.extend_from_slice(b"datum");
        let (fp, datum) = parse_single_object(&payload).unwrap();
        assert_eq!(fp, 0x0123_4567_89AB_CDEF);
        assert_eq!(datum, b"datum");
    }

    #[test]
    fn single_object_rejects_wrong_magic_and_truncation() {
        assert!(parse_single_object(&[0xC3, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(parse_single_object(&[0x00; 9]).is_err());
        assert!(parse_single_object(&[]).is_err());
    }
}
