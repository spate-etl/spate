//! Split-descriptor decoding over arbitrary bytes, and the encode/decode
//! round trip.
//!
//! A descriptor is read back out of a coordination store, so the bytes handed
//! to `decode` are whatever that store held. The target drives two arms. The
//! decode arm asserts that a decoded descriptor carries `DESCRIPTOR_VERSION`
//! and re-encodes to bytes that decode back to an equal descriptor. The
//! round-trip arm asserts that a descriptor built from arbitrary member
//! objects survives `encode` followed by `decode` unchanged.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::{DESCRIPTOR_VERSION, DescriptorObject, SplitDescriptor};

#[derive(Arbitrary, Debug)]
struct Input {
    encoded: Vec<u8>,
    objects: Vec<Object>,
}

#[derive(Arbitrary, Debug)]
struct Object {
    key: String,
    size: u64,
    etag: Option<String>,
    last_modified_ms: i64,
}

fuzz_target!(|input: Input| {
    if let Ok(decoded) = SplitDescriptor::decode(&input.encoded) {
        assert_eq!(
            decoded.version(),
            DESCRIPTOR_VERSION,
            "decode accepted a descriptor written under another version"
        );
        let reencoded = decoded.encode().expect("a decoded descriptor re-encodes");
        assert_eq!(
            SplitDescriptor::decode(&reencoded).expect("re-encoded bytes decode"),
            decoded,
            "the descriptor changed across encode and decode"
        );
    }

    let objects: Vec<DescriptorObject> = input
        .objects
        .into_iter()
        .map(|o| DescriptorObject {
            key: o.key,
            size: o.size,
            etag: o.etag,
            last_modified_ms: o.last_modified_ms,
        })
        .collect();
    let descriptor = SplitDescriptor::new(objects);
    let encoded = descriptor
        .encode()
        .expect("a descriptor built by new carries the current version");
    assert_eq!(
        SplitDescriptor::decode(&encoded).expect("encoded bytes decode"),
        descriptor,
        "the descriptor changed across encode and decode"
    );
});
