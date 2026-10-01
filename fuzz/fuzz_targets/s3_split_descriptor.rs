//! Split-descriptor decoding over arbitrary bytes, and the encode/decode
//! round trip.
//!
//! A descriptor is read back out of a coordination store, so the bytes handed
//! to `decode` are whatever that store held. The target drives three arms. The
//! decode arm asserts that a decoded descriptor carries `DESCRIPTOR_VERSION`
//! and re-encodes to bytes that decode back to an equal descriptor. The
//! round-trip arm asserts that a descriptor built from arbitrary member
//! objects survives `encode` followed by `decode` unchanged. The range arm
//! asserts that a ranged descriptor over one arbitrary object encodes exactly
//! when the object has an ETag and the range is non-empty and inside it, and
//! then survives the round trip.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::{DESCRIPTOR_VERSION, DescriptorObject, SplitDescriptor, SplitRange};

#[derive(Arbitrary, Debug)]
struct Input {
    encoded: Vec<u8>,
    objects: Vec<Object>,
    ranged: Object,
    range: (u64, u64, u8),
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

    let (start, end, delimiter) = input.range;
    let valid = input.ranged.etag.is_some() && start < end && end <= input.ranged.size;
    let ranged = SplitDescriptor::with_range(
        descriptor_object(input.ranged),
        SplitRange::new(start, end, delimiter),
    );
    match ranged.encode() {
        Ok(encoded) => {
            assert!(
                valid,
                "encode accepted an invalid ranged descriptor {ranged:?}"
            );
            assert_eq!(
                SplitDescriptor::decode(&encoded).expect("encoded bytes decode"),
                ranged,
                "the ranged descriptor changed across encode and decode"
            );
        }
        Err(e) => assert!(!valid, "encode refused a valid ranged descriptor: {e}"),
    }

    let objects: Vec<DescriptorObject> = input.objects.into_iter().map(descriptor_object).collect();
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

fn descriptor_object(o: Object) -> DescriptorObject {
    DescriptorObject {
        key: o.key,
        size: o.size,
        etag: o.etag,
        last_modified_ms: o.last_modified_ms,
    }
}
