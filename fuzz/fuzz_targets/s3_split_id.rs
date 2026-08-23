//! Split identity over adversarially named object keys.
//!
//! A split id is persisted identity: two different member sets that mint the
//! same id drop one set's work silently. The target mints ids for two member
//! sets built from arbitrary keys and ETags and asserts that the two ids are
//! equal exactly when the two member sets are equal once sorted by key. That
//! covers both halves at once: a preimage two distinct member sets share, and
//! a digest that moves when only the input order does.
//!
//! Every id is also checked against its published shape, 25 characters over
//! `[A-Za-z0-9_-]` behind the `s3-` prefix, whatever the keys contain.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::split_id_for;
use std::collections::BTreeSet;

#[derive(Arbitrary, Debug)]
struct Input {
    left: Vec<Member>,
    right: Vec<Member>,
}

#[derive(Arbitrary, Debug)]
struct Member {
    key: String,
    etag: Option<String>,
}

/// The members in the order given, keeping a repeated key only at its first
/// occurrence. `split_id_for` sorts its members by key with an unstable sort,
/// so two members sharing a key have an unspecified order in the digest
/// preimage; the target feeds it one member per key.
fn deduped(members: &[Member]) -> Vec<(&str, Option<&str>)> {
    let mut keys = BTreeSet::new();
    let mut out = Vec::with_capacity(members.len());
    for member in members {
        if keys.insert(member.key.as_str()) {
            out.push((member.key.as_str(), member.etag.as_deref()));
        }
    }
    out
}

/// The same members sorted by key: the form `split_id_for` digests.
fn canonical<'a>(members: &[(&'a str, Option<&'a str>)]) -> Vec<(&'a str, Option<&'a str>)> {
    let mut sorted = members.to_vec();
    sorted.sort_by_key(|(key, _)| *key);
    sorted
}

fuzz_target!(|input: Input| {
    let left = deduped(&input.left);
    let right = deduped(&input.right);
    if left.is_empty() || right.is_empty() {
        assert!(
            split_id_for(std::iter::empty()).is_err(),
            "an empty member set minted an id"
        );
        return;
    }

    let left_id = split_id_for(left.iter().copied()).expect("non-empty member set");
    let right_id = split_id_for(right.iter().copied()).expect("non-empty member set");

    for id in [&left_id, &right_id] {
        let id = id.as_str();
        assert_eq!(id.len(), 25, "id `{id}` is not 25 characters");
        assert!(id.starts_with("s3-"), "id `{id}` lacks the s3- prefix");
        assert!(
            id.bytes()
                .skip(3)
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "id `{id}` carries a character outside [A-Za-z0-9_-]"
        );
    }

    assert_eq!(
        left_id == right_id,
        canonical(&left) == canonical(&right),
        "ids {left_id:?} and {right_id:?} disagree with the member sets {left:?} and {right:?}"
    );
});
