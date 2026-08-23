//! Coordination-store record and key parsing over arbitrary bytes.
//!
//! A worker reads its plan record, its split records and their spec records
//! out of a shared key-value store, and acts on every one that parses: they
//! carry the fencing epoch, the owner, and the watermark a resumed split
//! reads from. Any process that reaches the prefix can write those bytes.
//!
//! The target builds each record from arbitrary fields, asserts a record this
//! build wrote parses back, then writes the fuzzer's bytes over it at a
//! fuzzer-chosen offset and runs every parser over the result. Each parser
//! also sees the other records' bytes and the fuzzer's bytes on their own, so
//! a record of one shape reaches the parser for another.
//!
//! A record that parses has to hold both pins, and the target asserts them:
//! the same bytes are rejected at any other key, because the key's id is
//! pinned, and rejected under any other job fingerprint. A record that parses
//! is also a fixed point of parse and re-encode, so a peer reads back what
//! this build wrote.
//!
//! The payload arms drive the base64 that carries a descriptor and a resume
//! state, and the split id a spec record claims.
//!
//! The key arm asserts the five prefixed keyspaces stay disjoint. At most one
//! of them reads a name out of an arbitrary key, and a key built for a split
//! id or an instance id is read back by exactly one of them, carrying the
//! name it was built from. A key two keyspaces claim would put a lease and a
//! progress record, or an assignment and a probe, on one entry.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_coordination::fuzz_seams::{
    committed_progress, encode_plan_record, encode_progress_record, encode_spec_record,
    instance_keys, parse_keys, parse_plan_record, parse_progress_record, parse_spec_record,
    spec_payload, split_keys,
};

#[derive(Arbitrary, Debug)]
struct Input {
    /// The fields the three records are built from.
    fields: Fields,
    /// Bytes written over a built record.
    splice: Vec<u8>,
    /// Offset into the record the splice lands at, taken modulo its length
    /// plus one.
    splice_at: u16,
    /// A split id or an instance id, for the key arm.
    name: String,
}

#[derive(Arbitrary, Debug)]
struct Fields {
    id: String,
    fp: u64,
    fingerprint: String,
    generation: u64,
    weight: u64,
    descriptor: Vec<u8>,
    epoch: u64,
    owner: Option<String>,
    attempts: u32,
    /// The committed watermark, resume state and terminal flag.
    progress: Option<(i64, Vec<u8>, bool)>,
    is_final: bool,
    planned: u64,
    planner_state: Option<Vec<u8>>,
}

/// `record` with `splice` written over it at `at`, which is `record` itself
/// when the splice is empty.
fn spliced(record: &[u8], splice: &[u8], at: u16) -> Vec<u8> {
    let at = usize::from(at) % (record.len() + 1);
    let mut out = Vec::with_capacity(record.len() + splice.len());
    out.extend_from_slice(&record[..at]);
    out.extend_from_slice(splice);
    out.extend_from_slice(&record[(at + splice.len()).min(record.len())..]);
    out
}

/// How many of the five keyspaces read a name out of `key`.
fn claimants(key: &str) -> usize {
    parse_keys(key).iter().filter(|read| read.is_some()).count()
}

/// Parse `bytes` as the spec record at `key`, and hold a record that parses
/// to the fixed point and both pins.
fn spec_arm(key: &str, bytes: &[u8], fp: u64) {
    let Some(encoded) = parse_spec_record(key, bytes, fp) else {
        return;
    };
    assert_eq!(
        parse_spec_record(key, &encoded, fp).as_ref(),
        Some(&encoded),
        "the spec record at {key:?} changed across parse and encode"
    );
    assert!(
        parse_spec_record(&format!("{key}0"), &encoded, fp).is_none(),
        "the spec record at {key:?} parsed at another key"
    );
    assert!(
        parse_spec_record(key, &encoded, fp ^ 1).is_none(),
        "the spec record at {key:?} parsed under another job fingerprint"
    );
    if let Some((id, _)) = spec_payload(key, bytes, fp) {
        assert_eq!(
            Some(id.as_str()),
            key.strip_prefix("spec."),
            "the spec record at {key:?} carries the split id {id:?}"
        );
    }
}

/// Parse `bytes` as the progress record at `key`, and hold a record that
/// parses to the fixed point and both pins.
fn progress_arm(key: &str, bytes: &[u8], fp: u64) {
    let Some(encoded) = parse_progress_record(key, bytes, fp) else {
        return;
    };
    assert_eq!(
        parse_progress_record(key, &encoded, fp).as_ref(),
        Some(&encoded),
        "the split record at {key:?} changed across parse and encode"
    );
    assert!(
        parse_progress_record(&format!("{key}0"), &encoded, fp).is_none(),
        "the split record at {key:?} parsed at another key"
    );
    assert!(
        parse_progress_record(key, &encoded, fp ^ 1).is_none(),
        "the split record at {key:?} parsed under another job fingerprint"
    );
    let _ = committed_progress(key, bytes, fp);
}

/// Parse `bytes` as the plan record, and hold a record that parses to the
/// fixed point and the fingerprint pin.
fn plan_arm(bytes: &[u8], fingerprint: &str) {
    let Some(encoded) = parse_plan_record(bytes, fingerprint) else {
        return;
    };
    assert_eq!(
        parse_plan_record(&encoded, fingerprint).as_ref(),
        Some(&encoded),
        "the plan record changed across parse and encode"
    );
    assert!(
        parse_plan_record(&encoded, &format!("{fingerprint}0")).is_none(),
        "the plan record parsed under another job fingerprint"
    );
}

fuzz_target!(|input: Input| {
    let Input {
        fields,
        splice,
        splice_at,
        name,
    } = input;
    let fp = fields.fp;

    let progress = fields
        .progress
        .as_ref()
        .map(|(watermark, state, completed)| (*watermark, state.as_slice(), *completed));
    let spec = encode_spec_record(
        &fields.id,
        fp,
        fields.generation,
        fields.weight,
        &fields.descriptor,
    );
    let split = encode_progress_record(
        &fields.id,
        fp,
        fields.epoch,
        fields.owner.as_deref(),
        fields.attempts,
        progress,
    );
    let plan = encode_plan_record(
        &fields.fingerprint,
        fields.generation,
        fields.is_final,
        fields.planned,
        fields.planner_state.as_deref(),
    );

    let spec_key = format!("spec.{}", fields.id);
    let split_key = format!("split.{}", fields.id);
    assert!(
        parse_spec_record(&spec_key, &spec, fp).is_some(),
        "a spec record this build wrote does not parse at {spec_key:?}"
    );
    assert!(
        parse_progress_record(&split_key, &split, fp).is_some(),
        "a split record this build wrote does not parse at {split_key:?}"
    );
    assert!(
        parse_plan_record(&plan, &fields.fingerprint).is_some(),
        "a plan record this build wrote does not parse"
    );

    // Every parser sees each record whole, each record with the fuzzer's
    // bytes written over it, and the fuzzer's bytes on their own.
    let candidates: Vec<Vec<u8>> = [&spec, &split, &plan]
        .into_iter()
        .map(|record| spliced(record, &splice, splice_at))
        .collect();
    for bytes in [
        spec.as_slice(),
        split.as_slice(),
        plan.as_slice(),
        splice.as_slice(),
    ]
    .into_iter()
    .chain(candidates.iter().map(Vec::as_slice))
    {
        spec_arm(&spec_key, bytes, fp);
        progress_arm(&split_key, bytes, fp);
        plan_arm(bytes, &fields.fingerprint);
    }

    let claimed = claimants(&name);
    assert!(
        claimed <= 1,
        "{claimed} keyspaces read a name out of {name:?}"
    );

    if let Some(keys) = split_keys(&name) {
        // `parse_keys` reads `[split, spec, worker, probe, assign]`, and
        // `split_keys` builds the first two of them.
        for (slot, built) in keys.iter().enumerate() {
            assert_eq!(
                parse_keys(built)[slot],
                Some(name.as_str()),
                "the key {built:?} does not read back as {name:?}"
            );
            assert_eq!(claimants(built), 1, "{built:?} is read by two keyspaces");
        }
    }

    if let Some(keys) = instance_keys(&name) {
        // `instance_keys` builds the last three of the five.
        for (slot, built) in keys.iter().enumerate() {
            assert_eq!(
                parse_keys(built)[slot + 2],
                Some(name.as_str()),
                "the key {built:?} does not read back as {name:?}"
            );
            assert_eq!(claimants(built), 1, "{built:?} is read by two keyspaces");
        }
    }
});
