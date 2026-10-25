//! Bounded backfills over S3 listings that hold keys `object_store` cannot
//! address, run against a local S3 stub.

mod support;

use spate_core::pipeline::ExitState;
use std::time::Duration;
use support::s3_stub::s3_stub;
use support::{PipelineYaml, captured_rows, launch, sorted, test_options};

/// Every object beside a key with an empty segment is read, the key's own
/// split quarantines, and the bounded job ends stalled. Regression for #1082.
#[test]
fn a_listing_with_an_unaddressable_key_reads_every_other_object() {
    let endpoint = s3_stub(&[
        ("data/a.ndjson", b"{\"k\":\"a\"}\n"),
        ("data/d//dbl.ndjson", b"{\"k\":\"dbl\"}\n"),
        ("data/dir/", b""),
        ("data/z.ndjson", b"{\"k\":\"z\"}\n"),
    ]);
    let yaml = PipelineYaml::endpoint("unaddressable", &endpoint).build();
    let l = launch(&yaml, test_options());
    let report = l
        .run
        .wait_exit(Duration::from_secs(60))
        .expect("the job reaches a verdict")
        .expect("no start error");
    assert_eq!(
        sorted(captured_rows(&l.script)),
        ["{\"k\":\"a\"}", "{\"k\":\"z\"}"]
    );
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "the unreadable key must stall the job, got {:?}",
            report.state
        );
    };
    assert!(
        failure.reason.contains("quarantined") || failure.reason.contains("stalled"),
        "{}",
        failure.reason
    );
}

/// Two keys that decode alike, the first listed with no bytes and the second
/// holding a record: the bounded job does not complete without reading or
/// poisoning the second.
#[test]
fn an_empty_key_that_decodes_alike_does_not_hide_one_with_bytes() {
    let endpoint = s3_stub(&[
        ("data/c\nr.ndjson", b""),
        ("data/c\rr.ndjson", b"{\"k\":\"x\"}\n"),
    ]);
    let yaml = PipelineYaml::endpoint("decode-alike", &endpoint).build();
    let l = launch(&yaml, test_options());
    let report = l
        .run
        .wait_exit(Duration::from_secs(60))
        .expect("the job reaches a verdict")
        .expect("no start error");
    assert!(
        matches!(report.state, ExitState::Failed(_)),
        "{:?}",
        report.state
    );
}
