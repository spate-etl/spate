//! `objects_listed_total` over a listing with keys `object_store` cannot
//! address. Its own test binary because it installs the process-wide metrics
//! recorder.

#![expect(deprecated, reason = "v0.4.0 removes this with spate-coordination")]

mod support;

use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_test::metric_value;
use std::time::Duration;
use support::s3_stub::s3_stub;
use support::{PipelineYaml, launch_tuned, shared_store, test_options, test_tuning};

/// Every object the store lists counts as listed, including those the S3
/// client takes out of the listing, those the planner leaves out of the plan,
/// and those whose key decodes to the same key as another's.
#[test]
fn unaddressable_objects_count_as_listed() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let endpoint = s3_stub(&[
        ("data/a.ndjson", b"{\"k\":\"a\"}\n"),
        ("data/d//dbl.ndjson", b"{\"k\":\"dbl\"}\n"),
        ("data/dir/", b""),
        ("data/z.ndjson", b"{\"k\":\"z\"}\n"),
        ("data/c\nr.ndjson", b"{\"k\":\"lf\"}\n"),
        ("data/c\rr.ndjson", b"{\"k\":\"cr\"}\n"),
    ]);
    let yaml = PipelineYaml::endpoint("objects-listed", &endpoint).build();
    let mut tuning = test_tuning();
    tuning.max_attempts = 2;
    let l = launch_tuned(&yaml, test_options(), &shared_store(), tuning, |_| {});
    l.run
        .wait_exit(Duration::from_secs(60))
        .expect("the job reaches a verdict")
        .expect("no start error");
    let rendered = handle.render();
    let listed = metric_value(&rendered, "spate_s3_source_objects_listed_total", &[])
        .unwrap_or_else(|| panic!("no objects_listed_total series:\n{rendered}"));
    assert_eq!(listed, 6.0, "{rendered}");
}
