//! `objects_failed_total{reason}` on the feed read path. Its own test binary
//! because it installs the process-wide metrics recorder.

mod support;

use spate_core::feed::Feed as _;
use spate_core::feed::memory::MemoryFeed;
use spate_core::metrics::{Exporter, MetricsHandle, MetricsSettings, install};
use spate_test::{metric_value, scripted_feed, wait_until};
use std::fs;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use support::spy::{SpyOptions, spying_local_store};
use support::{
    Launched, PipelineYaml, launch_customized, launch_on_notifications, line_framer, notification,
    stop, test_options, unit_over,
};

const DEADLINE: Duration = Duration::from_secs(30);

/// Each reason the S3 page documents for the counter, in the order the
/// tests' expected arrays list them.
const REASONS: [&str; 4] = [
    "not_found",
    "etag_drift",
    "undecodable",
    "retries_exhausted",
];

fn install_exporter() -> MetricsHandle {
    install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter")
}

/// The run's `objects_failed_total` series, one per reason in [`REASONS`].
fn objects_failed(handle: &MetricsHandle, l: &Launched) -> [Option<f64>; 4] {
    let rendered = handle.render();
    REASONS.map(|reason| {
        metric_value(
            &rendered,
            "spate_s3_source_objects_failed_total",
            &[("pipeline", &l.pipeline), ("reason", reason)],
        )
    })
}

/// Pins that a descriptor unit over a deleted key counts one `not_found`
/// per delivery until the feed dead-letters it, and nothing under any other
/// reason.
#[test]
fn a_missing_object_counts_not_found_once_per_delivery() {
    const CAP: u32 = 3;
    let handle = install_exporter();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();

    let feed = MemoryFeed::with_attempt_cap(NonZeroU32::new(CAP).unwrap());
    drop(feed.publish(unit_over(&data, "missing.ndjson")));
    let (spy_store, spy) = spying_local_store(SpyOptions::default());
    let yaml = PipelineYaml::file("s3-objects-failed-descriptor", &data)
        .fed_by_code()
        .build();
    let source_feed = Arc::new(feed.clone());
    let l = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _| {
            line_framer(source)
                .with_feed(source_feed)
                .with_store(spy_store)
        },
    );
    wait_until(DEADLINE, "the unit is dead-lettered", || {
        feed.counts().dead_lettered == 1
    });
    stop(&l);

    assert_eq!(spy.gets().len(), CAP as usize, "one GET per delivery");
    assert_eq!(
        objects_failed(&handle, &l),
        [Some(f64::from(CAP)), Some(0.0), Some(0.0), Some(0.0)],
        "{}",
        handle.render()
    );
}

/// Pins that a storage notification naming an object that does not exist
/// counts one `not_found` for its one delivery, and nothing under any other
/// reason.
#[test]
fn a_notification_naming_a_missing_object_counts_not_found() {
    let handle = install_exporter();
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("data")).unwrap();

    let (feed, script) = scripted_feed();
    script.deliver(notification(&[("data/missing.ndjson", 10)]));
    let l = launch_on_notifications("s3", dir.path(), Arc::new(feed), |_| {});
    wait_until(DEADLINE, "the unit is failed", || {
        !script.failed().is_empty()
    });
    stop(&l);

    assert_eq!(script.failed().len(), 1);
    assert_eq!(
        objects_failed(&handle, &l),
        [Some(1.0), Some(0.0), Some(0.0), Some(0.0)],
        "{}",
        handle.render()
    );
}
