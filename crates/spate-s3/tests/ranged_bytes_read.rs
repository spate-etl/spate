//! `bytes_read_total` for a ranged unit. Its own test binary because it
//! installs the process-wide metrics recorder.

mod support;

use spate_core::feed::Unit;
use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_test::{metric_value, scripted_feed, wait_until};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use support::spy::{RangeKind, SpyOptions, spying_local_store};
use support::{
    PipelineYaml, launch_customized, line_framer, line_starts, lines_bytes, ranged_spec, recs,
    stop, test_options,
};

/// A ranged unit's `bytes_read_total` equals the bytes of every window it
/// fetched from the store, trimmed bytes included.
#[test]
fn a_ranged_split_counts_every_fetched_byte_as_read() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let lines = recs("r", 20);
    fs::write(data.join("big.ndjson"), lines_bytes(&lines)).unwrap();
    let starts = line_starts(&lines);
    let body = ranged_spec(&data, "big.ndjson", starts[5], starts[10] + 3, b'\n');

    let (feed, script) = scripted_feed();
    script.deliver(Unit::new("ranged", body));
    let (spy_store, spy) = spying_local_store(SpyOptions::default());
    let yaml = PipelineYaml::file("s3-ranged-bytes-read", &data)
        .threads(1)
        .fed_by_code()
        .build();
    let l = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _| {
            line_framer(source)
                .with_feed(Arc::new(feed))
                .with_store(spy_store)
        },
    );
    wait_until(Duration::from_secs(30), "the unit is acknowledged", || {
        script.acknowledged().len() == 1
    });
    stop(&l);

    let fetched: u64 = spy
        .gets()
        .iter()
        .map(|g| match g.range {
            RangeKind::Bounded(start, end) => end - start,
            other => panic!("a ranged read issues bounded windows, got {other:?}"),
        })
        .sum();
    let owned: u64 = lines[5..=10].iter().map(|l| l.len() as u64 + 1).sum();
    assert!(fetched > owned, "the windows overlap the range's edges");

    let rendered = handle.render();
    let read = metric_value(&rendered, "spate_s3_source_bytes_read_total", &[])
        .unwrap_or_else(|| panic!("no bytes_read_total series:\n{rendered}"));
    assert_eq!(read, fetched as f64, "{rendered}");
}
