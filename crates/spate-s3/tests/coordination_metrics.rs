//! Metrics on a coordinator built from the `coordination:` section. Its own
//! test binary because it installs the process-wide metrics recorder.

mod support;

use spate_core::metrics::{Exporter, MetricsSettings, install};
use std::fs;
use std::time::Duration;
use support::{launch_customized, line_framer, lines_bytes, recs, test_options};

/// The coordinator the S3 source builds from the section registers the
/// `spate_coordination_*` families under the source's labels.
#[test]
fn a_section_built_coordinator_publishes_coordination_metrics() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("obj.ndjson"), lines_bytes(&recs("o", 5))).unwrap();
    let yaml = format!(
        r#"
pipeline: {{ name: s3-coordination-metrics, threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
sink: {{ capture: {{}} }}
coordination:
  op_timeout: 100ms
  lease_duration: 2s
  replan_interval: 2s
  startup_max_attempts: 1
  store:
    nats: {{ servers: ["nats://127.0.0.1:1"], job: metrics }}
"#,
        data = data.display(),
    );

    let launched = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        |source, _| line_framer(source),
    );
    launched
        .run
        .wait_exit(Duration::from_secs(60))
        .expect("run exits")
        .expect("no start error");

    let rendered = handle.render();
    assert!(
        rendered
            .lines()
            .any(|l| l.starts_with("spate_coordination_")
                && l.contains("s3-coordination-metrics")
                && l.contains(r#"component_type="s3""#)),
        "{rendered}"
    );
}
