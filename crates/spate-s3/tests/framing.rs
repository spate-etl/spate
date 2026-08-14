//! The record-framing seam: the source's framing contract, and a custom
//! [`RecordFramer`] supplied via [`S3Source::with_framer`], driven end-to-end.

mod support;

use spate_core::config::ComponentConfig;
use spate_core::framing::{FramingContract, RecordFramer};
use spate_core::pipeline::ExitState;
use spate_core::source::Source;
use spate_s3::S3Source;
use std::collections::VecDeque;
use std::io;
use std::time::Duration;
use support::{captured_rows, launch_customized, sorted, test_options};

/// A trivial custom framer that splits records on `;` instead of `\n`, a
/// non-newline layout, to show the framer is chosen by the caller and the
/// source itself is format-agnostic.
#[derive(Default)]
struct SemicolonSplitter {
    partial: Vec<u8>,
    ready: VecDeque<Vec<u8>>,
    decoded: u64,
}

impl RecordFramer for SemicolonSplitter {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.decoded += bytes.len() as u64;
        for &b in bytes {
            if b == b';' {
                self.ready.push_back(std::mem::take(&mut self.partial));
            } else {
                self.partial.push(b);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.partial.is_empty() {
            self.ready.push_back(std::mem::take(&mut self.partial));
        }
        Ok(())
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }

    fn decoded_bytes(&self) -> u64 {
        self.decoded
    }
}

fn s3_section(url: &str) -> ComponentConfig {
    let yaml = format!("s3:\n  url: \"{url}\"\n");
    let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
    ComponentConfig::new("s3", value["s3"].clone())
}

#[test]
fn a_framed_source_reports_per_record() {
    // framing_contract() is pure; a current-thread handle is enough.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let section = s3_section("file:///tmp/spate-s3-framing/data/");

    // The source is format-agnostic; the caller supplies the framer. A framed
    // source always emits one record per payload.
    let source = S3Source::from_component_config(&section, rt.handle().clone())
        .unwrap()
        .with_framer(|| Box::new(SemicolonSplitter::default()));
    assert_eq!(source.framing_contract(), FramingContract::PerRecord);
}

#[test]
fn a_source_without_a_framer_fails_to_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();

    let yaml = format!(
        r#"
pipeline: {{ name: s3-no-framer-test, threads: 1 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
sink: {{ capture: {{}} }}
"#,
        data = dir.path().join("data").display(),
    );

    // No framer supplied (identity `make_source`): the source must refuse to
    // open rather than silently framing nothing.
    let launched = launch_customized(&yaml, test_options(), |_| {}, |s, _io| s);
    let report = launched
        .run
        .wait_exit(Duration::from_secs(30))
        .expect("pipeline exits")
        .expect("no start error");
    let ExitState::Failed(failure) = report.state else {
        panic!("a source with no framer must fail, got {:?}", report.state);
    };
    assert!(
        failure.reason.contains("framer"),
        "actionable missing-framer error: {}",
        failure.reason
    );
}

#[test]
fn custom_framer_drives_a_non_ndjson_layout_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    // A single object with `;`-separated records, not newline-delimited.
    std::fs::write(dir.path().join("data/records.txt"), b"alpha;beta;gamma").unwrap();

    let yaml = format!(
        r#"
pipeline: {{ name: s3-framing-test, threads: 1 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
sink: {{ capture: {{}} }}
"#,
        data = dir.path().join("data").display(),
    );

    let launched = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        |source, _io| source.with_framer(|| Box::new(SemicolonSplitter::default())),
    );
    let report = launched
        .run
        .wait_exit(Duration::from_secs(30))
        .expect("bounded job exits on its own")
        .expect("no start error");
    assert_eq!(report.state, ExitState::Completed);

    assert_eq!(
        sorted(captured_rows(&launched.script)),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
        "the custom `;` framer split the object into three records"
    );
}
