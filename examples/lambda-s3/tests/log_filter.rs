//! The function's log filter over one batch, with the CloudWatch recorder
//! `main.rs` installs. The subscriber and the recorder belong to the process,
//! so this binary holds one test.

mod support;

use spate_example_lambda_s3::{LOG_FILTER, run_batch};
use spate_test::LogCapture;
use std::future::pending;
use support::*;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// The recorder's line for a histogram value it drops.
const DROPPED: &str = "more than 100 unflushed values";

/// Pins that the function's filter drops the recorder's line for each
/// histogram value past 100 and keeps the handler's own lines. The test
/// records 101 values itself, so the recorder drops one whatever the run's
/// poll count, and a layer at `info` beside the filter shows that it did.
#[tokio::test]
async fn the_log_filter_drops_the_recorders_lines_and_keeps_the_handlers() {
    let (function, control) = (LogCapture::new(), LogCapture::new());
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(function.clone())
                .with_filter(EnvFilter::new(LOG_FILTER)),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(control.clone())
                .with_filter(EnvFilter::new("info")),
        )
        .init();
    let _recorder = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("Spate")
        .init()
        .unwrap();
    #[expect(
        clippy::disallowed_macros,
        reason = "the test feeds the recorder directly, outside any pipeline"
    )]
    for value in 0..101 {
        metrics::histogram!("lambda_s3_test_seconds").record(f64::from(value));
    }

    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    let (sink, _script) = spate_test::capture_sink(1, 1);
    let batch = vec![message("a", descriptor(&data, "a.ndjson"))];
    let failed = bounded(run_batch(
        batch,
        assemble(yaml(&data, "{ memory: {} }"), sink),
        pending(),
    ))
    .await;
    assert_eq!(failed, Vec::<String>::new());

    assert!(
        control.lines().join("\n").contains(DROPPED),
        "the recorder dropped no value"
    );
    let kept = function.lines().join("\n");
    assert!(!kept.contains(DROPPED), "{kept}");
    assert!(kept.contains("the batch ended"), "{kept}");
}
