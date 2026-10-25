//! A bounded S3 backfill whose work arrives through an SQS queue: the
//! pipeline file names the queue and a job location in its `feed:` section,
//! `main()` builds the feed from that section, a lister run publishes one unit
//! per object, and the S3 source reads each unit the queue hands it.
//!
//! The objects are a local directory (`file://`), so the only thing to stand
//! up is the queue. A unit is deleted from the queue only once every record
//! in its object is written, and a unit whose worker dies returns to the
//! queue after the visibility period, for this process or another to read.
//! The lister writes the job's `fingerprint` and, once every unit is
//! published, its `end` marker at the location. The job ends on its own about
//! a minute after its last unit, once the queue has read empty for the `1m`
//! settling window.
//!
//! # Run it
//!
//! A standard queue with a dead-letter queue that keeps messages longer than
//! the queue, a job location, and AWS credentials and a region in the
//! environment. Each job takes a queue pair and a location of its own. Against
//! a local ElasticMQ:
//!
//! ```sh
//! docker run --rm -p 9324:9324 "$(cargo xtask container-image --pull elasticmq)"
//! export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=eu-west-2
//! export AWS_ENDPOINT_URL=http://127.0.0.1:9324
//! aws sqs create-queue --queue-name jobs-dlq --attributes MessageRetentionPeriod=1209600
//! aws sqs create-queue --queue-name jobs --attributes \
//!   '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:elasticmq:000000000000:jobs-dlq\",\"maxReceiveCount\":\"5\"}"}'
//! JOB_LOCATION="file://$(mktemp -d)/" \
//!   SQS_QUEUE_URL=http://127.0.0.1:9324/000000000000/jobs \
//!   cargo run -p spate --features s3,json,sqs --example s3_sqs_backfill
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  run an object-storage backfill as a bounded job whose work arrives through a queue
// INDEX-TECH:  object storage and Amazon SQS
// INDEX-NEEDS: an SQS queue and a dead-letter queue with a longer retention, and AWS credentials

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::feed::Feed;
use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::S3Source;
use spate::sqs::SqsFeed;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::path::Path;
use std::sync::Arc;

const OBJECTS: usize = 12;
const RECORDS_PER_OBJECT: usize = 20;

/// The `feed:` section, kept out of `format!` so the page that transcludes it
/// shows the text a pipeline file holds.
const FEED: &str = r#"
# ANCHOR: feed
feed:
  sqs:
    queue_url: "${SQS_QUEUE_URL}"
    visibility: 60s
  units_held: 8
  location: "${JOB_LOCATION}"
  settling_window: 1m
# ANCHOR_END: feed
"#;

fn config_yaml(data: &Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-sqs-demo, threads: 2 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 200ms }}
source:
  s3:
    url: "file://{data}/"
sink: {{ capture: {{}} }}
{FEED}"#,
        data = data.display(),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let root = tempfile::tempdir()?;
    let data = root.path().join("data");
    std::fs::create_dir_all(&data)?;
    for o in 0..OBJECTS {
        let body: String = (0..RECORDS_PER_OBJECT)
            .map(|r| format!("{{\"k\":\"obj-{o:02}-{r}\"}}\n"))
            .collect();
        std::fs::write(data.join(format!("obj-{o:02}.ndjson")), body)?;
    }

    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&config_yaml(&data))?)?;
    // ANCHOR: connect
    let section = pipeline
        .config()
        .feed
        .clone()
        .ok_or("the file names no feed")?;
    let feed = match section.backend().type_tag() {
        "sqs" => Arc::new(SqsFeed::connect(&section, &pipeline.io_handle())?),
        other => return Err(format!("this binary builds the `sqs` feed, not `{other}`").into()),
    };
    let pipeline = pipeline.feed(Arc::clone(&feed) as Arc<dyn Feed>);
    // ANCHOR_END: connect

    // A lister run blocks its thread and refuses a Tokio runtime context, so
    // it runs here on `main`'s thread, before the worker starts.
    S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .list_once(&section, Arc::clone(&feed) as Arc<dyn Feed>)?;
    println!("listed {OBJECTS} objects");

    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));
    let (sink, script) = capture_sink(1, 1);
    let report = pipeline
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        // The bounded job completes the pipeline itself: `run` returns once
        // the end marker is read and the queue has stayed empty for the
        // settling window.
        .run(source)?;

    let rows: usize = script
        .writes()
        .iter()
        .map(|w| decode_rows(&w.payload).len())
        .sum();
    println!("pipeline exit: {:?}; rows written: {rows}", report.state);
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(rows, OBJECTS * RECORDS_PER_OBJECT);
    Ok(())
}
