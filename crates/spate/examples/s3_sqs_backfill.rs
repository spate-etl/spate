//! An S3 backfill whose work arrives through an SQS queue: the pipeline file
//! names the queue in its `feed:` section, `main()` builds the feed from that
//! section, publishes one unit per object, and the S3 source reads each unit
//! the queue hands it.
//!
//! The objects are a local directory (`file://`), so the only thing to stand
//! up is the queue. A unit is deleted from the queue only once every record
//! in its object is written, and a unit whose worker dies returns to the
//! queue after the visibility period, for this process or another to read.
//! The feed path never ends on its own, so this demo stops itself once every
//! row is written.
//!
//! # Run it
//!
//! A standard queue with a dead-letter queue that keeps messages longer than
//! the queue, and AWS credentials and a region in the environment. Against a
//! local ElasticMQ:
//!
//! ```sh
//! docker run --rm -p 9324:9324 "$(cargo xtask container-image --pull elasticmq)"
//! export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=eu-west-2
//! export AWS_ENDPOINT_URL=http://127.0.0.1:9324
//! aws sqs create-queue --queue-name jobs-dlq --attributes MessageRetentionPeriod=1209600
//! aws sqs create-queue --queue-name jobs --attributes \
//!   '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:elasticmq:000000000000:jobs-dlq\",\"maxReceiveCount\":\"5\"}"}'
//! SQS_QUEUE_URL=http://127.0.0.1:9324/000000000000/jobs \
//!   cargo run -p spate --features s3,json,sqs --example s3_sqs_backfill
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  read an object-storage backfill's work from a queue
// INDEX-TECH:  object storage and Amazon SQS
// INDEX-NEEDS: an SQS queue and a dead-letter queue with a longer retention, and AWS credentials

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::feed::{Feed, Unit};
use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate::sqs::SqsFeed;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// The unit naming one staged object: a one-member split descriptor.
fn unit_over(data: &Path, name: &str) -> Result<Unit, Box<dyn std::error::Error>> {
    let path = data.join(name);
    let object = DescriptorObject {
        key: path.to_string_lossy().trim_start_matches('/').to_owned(),
        size: std::fs::metadata(&path)?.len(),
        etag: None,
        last_modified_ms: 1,
    };
    Ok(Unit::new(
        name,
        SplitDescriptor::new(vec![object]).encode()?,
    ))
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

    // A lister would publish these; here `main()` does.
    for o in 0..OBJECTS {
        let confirmation = feed.publish(unit_over(&data, &format!("obj-{o:02}.ndjson"))?);
        pipeline.block_on(confirmation)?;
    }
    println!("published {OBJECTS} units");

    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));
    let (sink, script) = capture_sink(1, 1);
    let runtime = pipeline
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
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let run = std::thread::spawn(move || runtime.run());

    let rows = || -> usize {
        script
            .writes()
            .iter()
            .map(|w| decode_rows(&w.payload).len())
            .sum()
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    while rows() < OBJECTS * RECORDS_PER_OBJECT && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
    }
    // A unit's delete follows its last commit; give the last ones a moment.
    std::thread::sleep(Duration::from_secs(2));
    shutdown.trigger();
    let report = run.join().map_err(|_| "the pipeline thread panicked")??;
    println!(
        "pipeline exit: {:?}; rows written: {}",
        report.state,
        rows()
    );
    Ok(())
}
