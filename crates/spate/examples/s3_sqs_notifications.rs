//! A continuous S3 job whose work is the bucket's storage notifications: an
//! S3 bucket sends an `ObjectCreated` notification to an SQS queue for each
//! object written, the pipeline file names the queue in its `feed:` section,
//! and the S3 source reads each object a notification announces.
//!
//! The `feed:` section names no `location`, so the job never ends on its own.
//! It reads notifications as they arrive until `SIGTERM` or Ctrl-C, then
//! drains and prints the rows it wrote.
//!
//! On AWS the bucket's notification configuration sends `s3:ObjectCreated:*`
//! to the queue. No local store sends notifications to SQS, so `main()` stands
//! in for it: it writes `OBJECTS` objects under the source's prefix and
//! publishes the notification S3 would send for each. Against AWS, drop that
//! block and configure the bucket instead.
//!
//! What a continuous job does not do:
//!
//! - It reads no object written before the bucket's notification
//!   configuration took effect, and never lists the prefix to find one
//!   (ADR-0072).
//! - An object both a backfill listing and a notification announce is read
//!   twice.
//! - An overwrite of a key past the feed's re-enqueue cap is dropped and
//!   counted in `spate_s3_source_overwrites_dropped_total`.
//! - An S3 message inside an SNS envelope is failed back to the queue.
//!
//! # Run it
//!
//! A standard queue with a dead-letter queue, an S3 bucket, and AWS
//! credentials and a region in the environment. Against a local ElasticMQ and
//! RustFS, where `AWS_ENDPOINT_URL` reaches the store and
//! `AWS_ENDPOINT_URL_SQS` the queue:
//!
//! ```sh
//! docker run --rm -p 9324:9324 "$(cargo xtask container-image --pull elasticmq)"
//! docker run --rm -p 9000:9000 -e RUSTFS_ACCESS_KEY=spate-test \
//!   -e RUSTFS_SECRET_KEY=spate-test-secret \
//!   "$(cargo xtask container-image --pull s3 rustfs)"
//! export AWS_ACCESS_KEY_ID=spate-test AWS_SECRET_ACCESS_KEY=spate-test-secret AWS_REGION=us-east-1
//! export AWS_ENDPOINT_URL=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true
//! export AWS_ENDPOINT_URL_SQS=http://127.0.0.1:9324
//! aws s3 mb s3://events
//! aws sqs create-queue --queue-name events-dlq --attributes MessageRetentionPeriod=1209600
//! aws sqs create-queue --queue-name events --attributes \
//!   '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:elasticmq:000000000000:events-dlq\",\"maxReceiveCount\":\"5\"}"}'
//! S3_BUCKET=events SQS_QUEUE_URL=http://127.0.0.1:9324/000000000000/events \
//!   cargo run -p spate --features s3,json,sqs --example s3_sqs_notifications
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  run a continuous object-storage job on a bucket's storage notifications
// INDEX-TECH:  object storage and Amazon SQS
// INDEX-NEEDS: an S3 bucket, an SQS queue and a dead-letter queue, and AWS credentials

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use spate::feed::{Feed, Unit};
use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::S3Source;
use spate::sqs::SqsFeed;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::sync::Arc;

const OBJECTS: usize = 12;
const RECORDS_PER_OBJECT: usize = 20;
const PREFIX: &str = "events";

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

fn config_yaml() -> String {
    format!(
        r#"
pipeline: {{ name: s3-sqs-notifications-demo, threads: 2 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 200ms }}
source:
  s3:
    url: "s3://${{S3_BUCKET}}/{PREFIX}/"
sink: {{ capture: {{}} }}
{FEED}"#
    )
}

/// Writes `objects` NDJSON objects under the prefix and returns, for each, the
/// `Records[]` notification S3 sends for an `ObjectCreated:Put`.
async fn write_objects(
    store: &dyn ObjectStore,
    bucket: &str,
    objects: usize,
) -> Result<Vec<Unit>, Box<dyn std::error::Error>> {
    let mut units = Vec::with_capacity(objects);
    for o in 0..objects {
        let key = format!("{PREFIX}/obj-{o:02}.ndjson");
        let body: String = (0..RECORDS_PER_OBJECT)
            .map(|r| format!("{{\"k\":\"obj-{o:02}-{r}\"}}\n"))
            .collect();
        let path = Path::from(key.as_str());
        store.put(&path, body.into_bytes().into()).await?;
        let meta = store.head(&path).await?;
        // S3 sends the ETag unquoted; the source quotes it in `If-Match`.
        let etag = meta.e_tag.as_deref().unwrap_or_default().trim_matches('"');
        let record = serde_json::json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventTime": meta.last_modified.to_rfc3339(),
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": { "name": bucket },
                "object": { "key": key, "size": meta.size, "eTag": etag },
            },
        });
        units.push(Unit::new(
            key,
            serde_json::to_vec(&serde_json::json!({ "Records": [record] }))?,
        ));
    }
    Ok(units)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&config_yaml())?)?;
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

    // The stand-in for the bucket's notification configuration.
    let bucket = std::env::var("S3_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .build()?;
    for unit in pipeline.block_on(write_objects(&store, &bucket, OBJECTS))? {
        pipeline.block_on(feed.publish(unit))?;
    }
    println!("published {OBJECTS} notifications");

    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));
    let (sink, script) = capture_sink(1, 1);
    // A job with no `location` ends only at a shutdown: `run` returns after
    // `SIGTERM` or Ctrl-C and the drain that follows.
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
        .run(source)?;

    let rows: usize = script
        .writes()
        .iter()
        .map(|w| decode_rows(&w.payload).len())
        .sum();
    println!("pipeline exit: {:?}; rows written: {rows}", report.state);
    Ok(())
}
