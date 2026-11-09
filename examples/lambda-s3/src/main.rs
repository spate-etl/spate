//! An AWS Lambda function that reads one SQS batch of S3 storage
//! notifications per invocation and writes the NDJSON records each announced
//! object holds to ClickHouse.
//!
//! The SQS event source mapping invokes it with up to `BatchSize` messages
//! and `ReportBatchItemFailures` set. Each message is one unit of a seeded
//! in-process feed, and the function returns every message whose unit was not
//! read in full as a batch item failure, for SQS to deliver again.
//!
//! The function stops the run before the invocation's deadline, early enough
//! for `checkpoint.drain_timeout` to pass, since a function with no extension
//! gets no shutdown phase. `examples/lambda-s3/README.md` covers the build
//! and the event source mapping.

use aws_lambda_events::event::sqs::{SqsBatchResponse, SqsEvent};
use lambda_runtime::{Error, LambdaEvent};
use metrics_cloudwatch_embedded::lambda::handler;
use serde::{Deserialize, Serialize};
use spate::clickhouse::{
    self, ClickHouseRow, ClickHouseSinkConfig, DateTime64Millis, NativeEncoder, ShardKey,
};
use spate::json::{JsonDeserializerBuilder, NdjsonFramer};
use spate::prelude::*;
use spate::s3::S3Source;
use spate_example_lambda_s3::{AssembleError, messages, run_batch, stop_at};
use std::time::{Duration, SystemTime};

/// The pipeline file. A function reads it once per invocation, so `${VAR}`
/// forms take the function's environment.
const CONFIG: &str = r#"
# ANCHOR: config
pipeline: { name: lambda-s3 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint:
  # The function stops the run this long, plus 3s, before its deadline.
  drain_timeout: 10s
feed: { memory: {} }
source:
  s3:
    url: "${SPATE_S3_URL}"
deserializer:
  json: {}
sink:
  clickhouse:
    table: ${CLICKHOUSE_TABLE:-page_views}
    format: native
    shards:
      - replicas: ["${CLICKHOUSE_URL}"]
    user: ${CLICKHOUSE_USER:-default}
    password: ${CLICKHOUSE_PASSWORD:-}
# ANCHOR_END: config
"#;

/// One NDJSON record.
#[derive(Debug, Deserialize)]
struct PageView {
    user_id: u64,
    path: String,
    at_ms: i64,
}

/// One ClickHouse row. Native maps columns by field declaration order.
#[derive(Debug, Serialize, ClickHouseRow)]
struct PageViewRow {
    user_id: u64,
    path: String,
    at: DateTime64Millis,
}

/// The pipeline and source for one batch.
fn assemble() -> Result<(Pipeline, S3Source), AssembleError> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));
    let deserializer = pipeline
        .config()
        .deserializer
        .clone()
        .ok_or("the pipeline file needs a `deserializer` section")?;
    let sink_config: ClickHouseSinkConfig = pipeline
        .config()
        .sink_config("default")?
        .deserialize_into()?;
    let sink = pipeline
        .block_on(clickhouse::config::build(sink_config)?.with_row::<Owned<PageViewRow>>())?;
    let router = sink.router::<Owned<PageViewRow>>(|row| ShardKey::U64(row.user_id));
    let encoder = NativeEncoder::<Owned<PageViewRow>>::new(sink.native_schema()?);
    let pipeline = pipeline.sink(sink)?.chains(move |ctx| {
        let chunk = ctx.chunk();
        let deser = JsonDeserializerBuilder::from_component(&deserializer)
            .and_then(|b| b.for_source_framing(ctx.source_framing))
            .expect("the deserializer section was validated with the pipeline file")
            .build_serde::<PageView>();
        chain_owned::<PageView, _>(deser)
            .map(|view: PageView| PageViewRow {
                user_id: view.user_id,
                path: view.path,
                at: DateTime64Millis(view.at_ms),
            })
            .sink(encoder.clone(), router.clone(), chunk, ctx.queues, ctx.budget)
            .build()
    });
    Ok((pipeline, source))
}

// ANCHOR: handler
async fn handle(
    event: LambdaEvent<SqsEvent>,
    drain_timeout: Duration,
) -> Result<SqsBatchResponse, Error> {
    let stop = stop_at(SystemTime::now(), event.context.deadline(), drain_timeout)?;
    let messages = messages(event.payload)?;
    let wait = stop
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    let failed = run_batch(messages, assemble, tokio::time::sleep(wait)).await;
    let mut response = SqsBatchResponse::default();
    for id in failed {
        response.add_failure(id);
    }
    Ok(response)
}
// ANCHOR_END: handler

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Before the first pipeline, so the handler's own lines are logged too.
    spate::telemetry::init(LogFormat::Json, "info");
    let drain_timeout = PipelineConfig::from_str(CONFIG)?.checkpoint.drain_timeout;
    // ANCHOR: metrics
    let metrics = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("Spate")
        .with_dimension(
            "function",
            std::env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or_default(),
        )
        .init()?;
    handler::run(metrics, |event| handle(event, drain_timeout)).await
    // ANCHOR_END: metrics
}
