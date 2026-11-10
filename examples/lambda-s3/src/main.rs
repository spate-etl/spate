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

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{Error, LambdaEvent};
use metrics_cloudwatch_embedded::lambda::handler;
use serde::{Deserialize, Serialize};
use spate::clickhouse::{
    self, ClickHouseRow, ClickHouseSinkConfig, DateTime64Millis, NativeEncoder, ShardKey,
};
use spate::json::{JsonDeserializerBuilder, NdjsonFramer};
use spate::prelude::*;
use spate::s3::S3Source;
use spate_example_lambda_s3::{BoxError, CONFIG, LOG_FILTER, MAX_RECORD_BYTES, handle_batch};

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
fn assemble() -> Result<(Pipeline, S3Source), BoxError> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(MAX_RECORD_BYTES)));
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
            .sink(
                encoder.clone(),
                router.clone(),
                chunk,
                ctx.queues,
                ctx.budget,
            )
            .build()
    });
    Ok((pipeline, source))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Before the first pipeline, so the handler's own lines are logged too.
    spate::telemetry::init(LogFormat::Json, LOG_FILTER);
    let drain_timeout = PipelineConfig::from_str(CONFIG)?.checkpoint.drain_timeout;
    // ANCHOR: metrics
    let metrics = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("Spate")
        .with_dimension(
            "function",
            std::env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or_default(),
        )
        .init()?;
    handler::run(metrics, |event: LambdaEvent<SqsEvent>| {
        handle_batch(
            event.payload,
            event.context.deadline(),
            drain_timeout,
            assemble,
        )
    })
    .await
    // ANCHOR_END: metrics
}
