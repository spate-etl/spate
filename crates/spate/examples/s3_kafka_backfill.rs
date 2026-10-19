//! An S3 backfill whose work arrives through Kafka topics: the pipeline file
//! names the topics in its `feed:` section, `main()` builds the feed from
//! that section, publishes one unit per object, and the S3 source reads each
//! unit the feed hands it.
//!
//! The objects are a local directory (`file://`), so the only thing to stand
//! up is a broker. A unit's offset is committed only once every record in
//! its object is written, and a unit whose worker dies is read again by the
//! group member its partition moves to. The feed path never ends on its own,
//! so this demo stops itself once every row is written.
//!
//! # Run it
//!
//! A broker at `KAFKA_BROKERS`, `localhost:9092` by default. The feed never
//! creates a topic, so `main()` creates the work, retry, dead-letter and
//! attempt topics first when they are missing, the work topic stamped with
//! `LogAppendTime`, the dead-letter topic keeping its messages for ever and
//! the attempt topic compacted. Against a local
//! broker:
//!
//! ```sh
//! docker run --rm -p 9092:9092 "$(cargo xtask container-image --pull kafka)"
//! cargo run -p spate --features s3,json,kafka --example s3_kafka_backfill
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  read an object-storage backfill's work from Kafka topics
// INDEX-TECH:  object storage and Kafka
// INDEX-NEEDS: Kafka

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use spate::feed::{Feed, Unit};
use spate::json::NdjsonFramer;
use spate::kafka::feed::KafkaFeed;
use spate::prelude::*;
use spate::s3::{DescriptorObject, S3Source, SplitDescriptor};
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
  kafka:
    brokers: "${KAFKA_BROKERS:-localhost:9092}"
    group_id: s3-kafka-demo
    topic: s3-kafka-demo
    retry_topic: s3-kafka-demo-retry
    dead_letter_topic: s3-kafka-demo-dead
    attempt_topic: s3-kafka-demo-attempts
  attempt_cap: 5
  units_held: 8
# ANCHOR_END: feed
"#;

fn config_yaml(data: &Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-kafka-demo, threads: 2 }}
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

/// Create the feed's four topics on `brokers`, leaving any that exist.
fn create_topics(pipeline: &Pipeline, brokers: &str) -> Result<(), Box<dyn std::error::Error>> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    let topics = [
        NewTopic::new("s3-kafka-demo", 3, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime"),
        NewTopic::new("s3-kafka-demo-retry", 1, TopicReplication::Fixed(1)),
        NewTopic::new("s3-kafka-demo-dead", 1, TopicReplication::Fixed(1))
            .set("retention.ms", "-1"),
        NewTopic::new("s3-kafka-demo-attempts", 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact"),
    ];
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    for created in pipeline.block_on(admin.create_topics(&topics, &options))? {
        match created {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => return Err(format!("creating {topic}: {code}").into()),
        }
    }
    Ok(())
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
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    create_topics(&pipeline, &brokers)?;
    // ANCHOR: connect
    let section = pipeline
        .config()
        .feed
        .clone()
        .ok_or("the file names no feed")?;
    let feed = match section.backend().type_tag() {
        "kafka" => Arc::new(KafkaFeed::connect(&section, &pipeline.io_handle())?),
        other => return Err(format!("this binary builds the `kafka` feed, not `{other}`").into()),
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
    // A unit's offset is committed after its last record; give the last
    // ones a moment.
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
