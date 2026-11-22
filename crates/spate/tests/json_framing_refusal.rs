//! A JSON deserializer that splits payloads, under an S3 source that frames
//! each line into its own payload, stops the pipeline at start.

use serde::Deserialize;
use spate::json::{JsonDeserializerBuilder, JsonFraming, JsonLinesFramer};
use spate::prelude::*;
use spate::s3::S3Source;
use spate_test::{TestEncoder, capture_sink};

#[derive(Debug, Deserialize)]
struct Rec {
    #[expect(dead_code, reason = "decoded, never read")]
    id: u64,
}

#[test]
fn array_framing_under_a_line_framed_s3_source_refuses_to_start() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(data.join("a.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let yaml = format!(
        r#"
pipeline: {{ name: json-framing-refusal, threads: 1 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 200ms }}
metrics: {{ exporter: none }}
feed: {{ memory: {{}} }}
source: {{ s3: {{ url: "file://{}/" }} }}
deserializer: {{ json: {{}} }}
sink: {{ capture: {{}} }}
"#,
        data.display()
    );
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml).unwrap()).unwrap();
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())
        .unwrap()
        .with_framer(|| Box::new(JsonLinesFramer::new(1 << 20)));
    let section = pipeline.config().deserializer.clone().unwrap();
    let (sink, script) = capture_sink(1, 1);
    let err = pipeline
        .sink(sink)
        .unwrap()
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            let deser = JsonDeserializerBuilder::from_component_config(&section)
                .unwrap()
                .framing(JsonFraming::Array)
                .build_serde::<Rec>();
            chain_owned::<Rec, _>(deser)
                .map(|_: Rec| b"row".to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .run(source)
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid runtime configuration: deserializer.json: splits each payload as \
         `JsonFraming::Array`, but the source already frames one record per payload; build \
         the deserializer with `JsonDeserializerBuilder::framing(JsonFraming::Single)`, or use \
         a source that leaves framing to the deserializer"
    );
    assert!(script.writes().is_empty(), "nothing reaches the sink");
}
