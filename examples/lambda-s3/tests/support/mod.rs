//! Helpers shared by this example's test binaries: a `file://` store, the
//! units that name its objects, and a pipeline over a capture sink.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use serde::Deserialize;
use spate::json::{JsonDeserializerBuilder, NdjsonFramer};
use spate::prelude::*;
use spate::s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_example_lambda_s3::{BoxError, MAX_RECORD_BYTES, Message};
use spate_test::{HANG, SinkScript, decode_rows};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub(crate) struct Rec {
    pub(crate) id: String,
}

/// A record id the chain fails on, which stops the run.
pub(crate) const POISON: &str = "poison";

pub(crate) const TEST_EVENT: &str =
    r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"b"}"#;
pub(crate) const FOREIGN: &str = r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"a.ndjson","size":3}}}]}"#;

pub(crate) fn message(id: &str, body: impl Into<String>) -> Message {
    Message {
        id: id.to_owned(),
        body: body.into(),
    }
}

/// A data directory holding `name` with one record per id.
pub(crate) fn stage(dir: &Path, name: &str, ids: &[&str]) -> PathBuf {
    let data = dir.join("data");
    fs::create_dir_all(&data).unwrap();
    let body: String = ids
        .iter()
        .map(|id| format!("{{\"id\":\"{id}\"}}\n"))
        .collect();
    fs::write(data.join(name), body).unwrap();
    data
}

/// A descriptor body naming the staged object `name`.
pub(crate) fn descriptor(data: &Path, name: &str) -> String {
    let path = data.join(name);
    let object = DescriptorObject {
        key: path.to_string_lossy().trim_start_matches('/').to_owned(),
        size: fs::metadata(&path).unwrap().len(),
        etag: None,
        last_modified_ms: 1,
    };
    String::from_utf8(SplitDescriptor::new(vec![object]).encode().unwrap()).unwrap()
}

pub(crate) fn yaml(data: &Path, feed: &str) -> String {
    format!(
        r#"
pipeline: {{ name: lambda-s3-test, threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
feed: {feed}
source: {{ s3: {{ url: "file://{}/" }} }}
deserializer: {{ json: {{}} }}
sink: {{ capture: {{}} }}
"#,
        data.display()
    )
}

/// The pipeline over `yaml` with `sink`, writing each record's id.
pub(crate) fn assemble(
    yaml: String,
    sink: spate_test::CaptureSink,
) -> impl FnOnce() -> Result<(Pipeline, S3Source), BoxError> + Send + 'static {
    move || {
        let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml)?)?;
        let source =
            S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
                .with_framer(|| Box::new(NdjsonFramer::new(MAX_RECORD_BYTES)));
        let section = pipeline
            .config()
            .deserializer
            .clone()
            .ok_or("deserializer")?;
        let pipeline = pipeline.sink(sink)?.chains(move |ctx| {
            let chunk = ctx.chunk();
            let deser = JsonDeserializerBuilder::from_component(&section)
                .and_then(|b| b.for_source_framing(ctx.source_framing))
                .expect("deserializer section")
                .build_serde::<Rec>();
            chain_owned::<Rec, _>(deser)
                .try_map(
                    |r: Rec| match r.id.as_str() {
                        POISON => Err("a poison record"),
                        _ => Ok(r.id.into_bytes()),
                    },
                    ErrorPolicy::Fail,
                )
                .sink(
                    spate_test::TestEncoder,
                    KeyHashRouter,
                    chunk,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        });
        Ok((pipeline, source))
    }
}

pub(crate) fn rows(script: &SinkScript) -> Vec<String> {
    let mut rows: Vec<String> = script
        .writes()
        .iter()
        .filter(|w| w.result == spate_test::ScriptedResult::Ok)
        .flat_map(|w| decode_rows(&w.payload))
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    rows.sort();
    rows
}

pub(crate) async fn bounded<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(HANG, f).await.expect("the batch ends")
}
