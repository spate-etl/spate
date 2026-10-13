//! A whole assembly through the `Pipeline` builder over this crate's mocks:
//! memory source in, capturing sink bundle out. Framework users take this
//! path to test their own pipelines.

use spate_core::config::PipelineConfig;
use spate_core::error::SinkError;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::{FrameCodec, FrameCodecFactory, KeyHashRouter};
use spate_core::source::LaneId;
use spate_test::{
    BytesPassthrough, CaptureSink, TestEncoder, capture_sink, decode_rows, memory_source,
    unique_name,
};
use std::time::Duration;

/// A one-thread pipeline config under a name no other test in this binary
/// uses, since gauge ownership is process-wide (INV-10).
fn config() -> PipelineConfig {
    let name = unique_name("bundle-test");
    PipelineConfig::from_str(&format!(
        r#"
pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
source: {{ memory: {{}} }}
sink: {{ capture: {{}} }}
"#
    ))
    .expect("config")
}

#[test]
fn builder_pipeline_over_mocks_delivers_and_commits() {
    let (sink, script) = capture_sink(1, 1);
    run_three_records(sink);

    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(
        rows,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()],
        "every record captured, in order"
    );
}

/// The tag a [`LengthTagCodec`] frame opens with.
const TAG: &[u8] = b"CODE";

/// A [`FrameCodec`] that writes [`TAG`], the chunk's length as a `u32`
/// little-endian, then the chunk, so frames stay separable once concatenated.
#[derive(Default)]
struct LengthTagCodec {
    out: Vec<u8>,
}

impl FrameCodec for LengthTagCodec {
    fn encode_frame(&mut self, chunk: &[u8]) -> Result<&[u8], SinkError> {
        self.out.clear();
        self.out.extend_from_slice(TAG);
        self.out.extend_from_slice(
            &u32::try_from(chunk.len())
                .expect("small chunk")
                .to_le_bytes(),
        );
        self.out.extend_from_slice(chunk);
        Ok(&self.out)
    }
}

/// Split a captured payload into its [`LengthTagCodec`] frames' contents.
fn untag(mut payload: &[u8]) -> Vec<&[u8]> {
    let mut frames = Vec::new();
    while !payload.is_empty() {
        let rest = payload
            .strip_prefix(TAG)
            .expect("every captured frame is the codec's output");
        let len = u32::from_le_bytes(rest[..4].try_into().expect("4 bytes")) as usize;
        frames.push(&rest[4..4 + len]);
        payload = &rest[4 + len..];
    }
    frames
}

/// A codec attached to the capturing sink codes every frame the pipeline
/// writes, and the written batch counts the bytes before the codec.
#[test]
fn a_capture_sinks_codec_codes_every_captured_frame() {
    let (sink, script) = capture_sink(1, 1);
    run_three_records(sink.with_frame_codec(FrameCodecFactory::new(|| {
        Box::new(LengthTagCodec::default())
    })));

    let writes = script.writes();
    let mut rows = Vec::new();
    for write in &writes {
        let frames = untag(&write.payload);
        let raw: usize = frames.iter().map(|f| f.len()).sum();
        assert_eq!(write.bytes, raw as u64, "the batch counts pre-codec bytes");
        rows.extend(frames.iter().flat_map(|f| decode_rows(f)));
    }
    assert_eq!(
        rows,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()],
        "every record captured, in order"
    );
}

/// Run `alpha`, `beta` and `gamma` through a one-thread pipeline into `sink`,
/// and stop once the source commits past them.
fn run_three_records(sink: CaptureSink) {
    let (source, handle) = memory_source();

    let runtime = Pipeline::from_config(config())
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
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
        .into_runtime(source)
        .expect("into_runtime");

    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let mut last = 0;
    for payload in [&b"alpha"[..], b"beta", b"gamma"] {
        last = handle.push(p, Some(b"key"), payload);
    }

    // At-least-once: the watermark (one-past-last) only reaches past
    // `last` once the capture writer durably acknowledged every record.
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(10)),
        "timed out waiting for the commit (last: {:?})",
        handle.last_committed(p)
    );
    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");
}
