//! A sink encoder that cannot finalize a block fails the run, through the
//! `Pipeline` builder over this crate's mocks. Regression for #351.

use bytes::BytesMut;
use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::{PartitionId, Record};
use spate_core::sink::{KeyHashRouter, RowEncoder};
use spate_core::source::LaneId;
use spate_test::{BytesPassthrough, PipelineRun, capture_sink, memory_source};
use std::time::Duration;

/// Accepts every row and holds it, then fails to finalize the block. The
/// shape of a columnar encoder whose schema cannot take the rows it buffered.
#[derive(Clone, Default)]
struct BrokenFinishEncoder;

impl RowEncoder<Owned<Vec<u8>>> for BrokenFinishEncoder {
    fn encode<'buf>(
        &mut self,
        _rec: &Record<Vec<u8>>,
        _buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        Ok(())
    }
    fn buffered_bytes(&self) -> usize {
        1
    }
    fn finish_chunk(&mut self, _buf: &mut BytesMut) -> Result<(), SinkError> {
        Err(SinkError::Client {
            class: ErrorClass::Fatal,
            reason: "cannot finalize the block".into(),
        })
    }
}

/// The intervals put the only finalize on the shutdown drain: no commit tick
/// and no idle lull arrives before the signal, so a run that reports the fatal
/// only on a later flush reports nothing at all.
const CONFIG: &str = r#"
pipeline: { name: broken-finish, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 30s, drain_timeout: 5s, stalled_fail_after: 60s }
source: { memory: {} }
sink: { capture: {} }
"#;

#[test]
fn a_finalize_failure_on_the_shutdown_drain_fails_the_run() {
    let (source, handle) = memory_source();
    let (sink_a, _script) = capture_sink(1, 1);

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .sink(sink_a)
        .expect("sink")
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    BrokenFinishEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            idle_flush: Duration::from_secs(3600),
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");

    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, b"row");

    // Long enough for the row to reach the terminal stage and be buffered,
    // short of the 30s commit tick that would flush it earlier.
    std::thread::sleep(Duration::from_millis(500));
    shutdown.trigger();

    let report = run
        .wait_exit(Duration::from_secs(20))
        .expect("pipeline did not exit")
        .expect("run");
    assert!(
        matches!(report.state, ExitState::Failed(_)),
        "a batch abandoned by a broken finalize must fail the run (got {:?})",
        report.state
    );
    assert_eq!(report.exit_code(), 1);
}
