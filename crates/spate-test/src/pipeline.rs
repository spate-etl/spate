//! A whole-pipeline assembly for tests: one chain from a source through an
//! encoder to one sink.

use spate_core::deser::{BytesPassthrough, Owned};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, PipelineRuntime, RuntimeOptions};
use spate_core::sink::{KeyHashRouter, RowEncoder, SinkBundle};
use spate_core::source::Source;

/// Assembles `pipeline`, which holds no sink yet, into a runtime that passes
/// `source`'s payloads through `encoder` to `sink`, with one chain named `main`.
///
/// Signal handling is off whatever `options` says, since a test stops the run
/// through [`PipelineRuntime::shutdown_handle`]. Everything else in `options`
/// applies, so start from [`RuntimeOptions::default`].
///
/// # Panics
///
/// If the sink cannot be installed or the runtime cannot be built.
///
/// # Examples
///
/// ```
/// use spate_core::config::PipelineConfig;
/// use spate_core::pipeline::{Pipeline, RuntimeOptions};
/// use spate_test::{
///     PipelineRun, TestEncoder, capture_sink, memory_source, passthrough_runtime, unique_name,
/// };
///
/// let config = PipelineConfig::from_str(&format!(
///     r#"
/// pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
/// admin: {{ listen: none }}
/// metrics: {{ exporter: none }}
/// source: {{ memory: {{}} }}
/// sink: {{ capture: {{}} }}
/// "#,
///     name = unique_name("docs"),
/// ))
/// .expect("config");
/// let (source, _handle) = memory_source();
/// let (sink, _script) = capture_sink(1, 1);
/// let pipeline = Pipeline::from_config(config).expect("builder");
/// let runtime =
///     passthrough_runtime(pipeline, source, sink, TestEncoder, RuntimeOptions::default());
/// let shutdown = runtime.shutdown_handle();
/// let run = PipelineRun::spawn(move || runtime.run());
/// shutdown.trigger();
/// run.join().expect("clean exit");
/// ```
#[must_use]
pub fn passthrough_runtime<S, B, E>(
    pipeline: Pipeline,
    source: S,
    sink: B,
    encoder: E,
    options: RuntimeOptions,
) -> PipelineRuntime<S>
where
    S: Source + 'static,
    B: SinkBundle,
    E: RowEncoder<Owned<Vec<u8>>> + Clone + Send + 'static,
{
    pipeline
        .sink(sink)
        .expect("install the sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    encoder.clone(),
                    KeyHashRouter,
                    chunk,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(without_signals(options))
        .into_runtime(source)
        .expect("build the runtime")
}

/// `options` with signal handling off, so a signal stays the test process's.
fn without_signals(options: RuntimeOptions) -> RuntimeOptions {
    RuntimeOptions {
        handle_signals: false,
        ..options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test stops its run through the shutdown handle, whatever the options
    /// it started from say about signals, and the rest of them still apply.
    #[test]
    fn only_signal_handling_is_overridden() {
        let options = without_signals(RuntimeOptions {
            handle_signals: true,
            max_records: 7,
            ..RuntimeOptions::default()
        });
        assert!(!options.handle_signals);
        assert_eq!(options.max_records, 7);
    }
}
