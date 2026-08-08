//! A malformed record is skipped and counted, not fatal.
//!
//! Shows the JSON deserializer decoding **N records from one payload** (JSON
//! Lines), with a malformed line skipped-and-counted
//! (`spate_json_deser_records_dropped_total`) and a filtered record dropped,
//! while the source offset only commits once every surviving record is durably
//! written (at-least-once). Runs anywhere via `spate-test`'s in-memory pieces —
//! no external systems:
//!
//! ```sh
//! cargo run -p spate --features json --example json_skip_bad_records
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-RANK:  20
// INDEX-TIER:  getting-started
// INDEX-GOAL:  decode many records from one NDJSON payload and skip malformed lines rather than stop
// INDEX-TECH:  JSON
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::Deserialize;
use spate::json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{TestEncoder, capture_sink, memory_source};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"
pipeline: { name: json-ndjson-demo, threads: 1 }
checkpoint: { interval: 200ms }
source: { memory: {} }
sink: { capture: {} }
"#;

#[derive(Debug, Deserialize)]
struct Event {
    #[expect(dead_code, reason = "decoded but not projected into the sink row")]
    id: u64,
    name: String,
    value: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;

    let (source, handle) = memory_source();

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    };
    let sink = sink.with_pool_config(pool_cfg);

    // NDJSON in → typed `Event` → filter → project to a row → capturing sink.
    let runtime = pipeline
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            // ANCHOR: deserializer
            let deser = JsonDeserializerBuilder::from_settings(JsonSettings {
                framing: JsonFraming::Ndjson,
                on_error: OnError::Skip,
                reject_duplicate_keys: false,
            })
            .with_metrics(ctx.pipeline.clone(), "main")
            .build_serde::<Event>();
            // ANCHOR_END: deserializer
            chain_owned::<Event, _>(deser)
                .with_metrics(ctx.pipeline, "main")
                .filter(|e: &Event| e.value >= 0)
                .map(|e: Event| format!("{}={}", e.name, e.value).into_bytes())
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
    let join = std::thread::spawn(move || runtime.run());

    // One payload, four NDJSON lines: line 2 is malformed (skipped), line 3
    // has a negative value (filtered out), lines 1 and 4 survive.
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let payload = concat!(
        "{\"id\":1,\"name\":\"alpha\",\"value\":10}\n",
        "NOT JSON\n",
        "{\"id\":2,\"name\":\"beta\",\"value\":-5}\n",
        "{\"id\":3,\"name\":\"gamma\",\"value\":30}"
    )
    .as_bytes();
    let last = handle.push(p0, Some(b"demo"), payload);

    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p0) != Some(last + 1) {
        assert!(Instant::now() < deadline, "commit not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;

    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();

    println!("\npipeline exit: {:?}", report.state);
    println!("final watermarks: {:?}", report.final_watermarks);
    println!("rows written ({}): {rows:?}", rows.len());
    assert_eq!(
        rows.len(),
        2,
        "4 NDJSON lines: 1 malformed skipped, 1 negative filtered"
    );
    assert!(rows.contains(&"alpha=10".to_string()));
    assert!(rows.contains(&"gamma=30".to_string()));
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
