//! E2E scenario: sustained traffic, sampled for growth. Records keep flowing through
//! every round, against a low pending-batch limit, and each round records the peak
//! pending reading and the process's resident memory.
//!
//! `SPATE_E2E_LONG_RUN_ROUNDS` sets the length, in rounds. The default is short, so
//! the suite's usual command runs it, and the nightly run sets a long one.
//!
//! Selecting it:
//!
//! ```sh
//! SPATE_E2E_LONG_RUN_ROUNDS=30 cargo test -p spate --features kafka,clickhouse,avro \
//!   --locked --test e2e_long_run -- --ignored --nocapture
//! ```

// The per-round figures go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use std::process::Command;
use std::time::Duration;
use support::*;

const DEFAULT_ROUNDS: u64 = 3;
const PARTITIONS: i32 = 3;
const WAVE: u64 = 5_000;
const ROUND: u64 = 20_000;
/// Final resident memory as a percentage of the reading after the first round, which
/// has built the pipeline's pools and clients.
const RSS_LIMIT_PERCENT: u64 = 150;

fn rounds() -> u64 {
    std::env::var("SPATE_E2E_LONG_RUN_ROUNDS").map_or(DEFAULT_ROUNDS, |rounds| {
        rounds.parse().expect("a round count")
    })
}

/// This process's resident memory in KiB. It holds the pipeline and the harness's
/// own clients, so it shows growth and says nothing of a pipeline's footprint.
fn resident_kib() -> u64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .expect("run ps");
    assert!(out.status.success(), "ps failed: {out:?}");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("a resident size")
}

/// Records `range`, spread over the partitions as [`events`] spreads them.
fn wave(range: std::ops::Range<u64>) -> Vec<(i32, Event)> {
    let partitions = PARTITIONS as u64;
    range
        .map(|i| {
            let partition = (i % partitions) as i32;
            let seq = (i / partitions) as i64;
            (
                partition,
                Event {
                    id: event_id(partition, seq),
                    name: format!("evt-{partition}-{seq}"),
                },
            )
        })
        .collect()
}

/// Pins that sustained traffic stays within the pending limit, that pending work
/// returns to zero once the traffic ends, and that resident memory grows by no more
/// than the limit above. A long run also has to reach the pending limit, which a
/// short one can end before.
#[test]
#[ignore = "requires Docker"]
fn sustained_traffic_holds_pending_work_and_memory() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-long-run", 19196);
    params.max_pending_batches = 4;
    let rounds = rounds();

    h.create_topic(&params.topic, PARTITIONS);
    h.create_table(&params.table);

    let mut produced = 0;
    h.produce(&params.topic, &wave(0..WAVE));
    produced += WAVE;
    let pipeline = h.spawn_pipeline(&params);

    let mut baseline = None;
    let mut last = 0;
    for round in 1..=rounds {
        // The next wave goes out while the sink still has one to write, so the
        // pipeline never idles and the broker's disk stays bounded.
        wait_until(Duration::from_secs(600), "the round's rows", || {
            // `count` stays cheap as the table grows. A replayed duplicate only
            // ends a round early, and the drained check below reads distinct ids.
            let rows = h.count(&params.table);
            if produced < round * ROUND && produced.saturating_sub(rows) <= WAVE {
                h.produce(&params.topic, &wave(produced..produced + WAVE));
                produced += WAVE;
            }
            rows >= round * ROUND
        });
        last = resident_kib();
        let base = *baseline.get_or_insert(last);
        println!(
            "round {round}/{rounds}: peak pending {}, resident {last} KiB, baseline {base} KiB",
            pipeline.sampler.peak_pending()
        );
    }

    pipeline.assert_drained(&h, produced);
    wait_until(
        Duration::from_secs(60),
        "pending work to reach zero",
        || pipeline.pending() == Some(0.0),
    );
    let (report, sampled) = pipeline.stop_sampled();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    if rounds > DEFAULT_ROUNDS {
        assert_eq!(
            sampled.peak_pending, params.max_pending_batches as f64,
            "sustained traffic reached the limit: {sampled:?}"
        );
    }

    let baseline = baseline.expect("at least one round");
    assert!(
        last * 100 <= RSS_LIMIT_PERCENT * baseline,
        "resident memory grew from {baseline} KiB to {last} KiB"
    );
}
