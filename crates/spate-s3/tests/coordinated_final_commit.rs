//! The final coordinated commit at shutdown: six held splits over an
//! in-process store, committed by a healthy store and bounded by
//! `op_timeout` over one that stops answering.

mod support;

use spate_coordination::StoreCoordinator;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_core::pipeline::ExitState;
use spate_test::{WriteOutcome, wait_until};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use support::{
    Launched, PipelineYaml, captured_rows, launch_customized, line_framer, lines_bytes, recs,
    test_options, test_tuning,
};

/// Delegates to a [`MemoryStore`] until `wedged` is set, then never answers.
#[derive(Clone)]
struct WedgeStore {
    inner: MemoryStore,
    wedged: Arc<AtomicBool>,
}

impl WedgeStore {
    async fn gate(&self) {
        if self.wedged.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
    }
}

impl CoordinationStore for WedgeStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate().await;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.gate().await;
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.gate().await;
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate().await;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.gate().await;
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.gate().await;
        self.inner.list(ks, prefix).await
    }
}

const LEASE: Duration = Duration::from_secs(2);
const OBJECTS: usize = 6;

fn split_records(store: &MemoryStore) -> Vec<serde_json::Value> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("reader runtime");
    rt.block_on(store.list(Keyspace::Durable, "split."))
        .expect("list split records")
        .iter()
        .map(|e| serde_json::from_slice(&e.value).expect("a JSON split record"))
        .collect()
}

/// A worker over `store` holding six one-object splits, each read slowly
/// enough to stay incomplete, with tick commits held off by a 60s interval.
fn holding_six(data: &std::path::Path, store: WedgeStore) -> Launched {
    for p in 0..OBJECTS {
        fs::write(
            data.join(format!("o{p}.ndjson")),
            lines_bytes(&recs(&format!("o{p}"), 40_000)),
        )
        .unwrap();
    }
    let yaml = PipelineYaml::file("s3-final-commit", data)
        .checkpoint("60s")
        .source("prefetch_bytes", "64KiB")
        .source("chunk_bytes", "16KiB")
        .section("backpressure: { max_inflight_bytes: 4KiB }")
        .build();
    let inner = store.inner.clone();
    let l = launch_customized(
        &yaml,
        test_options(),
        |sink| {
            for _ in 0..400 {
                sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
            }
        },
        move |source, io| {
            let mut tuning = test_tuning();
            tuning.op_timeout = Duration::from_secs(1);
            tuning.lease_duration = LEASE;
            tuning.replan_interval = LEASE;
            tuning.max_in_flight = 8;
            tuning.instance_id = Some("w".to_string());
            let coordinator =
                StoreCoordinator::new(store, tuning, io, None).expect("coordinator builds");
            line_framer(source).with_coordinator(Box::new(coordinator))
        },
    );
    wait_until(Duration::from_secs(30), "rows from all six objects", || {
        let rows = captured_rows(&l.script);
        (0..OBJECTS).all(|p| {
            let tag = format!("\"k\":\"o{p}-");
            rows.iter().any(|r| r.contains(&tag))
        })
    });
    wait_until(
        Duration::from_secs(30),
        "six owned, incomplete split records",
        || {
            let records = split_records(&inner);
            records.len() == OBJECTS
                && records
                    .iter()
                    .all(|r| r["owner"] == "w" && r["completed"] == false)
        },
    );
    l
}

/// A stop over a store that stops answering ends within the drain plus the
/// final commit's one `op_timeout` budget and the departure's.
#[test]
// The stop time on stderr is what a passing run reports.
#[allow(clippy::print_stderr)]
fn a_final_commit_over_a_store_that_stops_answering_ends_within_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let store = WedgeStore {
        inner: MemoryStore::new(LEASE),
        wedged: Arc::default(),
    };
    let wedged = Arc::clone(&store.wedged);
    let l = holding_six(&data, store);

    wedged.store(true, Ordering::SeqCst);
    let started = Instant::now();
    l.shutdown.trigger();
    let exit = l.run.wait_exit(Duration::from_secs(5));
    let took = started.elapsed();
    eprintln!("stop took {took:?}");

    let report = exit
        .unwrap_or_else(|| panic!("no exit within 5s of the trigger"))
        .expect("the pipeline started");
    assert_eq!(report.state, ExitState::Completed, "stop took {took:?}");
}

/// A stop over a healthy store commits every held split in its final commit.
#[test]
fn a_healthy_stop_commits_every_held_split_in_the_final_commit() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let store = WedgeStore {
        inner: MemoryStore::new(LEASE),
        wedged: Arc::default(),
    };
    let inner = store.inner.clone();
    let l = holding_six(&data, store);

    l.shutdown.trigger();
    let report = l
        .run
        .wait_exit(Duration::from_secs(20))
        .expect("the run exits")
        .expect("the pipeline started");
    assert_eq!(report.state, ExitState::Completed);

    let records = split_records(&inner);
    assert_eq!(records.len(), OBJECTS, "{records:?}");
    for record in &records {
        assert!(
            record["watermark"].as_i64().is_some_and(|w| w > 0),
            "every split committed progress: {record}"
        );
        assert_eq!(record["completed"], false, "{record}");
    }
}
