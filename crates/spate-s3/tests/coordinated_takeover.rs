//! Hard-crash takeover: an instance that stops heartbeating without
//! releasing (network death, SIGKILL) must lose its leases by expiry, and
//! a healthy peer on the same store must finish the job.
//!
//! The crash is simulated at the coordination-store seam: a kill switch
//! makes every store operation fail like a partitioned network, so the
//! victim can neither renew, commit, nor gracefully release — exactly the
//! state a killed process leaves behind. Its already-established watch
//! keeps streaming (an in-memory stream has no socket to sever), which
//! only lets the victim *observe* its losses; it cannot act on anything.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_coordination::{CoordinationConfig, StoreCoordinator};
use spate_core::pipeline::ExitState;
use spate_test::{WriteOutcome, wait_until};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use support::{
    Launched, captured_rows, launch_customized, launch_tuned, line_framer, lines_bytes, recs,
    shared_store, test_options, test_tuning,
};

/// Delegates to a [`MemoryStore`] until the switch flips, then fails every
/// operation like a partitioned network (retryable — a real outage is).
#[derive(Clone)]
struct KillSwitchStore {
    inner: MemoryStore,
    dead: Arc<AtomicBool>,
}

impl KillSwitchStore {
    fn new(inner: MemoryStore) -> (KillSwitchStore, Arc<AtomicBool>) {
        let dead = Arc::new(AtomicBool::new(false));
        (
            KillSwitchStore {
                inner,
                dead: Arc::clone(&dead),
            },
            dead,
        )
    }

    fn gate(&self) -> Result<(), StoreError> {
        if self.dead.load(Ordering::Relaxed) {
            return Err(StoreError::Retryable(
                "simulated network partition (hard crash)".into(),
            ));
        }
        Ok(())
    }
}

impl CoordinationStore for KillSwitchStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate()?;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.gate()?;
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.gate()?;
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate()?;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.gate()?;
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.gate()?;
        self.inner.list(ks, prefix).await
    }
}

fn config_yaml(data: &std::path::Path, name: &str) -> String {
    format!(
        r#"
pipeline: {{ name: {name}, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

#[test]
fn a_hard_crashed_instance_loses_its_leases_and_a_peer_finishes_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    // Enough objects for several splits at the 16-member cap, so the
    // victim demonstrably dies holding work a peer must take over.
    let mut expected: Vec<String> = Vec::new();
    for i in 0..48 {
        let lines = recs(&format!("t{i:02}"), 20);
        fs::write(data.join(format!("obj-{i:02}.ndjson")), lines_bytes(&lines)).unwrap();
        expected.extend(lines);
    }
    let store = shared_store();

    // Victim: kill-switch store, heavily paced sink so it is mid-flight
    // (progress committed, splits held) when the partition hits.
    let (wrapped, dead) = KillSwitchStore::new(store.clone());
    let victim: Launched = launch_customized(
        &config_yaml(&data, "s3-takeover-victim"),
        test_options(),
        |sink| {
            for _ in 0..40 {
                sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(100)));
            }
        },
        move |source, io| {
            let tuning = CoordinationConfig {
                instance_id: Some("victim".to_string()),
                max_in_flight: 2,
                ..test_tuning()
            };
            let coordinator =
                StoreCoordinator::new(wrapped, tuning, io, None).expect("coordinator builds");
            line_framer(source).with_coordinator(Box::new(coordinator))
        },
    );
    wait_until(
        Duration::from_secs(30),
        "victim streams and commits",
        || !captured_rows(&victim.script).is_empty(),
    );

    // Hard crash: from here the victim cannot renew, commit, claim, or
    // release — its leases can only expire.
    dead.store(true, Ordering::Relaxed);

    // Survivor: healthy on the same store, joins after the crash.
    let survivor = launch_tuned(
        &config_yaml(&data, "s3-takeover-survivor"),
        test_options(),
        &store,
        CoordinationConfig {
            instance_id: Some("survivor".to_string()),
            ..test_tuning()
        },
        |_sink| {},
    );

    let rs = survivor
        .run
        .wait_exit(Duration::from_secs(120))
        .unwrap()
        .unwrap();
    assert_eq!(
        rs.state,
        ExitState::Completed,
        "the survivor finishes the whole job on expired leases"
    );
    let rows_survivor = captured_rows(&survivor.script);
    assert!(
        !rows_survivor.is_empty(),
        "the survivor took over real work"
    );

    // Unwedge the victim (its exit state is incidental: a partitioned
    // instance may fail on the store, or observe its losses and drain).
    victim.shutdown.trigger();
    let _ = victim.run.wait_exit(Duration::from_secs(60));

    // At-least-once across the crash: nothing the victim left behind is
    // missing — the union of both sinks covers every staged record.
    let rows_victim = captured_rows(&victim.script);
    let mut union: Vec<String> = rows_victim
        .iter()
        .chain(rows_survivor.iter())
        .cloned()
        .collect();
    union.sort();
    union.dedup();
    expected.sort();
    assert_eq!(union, expected, "the union must cover every record");
}
