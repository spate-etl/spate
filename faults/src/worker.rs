//! A worker process's configuration, and the coordinated S3 pipeline it runs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use spate_coordination::store::CoordinationStore;
use spate_coordination::store::dynamodb::{DynamoDbConfig, DynamoDbStore};
use spate_coordination::store::nats::{NatsConfig, NatsStore};
use spate_coordination::{CoordinationConfig, StoreCoordinator};
use spate_core::config::PipelineConfig;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::sink::KeyHashRouter;
use spate_json::NdjsonFramer;
use spate_s3::S3Source;
use spate_test::{BytesPassthrough, TestEncoder};

use crate::classify::Classifier;
use crate::journal::Journal;
use crate::oracle::Timing;
use crate::sink::JournalSink;
use crate::store::{AbortAt, AbortPlan, BrokenFence, Fence, JournalStore, StopAt, StopPlan};

/// Upper bound on one generated record line.
const MAX_RECORD_BYTES: usize = 64 * 1024;

/// Everything one worker process reads at start, as JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Instance id, shared by a process and its replacements.
    pub instance: String,
    /// The journal this process appends to.
    pub journal: PathBuf,
    /// The coordination store.
    pub store: StoreConfig,
    /// The S3 gateway holding the data set under `data/`.
    pub s3: S3Config,
    /// Coordinator tuning.
    pub tuning: Tuning,
    /// How long the sink holds each write before journalling it.
    pub sink_delay_ms: u64,
    /// The in-process fault this process injects, if any.
    #[serde(default)]
    pub abort: Option<AbortPlan>,
    /// The write at which this process stops itself, if any.
    #[serde(default)]
    pub stop_at: Option<StopPlan>,
    /// The stopped write, once it loses its CAS, is re-sent at the current
    /// revision.
    #[serde(default)]
    pub broken_fence: bool,
}

/// The coordination store a worker connects to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreConfig {
    /// NATS JetStream.
    Nats {
        /// Server URL.
        server: String,
        /// Job identity, shared by every worker of the run.
        job: String,
    },
    /// DynamoDB, here DynamoDB Local. The table is created when missing.
    #[serde(rename = "dynamodb")]
    DynamoDb {
        /// Endpoint URL.
        endpoint: String,
        /// Table name.
        table: String,
        /// Job identity, shared by every worker of the run.
        job: String,
    },
}

/// An S3 gateway that accepts unsigned requests over plain HTTP.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    /// Endpoint URL.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
}

/// The coordinator settings a run gives every worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tuning {
    /// Split lease duration.
    pub lease_ms: u64,
    /// Per-call store timeout.
    pub op_timeout_ms: u64,
    /// Reconcile interval.
    pub reconcile_ms: u64,
    /// Replan interval.
    pub replan_ms: u64,
    /// Cooperative drain deadline.
    pub drain_deadline_ms: u64,
    /// Delivery attempts before a split is quarantined.
    pub max_attempts: u32,
    /// Interval between two listings of a watched prefix on a polled store;
    /// 0 on a push store.
    pub poll_ms: u64,
    /// Working-set bound per worker.
    pub max_in_flight: u32,
}

impl Tuning {
    /// The tuning for NATS workers. `max_attempts` sits far above any number
    /// of faults a schedule lands on one split, so a correct run never
    /// quarantines.
    #[must_use]
    pub fn nats() -> Tuning {
        Tuning {
            lease_ms: 2_000,
            op_timeout_ms: 500,
            reconcile_ms: 500,
            replan_ms: 2_000,
            drain_deadline_ms: 5_000,
            max_attempts: 1_000,
            poll_ms: 0,
            max_in_flight: 8,
        }
    }

    /// The tuning for DynamoDB workers: six polls per lease, and a lease of at
    /// least two store timeouts.
    #[must_use]
    pub fn dynamodb() -> Tuning {
        Tuning {
            lease_ms: 3_000,
            op_timeout_ms: 1_000,
            replan_ms: 3_000,
            poll_ms: 500,
            ..Tuning::nats()
        }
    }

    /// The coordinator config for `instance`, with no rebalance delay.
    #[must_use]
    pub fn coordination(&self, instance: &str) -> CoordinationConfig {
        let mut config = CoordinationConfig::default();
        config.instance_id = Some(instance.to_owned());
        config.lease_duration = Duration::from_millis(self.lease_ms);
        config.op_timeout = Duration::from_millis(self.op_timeout_ms);
        config.reconcile_interval = Duration::from_millis(self.reconcile_ms);
        config.replan_interval = Duration::from_millis(self.replan_ms);
        config.drain_deadline = Duration::from_millis(self.drain_deadline_ms);
        config.rebalance_delay = Duration::ZERO;
        config.max_attempts = self.max_attempts;
        config.max_in_flight = self.max_in_flight;
        config
    }

    /// The oracle's view of this tuning.
    #[must_use]
    pub fn timing(&self) -> Timing {
        Timing {
            lease_ms: self.lease_ms,
            drain_deadline_ms: self.drain_deadline_ms,
            op_timeout_ms: self.op_timeout_ms,
            poll_interval_ms: self.poll_ms,
        }
    }
}

/// The NATS store that `store` names, configured with no I/O.
///
/// # Errors
///
/// Fails on an invalid server URL or job, or when `store` is not NATS.
pub fn nats_store(store: &StoreConfig, tuning: &Tuning) -> Result<NatsStore, String> {
    let StoreConfig::Nats { server, job } = store else {
        return Err(format!("not a NATS store: {store:?}"));
    };
    NatsStore::new(
        NatsConfig::new(vec![server.clone()], job.clone()),
        Duration::from_millis(tuning.lease_ms),
    )
    .map_err(|e| e.to_string())
}

/// The DynamoDB store that `store` names, signing with a fixed local key
/// pair, configured with no I/O. It creates its table when missing.
///
/// # Errors
///
/// Fails on an invalid configuration, or when `store` is not DynamoDB.
pub fn dynamodb_store(store: &StoreConfig, tuning: &Tuning) -> Result<DynamoDbStore, String> {
    let StoreConfig::DynamoDb {
        endpoint,
        table,
        job,
    } = store
    else {
        return Err(format!("not a DynamoDB store: {store:?}"));
    };
    let mut config = DynamoDbConfig::new(table.clone(), job.clone());
    config.region = Some("us-east-1".to_owned());
    config.endpoint = Some(endpoint.clone());
    config.create_table = true;
    config.poll_interval = Duration::from_millis(tuning.poll_ms);
    DynamoDbStore::with_static_credentials(
        config,
        Duration::from_millis(tuning.lease_ms),
        Duration::from_millis(tuning.op_timeout_ms),
        "local",
        "local",
    )
    .map_err(|e| e.to_string())
}

/// Runs the worker's pipeline until it exits.
///
/// # Errors
///
/// Fails when the pipeline cannot be built or exits in any state but
/// [`ExitState::Completed`].
pub fn run(config: &WorkerConfig) -> Result<(), String> {
    match &config.store {
        StoreConfig::Nats { .. } => run_on(config, nats_store(&config.store, &config.tuning)?),
        StoreConfig::DynamoDb { .. } => {
            run_on(config, dynamodb_store(&config.store, &config.tuning)?)
        }
    }
}

fn run_on<S: CoordinationStore + Clone>(config: &WorkerConfig, store: S) -> Result<(), String> {
    let journal = Arc::new(Journal::open(&config.journal).map_err(|e| e.to_string())?);
    let yaml = pipeline_yaml(config);
    let pipeline_config = PipelineConfig::from_str(&yaml).map_err(|e| e.to_string())?;
    let pipeline = Pipeline::from_config(pipeline_config).map_err(|e| e.to_string())?;
    let classifier = Arc::new(Classifier::new(config.instance.clone()));
    let (stop_at, broken_fence) = (config.stop_at, config.broken_fence);
    match config.abort {
        Some(plan) => {
            let store = layered(
                store,
                stop_at,
                broken_fence,
                &journal,
                &classifier,
                |store| AbortAt::new(store, plan, Arc::clone(&journal), Arc::clone(&classifier)),
            );
            run_pipeline(config, pipeline, store, journal)
        }
        None => {
            let store = layered(store, stop_at, broken_fence, &journal, &classifier, |s| s);
            run_pipeline(config, pipeline, store, journal)
        }
    }
}

/// `store` under a worker's wrappers, innermost first: [`JournalStore`], the
/// layer `middle` adds, [`BrokenFence`] and [`StopAt`]. [`StopAt`] stops at
/// `stop_at`, and arms the fence there only when `broken_fence` is set.
///
/// The journal sits below [`BrokenFence`] so that each re-send is journalled
/// as its own `send`.
pub(crate) fn layered<S, M>(
    store: S,
    stop_at: Option<StopPlan>,
    broken_fence: bool,
    journal: &Arc<Journal>,
    classifier: &Arc<Classifier>,
    middle: impl FnOnce(JournalStore<S>) -> M,
) -> StopAt<BrokenFence<M>> {
    let store = JournalStore::new(store, Arc::clone(journal), Arc::clone(classifier));
    let fence = Arc::new(Fence::default());
    StopAt::new(
        BrokenFence::new(middle(store), Arc::clone(&fence)),
        stop_at,
        broken_fence.then_some(fence),
        Arc::clone(journal),
        Arc::clone(classifier),
    )
}

fn run_pipeline<S: CoordinationStore + Clone>(
    config: &WorkerConfig,
    pipeline: Pipeline,
    store: S,
    journal: Arc<Journal>,
) -> Result<(), String> {
    let io = pipeline.io_handle();
    let coordinator = StoreCoordinator::new(
        store,
        config.tuning.coordination(&config.instance),
        io.clone(),
        None,
    )
    .map_err(|e| e.to_string())?;
    let source = S3Source::from_component_config(&pipeline.config().source, io)
        .map_err(|e| e.to_string())?
        .with_framer(|| Box::new(NdjsonFramer::new(MAX_RECORD_BYTES)))
        .with_coordinator(Box::new(coordinator));
    let sink = JournalSink::new(journal, Duration::from_millis(config.sink_delay_ms));
    let runtime = pipeline
        .sink(sink)
        .map_err(|e| e.to_string())?
        .chains(|ctx| {
            let chunk = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .map_err(|e| e.to_string())?;
    let report = runtime.run().map_err(|e| e.to_string())?;
    match report.state {
        ExitState::Completed => Ok(()),
        state => Err(format!("the pipeline exited {state:?}")),
    }
}

fn pipeline_yaml(config: &WorkerConfig) -> String {
    format!(
        "pipeline: {{ name: faults-{instance}, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: \"s3://{bucket}/data/\"
    split_target_bytes: 1MiB
    store:
      endpoint: \"{endpoint}\"
      allow_http: \"true\"
      skip_signature: \"true\"
      region: \"us-east-1\"
sink: {{ journal: {{}} }}
",
        instance = config.instance,
        bucket = config.s3.bucket,
        endpoint = config.s3.endpoint,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assembles_without_io(store: StoreConfig, tuning: Tuning) {
        let dir = tempfile::tempdir().unwrap();
        let config = WorkerConfig {
            instance: "w0".to_owned(),
            journal: dir.path().join("w0.ndjson"),
            store,
            s3: S3Config {
                endpoint: "http://127.0.0.1:1".to_owned(),
                bucket: "b".to_owned(),
            },
            tuning,
            sink_delay_ms: 0,
            abort: None,
            stop_at: None,
            broken_fence: false,
        };
        let pipeline =
            Pipeline::from_config(PipelineConfig::from_str(&pipeline_yaml(&config)).unwrap())
                .unwrap();
        let journal = Arc::new(Journal::open(&config.journal).unwrap());
        let classifier = Arc::new(Classifier::new("w0"));
        let coordination = config.tuning.coordination(&config.instance);
        match &config.store {
            StoreConfig::Nats { .. } => {
                let store = nats_store(&config.store, &config.tuning).unwrap();
                let store = JournalStore::new(store, journal, classifier);
                StoreCoordinator::new(store, coordination, pipeline.io_handle(), None).unwrap();
            }
            StoreConfig::DynamoDb { .. } => {
                let store = dynamodb_store(&config.store, &config.tuning).unwrap();
                let store = JournalStore::new(store, journal, classifier);
                StoreCoordinator::new(store, coordination, pipeline.io_handle(), None).unwrap();
            }
        }
        S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle()).unwrap();
    }

    /// The NATS tuning passes the coordinator's checks and assembles a worker
    /// pipeline without I/O.
    #[test]
    fn nats_worker_assembles_without_io() {
        let store = StoreConfig::Nats {
            server: "nats://127.0.0.1:1".to_owned(),
            job: "job".to_owned(),
        };
        assembles_without_io(store, Tuning::nats());
    }

    /// The DynamoDB tuning passes the coordinator's checks and assembles a
    /// worker pipeline without I/O.
    #[test]
    fn dynamodb_worker_assembles_without_io() {
        let store = StoreConfig::DynamoDb {
            endpoint: "http://127.0.0.1:1".to_owned(),
            table: "table".to_owned(),
            job: "job".to_owned(),
        };
        assembles_without_io(store, Tuning::dynamodb());
    }
}
