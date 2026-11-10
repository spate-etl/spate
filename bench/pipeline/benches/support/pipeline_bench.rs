//! The rig for `pipeline_wall.rs`: a staged corpus of event-log objects, and
//! one whole bounded pipeline per run that reads it, decodes each line into a
//! typed struct, flattens it and encodes it as ClickHouse Native into a sink
//! that discards the batches.
//!
//! `tests/pipeline_bench_fixture.rs` drives the same rig on a smaller corpus,
//! so what a case measures is also what the test checks: every line reaches
//! the sink as a row on every run, not only on the first.

#![expect(
    clippy::disallowed_types,
    reason = "`labels` is the std map the decoded event carries, moved into the row unchanged"
)]

use serde::Serialize;
use spate_clickhouse::{NativeEncoder, NativeSchema};
use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::SinkError;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, PipelineRuntime, RuntimeOptions};
use spate_core::sink::{KeyHashRouter, SealedBatch, ShardWriter, SinkParts, SinkPoolConfig};
use spate_json::{JsonDeserializerBuilder, JsonSettings, NdjsonFramer};
use spate_s3::S3Source;
use spate_s3::bench_seams::pack_members;
use spate_test::PipelineRun;
use spate_test_support::events::{Event, event_line};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Longest line the framer accepts. Event lines stay well under it.
const MAX_LINE_BYTES: usize = 4_096;

/// Sink linger of every run, the value the measured cloud runs used (the
/// default is 1 s).
const LINGER: Duration = Duration::from_millis(5);

/// The split target every run sets, in MiB. At the bench's object sizes the
/// lister packs two uncompressed objects or sixteen zstd objects to a unit,
/// so every case's thread count divides the unit count; [`Rig::units`] checks
/// the staged objects against the lister's own packing.
const SPLIT_TARGET_MIB: u64 = 25;

/// How the staged objects are stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stored {
    /// `.ndjson.zst`, zstd level 3.
    Zstd,
    /// `.ndjson`, uncompressed.
    Plain,
}

impl Stored {
    /// The name a case id and the corpus use.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Stored::Zstd => "ndjson_zst",
            Stored::Plain => "ndjson",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Stored::Zstd => "ndjson.zst",
            Stored::Plain => "ndjson",
        }
    }
}

/// The shape of a staged corpus and of the pipeline that reads it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Shape {
    /// Objects staged.
    pub(crate) objects: usize,
    /// Lines in every object.
    pub(crate) lines: usize,
    /// How the objects are stored.
    pub(crate) stored: Stored,
    /// Pipeline threads.
    pub(crate) threads: usize,
}

impl Shape {
    /// Lines in the whole corpus.
    pub(crate) fn total_lines(&self) -> u64 {
        (self.objects * self.lines) as u64
    }
}

/// One ClickHouse row per event: the nested fields lifted into columns.
#[derive(Debug, Serialize)]
struct EventRow {
    ts: String,
    host: String,
    service: String,
    level: String,
    message: String,
    trace_id: String,
    method: String,
    path: String,
    status: u16,
    user_agent: String,
    referrer: String,
    ip: String,
    country: String,
    region: String,
    city: String,
    tags: Vec<String>,
    labels: HashMap<String, String>,
}

impl From<Event> for EventRow {
    fn from(e: Event) -> EventRow {
        EventRow {
            ts: e.ts,
            host: e.host,
            service: e.service,
            level: e.level,
            message: e.message,
            trace_id: e.trace_id,
            method: e.http.method,
            path: e.http.path,
            status: e.http.status,
            user_agent: e.http.user_agent,
            referrer: e.http.referrer,
            ip: e.client.ip,
            country: e.client.geo.country,
            region: e.client.geo.region,
            city: e.client.geo.city,
            tags: e.tags,
            labels: e.labels,
        }
    }
}

/// The table [`EventRow`] encodes into, in field order.
const COLUMNS: &[(&str, &str)] = &[
    ("ts", "String"),
    ("host", "String"),
    ("service", "LowCardinality(String)"),
    ("level", "LowCardinality(String)"),
    ("message", "String"),
    ("trace_id", "String"),
    ("method", "LowCardinality(String)"),
    ("path", "String"),
    ("status", "UInt16"),
    ("user_agent", "String"),
    ("referrer", "String"),
    ("ip", "String"),
    ("country", "LowCardinality(String)"),
    ("region", "String"),
    ("city", "String"),
    ("tags", "Array(String)"),
    ("labels", "Map(String, String)"),
];

/// A corpus staged in a temporary directory, and the shape every run over it
/// shares.
#[derive(Debug)]
pub(crate) struct Rig {
    _dir: tempfile::TempDir,
    data: PathBuf,
    shape: Shape,
    deadline: Duration,
    /// Each staged object's key, as the lister lists it, and stored size.
    stored: Vec<(String, u64)>,
    /// Uncompressed bytes in the whole corpus.
    bytes: u64,
}

impl Rig {
    /// Stages the corpus `shape` describes and hands `absorb` each object's
    /// uncompressed bytes. A run that has not finished within `deadline`
    /// panics.
    ///
    /// # Panics
    ///
    /// Panics when the corpus cannot be written.
    pub(crate) fn new(shape: Shape, deadline: Duration, mut absorb: impl FnMut(&[u8])) -> Rig {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let data = dir.path().join("data");
        fs::create_dir_all(&data).expect("the data directory");
        let mut object = Vec::new();
        let (mut stored, mut bytes) = (Vec::with_capacity(shape.objects), 0);
        for o in 0..shape.objects {
            object.clear();
            let first = (o * shape.lines) as u64;
            for line in first..first + shape.lines as u64 {
                event_line(line, &mut object);
            }
            absorb(&object);
            bytes += object.len() as u64;
            let body = match shape.stored {
                Stored::Zstd => zstd::encode_all(&object[..], 3).expect("the object compresses"),
                Stored::Plain => object.clone(),
            };
            let path = data.join(format!("obj-{o:05}.{}", shape.stored.extension()));
            let key = path.to_string_lossy().trim_start_matches('/').to_owned();
            stored.push((key, body.len() as u64));
            fs::write(path, body).expect("the object is written");
        }
        Rig {
            _dir: dir,
            data,
            shape,
            deadline,
            stored,
            bytes,
        }
    }

    /// Uncompressed bytes in the whole corpus.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// How many units the lister packs the staged objects into, by
    /// `spate_s3`'s own packing at the run's split target.
    ///
    /// # Panics
    ///
    /// Panics when the units do not all hold the same number of objects.
    pub(crate) fn units(&self) -> usize {
        let objects: Vec<(&str, u64)> = self
            .stored
            .iter()
            .map(|(key, size)| (key.as_str(), *size))
            .collect();
        let members = pack_members(&objects, SPLIT_TARGET_MIB << 20);
        assert!(
            members.iter().all(|m| *m == members[0]),
            "the objects pack into units of unequal size: {members:?}"
        );
        members.len()
    }

    /// Builds one run: a uniquely named bounded pipeline over the staged
    /// corpus and a fresh sink. Nothing is shared with an earlier run.
    ///
    /// # Panics
    ///
    /// Panics when the pipeline does not assemble.
    pub(crate) fn make(&self) -> Run {
        let mut config = PipelineConfig::from_str(&self.yaml()).expect("the config parses");
        config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
        let pipeline = Pipeline::from_config(config).expect("the pipeline builds");
        let source =
            S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())
                .expect("the source config")
                .with_framer(|| Box::new(NdjsonFramer::new(MAX_LINE_BYTES)));
        let json =
            JsonDeserializerBuilder::from_settings(JsonSettings::default()).build_serde::<Event>();
        let schema = NativeSchema::from_columns(COLUMNS).expect("the schema builds");
        let written = Arc::new(Written::default());
        let mut pool = SinkPoolConfig::default();
        pool.batch.linger = LINGER;
        let sink = SinkParts::new(
            NullWriter {
                written: Arc::clone(&written),
            },
            vec![vec![()]],
            pool,
        )
        .with_component_type("discard");
        let runtime = pipeline
            .sink(sink)
            .expect("the sink installs")
            .chains(move |ctx| {
                let chunk = ctx.chunk();
                chain_owned::<Event, _>(json.clone())
                    .map(EventRow::from)
                    .sink(
                        NativeEncoder::<Owned<EventRow>>::new(Arc::clone(&schema)),
                        KeyHashRouter,
                        chunk,
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
            .expect("the runtime assembles");
        Run {
            runtime,
            written,
            deadline: self.deadline,
        }
    }

    fn yaml(&self) -> String {
        format!(
            "pipeline: {{ name: s3-pipeline-bench, threads: {} }}\n\
             admin: {{ listen: none }}\n\
             metrics: {{ exporter: none }}\n\
             source:\n  s3:\n    url: \"file://{}/\"\n    split_target_bytes: {SPLIT_TARGET_MIB}MiB\n\
             sink: {{ discard: {{}} }}\n\
             feed:\n  memory: {{}}\n",
            self.shape.threads,
            self.data.display()
        )
    }
}

/// One assembled pipeline, ready to run once.
#[derive(Debug)]
pub(crate) struct Run {
    runtime: PipelineRuntime<S3Source>,
    written: Arc<Written>,
    deadline: Duration,
}

/// What one run's sink was handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Drove {
    /// Rows written.
    pub(crate) rows: u64,
    /// Encoded bytes written.
    pub(crate) bytes: u64,
}

impl Run {
    /// Runs the bounded job until it exits by itself and reports what its
    /// sink was handed.
    ///
    /// # Panics
    ///
    /// Panics when the run fails, does not complete, or misses the rig's
    /// deadline.
    pub(crate) fn drive(self) -> Drove {
        let Run {
            runtime,
            written,
            deadline,
        } = self;
        let report = PipelineRun::spawn(move || runtime.run())
            .wait_exit(deadline)
            .expect("the run exits within the deadline")
            .expect("the run starts");
        assert_eq!(report.state, ExitState::Completed, "the run completes");
        Drove {
            rows: written.rows.load(Ordering::Relaxed),
            bytes: written.bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct Written {
    rows: AtomicU64,
    bytes: AtomicU64,
}

/// A sink writer that counts what it is handed and discards it.
#[derive(Debug)]
struct NullWriter {
    written: Arc<Written>,
}

impl ShardWriter for NullWriter {
    type Endpoint = ();

    async fn write_batch(&self, (): &(), batch: &SealedBatch) -> Result<(), SinkError> {
        self.written.rows.fetch_add(batch.rows, Ordering::Relaxed);
        self.written.bytes.fetch_add(batch.bytes, Ordering::Relaxed);
        Ok(())
    }
}
