//! The handler core of an AWS Lambda function that reads one SQS batch of S3
//! storage notifications per invocation.
//!
//! [`handle_batch`] answers one invocation. [`run_batch`] publishes each
//! message into an in-process feed, runs the pipeline over that seeded feed on
//! a thread of its own, and returns the ids of the messages SQS must deliver
//! again. `main.rs` wires it to the Lambda runtime with a ClickHouse sink.

use aws_lambda_events::event::sqs::{SqsBatchResponse, SqsEvent};
use spate::feed::memory::MemoryFeed;
use spate::feed::{Feed, Unit};
use spate::hash::QualitySet;
use spate::pipeline::{ExitState, Pipeline, PipelineRuntime, ShutdownHandle};
use spate::s3::S3Source;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

/// The function's pipeline file. It is read once per invocation, so its
/// `${VAR}` forms take the function's environment.
pub const CONFIG: &str = r#"
# ANCHOR: config
pipeline: { name: lambda-s3 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint:
  # The function stops the run this long before its deadline, with time
  # added for the runtime to shut down and for the answer to Lambda.
  drain_timeout: 10s
feed: { memory: {} }
source:
  s3:
    url: "${SPATE_S3_URL}"
deserializer:
  json: {}
sink:
  clickhouse:
    table: ${CLICKHOUSE_TABLE:-page_views}
    format: native
    shards:
      - replicas: ["${CLICKHOUSE_URL}"]
    user: ${CLICKHOUSE_USER:-default}
    password: ${CLICKHOUSE_PASSWORD:-}
# ANCHOR_END: config
"#;

/// The function's log filter when `RUST_LOG` is unset. The CloudWatch recorder
/// logs an error for each histogram value past 100 in one invocation, which a
/// per-poll histogram passes in every invocation, so its collector is off.
pub const LOG_FILTER: &str = "info,metrics_cloudwatch_embedded::collector=off";

/// The longest NDJSON record the framer reads.
pub const MAX_RECORD_BYTES: usize = 1 << 20;

/// One SQS message of the batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    /// The SQS message id, which names the message in a batch item failure.
    pub id: String,
    /// The message body, published to the feed unchanged.
    pub body: String,
}

/// A message in the batch carries no id, so no failure can name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MissingId {
    /// The message's position in the batch.
    pub position: usize,
}

impl fmt::Display for MissingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "message {} of the batch has no message id; failing the whole batch",
            self.position
        )
    }
}

impl std::error::Error for MissingId {}

/// The batch's messages, in order.
///
/// # Errors
///
/// [`MissingId`] when a message has no id.
pub fn messages(event: SqsEvent) -> Result<Vec<Message>, MissingId> {
    event
        .records
        .into_iter()
        .enumerate()
        .map(|(position, message)| {
            let id = message.message_id.ok_or(MissingId { position })?;
            Ok(Message {
                id,
                body: message.body.unwrap_or_default(),
            })
        })
        .collect()
}

/// The longest `run` takes to return once its drain has ended: it shuts its
/// I/O runtime down with this timeout.
pub const RUNTIME_SHUTDOWN: Duration = Duration::from_secs(2);

/// The time left to answer Lambda once `run` has returned.
pub const RESPONSE_MARGIN: Duration = Duration::from_secs(1);

/// The invocation has too little time left for a stop to finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooShort {
    /// The time left until the invocation's deadline.
    pub left: Duration,
    /// The time a stop needs.
    pub needed: Duration,
}

impl fmt::Display for TooShort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the invocation has {:?} left and a stop needs {:?} (checkpoint.drain_timeout, \
             {RUNTIME_SHUTDOWN:?} to shut the runtime down and {RESPONSE_MARGIN:?} to \
             answer); raise the function timeout or lower checkpoint.drain_timeout",
            self.left, self.needed
        )
    }
}

impl std::error::Error for TooShort {}

/// When to stop a run that must return before `deadline`.
///
/// # Errors
///
/// [`TooShort`] when no time is left to read before the stop.
pub fn stop_at(
    now: SystemTime,
    deadline: SystemTime,
    drain_timeout: Duration,
) -> Result<SystemTime, TooShort> {
    let needed = drain_timeout
        .saturating_add(RUNTIME_SHUTDOWN)
        .saturating_add(RESPONSE_MARGIN);
    let left = deadline.duration_since(now).unwrap_or(Duration::ZERO);
    if left <= needed {
        return Err(TooShort { left, needed });
    }
    Ok(now + (left - needed))
}

/// An error from building the pipeline or answering an invocation.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ANCHOR: handler
/// Answer one invocation whose batch is `event` and whose deadline is
/// `deadline`: read the batch through the pipeline `assemble` builds, stop the
/// run in time for `drain_timeout`, and name each message to deliver again.
///
/// # Errors
///
/// [`TooShort`] when the invocation has too little time left, and
/// [`MissingId`] when a message has no id. Lambda then delivers the whole
/// batch again.
pub async fn handle_batch<A>(
    event: SqsEvent,
    deadline: SystemTime,
    drain_timeout: Duration,
    assemble: A,
) -> Result<SqsBatchResponse, BoxError>
where
    A: FnOnce() -> Result<(Pipeline, S3Source), BoxError> + Send + 'static,
{
    let stop = stop_at(SystemTime::now(), deadline, drain_timeout)?;
    let messages = messages(event)?;
    let wait = stop
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    let failed = run_batch(messages, assemble, tokio::time::sleep(wait)).await;
    let mut response = SqsBatchResponse::default();
    for id in failed {
        response.add_failure(id);
    }
    Ok(response)
}
// ANCHOR_END: handler

// ANCHOR: run_batch
/// Read `messages` through the pipeline and source `assemble` builds, and
/// return the ids SQS must deliver again, in batch order. Once `stop`
/// resolves, the run is stopped and drains.
///
/// A message is done only when the run exits `Completed` and its unit is
/// neither dead-lettered nor unsettled. Any other ending fails every message.
pub async fn run_batch<A, S>(messages: Vec<Message>, assemble: A, stop: S) -> Vec<String>
where
    A: FnOnce() -> Result<(Pipeline, S3Source), BoxError> + Send + 'static,
    S: Future<Output = ()>,
{
    let start = move |feed: Arc<dyn Feed>| {
        let (pipeline, source) = assemble()?;
        Ok(pipeline.seeded_feed(feed).into_runtime(source)?)
    };
    run_seeded(messages, start, stop).await
}
// ANCHOR_END: run_batch

/// [`run_batch`] for code that builds the runtime itself: `start` receives
/// the seeded feed, for `Pipeline::seeded_feed`.
pub async fn run_seeded<A, S>(messages: Vec<Message>, start: A, stop: S) -> Vec<String>
where
    A: FnOnce(Arc<dyn Feed>) -> Result<PipelineRuntime<S3Source>, BoxError> + Send + 'static,
    S: Future<Output = ()>,
{
    let ids: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
    // A unit that fails once is reported, so SQS owns the retry and its
    // `maxReceiveCount` moves the message to the dead-letter queue.
    let feed = MemoryFeed::with_attempt_cap(NonZeroU32::MIN);
    for message in messages {
        drop(feed.publish(Unit::new(message.id, message.body)));
    }

    let (handle_tx, handle_rx) = oneshot::channel::<ShutdownHandle>();
    let (exit_tx, mut exit_rx) = oneshot::channel::<Result<ExitState, String>>();
    let seeded: Arc<dyn Feed> = Arc::new(feed.clone());
    // `from_config` refuses a tokio context, so the pipeline is built and run
    // on a plain thread. It is not joined: `run` drops the pipeline before it
    // returns, and the exit is the thread's last act.
    #[expect(
        clippy::disallowed_methods,
        reason = "`run` returning ends the thread; joining it on drop would block the runtime"
    )]
    let spawned = std::thread::Builder::new()
        .name("spate-batch".to_owned())
        .spawn(move || {
            let exit = start(seeded)
                .map_err(|e| e.to_string())
                .and_then(|runtime| {
                    drop(handle_tx.send(runtime.shutdown_handle()));
                    runtime
                        .run()
                        .map(|report| report.state)
                        .map_err(|e| e.to_string())
                });
            drop(exit_tx.send(exit));
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "could not start the pipeline thread");
        return ids;
    }

    let exit = tokio::select! {
        exit = &mut exit_rx => exit,
        () = stop => {
            if let Ok(handle) = handle_rx.await {
                handle.trigger();
            }
            exit_rx.await
        }
    };

    let failed = match exit {
        Ok(Ok(ExitState::Completed)) => not_done(ids, &feed),
        Ok(Ok(state)) => {
            tracing::error!(
                ?state,
                "the pipeline failed; every message is reported failed"
            );
            ids
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "the pipeline did not run; every message is reported failed");
            ids
        }
        Err(_) => {
            tracing::error!("the pipeline thread panicked; every message is reported failed");
            ids
        }
    };
    let counts = feed.counts();
    tracing::info!(
        failed = failed.len(),
        acknowledged = counts.acknowledged,
        dead_lettered = counts.dead_lettered,
        unsettled = counts.visible + counts.held,
        "the batch ended"
    );
    failed
}

/// The ids in `ids` whose unit the feed did not acknowledge.
fn not_done(ids: Vec<String>, feed: &MemoryFeed) -> Vec<String> {
    // A re-enqueued unit keeps its id, so one id can be both acknowledged
    // and unsettled; it is then not done.
    let open: QualitySet<String> = feed
        .dead_letters()
        .into_iter()
        .chain(feed.unsettled())
        .map(|unit| unit.id)
        .collect();
    ids.into_iter().filter(|id| open.contains(id)).collect()
}
