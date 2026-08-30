//! librdkafka statistics → `spate_kafka_source_*` metric families.
//!
//! [`KafkaStatsMetrics`] translates the periodic [`Statistics`] snapshot
//! (captured by the `ClientContext::stats` callback, drained by
//! `KafkaSource::publish_stats` on the controller thread) into the
//! connector-owned families [the metrics reference] documents under Kafka
//! source. All fixed handles are resolved from the runtime-minted
//! [`Meter`](spate_core::metrics::Meter) once at `open`; per-broker and
//! per-partition handles are registered lazily on first sighting, on a
//! control-plane path and never per record.
//!
//! # Counter identity
//!
//! librdkafka reports **cumulative totals** inside each snapshot; they are
//! mirrored through [`Counter::absolute`], whose fetch-max contract makes
//! duplicate delivery idempotent and lets PromQL `rate()`/`increase()` work
//! natively. That mapping is sound only while the totals are monotonic, i.e.
//! scoped to a single consumer handle: `KafkaSource::open` creates the
//! consumer exactly once per source lifetime (a second `open` is a hard
//! error). If in-process consumer recreation is ever introduced, these
//! series would restart at zero and the fetch-max guard would silently
//! flat-line them at the old high-water mark (see Vector issue #20697);
//! switch to delta accumulation against the previous snapshot instead.
//!
//! # Windows
//!
//! `rtt`/`throttle` gauges expose librdkafka's HDR-histogram rolling-window
//! estimates. They are per-broker sampled quantiles over the last statistics
//! interval and **cannot be aggregated** across brokers or processes
//! (`max()` is the only defensible cross-series operator). librdkafka
//! reports `rtt` in microseconds but `throttle` in milliseconds; both are
//! converted to seconds here.
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

use rdkafka::statistics::{Partition, Statistics};
use spate_core::metrics::{Counter, Gauge, Meter};
use spate_core::record::PartitionId;
use std::collections::{HashMap, HashSet};

const TX_REQUESTS_TOTAL: &str = "tx_requests_total";
const TX_BYTES_TOTAL: &str = "tx_bytes_total";
const RX_RESPONSES_TOTAL: &str = "rx_responses_total";
const RX_BYTES_TOTAL: &str = "rx_bytes_total";
const RX_MESSAGES_TOTAL: &str = "rx_messages_total";
const RX_MESSAGE_BYTES_TOTAL: &str = "rx_message_bytes_total";
const BROKER_TX_RETRIES_TOTAL: &str = "broker_tx_retries_total";
const BROKER_REQ_TIMEOUTS_TOTAL: &str = "broker_req_timeouts_total";
const BROKER_CONNECTS_TOTAL: &str = "broker_connects_total";
const BROKER_DISCONNECTS_TOTAL: &str = "broker_disconnects_total";
const BROKER_UP: &str = "broker_up";
const BROKER_TX_ERRORS_TOTAL: &str = "broker_tx_errors_total";
const BROKER_RTT_AVG_SECONDS: &str = "broker_rtt_avg_seconds";
const BROKER_RTT_P99_SECONDS: &str = "broker_rtt_p99_seconds";
const BROKER_THROTTLE_AVG_SECONDS: &str = "broker_throttle_avg_seconds";
const BROKER_THROTTLE_P99_SECONDS: &str = "broker_throttle_p99_seconds";
const FETCH_QUEUE_MESSAGES: &str = "fetch_queue_messages";
const FETCH_QUEUE_BYTES: &str = "fetch_queue_bytes";
const REPLY_QUEUE_DEPTH: &str = "reply_queue_depth";
const GROUP_REBALANCES_TOTAL: &str = "group_rebalances_total";
const GROUP_ASSIGNMENT_SIZE: &str = "group_assignment_size";
const GROUP_HEALTHY: &str = "group_healthy";
const PARTITION_FETCH_QUEUE_MESSAGES: &str = "partition_fetch_queue_messages";
const PARTITION_LAG_STORED_RECORDS: &str = "partition_lag_stored_records";
const PARTITION_NOT_FETCHING: &str = "partition_not_fetching";
const L_BROKER: &str = "broker";
const L_PARTITION: &str = "partition";

/// The librdkafka fetch state in which a partition is being consumed. The
/// others in `rd_kafka_fetch_states[]` are `none`, `stopping`, `stopped`,
/// `offset-query`, `offset-wait` and `validate-epoch-wait`.
const FETCH_STATE_ACTIVE: &str = "active";

/// Windows a held partition must go without fetching before the run is
/// reported. One window is ordinary right after an assignment, while
/// librdkafka resolves the partition's offset.
const NOT_FETCHING_WARN_WINDOWS: u32 = 2;

/// Handles for the librdkafka statistics families. Owned by `KafkaSource`
/// and only touched from the controller thread, so the lazy maps need no
/// locking.
#[derive(Debug)]
pub(crate) struct KafkaStatsMetrics {
    meter: Meter,
    per_partition_detail: bool,
    tx_requests: Counter,
    tx_bytes: Counter,
    rx_responses: Counter,
    rx_bytes: Counter,
    rx_messages: Counter,
    rx_message_bytes: Counter,
    broker_tx_retries: Counter,
    broker_req_timeouts: Counter,
    broker_connects: Counter,
    broker_disconnects: Counter,
    fetch_queue_messages: Gauge,
    fetch_queue_bytes: Gauge,
    reply_queue_depth: Gauge,
    group_rebalances: Counter,
    group_assignment_size: Gauge,
    group_healthy: Gauge,
    brokers: HashMap<String, BrokerHandles>,
    partitions: HashMap<i32, PartitionHandles>,
    not_fetching: HashMap<i32, NotFetching>,
}

/// Per-broker series, labeled `broker="<host:port/id>"`, bounded by
/// cluster topology. The window gauges register lazily on the first
/// non-empty window: an eagerly-registered gauge renders `0`, which for a
/// latency reads as "no latency" rather than "no data".
#[derive(Debug)]
struct BrokerHandles {
    up: Gauge,
    tx_errors: Counter,
    rtt: Option<WindowGauges>,
    throttle: Option<WindowGauges>,
}

#[derive(Debug)]
struct WindowGauges {
    avg: Gauge,
    p99: Gauge,
}

impl WindowGauges {
    fn new(meter: &Meter, broker: &str, avg_name: &str, p99_name: &str) -> Self {
        WindowGauges {
            avg: meter.gauge(avg_name, &[(L_BROKER, broker.to_owned().into())]),
            p99: meter.gauge(p99_name, &[(L_BROKER, broker.to_owned().into())]),
        }
    }

    fn set(&self, avg_secs: f64, p99_secs: f64) {
        self.avg.set(avg_secs);
        self.p99.set(p99_secs);
    }
}

impl BrokerHandles {
    fn new(meter: &Meter, broker: &str) -> Self {
        BrokerHandles {
            up: meter.gauge(BROKER_UP, &[(L_BROKER, broker.to_owned().into())]),
            tx_errors: meter.counter(
                BROKER_TX_ERRORS_TOTAL,
                &[(L_BROKER, broker.to_owned().into())],
            ),
            rtt: None,
            throttle: None,
        }
    }
}

/// Per-partition series, gated by `metrics.per_partition_detail`. The lag
/// gauge registers lazily on the first known value: librdkafka reports `-1`
/// while lag is unknown (e.g. right after assignment), and rendering `0`
/// there would mask real lag. The not-fetching gauge registers lazily on the
/// first window in which this member holds the partition, so a partition
/// another member holds registers nothing at all.
#[derive(Debug)]
struct PartitionHandles {
    fetch_queue_messages: Gauge,
    lag_stored: Option<Gauge>,
    not_fetching: Option<Gauge>,
}

impl PartitionHandles {
    fn new(meter: &Meter, partition: i32) -> Self {
        PartitionHandles {
            fetch_queue_messages: meter.gauge(
                PARTITION_FETCH_QUEUE_MESSAGES,
                &[(L_PARTITION, partition.to_string().into())],
            ),
            lag_stored: None,
            not_fetching: None,
        }
    }
}

/// A run of consecutive statistics windows in which a partition this member
/// holds reported a fetch state other than `active`.
#[derive(Debug)]
struct NotFetching {
    /// Windows the run has lasted, counting the current one.
    windows: u32,
    /// The states already reported in this run, at most the six non-active
    /// ones. `offset-query` and `stopped` are different diagnoses and each
    /// earns a line, and a state already reported earns nothing.
    reported: Vec<String>,
}

impl KafkaStatsMetrics {
    /// Resolve all fixed handles. Build-time only (called from `open`).
    pub(crate) fn new(meter: Meter, per_partition_detail: bool) -> Self {
        KafkaStatsMetrics {
            tx_requests: meter.counter(TX_REQUESTS_TOTAL, &[]),
            tx_bytes: meter.counter(TX_BYTES_TOTAL, &[]),
            rx_responses: meter.counter(RX_RESPONSES_TOTAL, &[]),
            rx_bytes: meter.counter(RX_BYTES_TOTAL, &[]),
            rx_messages: meter.counter(RX_MESSAGES_TOTAL, &[]),
            rx_message_bytes: meter.counter(RX_MESSAGE_BYTES_TOTAL, &[]),
            broker_tx_retries: meter.counter(BROKER_TX_RETRIES_TOTAL, &[]),
            broker_req_timeouts: meter.counter(BROKER_REQ_TIMEOUTS_TOTAL, &[]),
            broker_connects: meter.counter(BROKER_CONNECTS_TOTAL, &[]),
            broker_disconnects: meter.counter(BROKER_DISCONNECTS_TOTAL, &[]),
            fetch_queue_messages: meter.gauge(FETCH_QUEUE_MESSAGES, &[]),
            fetch_queue_bytes: meter.gauge(FETCH_QUEUE_BYTES, &[]),
            reply_queue_depth: meter.gauge(REPLY_QUEUE_DEPTH, &[]),
            group_rebalances: meter.counter(GROUP_REBALANCES_TOTAL, &[]),
            group_assignment_size: meter.gauge(GROUP_ASSIGNMENT_SIZE, &[]),
            group_healthy: meter.gauge(GROUP_HEALTHY, &[]),
            brokers: HashMap::new(),
            partitions: HashMap::new(),
            not_fetching: HashMap::new(),
            meter,
            per_partition_detail,
        }
    }

    /// Translate one statistics snapshot. Controller thread only.
    ///
    /// `owned` is the member's live assignment. The snapshot carries every
    /// partition the client holds metadata for, and a partition another member
    /// holds reports no fetching of its own, so the series and the log line
    /// that describe fetching are restricted to `owned`.
    pub(crate) fn update(&mut self, stats: &Statistics, topic: &str, owned: &[PartitionId]) {
        self.tx_requests.absolute(to_u64(stats.tx));
        self.tx_bytes.absolute(to_u64(stats.tx_bytes));
        self.rx_responses.absolute(to_u64(stats.rx));
        self.rx_bytes.absolute(to_u64(stats.rx_bytes));
        self.rx_messages.absolute(to_u64(stats.rxmsgs));
        self.rx_message_bytes.absolute(to_u64(stats.rxmsg_bytes));
        self.reply_queue_depth.set(to_u64(stats.replyq) as f64);

        self.update_brokers(stats);
        self.update_partitions(stats, topic, owned);

        if let Some(cgrp) = &stats.cgrp {
            self.group_rebalances.absolute(to_u64(cgrp.rebalance_cnt));
            self.group_assignment_size
                .set(f64::from(cgrp.assignment_size.max(0)));
            // Boolean health rather than a state-labeled family: the state
            // string sets are librdkafka-version-dependent and would mint
            // unbounded label values.
            let healthy = cgrp.state == "up" && cgrp.join_state == "steady";
            self.group_healthy.set(if healthy { 1.0 } else { 0.0 });
            if !healthy {
                tracing::debug!(
                    state = %cgrp.state,
                    join_state = %cgrp.join_state,
                    reason = %cgrp.rebalance_reason,
                    "consumer group not settled"
                );
            }
        }
    }

    fn update_brokers(&mut self, stats: &Statistics) {
        let mut retries: u64 = 0;
        let mut timeouts: u64 = 0;
        let mut connects: u64 = 0;
        let mut disconnects: u64 = 0;
        let mut seen: HashSet<&str> = HashSet::new();
        let meter = &self.meter;
        // `logical` entries (the group coordinator) are separate librdkafka
        // connections that mirror an underlying broker. The join key is the
        // resolved `nodename` (host:port); a logical entry reports
        // `nodeid: -1` even once bound. Logical entries are excluded from
        // every sum and from per-broker series (they would double-count),
        // but they are real connections to that broker: `broker_up` reports
        // a broker as up if any connection to it, regular or logical, is
        // up. A broker whose only live link is the coordinator connection
        // is connected, not down.
        let logical_up: HashSet<&str> = stats
            .brokers
            .values()
            .filter(|b| b.source == "logical" && !b.nodename.is_empty() && b.state == "UP")
            .map(|b| b.nodename.as_str())
            .collect();
        for broker in stats.brokers.values() {
            // `internal` is the `:0/internal` pseudo-broker; `logical`
            // entries fold into `broker_up` above and count nowhere else.
            if broker.source == "internal" || broker.source == "logical" {
                continue;
            }
            retries += broker.txretries;
            timeouts += broker.req_timeouts;
            connects += to_u64(broker.connects.unwrap_or(0));
            disconnects += to_u64(broker.disconnects.unwrap_or(0));

            // Per-broker series only once the entry resolves to a real
            // broker id, so unresolved bootstrap placeholders don't mint
            // short-lived `/-1` series.
            if broker.nodeid < 0 {
                continue;
            }
            seen.insert(broker.name.as_str());
            let handles = self
                .brokers
                .entry(broker.name.clone())
                .or_insert_with(|| BrokerHandles::new(meter, &broker.name));
            let up = broker.state == "UP" || logical_up.contains(broker.nodename.as_str());
            handles.up.set(if up { 1.0 } else { 0.0 });
            handles.tx_errors.absolute(broker.txerrs);
            // Publish window estimates only when the window sampled
            // anything (see `BrokerHandles`).
            if let Some(rtt) = broker.rtt.as_ref().filter(|w| w.cnt > 0) {
                handles
                    .rtt
                    .get_or_insert_with(|| {
                        WindowGauges::new(
                            meter,
                            &broker.name,
                            BROKER_RTT_AVG_SECONDS,
                            BROKER_RTT_P99_SECONDS,
                        )
                    })
                    .set(us_to_secs(rtt.avg), us_to_secs(rtt.p99));
            }
            if let Some(throttle) = broker.throttle.as_ref().filter(|w| w.cnt > 0) {
                handles
                    .throttle
                    .get_or_insert_with(|| {
                        WindowGauges::new(
                            meter,
                            &broker.name,
                            BROKER_THROTTLE_AVG_SECONDS,
                            BROKER_THROTTLE_P99_SECONDS,
                        )
                    })
                    .set(ms_to_secs(throttle.avg), ms_to_secs(throttle.p99));
            }
        }
        // Summed across brokers (no label): monotonic within a consumer
        // lifetime because broker entries are never removed.
        self.broker_tx_retries.absolute(retries);
        self.broker_req_timeouts.absolute(timeouts);
        self.broker_connects.absolute(connects);
        self.broker_disconnects.absolute(disconnects);
        // Stop updating series for brokers that left the snapshot; the
        // exporter keeps rendering the last value until its idle timeout.
        self.brokers.retain(|name, _| seen.contains(name.as_str()));
    }

    fn update_partitions(&mut self, stats: &Statistics, topic: &str, owned: &[PartitionId]) {
        let mut fetchq_msgs: u64 = 0;
        let mut fetchq_bytes: u64 = 0;
        let mut seen: HashSet<i32> = HashSet::new();
        let meter = &self.meter;
        if let Some(t) = stats.topics.get(topic) {
            for (pid, p) in &t.partitions {
                // Partition -1 is librdkafka's internal UnAssigned partition.
                if *pid < 0 {
                    continue;
                }
                fetchq_msgs += to_u64(p.fetchq_cnt);
                fetchq_bytes += p.fetchq_size;
                let held = is_owned(owned, *pid);
                if self.per_partition_detail {
                    seen.insert(*pid);
                    let handles = self
                        .partitions
                        .entry(*pid)
                        .or_insert_with(|| PartitionHandles::new(meter, *pid));
                    handles
                        .fetch_queue_messages
                        .set(to_u64(p.fetchq_cnt) as f64);
                    if p.consumer_lag_stored >= 0 {
                        handles
                            .lag_stored
                            .get_or_insert_with(|| {
                                meter.gauge(
                                    PARTITION_LAG_STORED_RECORDS,
                                    &[(L_PARTITION, pid.to_string().into())],
                                )
                            })
                            .set(p.consumer_lag_stored as f64);
                    }
                    if held {
                        handles
                            .not_fetching
                            .get_or_insert_with(|| {
                                meter.gauge(
                                    PARTITION_NOT_FETCHING,
                                    &[(L_PARTITION, pid.to_string().into())],
                                )
                            })
                            .set(if p.fetch_state == FETCH_STATE_ACTIVE {
                                0.0
                            } else {
                                1.0
                            });
                    } else if let Some(gauge) = handles.not_fetching.as_ref() {
                        // A partition the member has lost reads 0. The
                        // exporter has no deletion and no idle timeout, so a
                        // series left unwritten renders the value the last
                        // window that held it recorded, for the life of the
                        // process. Absence is "never held", 0 is "held once,
                        // not now".
                        gauge.set(0.0);
                    }
                }
                // Ungated by `per_partition_detail`, so a deployment that
                // leaves the cardinality knob at its default still gets the
                // log line.
                if held {
                    track_fetch_state(&mut self.not_fetching, *pid, p);
                }
            }
        }
        self.fetch_queue_messages.set(fetchq_msgs as f64);
        self.fetch_queue_bytes.set(fetchq_bytes as f64);
        if self.per_partition_detail {
            self.partitions.retain(|pid, _| seen.contains(pid));
        }
        self.not_fetching.retain(|pid, _| is_owned(owned, *pid));
    }
}

/// Whether this member holds `pid`. The statistics snapshot carries every
/// partition in the topic's metadata (`rd_kafka_stats_emit_all` walks the
/// whole partition count), so in a group of several members the partitions
/// the others hold arrive here reporting `none` or `stopped`.
fn is_owned(owned: &[PartitionId], pid: i32) -> bool {
    u32::try_from(pid).is_ok_and(|p| owned.contains(&PartitionId(p)))
}

/// Follow one held partition's fetch state across statistics windows, and
/// report a run of windows in which it is not fetching.
///
/// A partition paused by backpressure keeps reporting `active`.
/// `rd_kafka_toppar_pause_resume` sets a pause flag, rewinds the fetch
/// position and purges the fetch queue without assigning `rktp_fetch_state`,
/// so a run of non-active windows means the partition is not being consumed.
///
/// The run is reported once it reaches `NOT_FETCHING_WARN_WINDOWS`, once more
/// for each further state it goes through, and closed by an info line naming
/// how long it lasted. A state already reported in the run is not reported
/// again, which bounds a run at six lines however long it lasts: librdkafka
/// retries a failed offset lookup by moving the partition between
/// `offset-query` and `offset-wait`, so consecutive windows sample different
/// non-active states for one stuck partition. A run that never reached the
/// threshold ends silently.
///
/// The state names the cause. `stopped` and `none` are a partition this
/// process stopped fetching, and `offset-query` and `offset-wait` are a
/// leader or an offset lookup that has not answered.
///
/// Free function taking the run map so a unit test drives the state machine
/// window by window, the same reason `publish_lag` is one.
fn track_fetch_state(runs: &mut HashMap<i32, NotFetching>, pid: i32, p: &Partition) {
    if p.fetch_state == FETCH_STATE_ACTIVE {
        if let Some(run) = runs.remove(&pid)
            && !run.reported.is_empty()
        {
            tracing::info!(
                partition = pid,
                windows = run.windows,
                "assigned partition resumed fetching"
            );
        }
        return;
    }
    let run = runs.entry(pid).or_insert_with(|| NotFetching {
        windows: 0,
        reported: Vec::new(),
    });
    run.windows += 1;
    if run.windows < NOT_FETCHING_WARN_WINDOWS || run.reported.contains(&p.fetch_state) {
        return;
    }
    run.reported.push(p.fetch_state.clone());
    tracing::warn!(
        partition = pid,
        state = %p.fetch_state,
        windows = run.windows,
        next_offset = p.next_offset,
        query_offset = p.query_offset,
        hi_offset = p.hi_offset,
        "assigned partition is not fetching"
    );
}

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn us_to_secs(v: i64) -> f64 {
    v as f64 / 1e6
}

fn ms_to_secs(v: i64) -> f64 {
    v as f64 / 1e3
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::statistics::{Broker, ConsumerGroup, Topic, Window};
    use std::sync::{Arc, Mutex};

    /// Run `f` against a local Prometheus recorder and return the rendered
    /// exposition. Handles must be resolved inside `f`.
    fn render(f: impl FnOnce()) -> String {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.run_upkeep();
        handle.render()
    }

    /// A test Meter under the `kafka` namespace: names render as
    /// `spate_kafka_<local>` (the runtime's role-scoped variant would be
    /// `spate_kafka_source_<local>`; the translation is identical).
    fn meter() -> Meter {
        Meter::with_namespace("kafka", "orders", "orders_in", "kafka")
    }

    const STD: &str = r#"pipeline="orders",component="orders_in",component_type="kafka""#;

    /// An assignment, as `KafkaSource::retained_partition_ids` reports it.
    fn owned(partitions: &[u32]) -> Vec<PartitionId> {
        partitions.iter().copied().map(PartitionId).collect()
    }

    /// Everything the subscriber installed for one test has formatted.
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Capture;

        fn make_writer(&'a self) -> Capture {
            self.clone()
        }
    }

    /// Run `f` under a subscriber at `info`, the level a deployment runs at,
    /// and return the lines it formatted. The subscriber is thread-local
    /// (`with_default`) rather than the process-wide `init()`, because
    /// `cargo test` shares one process across a binary.
    fn capture_logs(f: impl FnOnce()) -> Vec<String> {
        let capture = Capture(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::INFO)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8_lossy(&capture.0.lock().expect("capture"))
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// Lines carrying `needle`, so an assertion counts what was reported
    /// rather than testing for the absence of a phrase: a formatter that
    /// stopped rendering fields this way produces that absence too.
    fn lines_with<'a>(lines: &'a [String], needle: &str) -> Vec<&'a str> {
        lines
            .iter()
            .filter(|l| l.contains(needle))
            .map(String::as_str)
            .collect()
    }

    const NOT_FETCHING: &str = "assigned partition is not fetching";
    const RESUMED: &str = "assigned partition resumed fetching";

    /// A snapshot in which each partition reports the given fetch state. The
    /// offsets are the ones the warning carries, fixed so a test can read
    /// them back.
    fn stats_with_states(parts: &[(i32, &str)]) -> Statistics {
        Statistics {
            topics: HashMap::from([(
                "orders".to_owned(),
                Topic {
                    topic: "orders".to_owned(),
                    partitions: parts
                        .iter()
                        .map(|&(pid, state)| {
                            (
                                pid,
                                Partition {
                                    partition: pid,
                                    fetch_state: state.to_owned(),
                                    next_offset: 4_242,
                                    query_offset: -1,
                                    hi_offset: 1_250_000,
                                    consumer_lag_stored: -1,
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    fn broker(name: &str, source: &str, nodeid: i32) -> Broker {
        Broker {
            name: name.to_owned(),
            nodename: name
                .trim_end_matches(|c: char| c == '/' || c.is_ascii_digit())
                .to_owned(),
            source: source.to_owned(),
            nodeid,
            state: "UP".to_owned(),
            ..Default::default()
        }
    }

    fn window(avg: i64, p99: i64, cnt: i64) -> Window {
        Window {
            avg,
            p99,
            cnt,
            ..Default::default()
        }
    }

    #[test]
    fn transport_counters_render_absolute_totals() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let stats = Statistics {
                tx: 31,
                tx_bytes: 4_096,
                rx: 30,
                rx_bytes: 2_048,
                rxmsgs: 500,
                rxmsg_bytes: 65_536,
                replyq: 3,
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        for needle in [
            &format!("spate_kafka_tx_requests_total{{{STD}}} 31") as &str,
            &format!("spate_kafka_tx_bytes_total{{{STD}}} 4096"),
            &format!("spate_kafka_rx_responses_total{{{STD}}} 30"),
            &format!("spate_kafka_rx_bytes_total{{{STD}}} 2048"),
            &format!("spate_kafka_rx_messages_total{{{STD}}} 500"),
            &format!("spate_kafka_rx_message_bytes_total{{{STD}}} 65536"),
            &format!("spate_kafka_reply_queue_depth{{{STD}}} 3"),
        ] {
            assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
        }
    }

    #[test]
    fn window_units_convert_to_seconds() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let mut b = broker("k1:9092/1", "learned", 1);
            b.rtt = Some(window(1_500, 3_000, 10)); // microseconds
            b.throttle = Some(window(250, 500, 10)); // milliseconds
            let stats = Statistics {
                brokers: HashMap::from([(b.name.clone(), b)]),
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        let label = format!(r#"{STD},broker="k1:9092/1""#);
        for needle in [
            &format!("spate_kafka_broker_rtt_avg_seconds{{{label}}} 0.0015") as &str,
            &format!("spate_kafka_broker_rtt_p99_seconds{{{label}}} 0.003"),
            &format!("spate_kafka_broker_throttle_avg_seconds{{{label}}} 0.25"),
            &format!("spate_kafka_broker_throttle_p99_seconds{{{label}}} 0.5"),
        ] {
            assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
        }
    }

    #[test]
    fn empty_windows_and_missing_cgrp_publish_nothing() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let mut b = broker("k1:9092/1", "learned", 1);
            b.rtt = Some(window(0, 0, 0)); // sampled nothing
            b.throttle = None;
            let stats = Statistics {
                brokers: HashMap::from([(b.name.clone(), b)]),
                cgrp: None,
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(
            !rendered.contains("rtt_avg"),
            "empty window published:\n{rendered}"
        );
        assert!(!rendered.contains("rtt_p99"));
        assert!(!rendered.contains("throttle_avg"));
        assert!(!rendered.contains("throttle_p99"));
        // The `group_*` handles are fixed (registered in `new`), so with no
        // cgrp they render at their unset default of 0; assert only that
        // health was never set to 1.
        assert!(!rendered.contains(&format!("spate_kafka_group_healthy{{{STD}}} 1")));
    }

    #[test]
    fn internal_logical_and_bootstrap_brokers_are_filtered() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let mut learned = broker("k1:9092/1", "learned", 1);
            learned.txretries = 5;
            learned.txerrs = 2;
            let mut configured_bootstrap = broker("seed:9092/-1", "configured", -1);
            configured_bootstrap.txretries = 7; // counts in sums, no per-broker series
            let mut internal = broker(":0/internal", "internal", -1);
            internal.txretries = 100; // excluded everywhere
            let mut logical = broker("GroupCoordinator", "logical", 1);
            logical.txretries = 50; // excluded everywhere
            let stats = Statistics {
                brokers: HashMap::from([
                    (learned.name.clone(), learned),
                    (configured_bootstrap.name.clone(), configured_bootstrap),
                    (internal.name.clone(), internal),
                    (logical.name.clone(), logical),
                ]),
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(
            rendered.contains(&format!("spate_kafka_broker_tx_retries_total{{{STD}}} 12")),
            "sum should be learned(5) + configured(7):\n{rendered}"
        );
        assert!(rendered.contains(r#"broker="k1:9092/1""#));
        assert!(
            !rendered.contains(r#"broker="seed:9092/-1""#),
            "bootstrap placeholder minted a series"
        );
        assert!(!rendered.contains("internal"));
        assert!(!rendered.contains("GroupCoordinator"));
    }

    /// The regression #195 pins: a broker whose only live connection is the
    /// group-coordinator logical link reads as up. Sparse connections mean
    /// librdkafka never reopens a regular link it has no fetch-reason for,
    /// so after a coordinator-only outage the real entry sits DOWN (or
    /// INIT) for the process lifetime while the coordinator link it mirrors
    /// is healthy. The mirror joins by resolved `nodename`; its `nodeid`
    /// reads -1 even once bound (observed against a live client).
    #[test]
    fn a_coordinator_only_broker_counts_as_up() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            // Real link down, coordinator link up: up.
            let mut coordinator_only = broker("k1:9092/1", "learned", 1);
            coordinator_only.nodename = "k1:9092".to_owned();
            coordinator_only.state = "DOWN".to_owned();
            coordinator_only.txretries = 5;
            let mut coord_link = broker("GroupCoordinator", "logical", -1);
            coord_link.nodename = "k1:9092".to_owned();
            coord_link.txretries = 50; // logical entries still count in no sum
            // Both links down: down.
            let mut dark = broker("k2:9092/2", "learned", 2);
            dark.nodename = "k2:9092".to_owned();
            dark.state = "DOWN".to_owned();
            let mut dark_link = broker("TxnCoordinator", "logical", -1);
            dark_link.nodename = "k2:9092".to_owned();
            dark_link.state = "DOWN".to_owned();
            // A logical entry mirroring no real entry mints no series.
            let mut orphan_link = broker("OrphanCoordinator", "logical", -1);
            orphan_link.nodename = "k9:9092".to_owned();
            let stats = Statistics {
                brokers: HashMap::from([
                    (coordinator_only.name.clone(), coordinator_only),
                    (coord_link.name.clone(), coord_link),
                    (dark.name.clone(), dark),
                    (dark_link.name.clone(), dark_link),
                    (orphan_link.name.clone(), orphan_link),
                ]),
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(
            rendered.contains(&format!(
                r#"spate_kafka_broker_up{{{STD},broker="k1:9092/1"}} 1"#
            )),
            "coordinator-only broker must read as up:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!(
                r#"spate_kafka_broker_up{{{STD},broker="k2:9092/2"}} 0"#
            )),
            "a broker with every link down stays down:\n{rendered}"
        );
        assert!(
            !rendered.contains("Coordinator"),
            "logical entries must mint no series of their own:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!("spate_kafka_broker_tx_retries_total{{{STD}}} 5")),
            "logical entries stay out of the transport sums:\n{rendered}"
        );
    }

    #[test]
    fn group_health_truth_table() {
        let cases = [
            ("up", "steady", " 1"),
            ("up", "wait-join", " 0"),
            ("query-coord", "steady", " 0"),
        ];
        for (state, join_state, expect) in cases {
            let rendered = render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), false);
                let stats = Statistics {
                    cgrp: Some(ConsumerGroup {
                        state: state.to_owned(),
                        join_state: join_state.to_owned(),
                        rebalance_cnt: 4,
                        assignment_size: 8,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                m.update(&stats, "orders", &owned(&[0, 1]));
            });
            let needle = format!("spate_kafka_group_healthy{{{STD}}}{expect}");
            assert!(
                rendered.contains(&needle),
                "({state},{join_state}): missing `{needle}`:\n{rendered}"
            );
            assert!(rendered.contains(&format!("spate_kafka_group_rebalances_total{{{STD}}} 4")));
            assert!(rendered.contains(&format!("spate_kafka_group_assignment_size{{{STD}}} 8")));
        }
    }

    fn topic_with_partitions(topic: &str, parts: &[(i32, i64, u64, i64)]) -> Topic {
        Topic {
            topic: topic.to_owned(),
            partitions: parts
                .iter()
                .map(|&(pid, fetchq_cnt, fetchq_size, lag_stored)| {
                    (
                        pid,
                        Partition {
                            partition: pid,
                            fetchq_cnt,
                            fetchq_size,
                            consumer_lag_stored: lag_stored,
                            fetch_state: FETCH_STATE_ACTIVE.to_owned(),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn fetch_queue_aggregates_and_partition_detail_gating() {
        // Detail off: aggregate only, no partition label.
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let stats = Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    topic_with_partitions(
                        "orders",
                        &[(0, 10, 1_000, 5), (1, 20, 2_000, 7), (-1, 99, 9_999, 0)],
                    ),
                )]),
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(rendered.contains(&format!("spate_kafka_fetch_queue_messages{{{STD}}} 30")));
        assert!(rendered.contains(&format!("spate_kafka_fetch_queue_bytes{{{STD}}} 3000")));
        assert!(
            !rendered.contains("partition="),
            "detail off must not mint partition series:\n{rendered}"
        );

        // Detail on: per-partition series, pid -1 skipped, negative lag skipped.
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), true);
            let stats = Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    topic_with_partitions(
                        "orders",
                        &[(0, 10, 1_000, 5), (1, 20, 2_000, -1), (-1, 99, 9_999, 0)],
                    ),
                )]),
                ..Default::default()
            };
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_fetch_queue_messages{{{STD},partition="0"}} 10"#
        )));
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_lag_stored_records{{{STD},partition="0"}} 5"#
        )));
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_fetch_queue_messages{{{STD},partition="1"}} 20"#
        )));
        assert!(!rendered.contains(r#"partition="-1""#));
        // Partition 1's lag was -1 (unknown): the lag gauge registers lazily
        // on the first known value, so no series exists. An unknown lag must
        // not render as `0`.
        assert!(!rendered.contains(&format!(
            r#"spate_kafka_partition_lag_stored_records{{{STD},partition="1"}}"#
        )));
    }

    #[test]
    fn revoked_partitions_stop_updating_after_retain() {
        let mut m_holder: Option<KafkaStatsMetrics> = None;
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), true);
            let two = Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    topic_with_partitions("orders", &[(0, 10, 0, 0), (1, 20, 0, 0)]),
                )]),
                ..Default::default()
            };
            m.update(&two, "orders", &owned(&[0, 1]));
            let one = Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    topic_with_partitions("orders", &[(0, 11, 0, 0)]),
                )]),
                ..Default::default()
            };
            m.update(&one, "orders", &owned(&[0]));
            m_holder = Some(m);
        });
        let m = m_holder.unwrap();
        assert!(m.partitions.contains_key(&0));
        assert!(
            !m.partitions.contains_key(&1),
            "retain must drop the revoked partition"
        );
        // The exporter still renders partition 1's last value until its idle
        // timeout; only the handle is dropped.
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_fetch_queue_messages{{{STD},partition="0"}} 11"#
        )));
    }

    #[test]
    fn not_fetching_gauge_follows_the_fetch_state() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), true);
            let stats = stats_with_states(&[
                (0, "active"),
                (1, "offset-query"),
                (2, "stopped"),
                (-1, "none"),
            ]);
            m.update(&stats, "orders", &owned(&[0, 1]));
        });
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_not_fetching{{{STD},partition="0"}} 0"#
        )));
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_not_fetching{{{STD},partition="1"}} 1"#
        )));
        // Partition 2 is in the snapshot because librdkafka emits every
        // partition in the topic's metadata, and it is another member's. A
        // series for it would read as a stall in a healthy group.
        assert!(
            !rendered.contains(&format!(
                r#"spate_kafka_partition_not_fetching{{{STD},partition="2"}}"#
            )),
            "a partition another member holds minted a fetching series:\n{rendered}"
        );
        assert!(!rendered.contains(r#"partition="-1""#));
        // The ownership filter is scoped to the not-fetching series. The two
        // per-partition series beside it keep publishing for every partition
        // in the snapshot, which is what they do today.
        assert!(rendered.contains(&format!(
            r#"spate_kafka_partition_fetch_queue_messages{{{STD},partition="2"}}"#
        )));
    }

    #[test]
    fn a_revoked_partition_reads_zero() {
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), true);
            m.update(
                &stats_with_states(&[(0, "stopped")]),
                "orders",
                &owned(&[0]),
            );
            // The rebalance hands partition 0 to another member, which
            // fetches it. The series has to stop reading 1 here: the exporter
            // renders the last value written for the life of the process.
            m.update(&stats_with_states(&[(0, "active")]), "orders", &[]);
        });
        assert!(
            rendered.contains(&format!(
                r#"spate_kafka_partition_not_fetching{{{STD},partition="0"}} 0"#
            )),
            "a revoked partition kept reading as parked:\n{rendered}"
        );
    }

    #[test]
    fn each_state_of_one_episode_is_reported_once() {
        // librdkafka retries a failed offset lookup by moving the partition
        // between `offset-query` and `offset-wait`, so consecutive windows
        // sample different non-active states for one stuck partition.
        let lines = capture_logs(|| {
            render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), true);
                let held = owned(&[0]);
                for window in 0..10 {
                    let state = if window % 2 == 0 {
                        "offset-query"
                    } else {
                        "offset-wait"
                    };
                    m.update(&stats_with_states(&[(0, state)]), "orders", &held);
                }
            });
        });
        let warned = lines_with(&lines, NOT_FETCHING);
        assert_eq!(
            warned.len(),
            2,
            "expected one line per state, got:\n{}",
            lines.join("\n")
        );
        assert!(warned[0].contains("state=offset-wait"), "{}", warned[0]);
        assert!(warned[1].contains("state=offset-query"), "{}", warned[1]);
    }

    #[test]
    fn a_parked_partition_is_reported_once_per_episode() {
        let lines = capture_logs(|| {
            render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), true);
                let held = owned(&[0]);
                // Two windows stopped: the run crosses the threshold and is
                // reported once. Five more at the same state add nothing.
                for _ in 0..7 {
                    m.update(&stats_with_states(&[(0, "stopped")]), "orders", &held);
                }
                // A different non-active state is a different diagnosis.
                for _ in 0..2 {
                    m.update(&stats_with_states(&[(0, "offset-query")]), "orders", &held);
                }
                m.update(&stats_with_states(&[(0, "active")]), "orders", &held);
            });
        });
        let warned = lines_with(&lines, NOT_FETCHING);
        assert_eq!(
            warned.len(),
            2,
            "expected one line per state, got:\n{}",
            lines.join("\n")
        );
        assert!(warned[0].contains("state=stopped"), "{}", warned[0]);
        assert!(warned[0].contains("windows=2"), "{}", warned[0]);
        assert!(warned[0].contains("next_offset=4242"), "{}", warned[0]);
        assert!(warned[0].contains("query_offset=-1"), "{}", warned[0]);
        assert!(warned[0].contains("hi_offset=1250000"), "{}", warned[0]);
        assert!(warned[1].contains("state=offset-query"), "{}", warned[1]);
        assert!(warned[1].contains("windows=8"), "{}", warned[1]);

        let resumed = lines_with(&lines, RESUMED);
        assert_eq!(resumed.len(), 1, "{}", lines.join("\n"));
        assert!(resumed[0].contains("windows=9"), "{}", resumed[0]);
    }

    #[test]
    fn a_single_non_active_window_is_not_reported() {
        let lines = capture_logs(|| {
            render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), true);
                let held = owned(&[0]);
                // One window resolving an offset is ordinary right after an
                // assignment, and the recovery closes a run nobody was told
                // about.
                m.update(&stats_with_states(&[(0, "offset-query")]), "orders", &held);
                m.update(&stats_with_states(&[(0, "active")]), "orders", &held);
            });
        });
        assert!(lines.is_empty(), "{}", lines.join("\n"));
    }

    #[test]
    fn a_partition_another_member_holds_is_not_reported() {
        let lines = capture_logs(|| {
            render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), true);
                for _ in 0..5 {
                    m.update(
                        &stats_with_states(&[(0, "active"), (1, "stopped")]),
                        "orders",
                        &owned(&[0]),
                    );
                }
            });
        });
        assert!(lines.is_empty(), "{}", lines.join("\n"));
    }

    #[test]
    fn partition_detail_off_still_reports_a_parked_partition() {
        // The gauge is the alertable half and the log line is the diagnostic
        // half. `per_partition_detail` defaults to off, so a deployment that
        // never set it still has to be told.
        let mut rendered = String::new();
        let lines = capture_logs(|| {
            rendered = render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), false);
                let held = owned(&[0]);
                for _ in 0..2 {
                    m.update(&stats_with_states(&[(0, "stopped")]), "orders", &held);
                }
            });
        });
        assert!(
            !rendered.contains("partition="),
            "detail off must not mint partition series:\n{rendered}"
        );
        let warned = lines_with(&lines, NOT_FETCHING);
        assert_eq!(warned.len(), 1, "{}", lines.join("\n"));
        assert!(warned[0].contains("partition=0"), "{}", warned[0]);
    }

    #[test]
    fn a_revoked_partition_closes_its_run_silently() {
        let lines = capture_logs(|| {
            render(|| {
                let mut m = KafkaStatsMetrics::new(meter(), true);
                for _ in 0..2 {
                    m.update(
                        &stats_with_states(&[(0, "stopped")]),
                        "orders",
                        &owned(&[0]),
                    );
                }
                // The rebalance that took the partition is logged where it
                // happens; a resume line here would claim it started fetching.
                m.update(&stats_with_states(&[(0, "stopped")]), "orders", &[]);
                m.update(&stats_with_states(&[(0, "active")]), "orders", &[]);
            });
        });
        assert_eq!(lines_with(&lines, NOT_FETCHING).len(), 1);
        assert!(
            lines_with(&lines, RESUMED).is_empty(),
            "{}",
            lines.join("\n")
        );
    }

    #[test]
    fn absolute_counters_hold_the_high_water_mark_on_regression() {
        // Documents the fetch-max contract: a regressing upstream total
        // would flat-line, not dip. See the module docs.
        let rendered = render(|| {
            let mut m = KafkaStatsMetrics::new(meter(), false);
            let high = Statistics {
                rxmsgs: 100,
                ..Default::default()
            };
            m.update(&high, "orders", &[]);
            let regressed = Statistics {
                rxmsgs: 40,
                ..Default::default()
            };
            m.update(&regressed, "orders", &[]);
        });
        assert!(rendered.contains(&format!("spate_kafka_rx_messages_total{{{STD}}} 100")));
    }
}
