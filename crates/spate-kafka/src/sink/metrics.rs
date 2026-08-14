//! librdkafka producer statistics → `spate_kafka_sink_*` metric families.
//!
//! [`KafkaSinkStatsMetrics`] translates the periodic
//! [`Statistics`] snapshot into the connector-owned families [the metrics
//! reference] documents under Kafka sink. Unlike the source (whose snapshots
//! are drained on the controller thread), the sink has no control-plane tick:
//! the producer's `ClientContext::stats` callback publishes directly from
//! the producer's poll thread, once per statistics interval and never on
//! the record path, through the shared slot the writer's `attach_metrics`
//! fills (see the sink context module).
//!
//! # Counter identity
//!
//! As on the source side, librdkafka reports cumulative totals; they are
//! mirrored through [`Counter::absolute`] (fetch-max: idempotent under
//! duplicate delivery, PromQL `rate()` works natively). The mapping is
//! sound only while totals are monotonic, i.e. scoped to a single
//! producer client. The sink guarantees this by construction: `build()`
//! creates exactly one producer per sink instance, shared by every shard.
//! If in-process producer recreation is ever introduced, switch to delta
//! accumulation (see the source metrics module for the full rationale).
//!
//! # Windows
//!
//! The latency gauges expose librdkafka's rolling-window estimates over
//! the last statistics interval: `int_latency` (time in the producer
//! queue), `outbuf_latency` (time in the transmit queue), and broker
//! round-trip time, all reported in microseconds and converted to seconds.
//! They are per-broker sampled quantiles and **cannot be aggregated**
//! across brokers or processes (`max()` is the only defensible
//! cross-series operator). Windows that sampled nothing publish no series
//! (a `0` would read as "no latency" rather than "no data").
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

use rdkafka::statistics::Statistics;
use spate_core::metrics::{Counter, Gauge, Meter};
use std::collections::{HashMap, HashSet};

const TX_REQUESTS_TOTAL: &str = "tx_requests_total";
const TX_BYTES_TOTAL: &str = "tx_bytes_total";
const RX_RESPONSES_TOTAL: &str = "rx_responses_total";
const RX_BYTES_TOTAL: &str = "rx_bytes_total";
const TX_MESSAGES_TOTAL: &str = "tx_messages_total";
const TX_MESSAGE_BYTES_TOTAL: &str = "tx_message_bytes_total";
const PRODUCE_QUEUE_MESSAGES: &str = "produce_queue_messages";
const PRODUCE_QUEUE_BYTES: &str = "produce_queue_bytes";
const BROKER_TX_RETRIES_TOTAL: &str = "broker_tx_retries_total";
const BROKER_REQ_TIMEOUTS_TOTAL: &str = "broker_req_timeouts_total";
const BROKER_UP: &str = "broker_up";
const BROKER_TX_ERRORS_TOTAL: &str = "broker_tx_errors_total";
const BROKER_RTT_AVG_SECONDS: &str = "broker_rtt_avg_seconds";
const BROKER_RTT_P99_SECONDS: &str = "broker_rtt_p99_seconds";
const BROKER_INT_LATENCY_AVG_SECONDS: &str = "broker_int_latency_avg_seconds";
const BROKER_INT_LATENCY_P99_SECONDS: &str = "broker_int_latency_p99_seconds";
const BROKER_OUTBUF_LATENCY_AVG_SECONDS: &str = "broker_outbuf_latency_avg_seconds";
const BROKER_OUTBUF_LATENCY_P99_SECONDS: &str = "broker_outbuf_latency_p99_seconds";
const L_BROKER: &str = "broker";

/// Handles for the producer statistics families. Owned by the producer's
/// context behind a mutex; only the poll thread updates them.
#[derive(Debug)]
pub(crate) struct KafkaSinkStatsMetrics {
    meter: Meter,
    tx_requests: Counter,
    tx_bytes: Counter,
    rx_responses: Counter,
    rx_bytes: Counter,
    tx_messages: Counter,
    tx_message_bytes: Counter,
    produce_queue_messages: Gauge,
    produce_queue_bytes: Gauge,
    broker_tx_retries: Counter,
    broker_req_timeouts: Counter,
    brokers: HashMap<String, BrokerHandles>,
}

/// Per-broker series, labeled `broker="<host:port/id>"`, bounded by
/// cluster topology. Window gauges register lazily on the first non-empty
/// window (see the module docs).
#[derive(Debug)]
struct BrokerHandles {
    up: Gauge,
    tx_errors: Counter,
    rtt: Option<WindowGauges>,
    int_latency: Option<WindowGauges>,
    outbuf_latency: Option<WindowGauges>,
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
            int_latency: None,
            outbuf_latency: None,
        }
    }
}

impl KafkaSinkStatsMetrics {
    /// Resolve all fixed handles. Called once from `attach_metrics`.
    pub(crate) fn new(meter: Meter) -> Self {
        KafkaSinkStatsMetrics {
            tx_requests: meter.counter(TX_REQUESTS_TOTAL, &[]),
            tx_bytes: meter.counter(TX_BYTES_TOTAL, &[]),
            rx_responses: meter.counter(RX_RESPONSES_TOTAL, &[]),
            rx_bytes: meter.counter(RX_BYTES_TOTAL, &[]),
            tx_messages: meter.counter(TX_MESSAGES_TOTAL, &[]),
            tx_message_bytes: meter.counter(TX_MESSAGE_BYTES_TOTAL, &[]),
            produce_queue_messages: meter.gauge(PRODUCE_QUEUE_MESSAGES, &[]),
            produce_queue_bytes: meter.gauge(PRODUCE_QUEUE_BYTES, &[]),
            broker_tx_retries: meter.counter(BROKER_TX_RETRIES_TOTAL, &[]),
            broker_req_timeouts: meter.counter(BROKER_REQ_TIMEOUTS_TOTAL, &[]),
            brokers: HashMap::new(),
            meter,
        }
    }

    /// Translate one statistics snapshot. Poll thread only; not on the
    /// record path.
    pub(crate) fn update(&mut self, stats: &Statistics) {
        self.tx_requests.absolute(to_u64(stats.tx));
        self.tx_bytes.absolute(to_u64(stats.tx_bytes));
        self.rx_responses.absolute(to_u64(stats.rx));
        self.rx_bytes.absolute(to_u64(stats.rx_bytes));
        self.tx_messages.absolute(to_u64(stats.txmsgs));
        self.tx_message_bytes.absolute(to_u64(stats.txmsg_bytes));
        self.produce_queue_messages.set(stats.msg_cnt as f64);
        self.produce_queue_bytes.set(stats.msg_size as f64);

        let mut retries: u64 = 0;
        let mut timeouts: u64 = 0;
        let mut seen: HashSet<&str> = HashSet::new();
        let meter = &self.meter;
        // Same contract as the source: `logical` entries (coordinators) are
        // excluded from every sum and per-broker series, but a broker is up
        // if any connection to it, regular or logical, is up. The join
        // key is the resolved `nodename` (a logical entry's `nodeid` reads
        // -1 even once bound).
        let logical_up: HashSet<&str> = stats
            .brokers
            .values()
            .filter(|b| b.source == "logical" && !b.nodename.is_empty() && b.state == "UP")
            .map(|b| b.nodename.as_str())
            .collect();
        for broker in stats.brokers.values() {
            // Same filters as the source: `internal` is the `:0/internal`
            // pseudo-broker; `logical` entries fold into `broker_up` above
            // and count nowhere else.
            if broker.source == "internal" || broker.source == "logical" {
                continue;
            }
            retries += broker.txretries;
            timeouts += broker.req_timeouts;

            // Per-broker series only once the entry resolves to a real
            // broker id (no short-lived `/-1` bootstrap series).
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
            for (window, slot, avg_name, p99_name) in [
                (
                    &broker.rtt,
                    &mut handles.rtt,
                    BROKER_RTT_AVG_SECONDS,
                    BROKER_RTT_P99_SECONDS,
                ),
                (
                    &broker.int_latency,
                    &mut handles.int_latency,
                    BROKER_INT_LATENCY_AVG_SECONDS,
                    BROKER_INT_LATENCY_P99_SECONDS,
                ),
                (
                    &broker.outbuf_latency,
                    &mut handles.outbuf_latency,
                    BROKER_OUTBUF_LATENCY_AVG_SECONDS,
                    BROKER_OUTBUF_LATENCY_P99_SECONDS,
                ),
            ] {
                if let Some(w) = window.as_ref().filter(|w| w.cnt > 0) {
                    slot.get_or_insert_with(|| {
                        WindowGauges::new(meter, &broker.name, avg_name, p99_name)
                    })
                    .set(us_to_secs(w.avg), us_to_secs(w.p99));
                }
            }
        }
        self.broker_tx_retries.absolute(retries);
        self.broker_req_timeouts.absolute(timeouts);
        // Stop updating series for brokers that left the snapshot; the
        // exporter keeps rendering the last value until its idle timeout.
        self.brokers.retain(|name, _| seen.contains(name.as_str()));
    }
}

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn us_to_secs(v: i64) -> f64 {
    v as f64 / 1e6
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::statistics::{Broker, Window};

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
    /// `spate_kafka_sink_<local>`; the translation is identical).
    fn meter() -> Meter {
        Meter::with_namespace("kafka", "orders", "orders_out", "kafka")
    }

    const STD: &str = r#"pipeline="orders",component="orders_out",component_type="kafka""#;

    fn broker(name: &str, source: &str, nodeid: i32) -> Broker {
        Broker {
            name: name.to_owned(),
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
    fn producer_totals_and_queue_gauges_render() {
        let rendered = render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
            let stats = Statistics {
                tx: 42,
                tx_bytes: 8_192,
                rx: 41,
                rx_bytes: 1_024,
                txmsgs: 900,
                txmsg_bytes: 131_072,
                msg_cnt: 17,
                msg_size: 4_096,
                ..Default::default()
            };
            m.update(&stats);
        });
        for needle in [
            &format!("spate_kafka_tx_requests_total{{{STD}}} 42") as &str,
            &format!("spate_kafka_tx_bytes_total{{{STD}}} 8192"),
            &format!("spate_kafka_rx_responses_total{{{STD}}} 41"),
            &format!("spate_kafka_rx_bytes_total{{{STD}}} 1024"),
            &format!("spate_kafka_tx_messages_total{{{STD}}} 900"),
            &format!("spate_kafka_tx_message_bytes_total{{{STD}}} 131072"),
            &format!("spate_kafka_produce_queue_messages{{{STD}}} 17"),
            &format!("spate_kafka_produce_queue_bytes{{{STD}}} 4096"),
        ] {
            assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
        }
    }

    #[test]
    fn broker_latency_windows_convert_and_gate_on_cnt() {
        let rendered = render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
            let mut b = broker("k1:9092/1", "learned", 1);
            b.rtt = Some(window(1_500, 3_000, 10)); // microseconds
            b.int_latency = Some(window(2_000, 9_000, 25)); // microseconds
            b.outbuf_latency = Some(window(0, 0, 0)); // sampled nothing
            let stats = Statistics {
                brokers: HashMap::from([(b.name.clone(), b)]),
                ..Default::default()
            };
            m.update(&stats);
        });
        let label = format!(r#"{STD},broker="k1:9092/1""#);
        for needle in [
            &format!("spate_kafka_broker_rtt_avg_seconds{{{label}}} 0.0015") as &str,
            &format!("spate_kafka_broker_rtt_p99_seconds{{{label}}} 0.003"),
            &format!("spate_kafka_broker_int_latency_avg_seconds{{{label}}} 0.002"),
            &format!("spate_kafka_broker_int_latency_p99_seconds{{{label}}} 0.009"),
            &format!("spate_kafka_broker_up{{{label}}} 1"),
        ] {
            assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
        }
        assert!(
            !rendered.contains("outbuf_latency"),
            "empty window published:\n{rendered}"
        );
    }

    #[test]
    fn internal_logical_and_bootstrap_brokers_are_filtered() {
        let rendered = render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
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
            m.update(&stats);
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

    /// Mirror of the source-side regression for #195: a broker whose only
    /// live connection is a logical (coordinator) link reads as up, one with
    /// every link down stays down, and logical entries mint no series and
    /// join no sums.
    #[test]
    fn a_coordinator_only_broker_counts_as_up() {
        let rendered = render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
            let mut coordinator_only = broker("k1:9092/1", "learned", 1);
            coordinator_only.nodename = "k1:9092".to_owned();
            coordinator_only.state = "DOWN".to_owned();
            coordinator_only.txretries = 5;
            let mut coord_link = broker("TxnCoordinator", "logical", -1);
            coord_link.nodename = "k1:9092".to_owned();
            coord_link.txretries = 50;
            let mut dark = broker("k2:9092/2", "learned", 2);
            dark.nodename = "k2:9092".to_owned();
            dark.state = "DOWN".to_owned();
            let mut dark_link = broker("GroupCoordinator", "logical", -1);
            dark_link.nodename = "k2:9092".to_owned();
            dark_link.state = "DOWN".to_owned();
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
            m.update(&stats);
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
    fn departed_brokers_stop_updating_after_retain() {
        let mut m_holder: Option<KafkaSinkStatsMetrics> = None;
        render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
            let two = Statistics {
                brokers: HashMap::from([
                    ("k1:9092/1".to_owned(), broker("k1:9092/1", "learned", 1)),
                    ("k2:9092/2".to_owned(), broker("k2:9092/2", "learned", 2)),
                ]),
                ..Default::default()
            };
            m.update(&two);
            let one = Statistics {
                brokers: HashMap::from([(
                    "k1:9092/1".to_owned(),
                    broker("k1:9092/1", "learned", 1),
                )]),
                ..Default::default()
            };
            m.update(&one);
            m_holder = Some(m);
        });
        let m = m_holder.unwrap();
        assert!(m.brokers.contains_key("k1:9092/1"));
        assert!(
            !m.brokers.contains_key("k2:9092/2"),
            "retain must drop the departed broker"
        );
    }

    #[test]
    fn absolute_counters_hold_the_high_water_mark_on_regression() {
        // Documents the fetch-max contract (see the module docs): a
        // regressing upstream total (impossible with one producer per
        // sink) would flat-line, not dip.
        let rendered = render(|| {
            let mut m = KafkaSinkStatsMetrics::new(meter());
            m.update(&Statistics {
                txmsgs: 100,
                ..Default::default()
            });
            m.update(&Statistics {
                txmsgs: 40,
                ..Default::default()
            });
        });
        assert!(rendered.contains(&format!("spate_kafka_tx_messages_total{{{STD}}} 100")));
    }
}
