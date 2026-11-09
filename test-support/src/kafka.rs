//! A Kafka broker in a container, chosen by `SPATE_KAFKA_LANE`: the
//! listeners a client reaches it on, and the settings a suite asks of it.
//!
//! Each lane under `ci/kafka/` is a broker, and each broker has an arm in
//! [`Broker`] holding how it starts.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{Image, TestcontainersError};

use crate::{container_image, selected_lane};

/// The container port of the listener a client on the host reaches.
pub const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9092);

/// The broker address another container reaches from inside the broker's
/// network namespace: the listener the broker advertises to other containers.
pub const BROKER_FROM_INSIDE: &str = "localhost:9093";

/// The service whose lanes are brokers.
const SERVICE: &str = "kafka";

/// Where the container's command waits for [`KafkaBroker::start_script`]. The
/// advertised port is only known once the container runs, so the broker starts
/// after the script is written.
const START_SCRIPT: &str = "/tmp/spate_broker_start.sh";

/// The line Apache Kafka logs to stdout once it serves clients.
const APACHE_READY: &str = "Kafka Server started";

/// The line Redpanda logs to stderr once it serves clients.
const REDPANDA_READY: &str = "Successfully started Redpanda!";

/// A broker a Kafka lane runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Broker {
    /// Apache Kafka, in KRaft mode as one combined broker and controller.
    Apache,
    /// Redpanda, one node in dev-container mode.
    Redpanda,
}

impl Broker {
    /// The broker a lane directory under `ci/kafka/` names.
    ///
    /// # Errors
    ///
    /// Fails, naming the lane, when no broker has that lane.
    pub fn from_lane(lane: &str) -> Result<Broker, String> {
        match lane {
            "apache" => Ok(Broker::Apache),
            "redpanda" => Ok(Broker::Redpanda),
            other => Err(format!(
                "no broker runs Kafka lane `{other}`; each ci/kafka/ lane needs its arm in `Broker`"
            )),
        }
    }

    /// The broker the selected lane runs: the one `SPATE_KAFKA_LANE` names, or
    /// the primary lane.
    ///
    /// # Errors
    ///
    /// Fails as [`Broker::from_lane`] does.
    ///
    /// # Panics
    ///
    /// Panics as [`selected_lane`] does.
    pub fn selected() -> Result<Broker, String> {
        Broker::from_lane(&selected_lane(SERVICE))
    }

    /// The lane directory this broker runs under `ci/kafka/`.
    #[must_use]
    pub fn lane(self) -> &'static str {
        match self {
            Broker::Apache => "apache",
            Broker::Redpanda => "redpanda",
        }
    }
}

impl fmt::Display for Broker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Broker::Apache => "Apache Kafka",
            Broker::Redpanda => "Redpanda",
        })
    }
}

/// One broker on the image the selected lane pins, as a testcontainers
/// [`Image`]. A client on the host reaches it on [`KAFKA_PORT`], which the
/// image exposes itself, and another container in its network namespace on
/// [`BROKER_FROM_INSIDE`].
///
/// On Apache Kafka, `ImageExt::with_env_var` on the request sets broker
/// configuration over the defaults here, as the image's own environment does.
/// Redpanda reads no configuration from its environment, so a setting both
/// brokers need is a method here.
#[derive(Clone, Debug)]
pub struct KafkaBroker {
    broker: Broker,
    name: String,
    tag: String,
    env: BTreeMap<String, String>,
    advertised: Option<SocketAddr>,
}

impl KafkaBroker {
    /// The broker the selected lane runs, on its image pulled by digest.
    ///
    /// # Panics
    ///
    /// Panics when the selected lane names no broker, and as
    /// [`container_image`] does.
    #[must_use]
    pub fn selected() -> KafkaBroker {
        let broker = Broker::selected().unwrap_or_else(|e| panic!("{e}"));
        let (name, tag) = container_image(&["--pull", SERVICE]);
        KafkaBroker::new(broker, name, tag)
    }

    fn new(broker: Broker, name: String, tag: String) -> KafkaBroker {
        let env = match broker {
            Broker::Apache => apache_env(),
            Broker::Redpanda => BTreeMap::new(),
        };
        KafkaBroker {
            broker,
            name,
            tag,
            env,
            advertised: None,
        }
    }

    /// The broker this image runs.
    #[must_use]
    pub fn broker(&self) -> Broker {
        self.broker
    }

    /// Able to run transactions on its one node. Apache Kafka's transaction
    /// state log asks for three replicas by default, so this puts it on one.
    /// Redpanda runs them on one node as it starts.
    #[must_use]
    pub fn transactional(mut self) -> KafkaBroker {
        if self.broker == Broker::Apache {
            for key in [
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
            ] {
                self.env.insert(key.to_owned(), "1".to_owned());
            }
        }
        self
    }

    /// With no delay before a new group's first rebalance. Redpanda's
    /// dev-container mode starts with none.
    #[must_use]
    pub fn without_rebalance_delay(mut self) -> KafkaBroker {
        if self.broker == Broker::Apache {
            self.env.insert(
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_owned(),
                "0".to_owned(),
            );
        }
        self
    }

    /// Advertising `address` to clients in place of the host port mapped to
    /// [`KAFKA_PORT`], so a client reaches the broker through whatever listens
    /// there.
    #[must_use]
    pub fn advertised_at(mut self, address: SocketAddr) -> KafkaBroker {
        self.advertised = Some(address);
        self
    }

    /// The script that starts the broker once `host_port` is mapped to
    /// [`KAFKA_PORT`].
    fn start_script(&self, host_port: u16) -> String {
        let client = self
            .advertised
            .map_or_else(|| format!("127.0.0.1:{host_port}"), |a| a.to_string());
        match self.broker {
            Broker::Apache => format!(
                "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{client},BROKER://{BROKER_FROM_INSIDE}\n\
                 exec /etc/kafka/docker/run\n"
            ),
            // Dev-container mode raises a topic's `segment.ms` to at least ten
            // minutes and runs compaction every ten seconds. The compaction
            // suites roll a segment within the test and wait for the cleaner.
            Broker::Redpanda => format!(
                "echo log_segment_ms_min: 100 > /etc/redpanda/.bootstrap.yaml\n\
                 echo log_compaction_interval_ms: 500 >> /etc/redpanda/.bootstrap.yaml\n\
                 exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
                 --kafka-addr PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093 \
                 --advertise-kafka-addr PLAINTEXT://{client},BROKER://{BROKER_FROM_INSIDE}\n"
            ),
        }
    }

    fn ready_condition(&self) -> WaitFor {
        match self.broker {
            Broker::Apache => WaitFor::message_on_stdout(APACHE_READY),
            Broker::Redpanda => WaitFor::message_on_stderr(REDPANDA_READY),
        }
    }
}

/// One KRaft node acting as broker and controller, with a `PLAINTEXT`
/// listener for the host, a `BROKER` listener for other containers, and
/// internal topics on one replica.
fn apache_env() -> BTreeMap<String, String> {
    [
        (
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
        ),
        ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw"),
        ("KAFKA_PROCESS_ROLES", "broker,controller"),
        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
        (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        ),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
        ("KAFKA_BROKER_ID", "1"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
        ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect()
}

impl Image for KafkaBroker {
    fn name(&self) -> &str {
        &self.name
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::new()
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        [
            "-c".to_owned(),
            format!("while [ ! -f {START_SCRIPT} ]; do sleep 0.1; done; exec bash {START_SCRIPT}"),
        ]
    }

    fn exec_after_start(
        &self,
        cs: ContainerState,
    ) -> Result<Vec<ExecCommand>, TestcontainersError> {
        let script = self.start_script(cs.host_port_ipv4(KAFKA_PORT)?);
        // Written whole and then renamed, so the waiting command never runs a
        // partial script. The script holds no single quote, which would end the
        // quoting here.
        let write = format!(
            "printf '%s' '{script}' > {START_SCRIPT}.part && mv {START_SCRIPT}.part {START_SCRIPT}"
        );
        Ok(vec![
            ExecCommand::new(["sh".to_owned(), "-c".to_owned(), write])
                .with_container_ready_conditions(vec![self.ready_condition()]),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apache() -> KafkaBroker {
        KafkaBroker::new(Broker::Apache, "apache/kafka-native".into(), "1".into())
    }

    fn redpanda() -> KafkaBroker {
        KafkaBroker::new(Broker::Redpanda, "redpandadata/redpanda".into(), "1".into())
    }

    const REDPANDA_START: &str = "echo log_segment_ms_min: 100 > /etc/redpanda/.bootstrap.yaml\n\
         echo log_compaction_interval_ms: 500 >> /etc/redpanda/.bootstrap.yaml\n\
         exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
         --kafka-addr PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093 \
         --advertise-kafka-addr PLAINTEXT://ADDRESS,BROKER://localhost:9093\n";

    /// A client on the host is advertised the port mapped to [`KAFKA_PORT`],
    /// and a container in the namespace the inside listener.
    #[test]
    fn the_start_script_advertises_the_mapped_port_and_the_inside_listener() {
        assert_eq!(
            apache().start_script(31092),
            "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:31092,BROKER://localhost:9093\n\
             exec /etc/kafka/docker/run\n"
        );
    }

    #[test]
    fn an_advertised_address_replaces_the_mapped_port() {
        let proxy = SocketAddr::from(([127, 0, 0, 1], 40001));
        assert_eq!(
            apache().advertised_at(proxy).start_script(31092),
            "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:40001,BROKER://localhost:9093\n\
             exec /etc/kafka/docker/run\n"
        );
    }

    /// Redpanda advertises the same two listeners, after lowering the floors
    /// the compaction suites run under.
    #[test]
    fn redpanda_advertises_the_same_listeners_after_its_bootstrap() {
        assert_eq!(
            redpanda().start_script(31092),
            REDPANDA_START.replace("ADDRESS", "127.0.0.1:31092")
        );
        let proxy = SocketAddr::from(([127, 0, 0, 1], 40001));
        assert_eq!(
            redpanda().advertised_at(proxy).start_script(31092),
            REDPANDA_START.replace("ADDRESS", "127.0.0.1:40001")
        );
    }

    /// `exec_after_start` writes the script inside single quotes.
    #[test]
    fn no_start_script_holds_a_single_quote() {
        for lane in crate::lane_dirs(SERVICE) {
            let broker = Broker::from_lane(&lane).unwrap();
            let script = KafkaBroker::new(broker, "image".into(), "1".into()).start_script(31092);
            assert!(!script.contains('\''), "{lane}: {script}");
        }
    }

    /// Each broker is waited on for the stream it logs its ready line to.
    #[test]
    fn each_broker_is_ready_on_its_own_stream() {
        let debug = |w: WaitFor| format!("{w:?}");
        assert_eq!(
            debug(apache().ready_condition()),
            debug(WaitFor::message_on_stdout(APACHE_READY))
        );
        assert_eq!(
            debug(redpanda().ready_condition()),
            debug(WaitFor::message_on_stderr(REDPANDA_READY))
        );
    }

    #[test]
    fn a_transactional_broker_keeps_its_transaction_state_on_one_replica() {
        let env = apache().transactional().env;
        assert_eq!(env["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"], "1");
        assert_eq!(env["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"], "1");
        assert!(
            !apache()
                .env
                .contains_key("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR")
        );
    }

    #[test]
    fn a_broker_without_a_rebalance_delay_sets_it_to_zero_on_apache() {
        let env = apache().without_rebalance_delay().env;
        assert_eq!(env["KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"], "0");
        assert!(
            !apache()
                .env
                .contains_key("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS")
        );
    }

    /// Redpanda takes no `KAFKA_*` variable, so neither option sets one.
    #[test]
    fn redpanda_takes_no_environment() {
        assert!(
            redpanda()
                .transactional()
                .without_rebalance_delay()
                .env
                .is_empty()
        );
    }

    #[test]
    fn an_unknown_kafka_lane_is_rejected_naming_it() {
        let err = Broker::from_lane("nope").unwrap_err();
        assert!(err.contains("`nope`"), "{err}");
    }

    /// A lane added under `ci/kafka/` without its startup code fails here,
    /// without Docker.
    #[test]
    fn every_kafka_lane_names_a_known_broker() {
        let lanes = crate::lane_dirs(SERVICE);
        assert!(!lanes.is_empty(), "no lanes under ci/kafka/");
        for lane in lanes {
            assert_eq!(Broker::from_lane(&lane).unwrap().lane(), lane);
        }
    }

    /// A failed start names the broker.
    #[test]
    fn a_broker_displays_its_product_name() {
        assert_eq!(Broker::Apache.to_string(), "Apache Kafka");
        assert_eq!(Broker::Redpanda.to_string(), "Redpanda");
    }
}
