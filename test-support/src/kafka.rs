//! A Kafka broker in a container, chosen by `SPATE_KAFKA_LANE`: the
//! listeners a client reaches it on, and the settings a suite asks of it.
//! [`KafkaCluster`] runs three of them as one cluster.
//!
//! Each lane under `ci/kafka/` is a broker, and each broker has an arm in
//! [`Broker`] holding how it starts, alone or as a cluster node.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::net::{SocketAddr, TcpListener};

use testcontainers::core::{CmdWaitFor, ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, Image, ImageExt, TestcontainersError};

use crate::{container_image, selected_lane};

/// The container port of the listener a client on the host reaches.
pub const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9092);

/// The broker address another container reaches from inside the broker's
/// network namespace: the listener the broker advertises to other containers.
pub const BROKER_FROM_INSIDE: &str = "localhost:9093";

/// The container port Apache Kafka's KRaft controllers reach each other on.
const CONTROLLER_PORT: u16 = 9094;

/// The container port Redpanda nodes reach each other on.
const REDPANDA_RPC_PORT: u16 = 33145;

/// How many nodes a [`KafkaCluster`] runs. Node ids run from 1 to this.
pub const CLUSTER_NODES: i32 = 3;

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
    /// Apache Kafka in KRaft mode, each node a combined broker and controller.
    Apache,
    /// Redpanda in dev-container mode.
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
    member: Option<Member>,
}

/// A broker's place in a [`KafkaCluster`].
#[derive(Clone, Debug)]
struct Member {
    network: String,
    node: i32,
}

impl Member {
    /// The container name, which other nodes resolve on the cluster network.
    fn host(&self) -> String {
        node_host(&self.network, self.node)
    }

    /// Each node's address on `port`, in node order.
    fn peers(&self, port: u16) -> impl Iterator<Item = (i32, String)> + '_ {
        (1..=CLUSTER_NODES)
            .map(move |node| (node, format!("{}:{port}", node_host(&self.network, node))))
    }
}

fn node_host(network: &str, node: i32) -> String {
    format!("{network}-k{node}")
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
            member: None,
        }
    }

    /// Node `node` of a [`KafkaCluster`] on `network`. Apache Kafka's internal
    /// topics go on all three nodes, and an `acks=all` write needs two in
    /// sync. Redpanda keeps its internal topics on three replicas by default.
    fn member(mut self, network: &str, node: i32) -> KafkaBroker {
        let member = Member {
            network: network.to_owned(),
            node,
        };
        if self.broker == Broker::Apache {
            let voters = member
                .peers(CONTROLLER_PORT)
                .map(|(id, address)| format!("{id}@{address}"))
                .collect::<Vec<_>>()
                .join(",");
            for (key, value) in [
                ("KAFKA_BROKER_ID", node.to_string()),
                ("KAFKA_CONTROLLER_QUORUM_VOTERS", voters),
                ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3".to_owned()),
                (
                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                    "3".to_owned(),
                ),
                ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2".to_owned()),
                ("KAFKA_MIN_INSYNC_REPLICAS", "2".to_owned()),
                ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_owned()),
            ] {
                self.env.insert(key.to_owned(), value);
            }
        }
        self.member = Some(member);
        self
    }

    /// The listener address another container reaches this broker on. A
    /// cluster node is reached by its container name.
    fn inside(&self) -> String {
        self.member.as_ref().map_or_else(
            || BROKER_FROM_INSIDE.to_owned(),
            |m| BROKER_FROM_INSIDE.replacen("localhost", &m.host(), 1),
        )
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
        let inside = self.inside();
        match self.broker {
            Broker::Apache => format!(
                "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{client},BROKER://{inside}\n\
                 exec /etc/kafka/docker/run\n"
            ),
            Broker::Redpanda => {
                // Redpanda numbers nodes from 0 when none is set, and a
                // cluster's callers name nodes by the ids in its metadata.
                let (node_id, cluster) = self.member.as_ref().map_or_else(
                    || (String::new(), String::new()),
                    |m| {
                        let seeds = m
                            .peers(REDPANDA_RPC_PORT)
                            .map(|(_, address)| address)
                            .collect::<Vec<_>>()
                            .join(",");
                        (
                            format!("rpk redpanda config set redpanda.node_id {} && ", m.node),
                            format!(
                                "--seeds {seeds} --rpc-addr 0.0.0.0:{REDPANDA_RPC_PORT} \
                                 --advertise-rpc-addr {}:{REDPANDA_RPC_PORT} ",
                                m.host()
                            ),
                        )
                    },
                );
                format!(
                    "{node_id}exec rpk redpanda start --mode dev-container --smp 1 --memory 1G \
                     --overprovisioned {cluster}\
                     --kafka-addr PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093 \
                     --advertise-kafka-addr PLAINTEXT://{client},BROKER://{inside}\n"
                )
            }
        }
    }

    /// The commands run once the broker is ready, each expected to exit 0.
    /// A cluster node runs none: a restart runs them again, before the node
    /// has rejoined.
    fn after_ready(&self) -> Vec<Vec<String>> {
        if self.member.is_some() {
            return Vec::new();
        }
        match self.broker {
            Broker::Apache => Vec::new(),
            // Dev-container mode raises a topic's `segment.ms` to at least ten
            // minutes and compacts every ten seconds, and the compaction suites
            // roll a segment and wait for the cleaner within the test. A
            // bootstrap file would replace every setting the mode applies, so
            // these are set once the node runs.
            Broker::Redpanda => vec![
                [
                    "rpk",
                    "cluster",
                    "config",
                    "set",
                    "log_segment_ms_min=100",
                    "log_compaction_interval_ms=500",
                ]
                .map(str::to_owned)
                .to_vec(),
            ],
        }
    }

    /// What the start waits for. A cluster node waits for nothing, since it
    /// cannot finish starting before the other nodes are up.
    fn ready_conditions_after_start(&self) -> Vec<WaitFor> {
        if self.member.is_some() {
            return Vec::new();
        }
        vec![match self.broker {
            Broker::Apache => WaitFor::message_on_stdout(APACHE_READY),
            Broker::Redpanda => WaitFor::message_on_stderr(REDPANDA_READY),
        }]
    }
}

/// One KRaft node acting as broker and controller, with a `PLAINTEXT`
/// listener for the host, a `BROKER` listener for other containers, and
/// internal topics on one replica.
fn apache_env() -> BTreeMap<String, String> {
    [
        (
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:{CONTROLLER_PORT}"
            ),
        ),
        ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw".to_owned()),
        ("KAFKA_PROCESS_ROLES", "broker,controller".to_owned()),
        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER".to_owned()),
        (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
        ),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER".to_owned()),
        ("KAFKA_BROKER_ID", "1".to_owned()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1".to_owned()),
        (
            "KAFKA_CONTROLLER_QUORUM_VOTERS",
            format!("1@localhost:{CONTROLLER_PORT}"),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
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
        let mut commands = vec![
            ExecCommand::new(["sh".to_owned(), "-c".to_owned(), write])
                .with_container_ready_conditions(self.ready_conditions_after_start()),
        ];
        commands.extend(self.after_ready().into_iter().map(|command| {
            ExecCommand::new(command).with_cmd_ready_condition(CmdWaitFor::exit_code(0))
        }));
        Ok(commands)
    }
}

/// Three nodes of the broker the selected lane runs, as one cluster. Node ids
/// run from 1 to [`CLUSTER_NODES`], as the cluster's metadata reports them.
///
/// Each node advertises a host port chosen before it starts. A stopped and
/// restarted node keeps its port, its id and its data, so it rejoins at the
/// address clients already hold. The nodes start with no ready condition, so
/// the caller waits until its client sees all three.
#[derive(Debug)]
pub struct KafkaCluster {
    broker: Broker,
    nodes: Vec<Container<KafkaBroker>>,
    bootstrap: String,
}

impl KafkaCluster {
    /// Starts three nodes on `network`, a Docker network the caller names and
    /// no other test uses.
    ///
    /// # Errors
    ///
    /// Fails, naming the node, when a host port cannot be chosen or a
    /// container does not start.
    ///
    /// # Panics
    ///
    /// Panics as [`KafkaBroker::selected`] does.
    pub fn start(network: &str) -> Result<KafkaCluster, String> {
        let template = KafkaBroker::selected();
        let broker = template.broker();
        let ports = free_ports()?;
        let nodes = (1..=CLUSTER_NODES)
            .zip(&ports)
            .map(|(node, &port)| {
                template
                    .clone()
                    .member(network, node)
                    .advertised_at(SocketAddr::from(([127, 0, 0, 1], port)))
                    .with_network(network)
                    .with_container_name(node_host(network, node))
                    .with_mapped_port(port, KAFKA_PORT)
                    .start()
                    .map_err(|e| format!("start {broker} node {node}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bootstrap = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        Ok(KafkaCluster {
            broker,
            nodes,
            bootstrap,
        })
    }

    /// Every node's client address, for `bootstrap.servers`.
    #[must_use]
    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    /// Stops node `node` as a rolling restart does, and returns its exit code.
    /// The two-minute timeout lets a broker under load finish its controlled
    /// shutdown, where the runtime's default could end in a kill.
    ///
    /// # Errors
    ///
    /// Fails when the runtime cannot stop the container or read its exit code.
    ///
    /// # Panics
    ///
    /// Panics when `node` is not a node id of the cluster.
    pub fn stop(&self, node: i32) -> Result<i64, String> {
        let container = self.node(node);
        container
            .stop_with_timeout(Some(120))
            .map_err(|e| format!("stop {} node {node}: {e}", self.broker))?;
        container
            .exit_code()
            .map_err(|e| format!("exit code of {} node {node}: {e}", self.broker))?
            .ok_or_else(|| format!("{} node {node} stopped with no exit code", self.broker))
    }

    /// Starts node `node` again after [`KafkaCluster::stop`].
    ///
    /// # Errors
    ///
    /// Fails when the container does not start.
    ///
    /// # Panics
    ///
    /// Panics when `node` is not a node id of the cluster.
    pub fn restart(&self, node: i32) -> Result<(), String> {
        self.node(node)
            .start()
            .map_err(|e| format!("start {} node {node}: {e}", self.broker))
    }

    /// Moves the leadership of `topic`'s `partition` to node `node`.
    ///
    /// # Errors
    ///
    /// Fails on Apache Kafka, which has no transfer a test can ask for, and
    /// when the command fails.
    ///
    /// # Panics
    ///
    /// Panics when `node` is not a node id of the cluster.
    pub fn transfer_leadership(
        &self,
        topic: &str,
        partition: i32,
        node: i32,
    ) -> Result<(), String> {
        let command = transfer_command(self.broker, topic, partition, node)?;
        self.node(node)
            .exec(ExecCommand::new(command).with_cmd_ready_condition(CmdWaitFor::exit_code(0)))
            .map(drop)
            .map_err(|e| {
                format!(
                    "move {topic}/{partition} to {} node {node}: {e}",
                    self.broker
                )
            })
    }

    fn node(&self, node: i32) -> &Container<KafkaBroker> {
        &self.nodes[node_index(node)]
    }
}

/// Where node `node` sits in [`KafkaCluster`]'s nodes.
///
/// # Panics
///
/// Panics when `node` is not a node id of the cluster.
fn node_index(node: i32) -> usize {
    (1..=CLUSTER_NODES)
        .contains(&node)
        .then(|| usize::try_from(node - 1).ok())
        .flatten()
        .unwrap_or_else(|| panic!("no node {node} in a {CLUSTER_NODES}-node cluster"))
}

/// The command a node runs to move `topic`'s `partition` to node `node`.
/// Apache Kafka moves a leader by reordering a partition's replicas and
/// electing the first, and neither its image nor rdkafka can ask for that.
fn transfer_command(
    broker: Broker,
    topic: &str,
    partition: i32,
    node: i32,
) -> Result<Vec<String>, String> {
    match broker {
        Broker::Redpanda => Ok([
            "rpk",
            "cluster",
            "partitions",
            "transfer-leadership",
            "-X",
            "admin.hosts=localhost:9644",
            "--partition",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain([format!("{topic}/{partition}:{node}")])
        .collect()),
        Broker::Apache => Err(format!(
            "{broker} has no leadership transfer a test can run"
        )),
    }
}

/// A host port per node, each free when chosen. All three are held until the
/// last is bound, so no two are the same.
fn free_ports() -> Result<Vec<u16>, String> {
    let listeners = (0..CLUSTER_NODES)
        .map(|_| TcpListener::bind("127.0.0.1:0").map_err(|e| format!("choose a host port: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    listeners
        .iter()
        .map(|l| {
            l.local_addr()
                .map(|a| a.port())
                .map_err(|e| format!("read a chosen host port: {e}"))
        })
        .collect()
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

    const REDPANDA_START: &str = "exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
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

    /// Redpanda advertises the same two listeners.
    #[test]
    fn redpanda_advertises_the_same_listeners() {
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
            let single = KafkaBroker::new(broker, "image".into(), "1".into());
            for script in [
                single.start_script(31092),
                node_two(single).start_script(31092),
            ] {
                assert!(!script.contains('\''), "{lane}: {script}");
            }
        }
    }

    /// Once ready, Redpanda lowers the floors the compaction suites run under,
    /// and Apache Kafka runs nothing.
    #[test]
    fn redpanda_lowers_its_compaction_floors_once_ready() {
        assert!(apache().after_ready().is_empty());
        assert_eq!(
            redpanda().after_ready(),
            [[
                "rpk",
                "cluster",
                "config",
                "set",
                "log_segment_ms_min=100",
                "log_compaction_interval_ms=500",
            ]]
        );
    }

    /// Each broker is waited on for the stream it logs its ready line to.
    #[test]
    fn each_broker_is_ready_on_its_own_stream() {
        let debug = |w: Vec<WaitFor>| format!("{w:?}");
        assert_eq!(
            debug(apache().ready_conditions_after_start()),
            debug(vec![WaitFor::message_on_stdout(APACHE_READY)])
        );
        assert_eq!(
            debug(redpanda().ready_conditions_after_start()),
            debug(vec![WaitFor::message_on_stderr(REDPANDA_READY)])
        );
    }

    /// Node 2 of a cluster on network `net`.
    fn node_two(broker: KafkaBroker) -> KafkaBroker {
        broker
            .member("net", 2)
            .advertised_at(SocketAddr::from(([127, 0, 0, 1], 40002)))
    }

    /// A Redpanda node takes its id, seeds every node, and advertises its host
    /// port to clients and its container name to the other nodes.
    #[test]
    fn a_redpanda_cluster_node_names_its_id_seeds_and_listeners() {
        assert_eq!(
            node_two(redpanda()).start_script(31092),
            "rpk redpanda config set redpanda.node_id 2 && \
             exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
             --seeds net-k1:33145,net-k2:33145,net-k3:33145 \
             --rpc-addr 0.0.0.0:33145 --advertise-rpc-addr net-k2:33145 \
             --kafka-addr PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093 \
             --advertise-kafka-addr PLAINTEXT://127.0.0.1:40002,BROKER://net-k2:9093\n"
        );
    }

    /// An Apache Kafka node takes its id, votes with every node, and keeps
    /// its internal topics on all three.
    #[test]
    fn an_apache_cluster_node_names_its_id_voters_and_replicas() {
        let node = node_two(apache());
        assert_eq!(
            node.start_script(31092),
            "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:40002,BROKER://net-k2:9093\n\
             exec /etc/kafka/docker/run\n"
        );
        let mut expected = apache_env();
        for (key, value) in [
            ("KAFKA_BROKER_ID", "2"),
            (
                "KAFKA_CONTROLLER_QUORUM_VOTERS",
                "1@net-k1:9094,2@net-k2:9094,3@net-k3:9094",
            ),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
            ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3"),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
            ("KAFKA_MIN_INSYNC_REPLICAS", "2"),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ] {
            expected.insert(key.to_owned(), value.to_owned());
        }
        assert_eq!(node.env, expected);
    }

    /// Redpanda reads no `KAFKA_*` variable, so a node sets none.
    #[test]
    fn a_redpanda_cluster_node_takes_no_environment() {
        assert!(node_two(redpanda()).env.is_empty());
    }

    /// A node cannot finish starting before the others are up, and a restart
    /// runs its after-ready commands again, so a node has neither.
    #[test]
    fn a_cluster_node_waits_for_nothing_and_runs_nothing_once_ready() {
        for broker in [apache(), redpanda()] {
            let node = node_two(broker);
            assert!(
                node.ready_conditions_after_start().is_empty(),
                "{}",
                node.broker
            );
            assert!(node.after_ready().is_empty(), "{}", node.broker);
        }
    }

    /// Redpanda moves a leader through rpk, and Apache Kafka refuses, naming
    /// itself.
    #[test]
    fn only_redpanda_transfers_leadership() {
        assert_eq!(
            transfer_command(Broker::Redpanda, "t", 4, 3).unwrap(),
            [
                "rpk",
                "cluster",
                "partitions",
                "transfer-leadership",
                "-X",
                "admin.hosts=localhost:9644",
                "--partition",
                "t/4:3",
            ]
        );
        let err = transfer_command(Broker::Apache, "t", 4, 3).unwrap_err();
        assert!(err.contains("Apache Kafka"), "{err}");
    }

    /// Node ids run from 1, and an id outside the cluster names no node.
    #[test]
    fn node_ids_index_the_nodes_from_one() {
        assert_eq!(node_index(1), 0);
        assert_eq!(node_index(3), 2);
        for outside in [0, 4] {
            let err = std::panic::catch_unwind(|| node_index(outside)).unwrap_err();
            let message = err.downcast_ref::<String>().unwrap();
            assert_eq!(*message, format!("no node {outside} in a 3-node cluster"));
        }
    }

    /// One Apache Kafka broker is a KRaft quorum of one, with the inside
    /// listener on 9093 and its controller on 9094.
    #[test]
    fn a_single_apache_broker_is_its_own_quorum() {
        let env = apache().env;
        assert_eq!(
            env["KAFKA_LISTENERS"],
            "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
        );
        assert_eq!(env["KAFKA_CONTROLLER_QUORUM_VOTERS"], "1@localhost:9094");
        assert_eq!(env["KAFKA_BROKER_ID"], "1");
        assert_eq!(env["KAFKA_PROCESS_ROLES"], "broker,controller");
        assert_eq!(env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"], "1");
        assert_eq!(env.len(), 9, "{env:?}");
    }

    /// Each node gets its own port, so no two nodes map the same one.
    #[test]
    fn each_cluster_node_gets_its_own_free_port() {
        let ports = free_ports().unwrap();
        assert_eq!(ports.len(), usize::try_from(CLUSTER_NODES).unwrap());
        let mut distinct = ports.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), ports.len(), "{ports:?}");
        assert!(ports.iter().all(|&p| p != 0), "{ports:?}");
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
