//! Starts under `latest` on a slow, silent or refusing group, one module per
//! behaviour, against rdkafka's in-process MockCluster and, for a silent
//! coordinator, a real broker.

#![expect(
    clippy::disallowed_methods,
    reason = "test scaffolding builds its own clients and threads"
)]

mod support;

/// A rebalance offer that names another topic must not move the group's commit
/// on the source's own topic. The offer arrives in a SyncGroup reply. A raw
/// group leader on a MockCluster writes that reply's assignment, so the client
/// receives the bytes a hostile coordinator would send.
mod another_topic {
    use crate::support::MockKafka;
    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::types::RDKafkaApiKey;
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use spate_core::config::PipelineConfig;
    use spate_core::ops::chain_owned;
    use spate_core::pipeline::{Pipeline, RuntimeOptions};
    use spate_core::sink::KeyHashRouter;
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{
        BytesPassthrough, HANG, PipelineRun, TestEncoder, capture_sink, unique_name, wait_until,
    };
    use std::collections::BTreeMap;
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    use std::sync::mpsc;
    use std::time::Duration;

    const TOPIC: &str = "orders";
    const OTHER: &str = "other";
    const GROUP: &str = "offer-names-another-topic";
    /// The group's position on `orders` partition 0. Records from here on are unread.
    const RESUME_AT: i64 = 3;
    const END: i64 = 10;

    fn str16(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(&i16::try_from(s.len()).unwrap().to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    fn bytes32(buf: &mut Vec<u8>, b: &[u8]) {
        buf.extend_from_slice(&i32::try_from(b.len()).unwrap().to_be_bytes());
        buf.extend_from_slice(b);
    }

    fn request(s: &mut TcpStream, api: i16, version: i16, body: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&api.to_be_bytes());
        b.extend_from_slice(&version.to_be_bytes());
        b.extend_from_slice(&7i32.to_be_bytes());
        str16(&mut b, "rogue");
        b.extend_from_slice(body);
        s.write_all(&i32::try_from(b.len()).unwrap().to_be_bytes())
            .unwrap();
        s.write_all(&b).unwrap();
        let mut len = [0u8; 4];
        s.read_exact(&mut len).unwrap();
        let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
        s.read_exact(&mut resp).unwrap();
        resp
    }

    struct Reader<'a>(&'a [u8]);

    impl Reader<'_> {
        fn i16(&mut self) -> i16 {
            let v = i16::from_be_bytes(self.0[..2].try_into().unwrap());
            self.0 = &self.0[2..];
            v
        }
        fn i32(&mut self) -> i32 {
            let v = i32::from_be_bytes(self.0[..4].try_into().unwrap());
            self.0 = &self.0[4..];
            v
        }
        fn string(&mut self) -> String {
            let n = usize::try_from(self.i16()).unwrap();
            let v = String::from_utf8(self.0[..n].to_vec()).unwrap();
            self.0 = &self.0[n..];
            v
        }
        fn skip_bytes(&mut self) {
            let n = usize::try_from(self.i32()).unwrap();
            self.0 = &self.0[n..];
        }
    }

    /// Join `GROUP` first, so this connection leads it, then assign every other
    /// member `other` partition 0 once `members_synced` returns. The connection
    /// stays open until `release` hears from the test or hangs up.
    fn lead_the_group(brokers: &str, members_synced: impl FnOnce(), release: &mpsc::Receiver<()>) {
        let mut s = TcpStream::connect(brokers).unwrap();
        let mut subscription = Vec::new();
        subscription.extend_from_slice(&0i16.to_be_bytes());
        subscription.extend_from_slice(&1i32.to_be_bytes());
        str16(&mut subscription, TOPIC);
        subscription.extend_from_slice(&(-1i32).to_be_bytes());

        let mut join = Vec::new();
        str16(&mut join, GROUP);
        join.extend_from_slice(&30_000i32.to_be_bytes());
        join.extend_from_slice(&30_000i32.to_be_bytes());
        str16(&mut join, "");
        str16(&mut join, "consumer");
        join.extend_from_slice(&1i32.to_be_bytes());
        str16(&mut join, "range");
        bytes32(&mut join, &subscription);
        let resp = request(&mut s, 11, 1, &join);

        let mut r = Reader(&resp[4..]);
        assert_eq!(r.i16(), 0, "JoinGroup error");
        let generation = r.i32();
        let _protocol = r.string();
        let leader = r.string();
        let me = r.string();
        assert_eq!(leader, me, "the raw member leads the group");
        let mut others = Vec::new();
        for _ in 0..r.i32() {
            let id = r.string();
            r.skip_bytes();
            if id != me {
                others.push(id);
            }
        }
        assert!(
            !others.is_empty(),
            "the source joined before the group formed"
        );

        let mut assignment = Vec::new();
        assignment.extend_from_slice(&0i16.to_be_bytes());
        assignment.extend_from_slice(&1i32.to_be_bytes());
        str16(&mut assignment, OTHER);
        assignment.extend_from_slice(&1i32.to_be_bytes());
        assignment.extend_from_slice(&0i32.to_be_bytes());
        assignment.extend_from_slice(&(-1i32).to_be_bytes());

        let mut sync = Vec::new();
        str16(&mut sync, GROUP);
        sync.extend_from_slice(&generation.to_be_bytes());
        str16(&mut sync, &me);
        sync.extend_from_slice(&i32::try_from(others.len() + 1).unwrap().to_be_bytes());
        str16(&mut sync, &me);
        bytes32(&mut sync, &[]);
        for id in &others {
            str16(&mut sync, id);
            bytes32(&mut sync, &assignment);
        }
        // The mock answers every waiting member when the leader syncs, and refuses a
        // SyncGroup that arrives after that, so the source's goes first.
        members_synced();
        let resp = request(&mut s, 14, 1, &sync);
        let mut r = Reader(&resp[4..]);
        let _throttle = r.i32();
        assert_eq!(r.i16(), 0, "SyncGroup error");
        // The connection stays open past the test's reads of the commit.
        let _ = release.recv_timeout(HANG);
        drop(s);
    }

    fn probe(brokers: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", GROUP)
            .create()
            .unwrap()
    }

    fn committed(probe: &BaseConsumer) -> Offset {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(TOPIC, 0);
        probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .unwrap()
            .elements()[0]
            .offset()
    }

    /// An offer for `other` partition 0 stops the run and leaves the group's
    /// commit on `orders` partition 0 where it was: the source read no record of
    /// `orders`.
    #[test]
    fn an_offer_naming_another_topic_leaves_the_commit_on_the_configured_topic() {
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic(TOPIC, 1, 1).unwrap();
        cluster.create_topic(OTHER, 1, 1).unwrap();
        cluster.track_requests();
        let brokers = cluster.bootstrap_servers();

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for i in 0..END {
            let payload = i.to_string();
            producer
                .send(
                    BaseRecord::<str, str>::to(TOPIC)
                        .partition(0)
                        .payload(&payload),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();

        let probe = probe(&brokers);
        let mut position = TopicPartitionList::new();
        position
            .add_partition_offset(TOPIC, 0, Offset::Offset(RESUME_AT))
            .unwrap();
        probe.commit(&position, CommitMode::Sync).unwrap();
        assert_eq!(committed(&probe), Offset::Offset(RESUME_AT));

        std::thread::scope(|scope| {
            let (release, released) = mpsc::channel();
            let (cluster, brokers) = (&cluster, &brokers);
            let leader = scope.spawn(move || {
                // No other client joins the group, so the first SyncGroup is the
                // source's.
                let source_synced = || {
                    wait_until(HANG, "the source's SyncGroup", || {
                        cluster.received(RDKafkaApiKey::SyncGroup) >= 1
                    });
                };
                lead_the_group(brokers, source_synced, &released);
            });
            run_against_the_offer(brokers, &probe);
            drop(release);
            leader
                .join()
                .expect("the raw leader completed the rebalance");
        });
    }

    /// Runs a pipeline whose group is offered `other` partition 0, and asserts
    /// it stops on the offer with the commit on `orders` where it was.
    fn run_against_the_offer(brokers: &str, probe: &BaseConsumer) {
        let name = unique_name("offer");
        let yaml = format!(
            "pipeline: {{ name: {name}, threads: 2, io_threads: 1 }}\n\
             admin: {{ listen: none }}\n\
             checkpoint: {{ drain_timeout: 200ms }}\n\
             metrics: {{ exporter: none }}\n\
             source: {{ memory: {{}} }}\n\
             sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n"
        );
        let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml).expect("config"))
            .expect("builder");
        let (sink, script) = capture_sink(1, 1);
        let mut cfg = KafkaSourceConfig::new(brokers, TOPIC, GROUP);
        cfg.startup_timeout = Duration::from_secs(60);
        cfg.statistics_interval = Duration::ZERO;
        cfg.rdkafka = BTreeMap::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("session.timeout.ms".to_string(), "30000".to_string()),
            ("heartbeat.interval.ms".to_string(), "2000".to_string()),
        ]);
        let runtime = pipeline
            .sink(sink)
            .expect("sink")
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
            .into_runtime(KafkaSource::new(cfg))
            .expect("runtime");
        let run = PipelineRun::spawn(move || runtime.run());

        let exit = run.wait_exit(HANG).expect("the run stops on the offer");
        let outcome = match &exit {
            Ok(report) => format!("{:?}", report.state),
            Err(e) => e.to_string(),
        };
        assert!(
            outcome.contains("a topic other than `orders`"),
            "the run did not stop on the offer: {outcome}"
        );
        assert!(script.writes().is_empty(), "the sink wrote nothing");
        assert_eq!(
            committed(probe),
            Offset::Offset(RESUME_AT),
            "the commit on `{TOPIC}` moved with nothing read, past records {RESUME_AT}..{END}"
        );
    }
}

/// A start that fails during a reassign must not split the reassign.
mod retried_reassign {
    use crate::support;
    use crate::support::MockKafka;
    use rdkafka::Offset;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_test::{HANG, unique_name, wait_until};
    use support::latest::{Run, config};
    use support::{committed_by, produce_to};

    const TOPIC: &str = "orders";
    const PARTITIONS: i32 = 4;

    /// One consumer of a two-consumer source gets its offer past the hold, so the
    /// other is accepted alone and then drained for a reassign whose first lookup
    /// fails. Every partition is read once the retry has run.
    #[test]
    fn a_retried_reassign_reads_every_partition() {
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster
            .create_topic(TOPIC, PARTITIONS, 1)
            .expect("create topic");
        // SAFETY: the pointer is valid while `cluster` lives. The list holds one
        // error and one round trip in the C types the variadic reads.
        unsafe {
            rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                RDKafkaApiKey::SyncGroup as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                5000 as std::ffi::c_int,
            );
        }
        let brokers = cluster.bootstrap_servers();
        let group = unique_name("split");
        let mut cfg = config(&brokers, TOPIC, &group, "latest");
        cfg.consumers = 2;
        let run = Run::start(cfg);

        // The first consumer's start commit shows it was accepted alone. The next
        // lookup is the reassign's, once the second consumer's offer arrives.
        wait_until(HANG, "the first accept", || {
            committed_by(&brokers, &group, TOPIC, PARTITIONS)
                .iter()
                .any(|o| matches!(o, Offset::Offset(_)))
        });
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );

        // Each round is one record per partition: the first run's start moved past
        // nothing here, so any partition with a reader writes its round.
        let mut next = 0;
        let mut tick = 0;
        wait_until(HANG, "a write from every partition", || {
            if tick % 200 == 0 {
                for p in 0..PARTITIONS {
                    produce_to(&brokers, TOPIC, p, next..next + 1);
                }
                next += 1;
            }
            tick += 1;
            (0..PARTITIONS).all(|p| !run.written(p).is_empty())
        });
        run.stop();
    }
}

/// A retried re-accept that comes due while a poll waits accepts the held offer
/// alone, and the runtime then drops those lanes for the next assignment.
mod late_offers {
    use crate::support;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_test::{HANG, wait_until};
    use support::latest::{self, Run};
    use support::{MockKafka, produce_to};

    const TOPIC: &str = "orders";

    /// Run two consumers on four partitions, one consumer's `SyncGroup` answered
    /// past the hold, and fail the lookups named by `offset_fetch`. Every partition
    /// must then reach the sink.
    fn every_partition_reads(group: &str, offset_fetch: &[RDKafkaRespErr]) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .try_init();
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic(TOPIC, 4, 1).expect("create topic");
        // SAFETY: the pointer is valid while `cluster` lives, and the arguments
        // match the (error, rtt in ms) pairs the variadic reads.
        unsafe {
            let err = rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                RDKafkaApiKey::SyncGroup as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                4_500i32,
            );
            assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
        }
        if !offset_fetch.is_empty() {
            cluster.request_errors(RDKafkaApiKey::OffsetFetch, offset_fetch);
        }
        let brokers = cluster.bootstrap_servers();
        let mut cfg = latest::config(&brokers, TOPIC, group, "latest");
        cfg.consumers = 2;
        let run = Run::start(cfg);
        let mut next = 1_000;
        let mut tick = 0;
        wait_until(HANG, "every partition to reach the sink", || {
            if tick % 20 == 0 {
                for p in 0..4 {
                    produce_to(&brokers, TOPIC, p, next..next + 1);
                }
                next += 1;
            }
            tick += 1;
            let unread: Vec<i32> = (0..4).filter(|p| run.written(*p).is_empty()).collect();
            unread.is_empty()
        });
        run.stop();
    }

    /// The control: with no failed lookup the late offer's re-accept covers every
    /// partition.
    #[test]
    fn a_late_offer_reads_every_partition() {
        every_partition_reads("late-offer", &[]);
    }

    /// The re-accept's first lookup fails once. Its retry must still accept every
    /// consumer's partitions together.
    #[test]
    fn a_retried_reaccept_reads_every_partition() {
        every_partition_reads(
            "late-offer-retried",
            &[
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN,
            ],
        );
    }
}

/// A start commit whose reply is slow. The group has one member: the mock refuses
/// a follower's SyncGroup that arrives after the leader's, and the rejoin holds the
/// group for most of a session timeout. The source's unit tests pin the bound that
/// several commits of one accept share.
mod slow_commits {
    use crate::support::MockKafka;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_core::checkpoint::Checkpointer;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx, SourceEvent};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{HANG, wait_until};
    use std::collections::BTreeMap;
    use std::time::Duration;

    const TOPIC: &str = "orders";

    /// `latest` starts while the reply to its start commit takes 5 s.
    #[test]
    fn latest_starts_while_its_commit_reply_is_slow() {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        // SAFETY: the pointer is valid while `cluster` lives, and the arguments
        // match the (error, rtt in ms) pair the variadic reads.
        let err = unsafe {
            rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                RDKafkaApiKey::OffsetCommit as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                5_000i32,
            )
        };
        assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
        let mut cfg = KafkaSourceConfig::new(cluster.bootstrap_servers(), TOPIC, "series");
        cfg.statistics_interval = Duration::ZERO;
        // Far past `HANG`, so a slow runner fails on the test's wait, which names it.
        cfg.startup_timeout = Duration::from_secs(300);
        cfg.rdkafka = BTreeMap::from([("auto.offset.reset".to_string(), "latest".to_string())]);
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        wait_until(HANG, "lanes", || {
            match source.poll_events(Duration::from_millis(100)) {
                Ok(SourceEvent::LanesAssigned(_)) => true,
                Err(e) if e.class() == ErrorClass::Fatal => panic!("{e}"),
                _ => false,
            }
        });
    }
}

/// `auto.offset.reset` with leading whitespace, which librdkafka reads as the
/// policy it names.
mod policy_spelling {
    use crate::support;
    use crate::support::MockKafka;
    use rdkafka::Offset;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::source::{Source, SourceCtx};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::HANG;
    use std::time::Duration;

    const TOPIC: &str = "orders";

    /// A policy spelled with leading whitespace takes the start rule of the policy
    /// librdkafka reads it as, so the partition's end is on the group.
    #[test]
    fn a_latest_policy_with_leading_whitespace_commits_its_start() {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        support::produce(&brokers, TOPIC, 3, 1, "before");
        let mut cfg = KafkaSourceConfig::new(brokers.clone(), TOPIC, "spaced");
        cfg.statistics_interval = Duration::ZERO;
        cfg.rdkafka
            .insert("auto.offset.reset".into(), " latest".into());
        cfg.validate().expect("the value loads");
        let checkpointer = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source
            .open(SourceCtx::new(checkpointer.handle()))
            .expect("librdkafka accepts the value");
        let _lanes = support::await_assignment(&mut source, HANG);
        assert_eq!(
            support::committed_by(&brokers, "spaced", TOPIC, 1),
            [Offset::Offset(3)]
        );
    }
}

/// The startup deadline's error for a start that keeps failing.
mod named_failure {
    use crate::support::MockKafka;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_core::checkpoint::Checkpointer;
    use spate_core::clock::TestClock;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{HANG, wait_until};
    use std::sync::Arc;
    use std::time::Duration;

    const TOPIC: &str = "orders";

    /// A start commit that keeps failing ahead of the first assignment ends at
    /// `startup_timeout` with an error that names the failure.
    #[test]
    fn the_startup_deadline_names_the_start_that_kept_failing() {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN; 100_000],
        );
        let mut cfg =
            KafkaSourceConfig::new(cluster.bootstrap_servers(), TOPIC, "startup-deadline");
        cfg.startup_timeout = Duration::from_secs(5);
        cfg.statistics_interval = Duration::ZERO;
        let clock = TestClock::frozen();
        let checkpointer = Checkpointer::new();
        let mut source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        source
            .open(SourceCtx::new(checkpointer.handle()))
            .expect("open");

        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            wait_until(HANG, "the failed start commit", || {
                source.poll_events(Duration::from_millis(50)).expect("poll");
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
        });

        clock.advance(Duration::from_secs(6));
        match source.poll_events(Duration::from_millis(50)) {
            Err(e) if e.class() == ErrorClass::Fatal => assert!(
                e.to_string().contains("committing a start offset"),
                "the deadline error names no start failure: {e}"
            ),
            other => panic!("expected the deadline error, got {other:?}"),
        }
    }
}

/// A permanent code while settling a start stops the run.
mod permanent_codes {
    use crate::support::MockKafka;
    use rdkafka::types::RDKafkaApiKey as Api;
    use rdkafka::types::RDKafkaRespErr::{
        RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED as GROUP_DENIED,
        RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED as TOPIC_DENIED,
    };
    use spate_core::checkpoint::Checkpointer;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx, SourceEvent};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{HANG, wait_until};
    use std::time::Duration;

    /// A code `classify_start_error` classes as fatal ends the run on each request
    /// a start needs, with no lane handed out.
    #[test]
    fn a_permanent_code_while_settling_a_start_stops_the_run() {
        for (api, code, doing) in [
            (Api::OffsetFetch, GROUP_DENIED, "committed offset"),
            (Api::ListOffsets, TOPIC_DENIED, "end offset"),
            (Api::OffsetCommit, GROUP_DENIED, "committing a start offset"),
        ] {
            let cluster = MockKafka::new();
            cluster.create_topic("orders", 1, 1).expect("create topic");
            cluster.request_errors(api, &[code; 1000]);
            let mut cfg = KafkaSourceConfig::new(cluster.bootstrap_servers(), "orders", "denied");
            cfg.statistics_interval = Duration::ZERO;
            // Out of real time's reach: the startup deadline's error names the
            // failing request too, so only the classification may end the run.
            cfg.startup_timeout = Duration::from_hours(1);
            let cp = Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source.open(SourceCtx::new(cp.handle())).expect("open");
            let mut stopped = None;
            wait_until(HANG, "the run to stop", || {
                match source.poll_events(Duration::from_millis(50)) {
                    Err(e) if e.class() == ErrorClass::Fatal => {
                        stopped = Some(e.reason().to_owned());
                        true
                    }
                    Ok(SourceEvent::LanesAssigned(_)) => panic!("lanes on {api:?}"),
                    _ => false,
                }
            });
            let reason = stopped.expect("a reason");
            assert!(reason.contains(doing), "{api:?}: {reason}");
        }
    }
}

/// A start that keeps failing before the source's first assignment ends at
/// `startup_timeout`.
mod startup_deadline {
    use crate::support::MockKafka;
    use rdkafka::types::RDKafkaApiKey as Api;
    use rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN as UNKNOWN;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::clock::TestClock;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{HANG, wait_until};
    use std::sync::Arc;
    use std::time::Duration;

    /// The first assignment's lookup fails on every attempt, and the startup
    /// deadline ends the run.
    #[test]
    fn a_start_that_keeps_failing_ends_at_the_startup_deadline() {
        let cluster = MockKafka::new();
        cluster.create_topic("orders", 1, 1).expect("create topic");
        cluster.request_errors(Api::OffsetFetch, &vec![UNKNOWN; 100_000]);
        let mut cfg = KafkaSourceConfig::new(cluster.bootstrap_servers(), "orders", "start-hold");
        cfg.statistics_interval = Duration::ZERO;
        cfg.startup_timeout = Duration::from_secs(5);
        let clock = TestClock::frozen();
        let mut source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let cp = Checkpointer::new();
        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            source.open(SourceCtx::new(cp.handle())).expect("open");
            wait_until(HANG, "a failed lookup", || {
                let _ = source.poll_events(Duration::from_millis(50));
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
        });
        clock.advance(Duration::from_secs(6));
        match source.poll_events(Duration::from_millis(50)) {
            Err(e) if e.class() == ErrorClass::Fatal => {
                assert!(e.to_string().contains("no partition assignment"), "{e}")
            }
            other => panic!("expected the startup deadline, got {other:?}"),
        }
    }
}

/// A listing slower than the accept's lookup bound, against a source that still
/// starts. The source's unit tests pin the bound the listing gets.
mod slow_listing {
    use crate::support;
    use rdkafka::types::RDKafkaApiKey as Api;
    use rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as NO_ERROR;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::source::{Source, SourceCtx, SourceEvent};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::wait_until;
    use std::time::Duration;
    use support::MockKafka;

    /// Longer than the lookup bound, and shorter than the socket timeout.
    const SLOW_MS: i32 = 20_000;
    /// A lookup that leaves part of the bound to the listing.
    const LOOKUP_MS: i32 = 5_000;

    /// A slow lookup, then three listings that each answer past the bound, and
    /// the source starts.
    #[test]
    fn a_slow_listing_is_retried_until_the_source_starts() {
        let cluster = MockKafka::new();
        cluster.create_topic("orders", 3, 1).expect("create topic");
        cluster.track_requests();
        // SAFETY: the pointer is valid while `cluster` lives. The call reads one
        // code and one round-trip time per counted entry.
        unsafe {
            let code = rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                Api::ListOffsets as i16,
                3,
                NO_ERROR,
                SLOW_MS,
                NO_ERROR,
                SLOW_MS,
                NO_ERROR,
                SLOW_MS,
            );
            assert_eq!(code, NO_ERROR);
            let code = rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                Api::OffsetFetch as i16,
                1,
                NO_ERROR,
                LOOKUP_MS,
            );
            assert_eq!(code, NO_ERROR);
        }
        let mut cfg = KafkaSourceConfig::new(cluster.bootstrap_servers(), "orders", "slow-list");
        cfg.statistics_interval = Duration::ZERO;
        cfg.startup_timeout = Duration::from_secs(120);
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source.open(SourceCtx::new(cp.handle())).expect("open");
        // Three listings wait 20 s each, so this sits above `HANG` and under the
        // nextest kill.
        wait_until(Duration::from_secs(90), "every lane", || {
            let outcome = source.poll_events(Duration::from_millis(100));
            matches!(outcome, Ok(SourceEvent::LanesAssigned(lanes)) if lanes.len() == 3)
        });
        assert!(
            cluster.received(Api::ListOffsets) >= 3,
            "the slow listings were made"
        );
    }
}

/// A start commit left unanswered by a silent coordinator, against the run
/// ending, on a real broker behind a proxy that holds every byte once told to.
mod silent_coordinator {
    use crate::support;

    use spate_core::checkpoint::Checkpointer;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx, SourceEvent};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::HANG;
    use spate_test_support::container_image;
    use std::io::{Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI16, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use support::latest;
    use testcontainers::core::{ContainerPort, WaitFor};
    use testcontainers::runners::SyncRunner;
    use testcontainers::{Container, GenericImage, ImageExt as _};

    const OFFSET_COMMIT: i16 = 8;
    const OFFSET_FETCH: i16 = 9;
    const TOPIC: &str = "silent";

    /// Forwards every connection to `upstream` until `silent`, then holds every
    /// byte in both directions, on old and new connections, until released.
    struct Proxy {
        addr: String,
        trigger: Arc<AtomicI16>,
        armed: Arc<AtomicBool>,
        silent: Arc<AtomicBool>,
    }

    fn hold(silent: &AtomicBool) {
        while silent.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    impl Proxy {
        fn on(listener: TcpListener, upstream: String) -> Proxy {
            let addr = listener.local_addr().expect("addr").to_string();
            let armed = Arc::new(AtomicBool::new(false));
            let trigger = Arc::new(AtomicI16::new(OFFSET_COMMIT));
            let t = Arc::clone(&trigger);
            let silent = Arc::new(AtomicBool::new(false));
            let (a, s) = (Arc::clone(&armed), Arc::clone(&silent));
            std::thread::spawn(move || {
                for client in listener.incoming().flatten() {
                    let Ok(broker) = TcpStream::connect(&upstream) else {
                        continue;
                    };
                    let (mut c_in, mut b_out) = (
                        client.try_clone().expect("clone"),
                        broker.try_clone().expect("clone"),
                    );
                    let (mut b_in, mut c_out) = (broker, client);
                    let (a1, s1, s2) = (Arc::clone(&a), Arc::clone(&s), Arc::clone(&s));
                    let t1 = Arc::clone(&t);
                    std::thread::spawn(move || {
                        loop {
                            let mut len = [0u8; 4];
                            if c_in.read_exact(&mut len).is_err() {
                                return;
                            }
                            let mut frame =
                                vec![0u8; usize::try_from(i32::from_be_bytes(len)).expect("len")];
                            if c_in.read_exact(&mut frame).is_err() {
                                return;
                            }
                            if i16::from_be_bytes([frame[0], frame[1]]) == t1.load(Ordering::SeqCst)
                                && a1.swap(false, Ordering::SeqCst)
                            {
                                s1.store(true, Ordering::SeqCst);
                            }
                            hold(&s1);
                            if b_out.write_all(&len).is_err() || b_out.write_all(&frame).is_err() {
                                return;
                            }
                        }
                    });
                    std::thread::spawn(move || {
                        let mut buf = [0u8; 65536];
                        loop {
                            let Ok(n) = b_in.read(&mut buf) else { return };
                            if n == 0 {
                                return;
                            }
                            hold(&s2);
                            if c_out.write_all(&buf[..n]).is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            Proxy {
                addr,
                trigger,
                armed,
                silent,
            }
        }

        fn release(&self) {
            self.armed.store(false, Ordering::SeqCst);
            self.silent.store(false, Ordering::SeqCst);
        }
    }

    /// A broker on the pinned image that advertises `proxy`'s address.
    fn broker_behind_a_proxy() -> (Container<GenericImage>, Proxy, String) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let advertised = listener.local_addr().expect("addr");
        let (name, tag) = container_image(&["--pull", "kafka"]);
        let container = GenericImage::new(name, tag)
            .with_exposed_port(ContainerPort::Tcp(9092))
            .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
            .with_env_var("KAFKA_NODE_ID", "1")
            .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
            .with_env_var(
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://:9093",
            )
            .with_env_var(
                "KAFKA_ADVERTISED_LISTENERS",
                format!("PLAINTEXT://{advertised}"),
            )
            .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env_var(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            )
            .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
            .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .start()
            .expect("start kafka");
        let port = container.get_host_port_ipv4(9092).expect("port");
        let proxy = Proxy::on(listener, format!("127.0.0.1:{port}"));
        let brokers = proxy.addr.clone();
        (container, proxy, brokers)
    }

    /// A source whose client gives up on a silent broker in seconds.
    fn short_timeouts(brokers: &str, group: &str) -> KafkaSourceConfig {
        let mut cfg = latest::config(brokers, TOPIC, group, "latest");
        cfg.startup_timeout = Duration::from_secs(15);
        cfg.rdkafka
            .insert("socket.timeout.ms".to_string(), "5000".to_string());
        cfg
    }

    /// The source's deadline error, and then its drop, with the coordinator still
    /// silent. Returns what the driver thread reported, in order.
    fn deadline_then_drop(trigger: i16) -> (Vec<String>, Duration) {
        let (_container, proxy, brokers) = broker_behind_a_proxy();
        support::create_topic(&brokers, TOPIC, 1);
        let cfg = short_timeouts(&brokers, "deadline");
        proxy.trigger.store(trigger, Ordering::SeqCst);
        proxy.armed.store(true, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        let driver = std::thread::spawn(move || {
            let cp = Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source.open(SourceCtx::new(cp.handle())).expect("open");
            loop {
                match source.poll_events(Duration::from_millis(200)) {
                    Ok(SourceEvent::LanesAssigned(_)) => {
                        tx.send("assigned".to_string()).expect("send");
                        break;
                    }
                    Err(e) if e.class() == ErrorClass::Fatal => {
                        tx.send(format!("fatal: {}", e.reason())).expect("send");
                        break;
                    }
                    _ => {}
                }
            }
            drop(source);
            tx.send("dropped".to_string()).expect("send");
        });
        let started = Instant::now();
        let mut seen = Vec::new();
        while seen.last().map(String::as_str) != Some("dropped") {
            match rx.recv_timeout(HANG) {
                Ok(line) => seen.push(line),
                Err(_) => break,
            }
        }
        let waited = started.elapsed();
        proxy.release();
        driver.join().expect("driver");
        (seen, waited)
    }

    /// A source whose start commit got no answer drops once the startup deadline has
    /// ended it, with the coordinator still silent.
    #[test]
    #[ignore = "requires Docker"]
    fn a_source_with_an_unanswered_start_commit_drops_at_the_startup_deadline() {
        let (seen, waited) = deadline_then_drop(OFFSET_COMMIT);
        assert_eq!(
            seen.last().map(String::as_str),
            Some("dropped"),
            "after {waited:?}: {seen:?}"
        );
    }

    /// Control: silent from the committed-offset lookup, so no commit is in flight.
    #[test]
    #[ignore = "requires Docker"]
    fn a_source_silent_from_its_lookup_drops_at_the_startup_deadline() {
        let (seen, waited) = deadline_then_drop(OFFSET_FETCH);
        assert_eq!(
            seen.last().map(String::as_str),
            Some("dropped"),
            "after {waited:?}: {seen:?}"
        );
    }

    /// A pipeline whose start commit got no answer ends once `startup_timeout` has
    /// failed it, with the coordinator still silent.
    #[test]
    #[ignore = "requires Docker"]
    fn a_pipeline_with_an_unanswered_start_commit_ends_at_the_startup_deadline() {
        let (_container, proxy, brokers) = broker_behind_a_proxy();
        support::create_topic(&brokers, TOPIC, 1);
        let cfg = short_timeouts(&brokers, "pipeline");
        proxy.armed.store(true, Ordering::SeqCst);
        let run = latest::Run::start(cfg);
        let exit = run.wait_exit(HANG);
        let silent = proxy.silent.load(Ordering::SeqCst);
        proxy.release();
        assert!(
            exit.is_some(),
            "silent={silent}: the run had not ended {HANG:?} after its start, past a 15 s startup_timeout"
        );
    }

    /// A source drops once the startup deadline has ended it, with its broker killed
    /// at the start commit.
    #[test]
    #[ignore = "requires Docker"]
    fn a_source_whose_broker_dies_at_the_start_commit_drops_at_the_startup_deadline() {
        let (container, proxy, brokers) = broker_behind_a_proxy();
        support::create_topic(&brokers, TOPIC, 1);
        let cfg = short_timeouts(&brokers, "killed");
        proxy.armed.store(true, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        let driver = std::thread::spawn(move || {
            let cp = Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source.open(SourceCtx::new(cp.handle())).expect("open");
            loop {
                match source.poll_events(Duration::from_millis(200)) {
                    Ok(SourceEvent::LanesAssigned(_)) => {
                        tx.send("assigned".to_string()).expect("send");
                        break;
                    }
                    Err(e) if e.class() == ErrorClass::Fatal => {
                        tx.send(format!("fatal: {}", e.reason())).expect("send");
                        break;
                    }
                    _ => {}
                }
            }
            drop(source);
            tx.send("dropped".to_string()).expect("send");
        });
        spate_test::wait_until(HANG, "the start commit", || {
            proxy.silent.load(Ordering::SeqCst)
        });
        container.stop_with_timeout(Some(0)).expect("kill");
        proxy.release();
        let started = Instant::now();
        let mut seen = Vec::new();
        while seen.last().map(String::as_str) != Some("dropped") {
            match rx.recv_timeout(HANG) {
                Ok(line) => seen.push(line),
                Err(_) => break,
            }
        }
        let waited = started.elapsed();
        let ended = seen.last().map(String::as_str) == Some("dropped");
        // The driver is left behind on failure: its drop has no end with the broker gone.
        assert!(ended, "after {waited:?}: {seen:?}");
        driver.join().expect("driver");
    }
}

/// A committed offset naming a logical position must not read as no commit. A raw
/// OffsetCommit stores the value, and the group's OffsetFetch reply then carries
/// it, the same bytes a hostile coordinator would answer with.
mod logical_committed_offset {
    use crate::support::MockKafka;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::types::RDKafkaApiKey;
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use spate_core::checkpoint::Checkpointer;
    use spate_core::clock::TestClock;
    use spate_core::config::PipelineConfig;
    use spate_core::error::ErrorClass;
    use spate_core::ops::chain_owned;
    use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
    use spate_core::sink::KeyHashRouter;
    use spate_core::source::{Source, SourceCtx};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{
        BytesPassthrough, HANG, PipelineRun, TestEncoder, capture_sink, unique_name, wait_until,
    };
    use std::collections::BTreeMap;
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    const TOPIC: &str = "orders";
    const GROUP: &str = "logical-commit";
    const END: i64 = 10;

    fn str16(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(&i16::try_from(s.len()).unwrap().to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    /// OffsetCommit v2 of `offset` on `orders` partition 0, as a simple consumer.
    fn raw_commit(brokers: &str, offset: i64) {
        let mut s = TcpStream::connect(brokers).unwrap();
        let mut b = Vec::new();
        b.extend_from_slice(&8i16.to_be_bytes());
        b.extend_from_slice(&2i16.to_be_bytes());
        b.extend_from_slice(&7i32.to_be_bytes());
        str16(&mut b, "rogue");
        str16(&mut b, GROUP);
        b.extend_from_slice(&(-1i32).to_be_bytes());
        str16(&mut b, "");
        b.extend_from_slice(&(-1i64).to_be_bytes());
        b.extend_from_slice(&1i32.to_be_bytes());
        str16(&mut b, TOPIC);
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(&0i32.to_be_bytes());
        b.extend_from_slice(&offset.to_be_bytes());
        b.extend_from_slice(&(-1i16).to_be_bytes());
        s.write_all(&i32::try_from(b.len()).unwrap().to_be_bytes())
            .unwrap();
        s.write_all(&b).unwrap();
        let mut len = [0u8; 4];
        s.read_exact(&mut len).unwrap();
        let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
        s.read_exact(&mut resp).unwrap();
    }

    fn committed(probe: &BaseConsumer) -> Offset {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(TOPIC, 0);
        probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .unwrap()
            .elements()[0]
            .offset()
    }

    /// Under `latest`, a committed offset of -2 (`Beginning`) is a commit, so the
    /// source does not take the partition as uncommitted and commit its end over
    /// records it has not read.
    #[test]
    fn a_logical_committed_offset_does_not_move_the_commit_past_unread_records() {
        holds_the_commit(-2, Offset::Beginning);
    }

    /// A committed offset of -3, which rdkafka reads as `Offset::Offset(-3)`, is no
    /// position to start from, so the run holds it and does not stop.
    #[test]
    fn a_negative_committed_offset_is_not_a_start() {
        holds_the_commit(-3, Offset::Offset(-3));
    }

    /// The reason a run stops on such an offset names the partition and the value,
    /// and claims no broker condition.
    #[test]
    fn the_deadline_error_names_the_committed_offset() {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).unwrap();
        let brokers = cluster.bootstrap_servers();
        raw_commit(&brokers, -2);
        let mut cfg = KafkaSourceConfig::new(&brokers, TOPIC, GROUP);
        cfg.startup_timeout = Duration::from_secs(8);
        cfg.statistics_interval = Duration::ZERO;
        cfg.rdkafka = BTreeMap::from([("auto.offset.reset".to_string(), "latest".to_string())]);
        let clock = TestClock::frozen();
        let checkpointer = Checkpointer::new();
        let mut source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        source.open(SourceCtx::new(checkpointer.handle())).unwrap();
        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            wait_until(HANG, "the held start", || {
                let _ = source.poll_events(Duration::from_millis(50));
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
        });

        clock.advance(Duration::from_secs(9));
        let error = match source.poll_events(Duration::from_millis(50)) {
            Err(e) if e.class() == ErrorClass::Fatal => e.to_string(),
            other => panic!("expected the startup deadline, got {other:?}"),
        };
        assert!(error.contains("no partition assignment"), "{error}");
        assert!(
            error.contains("partition 0 holds Beginning, which names no position"),
            "{error}"
        );
        assert!(!error.contains("high watermark"), "{error}");
    }

    /// Runs a pipeline over a group whose committed offset is `raw`, read back as
    /// `read`, and asserts the commit stays and the run survives to its shutdown.
    fn holds_the_commit(raw: i64, read: Offset) {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).unwrap();
        cluster.track_requests();
        let brokers = cluster.bootstrap_servers();

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for i in 0..END {
            let payload = i.to_string();
            producer
                .send(
                    BaseRecord::<str, str>::to(TOPIC)
                        .partition(0)
                        .payload(&payload),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();

        raw_commit(&brokers, raw);
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", GROUP)
            .create()
            .unwrap();
        assert_eq!(committed(&probe), read, "the reply carries {raw}");

        let name = unique_name("logicalcommit");
        let yaml = format!(
            "pipeline: {{ name: {name}, threads: 2, io_threads: 1 }}\n\
             admin: {{ listen: none }}\n\
             checkpoint: {{ drain_timeout: 200ms }}\n\
             metrics: {{ exporter: none }}\n\
             source: {{ memory: {{}} }}\n\
             sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n"
        );
        let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml).expect("config"))
            .expect("builder");
        let (sink, script) = capture_sink(1, 1);
        let mut cfg = KafkaSourceConfig::new(&brokers, TOPIC, GROUP);
        cfg.startup_timeout = Duration::from_secs(60);
        cfg.statistics_interval = Duration::ZERO;
        cfg.rdkafka = BTreeMap::from([("auto.offset.reset".to_string(), "latest".to_string())]);
        let runtime = pipeline
            .sink(sink)
            .expect("sink")
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
            .into_runtime(KafkaSource::new(cfg))
            .expect("runtime");
        let shutdown = runtime.shutdown_handle();
        let looked_up = cluster.received(RDKafkaApiKey::OffsetFetch);
        let committed_before = cluster.received(RDKafkaApiKey::OffsetCommit);
        let run = PipelineRun::spawn(move || runtime.run());

        // A held start looks the committed offset up again, and a start that
        // moves the commit sends it, so either means the first decision is made.
        wait_until(HANG, "a second committed-offset lookup or a commit", || {
            cluster.received(RDKafkaApiKey::OffsetFetch) >= looked_up + 2
                || cluster.received(RDKafkaApiKey::OffsetCommit) > committed_before
        });
        shutdown.trigger();
        let exit = run.wait_exit(HANG).expect("the run stops");
        // The run has made every commit it will make, and written all it covers.
        let wrote = !script.writes().is_empty();
        let seen = committed(&probe);
        assert!(
            seen == read || wrote,
            "the commit moved to {seen:?} with nothing written; run: {:?}",
            exit.as_ref()
                .map(|r| format!("{:?}", r.state))
                .map_err(|e| e.to_string())
        );
        let report = exit.expect("the run starts");
        assert!(
            !matches!(report.state, ExitState::Failed(_)),
            "{:?}",
            report.state
        );
    }
}

/// A group that refuses every start commit, against a source's deadlines.
mod refused_starts {
    use crate::support::MockKafka;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_core::checkpoint::Checkpointer;
    use spate_core::error::ErrorClass;
    use spate_core::source::{Source, SourceCtx};
    use spate_kafka::{KafkaSource, KafkaSourceConfig};
    use spate_test::{HANG, wait_until};
    use std::collections::BTreeMap;
    use std::time::Duration;

    const TOPIC: &str = "orders";
    const REFUSED: RDKafkaRespErr = RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS;

    /// Polls `consumers` consumers on one partition, with every start commit
    /// and heartbeat refused, until the source fails fatally. The startup and
    /// assignment deadlines together end it within 16 s.
    fn ends(consumers: usize) {
        let cluster = MockKafka::new();
        if consumers > 1 {
            cluster.hold_first_rebalance();
        }
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        cluster.request_errors(RDKafkaApiKey::OffsetCommit, &[REFUSED; 1000]);
        cluster.request_errors(RDKafkaApiKey::Heartbeat, &[REFUSED; 1000]);
        let mut cfg = KafkaSourceConfig::new(&brokers, TOPIC, "refused");
        cfg.consumers = consumers;
        cfg.startup_timeout = Duration::from_secs(8);
        cfg.assignment_timeout = Duration::from_secs(8);
        cfg.statistics_interval = Duration::ZERO;
        cfg.rdkafka = BTreeMap::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("session.timeout.ms".to_string(), "6000".to_string()),
            ("heartbeat.interval.ms".to_string(), "500".to_string()),
        ]);
        let checkpointer = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source
            .open(SourceCtx::new(checkpointer.handle()))
            .expect("open");
        wait_until(HANG, "a fatal error", || {
            matches!(
                source.poll_events(Duration::from_millis(100)),
                Err(e) if e.class() == ErrorClass::Fatal
            )
        });
    }

    /// The control: one consumer ends at the deadline.
    #[test]
    fn one_consumer_ends_at_the_deadline() {
        ends(1);
    }

    /// A consumer the group assigns nothing does not keep a run alive whose every
    /// offered partition was discarded.
    #[test]
    fn an_empty_peer_does_not_hold_off_the_deadline() {
        ends(2);
    }
}
