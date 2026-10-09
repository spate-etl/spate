//! Leader assignment through the task's watch-event application, with the
//! delivery order of lease and progress-record events set by the test.

use super::*;
use crate::records::{SCHEMA, WorkerVal};
use crate::store::memory::MemoryStore;
use spate_core::clock::tokio::TestClock;
use spate_core::coordination::{PlanContext, SplitPlan};

const FINGERPRINT: &str = "assignment-tests:v1";

struct Planner;

impl SplitPlanner for Planner {
    fn fingerprint(&self) -> String {
        FINGERPRINT.into()
    }

    fn plan(&mut self, _: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        panic!("publishing an assignment runs no planner");
    }
}

/// What the leader has seen when it publishes. Every view starts from `a`
/// assigned to w1 and unclaimed, `b` assigned to w2 and still leased by w1,
/// and `c` held by the leader, each member with one lane.
enum View {
    /// Nothing has changed since.
    Settled,
    /// w1 has claimed `a` and w2 has claimed `b`, deleting w1's lease. The
    /// leader holds both new progress records and neither new lease, and
    /// still holds w1's deleted lease on `b`.
    RecordsAhead,
    /// As `RecordsAhead`, then both new leases reach the leader.
    CaughtUp,
}

async fn create(store: &MemoryStore, ks: Keyspace, name: &str, value: Vec<u8>) -> Revision {
    store.create(ks, name, value).await.unwrap().won().unwrap()
}

async fn assignment(store: &MemoryStore, member: &str) -> Vec<String> {
    let entry = store
        .get(Keyspace::Durable, &records::assign_key(member))
        .await
        .unwrap()
        .unwrap();
    records::parse_val::<AssignmentVal>(&entry.key, &entry.value)
        .unwrap()
        .splits
}

fn lease_val(owner: &str, nonce: &str, epoch: u64) -> Vec<u8> {
    records::encode_val(&LeaseVal {
        schema: SCHEMA,
        owner: owner.into(),
        nonce: nonce.into(),
        epoch,
    })
}

/// Publish from `view` and return the stored assignments of w1 and w2.
async fn publish(view: View) -> (Vec<String>, Vec<String>) {
    let clock = TestClock::frozen();
    let cfg = CoordinationConfig {
        max_in_flight: 1,
        rebalance_delay: Duration::ZERO,
        ..CoordinationConfig::default()
    };
    let store = MemoryStore::with_clock(cfg.lease_duration, clock.clone());
    let fp = records::fingerprint_hash(FINGERPRINT);
    let nonces: BTreeMap<&str, String> = ["leader", "w1", "w2"]
        .into_iter()
        .map(|m| (m, uuid::Uuid::new_v4().simple().to_string()))
        .collect();
    let (_commands, commands_rx) = mpsc::channel(8);
    let (events_tx, _events) = std_mpsc::channel();
    let mut task = Task::new(
        store.clone(),
        cfg,
        clock,
        FINGERPRINT.to_string(),
        "leader".into(),
        nonces["leader"].clone(),
        Box::new(Planner),
        None,
        commands_rx,
        events_tx,
        None,
    );
    let leader = LeaderVal {
        schema: SCHEMA,
        owner: "leader".into(),
        nonce: nonces["leader"].clone(),
        generation: 1,
    };
    let leader_rev = create(
        &store,
        Keyspace::Ephemeral,
        records::LEADER_KEY,
        records::encode_val(&leader),
    )
    .await;
    task.leadership = Some(leader_rev);
    let mut plan = PlanRecord::new(FINGERPRINT.to_string());
    plan.generation = 1;
    plan.planned = 3;
    let plan_rev = create(&store, Keyspace::Durable, records::PLAN_KEY, plan.encode()).await;
    task.plan = Some((plan, plan_rev));
    task.plan_rev_seen = plan_rev.0;
    for (member, split) in [("leader", "c"), ("w1", "a"), ("w2", "b")] {
        let presence = WorkerVal {
            schema: SCHEMA,
            nonce: nonces[member].clone(),
            max_in_flight: 1,
        };
        create(
            &store,
            Keyspace::Ephemeral,
            &records::worker_key(member),
            records::encode_val(&presence),
        )
        .await;
        let assigned = AssignmentVal {
            schema: SCHEMA,
            generation: 1,
            splits: vec![split.into()],
        };
        create(
            &store,
            Keyspace::Durable,
            &records::assign_key(member),
            records::encode_val(&assigned),
        )
        .await;
    }
    for (id, owner) in [("a", None), ("b", Some("w1")), ("c", Some("leader"))] {
        let mut progress = SplitProgressRecord::planned(&SplitId::new(id).unwrap(), fp, None);
        progress.owner = owner.map(str::to_string);
        progress.epoch = u64::from(owner.is_some());
        create(
            &store,
            Keyspace::Durable,
            &records::split_key_str(id),
            progress.encode(),
        )
        .await;
        let spec = SplitSpecRecord {
            schema: SCHEMA,
            id: id.into(),
            fp,
            generation: 1,
            weight: 1,
            descriptor: String::new(),
        };
        create(
            &store,
            Keyspace::Durable,
            &records::spec_key_str(id),
            spec.encode(),
        )
        .await;
        if let Some(owner) = owner {
            create(
                &store,
                Keyspace::Ephemeral,
                &records::split_key_str(id),
                lease_val(owner, &nonces[owner], 1),
            )
            .await;
        }
    }
    for entry in store.list(Keyspace::Ephemeral, "").await.unwrap() {
        task.apply_lease_put(&entry).unwrap();
    }
    for entry in store.list(Keyspace::Durable, "").await.unwrap() {
        task.apply_state_put(&entry).unwrap();
    }
    assert_eq!(assignment(&store, "w1").await, ["a"]);
    assert_eq!(assignment(&store, "w2").await, ["b"]);

    if !matches!(view, View::Settled) {
        let b = records::split_key_str("b");
        let prior = store.get(Keyspace::Ephemeral, &b).await.unwrap().unwrap();
        let deleted = store
            .delete(Keyspace::Ephemeral, &b, Some(prior.revision))
            .await
            .unwrap();
        assert!(deleted.won().is_some(), "w1's lease on b is deleted");
        for (id, owner) in [("a", "w1"), ("b", "w2")] {
            let name = records::split_key_str(id);
            create(
                &store,
                Keyspace::Ephemeral,
                &name,
                lease_val(owner, &nonces[owner], 2),
            )
            .await;
            let before = store.get(Keyspace::Durable, &name).await.unwrap().unwrap();
            let mut progress = SplitProgressRecord::parse(&before.key, &before.value, fp).unwrap();
            progress.owner = Some(owner.into());
            progress.epoch = 2;
            let updated = store
                .update(Keyspace::Durable, &name, progress.encode(), before.revision)
                .await
                .unwrap();
            assert!(updated.won().is_some(), "{owner} claims {id}");
            let after = store.get(Keyspace::Durable, &name).await.unwrap().unwrap();
            task.apply_state_put(&after).unwrap();
        }
        let current = store.get(Keyspace::Ephemeral, &b).await.unwrap().unwrap();
        assert_ne!(current.revision, prior.revision);
        let current_owner = records::parse_val::<LeaseVal>(&current.key, &current.value)
            .unwrap()
            .owner;
        assert_eq!(current_owner, "w2");
        let seen = &task.splits["b"].lease.as_ref().unwrap().0;
        assert_eq!(seen.owner, "w1", "the leader still holds w1's lease on b");
        assert!(
            task.splits["a"].lease.is_none(),
            "w1's lease on a is unseen"
        );
    }
    if matches!(view, View::CaughtUp) {
        for id in ["a", "b"] {
            let entry = store
                .get(Keyspace::Ephemeral, &records::split_key_str(id))
                .await
                .unwrap()
                .unwrap();
            task.apply_lease_put(&entry).unwrap();
        }
    }

    task.assign_dirty = true;
    task.publish_assignments().await.unwrap();
    (
        assignment(&store, "w1").await,
        assignment(&store, "w2").await,
    )
}

/// A lease deleted by a later claim, still in the leader's view behind that
/// claim's record, takes no lane from its old owner at publish. Regression
/// for #824.
#[tokio::test]
async fn a_lease_its_record_has_moved_past_keeps_no_lane() {
    let (w1, w2) = publish(View::RecordsAhead).await;
    assert_eq!(w1, ["a"]);
    assert_eq!(w2, ["b"]);
}

/// Once the new tenancies' leases reach the leader, each split stays with
/// the worker holding it.
#[tokio::test]
async fn a_current_lease_keeps_its_lane_after_a_transfer() {
    let (w1, w2) = publish(View::CaughtUp).await;
    assert_eq!(w1, ["a"]);
    assert_eq!(w2, ["b"]);
}

/// A lease its record names keeps its owner's lane ahead of a split kept
/// only by its last assignee.
#[tokio::test]
async fn a_live_lease_keeps_its_owners_lane() {
    let (w1, w2) = publish(View::Settled).await;
    assert_eq!(w1, ["b"]);
    assert_eq!(w2, ["a"]);
}

/// An owner's reclaim of its own split keeps the split on that owner, both
/// while the leader holds the new record and the old lease, and after the new
/// lease arrives.
#[tokio::test]
async fn an_owners_reclaim_keeps_its_split_before_its_new_lease_arrives() {
    let clock = TestClock::frozen();
    let cfg = CoordinationConfig {
        max_in_flight: 1,
        rebalance_delay: Duration::ZERO,
        ..CoordinationConfig::default()
    };
    let store = MemoryStore::with_clock(cfg.lease_duration, clock.clone());
    let fp = records::fingerprint_hash(FINGERPRINT);
    let nonces: BTreeMap<&str, String> = ["leader", "w1", "w2", "w1-new"]
        .into_iter()
        .map(|m| (m, uuid::Uuid::new_v4().simple().to_string()))
        .collect();
    let (_commands, commands_rx) = mpsc::channel(8);
    let (events_tx, _events) = std_mpsc::channel();
    let mut task = Task::new(
        store.clone(),
        cfg,
        clock,
        FINGERPRINT.to_string(),
        "leader".into(),
        nonces["leader"].clone(),
        Box::new(Planner),
        None,
        commands_rx,
        events_tx,
        None,
    );
    let leader = LeaderVal {
        schema: SCHEMA,
        owner: "leader".into(),
        nonce: nonces["leader"].clone(),
        generation: 1,
    };
    let leader_rev = create(
        &store,
        Keyspace::Ephemeral,
        records::LEADER_KEY,
        records::encode_val(&leader),
    )
    .await;
    task.leadership = Some(leader_rev);
    let mut plan = PlanRecord::new(FINGERPRINT.to_string());
    plan.generation = 1;
    plan.planned = 3;
    let plan_rev = create(&store, Keyspace::Durable, records::PLAN_KEY, plan.encode()).await;
    task.plan = Some((plan, plan_rev));
    task.plan_rev_seen = plan_rev.0;
    for (member, split) in [("leader", "c"), ("w1", "a"), ("w2", "b")] {
        let presence = WorkerVal {
            schema: SCHEMA,
            nonce: nonces[member].clone(),
            max_in_flight: 1,
        };
        create(
            &store,
            Keyspace::Ephemeral,
            &records::worker_key(member),
            records::encode_val(&presence),
        )
        .await;
        let assigned = AssignmentVal {
            schema: SCHEMA,
            generation: 1,
            splits: vec![split.into()],
        };
        create(
            &store,
            Keyspace::Durable,
            &records::assign_key(member),
            records::encode_val(&assigned),
        )
        .await;
    }
    for (id, owner) in [("a", "w1"), ("b", "w1"), ("c", "leader")] {
        let mut progress = SplitProgressRecord::planned(&SplitId::new(id).unwrap(), fp, None);
        progress.owner = Some(owner.to_string());
        progress.epoch = 1;
        create(
            &store,
            Keyspace::Durable,
            &records::split_key_str(id),
            progress.encode(),
        )
        .await;
        let spec = SplitSpecRecord {
            schema: SCHEMA,
            id: id.into(),
            fp,
            generation: 1,
            weight: 1,
            descriptor: String::new(),
        };
        create(
            &store,
            Keyspace::Durable,
            &records::spec_key_str(id),
            spec.encode(),
        )
        .await;
        create(
            &store,
            Keyspace::Ephemeral,
            &records::split_key_str(id),
            lease_val(owner, &nonces[owner], 1),
        )
        .await;
    }
    for entry in store.list(Keyspace::Ephemeral, "").await.unwrap() {
        task.apply_lease_put(&entry).unwrap();
    }
    for entry in store.list(Keyspace::Durable, "").await.unwrap() {
        task.apply_state_put(&entry).unwrap();
    }
    task.publish_assignments().await.unwrap();
    assert_eq!(assignment(&store, "w1").await, ["a"]);
    assert_eq!(assignment(&store, "w2").await, ["b"]);

    // The restarted w1 rewrites its own lease on `a`, then its record.
    let name = records::split_key_str("a");
    let lease_before = store
        .get(Keyspace::Ephemeral, &name)
        .await
        .unwrap()
        .unwrap();
    let won = store
        .update(
            Keyspace::Ephemeral,
            &name,
            lease_val("w1", &nonces["w1-new"], 2),
            lease_before.revision,
        )
        .await
        .unwrap();
    assert!(won.won().is_some());
    let before = store.get(Keyspace::Durable, &name).await.unwrap().unwrap();
    let mut progress = SplitProgressRecord::parse(&before.key, &before.value, fp).unwrap();
    progress.epoch = 2;
    let won = store
        .update(Keyspace::Durable, &name, progress.encode(), before.revision)
        .await
        .unwrap();
    assert!(won.won().is_some());

    // The record reaches the leader first; another input dirties the
    // assignment inside that window.
    let after = store.get(Keyspace::Durable, &name).await.unwrap().unwrap();
    task.apply_state_put(&after).unwrap();
    task.assign_dirty = true;
    task.publish_assignments().await.unwrap();
    assert_eq!(
        assignment(&store, "w1").await,
        ["a"],
        "w1 holds a while its new lease is in flight"
    );

    // Then the new lease arrives.
    let lease_now = store
        .get(Keyspace::Ephemeral, &name)
        .await
        .unwrap()
        .unwrap();
    task.apply_lease_put(&lease_now).unwrap();
    task.publish_assignments().await.unwrap();
    assert_eq!(
        assignment(&store, "w1").await,
        ["a"],
        "w1 holds a at epoch 2"
    );
}

/// A drain applies at most its bound and stops at the first error, leaving
/// every later event unread.
#[test]
fn a_drain_stops_at_its_bound() {
    fn put(revision: u64) -> Result<WatchEvent, StoreError> {
        Ok(WatchEvent::Put(Entry {
            key: records::split_key_str(&format!("s{revision}")),
            value: Vec::new(),
            revision: Revision(revision),
        }))
    }
    fn drain(stream: &mut WatchStream) -> (Drained, Vec<u64>) {
        let mut applied = Vec::new();
        let stop = drain_ready(stream, DRAIN_BOUND, |event| {
            let WatchEvent::Put(entry) = event else {
                panic!("only puts are queued");
            };
            applied.push(entry.revision.0);
            Ok(())
        })
        .unwrap();
        (stop, applied)
    }
    fn next_revision(stream: &mut WatchStream) -> Option<u64> {
        match stream.next().now_or_never() {
            Some(Some(Ok(WatchEvent::Put(entry)))) => Some(entry.revision.0),
            _ => None,
        }
    }
    let bound = DRAIN_BOUND as u64;

    let mut stream = futures_util::stream::iter((0..bound + 5).map(put)).boxed();
    let (stop, applied) = drain(&mut stream);
    assert!(matches!(stop, Drained::Bound), "{stop:?}");
    assert_eq!(applied, (0..bound).collect::<Vec<_>>());
    assert_eq!(next_revision(&mut stream), Some(bound));

    let events = vec![
        put(0),
        put(1),
        Err(StoreError::Retryable("watch broke".into())),
        put(3),
    ];
    let mut stream = futures_util::stream::iter(events).boxed();
    let (stop, applied) = drain(&mut stream);
    assert!(matches!(stop, Drained::Broken(_)), "{stop:?}");
    assert_eq!(applied, [0, 1]);
    assert_eq!(next_revision(&mut stream), Some(3));

    let mut stream = futures_util::stream::iter([put(0)])
        .chain(futures_util::stream::pending())
        .boxed();
    let (stop, applied) = drain(&mut stream);
    assert!(matches!(stop, Drained::Idle), "{stop:?}");
    assert_eq!(applied, [0]);

    let mut stream = futures_util::stream::iter([put(0)]).boxed();
    let (stop, applied) = drain(&mut stream);
    assert!(matches!(stop, Drained::Ended), "{stop:?}");
    assert_eq!(applied, [0]);
}

/// A drain over a memory-store watch, whose tail is a tokio channel, stops
/// at its bound when more events are queued than one task poll's coop budget.
#[tokio::test]
async fn a_drain_over_a_memory_watch_reaches_its_bound() {
    let store = MemoryStore::with_clock(Duration::from_secs(10), TestClock::frozen());
    let mut watch = store.watch(Keyspace::Durable, "").await.unwrap();
    while !matches!(watch.next().await, Some(Ok(WatchEvent::SnapshotDone))) {}
    for i in 0..DRAIN_BOUND + 44 {
        let name = records::split_key_str(&format!("s{i:03}"));
        create(&store, Keyspace::Durable, &name, Vec::new()).await;
    }
    tokio::task::yield_now().await;
    let mut applied = 0;
    let stop = drain_ready(&mut watch, DRAIN_BOUND, |_| {
        applied += 1;
        Ok(())
    })
    .unwrap();
    assert!(matches!(stop, Drained::Bound), "{stop:?} after {applied}");
    assert_eq!(applied, DRAIN_BOUND);
}

mod ready_bursts {
    //! The coordination loop over a store whose watch delivers a burst of
    //! events all ready at once.

    use super::*;
    use crate::records::SplitStatus;
    use crate::store::CasOutcome;
    use std::sync::Mutex;

    /// A push store over `memory` whose watch on `burst_ks` yields `burst`
    /// right after its snapshot, every event ready at once, and reports each
    /// won write of the follower's assignment record on `follower`.
    ///
    /// The burst's revisions start at 1000, above any the memory store has
    /// issued, and are never written to it, so no snapshot repeats them.
    #[derive(Clone)]
    struct ReadyBurst {
        memory: MemoryStore,
        burst_ks: Keyspace,
        burst: Arc<Mutex<Option<Vec<Entry>>>>,
        follower: tokio::sync::watch::Sender<(usize, Vec<String>)>,
    }

    impl CoordinationStore for ReadyBurst {
        fn lease_ttl(&self) -> Duration {
            self.memory.lease_ttl()
        }

        fn watch_mode(&self) -> WatchMode {
            WatchMode::Push
        }

        fn op_timeout(&self) -> Option<Duration> {
            self.memory.op_timeout()
        }

        fn attach_metrics(&self, metrics: &spate_core::metrics::CoordinationMetrics) {
            self.memory.attach_metrics(metrics);
        }

        async fn create(
            &self,
            ks: Keyspace,
            key: &str,
            value: Vec<u8>,
        ) -> Result<CasOutcome, StoreError> {
            self.memory.create(ks, key, value).await
        }

        async fn update(
            &self,
            ks: Keyspace,
            key: &str,
            value: Vec<u8>,
            expected: Revision,
        ) -> Result<CasOutcome, StoreError> {
            let splits = (key == records::assign_key("follower")).then(|| {
                records::parse_val::<AssignmentVal>(key, &value)
                    .unwrap()
                    .splits
            });
            let outcome = self.memory.update(ks, key, value, expected).await?;
            if let (Some(splits), Some(_)) = (splits, outcome.won()) {
                self.follower.send_modify(|(writes, current)| {
                    *writes += 1;
                    *current = splits;
                });
            }
            Ok(outcome)
        }

        async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
            self.memory.get(ks, key).await
        }

        async fn delete(
            &self,
            ks: Keyspace,
            key: &str,
            expected: Option<Revision>,
        ) -> Result<CasOutcome, StoreError> {
            self.memory.delete(ks, key, expected).await
        }

        async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
            self.memory.list(ks, prefix).await
        }

        async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
            let mut events: Vec<Result<WatchEvent, StoreError>> = self
                .memory
                .list(ks, prefix)
                .await?
                .into_iter()
                .map(|entry| Ok(WatchEvent::Put(entry)))
                .collect();
            events.push(Ok(WatchEvent::SnapshotDone));
            if ks == self.burst_ks
                && let Some(burst) = self.burst.lock().unwrap().take()
            {
                events.extend(burst.into_iter().map(|entry| Ok(WatchEvent::Put(entry))));
            }
            Ok(futures_util::stream::iter(events)
                .chain(futures_util::stream::pending())
                .boxed())
        }
    }

    /// A leader at its one-lane cap holding `own`, and a follower holding
    /// `held` splits at a cap of `held`, with both assignments published.
    struct Fleet {
        store: MemoryStore,
        clock: Arc<TestClock>,
        cfg: CoordinationConfig,
        nonces: BTreeMap<&'static str, String>,
        leader_rev: Revision,
        plan: PlanRecord,
        plan_rev: Revision,
        held: Vec<String>,
    }

    impl Fleet {
        async fn new(held: usize) -> Fleet {
            let clock = TestClock::frozen();
            let cfg = CoordinationConfig {
                max_in_flight: 1,
                rebalance_delay: Duration::ZERO,
                ..CoordinationConfig::default()
            };
            let store = MemoryStore::with_clock(cfg.lease_duration, clock.clone());
            let fp = records::fingerprint_hash(FINGERPRINT);
            let nonces: BTreeMap<&str, String> = ["leader", "follower"]
                .into_iter()
                .map(|m| (m, uuid::Uuid::new_v4().simple().to_string()))
                .collect();
            let leader = LeaderVal {
                schema: SCHEMA,
                owner: "leader".into(),
                nonce: nonces["leader"].clone(),
                generation: 1,
            };
            let leader_rev = create(
                &store,
                Keyspace::Ephemeral,
                records::LEADER_KEY,
                records::encode_val(&leader),
            )
            .await;
            let ids: Vec<String> = (0..held).map(|i| format!("s{i:02}")).collect();
            let mut plan = PlanRecord::new(FINGERPRINT.to_string());
            plan.generation = 1;
            plan.planned = held as u64 + 1;
            let plan_rev =
                create(&store, Keyspace::Durable, records::PLAN_KEY, plan.encode()).await;
            let members = [
                ("leader", 1, vec!["own".to_string()]),
                ("follower", u32::try_from(held).unwrap(), ids.clone()),
            ];
            for (member, cap, splits) in members {
                let presence = WorkerVal {
                    schema: SCHEMA,
                    nonce: nonces[member].clone(),
                    max_in_flight: cap,
                };
                create(
                    &store,
                    Keyspace::Ephemeral,
                    &records::worker_key(member),
                    records::encode_val(&presence),
                )
                .await;
                let assigned = AssignmentVal {
                    schema: SCHEMA,
                    generation: 1,
                    splits: splits.clone(),
                };
                create(
                    &store,
                    Keyspace::Durable,
                    &records::assign_key(member),
                    records::encode_val(&assigned),
                )
                .await;
                for id in splits {
                    let mut progress =
                        SplitProgressRecord::planned(&SplitId::new(id.clone()).unwrap(), fp, None);
                    progress.owner = Some(member.into());
                    progress.epoch = 1;
                    let name = records::split_key_str(&id);
                    create(&store, Keyspace::Durable, &name, progress.encode()).await;
                    let spec = SplitSpecRecord {
                        schema: SCHEMA,
                        id: id.clone(),
                        fp,
                        generation: 1,
                        weight: 1,
                        descriptor: String::new(),
                    };
                    create(
                        &store,
                        Keyspace::Durable,
                        &records::spec_key_str(&id),
                        spec.encode(),
                    )
                    .await;
                    let lease = lease_val(member, &nonces[member], 1);
                    create(&store, Keyspace::Ephemeral, &name, lease).await;
                }
            }
            Fleet {
                store,
                clock,
                cfg,
                nonces,
                leader_rev,
                plan,
                plan_rev,
                held: ids,
            }
        }

        /// Run the leader's loop with `burst` ready on its `burst_ks` watch,
        /// until the follower's assignment satisfies `done`, and return how
        /// many times that assignment was written.
        async fn run(
            self,
            burst_ks: Keyspace,
            burst: Vec<Entry>,
            done: impl Fn(&[String]) -> bool,
        ) -> usize {
            let (follower, mut written) = tokio::sync::watch::channel((0, Vec::new()));
            let store = ReadyBurst {
                memory: self.store,
                burst_ks,
                burst: Arc::new(Mutex::new(Some(burst))),
                follower,
            };
            let (commands, commands_rx) = mpsc::channel(8);
            let (events_tx, _events) = std_mpsc::channel();
            let mut task = Task::new(
                store,
                self.cfg,
                self.clock,
                FINGERPRINT.to_string(),
                "leader".into(),
                self.nonces["leader"].clone(),
                Box::new(Planner),
                None,
                commands_rx,
                events_tx,
                None,
            );
            task.leadership = Some(self.leader_rev);
            task.plan = Some((self.plan, self.plan_rev));
            task.plan_rev_seen = self.plan_rev.0;
            let running = tokio::spawn(async move { task.run_inner().await });
            let writes = tokio::time::timeout(
                Duration::from_secs(60),
                written.wait_for(|(writes, splits)| *writes > 0 && done(splits)),
            )
            .await
            .expect("the burst reaches the follower's assignment")
            .unwrap()
            .0;
            drop(commands);
            running.await.unwrap().unwrap();
            writes
        }
    }

    /// Completions delivered together cost the follower's assignment one
    /// write.
    #[tokio::test(start_paused = true)]
    async fn ready_completions_rewrite_an_assignment_once() {
        let fleet = Fleet::new(4).await;
        let fp = records::fingerprint_hash(FINGERPRINT);
        let mut burst = Vec::new();
        for (revision, id) in (1000..).zip(&fleet.held) {
            let name = records::split_key_str(id);
            let entry = fleet.store.get(Keyspace::Durable, &name).await;
            let entry = entry.unwrap().unwrap();
            let mut progress = SplitProgressRecord::parse(&entry.key, &entry.value, fp).unwrap();
            progress.completed = true;
            progress.status = SplitStatus::Completed;
            progress.watermark = Some(1);
            burst.push(Entry {
                key: entry.key,
                value: progress.encode(),
                revision: Revision(revision),
            });
        }
        let writes = fleet
            .run(Keyspace::Durable, burst, <[String]>::is_empty)
            .await;
        assert_eq!(writes, 1, "one assignment write for four completions");
    }

    /// Members whose presence arrives together cost the follower's
    /// assignment one write.
    #[tokio::test(start_paused = true)]
    async fn members_joining_together_rewrite_an_assignment_once() {
        let fleet = Fleet::new(6).await;
        let burst = (0..3)
            .map(|i| {
                let presence = WorkerVal {
                    schema: SCHEMA,
                    nonce: uuid::Uuid::new_v4().simple().to_string(),
                    max_in_flight: 1,
                };
                Entry {
                    key: records::worker_key(&format!("joiner{i}")),
                    value: records::encode_val(&presence),
                    revision: Revision(1000 + i),
                }
            })
            .collect();
        let writes = fleet
            .run(Keyspace::Ephemeral, burst, |splits| splits.len() == 3)
            .await;
        assert_eq!(writes, 1, "one assignment write for three joins");
    }

    /// A store whose first durable watch yields one event after its snapshot, then an error or, with `ends`, the stream's end, all ready.
    #[derive(Clone)]
    struct BreakAfterEvent {
        memory: MemoryStore,
        durable_watches: tokio::sync::watch::Sender<usize>,
        ends: bool,
    }

    impl CoordinationStore for BreakAfterEvent {
        fn lease_ttl(&self) -> Duration {
            self.memory.lease_ttl()
        }
        fn watch_mode(&self) -> WatchMode {
            WatchMode::Push
        }
        fn op_timeout(&self) -> Option<Duration> {
            self.memory.op_timeout()
        }
        fn attach_metrics(&self, metrics: &spate_core::metrics::CoordinationMetrics) {
            self.memory.attach_metrics(metrics);
        }
        async fn create(
            &self,
            ks: Keyspace,
            key: &str,
            value: Vec<u8>,
        ) -> Result<CasOutcome, StoreError> {
            self.memory.create(ks, key, value).await
        }
        async fn update(
            &self,
            ks: Keyspace,
            key: &str,
            value: Vec<u8>,
            expected: Revision,
        ) -> Result<CasOutcome, StoreError> {
            self.memory.update(ks, key, value, expected).await
        }
        async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
            self.memory.get(ks, key).await
        }
        async fn delete(
            &self,
            ks: Keyspace,
            key: &str,
            expected: Option<Revision>,
        ) -> Result<CasOutcome, StoreError> {
            self.memory.delete(ks, key, expected).await
        }
        async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
            self.memory.list(ks, prefix).await
        }
        async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
            let mut earlier = 0;
            if ks == Keyspace::Durable {
                self.durable_watches.send_modify(|n| {
                    earlier = *n;
                    *n += 1;
                });
            }
            if ks != Keyspace::Durable || earlier > 0 {
                return self.memory.watch(ks, prefix).await;
            }
            let mut events: Vec<Result<WatchEvent, StoreError>> = self
                .memory
                .list(ks, prefix)
                .await?
                .into_iter()
                .map(|e| Ok(WatchEvent::Put(e)))
                .collect();
            events.push(Ok(WatchEvent::SnapshotDone));
            events.push(Ok(WatchEvent::Put(Entry {
                key: "unrelated".into(),
                value: Vec::new(),
                revision: Revision(1000),
            })));
            if self.ends {
                let mut events = std::collections::VecDeque::from(events);
                let mut ended = false;
                return Ok(futures_util::stream::poll_fn(move |_| {
                    if let Some(e) = events.pop_front() {
                        return std::task::Poll::Ready(Some(e));
                    }
                    if ended {
                        return std::task::Poll::Pending;
                    }
                    ended = true;
                    std::task::Poll::Ready(None)
                })
                .boxed());
            }
            events.push(Err(StoreError::Retryable("watch broke".into())));
            Ok(futures_util::stream::iter(events)
                .chain(futures_util::stream::pending())
                .boxed())
        }
    }

    /// A stream that breaks behind a ready event is watched again.
    #[tokio::test(start_paused = true)]
    async fn a_break_met_while_draining_is_watched_again() {
        let fleet = Fleet::new(1).await;
        let (durable_watches, mut watches) = tokio::sync::watch::channel(0);
        let store = BreakAfterEvent {
            memory: fleet.store.clone(),
            durable_watches,
            ends: false,
        };
        let (commands, commands_rx) = mpsc::channel(8);
        let (events_tx, _events) = std_mpsc::channel();
        let mut task = Task::new(
            store,
            fleet.cfg,
            fleet.clock,
            FINGERPRINT.to_string(),
            "leader".into(),
            fleet.nonces["leader"].clone(),
            Box::new(Planner),
            None,
            commands_rx,
            events_tx,
            None,
        );
        task.leadership = Some(fleet.leader_rev);
        task.plan = Some((fleet.plan, fleet.plan_rev));
        task.plan_rev_seen = fleet.plan_rev.0;
        let running = tokio::spawn(async move { task.run_inner().await });
        tokio::time::timeout(Duration::from_secs(60), watches.wait_for(|n| *n >= 2))
            .await
            .expect("the durable watch is established again")
            .unwrap();
        drop(commands);
        running.await.unwrap().unwrap();
    }

    /// A stream that ends behind a ready event is watched again.
    #[tokio::test(start_paused = true)]
    async fn an_end_met_while_draining_is_watched_again() {
        let fleet = Fleet::new(1).await;
        let (durable_watches, mut watches) = tokio::sync::watch::channel(0);
        let store = BreakAfterEvent {
            memory: fleet.store.clone(),
            durable_watches,
            ends: true,
        };
        let (commands, commands_rx) = mpsc::channel(8);
        let (events_tx, _events) = std_mpsc::channel();
        let mut task = Task::new(
            store,
            fleet.cfg,
            fleet.clock,
            FINGERPRINT.to_string(),
            "leader".into(),
            fleet.nonces["leader"].clone(),
            Box::new(Planner),
            None,
            commands_rx,
            events_tx,
            None,
        );
        task.leadership = Some(fleet.leader_rev);
        task.plan = Some((fleet.plan, fleet.plan_rev));
        task.plan_rev_seen = fleet.plan_rev.0;
        let running = tokio::spawn(async move { task.run_inner().await });
        tokio::time::timeout(Duration::from_secs(60), watches.wait_for(|n| *n >= 2))
            .await
            .expect("the durable watch is established again")
            .unwrap();
        drop(commands);
        running.await.unwrap().unwrap();
    }
}

/// A rewrite of an own-id lease from another process is fatal on a split's
/// last attempt and ignored below it; the first sighting is never fatal.
#[tokio::test]
async fn a_lease_write_under_this_id_on_a_last_attempt_is_fatal() {
    let clock = TestClock::frozen();
    let cfg = CoordinationConfig {
        max_attempts: 2,
        ..CoordinationConfig::default()
    };
    let store = MemoryStore::with_clock(cfg.lease_duration, clock.clone());
    let fp = records::fingerprint_hash(FINGERPRINT);
    let (_commands, commands_rx) = mpsc::channel(8);
    let (events_tx, _events) = std_mpsc::channel();
    let mut task = Task::new(
        store,
        cfg,
        clock,
        FINGERPRINT.to_string(),
        "w1".into(),
        uuid::Uuid::new_v4().simple().to_string(),
        Box::new(Planner),
        None,
        commands_rx,
        events_tx,
        None,
    );
    let twin = uuid::Uuid::new_v4().simple().to_string();
    let put = |id: &str, revision: u64| Entry {
        key: records::split_key_str(id),
        value: lease_val("w1", &twin, 1),
        revision: Revision(revision),
    };
    for (id, attempts) in [("last", 1), ("spare", 0)] {
        let mut progress = SplitProgressRecord::planned(&SplitId::new(id).unwrap(), fp, None);
        progress.owner = Some("w1".to_string());
        progress.epoch = 1;
        progress.attempts = attempts;
        task.apply_state_put(&Entry {
            key: records::split_key_str(id),
            value: progress.encode(),
            revision: Revision(1),
        })
        .unwrap();
    }

    task.apply_lease_put(&put("last", 1)).unwrap();
    let err = task.apply_lease_put(&put("last", 2)).unwrap_err();
    assert!(
        err.kind == CoordinationErrorKind::Fatal && err.reason.contains("share instance_id"),
        "{err:?}"
    );
    task.apply_lease_put(&put("spare", 1)).unwrap();
    task.apply_lease_put(&put("spare", 2)).unwrap();
}
