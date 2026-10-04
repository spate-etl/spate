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
