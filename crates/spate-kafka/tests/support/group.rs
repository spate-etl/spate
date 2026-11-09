//! A consumer group led by the test over the wire, so that no member's
//! `SyncGroup` reaches the mock after the leader's.

use super::mock::MockKafka;
use super::wire::{Reader, bytes32, request, str16};
use rdkafka::types::RDKafkaApiKey;
use spate_core::task::{ThreadHandle, spawn_thread};
use spate_test::{HANG, wait_until};
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc;
use std::time::Duration;

/// The session timeout the leader joins with. The mock applies the latest
/// joiner's to the whole group, and the consumers join after the leader.
const SESSION_MS: i32 = 6_000;

/// How often the leader heartbeats: the mock removes a member that sends
/// nothing for a session timeout.
const HEARTBEAT: Duration = Duration::from_secs(1);

const UNKNOWN_MEMBER_ID: i16 = 25;

/// The partitions each follower is offered, given its place among the
/// followers sorted by member id and how many there are.
type Assign = Box<dyn Fn(usize, usize) -> Vec<(String, Vec<i32>)> + Send>;

/// The test's own member of a group, which leads every generation.
///
/// The librdkafka mock refuses a follower's `SyncGroup` that arrives after the
/// leader's, and the follower rejoins. A client leader sends its `SyncGroup`
/// once it has metadata, with nothing to order it after its followers', so a
/// test of two consumers can run on a path its doc does not name. This member
/// joins before the consumers under test, so the mock elects it, and in each
/// generation it sends its `SyncGroup` only once the mock has received one from
/// every follower.
///
/// It heartbeats on a thread of its own and rejoins whenever the group
/// rebalances. Dropping it closes its connection and joins the thread, and a
/// test drops it after the consumers it leads.
pub(crate) struct GroupLeader {
    stop: Option<mpsc::Sender<()>>,
    conn: TcpStream,
    thread: Option<ThreadHandle<()>>,
}

impl MockKafka {
    /// Lead `group` on `topic`, splitting `partitions` across the followers in
    /// contiguous ranges. See [`MockKafka::lead_group_with`].
    pub(crate) fn lead_group(&self, group: &str, topic: &str, partitions: i32) -> GroupLeader {
        let owned = topic.to_owned();
        self.lead_group_with(group, topic, move |k, followers| {
            vec![(owned.clone(), range(k, followers, partitions))]
        })
    }

    /// Join `group` subscribed to `topic`, ahead of any other member, and lead
    /// it with `assign` until dropped. Returns once the mock has received the
    /// leader's `JoinGroup`, so a consumer started after it joins behind it.
    ///
    /// It holds the group's first rebalance, so the consumers the test starts
    /// next join the first generation, and it restarts the cluster's request
    /// tracking: a test counts requests from here on.
    ///
    /// # Panics
    ///
    /// On a cluster of more than one broker, whose group coordinator this does
    /// not look up, or when the leader's `JoinGroup` does not arrive. The
    /// leader's thread panics on drop when a client joined the group first.
    pub(crate) fn lead_group_with(
        &self,
        group: &str,
        topic: &str,
        assign: impl Fn(usize, usize) -> Vec<(String, Vec<i32>)> + Send + 'static,
    ) -> GroupLeader {
        let brokers = self.bootstrap_servers();
        assert!(!brokers.contains(','), "one broker coordinates every group");
        self.hold_first_rebalance();
        self.track_requests();
        let conn = TcpStream::connect(&brokers).expect("connect to the mock");
        let (stop, stopped) = mpsc::channel();
        let member = Member {
            cluster: self.clone(),
            conn: conn.try_clone().expect("clone the connection"),
            group: group.to_owned(),
            topic: topic.to_owned(),
            assign: Box::new(assign),
            stopped,
        };
        let thread = spawn_thread("group-leader", move || member.lead()).expect("spawn");
        wait_until(HANG, "the leader's JoinGroup", || {
            self.received(RDKafkaApiKey::JoinGroup) >= 1
        });
        GroupLeader {
            stop: Some(stop),
            conn,
            thread: Some(thread),
        }
    }
}

impl Drop for GroupLeader {
    fn drop(&mut self) {
        drop(self.stop.take());
        let _ = self.conn.shutdown(Shutdown::Both);
        if let Some(thread) = self.thread.take()
            && let Err(panic) = thread.join()
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

/// Partitions `0..partitions` in contiguous ranges, the `k`th of `followers`.
fn range(k: usize, followers: usize, partitions: i32) -> Vec<i32> {
    let n = i32::try_from(followers).unwrap();
    let k = i32::try_from(k).unwrap();
    let (each, extra) = (partitions / n, partitions % n);
    let start = k * each + k.min(extra);
    let len = each + i32::from(k < extra);
    (start..start + len).collect()
}

struct Member {
    cluster: MockKafka,
    conn: TcpStream,
    group: String,
    topic: String,
    assign: Assign,
    stopped: mpsc::Receiver<()>,
}

/// The leader's view of one generation: its id, and its followers' ids sorted.
struct Generation {
    id: i32,
    me: String,
    followers: Vec<String>,
}

impl Member {
    /// Join, sync and heartbeat until the test drops the leader, which closes
    /// the connection under any request in flight.
    fn lead(mut self) {
        let mut member_id = String::new();
        loop {
            let Some(generation) = self.join(&mut member_id) else {
                return;
            };
            if !self.await_followers(generation.followers.len()) {
                return;
            }
            if self.sync(&generation) && !self.heartbeat(&generation, &mut member_id) {
                return;
            }
        }
    }

    /// One `JoinGroup` as `member_id`, which it updates. `None` once the
    /// connection is closed.
    fn join(&mut self, member_id: &mut String) -> Option<Generation> {
        loop {
            let mut subscription = Vec::new();
            subscription.extend_from_slice(&0i16.to_be_bytes());
            subscription.extend_from_slice(&1i32.to_be_bytes());
            str16(&mut subscription, &self.topic);
            subscription.extend_from_slice(&(-1i32).to_be_bytes());
            let mut body = Vec::new();
            str16(&mut body, &self.group);
            body.extend_from_slice(&SESSION_MS.to_be_bytes());
            body.extend_from_slice(&SESSION_MS.to_be_bytes());
            str16(&mut body, member_id);
            str16(&mut body, "consumer");
            body.extend_from_slice(&1i32.to_be_bytes());
            str16(&mut body, "range");
            bytes32(&mut body, &subscription);
            let reply = request(&mut self.conn, RDKafkaApiKey::JoinGroup as i16, 1, &body).ok()?;
            let mut r = Reader(&reply);
            match r.i16() {
                0 => {}
                UNKNOWN_MEMBER_ID => {
                    member_id.clear();
                    continue;
                }
                error => panic!("JoinGroup error {error}"),
            }
            let id = r.i32();
            let _protocol = r.string();
            let leader = r.string();
            let me = r.string();
            assert_eq!(
                leader, me,
                "a member joined the group before the test's leader"
            );
            let mut followers = Vec::new();
            for _ in 0..r.i32() {
                let member = r.string();
                r.skip_bytes();
                if member != me {
                    followers.push(member);
                }
            }
            followers.sort();
            member_id.clone_from(&me);
            return Some(Generation { id, me, followers });
        }
    }

    /// Wait until the mock has received a `SyncGroup` from each of `followers`
    /// members since the last `JoinGroup` of this generation, or a later
    /// `JoinGroup` has started another rebalance. `false` once the test drops
    /// the leader.
    ///
    /// A follower sends its `JoinGroup` and `SyncGroup` in turn on one
    /// connection, so every `SyncGroup` after the generation's last `JoinGroup`
    /// is for this generation.
    fn await_followers(&self, followers: usize) -> bool {
        let join = RDKafkaApiKey::JoinGroup as i16;
        let sync = RDKafkaApiKey::SyncGroup as i16;
        let keys = self.cluster.received_keys();
        let after_join = keys.iter().rposition(|k| *k == join).map_or(0, |i| i + 1);
        let mut dropped = false;
        wait_until(HANG, "every follower's SyncGroup", || {
            dropped = !matches!(self.stopped.try_recv(), Err(mpsc::TryRecvError::Empty));
            let since = &self.cluster.received_keys()[after_join..];
            dropped
                || since.iter().filter(|k| **k == sync).count() >= followers
                || since.contains(&join)
        });
        !dropped
    }

    /// The leader's `SyncGroup`, carrying each follower's offer. `false` when
    /// the mock refuses it, which leaves a rebalance to rejoin.
    fn sync(&mut self, generation: &Generation) -> bool {
        let mut body = Vec::new();
        str16(&mut body, &self.group);
        body.extend_from_slice(&generation.id.to_be_bytes());
        str16(&mut body, &generation.me);
        let members = i32::try_from(generation.followers.len() + 1).unwrap();
        body.extend_from_slice(&members.to_be_bytes());
        str16(&mut body, &generation.me);
        bytes32(&mut body, &[]);
        let count = generation.followers.len();
        for (k, follower) in generation.followers.iter().enumerate() {
            str16(&mut body, follower);
            bytes32(&mut body, &assignment(&(self.assign)(k, count)));
        }
        let Ok(reply) = request(&mut self.conn, RDKafkaApiKey::SyncGroup as i16, 1, &body) else {
            return false;
        };
        let mut r = Reader(&reply);
        let _throttle = r.i32();
        r.i16() == 0
    }

    /// Heartbeat each [`HEARTBEAT`] until the group rebalances, which returns
    /// `true` to rejoin, or the test drops the leader, which returns `false`.
    fn heartbeat(&mut self, generation: &Generation, member_id: &mut String) -> bool {
        loop {
            if !matches!(
                self.stopped.recv_timeout(HEARTBEAT),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                return false;
            }
            let mut body = Vec::new();
            str16(&mut body, &self.group);
            body.extend_from_slice(&generation.id.to_be_bytes());
            str16(&mut body, &generation.me);
            let Ok(reply) = request(&mut self.conn, RDKafkaApiKey::Heartbeat as i16, 1, &body)
            else {
                return false;
            };
            let mut r = Reader(&reply);
            let _throttle = r.i32();
            match r.i16() {
                0 => {}
                UNKNOWN_MEMBER_ID => {
                    member_id.clear();
                    return true;
                }
                _ => return true,
            }
        }
    }
}

/// A consumer protocol assignment of `partitions` per topic.
fn assignment(partitions: &[(String, Vec<i32>)]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&i32::try_from(partitions.len()).unwrap().to_be_bytes());
    for (topic, ids) in partitions {
        str16(&mut b, topic);
        b.extend_from_slice(&i32::try_from(ids.len()).unwrap().to_be_bytes());
        for id in ids {
            b.extend_from_slice(&id.to_be_bytes());
        }
    }
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b
}
