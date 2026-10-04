//! Watches that list their prefix every poll interval and report the
//! difference, one poller per handle, keyspace and prefix.

use super::Inner;
use super::table::{Item, Query};
use crate::store::{Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream};
use futures_util::StreamExt as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

type Event = Result<WatchEvent, StoreError>;
type Subscribed = Result<(Vec<Entry>, mpsc::UnboundedReceiver<Event>), StoreError>;

/// The handle every stream of one poller holds; the poller's task stops
/// once the last one drops.
#[derive(Debug)]
pub(super) struct Poller {
    subscribe: mpsc::UnboundedSender<oneshot::Sender<Subscribed>>,
}

/// A stream of `prefix` in `ks`, from the handle's poller for it.
pub(super) async fn watch(
    inner: &Arc<Inner>,
    ks: Keyspace,
    prefix: &str,
) -> Result<WatchStream, StoreError> {
    // A poller whose runtime stopped is replaced once.
    for _ in 0..2 {
        let poller = poller(inner, ks, prefix);
        let (reply, subscribed) = oneshot::channel();
        if poller.subscribe.send(reply).is_err() {
            continue;
        }
        let Ok(subscribed) = subscribed.await else {
            continue;
        };
        let (snapshot, events) = subscribed?;
        let head = futures_util::stream::iter(
            snapshot
                .into_iter()
                .map(WatchEvent::Put)
                .chain(std::iter::once(WatchEvent::SnapshotDone))
                .map(Ok),
        );
        let tail =
            futures_util::stream::unfold((events, poller), |(mut events, poller)| async move {
                events.recv().await.map(|e| (e, (events, poller)))
            });
        return Ok(head.chain(tail).boxed());
    }
    Err(StoreError::Retryable("the watch poller stopped".into()))
}

fn poller(inner: &Arc<Inner>, ks: Keyspace, prefix: &str) -> Arc<Poller> {
    let mut pollers = inner.pollers.lock().expect("pollers poisoned");
    let key = (ks, prefix.to_string());
    if let Some(poller) = pollers.get(&key).and_then(Weak::upgrade)
        && !poller.subscribe.is_closed()
    {
        return poller;
    }
    let (subscribe, requests) = mpsc::unbounded_channel();
    let poller = Arc::new(Poller { subscribe });
    tokio::spawn(run(Arc::downgrade(inner), ks, prefix.to_string(), requests));
    pollers.insert(key, Arc::downgrade(&poller));
    poller
}

async fn run(
    inner: Weak<Inner>,
    ks: Keyspace,
    prefix: String,
    mut requests: mpsc::UnboundedReceiver<oneshot::Sender<Subscribed>>,
) {
    let Some(interval) = inner.upgrade().map(|i| i.config.poll_interval) else {
        return;
    };
    let mut poll = Poll {
        ks,
        prefix,
        seen: BTreeMap::new(),
        deleted: BTreeMap::new(),
        subscribers: Vec::new(),
    };
    let mut next = Instant::now() + interval;
    loop {
        tokio::select! {
            request = requests.recv() => {
                let (Some(reply), Some(inner)) = (request, inner.upgrade()) else {
                    return;
                };
                poll.subscribe(&inner, reply).await;
            }
            () = tokio::time::sleep_until(next) => {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                poll.tick(&inner).await;
                next = Instant::now() + interval;
            }
        }
    }
}

struct Poll {
    ks: Keyspace,
    prefix: String,
    /// The revision last delivered for each key.
    seen: BTreeMap<String, u64>,
    /// The highest tombstone revision read for each durable key that holds
    /// no delivered revision; a later put must sit above it. A consistent
    /// read that no longer lists the tombstone drops it.
    deleted: BTreeMap<String, u64>,
    subscribers: Vec<mpsc::UnboundedSender<Event>>,
}

struct Read {
    s0: u64,
    t0: Instant,
    items: Vec<(String, Item)>,
}

fn send(subscribers: &mut Vec<mpsc::UnboundedSender<Event>>, event: &WatchEvent) {
    subscribers.retain(|s| s.send(Ok(event.clone())).is_ok());
}

fn entry(key: &str, item: &Item) -> Entry {
    Entry {
        key: key.to_string(),
        value: item.b.clone().unwrap_or_default(),
        revision: Revision(item.v),
    }
}

impl Poll {
    async fn read(&self, inner: &Inner, consistent: bool) -> Result<Read, StoreError> {
        let table = inner
            .table
            .get()
            .expect("a watch starts after the table is ready");
        let (s0, t0) = inner.mark();
        let mut items = Vec::new();
        let mut start = None;
        loop {
            let page = table
                .query(Query {
                    pk: inner.pk(self.ks).to_string(),
                    prefix: (!self.prefix.is_empty()).then(|| self.prefix.clone()),
                    consistent,
                    start,
                    filter_tombs: false,
                })
                .await?;
            items.extend(page.items);
            match page.next {
                Some(next) => start = Some(next),
                None => return Ok(Read { s0, t0, items }),
            }
        }
    }

    /// Reads the prefix consistently, reports the difference to the
    /// current subscribers, then hands the new one that read's live keys.
    async fn subscribe(&mut self, inner: &Inner, reply: oneshot::Sender<Subscribed>) {
        let read = match self.read(inner, true).await {
            Ok(read) => read,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let snapshot = self.apply(inner, read, true);
        let (events, receiver) = mpsc::unbounded_channel();
        if reply.send(Ok((snapshot, receiver))).is_ok() {
            self.subscribers.push(events);
        }
    }

    /// One poll. A retryable failure judges nothing and waits for the next
    /// interval; a fatal one ends every stream with it.
    async fn tick(&mut self, inner: &Inner) {
        let consistent = self.ks == Keyspace::Ephemeral;
        let started = std::time::Instant::now();
        let read = self.read(inner, consistent).await;
        if let Some(record) = inner.poll_recorder.get() {
            record(started.elapsed());
        }
        match read {
            Ok(read) => {
                self.apply(inner, read, consistent);
            }
            Err(StoreError::Fatal(reason)) => {
                for s in self.subscribers.drain(..) {
                    let _ = s.send(Err(StoreError::Fatal(reason.clone())));
                }
            }
            Err(e) => tracing::debug!(error = %e, prefix = %self.prefix, "a watch poll failed"),
        }
        if self.ks == Keyspace::Ephemeral {
            inner.observed().evict(inner.clock.now());
        }
    }

    /// Reports what `read` changed and returns the keys it holds live.
    fn apply(&mut self, inner: &Inner, read: Read, consistent: bool) -> Vec<Entry> {
        match self.ks {
            Keyspace::Durable => self.apply_durable(read.items, consistent),
            Keyspace::Ephemeral => self.apply_ephemeral(inner, read),
        }
    }

    /// A durable read may be eventually consistent, so only a tombstone
    /// above the delivered revision deletes, and only a revision above both
    /// the delivered one and any tombstone already read puts.
    fn apply_durable(&mut self, items: Vec<(String, Item)>, consistent: bool) -> Vec<Entry> {
        let mut listed = BTreeSet::new();
        let mut snapshot = Vec::new();
        for (key, item) in items {
            let delivered = self.seen.get(&key).copied();
            if item.tomb {
                if delivered.is_some_and(|d| item.v > d) {
                    let revision = Revision(item.v);
                    send(
                        &mut self.subscribers,
                        &WatchEvent::Delete {
                            key: key.clone(),
                            revision,
                        },
                    );
                    self.seen.remove(&key);
                }
                if !self.seen.contains_key(&key) {
                    let floor = self.deleted.entry(key.clone()).or_insert(item.v);
                    *floor = (*floor).max(item.v);
                }
                listed.insert(key);
                continue;
            }
            let floor = delivered.or_else(|| self.deleted.get(&key).copied());
            if floor.is_some_and(|f| item.v <= f) {
                // Unchanged, or an eventually consistent read of the item
                // from before a tombstone this watch read.
                if delivered.is_some() {
                    snapshot.push(entry(&key, &item));
                }
                listed.insert(key);
                continue;
            }
            let live = entry(&key, &item);
            send(&mut self.subscribers, &WatchEvent::Put(live.clone()));
            self.seen.insert(key.clone(), item.v);
            self.deleted.remove(&key);
            snapshot.push(live);
            listed.insert(key);
        }
        if consistent {
            self.seen.retain(|k, _| listed.contains(k));
            self.deleted.retain(|k, _| listed.contains(k));
        }
        snapshot
    }

    /// Applies the expiry rule under the handle's observation lock, and
    /// sends while holding it, so no own write lands between a decision
    /// and its event. A key an own write or a newer read touched after the
    /// read began is left to the next poll.
    fn apply_ephemeral(&mut self, inner: &Inner, read: Read) -> Vec<Entry> {
        let Read { s0, t0, items } = read;
        let mut observed = inner.observed();
        let t1 = inner.clock.now();
        let mut listed = BTreeSet::new();
        let mut snapshot = Vec::new();
        for (key, item) in items {
            listed.insert(key.clone());
            let delivered = self.seen.get(&key).copied();
            if observed.newer_than(&key, s0) {
                if delivered == Some(item.v) {
                    snapshot.push(entry(&key, &item));
                }
                continue;
            }
            observed.observe(&key, Some(item.v), s0, t1);
            if observed.expired(&key, item.v, t0) {
                if let Some(d) = delivered {
                    let revision = Revision(observed.emit_delete(&key, d, true, t1));
                    send(
                        &mut self.subscribers,
                        &WatchEvent::Delete {
                            key: key.clone(),
                            revision,
                        },
                    );
                    self.seen.remove(&key);
                }
                continue;
            }
            let live = entry(&key, &item);
            match delivered {
                Some(d) if d == item.v => {}
                // Deleted and re-created lower between two polls: a delete
                // above what was delivered clears it for the new put.
                Some(d) if d > item.v => {
                    let revision = Revision(observed.emit_delete(&key, d, false, t1));
                    send(
                        &mut self.subscribers,
                        &WatchEvent::Delete {
                            key: key.clone(),
                            revision,
                        },
                    );
                    send(&mut self.subscribers, &WatchEvent::Put(live.clone()));
                    self.seen.insert(key.clone(), item.v);
                }
                _ => {
                    send(&mut self.subscribers, &WatchEvent::Put(live.clone()));
                    self.seen.insert(key.clone(), item.v);
                }
            }
            snapshot.push(live);
        }
        let vanished: Vec<(String, u64)> = self
            .seen
            .iter()
            .filter(|(k, _)| !listed.contains(*k))
            .map(|(k, d)| (k.clone(), *d))
            .collect();
        for (key, d) in vanished {
            if observed.newer_than(&key, s0) {
                continue;
            }
            observed.observe(&key, None, s0, t1);
            let revision = Revision(observed.emit_delete(&key, d, false, t1));
            send(
                &mut self.subscribers,
                &WatchEvent::Delete {
                    key: key.clone(),
                    revision,
                },
            );
            self.seen.remove(&key);
        }
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(v: u64, tomb: bool) -> (String, Item) {
        let item = Item {
            v,
            b: (!tomb).then(|| b"v".to_vec()),
            w: None,
            tomb,
            x: None,
        };
        ("assign.x".to_string(), item)
    }

    /// An eventually consistent read of an older tombstone after a newer one
    /// leaves the floor at the newer, so the older incarnation between them
    /// is not put.
    #[test]
    fn a_floor_never_falls_to_an_older_tombstone() {
        let (events, mut received) = mpsc::unbounded_channel();
        let mut poll = Poll {
            ks: Keyspace::Durable,
            prefix: "assign.".to_string(),
            seen: BTreeMap::new(),
            deleted: BTreeMap::new(),
            subscribers: vec![events],
        };
        for read in [item(4, true), item(2, true), item(3, false)] {
            poll.apply_durable(vec![read], false);
        }
        let event = received.try_recv();
        assert!(event.is_err(), "{event:?}");
    }
}
