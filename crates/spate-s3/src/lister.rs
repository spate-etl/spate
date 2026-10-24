//! A one-pass lister for a feed job: it compares the job fingerprint at the
//! job location, or creates it when there is none after deleting a
//! checkpoint there, deletes an end marker an earlier run left, then
//! publishes one unit per part of each listed object, after the lister's
//! checkpoint when it matched the fingerprint, and writes the end marker.
//!
//! The checkpoint is the last listed key whose units, and those of every key
//! listed before it, the feed has confirmed. The lister rewrites it at most
//! once each [`CHECKPOINT_EVERY`] listed keys and once at the end. It
//! publishes nothing for an object of size 0, publishes each object whose key
//! the S3 client cannot address as one whole unit, and checkpoints at
//! neither. It writes the end marker after the feed confirmed every unit of
//! the run and the last checkpoint is written. `S3Source::list_once` runs it
//! for a lister run, and a solo run on the in-process feed runs it with no
//! location.

use crate::error::{bounded_reason, classify};
use crate::fetch::ObjectEntry;
use crate::listing::{UnaddressableKeys, addressable};
use crate::location::{
    LOCATION_OBJECT_MAX_BYTES, Location, compare, io_error, read_error, read_object, starts_with,
};
use crate::split::Packing;
use crate::unit::units;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutPayload};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::{Confirmation, Feed, FeedErrorKind, Unit};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

/// The most publishes the lister leaves unconfirmed at once. At least one
/// SQS send batch of 10.
pub(crate) const LISTER_IN_FLIGHT: usize = 32;

/// How many keys the lister lists after a checkpoint write before it writes a
/// moved checkpoint again, ahead of the next key: the largest `ListObjectsV2`
/// page AWS returns. A store whose pages hold at most this many keys sees at
/// most one write per page, and one more when the listing ends.
pub(crate) const CHECKPOINT_EVERY: usize = 1000;

/// How many times the lister publishes one unit the feed rejects as
/// retryable before the run fails.
pub(crate) const LISTER_PUBLISH_ATTEMPTS: u32 = 5;

/// The wait before the lister publishes a rejected unit again, doubled on
/// each later attempt.
pub(crate) const LISTER_RETRY_BASE: Duration = Duration::from_millis(200);

/// One lister run over a listing prefix.
pub(crate) struct Lister {
    /// The store the source reads.
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The listing prefix within `store`.
    pub(crate) prefix: Path,
    /// How each object is cut into units.
    pub(crate) packing: Packing,
    /// Where the units go.
    pub(crate) feed: Arc<dyn Feed>,
    /// The job location and its store; `None` on the in-process feed.
    pub(crate) location: Option<(Arc<dyn ObjectStore>, Location)>,
    /// Whether `store` lists the keys after an offset with `start-after`.
    /// Without it the run keeps no checkpoint.
    pub(crate) resume: bool,
    /// How many keys are listed after a checkpoint write before a moved
    /// checkpoint is written again.
    pub(crate) checkpoint_every: usize,
    /// The objects the S3 client takes out of `store`'s listing pages.
    pub(crate) unaddressable: Option<UnaddressableKeys>,
    /// The job fingerprint this run writes.
    pub(crate) fingerprint: String,
    /// The first wait before a unit the feed rejected as retryable is
    /// published again.
    pub(crate) retry_base: Duration,
}

/// A publish the lister has not settled: the unit, kept to publish again,
/// the key of the object it names, and the feed's confirmation.
struct Pending {
    unit: Unit,
    key: String,
    confirmation: Confirmation,
}

/// What the fingerprint write found at the location.
enum Fingerprint {
    Created,
    Matched,
}

impl Lister {
    /// Read the fingerprint and compare it, or delete a checkpoint and create
    /// the fingerprint when there is none, read the checkpoint beside a
    /// fingerprint it matched, delete an earlier end marker, then publish
    /// every listed object's units in the order the store lists them, after
    /// the checkpoint when there is one. An object the S3 client cannot
    /// address is published before the next key the listing yields after its
    /// page is fetched, or when the listing ends, ahead of any key listed
    /// before it on that page. With a location, it then writes the end
    /// marker, holding an id fresh to this run, once the feed has confirmed
    /// every unit and the last checkpoint is written. A unit the feed rejects
    /// as retryable is published again in its place, up to
    /// [`LISTER_PUBLISH_ATTEMPTS`] times.
    ///
    /// # Errors
    ///
    /// Fatal when the location holds another fingerprint, when its store
    /// cannot create an object conditionally, and when the checkpoint is not
    /// a key under the listing prefix. A publish the feed refuses or still
    /// rejects after the last attempt, a listing error, a location error and
    /// a failed checkpoint or end marker write carry their own class.
    pub(crate) async fn run(self) -> Result<(), SourceError> {
        let mut start = None;
        if let Some((store, location)) = &self.location {
            // Decided by the read, since a store that ignores `If-None-Match`
            // answers a create over an existing object 200.
            let found = match read_object(store.as_ref(), location, &location.fingerprint()).await?
            {
                Some(stored) => {
                    compare(location, &self.fingerprint, &stored)?;
                    Fingerprint::Matched
                }
                None => {
                    // A checkpoint beside no fingerprint is not this job's. It
                    // goes before the create, so a run that stops between the
                    // two never leaves it beside a fingerprint, and a run that
                    // keeps none still deletes it for a later run that does.
                    delete_checkpoint(store.as_ref(), location).await?;
                    write_fingerprint(store.as_ref(), location, &self.fingerprint).await?
                }
            };
            if !self.resume {
                tracing::warn!(
                    location = %location.shown(),
                    "the store does not list keys after an offset in its own order, so this \
                     lister run keeps no checkpoint and lists the whole prefix"
                );
            } else if let Fingerprint::Matched = found {
                start = read_checkpoint(store.as_ref(), location, &self.prefix).await?;
            }
            // Before the first publish, so no worker of this run can read a
            // marker an earlier run left.
            delete_end(store.as_ref(), location).await?;
        }
        let published = self.publish_listing(start).await?;
        if let Some((store, location)) = &self.location {
            let run = uuid::Uuid::new_v4().simple().to_string();
            write_end(store.as_ref(), location, &run).await?;
            tracing::info!(
                location = %location.shown(),
                run = %run,
                units = published,
                "wrote the end marker at the job location after the feed confirmed every unit \
                 of this lister run"
            );
        } else {
            tracing::info!(
                units = published,
                "listed the prefix into the feed, which confirmed every unit"
            );
        }
        Ok(())
    }

    /// Publish the listing after `start` and return how many units it
    /// published.
    async fn publish_listing(&self, start: Option<Path>) -> Result<usize, SourceError> {
        let mut pending: VecDeque<Pending> = VecDeque::new();
        let mut published = 0;
        let mut listing = match &start {
            Some(after) => self.store.list_with_offset(Some(&self.prefix), after),
            None => self.store.list(Some(&self.prefix)),
        };
        let mut written = start;
        let mut candidate: Option<Path> = None;
        let mut since_write = 0;
        let mut unaddressable_seen = HashSet::new();
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| SourceError::Client {
                class: classify(&e),
                reason: format!("listing {}: {}", self.prefix, bounded_reason(&e)),
            })?;
            // `object_store` fetches a page before it yields the page's first
            // key, so every unaddressable object up to this page is recorded.
            published += self
                .publish_unaddressable(&mut pending, &mut unaddressable_seen)
                .await?;
            if since_write >= self.checkpoint_every && candidate != written {
                self.settle_all(&mut pending).await?;
                self.write_checkpoint(candidate.as_ref()).await?;
                written.clone_from(&candidate);
                since_write = 0;
            }
            since_write += 1;
            if meta.size == 0 {
                tracing::debug!(key = %meta.location, "not publishing an empty object");
                continue;
            }
            let entry = ObjectEntry {
                key: meta.location.to_string(),
                size: meta.size,
                etag: meta.e_tag,
                last_modified_ms: meta.last_modified.timestamp_millis(),
            };
            let fits =
                addressable(&entry.key) && entry.key.len() as u64 <= LOCATION_OBJECT_MAX_BYTES;
            published += self.publish(entry, &mut pending).await?;
            if fits {
                candidate = Some(meta.location);
            }
        }
        published += self
            .publish_unaddressable(&mut pending, &mut unaddressable_seen)
            .await?;
        self.settle_all(&mut pending).await?;
        if candidate.is_some() && candidate != written {
            self.write_checkpoint(candidate.as_ref()).await?;
        }
        Ok(published)
    }

    /// Publish every part of `entry`, settling the oldest publish first when
    /// [`LISTER_IN_FLIGHT`] are unconfirmed, and return how many it
    /// published.
    async fn publish(
        &self,
        entry: ObjectEntry,
        pending: &mut VecDeque<Pending>,
    ) -> Result<usize, SourceError> {
        let key = entry.key.clone();
        let mut published = 0;
        for unit in units(entry, self.packing) {
            let unit = unit.map_err(|e| SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("minting a unit: {e}"),
            })?;
            if pending.len() == LISTER_IN_FLIGHT
                && let Some(oldest) = pending.pop_front()
            {
                self.settle(oldest).await?;
            }
            let confirmation = self.feed.publish(unit.clone());
            pending.push_back(Pending {
                unit,
                key: key.clone(),
                confirmation,
            });
            published += 1;
        }
        Ok(published)
    }

    /// Wait for `pending`'s confirmation. A retryable rejection publishes the
    /// same unit again after a doubling wait, in the same place in the
    /// settling order, so no checkpoint passes it before it is confirmed.
    async fn settle(&self, pending: Pending) -> Result<(), SourceError> {
        let Pending {
            unit,
            key,
            mut confirmation,
        } = pending;
        let mut attempt = 1;
        loop {
            match confirmation.await {
                Ok(()) => return Ok(()),
                Err(e)
                    if e.kind == FeedErrorKind::Retryable && attempt < LISTER_PUBLISH_ATTEMPTS =>
                {
                    // Per retry, so DEBUG; a unit that exhausts its attempts
                    // fails the run with the feed's error.
                    tracing::debug!(
                        key = ?key,
                        attempt,
                        error = %e,
                        "the feed rejected a unit as retryable; publishing it again"
                    );
                    tokio::time::sleep(self.retry_base * 2u32.pow(attempt - 1)).await;
                    confirmation = self.feed.publish(unit.clone());
                    attempt += 1;
                }
                Err(e) => {
                    return Err(SourceError::Client {
                        class: e.class(),
                        reason: format!("publishing unit {} for {key:?}: {e}", unit.id),
                    });
                }
            }
        }
    }

    /// Settle every pending publish, oldest first.
    async fn settle_all(&self, pending: &mut VecDeque<Pending>) -> Result<(), SourceError> {
        while let Some(oldest) = pending.pop_front() {
            self.settle(oldest).await?;
        }
        Ok(())
    }

    /// Publish each object the S3 client took out of the listing so far, one
    /// whole unit each, which poisons on its first read. A key in `seen` was
    /// published from an earlier page and is not published again.
    async fn publish_unaddressable(
        &self,
        pending: &mut VecDeque<Pending>,
        seen: &mut HashSet<String>,
    ) -> Result<usize, SourceError> {
        let Some(keys) = &self.unaddressable else {
            return Ok(0);
        };
        let mut published = 0;
        for entry in largest_per_key(keys.take()) {
            if entry.size == 0 {
                tracing::debug!(key = ?entry.key, "not publishing an empty object whose key the store cannot address");
                continue;
            }
            if !seen.insert(entry.key.clone()) {
                continue;
            }
            published += self.publish(entry, pending).await?;
        }
        Ok(published)
    }

    /// Overwrite the checkpoint with `key`.
    async fn write_checkpoint(&self, key: Option<&Path>) -> Result<(), SourceError> {
        let (Some((store, location)), Some(key)) = (&self.location, key) else {
            return Ok(());
        };
        if !self.resume {
            return Ok(());
        }
        let object = location.checkpoint();
        store
            .put(&object, PutPayload::from(key.as_ref().to_owned()))
            .await
            .map_err(|e| io_error(location, &object, &e))?;
        Ok(())
    }
}

/// `entries` in their order with one entry per key, the largest, or the
/// first of the largest. XML end-of-line handling decodes keys that differ
/// only in CR, LF or CRLF to one key, and an empty one must not hide one with
/// records.
fn largest_per_key(entries: Vec<ObjectEntry>) -> Vec<ObjectEntry> {
    let mut kept: Vec<ObjectEntry> = Vec::with_capacity(entries.len());
    let mut at: HashMap<String, usize> = HashMap::with_capacity(entries.len());
    for entry in entries {
        match at.get(&entry.key) {
            Some(&i) if entry.size > kept[i].size => kept[i] = entry,
            Some(_) => {}
            None => {
                at.insert(entry.key.clone(), kept.len());
                kept.push(entry);
            }
        }
    }
    kept
}

/// Create the fingerprint object, or compare the one already there.
async fn write_fingerprint(
    store: &dyn ObjectStore,
    location: &Location,
    mine: &str,
) -> Result<Fingerprint, SourceError> {
    let object = location.fingerprint();
    // A second pass covers a fingerprint deleted between the refused
    // create and the read.
    for _ in 0..2 {
        let put = store
            .put_opts(
                &object,
                PutPayload::from(mine.to_owned()),
                PutMode::Create.into(),
            )
            .await;
        match put {
            Ok(_) => return Ok(Fingerprint::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                if let Some(stored) = read_object(store, location, &object).await? {
                    return compare(location, mine, &stored).map(|()| Fingerprint::Matched);
                }
            }
            Err(object_store::Error::NotImplemented { .. }) => {
                return Err(location.fatal(
                    &object,
                    "the store client has conditional put turned off, and the lister creates \
                     the fingerprint only if it is absent; remove `aws_conditional_put: \
                     disabled` from the source's `store`",
                ));
            }
            Err(e) => return Err(io_error(location, &object, &e)),
        }
    }
    Err(location.fatal(
        &object,
        "it was reported present and then read as absent twice; another lister is writing \
         this location",
    ))
}

/// Read the checkpoint, the key to list after, or `None` when there is none.
///
/// # Errors
///
/// Fatal for a body that is not UTF-8, not a key `object_store` addresses
/// under its own name, or not a key under `prefix`.
async fn read_checkpoint(
    store: &dyn ObjectStore,
    location: &Location,
    prefix: &Path,
) -> Result<Option<Path>, SourceError> {
    let object = location.checkpoint();
    let Some(body) = read_object(store, location, &object).await? else {
        return Ok(None);
    };
    let key = std::str::from_utf8(&body)
        .ok()
        .filter(|key| addressable(key))
        .and_then(|key| Path::parse(key).ok())
        .filter(|key| starts_with(key, prefix) && key.parts().count() > prefix.parts().count());
    let Some(key) = key else {
        let shown = String::from_utf8_lossy(&body);
        return Err(location.fatal(
            &object,
            format_args!(
                "it holds {shown:?}, which is not a key under the listing prefix {:?}; \
                 delete it and the lister lists the prefix from the start",
                prefix.as_ref()
            ),
        ));
    };
    tracing::info!(
        location = %location.shown(),
        key = ?key.as_ref(),
        "resuming the listing after the lister's checkpoint"
    );
    Ok(Some(key))
}

/// Delete the checkpoint, if there is one.
async fn delete_checkpoint(
    store: &dyn ObjectStore,
    location: &Location,
) -> Result<(), SourceError> {
    let object = location.checkpoint();
    match store.head(&object).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(read_error(location, &object, &e)),
    }
    match store.delete(&object).await {
        // Gone already: what the delete was for.
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(io_error(location, &object, &e)),
    }
    tracing::info!(
        location = %location.shown(),
        "deleted a checkpoint beside no job fingerprint; the lister lists the whole prefix"
    );
    Ok(())
}

/// Delete the end marker, if there is one.
async fn delete_end(store: &dyn ObjectStore, location: &Location) -> Result<(), SourceError> {
    let end = location.end();
    match store.head(&end).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(read_error(location, &end, &e)),
    }
    match store.delete(&end).await {
        // Gone already: what the delete was for.
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(io_error(location, &end, &e)),
    }
    tracing::info!(
        location = %location.shown(),
        "deleted the end marker an earlier lister run left at the job location"
    );
    Ok(())
}

/// Write the end marker, `run=<run>` and a newline. An overwrite, which
/// `object_store` retries as idempotent, since a conditional create applied
/// before a lost reply would fail its own retry.
async fn write_end(
    store: &dyn ObjectStore,
    location: &Location,
    run: &str,
) -> Result<(), SourceError> {
    let end = location.end();
    store
        .put(&end, PutPayload::from(format!("run={run}\n")))
        .await
        .map_err(|e| io_error(location, &end, &e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::SplitDescriptor;
    use crate::testutil::HookStore;
    use spate_core::feed::memory::MemoryFeed;
    use spate_core::feed::{
        Confirmer, Delivery, FeedError, FeedWaker, Receipt, Unit, confirmation,
    };
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(30);

    fn packing() -> Packing {
        Packing {
            target_bytes: 1 << 20,
            compression: Compression::None,
            delimiter: Some(b'\n'),
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    async fn stage(store: &HookStore, keys: &[String]) {
        for key in keys {
            store
                .inner
                .put(&Path::from(key.as_str()), PutPayload::from_static(b"row\n"))
                .await
                .unwrap();
        }
    }

    fn lister(store: Arc<dyn ObjectStore>, feed: Arc<dyn Feed>) -> Lister {
        Lister {
            store,
            prefix: Path::from("data"),
            packing: packing(),
            feed,
            location: None,
            resume: true,
            checkpoint_every: CHECKPOINT_EVERY,
            unaddressable: None,
            fingerprint: String::new(),
            retry_base: Duration::ZERO,
        }
    }

    /// Pins that the lister publishes in the order the store lists, which
    /// it neither sorts nor collects first.
    #[test]
    fn the_lister_publishes_in_the_order_the_store_lists() {
        let rt = runtime();
        let store = HookStore {
            reverse_list: true,
            ..HookStore::default()
        };
        let keys: Vec<String> = ["a", "b", "c"]
            .iter()
            .map(|k| format!("data/{k}"))
            .collect();
        rt.block_on(stage(&store, &keys));
        let (feed, script) = spate_test::scripted_feed();
        rt.block_on(lister(Arc::new(store), Arc::new(feed)).run())
            .unwrap();
        let published: Vec<String> = script
            .published()
            .iter()
            .map(|u| {
                SplitDescriptor::decode(&u.body).unwrap().objects[0]
                    .key
                    .clone()
            })
            .collect();
        assert_eq!(published, ["data/c", "data/b", "data/a"]);
    }

    /// Pins that a fingerprint reported present and then read as absent on
    /// both passes fails the lister after exactly two conditional creates.
    #[test]
    fn a_fingerprint_absent_after_two_refused_creates_fails_the_lister() {
        let store = HookStore {
            creates_exist: true,
            ..HookStore::default()
        };
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        let err = runtime()
            .block_on(write_fingerprint(&store, &location, "mine"))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("read as absent twice"), "{err}");
        assert_eq!(store.creates.load(Ordering::SeqCst), 2);
    }

    /// Pins that a 403 on the end marker's HEAD, a read, names
    /// `s3:ListBucket`.
    #[test]
    fn a_refused_end_marker_read_names_s3_list_bucket() {
        let store = HookStore {
            get: Some(|path| {
                Some(Err(object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: "403 Forbidden".into(),
                }))
            }),
            ..HookStore::default()
        };
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        let err = runtime()
            .block_on(delete_end(&store, &location))
            .unwrap_err()
            .to_string();
        assert!(err.contains("s3:ListBucket"), "{err}");
    }

    /// Pins that a location with no end marker logs no delete.
    #[test]
    fn a_location_with_no_end_marker_logs_no_delete() {
        let store = HookStore::default();
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        let rt = runtime();
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(delete_end(&store, &location)).unwrap();
        });
        assert!(
            !lines.iter().any(|l| l.contains("deleted the end marker")),
            "{lines:#?}"
        );
    }

    /// A feed that leaves every publish unsettled until the test settles it,
    /// and records the most it held unsettled at once.
    #[derive(Default)]
    struct HoldingFeed {
        held: Mutex<VecDeque<Confirmer>>,
        published: Mutex<usize>,
        most_held: Mutex<usize>,
    }

    impl Feed for HoldingFeed {
        fn backend(&self) -> &str {
            "holding"
        }
        fn set_waker(&self, _waker: FeedWaker) {}
        fn receive(&self, _max: usize) -> Result<Vec<Delivery>, FeedError> {
            Ok(Vec::new())
        }
        fn renew(&self, _receipt: Receipt) -> Result<(), FeedError> {
            Ok(())
        }
        fn acknowledge(&self, _receipt: Receipt) -> Result<(), FeedError> {
            Ok(())
        }
        fn fail(&self, _receipt: Receipt, _reason: &str) -> Result<(), FeedError> {
            Ok(())
        }
        fn publish(&self, _unit: Unit) -> Confirmation {
            let (confirmer, confirmation) = confirmation();
            let mut held = self.held.lock().unwrap();
            held.push_back(confirmer);
            *self.published.lock().unwrap() += 1;
            let mut most = self.most_held.lock().unwrap();
            *most = (*most).max(held.len());
            confirmation
        }
    }

    /// Pins that the lister leaves at most `LISTER_IN_FLIGHT` publishes
    /// unconfirmed over a listing larger than that, and finishes once the
    /// rest are confirmed.
    #[test]
    fn the_lister_keeps_at_most_its_window_of_publishes_unconfirmed() {
        let rt = runtime();
        let store = HookStore::default();
        let total = LISTER_IN_FLIGHT + 8;
        let keys: Vec<String> = (0..total).map(|i| format!("data/{i:03}")).collect();
        rt.block_on(stage(&store, &keys));
        let feed = Arc::new(HoldingFeed::default());
        let run = rt.spawn(lister(Arc::new(store), Arc::clone(&feed) as Arc<dyn Feed>).run());

        let mut settled = 0;
        while settled < total {
            spate_test::wait_until(DEADLINE, "the lister fills its window or ends", || {
                let held = feed.held.lock().unwrap().len();
                held == LISTER_IN_FLIGHT || *feed.published.lock().unwrap() == total
            });
            // Settle the oldest; the lister waits on it before its next
            // publish.
            if let Some(oldest) = feed.held.lock().unwrap().pop_front() {
                oldest.confirm();
                settled += 1;
            }
        }
        rt.block_on(run).unwrap().unwrap();
        assert_eq!(*feed.published.lock().unwrap(), total);
        assert_eq!(*feed.most_held.lock().unwrap(), LISTER_IN_FLIGHT);
    }

    use crate::listing::UnaddressableConnector;
    use crate::split::SplitRange;
    use crate::testutil::{Event, EventLog, PutAnswer};
    use spate_core::feed::FeedErrorKind;
    use std::collections::HashSet;

    const LOCATION: &str = "s3://ops/jobs/a/";
    const CHECKPOINT: &str = "jobs/a/checkpoint";

    /// What a [`KeyedFeed`] does with a publish.
    #[derive(Clone, Copy)]
    enum Reply {
        Confirm,
        Reject(FeedErrorKind),
        Hold,
    }

    /// A feed that answers each publish by its unit's key and range, and
    /// logs every publish and confirmation to a shared event log.
    struct KeyedFeed {
        events: EventLog,
        reply: fn(&str, Option<SplitRange>) -> Reply,
        /// Each published unit's id, key and object size, in publish order.
        published: Mutex<Vec<(String, String, u64)>>,
        /// Each held unit's id and confirmer, in publish order.
        held: Mutex<Vec<(String, Confirmer)>>,
    }

    impl KeyedFeed {
        fn new(events: &EventLog, reply: fn(&str, Option<SplitRange>) -> Reply) -> Arc<KeyedFeed> {
            Arc::new(KeyedFeed {
                events: Arc::clone(events),
                reply,
                published: Mutex::new(Vec::new()),
                held: Mutex::new(Vec::new()),
            })
        }

        fn confirming(events: &EventLog) -> Arc<KeyedFeed> {
            KeyedFeed::new(events, |_, _| Reply::Confirm)
        }

        /// The published keys, one per object, in publish order.
        fn keys(&self) -> Vec<String> {
            let mut keys: Vec<String> = Vec::new();
            for (_, key, _) in self.published.lock().unwrap().iter() {
                if keys.last() != Some(key) {
                    keys.push(key.clone());
                }
            }
            keys
        }

        fn confirm(&self, id: String, confirmer: Confirmer) {
            self.events.lock().unwrap().push(Event::Confirmed(id));
            confirmer.confirm();
        }

        /// Confirm the newest held unit; `false` when none is held.
        fn confirm_newest(&self) -> bool {
            let newest = self.held.lock().unwrap().pop();
            match newest {
                Some((id, confirmer)) => {
                    self.confirm(id, confirmer);
                    true
                }
                None => false,
            }
        }
    }

    impl Feed for KeyedFeed {
        fn backend(&self) -> &str {
            "keyed"
        }
        fn set_waker(&self, _waker: FeedWaker) {}
        fn receive(&self, _max: usize) -> Result<Vec<Delivery>, FeedError> {
            Ok(Vec::new())
        }
        fn renew(&self, _receipt: Receipt) -> Result<(), FeedError> {
            Ok(())
        }
        fn acknowledge(&self, _receipt: Receipt) -> Result<(), FeedError> {
            Ok(())
        }
        fn fail(&self, _receipt: Receipt, _reason: &str) -> Result<(), FeedError> {
            Ok(())
        }
        fn publish(&self, unit: Unit) -> Confirmation {
            let descriptor = SplitDescriptor::decode(&unit.body).unwrap();
            let key = descriptor.objects[0].key.clone();
            let size = descriptor.objects[0].size;
            self.events
                .lock()
                .unwrap()
                .push(Event::Published(unit.id.clone()));
            self.published
                .lock()
                .unwrap()
                .push((unit.id.clone(), key.clone(), size));
            let (confirmer, confirmation) = confirmation();
            match (self.reply)(&key, descriptor.range) {
                Reply::Confirm => self.confirm(unit.id, confirmer),
                Reply::Reject(kind) => confirmer.reject(FeedError::new(kind, "scripted")),
                Reply::Hold => self.held.lock().unwrap().push((unit.id, confirmer)),
            }
            confirmation
        }
    }

    /// A bounded job: a data store under `data/`, a location store at
    /// [`LOCATION`] logging its writes, and the log they share with a feed.
    struct Job {
        data: Arc<HookStore>,
        ops: Arc<HookStore>,
        events: EventLog,
    }

    impl Job {
        fn new() -> Job {
            Job::with_ops(HookStore::default())
        }

        fn with_ops(ops: HookStore) -> Job {
            let events = EventLog::default();
            Job {
                data: Arc::new(HookStore::default()),
                ops: Arc::new(HookStore {
                    events: Some(Arc::clone(&events)),
                    ..ops
                }),
                events,
            }
        }

        /// The same data and location objects behind stores with no hooks
        /// and a fresh log.
        fn rerun(&self) -> Job {
            let events = EventLog::default();
            Job {
                data: Arc::clone(&self.data),
                ops: Arc::new(HookStore {
                    inner: self.ops.inner.fork(),
                    events: Some(Arc::clone(&events)),
                    ..HookStore::default()
                }),
                events,
            }
        }

        fn lister(&self, feed: Arc<dyn Feed>, every: usize) -> Lister {
            let location = Location::parse(LOCATION, "s3://data/events/").unwrap();
            Lister {
                store: Arc::clone(&self.data) as Arc<dyn ObjectStore>,
                prefix: Path::from("data"),
                packing: packing(),
                feed,
                location: Some((Arc::clone(&self.ops) as Arc<dyn ObjectStore>, location)),
                resume: true,
                checkpoint_every: every,
                unaddressable: None,
                fingerprint: "fp".to_owned(),
                retry_base: Duration::ZERO,
            }
        }

        /// Write `body` at the raw `key` in the data store.
        async fn put(&self, key: &str, body: &[u8]) {
            self.data
                .inner
                .put(&Path::parse(key).unwrap(), PutPayload::from(body.to_vec()))
                .await
                .unwrap();
        }

        async fn put_rows(&self, keys: &[&str]) {
            for key in keys {
                self.put(key, b"row\n").await;
            }
        }

        /// Store this job's fingerprint, so a lister run matches it.
        async fn matched(&self) {
            self.ops
                .inner
                .put(
                    &Path::from("jobs/a/fingerprint"),
                    PutPayload::from_static(b"fp"),
                )
                .await
                .unwrap();
        }

        async fn seed_checkpoint(&self, body: &[u8]) {
            self.ops
                .inner
                .put(&Path::from(CHECKPOINT), PutPayload::from(body.to_vec()))
                .await
                .unwrap();
        }

        async fn checkpoint(&self) -> Option<String> {
            match self.ops.inner.get(&Path::from(CHECKPOINT)).await {
                Ok(got) => Some(String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => panic!("{e}"),
            }
        }

        /// Every checkpoint body written, in order.
        fn checkpoint_writes(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    Event::Put(path, body) if path == CHECKPOINT => {
                        Some(String::from_utf8(body.clone()).unwrap())
                    }
                    _ => None,
                })
                .collect()
        }
    }

    fn class(err: &SourceError) -> ErrorClass {
        match err {
            SourceError::Client { class, .. } => *class,
            other => panic!("expected a client error, got {other:?}"),
        }
    }

    /// Pins the commit rule: with a checkpoint written before every key and
    /// the feed confirming later units first, each write names a key whose
    /// units, its four ranges included, and every earlier key's units were
    /// confirmed before the write.
    #[test]
    fn every_checkpoint_write_names_a_key_whose_listing_prefix_is_confirmed() {
        let rt = runtime();
        let job = Job::new();
        let rows: Vec<u8> = (0..25)
            .flat_map(|i| format!("row-{i:03}\n").into_bytes())
            .collect();
        assert_eq!(rows.len(), 200);
        rt.block_on(async {
            job.put("data/a", b"row\n").await;
            job.put("data/b", &rows).await;
            job.put("data/c", b"row\n").await;
        });
        let feed = KeyedFeed::new(&job.events, |_, _| Reply::Hold);
        let mut lister = job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1);
        lister.packing.target_bytes = 64;
        let run = rt.spawn(lister.run());
        while !run.is_finished() {
            spate_test::wait_until(DEADLINE, "a held unit or the run's end", || {
                !feed.held.lock().unwrap().is_empty() || run.is_finished()
            });
            feed.confirm_newest();
        }
        rt.block_on(run).unwrap().unwrap();

        let published = feed.published.lock().unwrap().clone();
        assert_eq!(
            published
                .iter()
                .filter(|(_, key, _)| key == "data/b")
                .count(),
            4,
            "data/b is read in four ranges"
        );
        let events = job.events.lock().unwrap().clone();
        let mut confirmed = HashSet::new();
        let mut written = Vec::new();
        for event in &events {
            match event {
                Event::Confirmed(id) => {
                    confirmed.insert(id.clone());
                }
                Event::Put(path, body) if path == CHECKPOINT => {
                    let key = String::from_utf8(body.clone()).unwrap();
                    for (id, unit_key, _) in &published {
                        if *unit_key <= key {
                            assert!(confirmed.contains(id), "{id} of {unit_key} before {key}");
                        }
                    }
                    written.push(key);
                }
                _ => {}
            }
        }
        assert_eq!(written, ["data/a", "data/b", "data/c"]);
    }

    /// Pins that a unit the feed refuses holds the checkpoint at the key
    /// before its own, and fails the run with the refusal's class.
    #[test]
    fn a_failed_batch_entry_holds_the_checkpoint_before_its_key() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c", "data/d"]));
        let feed = KeyedFeed::new(&job.events, |key, _| match key {
            "data/c" => Reply::Reject(FeedErrorKind::Refused),
            _ => Reply::Confirm,
        });
        let err = rt.block_on(job.lister(feed, 2).run()).unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/b"));
    }

    /// Pins that a unit whose publish times out on every attempt holds the
    /// checkpoint at the key before its own, though a later key's unit was
    /// confirmed first.
    #[test]
    fn a_timed_out_produce_holds_the_checkpoint() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c", "data/d", "data/e"]));
        let feed = KeyedFeed::new(&job.events, |key, _| match key {
            "data/c" => Reply::Hold,
            _ => Reply::Confirm,
        });
        let run = rt.spawn(job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 2).run());
        spate_test::wait_until(DEADLINE, "data/d is confirmed while data/c is held", || {
            feed.keys().contains(&"data/d".to_owned())
        });
        let mut rejected = 0;
        while !run.is_finished() {
            spate_test::wait_until(DEADLINE, "a held data/c or the run's end", || {
                !feed.held.lock().unwrap().is_empty() || run.is_finished()
            });
            if let Some((_, held)) = feed.held.lock().unwrap().pop() {
                held.reject(FeedError::new(
                    FeedErrorKind::Retryable,
                    "message timed out",
                ));
                rejected += 1;
                assert!(
                    rejected <= LISTER_PUBLISH_ATTEMPTS,
                    "published past the budget"
                );
            }
        }
        assert_eq!(rejected, LISTER_PUBLISH_ATTEMPTS);
        let err = rt.block_on(run).unwrap().unwrap_err();
        assert_eq!(class(&err), ErrorClass::Retryable, "{err}");
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/b"));
        assert_eq!(job.checkpoint_writes(), ["data/b"]);
    }

    /// Pins that a key read in ranges is checkpointed only once every range
    /// is confirmed: a refused last range holds the checkpoint at the key
    /// before it.
    #[test]
    fn every_range_of_a_key_is_confirmed_before_it_is_checkpointed() {
        let rt = runtime();
        let rows: Vec<u8> = (0..25)
            .flat_map(|i| format!("row-{i:03}\n").into_bytes())
            .collect();
        for (reply, want) in [
            (
                (|_: &str, range: Option<SplitRange>| match range {
                    Some(r) if r.end == 200 => Reply::Reject(FeedErrorKind::Refused),
                    _ => Reply::Confirm,
                }) as fn(&str, Option<SplitRange>) -> Reply,
                "data/a",
            ),
            (|_, _| Reply::Confirm, "data/b"),
        ] {
            let job = Job::new();
            rt.block_on(async {
                job.put("data/a", b"row\n").await;
                job.put("data/b", &rows).await;
            });
            let feed = KeyedFeed::new(&job.events, reply);
            let mut lister = job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1);
            lister.packing.target_bytes = 64;
            let _ = rt.block_on(lister.run());
            assert_eq!(
                feed.published
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, key, _)| key == "data/b")
                    .count(),
                4
            );
            assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some(want));
        }
    }

    /// Pins that an object of size 0 is not published and never becomes the
    /// checkpoint, while a one-byte object is, that an empty object leaves
    /// the checkpoint unwritten after it, and that a listing of empty objects
    /// alone publishes and writes nothing.
    #[test]
    fn a_zero_byte_object_is_skipped_and_never_checkpointed() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put("data/a", b"row\n").await;
            job.put("data/a0", b"").await;
            job.put("data/b", b"\n").await;
            job.put("data/x", b"").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1).run())
            .unwrap();
        assert_eq!(feed.keys(), ["data/a", "data/b"]);
        // No write repeats the key before an empty object, mid-listing or at
        // the end.
        assert_eq!(job.checkpoint_writes(), ["data/a", "data/b"]);

        let empty = Job::new();
        rt.block_on(async {
            empty.put("data/x", b"").await;
            empty.put("data/y", b"").await;
        });
        let feed = KeyedFeed::confirming(&empty.events);
        rt.block_on(empty.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1).run())
            .unwrap();
        assert!(feed.keys().is_empty());
        assert_eq!(rt.block_on(empty.checkpoint()), None);
    }

    /// Pins the write cadence: before every `checkpoint_every`-th next key
    /// and once at the end.
    #[test]
    fn the_checkpoint_is_written_once_per_interval_of_listed_keys() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c", "data/d", "data/e"]));
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(job.lister(feed, 2).run()).unwrap();
        assert_eq!(job.checkpoint_writes(), ["data/b", "data/d", "data/e"]);
    }

    /// Pins that the checkpoint is the last key in the store's listing
    /// order, which the lister does not sort.
    #[test]
    fn the_checkpoint_is_the_last_key_listed() {
        let rt = runtime();
        let job = Job {
            data: Arc::new(HookStore {
                reverse_list: true,
                ..HookStore::default()
            }),
            ..Job::new()
        };
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c"]));
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/c", "data/b", "data/a"]);
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/a"));
    }

    /// Pins that a resume lists after the checkpoint's own bytes, for keys
    /// `Path::from` would encode, and writes the listed key's bytes back.
    #[test]
    fn a_resume_lists_after_the_checkpoint_byte_for_byte() {
        let rt = runtime();
        let long = format!("data/{}", "k".repeat(1019));
        let long_next = format!("{long}2");
        assert_eq!(long.len(), 1024);
        for (first, second) in [
            ("data/a#1", "data/a#2"),
            ("data/\u{e9}1", "data/\u{e9}2"),
            (long.as_str(), long_next.as_str()),
        ] {
            let job = Job::new();
            rt.block_on(async {
                job.put_rows(&[first, second]).await;
                job.matched().await;
                job.seed_checkpoint(first.as_bytes()).await;
            });
            let feed = KeyedFeed::confirming(&job.events);
            rt.block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap();
            assert_eq!(
                *job.data.offsets.lock().unwrap(),
                [Path::parse(first).unwrap()]
            );
            assert_eq!(feed.keys(), [second]);
            assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some(second));
        }
    }

    /// Pins that a checkpoint that is not a key the lister could have
    /// written, under its listing prefix, fails the run before it publishes,
    /// quoting the body with its control characters escaped.
    #[test]
    fn a_checkpoint_that_is_not_an_addressable_key_under_the_prefix_is_fatal() {
        let rt = runtime();
        for body in [
            &b"\xff"[..],
            b"data//b",
            b"data/x/",
            b"data/a\nb",
            b"other/x",
            b"data",
        ] {
            let job = Job::new();
            rt.block_on(async {
                job.put_rows(&["data/a", "data/b"]).await;
                job.matched().await;
                job.seed_checkpoint(body).await;
            });
            let feed = KeyedFeed::confirming(&job.events);
            let err = rt
                .block_on(
                    job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                        .run(),
                )
                .unwrap_err();
            assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
            let text = err.to_string();
            assert!(text.contains("checkpoint"), "{text}");
            assert!(!text.contains('\n'), "{text}");
            assert!(feed.keys().is_empty(), "{body:?}");
            if body == b"data/a\nb" {
                assert!(text.contains("a\\nb"), "{text}");
            }
            if body == b"other/x" {
                assert!(text.contains("prefix \"data\""), "{text}");
            }
        }
    }

    /// Pins that a 403 reading the checkpoint names `s3:ListBucket`.
    #[test]
    fn a_refused_checkpoint_read_names_s3_list_bucket() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            get: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| {
                    Err(object_store::Error::PermissionDenied {
                        path: path.to_string(),
                        source: "403 Forbidden".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.matched());
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(job.lister(feed, CHECKPOINT_EVERY).run())
            .unwrap_err()
            .to_string();
        assert!(err.contains("s3:ListBucket"), "{err}");
    }

    /// Pins that a refused checkpoint write fails the run, naming the
    /// checkpoint.
    #[test]
    fn a_failed_checkpoint_write_fails_the_run() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            put: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| {
                    PutAnswer::Fail(object_store::Error::PermissionDenied {
                        path: path.to_string(),
                        source: "403 Forbidden".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.put_rows(&["data/a"]));
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(job.lister(feed, CHECKPOINT_EVERY).run())
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert!(
            err.to_string().contains("checkpoint at the job location"),
            "{err}"
        );
    }

    /// Pins that a checkpoint stored with its reply lost fails the run as
    /// retryable, and the next run resumes after the stored key.
    #[test]
    fn a_checkpoint_written_with_its_reply_lost_resumes_after_it() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            put: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| {
                    PutAnswer::StoreThenFail(object_store::Error::Generic {
                        store: "test",
                        source: "connection reset".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c"]));
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt.block_on(job.lister(feed, 2).run()).unwrap_err();
        assert_eq!(class(&err), ErrorClass::Retryable, "{err}");

        let again = job.rerun();
        assert_eq!(rt.block_on(again.checkpoint()).as_deref(), Some("data/b"));
        let feed = KeyedFeed::confirming(&again.events);
        rt.block_on(again.lister(Arc::clone(&feed) as Arc<dyn Feed>, 2).run())
            .unwrap();
        assert_eq!(feed.keys(), ["data/c"]);
    }

    /// Pins that a run after one whose last unit the feed refused publishes
    /// exactly the keys after the checkpoint the first run left.
    #[test]
    fn a_rejected_unit_holds_the_checkpoint_and_a_resumed_run_publishes_from_it() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c", "data/d"]));
        let feed = KeyedFeed::new(&job.events, |key, _| match key {
            "data/d" => Reply::Reject(FeedErrorKind::Refused),
            _ => Reply::Confirm,
        });
        rt.block_on(job.lister(feed, 2).run()).unwrap_err();
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/b"));

        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 2).run())
            .unwrap();
        assert_eq!(feed.keys(), ["data/c", "data/d"]);
    }

    /// Pins the cap on a checkpoint key: a key of the location's object cap
    /// is checkpointed and read back, and one byte longer is published but
    /// never checkpointed.
    #[test]
    fn a_key_above_the_location_cap_is_never_checkpointed() {
        let rt = runtime();
        let cap = LOCATION_OBJECT_MAX_BYTES as usize;
        let at_cap = format!("data/{}", "k".repeat(cap - 5));
        let over_cap = format!("data/{}", "k".repeat(cap - 4));
        for (long, want) in [(&at_cap, at_cap.as_str()), (&over_cap, "data/a")] {
            let job = Job::new();
            rt.block_on(job.put_rows(&["data/a", long]));
            let feed = KeyedFeed::confirming(&job.events);
            rt.block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap();
            assert_eq!(feed.keys(), ["data/a", long.as_str()]);
            assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some(want));

            let feed = KeyedFeed::confirming(&job.events);
            rt.block_on(job.lister(feed, CHECKPOINT_EVERY).run())
                .unwrap();
        }
    }

    /// Pins that a lister over a store it cannot resume on reads no
    /// checkpoint, writes none, publishes every key and warns once.
    #[test]
    fn a_lister_that_cannot_resume_reads_and_writes_no_checkpoint() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            get: Some(|path| {
                assert_ne!(
                    path.filename(),
                    Some("checkpoint"),
                    "the checkpoint is read"
                );
                None
            }),
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b", "data/c"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        let mut lister = job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1);
        lister.resume = false;
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(lister.run()).unwrap();
        });
        assert_eq!(feed.keys(), ["data/a", "data/b", "data/c"]);
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/b"));
        assert!(job.checkpoint_writes().is_empty());
        assert!(job.data.offsets.lock().unwrap().is_empty());
        let warned: Vec<&String> = lines
            .iter()
            .filter(|l| l.contains("keeps no checkpoint"))
            .collect();
        assert_eq!(warned.len(), 1, "{lines:#?}");
        assert!(
            warned[0].contains("WARN") && warned[0].contains(LOCATION),
            "{lines:#?}"
        );
    }

    /// Pins that a lister creating the fingerprint deletes a checkpoint it
    /// finds before its first publish, and lists from the start.
    #[test]
    fn a_lister_that_creates_the_fingerprint_lists_from_the_start() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b", "data/c"]).await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/a", "data/b", "data/c"]);
        let events = job.events.lock().unwrap().clone();
        let deleted = events
            .iter()
            .position(|e| *e == Event::Delete(CHECKPOINT.to_owned()))
            .expect("the checkpoint is deleted");
        let first = events
            .iter()
            .position(|e| matches!(e, Event::Published(_)))
            .unwrap();
        assert!(deleted < first, "{events:#?}");
    }

    /// Pins that a resumed run logs the location and the key it resumes
    /// after.
    #[test]
    fn a_resumed_lister_logs_the_checkpoint_it_resumes_after() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/a").await;
        });
        let lister = job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(lister.run()).unwrap();
        });
        assert!(
            lines.iter().any(|l| l.contains("INFO")
                && l.contains("resuming the listing")
                && l.contains(LOCATION)
                && l.contains("\"data/a\"")),
            "{lines:#?}"
        );
    }

    /// Pins that a run with no checkpoint logs no resume.
    #[test]
    fn a_lister_with_no_checkpoint_logs_no_resume() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a"]).await;
            job.matched().await;
        });
        let lister = job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(lister.run()).unwrap();
        });
        assert!(
            !lines.iter().any(|l| l.contains("resuming the listing")),
            "{lines:#?}"
        );
    }

    /// Pins that an empty listing ends without a checkpoint.
    #[test]
    fn an_empty_listing_writes_no_checkpoint() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.matched());
        rt.block_on(job.lister(KeyedFeed::confirming(&job.events), 1).run())
            .unwrap();
        assert!(job.checkpoint_writes().is_empty());
        assert_eq!(rt.block_on(job.checkpoint()), None);
    }

    /// Pins that a one-key listing at a fresh location checkpoints that key,
    /// and sends no checkpoint delete there.
    #[test]
    fn a_one_key_listing_checkpoints_that_key() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a"]));
        rt.block_on(
            job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(job.checkpoint_writes(), ["data/a"]);
        let events = job.events.lock().unwrap().clone();
        assert!(
            !events.contains(&Event::Delete(CHECKPOINT.to_owned())),
            "{events:#?}"
        );
    }

    /// Pins that a checkpoint at the last key publishes nothing and leaves
    /// the checkpoint as it was.
    #[test]
    fn a_checkpoint_at_the_last_key_publishes_nothing() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1).run())
            .unwrap();
        assert!(feed.keys().is_empty());
        assert_eq!(*job.data.offsets.lock().unwrap(), [Path::from("data/b")]);
        assert!(job.checkpoint_writes().is_empty());
    }

    /// Pins that a checkpoint naming a key since deleted resumes after it.
    #[test]
    fn a_checkpoint_at_a_deleted_key_resumes_after_it() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a", "data/c"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/c"]);
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/c"));
    }

    fn list_page(objects: &[(&str, u64)], token: Option<&str>) -> String {
        let contents: String = objects
            .iter()
            .map(|(key, size)| {
                format!(
                    "<Contents><Key>{key}</Key><LastModified>2026-10-11T13:38:17.000Z\
                     </LastModified><ETag>&quot;e&quot;</ETag><Size>{size}</Size></Contents>"
                )
            })
            .collect();
        let next = token.map_or_else(String::new, |t| {
            format!("<NextContinuationToken>{t}</NextContinuationToken>")
        });
        format!(
            "<ListBucketResult><IsTruncated>{}</IsTruncated>{contents}{next}</ListBucketResult>",
            token.is_some()
        )
    }

    /// Pins that each object the S3 client cannot address and that has bytes
    /// is published once as one whole unit, though keys that decode alike
    /// span two pages, before the checkpoint after it is written, that one on
    /// a last page holding no other key is published, and that none becomes
    /// the checkpoint.
    #[test]
    fn an_unaddressable_key_with_bytes_is_published_and_never_checkpointed() {
        let rt = runtime();
        let url = crate::test_servers::listing_server(vec![
            list_page(
                &[("data/a", 7), ("data/d//x.ndjson", 7), ("data/b", 7)],
                Some("1"),
            ),
            list_page(
                &[("data/a\r\nb", 9), ("data/e\nf/", 0), ("data/z", 7)],
                Some("2"),
            ),
            // No addressable key: only the drain after the listing ends
            // publishes this page.
            list_page(&[("data/a\rb", 3), ("data/q//r", 5)], None),
        ]);
        let keys = UnaddressableKeys::default();
        let store = crate::test_servers::builder_at(&url)
            .with_access_key_id("AKIDTEST")
            .with_secret_access_key("secret")
            .with_http_connector(UnaddressableConnector::new(keys.clone()))
            .build()
            .unwrap();
        let job = Job::new();
        let feed = KeyedFeed::confirming(&job.events);
        let mut lister = job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 2);
        lister.store = Arc::new(store);
        lister.unaddressable = Some(keys);
        let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
            rt.block_on(lister.run()).unwrap();
        });

        assert_eq!(
            feed.keys(),
            [
                "data/d//x.ndjson",
                "data/a",
                "data/b",
                "data/a\nb",
                "data/z",
                "data/q//r"
            ]
        );
        // The empty object's key is logged escaped, on one line.
        assert!(
            lines.iter().any(|l| l.contains("data/e\\nf/")),
            "{lines:#?}"
        );
        assert!(!lines.iter().any(|l| l.starts_with("f/")), "{lines:#?}");
        let published = feed.published.lock().unwrap().clone();
        let crlf: Vec<u64> = published
            .iter()
            .filter(|(_, key, _)| key == "data/a\nb")
            .map(|(_, _, size)| *size)
            .collect();
        assert_eq!(
            crlf,
            [9],
            "one whole unit, from the first page that lists it"
        );
        let (dbl, _, _) = published
            .iter()
            .find(|(_, key, _)| key == "data/d//x.ndjson")
            .unwrap();
        assert_eq!(job.checkpoint_writes(), ["data/b", "data/z"]);
        let events = job.events.lock().unwrap().clone();
        let confirmed = events
            .iter()
            .position(|e| *e == Event::Confirmed(dbl.clone()))
            .unwrap();
        let first_write = events
            .iter()
            .position(|e| matches!(e, Event::Put(path, _) if path == CHECKPOINT))
            .unwrap();
        assert!(confirmed < first_write, "{events:#?}");
    }

    fn entry(key: &str, size: u64, etag: &str) -> ObjectEntry {
        ObjectEntry {
            key: key.to_owned(),
            size,
            etag: Some(etag.to_owned()),
            last_modified_ms: 0,
        }
    }

    /// Pins that entries sharing a key collapse to the largest whichever
    /// comes first, to the first of equal ones, and keep their order.
    #[test]
    fn largest_per_key_keeps_the_largest_entry_in_page_order() {
        let kept = largest_per_key(vec![
            entry("data/b", 9, "big"),
            entry("data/a", 1, "a"),
            entry("data/b", 3, "small"),
            entry("data/c", 7, "first"),
            entry("data/c", 7, "second"),
            entry("data/d", 2, "small"),
            entry("data/d", 8, "big"),
        ]);
        let kept: Vec<(&str, u64, &str)> = kept
            .iter()
            .map(|e| (e.key.as_str(), e.size, e.etag.as_deref().unwrap()))
            .collect();
        assert_eq!(
            kept,
            [
                ("data/b", 9, "big"),
                ("data/a", 1, "a"),
                ("data/c", 7, "first"),
                ("data/d", 8, "big")
            ]
        );
    }

    /// Pins that a run whose fingerprint create was stored but answered with
    /// an error leaves no earlier checkpoint for the next run to resume from.
    #[test]
    fn a_lost_fingerprint_create_reply_leaves_no_foreign_checkpoint() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            put: Some(|path| {
                (path.filename() == Some("fingerprint")).then(|| {
                    PutAnswer::StoreThenFail(object_store::Error::Generic {
                        store: "S3",
                        source: "connection reset".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b", "data/c"]).await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(job.lister(feed, CHECKPOINT_EVERY).run())
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Retryable, "{err}");

        let again = job.rerun();
        let feed = KeyedFeed::confirming(&again.events);
        rt.block_on(
            again
                .lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/a", "data/b", "data/c"]);
    }

    /// Pins that a lister resumes on a store that applies a conditional
    /// create over an existing object, and that it sends no create there.
    #[test]
    fn a_store_that_ignores_conditional_create_still_resumes() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            creates_overwrite: true,
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b", "data/c"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/c"]);
        assert_eq!(job.ops.creates.load(Ordering::SeqCst), 0);
    }

    /// Pins that a checkpoint found beside no fingerprint is deleted with an
    /// INFO line naming the location, by a run that keeps a checkpoint and by
    /// one that keeps none.
    #[test]
    fn a_deleted_foreign_checkpoint_is_logged() {
        let rt = runtime();
        for resume in [true, false] {
            let job = Job::new();
            rt.block_on(async {
                job.put_rows(&["data/a"]).await;
                job.seed_checkpoint(b"data/a").await;
            });
            let mut lister = job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY);
            lister.resume = resume;
            let lines = spate_test::capture_logs(tracing::Level::INFO, || {
                rt.block_on(lister.run()).unwrap();
            });
            assert!(
                lines.iter().any(|l| l.contains("INFO")
                    && l.contains("deleted a checkpoint")
                    && l.contains(LOCATION)),
                "{lines:#?}"
            );
            let events = job.events.lock().unwrap().clone();
            assert!(
                events.contains(&Event::Delete(CHECKPOINT.to_owned())),
                "resume {resume}: {events:#?}"
            );
        }
    }

    /// The error S3 gives for one key a `DeleteObjects` request refused.
    fn refused_delete(path: &Path) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: format!(
                "DeleteObjects request failed for key {path}: Access Denied (code: AccessDenied)"
            )
            .into(),
        }
    }

    /// Pins that a refused delete of a checkpoint found beside no
    /// fingerprint fails the run as fatal before it creates the fingerprint
    /// or publishes, and leaves the checkpoint in place.
    #[test]
    fn a_refused_checkpoint_delete_is_fatal() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            delete: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| refused_delete(path))
            }),
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a"]).await;
            job.seed_checkpoint(b"data/a").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert!(feed.keys().is_empty());
        assert!(
            rt.block_on(job.ops.inner.head(&Path::from("jobs/a/fingerprint")))
                .is_err()
        );
        assert_eq!(rt.block_on(job.checkpoint()).as_deref(), Some("data/a"));
    }

    /// Pins that a refused delete of an earlier end marker fails the run as
    /// fatal before it publishes, and leaves the marker in place.
    #[test]
    fn a_refused_end_marker_delete_fails_the_run() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            delete: Some(|path| (path.filename() == Some("end")).then(|| refused_delete(path))),
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a"]).await;
            job.matched().await;
            job.ops
                .inner
                .put(
                    &Path::from("jobs/a/end"),
                    PutPayload::from_static(b"run=earlier\n"),
                )
                .await
                .unwrap();
        });
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert!(feed.keys().is_empty());
        assert!(
            rt.block_on(job.ops.inner.head(&Path::from("jobs/a/end")))
                .is_ok()
        );
    }

    const END: &str = "jobs/a/end";

    impl Job {
        /// The end marker's body, if there is one.
        async fn end(&self) -> Option<String> {
            match self.ops.inner.get(&Path::from(END)).await {
                Ok(got) => Some(String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => panic!("{e}"),
            }
        }
    }

    /// Whether `body` is an end marker: `run=`, 32 lowercase hex digits and
    /// a newline.
    fn is_marker(body: &str) -> bool {
        body.strip_prefix("run=")
            .and_then(|rest| rest.strip_suffix('\n'))
            .is_some_and(|id| {
                id.len() == 32
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
    }

    /// Pins that the end marker is written once, after the feed confirmed
    /// every unit and after the last checkpoint write, with the feed
    /// confirming later units first.
    #[test]
    fn the_end_marker_follows_every_confirmation_and_the_last_checkpoint() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c"]));
        let feed = KeyedFeed::new(&job.events, |_, _| Reply::Hold);
        let run = rt.spawn(job.lister(Arc::clone(&feed) as Arc<dyn Feed>, 1).run());
        while !run.is_finished() {
            spate_test::wait_until(DEADLINE, "a held unit or the run's end", || {
                !feed.held.lock().unwrap().is_empty() || run.is_finished()
            });
            feed.confirm_newest();
        }
        rt.block_on(run).unwrap().unwrap();
        let events = job.events.lock().unwrap().clone();
        let at = |want: &dyn Fn(&Event) -> bool| events.iter().rposition(want).unwrap();
        let end = events
            .iter()
            .position(|e| matches!(e, Event::Put(path, _) if path == END))
            .expect("an end marker");
        assert_eq!(end, events.len() - 1, "{events:#?}");
        assert!(
            at(&|e| matches!(e, Event::Confirmed(_))) < end,
            "{events:#?}"
        );
        assert!(
            at(&|e| matches!(e, Event::Put(path, _) if path == CHECKPOINT)) < end,
            "{events:#?}"
        );
        assert!(is_marker(&rt.block_on(job.end()).unwrap()));
    }

    /// Pins that a rerun deletes the marker a finished run left before its
    /// first publish, and writes one holding another run id.
    #[test]
    fn a_rerun_after_a_finished_run_replaces_its_marker() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a"]));
        rt.block_on(
            job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        let first = rt.block_on(job.end()).unwrap();

        let again = job.rerun();
        rt.block_on(again.put_rows(&["data/b"]));
        rt.block_on(
            again
                .lister(KeyedFeed::confirming(&again.events), CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        let events = again.events.lock().unwrap().clone();
        let deleted = events
            .iter()
            .position(|e| *e == Event::Delete(END.to_owned()))
            .expect("the earlier marker is deleted");
        let published = events
            .iter()
            .position(|e| matches!(e, Event::Published(_)))
            .unwrap();
        assert!(deleted < published, "{events:#?}");
        let second = rt.block_on(again.end()).unwrap();
        assert!(
            is_marker(&first) && is_marker(&second),
            "{first:?} {second:?}"
        );
        assert_ne!(first, second);
        assert!(second.len() as u64 <= LOCATION_OBJECT_MAX_BYTES);
    }

    /// Pins that a run whose checkpoint is the last key publishes nothing
    /// and still writes the end marker.
    #[test]
    fn a_rerun_after_its_last_checkpoint_publishes_nothing_and_writes_the_marker() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b"]).await;
            job.matched().await;
            job.seed_checkpoint(b"data/b").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert!(feed.keys().is_empty());
        assert!(is_marker(&rt.block_on(job.end()).unwrap()));
    }

    /// Pins that a run writes the end marker whatever it publishes: nothing,
    /// only empty objects, one object, and a window's worth and one more,
    /// each marker after every confirmation.
    #[test]
    fn a_lister_run_writes_its_marker_whatever_it_publishes() {
        let rt = runtime();
        for (objects, empty) in [
            (0, false),
            (2, true),
            (1, false),
            (LISTER_IN_FLIGHT, false),
            (LISTER_IN_FLIGHT + 1, false),
        ] {
            let job = Job::new();
            rt.block_on(async {
                for i in 0..objects {
                    let body: &[u8] = if empty { b"" } else { b"row\n" };
                    job.put(&format!("data/{i:03}"), body).await;
                }
            });
            let feed = KeyedFeed::confirming(&job.events);
            rt.block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap();
            let want = if empty { 0 } else { objects };
            assert_eq!(feed.keys().len(), want, "{objects} objects");
            let events = job.events.lock().unwrap().clone();
            let end = events
                .iter()
                .position(|e| matches!(e, Event::Put(path, _) if path == END))
                .unwrap_or_else(|| panic!("{objects} objects: no marker"));
            assert!(
                !events[end..]
                    .iter()
                    .any(|e| matches!(e, Event::Confirmed(_))),
                "{objects} objects: {events:#?}"
            );
        }
    }

    /// Pins that the end marker is an overwrite: a store answering every
    /// conditional create `AlreadyExists` still gets the marker, and the run
    /// sends no create.
    #[test]
    fn the_end_marker_is_written_without_a_conditional_create() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            creates_exist: true,
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a"]).await;
            job.matched().await;
        });
        rt.block_on(
            job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert!(is_marker(&rt.block_on(job.end()).unwrap()));
        assert_eq!(job.ops.creates.load(Ordering::SeqCst), 0);
    }

    /// Pins that a refused end marker write fails the run as fatal, naming
    /// the marker and the location.
    #[test]
    fn a_failed_end_marker_write_fails_the_run() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            put: Some(|path| {
                (path.filename() == Some("end")).then(|| {
                    PutAnswer::Fail(object_store::Error::PermissionDenied {
                        path: path.to_string(),
                        source: "403 Forbidden".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.put_rows(&["data/a"]));
        let err = rt
            .block_on(
                job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        let text = err.to_string();
        assert!(
            text.contains("end at the job location") && text.contains(LOCATION),
            "{text}"
        );
        assert_eq!(rt.block_on(job.end()), None);
    }

    /// Pins that an end marker stored with its reply lost fails the run as
    /// retryable after every unit was confirmed, and that the next run
    /// deletes it before its first publish and writes its own.
    #[test]
    fn an_end_marker_stored_with_its_reply_lost_fails_the_run() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            put: Some(|path| {
                (path.filename() == Some("end")).then(|| {
                    PutAnswer::StoreThenFail(object_store::Error::Generic {
                        store: "test",
                        source: "connection reset".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.put_rows(&["data/a", "data/b"]));
        let err = rt
            .block_on(
                job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Retryable, "{err}");
        let first = rt
            .block_on(job.end())
            .expect("the marker reached the store");
        let events = job.events.lock().unwrap().clone();
        let end = events
            .iter()
            .position(|e| matches!(e, Event::Put(path, _) if path == END))
            .unwrap();
        assert_eq!(
            events[..end]
                .iter()
                .filter(|e| matches!(e, Event::Confirmed(_)))
                .count(),
            2,
            "{events:#?}"
        );

        let again = job.rerun();
        rt.block_on(
            again
                .lister(KeyedFeed::confirming(&again.events), CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        let events = again.events.lock().unwrap().clone();
        assert_eq!(
            events.first(),
            Some(&Event::Delete(END.to_owned())),
            "{events:#?}"
        );
        assert_ne!(rt.block_on(again.end()).unwrap(), first);
    }

    /// Pins the retry of a unit the feed rejects as retryable: four
    /// rejections are followed by four waits doubling from the retry base,
    /// on paused tokio time, and the fifth publish of the same unit
    /// completes the run.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_rejection_is_published_again_in_place() {
        let job = Job::new();
        job.put_rows(&["data/a"]).await;
        let (feed, script) = spate_test::scripted_feed();
        for _ in 0..4 {
            script.refuse_next_publish(FeedErrorKind::Retryable);
        }
        let mut lister = job.lister(Arc::new(feed), CHECKPOINT_EVERY);
        lister.retry_base = LISTER_RETRY_BASE;
        let started = tokio::time::Instant::now();
        lister.run().await.unwrap();
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(200 + 400 + 800 + 1600)
        );
        let published = script.published();
        assert_eq!(published.len(), 5);
        assert!(published.iter().all(|u| *u == published[0]));
        assert!(is_marker(&job.end().await.unwrap()));
    }

    /// Pins that a retryable rejection logs its retry at DEBUG and at no
    /// higher level.
    #[test]
    fn a_retry_logs_at_debug() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a"]));
        let (feed, script) = spate_test::scripted_feed();
        script.refuse_next_publish(FeedErrorKind::Retryable);
        let lister = job.lister(Arc::new(feed), CHECKPOINT_EVERY);
        let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
            rt.block_on(lister.run()).unwrap();
        });
        let retries: Vec<&String> = lines
            .iter()
            .filter(|l| l.contains("publishing it again"))
            .collect();
        assert_eq!(retries.len(), 1, "{lines:#?}");
        assert!(retries[0].contains("DEBUG"), "{lines:#?}");
    }

    /// Pins that a unit the feed rejects as retryable on every attempt fails
    /// the run as retryable after the last attempt, naming its key, and
    /// leaves no end marker.
    #[test]
    fn a_unit_rejected_past_the_retry_budget_fails_the_run() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a"]));
        let (feed, script) = spate_test::scripted_feed();
        for _ in 0..LISTER_PUBLISH_ATTEMPTS {
            script.refuse_next_publish(FeedErrorKind::Retryable);
        }
        let err = rt
            .block_on(job.lister(Arc::new(feed), CHECKPOINT_EVERY).run())
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Retryable, "{err}");
        assert!(err.to_string().contains("\"data/a\""), "{err}");
        assert_eq!(script.published().len(), LISTER_PUBLISH_ATTEMPTS as usize);
        assert_eq!(rt.block_on(job.end()), None);
    }

    /// Pins that an empty unaddressable object does not hide one whose key
    /// decodes alike and that lists with bytes on a later page.
    #[test]
    fn an_empty_unaddressable_key_does_not_hide_a_later_one_with_bytes() {
        let rt = runtime();
        let url = crate::test_servers::listing_server(vec![
            list_page(&[("data/a", 7), ("data/e\rf", 0)], Some("1")),
            list_page(&[("data/e\r\nf", 4), ("data/z", 7)], None),
        ]);
        let keys = UnaddressableKeys::default();
        let store = crate::test_servers::builder_at(&url)
            .with_access_key_id("AKIDTEST")
            .with_secret_access_key("secret")
            .with_http_connector(UnaddressableConnector::new(keys.clone()))
            .build()
            .unwrap();
        let job = Job::new();
        let feed = KeyedFeed::confirming(&job.events);
        let mut lister = job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY);
        lister.store = Arc::new(store);
        lister.unaddressable = Some(keys);
        rt.block_on(lister.run()).unwrap();
        assert_eq!(feed.keys(), ["data/a", "data/e\nf", "data/z"]);
    }

    /// A source over the S3 stub at `endpoint`, its location in `ops`.
    fn stub_source(rt: &tokio::runtime::Runtime, endpoint: &str) -> crate::source::S3Source {
        let yaml = format!(
            "url: \"s3://b/data/\"\nsplit_target_bytes: 1MiB\nstore:\n  endpoint: \"{endpoint}\"\n  \
             allow_http: \"true\"\n  access_key_id: \"AKIDTEST\"\n  secret_access_key: \"secret\"\n  \
             region: \"us-east-1\"\n"
        );
        crate::source::S3Source::from_component_config(
            &spate_test::component_config("s3", &yaml),
            rt.handle().clone(),
        )
        .unwrap()
        .with_framer(|| Box::new(crate::testutil::TestLineFramer::new(1 << 20)))
    }

    fn scripted_section() -> spate_core::config::FeedSection {
        let mut section =
            spate_core::config::FeedSection::new(spate_test::component_config("scripted", "{}"));
        section.location = Some(LOCATION.to_owned());
        section
    }

    /// Pins that `list_once` over the S3 client publishes the objects its
    /// connector takes out of a listing page.
    #[test]
    fn list_once_publishes_the_objects_the_s3_client_cannot_address() {
        let rt = runtime();
        let endpoint = crate::test_servers::listing_server(vec![list_page(
            &[("data/a", 7), ("data/d//dbl.ndjson", 7)],
            None,
        )]);
        let job = Job::new();
        let (feed, script) = spate_test::scripted_feed();
        stub_source(&rt, &endpoint)
            .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
            .list_once(&scripted_section(), Arc::new(feed))
            .unwrap();
        let keys: Vec<String> = script
            .published()
            .iter()
            .map(|u| {
                SplitDescriptor::decode(&u.body).unwrap().objects[0]
                    .key
                    .clone()
            })
            .collect();
        assert_eq!(keys, ["data/d//dbl.ndjson", "data/a"]);
    }

    /// Pins that the end marker's log line counts each object the S3 client
    /// cannot address, here one on the listing's last page, after the last
    /// key the listing yields.
    #[test]
    fn the_marker_log_counts_the_objects_the_s3_client_cannot_address() {
        let rt = runtime();
        let endpoint = crate::test_servers::listing_server(vec![
            list_page(&[("data/a", 7)], Some("1")),
            list_page(&[("data/d//dbl.ndjson", 7)], None),
        ]);
        let job = Job::new();
        let (feed, script) = spate_test::scripted_feed();
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            stub_source(&rt, &endpoint)
                .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
                .list_once(&scripted_section(), Arc::new(feed))
                .unwrap();
        });
        assert_eq!(script.published().len(), 2);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("wrote the end marker") && l.contains("units=2")),
            "{lines:#?}"
        );
    }

    /// Pins that the end marker's log line counts an object the S3 client
    /// cannot address on a page that also yields a key, which the lister
    /// publishes before that key.
    #[test]
    fn the_marker_log_counts_an_unaddressable_object_beside_a_key() {
        let rt = runtime();
        let endpoint = crate::test_servers::listing_server(vec![list_page(
            &[("data/a", 7), ("data/d//dbl.ndjson", 7)],
            None,
        )]);
        let job = Job::new();
        let (feed, script) = spate_test::scripted_feed();
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            stub_source(&rt, &endpoint)
                .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
                .list_once(&scripted_section(), Arc::new(feed))
                .unwrap();
        });
        assert_eq!(script.published().len(), 2);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("wrote the end marker") && l.contains("units=2")),
            "{lines:#?}"
        );
    }

    /// Pins that a listing the store refuses fails `list_once` as fatal,
    /// the class of its store error, and leaves no end marker.
    #[test]
    fn a_refused_listing_fails_the_lister_run_as_fatal() {
        let rt = runtime();
        let endpoint = crate::test_servers::status_server("403 Forbidden");
        let job = Job::new();
        let (feed, script) = spate_test::scripted_feed();
        let err = stub_source(&rt, &endpoint)
            .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
            .list_once(&scripted_section(), Arc::new(feed))
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert!(err.to_string().contains("listing"), "{err}");
        assert!(script.published().is_empty());
        assert_eq!(rt.block_on(job.end()), None);
    }

    /// Pins that a listing refused with a 64 KiB error body fails the run
    /// with the store's text cut to a bound.
    #[test]
    fn a_refused_listing_with_a_large_body_is_cut() {
        let rt = runtime();
        let body = format!(
            "<Error><Code>AccessDenied</Code><Message>{}</Message></Error>",
            "x".repeat(64 * 1024)
        );
        let endpoint = crate::test_servers::sequence_server(vec![("403 Forbidden", body)]);
        let job = Job::new();
        let (feed, _script) = spate_test::scripted_feed();
        let err = stub_source(&rt, &endpoint)
            .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
            .list_once(&scripted_section(), Arc::new(feed))
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        let shown = err.to_string();
        assert!(shown.contains("listing"), "{shown}");
        assert!(shown.contains("403 Forbidden"), "{shown}");
        assert!(shown.contains('…'), "{shown}");
        assert!(shown.len() < 2048, "{} bytes", shown.len());
    }

    /// Pins the cadence `list_once` gives the lister: over 1,001 keys the
    /// checkpoint is written before the 1,001st and at the end.
    #[test]
    fn list_once_writes_the_checkpoint_once_per_thousand_keys() {
        let rt = runtime();
        let data = Arc::new(object_store::memory::InMemory::new());
        let keys: Vec<String> = (0..1001).map(|i| format!("data/k{i:04}")).collect();
        rt.block_on(async {
            for key in &keys {
                data.put(&Path::from(key.as_str()), PutPayload::from_static(b"row\n"))
                    .await
                    .unwrap();
            }
        });
        let job = Job::new();
        let (feed, _) = spate_test::scripted_feed();
        stub_source(&rt, "http://127.0.0.1:1")
            .with_store(data)
            .with_location_store(Arc::clone(&job.ops) as Arc<dyn ObjectStore>)
            .list_once(&scripted_section(), Arc::new(feed))
            .unwrap();
        assert_eq!(
            job.checkpoint_writes(),
            [keys[999].clone(), keys[1000].clone()]
        );
    }

    /// Pins that the S3 client's own reply to a `DeleteObjects` entry refused
    /// with `AccessDenied` classifies as fatal, so a change to the text
    /// `object_store` gives that entry fails here.
    #[test]
    fn the_s3_clients_refused_delete_entry_is_fatal() {
        let endpoint = crate::test_servers::sequence_server(vec![(
            "200 OK",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Error><Key>jobs/a/checkpoint</Key><Code>AccessDenied</Code>\
             <Message>Access Denied</Message></Error></DeleteResult>"
                .to_owned(),
        )]);
        let store = crate::test_servers::store_at(&endpoint);
        let e = runtime()
            .block_on(store.delete(&Path::from(CHECKPOINT)))
            .unwrap_err();
        assert_eq!(crate::error::classify(&e), ErrorClass::Fatal, "{e}");
    }

    /// Pins that a lister that cannot resume warns that it keeps no
    /// checkpoint on a run that creates the fingerprint too.
    #[test]
    fn a_lister_that_cannot_resume_warns_at_a_new_location() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a"]));
        let mut lister = job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY);
        lister.resume = false;
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(lister.run()).unwrap();
        });
        assert!(
            lines
                .iter()
                .any(|l| l.contains("WARN") && l.contains("keeps no checkpoint")),
            "{lines:#?}"
        );
        assert!(job.checkpoint_writes().is_empty());
    }

    /// Pins that a refused read of a checkpoint beside no fingerprint fails
    /// the run before it creates the fingerprint or publishes.
    #[test]
    fn a_refused_checkpoint_head_beside_no_fingerprint_fails_the_run() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            get: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| {
                    Err(object_store::Error::PermissionDenied {
                        path: path.to_string(),
                        source: "403 Forbidden".into(),
                    })
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(job.put_rows(&["data/a"]));
        let feed = KeyedFeed::confirming(&job.events);
        let err = rt
            .block_on(
                job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                    .run(),
            )
            .unwrap_err();
        assert_eq!(class(&err), ErrorClass::Fatal, "{err}");
        assert!(feed.keys().is_empty());
        assert!(
            rt.block_on(job.ops.inner.head(&Path::from("jobs/a/fingerprint")))
                .is_err()
        );
    }

    /// Pins that a checkpoint gone between its read and its delete, deleted
    /// by another run, leaves the run listing the whole prefix.
    #[test]
    fn a_checkpoint_deleted_before_its_delete_lands_is_accepted() {
        let rt = runtime();
        let job = Job::with_ops(HookStore {
            delete: Some(|path| {
                (path.filename() == Some("checkpoint")).then(|| object_store::Error::NotFound {
                    path: path.to_string(),
                    source: "deleted by another run".into(),
                })
            }),
            ..HookStore::default()
        });
        rt.block_on(async {
            job.put_rows(&["data/a", "data/b"]).await;
            job.seed_checkpoint(b"data/a").await;
        });
        let feed = KeyedFeed::confirming(&job.events);
        rt.block_on(
            job.lister(Arc::clone(&feed) as Arc<dyn Feed>, CHECKPOINT_EVERY)
                .run(),
        )
        .unwrap();
        assert_eq!(feed.keys(), ["data/a", "data/b"]);
    }

    /// Pins that a solo run on the in-process feed over the S3 client
    /// publishes the objects its connector takes out of a listing page.
    #[test]
    fn a_solo_run_publishes_the_objects_the_s3_client_cannot_address() {
        use spate_core::source::{Source as _, SourceCtx};
        let rt = runtime();
        let endpoint = crate::test_servers::listing_server(vec![list_page(
            &[("data/a", 7), ("data/d//dbl.ndjson", 7)],
            None,
        )]);
        let feed = MemoryFeed::new();
        let mut source = stub_source(&rt, &endpoint);
        source
            .configure_feed(
                &spate_core::config::FeedSection::new(spate_test::component_config("memory", "{}")),
                Arc::new(feed.clone()),
                spate_core::feed::FeedOrigin::Section,
            )
            .unwrap();
        source
            .open(SourceCtx::new(
                spate_core::checkpoint::Checkpointer::new().handle(),
            ))
            .unwrap();
        spate_test::wait_until(DEADLINE, "both objects are published", || {
            feed.counts().visible == 2
        });
    }

    /// Pins that a lister run with no location, a solo run's, logs at INFO
    /// how many units it published once the feed confirmed them.
    #[test]
    fn a_solo_listing_logs_its_unit_count_when_it_ends() {
        let rt = runtime();
        let job = Job::new();
        rt.block_on(job.put_rows(&["data/a", "data/b", "data/c"]));
        let mut lister = job.lister(KeyedFeed::confirming(&job.events), CHECKPOINT_EVERY);
        lister.location = None;
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            rt.block_on(lister.run()).unwrap();
        });
        assert!(
            lines.iter().any(|l| l.contains("INFO")
                && l.contains("listed the prefix")
                && l.contains("units=3")),
            "{lines:#?}"
        );
    }
}
