//! A one-pass lister for a bounded feed job: it writes the job fingerprint
//! at the job location, deletes an end marker an earlier run left there,
//! then publishes one unit per part of each listed object.
//!
//! It writes no end marker and keeps no checkpoint. Reached only through
//! the `testing`-gated `S3Source::list_once`.

use crate::error::classify;
use crate::fetch::ObjectEntry;
use crate::location::{Location, bounded_reason, compare, io_error, read_error, read_object};
use crate::split::Packing;
use crate::unit::units;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutPayload};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::{Confirmation, Feed};
use std::collections::VecDeque;
use std::sync::Arc;

/// The most publishes the lister leaves unconfirmed at once. At least one
/// SQS send batch of 10.
pub(crate) const LISTER_IN_FLIGHT: usize = 32;

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
    /// The job fingerprint this run writes.
    pub(crate) fingerprint: String,
}

impl Lister {
    /// Write the fingerprint, delete an earlier end marker, then publish
    /// every listed object's units in the order the store lists them.
    ///
    /// # Errors
    ///
    /// Fatal when the location holds another fingerprint and when its store
    /// cannot create an object conditionally. A publish the feed refuses, a
    /// listing error and a location error carry their own class.
    pub(crate) async fn run(self) -> Result<(), SourceError> {
        if let Some((store, location)) = &self.location {
            write_fingerprint(store.as_ref(), location, &self.fingerprint).await?;
            // Before the first publish, so no worker of this run can read a
            // marker an earlier run left.
            delete_end(store.as_ref(), location).await?;
        }
        self.publish_listing().await
    }

    async fn publish_listing(&self) -> Result<(), SourceError> {
        let mut pending: VecDeque<(String, Confirmation)> = VecDeque::new();
        let mut listing = self.store.list(Some(&self.prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| SourceError::Client {
                class: classify(&e),
                reason: format!("listing {}: {}", self.prefix, bounded_reason(&e)),
            })?;
            let entry = ObjectEntry {
                key: meta.location.to_string(),
                size: meta.size,
                etag: meta.e_tag,
                last_modified_ms: meta.last_modified.timestamp_millis(),
            };
            for unit in units(entry, self.packing) {
                let unit = unit.map_err(|e| SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("minting a unit: {e}"),
                })?;
                if pending.len() == LISTER_IN_FLIGHT
                    && let Some(oldest) = pending.pop_front()
                {
                    settle(oldest).await?;
                }
                let id = unit.id.clone();
                pending.push_back((id, self.feed.publish(unit)));
            }
        }
        while let Some(oldest) = pending.pop_front() {
            settle(oldest).await?;
        }
        Ok(())
    }
}

async fn settle((id, confirmation): (String, Confirmation)) -> Result<(), SourceError> {
    confirmation.await.map_err(|e| SourceError::Client {
        class: e.class(),
        reason: format!("publishing unit {id}: {e}"),
    })
}

/// Create the fingerprint object, or compare the one already there.
async fn write_fingerprint(
    store: &dyn ObjectStore,
    location: &Location,
    mine: &str,
) -> Result<(), SourceError> {
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
            Ok(_) => return Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                if let Some(stored) = read_object(store, location, &object).await? {
                    return compare(location, mine, &stored);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::SplitDescriptor;
    use crate::testutil::HookStore;
    use spate_core::feed::{
        Confirmer, Delivery, FeedError, FeedWaker, Receipt, Unit, confirmation,
    };
    use std::sync::Mutex;
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
            fingerprint: String::new(),
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
            .unwrap_err()
            .to_string();
        assert!(err.contains("read as absent twice"), "{err}");
        assert_eq!(store.creates.load(std::sync::atomic::Ordering::SeqCst), 2);
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
}
