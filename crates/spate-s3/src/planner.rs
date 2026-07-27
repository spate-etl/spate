//! The leader-only split planner: one listing per plan, fleet-wide.
//!
//! [`S3Planner`] runs on whichever instance holds job leadership (the
//! coordination backend calls it off the async loop, so blocking on the
//! listing is safe). Every plan run re-lists the prefix in full and packs
//! it with [`pack`](crate::split::pack); deterministic split ids make the
//! re-submission of already-planned work a store-side create-if-absent
//! no-op, so replanning a growing prefix costs one LIST and nothing else.
//! Workers never list — they read member objects straight from the split
//! descriptors the planner wrote.

use crate::config::Compression;
use crate::error::classify;
use crate::fetch::{MAX_ATTEMPTS, list_all};
use crate::split::{SplitDescriptor, split_id_for};
use object_store::ObjectStore;
use object_store::path::Path;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, PlanContext, PlanFinality, PlannedSplit, SplitPlan,
    SplitPlanner, SplitSpec,
};
use spate_core::error::ErrorClass;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Hard ceiling on one split's encoded descriptor. The open-cost floor
/// keeps real descriptors three orders of magnitude below this; the guard
/// exists so a future packing change can never silently exceed a backend's
/// value-size cap (the NATS backend stores at most 512 KiB per value).
const MAX_DESCRIPTOR_BYTES: usize = 400 * 1024;

/// Job identity presented by every worker. Derived from configuration
/// only — never from the listing — so all correctly-configured workers are
/// byte-equal and a misconfigured one is rejected at startup instead of
/// interpreting the shared split table differently. The descriptor and
/// packing versions are included because either changes what planned
/// records *mean*.
pub(crate) fn job_fingerprint(
    url: &str,
    compression: Compression,
    split_target_bytes: u64,
    refresh_listing: bool,
) -> String {
    use crate::split::{DESCRIPTOR_VERSION, PACKING_VERSION};
    format!(
        "spate-s3:fp1:d{DESCRIPTOR_VERSION}:p{PACKING_VERSION}:url={url}:\
         compression={compression:?}:target={split_target_bytes}:refresh={refresh_listing}"
    )
}

/// [`SplitPlanner`] over an object-store prefix: LIST, pack, mint ids.
pub(crate) struct S3Planner {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    /// The pipeline's I/O runtime. [`PlanContext`] carries no I/O handle;
    /// the planner captures its own at construction and blocks on it
    /// (safe: backends run `plan` on the blocking pool).
    handle: Handle,
    target_bytes: u64,
    finality: PlanFinality,
    fingerprint: String,
    /// `objects_listed_total` is leader-only by construction: the planner
    /// runs nowhere else.
    metrics: Option<crate::metrics::S3Metrics>,
    /// Consecutive retryable listing failures. A persistent outage must
    /// fail fast (the crate's retry philosophy) instead of idling the
    /// fleet behind per-tick WARNs forever.
    consecutive_failures: u32,
}

impl std::fmt::Debug for S3Planner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Planner")
            .field("prefix", &self.prefix)
            .field("target_bytes", &self.target_bytes)
            .field("finality", &self.finality)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl S3Planner {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: Option<Path>,
        handle: Handle,
        target_bytes: u64,
        finality: PlanFinality,
        fingerprint: String,
        metrics: Option<crate::metrics::S3Metrics>,
    ) -> S3Planner {
        S3Planner {
            store,
            prefix,
            handle,
            target_bytes,
            finality,
            fingerprint,
            metrics,
            consecutive_failures: 0,
        }
    }
}

impl SplitPlanner for S3Planner {
    fn fingerprint(&self) -> String {
        self.fingerprint.clone()
    }

    fn plan(&mut self, _ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        let entries = match self
            .handle
            .block_on(list_all(&self.store, self.prefix.as_ref()))
        {
            Ok(entries) => {
                self.consecutive_failures = 0;
                entries
            }
            Err(e) => {
                let kind = if classify(&e) == ErrorClass::Retryable {
                    self.consecutive_failures += 1;
                    if self.consecutive_failures >= MAX_ATTEMPTS {
                        // The leader retries a Retryable plan on every
                        // replan tick; without a budget a persistent
                        // outage idles the whole fleet invisibly forever.
                        CoordinationErrorKind::Fatal
                    } else {
                        CoordinationErrorKind::Retryable
                    }
                } else {
                    CoordinationErrorKind::Fatal
                };
                let attempts = self.consecutive_failures;
                return Err(CoordinationError::new(
                    kind,
                    if kind == CoordinationErrorKind::Fatal && attempts >= MAX_ATTEMPTS {
                        format!(
                            "listing the backfill prefix still failing after {attempts} \
                             plan attempts: {e}"
                        )
                    } else {
                        format!("listing the backfill prefix: {e}")
                    },
                ));
            }
        };
        if let Some(m) = &self.metrics {
            m.objects_listed.increment(entries.len() as u64);
        }

        let bins = crate::split::pack(entries, self.target_bytes);
        let mut splits = Vec::with_capacity(bins.len());
        for members in bins {
            let id = split_id_for(members.iter().map(|m| (m.key.as_str(), m.etag.as_deref())))?;
            let descriptor = SplitDescriptor::from_entries(&members).encode()?;
            if descriptor.len() > MAX_DESCRIPTOR_BYTES {
                return Err(CoordinationError::new(
                    CoordinationErrorKind::Fatal,
                    format!(
                        "split {id} descriptor is {} bytes, above the {MAX_DESCRIPTOR_BYTES}-byte \
                         ceiling; this is a planner bug (the open-cost floor should bound members \
                         per split), please report it",
                        descriptor.len()
                    ),
                ));
            }
            // Saturating: sizes are remote listing data; a hostile listing
            // must not overflow (debug panic) the leader's planner.
            let weight = members
                .iter()
                .fold(0u64, |acc, m| acc.saturating_add(m.size))
                .max(1);
            splits.push(PlannedSplit::new(
                SplitSpec::new(id, descriptor).with_weight(weight),
            ));
        }
        Ok(SplitPlan::new(splits, self.finality))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::DESCRIPTOR_VERSION;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt as _, PutPayload, path::Path};

    const MB: u64 = 1024 * 1024;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn seeded_store(keys_and_sizes: &[(&str, usize)]) -> Arc<dyn ObjectStore> {
        let store = Arc::new(InMemory::new());
        let rt = runtime();
        for (key, size) in keys_and_sizes {
            rt.block_on(store.put(&Path::from(*key), PutPayload::from(vec![b'x'; *size])))
                .unwrap();
        }
        store
    }

    fn planner(store: Arc<dyn ObjectStore>, handle: Handle, target: u64) -> S3Planner {
        S3Planner::new(
            store,
            Some(Path::from("data")),
            handle,
            target,
            PlanFinality::Final,
            job_fingerprint("s3://bucket/data/", Compression::Auto, target, false),
            None,
        )
    }

    #[test]
    fn plans_are_deterministic_and_replans_reproduce_identical_ids() {
        let rt = runtime();
        let store = seeded_store(&[
            ("data/a.ndjson", 3 * MB as usize),
            ("data/b.ndjson", 3 * MB as usize),
            ("data/c.ndjson", 9 * MB as usize),
        ]);
        let mut p = planner(store, rt.handle().clone(), 8 * MB);

        let first = p.plan(PlanContext::new(None, 1)).unwrap();
        let replan = p.plan(PlanContext::new(None, 2)).unwrap();
        assert_eq!(
            first, replan,
            "replanning unchanged work is a no-op by identity"
        );
        assert_eq!(first.finality, PlanFinality::Final);
        assert!(first.planner_state.is_none());
        assert!(!first.splits.is_empty());
        assert!(first.splits.iter().all(|s| s.seed.is_none()));
    }

    #[test]
    fn descriptors_carry_the_members_and_weights_carry_the_bytes() {
        let rt = runtime();
        let store = seeded_store(&[
            ("data/a.ndjson", MB as usize),
            ("data/b.ndjson", 2 * MB as usize),
        ]);
        let mut p = planner(store, rt.handle().clone(), 64 * MB);

        let plan = p.plan(PlanContext::new(None, 1)).unwrap();
        assert_eq!(
            plan.splits.len(),
            1,
            "two small objects coalesce into one split"
        );
        let spec = &plan.splits[0].spec;
        assert_eq!(spec.weight, 3 * MB);

        let desc = SplitDescriptor::decode(&spec.descriptor).unwrap();
        assert_eq!(desc.v, DESCRIPTOR_VERSION);
        let keys: Vec<&str> = desc.objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(
            keys,
            ["data/a.ndjson", "data/b.ndjson"],
            "listing order preserved"
        );
        assert!(
            desc.objects.iter().all(|o| o.etag.is_some()),
            "listing etags pinned"
        );
    }

    #[test]
    fn empty_prefix_yields_an_empty_final_plan() {
        let rt = runtime();
        let store = seeded_store(&[]);
        let mut p = planner(store, rt.handle().clone(), 64 * MB);
        let plan = p.plan(PlanContext::new(None, 1)).unwrap();
        assert!(plan.splits.is_empty());
        assert_eq!(plan.finality, PlanFinality::Final);
    }

    #[test]
    fn fingerprint_is_config_derived_and_listing_independent() {
        let rt = runtime();
        let a = planner(
            seeded_store(&[("data/x", 10)]),
            rt.handle().clone(),
            64 * MB,
        );
        let b = planner(
            seeded_store(&[("data/y", 999)]),
            rt.handle().clone(),
            64 * MB,
        );
        assert_eq!(
            SplitPlanner::fingerprint(&a),
            SplitPlanner::fingerprint(&b),
            "same config, different listings: identical fingerprint"
        );

        let other_url = S3Planner::new(
            seeded_store(&[]),
            None,
            rt.handle().clone(),
            64 * MB,
            PlanFinality::Final,
            job_fingerprint("s3://other/", Compression::Auto, 64 * MB, false),
            None,
        );
        assert_ne!(
            SplitPlanner::fingerprint(&a),
            SplitPlanner::fingerprint(&other_url)
        );

        let other_target = job_fingerprint("s3://bucket/data/", Compression::Auto, 32 * MB, false);
        assert_ne!(SplitPlanner::fingerprint(&a), other_target);
        let other_compression =
            job_fingerprint("s3://bucket/data/", Compression::Gzip, 64 * MB, false);
        assert_ne!(SplitPlanner::fingerprint(&a), other_compression);
    }

    #[test]
    fn listing_failure_maps_through_the_error_taxonomy() {
        // An InMemory store never fails a list, so exercise the mapping at
        // the classify seam instead: a NotFound during LIST is fatal (the
        // prefix itself is wrong), transport errors are retryable.
        let not_found = object_store::Error::NotFound {
            path: "data".into(),
            source: "gone".into(),
        };
        assert_eq!(classify(&not_found), ErrorClass::Fatal);
        let generic = object_store::Error::Generic {
            store: "s3",
            source: "timeout".into(),
        };
        assert_eq!(classify(&generic), ErrorClass::Retryable);
    }
}
