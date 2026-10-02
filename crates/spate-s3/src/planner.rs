//! The leader-only split planner: one listing per plan, fleet-wide.
//!
//! [`S3Planner`] runs on whichever instance holds job leadership (the
//! coordination backend calls it off the async loop, so blocking on the
//! listing is safe). Every plan run re-lists the prefix in full, packs it
//! with [`pack`](crate::split::pack) and returns every split; deterministic
//! split ids make re-submitting already-planned work idempotent.
//! Workers never list; they read member objects straight from the split
//! descriptors the planner wrote.

use crate::config::Compression;
use crate::error::classify;
use crate::fetch::{MAX_ATTEMPTS, list_all};
use crate::split::Packing;
use object_store::ObjectStore;
use object_store::path::Path;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, PlanContext, PlanFinality, PlannedSplit, SplitId,
    SplitPlan, SplitPlanner, SplitSpec,
};
use spate_core::error::ErrorClass;
use std::sync::Arc;
use tokio::runtime::Handle;

// Reserves padded-base64 expansion and the maximum schema-3 spec envelope.
const MAX_DESCRIPTOR_BYTES: usize = 294_720;

fn validate_descriptor_size(id: &SplitId, descriptor: &[u8]) -> Result<(), CoordinationError> {
    if descriptor.len() > MAX_DESCRIPTOR_BYTES {
        return Err(CoordinationError::new(
            CoordinationErrorKind::Fatal,
            format!(
                "split {id} descriptor is {} raw bytes, above the portable \
                 {MAX_DESCRIPTOR_BYTES}-byte raw limit",
                descriptor.len()
            ),
        ));
    }
    Ok(())
}

/// Job identity presented by every worker. Derived from configuration
/// and the framer's resync delimiter only, never from the listing, so all
/// correctly-configured workers are byte-equal and a misconfigured one is
/// rejected at startup instead of interpreting the shared split table
/// differently. The descriptor and packing versions are included because
/// either changes what planned records *mean*.
pub(crate) fn job_fingerprint(
    url: &str,
    compression: Compression,
    split_target_bytes: u64,
    refresh_listing: bool,
    delimiter: Option<u8>,
) -> String {
    use crate::split::{DESCRIPTOR_VERSION, PACKING_VERSION};
    let delimiter = delimiter.map_or_else(|| "none".to_owned(), |d| format!("{d:02x}"));
    format!(
        "spate-s3:fp1:d{DESCRIPTOR_VERSION}:p{PACKING_VERSION}:url={url}:\
         compression={compression:?}:target={split_target_bytes}:refresh={refresh_listing}:\
         delim={delimiter}"
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
    packing: Packing,
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
            .field("packing", &self.packing)
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
        packing: Packing,
        finality: PlanFinality,
        fingerprint: String,
        metrics: Option<crate::metrics::S3Metrics>,
    ) -> S3Planner {
        S3Planner {
            store,
            prefix,
            handle,
            packing,
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
                let reason = crate::error::reason(&e);
                return Err(CoordinationError::new(
                    kind,
                    if kind == CoordinationErrorKind::Fatal && attempts >= MAX_ATTEMPTS {
                        format!(
                            "listing the backfill prefix still failing after {attempts} \
                             plan attempts: {reason}"
                        )
                    } else {
                        format!("listing the backfill prefix: {reason}")
                    },
                ));
            }
        };
        if let Some(m) = &self.metrics {
            m.objects_listed.increment(entries.len() as u64);
        }

        let packed = crate::split::pack(entries, &self.packing);
        let mut splits = Vec::with_capacity(packed.len());
        for split in packed {
            let id = split.id()?;
            let descriptor = split.descriptor().encode()?;
            validate_descriptor_size(&id, &descriptor)?;
            splits.push(PlannedSplit::new(
                SplitSpec::new(id, descriptor).with_weight(split.weight()),
            ));
        }
        Ok(SplitPlan::new(splits, self.finality))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::{DESCRIPTOR_VERSION, SplitDescriptor, split_id_for_range};
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

    fn packing(target: u64) -> Packing {
        Packing {
            target_bytes: target,
            compression: Compression::Auto,
            delimiter: Some(b'\n'),
        }
    }

    fn planner(store: Arc<dyn ObjectStore>, handle: Handle, target: u64) -> S3Planner {
        S3Planner::new(
            store,
            Some(Path::from("data")),
            handle,
            packing(target),
            PlanFinality::Final,
            job_fingerprint(
                "s3://bucket/data/",
                Compression::Auto,
                target,
                false,
                Some(b'\n'),
            ),
            None,
        )
    }

    /// The raw descriptor ceiling accepts its boundary and rejects the next byte.
    /// Regression for #849.
    #[test]
    fn descriptor_size_boundary_is_inclusive() {
        let id = SplitId::new("boundary").unwrap();
        assert!(validate_descriptor_size(&id, &vec![0; 294_719]).is_ok());
        assert!(validate_descriptor_size(&id, &vec![0; 294_720]).is_ok());
        let err = validate_descriptor_size(&id, &vec![0; 294_721]).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal);
        assert!(err.to_string().contains("294721"), "{err}");
    }

    /// Listed metadata above the portable descriptor budget is rejected.
    /// Regression for #849.
    #[test]
    fn planner_rejects_a_descriptor_above_the_portable_budget() {
        let rt = runtime();
        let key = format!("data/{}", "a".repeat(300 * 1024));
        let store = seeded_store(&[(&key, 1)]);
        let entries = rt
            .block_on(list_all(&store, Some(&Path::from("data"))))
            .unwrap();
        let packed = crate::split::pack(entries, &packing(64 * MB));
        assert_eq!(packed.len(), 1);
        let descriptor = packed[0].descriptor().encode().unwrap();
        assert!(descriptor.len() > 294_720);
        assert!(descriptor.len() < 409_600);
        let id = packed[0].id().unwrap();
        let mut p = planner(store, rt.handle().clone(), 64 * MB);
        let err = match p.plan(PlanContext::new(None, 1)) {
            Ok(_) => panic!("planner accepted a descriptor above the portable budget"),
            Err(err) => err,
        };
        assert_eq!(err.kind, CoordinationErrorKind::Fatal);
        let message = err.to_string();
        assert!(message.contains(id.as_str()), "{message}");
        assert!(message.contains(&descriptor.len().to_string()), "{message}");
        assert!(message.contains("294720"), "{message}");
        assert!(message.contains("portable"), "{message}");
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

    /// A plain object above the target plans into byte-range splits whose
    /// ids, descriptors and weights are the range's.
    #[test]
    fn a_large_plain_object_plans_into_ranged_splits() {
        let rt = runtime();
        let store = seeded_store(&[("data/big.ndjson", 5 * MB as usize / 2)]);
        let mut p = planner(store, rt.handle().clone(), MB);

        let plan = p.plan(PlanContext::new(None, 1)).unwrap();
        let mut next = 0;
        for split in &plan.splits {
            let spec = &split.spec;
            let desc = SplitDescriptor::decode(&spec.descriptor).unwrap();
            let range = desc.range.expect("a ranged split");
            let object = &desc.objects[0];
            assert_eq!(range.start, next, "ranges tile the object in order");
            assert_eq!(range.delimiter, b'\n');
            assert_eq!(spec.weight, range.end - range.start);
            assert_eq!(
                spec.id,
                split_id_for_range(&object.key, object.etag.as_deref().unwrap(), range).unwrap()
            );
            next = range.end;
        }
        assert_eq!(plan.splits.len(), 3);
        assert_eq!(next, 5 * MB / 2);
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
            packing(64 * MB),
            PlanFinality::Final,
            job_fingerprint(
                "s3://other/",
                Compression::Auto,
                64 * MB,
                false,
                Some(b'\n'),
            ),
            None,
        );
        assert_ne!(
            SplitPlanner::fingerprint(&a),
            SplitPlanner::fingerprint(&other_url)
        );

        let fingerprint = |compression, target, delimiter| {
            job_fingerprint("s3://bucket/data/", compression, target, false, delimiter)
        };
        for other in [
            fingerprint(Compression::Auto, 32 * MB, Some(b'\n')),
            fingerprint(Compression::Gzip, 64 * MB, Some(b'\n')),
            fingerprint(Compression::Auto, 64 * MB, Some(b';')),
            fingerprint(Compression::Auto, 64 * MB, None),
        ] {
            assert_ne!(SplitPlanner::fingerprint(&a), other);
        }
        assert!(
            SplitPlanner::fingerprint(&a).ends_with(":delim=0a"),
            "{}",
            SplitPlanner::fingerprint(&a)
        );
        assert!(fingerprint(Compression::Auto, 64 * MB, None).ends_with(":delim=none"));
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

    /// A listing the store answers 403 or 401 fails the plan as `Fatal` on the
    /// first attempt, naming the status.
    #[test]
    fn a_rejected_listing_fails_the_first_plan() {
        let rt = runtime();
        for status in ["403 Forbidden", "401 Unauthorized"] {
            let store = crate::test_servers::store_at(&crate::test_servers::status_server(status));
            let mut p = planner(Arc::new(store), rt.handle().clone(), 64 * MB);
            let err = p.plan(PlanContext::new(None, 1)).unwrap_err();
            assert_eq!(err.kind, CoordinationErrorKind::Fatal, "{}", err.reason);
            assert!(err.reason.contains(status), "{}", err.reason);
            assert!(!err.reason.contains("plan attempts"), "{}", err.reason);
        }
    }

    /// A credential endpoint that answers 403 leaves the plan `Retryable`.
    #[test]
    fn a_rejected_credential_fetch_is_retryable() {
        let rt = runtime();
        let url = crate::test_servers::status_server("403 Forbidden");
        let store = crate::test_servers::builder_at(&url)
            .with_metadata_endpoint(&url)
            .build()
            .unwrap();
        let mut p = planner(Arc::new(store), rt.handle().clone(), 64 * MB);
        let err = p.plan(PlanContext::new(None, 1)).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Retryable, "{}", err.reason);
        assert!(err.reason.contains("latest/api/token"), "{}", err.reason);
    }

    /// A listing refused with a `handshake_failure` alert fails the plan as
    /// `Fatal` on the first attempt, naming the alert.
    #[test]
    fn a_rejecting_tls_alert_fails_the_first_plan() {
        let rt = runtime();
        let addr =
            spate_test::tls_alert_server(b"", u8::from(rustls::AlertDescription::HandshakeFailure));
        let store = crate::test_servers::tls_store_at(
            &format!("https://{addr}"),
            &crate::test_servers::TestCa::new("any"),
        );
        let mut p = planner(Arc::new(store), rt.handle().clone(), 64 * MB);
        let err = p.plan(PlanContext::new(None, 1)).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal, "{}", err.reason);
        assert!(err.reason.contains("HandshakeFailure"), "{}", err.reason);
    }

    /// A credential endpoint that rejects the TLS handshake fails the plan as
    /// `Fatal` on the first attempt, naming the endpoint and the alert.
    #[test]
    fn a_tls_rejection_by_the_credential_endpoint_fails_the_first_plan() {
        let rt = runtime();
        let addr =
            spate_test::tls_alert_server(b"", u8::from(rustls::AlertDescription::HandshakeFailure));
        let url = format!("https://{addr}");
        let store = crate::test_servers::builder_at(&url)
            .with_client_options(crate::test_servers::tls_trusting(
                &crate::test_servers::TestCa::new("any"),
            ))
            .with_metadata_endpoint(&url)
            .build()
            .unwrap();
        let mut p = planner(Arc::new(store), rt.handle().clone(), 64 * MB);
        let err = p.plan(PlanContext::new(None, 1)).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal, "{}", err.reason);
        assert!(err.reason.contains("latest/api/token"), "{}", err.reason);
        assert!(err.reason.contains("HandshakeFailure"), "{}", err.reason);
    }
}
