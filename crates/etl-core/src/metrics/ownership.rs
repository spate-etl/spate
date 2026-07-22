//! Series ownership: one live owner per gauge series, process-wide.
//!
//! A handle struct addresses its series by label tuple, but the state that
//! decides what to publish — a cached health bool, a backoff map, a partition
//! set — is held per instance. Two instances resolving the same series
//! therefore each believe they own it, and because the interesting gauge
//! writers are edge-triggered, the loser's stale reading is never republished.
//! Counters degrade into a sum under that collision; gauges degrade into a
//! lie.
//!
//! So gauge series are claimed. The first handle set to resolve a given
//! `(root, pipeline, component, component_type, …)` key owns it and publishes
//! normally; a later one becomes a **shadow** — its counters and histograms
//! still record (they aggregate correctly), its gauge writes are dropped (see
//! [`OwnedGauge`](super::labels::OwnedGauge)). Ownership is checked once, at
//! construction, so nothing is added to any write path.
//!
//! The registry is process-global and cannot see recorder scope: two handle
//! sets built under separate [`metrics::with_local_recorder`] recorders still
//! collide here. That is deliberate — the framework installs one global
//! recorder — but it means test helpers must carry per-test labels rather than
//! relying on recorder isolation for independence.

use super::MetricsError;
use super::labels::ComponentLabels;
use std::collections::HashSet;
use std::sync::{LazyLock, Mutex, PoisonError};

/// Series keys with a live owner. Touched at build time only.
static CLAIMS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Ownership of one handle set's gauge series, held for as long as the owning
/// struct lives.
///
/// Dropping the claim frees the key, so a pipeline rebuilt **sequentially** in
/// one process re-claims cleanly — but only once the previous owner is really
/// gone. `SinkShardMetrics` in particular sits behind `Arc`s held by the
/// shard's `BreakerSet` and its in-flight write tasks, so the claim frees when
/// the *last* clone drops: after the drain, not when `SinkPool` is handed off.
/// Overlapping lifetimes collide, and a shadow is never promoted (that would
/// cost a check on every gauge write) — so an overlapping rebuild leaves the
/// new instance gauge-silent for its whole life. Drop, then build.
#[derive(Debug)]
pub(crate) struct SeriesClaim {
    key: String,
}

impl SeriesClaim {
    /// Claim `key`, or fail because another live handle set owns it. The
    /// fallible path, for the pipeline builder and runtime: a duplicate there
    /// is a wiring mistake the caller can still refuse to start on.
    pub(crate) fn try_claim(key: String) -> Result<Self, MetricsError> {
        if Self::insert(&key) {
            Ok(SeriesClaim { key })
        } else {
            Err(MetricsError::DuplicateSeries(key))
        }
    }

    /// Claim `key`, or log and return `None` so the caller becomes a shadow.
    /// The infallible path, for direct construction (benchmarks, hand
    /// assembly, tests): a label collision must not take down a process whose
    /// data path is fine, but it must be impossible to miss in the logs.
    pub(crate) fn claim_or_shadow(key: String) -> Option<Self> {
        if Self::insert(&key) {
            return Some(SeriesClaim { key });
        }
        tracing::error!(
            series = %key,
            "another live handle set already owns this metric series; this one \
             will keep counting (counters sum) but publish no gauges, so the \
             owner's readings stay truthful. Two pipelines or two components \
             sharing a name in one process is the usual cause."
        );
        None
    }

    fn insert(key: &str) -> bool {
        Self::registry().insert(key.to_owned())
    }

    /// Poison-tolerant: `Drop` runs during unwinding, where a panicking
    /// `expect` would abort the process. The critical section only inserts and
    /// removes, so a poisoned set is not a corrupt one.
    fn registry() -> std::sync::MutexGuard<'static, HashSet<String>> {
        CLAIMS.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for SeriesClaim {
    fn drop(&mut self) {
        Self::registry().remove(&self.key);
    }
}

/// The claim key for one handle set: the stage root, the three standard
/// labels, and whatever extra label pins the instance down (`shard=3`, a queue
/// name) — `""` when the labels alone identify it.
///
/// `root` is a [`RESERVED_ROOTS`](super::names::RESERVED_ROOTS) segment, so
/// two different stages on identical labels never collide with each other.
pub(crate) fn series_key(root: &str, labels: &ComponentLabels, extra: &str) -> String {
    format!(
        "etl_{root}_|{}|{}|{}|{extra}",
        labels.pipeline, labels.component, labels.component_type
    )
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn labels(component: &str) -> ComponentLabels {
        ComponentLabels::new("orders", component.to_owned(), "kafka")
    }

    #[test]
    fn the_first_claim_wins_and_the_second_shadows() {
        let key = series_key("sink", &labels("first-wins"), "shard=0");
        let owner = SeriesClaim::try_claim(key.clone()).expect("free series");
        assert!(
            SeriesClaim::claim_or_shadow(key.clone()).is_none(),
            "a second claim on a live series must shadow"
        );
        assert!(matches!(
            SeriesClaim::try_claim(key.clone()),
            Err(MetricsError::DuplicateSeries(_))
        ));
        drop(owner);
    }

    /// A sequential rebuild — the supported way to replace a pipeline in one
    /// process — must re-claim and publish, not inherit the shadow.
    #[test]
    fn dropping_the_owner_frees_the_key() {
        let key = series_key("sink", &labels("drop-frees"), "shard=0");
        drop(SeriesClaim::try_claim(key.clone()).expect("free series"));
        let second = SeriesClaim::try_claim(key.clone()).expect("freed by the drop");
        drop(second);
    }

    #[test]
    fn keys_separate_stages_shards_and_components() {
        let a = series_key("sink", &labels("keys"), "shard=0");
        for other in [
            series_key("source", &labels("keys"), "shard=0"),
            series_key("sink", &labels("keys"), "shard=1"),
            series_key("sink", &labels("keys-other"), "shard=0"),
        ] {
            assert_ne!(a, other);
        }
    }
}
