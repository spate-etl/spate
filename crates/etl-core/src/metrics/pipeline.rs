//! Pipeline-level handles (`etl_pipeline_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};

/// Lifecycle state of the pipeline, exported via `etl_pipeline_state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineState {
    /// Starting up: connecting, awaiting assignment.
    Starting,
    /// Processing records.
    Running,
    /// Draining after SIGTERM or a full revocation.
    Draining,
    /// Failed; the process will exit non-zero.
    Failed,
}

/// Pipeline-level handles (`etl_pipeline_*`).
#[derive(Debug)]
pub struct PipelineMetrics {
    starting: OwnedGauge,
    running: OwnedGauge,
    draining: OwnedGauge,
    failed: OwnedGauge,
    threads: OwnedGauge,
    _claim: Option<SeriesClaim>,
}

impl PipelineMetrics {
    /// Resolve pipeline handles and publish the info series.
    ///
    /// Claims the `etl_pipeline_*` series for these labels; a second live
    /// handle set logs and becomes a shadow, publishing neither state nor
    /// thread count (see "Series ownership" in `docs/METRICS.md`).
    pub fn new(labels: &ComponentLabels, version: &str) -> Self {
        let claim = SeriesClaim::claim_or_shadow(Self::key(labels));
        Self::build(labels, version, claim)
    }

    /// Resolve pipeline handles, failing when another live handle set already
    /// owns the series. The pipeline runtime's path.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(labels: &ComponentLabels, version: &str) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels))?;
        Ok(Self::build(labels, version, Some(claim)))
    }

    fn key(labels: &ComponentLabels) -> String {
        series_key("pipeline", labels, "")
    }

    fn build(labels: &ComponentLabels, version: &str, claim: Option<SeriesClaim>) -> Self {
        let owned = claim.is_some();
        OwnedGauge::new(
            labels.gauge1(names::PIPELINE_INFO, names::L_VERSION, version.to_owned()),
            owned,
        )
        .set(1.0);
        let state = |s: &'static str| {
            OwnedGauge::new(
                labels.gauge1(names::PIPELINE_STATE, names::L_STATE, s),
                owned,
            )
        };
        let m = PipelineMetrics {
            starting: state("starting"),
            running: state("running"),
            draining: state("draining"),
            failed: state("failed"),
            threads: OwnedGauge::new(labels.gauge(names::PIPELINE_THREADS), owned),
            _claim: claim,
        };
        m.set_state(PipelineState::Starting);
        m
    }

    /// Flip the state gauges so exactly the current state reads 1.
    pub fn set_state(&self, state: PipelineState) {
        self.starting.set(if state == PipelineState::Starting {
            1.0
        } else {
            0.0
        });
        self.running.set(if state == PipelineState::Running {
            1.0
        } else {
            0.0
        });
        self.draining.set(if state == PipelineState::Draining {
            1.0
        } else {
            0.0
        });
        self.failed.set(if state == PipelineState::Failed {
            1.0
        } else {
            0.0
        });
    }

    /// Publish the pinned pipeline thread count.
    pub fn set_threads(&self, threads: usize) {
        self.threads.set(threads as f64);
    }
}
