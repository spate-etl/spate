//! Pipeline-level handles (`etl_pipeline_*`).

use super::labels::ComponentLabels;
use super::names;
use metrics::Gauge;

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
    starting: Gauge,
    running: Gauge,
    draining: Gauge,
    failed: Gauge,
    threads: Gauge,
}

impl PipelineMetrics {
    /// Resolve pipeline handles and publish the info series.
    pub fn new(labels: &ComponentLabels, version: &str) -> Self {
        labels
            .gauge1(names::PIPELINE_INFO, names::L_VERSION, version.to_owned())
            .set(1.0);
        let state = |s: &'static str| labels.gauge1(names::PIPELINE_STATE, names::L_STATE, s);
        let m = PipelineMetrics {
            starting: state("starting"),
            running: state("running"),
            draining: state("draining"),
            failed: state("failed"),
            threads: labels.gauge(names::PIPELINE_THREADS),
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
