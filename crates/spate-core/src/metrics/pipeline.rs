//! Pipeline-level handles (`spate_pipeline_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::Gauge;
use std::time::{Duration, Instant};

/// Lifecycle state of the pipeline, exported via `spate_pipeline_state`.
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

/// Pipeline-level handles (`spate_pipeline_*`).
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
    /// Claims the `spate_pipeline_*` series for these labels; a second live
    /// handle set logs and becomes a shadow, publishing neither state nor
    /// thread count.
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

/// The I/O runtime's busy gauge (`spate_pipeline_io_busy_ratio`).
///
/// One series per pipeline, published from the thread that waits on the
/// controller. It reads 0 from construction until the first sample.
#[derive(Debug)]
pub(crate) struct IoRuntimeMetrics {
    busy_ratio: Gauge,
    _claim: SeriesClaim,
}

impl IoRuntimeMetrics {
    /// Resolve the busy gauge, failing when another live handle set already
    /// owns the series.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub(crate) fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        // The `io` segment keeps this key distinct from the `PipelineMetrics`
        // claim on the same labels.
        let claim = SeriesClaim::try_claim(series_key("pipeline", labels, "io"))?;
        let busy_ratio = labels.gauge(names::PIPELINE_IO_BUSY_RATIO);
        busy_ratio.set(0.0);
        Ok(IoRuntimeMetrics {
            busy_ratio,
            _claim: claim,
        })
    }

    /// Publish one sample's ratio.
    pub(crate) fn set_busy_ratio(&self, ratio: f64) {
        self.busy_ratio.set(ratio);
    }
}

/// One reading of [`IoBusySampler`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct IoBusySample {
    /// Mean busy fraction of the workers over the interval, in `0..=1`.
    pub(crate) ratio: f64,
    /// Every sample over the last [`IoBusySampler::SUSTAIN`] read at least
    /// [`IoBusySampler::SATURATED`]. True on the one sample that completes
    /// such a run; it re-arms after a sample below the threshold.
    pub(crate) sustained: bool,
}

/// Turns the I/O runtime's cumulative busy time into a per-interval ratio.
///
/// Pure: the caller passes the instant and the busy total, so tests drive it
/// without a clock.
#[derive(Debug)]
pub(crate) struct IoBusySampler {
    interval: Duration,
    last_at: Instant,
    last_busy: Duration,
    /// Start of the current run of saturated samples.
    saturated_since: Option<Instant>,
    warned: bool,
}

impl IoBusySampler {
    /// The ratio at or above which a sample counts as saturated.
    pub(crate) const SATURATED: f64 = 0.9;
    /// How long every sample must read saturated before the warning.
    pub(crate) const SUSTAIN: Duration = Duration::from_secs(60);

    /// A sampler that yields at most one sample per `interval`, with its
    /// baseline at `now` and `busy_total`.
    pub(crate) fn new(interval: Duration, now: Instant, busy_total: Duration) -> Self {
        IoBusySampler {
            interval,
            last_at: now,
            last_busy: busy_total,
            saturated_since: None,
            warned: false,
        }
    }

    /// Take a sample when at least one interval has passed since the last.
    ///
    /// `busy_total` is the sum over all `workers` of their cumulative busy
    /// time. Returns `None` before a full interval has passed.
    pub(crate) fn observe(
        &mut self,
        now: Instant,
        busy_total: Duration,
        workers: usize,
    ) -> Option<IoBusySample> {
        let elapsed = now.saturating_duration_since(self.last_at);
        if elapsed.is_zero() || elapsed < self.interval {
            return None;
        }
        let busy = busy_total.saturating_sub(self.last_busy);
        let capacity = elapsed.as_secs_f64() * workers.max(1) as f64;
        let ratio = (busy.as_secs_f64() / capacity).min(1.0);

        // A saturated run starts where the first saturated interval began.
        let started = self.last_at;
        self.last_at = now;
        self.last_busy = busy_total;

        let mut sustained = false;
        if ratio >= Self::SATURATED {
            let since = *self.saturated_since.get_or_insert(started);
            if !self.warned && now.saturating_duration_since(since) >= Self::SUSTAIN {
                self.warned = true;
                sustained = true;
            }
        } else {
            self.saturated_since = None;
            self.warned = false;
        }
        Some(IoBusySample { ratio, sustained })
    }

    /// [`observe`](Self::observe) with the busy total and worker count read
    /// from a runtime's metrics.
    pub(crate) fn observe_runtime(
        &mut self,
        now: Instant,
        metrics: &tokio::runtime::RuntimeMetrics,
    ) -> Option<IoBusySample> {
        self.observe(now, busy_total(metrics), metrics.num_workers())
    }
}

/// The sum of every async worker's cumulative busy time. The blocking pool
/// is not included.
pub(crate) fn busy_total(metrics: &tokio::runtime::RuntimeMetrics) -> Duration {
    (0..metrics.num_workers())
        .map(|w| metrics.worker_total_busy_duration(w))
        .sum()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const SECS: fn(u64) -> Duration = Duration::from_secs;

    /// A sampler on the production cadence, with its baseline at `t0`.
    fn sampler(t0: Instant) -> IoBusySampler {
        IoBusySampler::new(SECS(5), t0, Duration::ZERO)
    }

    fn ratio(busy: Duration, elapsed: Duration, workers: usize) -> f64 {
        let t0 = Instant::now();
        sampler(t0)
            .observe(t0 + elapsed, busy, workers)
            .expect("a full interval has passed")
            .ratio
    }

    /// The ratio is busy time over wall time, averaged over workers, and
    /// clamped to 1.
    #[test]
    fn the_ratio_is_mean_busy_time_over_elapsed_time() {
        assert_eq!(ratio(Duration::ZERO, SECS(5), 2), 0.0);
        assert_eq!(ratio(SECS(5), SECS(10), 1), 0.5);
        assert_eq!(ratio(SECS(5), SECS(5), 2), 0.5, "one of two workers busy");
        assert_eq!(ratio(SECS(10), SECS(5), 2), 1.0, "both workers busy");
        assert_eq!(ratio(SECS(30), SECS(5), 2), 1.0, "clamped at 1");
    }

    /// Each sample covers the busy time since the previous one only.
    #[test]
    fn each_sample_covers_the_interval_since_the_last() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let first = s.observe(t0 + SECS(5), SECS(5), 1).expect("sample");
        assert_eq!(first.ratio, 1.0);
        let second = s.observe(t0 + SECS(10), SECS(5), 1).expect("sample");
        assert_eq!(second.ratio, 0.0, "no busy time accrued since the first");
    }

    /// No sample before a full interval has passed, including at zero elapsed.
    #[test]
    fn no_sample_before_the_interval_has_passed() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        assert_eq!(s.observe(t0, Duration::ZERO, 1), None);
        assert_eq!(
            s.observe(t0 + Duration::from_millis(4_999), SECS(4), 1),
            None
        );
        assert!(s.observe(t0 + SECS(5), SECS(5), 1).is_some());
        let mut unpaced = IoBusySampler::new(Duration::ZERO, t0, Duration::ZERO);
        assert_eq!(unpaced.observe(t0, SECS(1), 1), None, "zero elapsed");
    }

    /// Feeds saturated (or idle) intervals of 5 s to `s`, starting at `from`
    /// seconds after `t0`, and returns the elapsed seconds of each sample that
    /// asked for the warning.
    fn feed(
        s: &mut IoBusySampler,
        t0: Instant,
        busy: &mut Duration,
        from: u64,
        to: u64,
        saturated: bool,
    ) -> Vec<u64> {
        let mut warned = Vec::new();
        let mut at = from + 5;
        while at <= to {
            if saturated {
                *busy += SECS(5);
            }
            let sample = s.observe(t0 + SECS(at), *busy, 1).expect("sample");
            if sample.sustained {
                warned.push(at);
            }
            at += 5;
        }
        warned
    }

    /// The warning fires once, when every sample over 60 s read saturated.
    #[test]
    fn the_warning_fires_once_after_sixty_saturated_seconds() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut busy = Duration::ZERO;
        assert_eq!(
            feed(&mut s, t0, &mut busy, 0, 120, true),
            vec![60],
            "fires at 60 s, not at 55 s, and once"
        );
    }

    /// A sample below the threshold restarts the window.
    #[test]
    fn a_dip_restarts_the_window() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut busy = Duration::ZERO;
        assert!(feed(&mut s, t0, &mut busy, 0, 30, true).is_empty());
        assert!(feed(&mut s, t0, &mut busy, 30, 35, false).is_empty());
        assert_eq!(
            feed(&mut s, t0, &mut busy, 35, 120, true),
            vec![95],
            "60 s after the dip"
        );
    }

    /// After a recovery the warning fires again on the next sustained run.
    #[test]
    fn the_warning_rearms_after_a_recovery() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut busy = Duration::ZERO;
        assert_eq!(feed(&mut s, t0, &mut busy, 0, 60, true), vec![60]);
        assert!(feed(&mut s, t0, &mut busy, 60, 65, false).is_empty());
        assert_eq!(feed(&mut s, t0, &mut busy, 65, 125, true), vec![125]);
    }

    /// A ratio at the threshold counts as saturated; just under it does not.
    #[test]
    fn the_threshold_is_inclusive() {
        let t0 = Instant::now();
        let mut at = IoBusySampler::new(SECS(10), t0, Duration::ZERO);
        let mut under = IoBusySampler::new(SECS(10), t0, Duration::ZERO);
        let (mut warned_at, mut warned_under) = (false, false);
        for i in 1..=6 {
            let now = t0 + SECS(10 * i);
            warned_at |= at.observe(now, SECS(9 * i), 1).expect("sample").sustained;
            warned_under |= under
                .observe(now, Duration::from_millis(8_900 * i), 1)
                .expect("sample")
                .sustained;
        }
        assert!(warned_at, "0.9 is saturated");
        assert!(!warned_under, "0.89 is not");
    }

    /// A second owner of the series fails to construct.
    #[test]
    fn a_second_io_runtime_owner_is_a_duplicate() {
        let labels = ComponentLabels::new("io-busy-duplicate", "runtime", "pipeline");
        let first = IoRuntimeMetrics::try_new(&labels).expect("free series");
        assert!(matches!(
            IoRuntimeMetrics::try_new(&labels),
            Err(MetricsError::DuplicateSeries(_))
        ));
        // The pipeline handle set on the same labels claims a different key.
        let _pipeline = PipelineMetrics::try_new(&labels, "test").expect("distinct claim");
        drop(first);
    }

    /// Samples `io` on a short cadence until `done` accepts a ratio, under a
    /// deadline, and returns that ratio.
    fn sample_until(io: &tokio::runtime::Runtime, done: impl Fn(f64) -> bool) -> f64 {
        let metrics = io.handle().metrics();
        let interval = Duration::from_millis(20);
        let mut s = IoBusySampler::new(interval, Instant::now(), busy_total(&metrics));
        let deadline = Instant::now() + SECS(10);
        loop {
            if let Some(sample) = s.observe_runtime(Instant::now(), &metrics)
                && done(sample.ratio)
            {
                return sample.ratio;
            }
            assert!(
                Instant::now() < deadline,
                "no matching sample before the deadline"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn one_worker_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// A worker kept busy by a spinning task reads as busy through the
    /// runtime's own metrics.
    #[test]
    fn a_spinning_worker_reads_busy() {
        let io = one_worker_runtime();
        let stop = Arc::new(AtomicBool::new(false));
        let spin = Arc::clone(&stop);
        io.spawn(async move {
            // CPU work inside a poll is what tokio counts as busy. A bare
            // `yield_now` loop would not do: the worker leaves its busy span
            // to poll the drivers on every yield.
            while !spin.load(Ordering::Relaxed) {
                let polled = Instant::now();
                while polled.elapsed() < Duration::from_millis(10) {
                    std::hint::spin_loop();
                }
                tokio::task::yield_now().await;
            }
        });
        let ratio = sample_until(&io, |r| r > 0.5);
        stop.store(true, Ordering::Relaxed);
        assert!(ratio > 0.5);
    }

    /// A worker with nothing to run reads as idle.
    #[test]
    fn an_idle_worker_reads_idle() {
        let io = one_worker_runtime();
        let ratio = sample_until(&io, |_| true);
        assert!(ratio < 0.5, "idle runtime read {ratio}");
    }

    /// Holds `held` of a two-worker runtime's workers for one 500 ms poll each
    /// and returns the ratio over that span once the busy time is published.
    fn two_worker_ratio(held: usize) -> f64 {
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let metrics = io.handle().metrics();
        let (t0, base) = (Instant::now(), busy_total(&metrics));
        // The barrier holds each poll until every task is polled, so no two
        // share a worker.
        let all = Arc::new(std::sync::Barrier::new(held));
        let tasks: Vec<_> = (0..held)
            .map(|_| {
                let all = Arc::clone(&all);
                io.spawn(async move {
                    all.wait();
                    let polled = Instant::now();
                    while polled.elapsed() < Duration::from_millis(500) {
                        std::hint::spin_loop();
                    }
                })
            })
            .collect();
        for task in tasks {
            io.block_on(task).expect("task");
        }
        // A worker publishes its busy time when it parks.
        let deadline = Instant::now() + SECS(10);
        while busy_total(&metrics).saturating_sub(base) < Duration::from_millis(400) * held as u32
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        IoBusySampler::new(Duration::ZERO, t0, base)
            .observe_runtime(Instant::now(), &metrics)
            .expect("sample")
            .ratio
    }

    /// The runtime path divides by the runtime's worker count and sums every
    /// worker: one held worker of two reads about half, both read busy.
    #[test]
    fn the_runtime_reading_covers_every_worker() {
        let one = two_worker_ratio(1);
        assert!((0.3..0.7).contains(&one), "one of two read {one}");
        let both = two_worker_ratio(2);
        assert!(both > 0.7, "two of two read {both}");
    }
}
