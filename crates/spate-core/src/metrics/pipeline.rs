//! Pipeline-level handles (`spate_pipeline_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::Gauge;
use std::collections::VecDeque;
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
    /// Mean busy fraction of the workers over the trailing window, clamped
    /// to `0..=1` for publication.
    pub(crate) ratio: f64,
    /// True on the one sample that warns: the window spans its full length
    /// and its unclamped ratio reached [`IoBusySampler::SATURATED`].
    pub(crate) sustained: bool,
}

/// Turns the I/O runtime's cumulative busy time into a ratio over a
/// trailing window, and decides when that ratio warns.
///
/// A worker records its busy time about every 31 polls, at its periodic
/// maintenance, or when it parks. A worker held by long polls therefore
/// reports in lumps of about 31 polls' worth of busy time, and once the
/// window is full a reading can be off by up to one lump over it: a few
/// percent for polls under about 500 ms over [`Self::WINDOW`], and about 10%
/// at 1 s polls. Longer polls read late and unevenly.
///
/// The warning fires once, on the first sample whose window spans the full
/// length with an unclamped ratio at or above [`Self::SATURATED`]. It re-arms
/// only after every sample for one full window has read below that.
///
/// Pure: the caller passes the instant and the busy total, so tests drive it
/// without a clock.
#[derive(Debug)]
pub(crate) struct IoBusySampler {
    interval: Duration,
    window: Duration,
    /// `(instant, busy_total)` at the baseline and at each sample, oldest
    /// first. The front is the newest entry at least `window` old, or the
    /// baseline while none is.
    history: VecDeque<(Instant, Duration)>,
    warned: bool,
    /// The first sample of the current run of samples below
    /// [`Self::SATURATED`].
    below_since: Option<Instant>,
}

impl IoBusySampler {
    /// The unclamped ratio at or above which the window counts as saturated.
    pub(crate) const SATURATED: f64 = 0.9;
    /// The trailing window the pipeline averages over.
    pub(crate) const WINDOW: Duration = Duration::from_secs(300);

    /// A sampler that yields at most one sample per `interval`, averaged over
    /// the trailing `window`, with its baseline at `now` and `busy_total`.
    /// `window` is also how long the ratio must stay below
    /// [`Self::SATURATED`] before the warning re-arms.
    pub(crate) fn new(
        interval: Duration,
        window: Duration,
        now: Instant,
        busy_total: Duration,
    ) -> Self {
        IoBusySampler {
            interval,
            window,
            history: VecDeque::from([(now, busy_total)]),
            warned: false,
            below_since: None,
        }
    }

    /// Take a sample when at least one interval has passed since the last.
    ///
    /// `busy_total` is the sum over all `workers` of their cumulative busy
    /// time. The ratio covers the trailing window, or the time since the
    /// baseline while that is shorter. Returns `None` before a full interval
    /// has passed.
    pub(crate) fn observe(
        &mut self,
        now: Instant,
        busy_total: Duration,
        workers: usize,
    ) -> Option<IoBusySample> {
        let &(last_at, _) = self.history.back()?;
        let elapsed = now.saturating_duration_since(last_at);
        if elapsed.is_zero() || elapsed < self.interval {
            return None;
        }
        self.history.push_back((now, busy_total));
        while self
            .history
            .get(1)
            .is_some_and(|&(at, _)| now.saturating_duration_since(at) >= self.window)
        {
            self.history.pop_front();
        }
        let &(start, start_busy) = self.history.front()?;
        let span = now.saturating_duration_since(start);
        let busy = busy_total.saturating_sub(start_busy);
        let unclamped = busy.as_secs_f64() / (span.as_secs_f64() * workers.max(1) as f64);

        // Judged unclamped: a lump that lands inside the window carries busy
        // time from before it, and clamping would drop that time.
        let mut sustained = false;
        if unclamped < Self::SATURATED {
            let since = *self.below_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= self.window {
                self.warned = false;
            }
        } else {
            self.below_since = None;
            if span >= self.window && !self.warned {
                self.warned = true;
                sustained = true;
            }
        }
        Some(IoBusySample {
            ratio: unclamped.min(1.0),
            sustained,
        })
    }
}

/// The I/O runtime's reading: the sum of every async worker's cumulative
/// busy time, and the worker count. The blocking pool is not included.
pub(crate) fn busy_reading(metrics: &tokio::runtime::RuntimeMetrics) -> (Duration, usize) {
    (busy_total(metrics), metrics.num_workers())
}

fn busy_total(metrics: &tokio::runtime::RuntimeMetrics) -> Duration {
    (0..metrics.num_workers())
        .map(|w| metrics.worker_total_busy_duration(w))
        .sum()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const SECS: fn(u64) -> Duration = Duration::from_secs;

    /// A sampler on the production cadence and window, with its baseline at
    /// `t0`.
    fn sampler(t0: Instant) -> IoBusySampler {
        IoBusySampler::new(SECS(5), IoBusySampler::WINDOW, t0, Duration::ZERO)
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

    /// The ratio averages since start until the window is full, then over
    /// the trailing five minutes, so busy time older than that drops out.
    #[test]
    fn the_ratio_covers_the_trailing_window() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let busy = SECS(5);
        let mut at = |secs| s.observe(t0 + SECS(secs), busy, 1).expect("sample").ratio;
        assert_eq!(at(5), 1.0);
        assert_eq!(at(10), 0.5, "averaged since start");
        let mut last = 0.0;
        for secs in (15..=300).step_by(5) {
            last = at(secs);
        }
        assert_eq!(last, 5.0 / 300.0);
        assert_eq!(at(305), 0.0, "the busy first interval left the window");
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
        let mut unpaced = IoBusySampler::new(Duration::ZERO, SECS(300), t0, Duration::ZERO);
        assert_eq!(unpaced.observe(t0, SECS(1), 1), None, "zero elapsed");
    }

    /// Feeds one-worker samples 5 s apart, from `from` to `to` seconds after
    /// `t0`, adding `add(at)` busy time at each, and returns every sample with
    /// its elapsed seconds.
    fn feed(
        s: &mut IoBusySampler,
        t0: Instant,
        busy: &mut Duration,
        (from, to): (u64, u64),
        add: impl Fn(u64) -> Duration,
    ) -> Vec<(u64, IoBusySample)> {
        (from + 5..=to)
            .step_by(5)
            .map(|at| {
                *busy += add(at);
                (at, s.observe(t0 + SECS(at), *busy, 1).expect("sample"))
            })
            .collect()
    }

    /// The elapsed seconds of each sample that asked for the warning.
    fn warnings(samples: &[(u64, IoBusySample)]) -> Vec<u64> {
        samples
            .iter()
            .filter(|(_, sample)| sample.sustained)
            .map(|(at, _)| *at)
            .collect()
    }

    fn busy(_: u64) -> Duration {
        SECS(5)
    }

    fn idle(_: u64) -> Duration {
        Duration::ZERO
    }

    /// A span of elapsed seconds and the busy time each sample in it adds.
    type Span = ((u64, u64), fn(u64) -> Duration);

    /// Runs one scenario of consecutive spans against a fresh sampler and
    /// returns the warnings each span asked for.
    fn scenario(spans: &[Span]) -> Vec<Vec<u64>> {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut total = Duration::ZERO;
        spans
            .iter()
            .map(|(span, add)| warnings(&feed(&mut s, t0, &mut total, *span, add)))
            .collect()
    }

    /// The warning fires once, when the window first spans five minutes of
    /// saturation.
    #[test]
    fn the_warning_fires_once_after_a_saturated_window() {
        assert_eq!(
            scenario(&[((0, 900), busy)]),
            [vec![300]],
            "fires at 300 s, not at 295 s, and once"
        );
    }

    /// Until the window spans five minutes nothing warns, however far the
    /// busy time runs over capacity.
    #[test]
    fn no_warning_before_the_window_spans_five_minutes() {
        let over = |_| SECS(30);
        assert_eq!(
            scenario(&[((0, 295), over), ((295, 300), over)]),
            [vec![], vec![300]]
        );
    }

    /// Counts the warnings over `secs` of samples taken every 100.3 ms, the
    /// run loop's polling, with `busy_at(t)` the cumulative busy seconds of one
    /// worker at `t` seconds.
    fn warnings_over(secs: f64, busy_at: impl Fn(f64) -> f64) -> u32 {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let (mut t, mut warns) = (0.0f64, 0);
        while t < secs {
            t += 0.1003;
            let busy = Duration::from_secs_f64(busy_at(t));
            if let Some(x) = s.observe(t0 + Duration::from_secs_f64(t), busy, 1) {
                warns += u32::from(x.sustained);
            }
        }
        warns
    }

    /// A saturated worker whose busy time arrives as one 25 s lump every
    /// 25 s, as a worker held by 800 ms polls reports it, warns exactly once
    /// in 30 minutes.
    #[test]
    fn a_saturated_worker_reporting_in_lumps_warns_once() {
        let lumps = |t: f64| {
            if t < 0.3 {
                0.0
            } else {
                (((t - 0.3) / 25.0).floor() + 1.0) * 25.0
            }
        };
        assert_eq!(warnings_over(1_800.0, lumps), 1);
    }

    /// A worker 70% busy whose busy time arrives as one 35 s lump every 50 s
    /// never warns.
    #[test]
    fn a_worker_busy_in_lumps_below_the_threshold_does_not_warn() {
        let lumps = |t: f64| {
            if t < 0.3 {
                0.0
            } else {
                (((t - 0.3) / 50.0).floor() + 1.0) * 35.0
            }
        };
        assert_eq!(warnings_over(1_800.0, lumps), 0);
    }

    /// A saturated worker whose busy time arrives in lumps, here a 15 s lump
    /// every third sample, reads saturated once the window is full.
    #[test]
    fn busy_time_published_in_lumps_reads_saturated() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut total = Duration::ZERO;
        let lumps = |at| {
            if at % 15 == 0 {
                SECS(15)
            } else {
                Duration::ZERO
            }
        };
        let samples = feed(&mut s, t0, &mut total, (0, 600), lumps);
        assert_eq!(warnings(&samples), [300]);
        for (at, sample) in samples.iter().filter(|(at, _)| *at >= 300) {
            assert!(sample.ratio >= 0.9, "{at} s read {}", sample.ratio);
        }
    }

    /// A worker busy half the time reads half and never warns.
    #[test]
    fn a_half_busy_worker_does_not_warn() {
        let t0 = Instant::now();
        let mut s = sampler(t0);
        let mut total = Duration::ZERO;
        let samples = feed(&mut s, t0, &mut total, (0, 900), |_| {
            Duration::from_millis(2_500)
        });
        assert!(warnings(&samples).is_empty());
        assert!(samples.iter().all(|(_, sample)| sample.ratio == 0.5));
    }

    /// A 65 s idle gap keeps the trailing ratio below the threshold for
    /// 295 s, from 335 s to 630 s, one interval short of a window. A 40 s
    /// gap keeps it below from 735 s to 1005 s. Neither re-arms the warning,
    /// though the two together span more than a window. A 70 s gap keeps it
    /// below from 1135 s to 1435 s, exactly one window, which re-arms it, and
    /// the warning fires again at 1440 s when the window is back at the
    /// threshold.
    #[test]
    fn the_warning_rearms_only_after_a_full_window_below() {
        assert_eq!(
            scenario(&[
                ((0, 300), busy),
                ((300, 365), idle),
                ((365, 700), busy),
                ((700, 740), idle),
                ((740, 1_100), busy),
                ((1_100, 1_170), idle),
                ((1_170, 1_500), busy),
            ]),
            [
                vec![300],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![1_440]
            ],
        );
    }

    /// A ratio at the threshold counts as saturated; just under it does not.
    #[test]
    fn the_threshold_is_inclusive() {
        let t0 = Instant::now();
        let mut at = IoBusySampler::new(SECS(10), SECS(300), t0, Duration::ZERO);
        let mut under = IoBusySampler::new(SECS(10), SECS(300), t0, Duration::ZERO);
        let (mut warned_at, mut warned_under) = (false, false);
        for i in 1..=30 {
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
        let mut s = IoBusySampler::new(interval, SECS(1), Instant::now(), busy_total(&metrics));
        let deadline = Instant::now() + SECS(10);
        loop {
            let (busy, workers) = busy_reading(&metrics);
            if let Some(sample) = s.observe(Instant::now(), busy, workers)
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
        let started = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + SECS(10);
        let mut tasks = Vec::with_capacity(held);
        for n in 1..=held {
            let started_here = Arc::clone(&started);
            tasks.push(io.spawn(async move {
                started_here.fetch_add(1, Ordering::SeqCst);
                let polled = Instant::now();
                while polled.elapsed() < Duration::from_millis(500) {
                    std::hint::spin_loop();
                }
            }));
            // The next task is spawned only once this one is inside its poll,
            // so it lands in the shared queue and not behind the held worker.
            while started.load(Ordering::SeqCst) < n {
                assert!(Instant::now() < deadline, "task {n} was not polled");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
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
        let (busy, workers) = busy_reading(&metrics);
        IoBusySampler::new(Duration::ZERO, SECS(300), t0, base)
            .observe(Instant::now(), busy, workers)
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
