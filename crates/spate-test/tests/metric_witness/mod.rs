//! A recorder that reports which metric names production code has written.
//!
//! Wraps another [`Recorder`] and returns handles that delegate to it, marking
//! the series as written on the way through. [`Witness::written`] is therefore
//! the set of names something actually recorded, as distinct from
//! [`Witness::registered`], the set that merely exists.
//!
//! That distinction is the whole point. A framework metric handle is resolved
//! at pipeline build time, and resolving a gauge publishes it, so every
//! declared series renders `0` whether or not any code path writes it.
//! "Present in the exposition" is not evidence, and neither is a value: a gauge
//! may legitimately read `0`.
//!
//! `metrics_util::debugging::DebuggingRecorder` does not answer this. It tracks
//! at registration, so its snapshot contains never-written series, and its
//! snapshot consumes, swapping counters and gauges to zero as it reads.
//!
//! Install this globally, before the pipeline builds. Handles bind to whichever
//! recorder exists when they are constructed, and `metrics::with_local_recorder`
//! is thread-local, so it would miss every write from the driver and controller
//! threads.

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// One registered series, and whether anything has written it.
struct Site {
    name: String,
    written: AtomicBool,
}

/// The names registered and written since installation.
///
/// Registration takes the lock and is cold; a write is a relaxed store on a
/// flag the handle already holds, so instrumented hot paths stay lock-free.
/// Reading does not consume, so a caller may sample repeatedly.
#[derive(Clone, Default)]
pub(crate) struct Witness(Arc<Mutex<Vec<Arc<Site>>>>);

impl Witness {
    fn site(&self, name: &str) -> Arc<Site> {
        let site = Arc::new(Site {
            name: name.to_owned(),
            written: AtomicBool::new(false),
        });
        self.0.lock().expect("witness lock").push(Arc::clone(&site));
        site
    }

    /// Every metric name something has written.
    pub(crate) fn written(&self) -> BTreeSet<String> {
        self.collect(|s| s.written.load(Ordering::Relaxed))
    }

    /// Every metric name that has been registered, written or not.
    pub(crate) fn registered(&self) -> BTreeSet<String> {
        self.collect(|_| true)
    }

    fn collect(&self, keep: impl Fn(&Site) -> bool) -> BTreeSet<String> {
        self.0
            .lock()
            .expect("witness lock")
            .iter()
            .filter(|s| keep(s))
            .map(|s| s.name.clone())
            .collect()
    }
}

struct CounterSpy {
    inner: Counter,
    site: Arc<Site>,
}

impl CounterFn for CounterSpy {
    fn increment(&self, value: u64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.increment(value);
    }

    fn absolute(&self, value: u64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.absolute(value);
    }
}

struct GaugeSpy {
    inner: Gauge,
    site: Arc<Site>,
}

impl GaugeFn for GaugeSpy {
    fn increment(&self, value: f64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.decrement(value);
    }

    fn set(&self, value: f64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.set(value);
    }
}

struct HistogramSpy {
    inner: Histogram,
    site: Arc<Site>,
}

impl HistogramFn for HistogramSpy {
    fn record(&self, value: f64) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.record(value);
    }

    /// Overridden rather than left to the default, which would loop over
    /// [`record`](Self::record) and re-mark the same site.
    fn record_many(&self, value: f64, count: usize) {
        self.site.written.store(true, Ordering::Relaxed);
        self.inner.record_many(value, count);
    }
}

/// A [`Recorder`] that delegates to `inner` and witnesses every write.
pub(crate) struct WitnessRecorder<R> {
    inner: R,
    witness: Witness,
}

impl<R: Recorder> WitnessRecorder<R> {
    pub(crate) fn new(inner: R) -> Self {
        WitnessRecorder {
            inner,
            witness: Witness::default(),
        }
    }

    /// A handle to what this recorder has seen. Cloneable and cheap; the
    /// recorder itself moves into the global slot on install.
    pub(crate) fn witness(&self) -> Witness {
        self.witness.clone()
    }
}

impl<R: Recorder> Recorder for WitnessRecorder<R> {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        let site = self.witness.site(key.name());
        Counter::from_arc(Arc::new(CounterSpy {
            inner: self.inner.register_counter(key, metadata),
            site,
        }))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let site = self.witness.site(key.name());
        Gauge::from_arc(Arc::new(GaugeSpy {
            inner: self.inner.register_gauge(key, metadata),
            site,
        }))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let site = self.witness.site(key.name());
        Histogram::from_arc(Arc::new(HistogramSpy {
            inner: self.inner.register_histogram(key, metadata),
            site,
        }))
    }
}
