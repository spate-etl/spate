//! A recorder that reports which metric names and series have been written.
//!
//! [`WitnessRecorder`] wraps another recorder and returns handles that
//! delegate to it. [`Witness::written`] lists the names something recorded.
//! [`Witness::registered`] lists the names that exist. Resolving a gauge
//! handle publishes the series, so the two sets differ.
//!
//! [`Witness::written_series`] reports the same writes keyed by name and
//! labels together. A family that publishes both an aggregate and a labeled
//! series carries one name, so a name alone cannot separate a family whose
//! labeled half nothing writes from a complete one.
//!
//! Install it globally before the pipeline builds. A handle binds to the
//! recorder present when it is constructed, and `metrics::with_local_recorder`
//! applies to one thread, so it does not see the driver or controller
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
    /// `name{k="v",…}` in the key's own label order, or the bare name when
    /// the key carries no labels.
    series: String,
    written: AtomicBool,
}

/// Render a key the way [`Site::series`] holds it.
fn render_series(key: &Key) -> String {
    let labels: Vec<String> = key
        .labels()
        .map(|l| format!(r#"{}="{}""#, l.key(), l.value()))
        .collect();
    if labels.is_empty() {
        return key.name().to_owned();
    }
    format!("{}{{{}}}", key.name(), labels.join(","))
}

/// The names and series registered and written since installation.
///
/// A write is a relaxed store on a flag the handle already holds, so an
/// instrumented hot path takes no lock. Reading leaves the flags in place, so
/// a caller may sample repeatedly.
#[derive(Clone, Default)]
pub(crate) struct Witness(Arc<Mutex<Vec<Arc<Site>>>>);

impl Witness {
    fn site(&self, key: &Key) -> Arc<Site> {
        let site = Arc::new(Site {
            name: key.name().to_owned(),
            series: render_series(key),
            written: AtomicBool::new(false),
        });
        self.0.lock().expect("witness lock").push(Arc::clone(&site));
        site
    }

    /// Every metric name something has written.
    pub(crate) fn written(&self) -> BTreeSet<String> {
        self.collect(|s| s.written.load(Ordering::Relaxed), |s| &s.name)
    }

    /// Every series something has written, as `name{k="v",…}`.
    pub(crate) fn written_series(&self) -> BTreeSet<String> {
        self.collect(|s| s.written.load(Ordering::Relaxed), |s| &s.series)
    }

    /// Every metric name that has been registered, written or not.
    pub(crate) fn registered(&self) -> BTreeSet<String> {
        self.collect(|_| true, |s| &s.name)
    }

    /// Clear every write flag, keeping the registrations. A later
    /// [`written`](Self::written) reports only what was written after this
    /// call.
    pub(crate) fn reset(&self) {
        for site in self.0.lock().expect("witness lock").iter() {
            site.written.store(false, Ordering::Relaxed);
        }
    }

    fn collect(
        &self,
        keep: impl Fn(&Site) -> bool,
        of: impl Fn(&Site) -> &str,
    ) -> BTreeSet<String> {
        self.0
            .lock()
            .expect("witness lock")
            .iter()
            .filter(|s| keep(s))
            .map(|s| of(s).to_owned())
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

    /// A handle to what this recorder has seen. The recorder itself moves
    /// into the global slot on install.
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
        let site = self.witness.site(key);
        Counter::from_arc(Arc::new(CounterSpy {
            inner: self.inner.register_counter(key, metadata),
            site,
        }))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let site = self.witness.site(key);
        Gauge::from_arc(Arc::new(GaugeSpy {
            inner: self.inner.register_gauge(key, metadata),
            site,
        }))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let site = self.witness.site(key);
        Histogram::from_arc(Arc::new(HistogramSpy {
            inner: self.inner.register_histogram(key, metadata),
            site,
        }))
    }
}
