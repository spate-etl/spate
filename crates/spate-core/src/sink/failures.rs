//! The reason each sink last abandoned a batch, shared between the sink pools
//! and the pipeline controller.

use std::sync::{Arc, Mutex};

/// The latest reason each sink abandoned a batch for.
///
/// One instance serves a whole pipeline: pass a clone to every
/// [`SinkPool::spawn`](super::SinkPool::spawn) and to
/// [`SinkRuntime`](crate::pipeline::SinkRuntime). A pipeline that fails behind
/// an abandoned batch names each sink recorded here and its reason.
#[derive(Clone, Debug, Default)]
pub struct SinkFailures(Arc<Mutex<Vec<(String, String)>>>);

impl SinkFailures {
    /// An empty register.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `reason` as `sink`'s latest, replacing any earlier one.
    pub(crate) fn record(&self, sink: &str, reason: String) {
        let mut entries = self.0.lock().expect("sink failures lock");
        match entries.iter_mut().find(|(name, _)| name == sink) {
            Some(entry) => entry.1 = reason,
            None => entries.push((sink.to_owned(), reason)),
        }
    }

    /// Each recorded sink and its reason, in the order the sinks first
    /// abandoned a batch, or `None` when none has.
    pub(crate) fn describe(&self) -> Option<String> {
        let entries = self.0.lock().expect("sink failures lock");
        (!entries.is_empty()).then(|| {
            entries
                .iter()
                .map(|(sink, reason)| format!("sink `{sink}` abandoned a batch: {reason}"))
                .collect::<Vec<_>>()
                .join("; ")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_keeps_the_latest_reason_per_sink() {
        let failures = SinkFailures::new();
        assert_eq!(failures.describe(), None);
        failures.record("a", "first".into());
        failures.record("b", "other".into());
        failures.record("a", "second".into());
        assert_eq!(
            failures.describe().as_deref(),
            Some("sink `a` abandoned a batch: second; sink `b` abandoned a batch: other")
        );
    }
}
