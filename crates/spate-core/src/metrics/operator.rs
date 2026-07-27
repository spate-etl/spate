//! Operator-stage handles (`spate_operator_*`).

use super::labels::ComponentLabels;
use super::names;
use crate::error::ErrorClass;
use metrics::{Counter, Histogram};
use std::time::Duration;

/// Operator-stage handles (`spate_operator_*`).
#[derive(Debug)]
pub struct OperatorMetrics {
    records_in: Counter,
    records_out: Counter,
    dropped_filtered: Counter,
    dropped_skip: Counter,
    dropped_unrouted: Counter,
    err_retryable: Counter,
    err_record: Counter,
    err_fatal: Counter,
    batch_duration: Histogram,
}

impl OperatorMetrics {
    /// Resolve all operator handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        OperatorMetrics {
            records_in: labels.counter(names::OPERATOR_RECORDS_IN_TOTAL),
            records_out: labels.counter(names::OPERATOR_RECORDS_OUT_TOTAL),
            dropped_filtered: labels.counter1(
                names::OPERATOR_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "filtered",
            ),
            dropped_skip: labels.counter1(
                names::OPERATOR_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "skip_policy",
            ),
            dropped_unrouted: labels.counter1(
                names::OPERATOR_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "unrouted",
            ),
            err_retryable: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::Retryable.label(),
            ),
            err_record: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::RecordLevel.label(),
            ),
            err_fatal: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::Fatal.label(),
            ),
            batch_duration: labels.histogram(names::OPERATOR_BATCH_DURATION_SECONDS),
        }
    }

    /// Record one processed batch.
    #[inline]
    pub fn batch(&self, records_in: u64, records_out: u64, d: Duration) {
        self.records_in.increment(records_in);
        self.records_out.increment(records_out);
        self.batch_duration.record(d.as_secs_f64());
    }

    /// Count records removed by a predicate.
    #[inline]
    pub fn filtered(&self, n: u64) {
        self.dropped_filtered.increment(n);
    }

    /// Count records dropped by the Skip error policy.
    #[inline]
    pub fn skipped(&self, n: u64) {
        self.dropped_skip.increment(n);
    }

    /// Count records dropped because they matched no split-sink branch and
    /// the split's `unmatched` policy is `Skip`.
    #[inline]
    pub fn unrouted(&self, n: u64) {
        self.dropped_unrouted.increment(n);
    }

    /// Count user-code errors of one taxonomy class.
    #[inline]
    pub fn errors(&self, class: ErrorClass, n: u64) {
        match class {
            ErrorClass::Retryable => self.err_retryable.increment(n),
            ErrorClass::RecordLevel => self.err_record.increment(n),
            ErrorClass::Fatal => self.err_fatal.increment(n),
        }
    }
}
