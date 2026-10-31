//! The SQS feed's own metric families, registered through the [`Meter`] the
//! runtime hands the feed (backend `sqs`, role `feed` → `spate_sqs_feed_*`).
//!
//! Every handle is resolved once, when the feed is given its `Meter`, and the
//! feed's tasks select one by `match` at each batch call. Nothing resolves a
//! name per event.

use crate::error::code_is;
use spate_core::metrics::{Counter, Meter};

/// Why SQS refused to extend one entry, as the `code` label names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    ReceiptHandleIsInvalid,
    MessageNotInflight,
    /// Any other code, an empty one included.
    Other,
}

impl Refusal {
    /// The refusal SQS's `code` names, bare or with its
    /// `AWS.SimpleQueueService.` prefix.
    pub(crate) fn of(code: &str) -> Refusal {
        if code_is(code, "ReceiptHandleIsInvalid") {
            Refusal::ReceiptHandleIsInvalid
        } else if code_is(code, "MessageNotInflight") {
            Refusal::MessageNotInflight
        } else {
            Refusal::Other
        }
    }

    /// Whether the receipt no longer holds the message.
    pub(crate) fn takes_unit(self) -> bool {
        self != Refusal::Other
    }
}

/// A feed operation whose call the feed sends again, as the `op` label names
/// it. The values are those of `spate_feed_errors_total`'s `op` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Receive,
    Renew,
    Acknowledge,
    Fail,
    HandBack,
    Publish,
}

/// Pre-registered `spate_sqs_feed_*` handles.
pub(crate) struct SqsFeedMetrics {
    refused_receipt_handle_is_invalid: Counter,
    refused_message_not_inflight: Counter,
    refused_other: Counter,
    /// Fails the feed accepted and gave up unsent at their unit's deadline.
    pub(crate) fails_dropped: Counter,
    retried_receive: Counter,
    retried_renew: Counter,
    retried_acknowledge: Counter,
    retried_fail: Counter,
    retried_hand_back: Counter,
    retried_publish: Counter,
}

impl std::fmt::Debug for SqsFeedMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqsFeedMetrics")
    }
}

impl SqsFeedMetrics {
    /// Resolve every family under `meter`.
    pub(crate) fn new(meter: &Meter) -> SqsFeedMetrics {
        let refused = |code: &'static str| {
            meter.counter("extensions_refused_total", &[("code", code.into())])
        };
        let retried = |op: &'static str| meter.counter("retries_total", &[("op", op.into())]);
        SqsFeedMetrics {
            refused_receipt_handle_is_invalid: refused("ReceiptHandleIsInvalid"),
            refused_message_not_inflight: refused("MessageNotInflight"),
            refused_other: refused("other"),
            fails_dropped: meter.counter("fails_dropped_total", &[]),
            retried_receive: retried("receive"),
            retried_renew: retried("renew"),
            retried_acknowledge: retried("acknowledge"),
            retried_fail: retried("fail"),
            retried_hand_back: retried("hand_back"),
            retried_publish: retried("publish"),
        }
    }

    /// The counter of extensions refused for `refusal`.
    pub(crate) fn refused(&self, refusal: Refusal) -> &Counter {
        match refusal {
            Refusal::ReceiptHandleIsInvalid => &self.refused_receipt_handle_is_invalid,
            Refusal::MessageNotInflight => &self.refused_message_not_inflight,
            Refusal::Other => &self.refused_other,
        }
    }

    /// The counter of calls sent again for `op`.
    pub(crate) fn retried(&self, op: Op) -> &Counter {
        match op {
            Op::Receive => &self.retried_receive,
            Op::Renew => &self.retried_renew,
            Op::Acknowledge => &self.retried_acknowledge,
            Op::Fail => &self.retried_fail,
            Op::HandBack => &self.retried_hand_back,
            Op::Publish => &self.retried_publish,
        }
    }
}
