//! The SQS feed's own metric families, registered through the [`Meter`] the
//! runtime hands the feed (backend `sqs`, role `feed` → `spate_sqs_feed_*`).
//!
//! Every handle is resolved once, when the feed is given its `Meter`, and the
//! feed's tasks select one by `match` at each batch call. Nothing resolves a
//! name per event.

use crate::error::code_is;
use spate_core::metrics::{Counter, FeedOp, Meter};

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

/// Pre-registered `spate_sqs_feed_*` handles.
pub(crate) struct SqsFeedMetrics {
    refused_receipt_handle_is_invalid: Counter,
    refused_message_not_inflight: Counter,
    refused_other: Counter,
    /// Fails the feed accepted and gave up unsent at their unit's deadline.
    pub(crate) fails_dropped: Counter,
    /// Calls sent again, one per [`FeedOp`], indexed by `op as usize`.
    retried: [Counter; FeedOp::ALL.len()],
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
        SqsFeedMetrics {
            refused_receipt_handle_is_invalid: refused("ReceiptHandleIsInvalid"),
            refused_message_not_inflight: refused("MessageNotInflight"),
            refused_other: refused("other"),
            fails_dropped: meter.counter("fails_dropped_total", &[]),
            retried: FeedOp::ALL
                .map(|op| meter.counter("retries_total", &[("op", op.label().into())])),
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
    pub(crate) fn retried(&self, op: FeedOp) -> &Counter {
        &self.retried[op as usize]
    }
}
