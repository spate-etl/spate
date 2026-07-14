//! Connector-owned decode metrics → `etl_json_deser_*` families.
//!
//! The framework already wraps every deserializer in generic `etl_deser_*`
//! stage metrics, but those count at **payload** granularity. When `ndjson`
//! (or `array`) decoding skips an individual bad record under `on_error: skip`,
//! the framework sees the `deserialize` call return `Ok` with the good records
//! emitted — the skipped record is invisible to it. [`JsonDeserMetrics`]
//! surfaces exactly those per-record drops, mirroring the connector-owned
//! convention used by `etl_kafka_source_*` / `etl_kafka_sink_*`.
//!
//! Handles are resolved once, at build time, from a
//! [`Meter`](etl_core::metrics::Meter) scoped to the `json_deser` namespace;
//! the per-record path only touches the resolved `Counter`.

use etl_core::metrics::{Counter, Meter};

const RECORDS_DROPPED_TOTAL: &str = "records_dropped_total";
const L_REASON: &str = "reason";
const REASON_MALFORMED: &str = "malformed";
const REASON_DUPLICATE_KEY: &str = "duplicate_key";

/// Pre-registered `etl_json_deser_*` handles. Cheap to clone (the `metrics`
/// facade's `Arc`-backed handles); every clone feeds the same series.
#[derive(Clone, Debug)]
pub(crate) struct JsonDeserMetrics {
    malformed: Counter,
    duplicate_key: Counter,
}

impl JsonDeserMetrics {
    /// Resolve the drop counters from `meter` (namespace `json_deser`).
    /// Build-time only.
    pub(crate) fn new(meter: &Meter) -> Self {
        JsonDeserMetrics {
            malformed: meter.counter(
                RECORDS_DROPPED_TOTAL,
                &[(L_REASON, REASON_MALFORMED.into())],
            ),
            duplicate_key: meter.counter(
                RECORDS_DROPPED_TOTAL,
                &[(L_REASON, REASON_DUPLICATE_KEY.into())],
            ),
        }
    }

    /// Count one record dropped by `on_error: skip` because it did not parse
    /// or did not match the target type.
    pub(crate) fn dropped_malformed(&self) {
        self.malformed.increment(1);
    }

    /// Count one record dropped by `on_error: skip` because it contained a
    /// duplicate object key (`reject_duplicate_keys`).
    pub(crate) fn dropped_duplicate_key(&self) {
        self.duplicate_key.increment(1);
    }
}
