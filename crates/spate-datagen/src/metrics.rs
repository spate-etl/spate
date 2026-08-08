//! Connector-owned metric families, registered through the source's [`Meter`]
//! (namespace `datagen`, role `source` → `spate_datagen_source_*`).
//!
//! Every handle is resolved once at `open` and never on the record path
//! (INV-8). The `event` label has three values and exactly three
//! pre-registered handles behind it, so counting a generated event is a
//! pointer selection rather than a name lookup.
//!
//! The split between the two structs is INV-10, not tidiness. **Lanes own
//! counters only**; every gauge belongs to the control plane, which writes it
//! once per `poll_events` from the shared atomics the lanes publish into. Two
//! lanes writing one gauge series would be two writers racing to describe one
//! piece of state, and the exposition could not show that had happened.
//!
//! Deliberately absent: `spate_source_lag_records`. Lag for an unbounded
//! generator is infinite, so the series would exist or not depending on
//! whether `count` was set — a series that appears and disappears with a
//! configuration key is worse than one that is absent. `SourceCtx`'s
//! `stage_metrics` is therefore left untouched.

use crate::events::StorefrontEvent;
use spate_core::metrics::{Counter, Gauge, Meter};

/// The counters a lane increments. Cloned into every lane; `metrics` handles
/// are `Arc`-backed, so every clone feeds the same series.
#[derive(Clone, Debug)]
pub(crate) struct LaneCounters {
    order_placed: Counter,
    payment_captured: Counter,
    refund_issued: Counter,
    /// Release cadences that fired.
    pub(crate) ticks: Counter,
    /// Cadences that fired late enough to have missed a whole interval.
    pub(crate) tick_overruns: Counter,
}

impl LaneCounters {
    /// The pre-registered handle for `event`'s kind. Never resolves a name.
    pub(crate) fn generated(&self, event: &StorefrontEvent) -> &Counter {
        match event {
            StorefrontEvent::OrderPlaced(_) => &self.order_placed,
            StorefrontEvent::PaymentCaptured(_) => &self.payment_captured,
            StorefrontEvent::RefundIssued(_) => &self.refund_issued,
        }
    }
}

/// Every `spate_datagen_source_*` handle, resolved once.
#[derive(Debug)]
pub(crate) struct DatagenMetrics {
    /// The lane half, handed out by cloning.
    counters: LaneCounters,
    /// Events left to generate across every lane; 0 for an unbounded stream.
    events_remaining: Gauge,
    /// Orders placed and not yet captured, across every lane.
    open_orders: Gauge,
    /// Last committed watermark per partition, indexed by partition id.
    /// Empty unless `metrics.per_partition_detail` is on — this is the one
    /// cardinality-sensitive family here.
    committed_offset: Vec<Gauge>,
}

impl DatagenMetrics {
    /// Resolve every family under the runtime-minted scope. Build-time only.
    pub(crate) fn new(
        meter: &Meter,
        partitions: u32,
        per_partition_detail: bool,
    ) -> DatagenMetrics {
        let generated = |event: &'static str| {
            meter.counter("events_generated_total", &[("event", event.into())])
        };
        DatagenMetrics {
            counters: LaneCounters {
                order_placed: generated("order_placed"),
                payment_captured: generated("payment_captured"),
                refund_issued: generated("refund_issued"),
                ticks: meter.counter("ticks_total", &[]),
                tick_overruns: meter.counter("tick_overrun_total", &[]),
            },
            events_remaining: meter.gauge("events_remaining", &[]),
            open_orders: meter.gauge("open_orders", &[]),
            committed_offset: if per_partition_detail {
                (0..partitions)
                    .map(|p| {
                        meter.gauge("committed_offset", &[("partition", p.to_string().into())])
                    })
                    .collect()
            } else {
                Vec::new()
            },
        }
    }

    /// The lane half of these handles.
    pub(crate) fn counters(&self) -> LaneCounters {
        self.counters.clone()
    }

    /// Publish the control plane's gauges. Called once per `poll_events`, from
    /// the controller thread and nowhere else.
    pub(crate) fn publish(&self, events_remaining: u64, open_orders: u64) {
        self.events_remaining.set(events_remaining as f64);
        self.open_orders.set(open_orders as f64);
    }

    /// Record a committed watermark. A no-op unless per-partition detail is
    /// on, and unless the partition is one this source assigned.
    pub(crate) fn set_committed(&self, partition: u32, offset: i64) {
        if let Some(gauge) = self.committed_offset.get(partition as usize) {
            gauge.set(offset as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{OrderPlaced, PaymentCaptured, RefundIssued};
    use std::borrow::Cow;

    /// Run `f` against a local Prometheus recorder and return the rendered
    /// exposition. Handles must be resolved inside `f`.
    fn render(f: impl FnOnce()) -> String {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.run_upkeep();
        handle.render()
    }

    /// A test `Meter` under the `datagen` namespace. Names render as
    /// `spate_datagen_<local>`; the runtime's role-scoped variant is
    /// `spate_datagen_source_<local>` and the translation is identical.
    ///
    /// The component label is per-test: `cargo test` runs a binary's tests in
    /// one process, so a shared label would let one test read another's
    /// series.
    fn meter(component: &'static str) -> Meter {
        Meter::with_namespace("datagen", "orders", component, "datagen")
    }

    fn one_of_each() -> [StorefrontEvent; 3] {
        [
            StorefrontEvent::OrderPlaced(OrderPlaced {
                order_id: 1,
                customer_id: 2,
                region: Cow::Borrowed("eu-west"),
                placed_at: 0,
                lines: Vec::new(),
            }),
            StorefrontEvent::PaymentCaptured(PaymentCaptured {
                order_id: 1,
                amount_cents: 10,
            }),
            StorefrontEvent::RefundIssued(RefundIssued {
                order_id: 1,
                amount_cents: 5,
                reason: Cow::Borrowed("damaged"),
            }),
        ]
    }

    /// Each event kind has its own pre-registered handle, so the `event` label
    /// costs no lookup on the record path (INV-8) and its cardinality is three
    /// by construction.
    #[test]
    fn every_event_kind_counts_against_its_own_pre_registered_handle() {
        let rendered = render(|| {
            let metrics = DatagenMetrics::new(&meter("events"), 2, false);
            let counters = metrics.counters();
            for (repeats, event) in one_of_each().iter().enumerate() {
                counters.generated(event).increment(repeats as u64 + 1);
            }
            counters.ticks.increment(4);
            counters.tick_overruns.increment(1);
        });
        for (event, count) in [
            ("order_placed", 1),
            ("payment_captured", 2),
            ("refund_issued", 3),
        ] {
            let want = format!(
                r#"spate_datagen_events_generated_total{{pipeline="orders",component="events",component_type="datagen",event="{event}"}} {count}"#
            );
            assert!(rendered.contains(&want), "missing {want} in:\n{rendered}");
        }
        assert!(rendered.contains("spate_datagen_ticks_total"), "{rendered}");
        assert!(
            rendered.contains("spate_datagen_tick_overrun_total"),
            "{rendered}"
        );
    }

    #[test]
    fn the_control_plane_gauges_publish_what_it_was_given() {
        let rendered = render(|| {
            DatagenMetrics::new(&meter("gauges"), 2, false).publish(97, 12);
        });
        assert!(
            rendered.contains("spate_datagen_events_remaining{") && rendered.contains("} 97"),
            "{rendered}"
        );
        assert!(
            rendered.contains("spate_datagen_open_orders{") && rendered.contains("} 12"),
            "{rendered}"
        );
        // The one series this source refuses to publish: an unbounded
        // generator's lag is infinite, so it must not appear at all.
        assert!(!rendered.contains("lag"), "{rendered}");
    }

    /// The cardinality-sensitive family is the only one behind the flag, and
    /// with the flag off it must not register at all — a series that exists
    /// but never moves is worse than one that is absent.
    #[test]
    fn committed_offset_is_gated_on_per_partition_detail() {
        let off =
            render(|| DatagenMetrics::new(&meter("detail-off"), 2, false).set_committed(1, 9));
        assert!(!off.contains("committed_offset"), "{off}");

        let on = render(|| {
            let metrics = DatagenMetrics::new(&meter("detail-on"), 2, true);
            metrics.set_committed(0, 5);
            metrics.set_committed(1, 9);
            // Out of range: a partition this source never assigned.
            metrics.set_committed(7, 11);
        });
        for (partition, offset) in [(0, 5), (1, 9)] {
            let want = format!(
                r#"spate_datagen_committed_offset{{pipeline="orders",component="detail-on",component_type="datagen",partition="{partition}"}} {offset}"#
            );
            assert!(on.contains(&want), "missing {want} in:\n{on}");
        }
        assert!(!on.contains(r#"partition="7""#), "{on}");
    }
}
