//! A live-heap high-water allocator and a pipeline that feeds it zero-width
//! item claims, shared by the items tests of the value and serde paths. Each
//! of those is a binary of its own, because the allocator is process-wide.

use spate_avro::AvroDeserializerBuilder;
use spate_core::config::PipelineConfig;
use spate_core::deser::{Deserializer, Owned};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{HANG, PipelineRun, TestEncoder, capture_sink, decode_rows, memory_source};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Live heap bytes, and the most there have been since the last [`reset`].
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct HighWater;

// SAFETY: each method forwards to `System` with the arguments it was given;
// the counters are atomics, which allocate nothing.
unsafe impl GlobalAlloc for HighWater {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            let live = LIVE.fetch_add(new_size, Ordering::Relaxed) + new_size;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        new
    }
}

/// Start a new high-water window at the current live bytes.
fn reset() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

/// An array of arrays of a record with no fields: each item costs no bytes.
pub(crate) const GROUPS: &str = r#"{"type":"record","name":"Batch","fields":[{"name":"groups","type":{"type":"array","items":{"type":"array","items":{"type":"record","name":"Tick","fields":[]}}}}]}"#;

/// Avro's zigzag varint encoding of a long.
fn zig(n: i64) -> Vec<u8> {
    let mut z = ((n << 1) ^ (n >> 63)).cast_unsigned();
    let mut out = Vec::new();
    while z > 0x7F {
        out.push(0x80 | (z & 0x7F) as u8);
        z >>= 7;
    }
    out.push(z as u8);
    out
}

/// A datum of `groups` inner arrays, each one block claiming `ticks` ticks: a
/// few bytes per group.
fn batch(groups: i64, ticks: i64) -> Vec<u8> {
    let mut datum = zig(groups);
    for _ in 0..groups {
        datum.extend(zig(ticks));
        datum.push(0x00);
    }
    datum.push(0x00);
    datum
}

/// The per-datum item budget's floor: a short datum may claim this many
/// items in all, here one group and its ticks.
pub(crate) const ADMITTED_TICKS: i64 = (1 << 16) - 1;

/// What one items run measured.
pub(crate) struct ItemsRun {
    /// Peak heap growth while 1 group claiming 2^20 ticks went through.
    pub(crate) one: usize,
    /// The same for 10 such groups.
    pub(crate) ten: usize,
    /// The same for one group of [`ADMITTED_TICKS`] ticks.
    pub(crate) admitted: usize,
    /// The tick count of each record the sink was written.
    pub(crate) delivered: Vec<u64>,
}

/// Push the two claims and the admitted datum through a pipeline built on
/// `build`'s deserializer, whose chain maps each record to its tick count.
pub(crate) fn run<T, D>(
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
    ticks: fn(&T) -> u64,
) -> ItemsRun
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Clone + Send + 'static,
{
    let yaml = format!(
        "pipeline: {{ name: value-items, threads: 1, io_threads: 1 }}\n\
         admin: {{ listen: none }}\n\
         metrics: {{ exporter: none }}\n\
         checkpoint: {{ interval: 100ms }}\n\
         source: {{ memory: {{}} }}\n\
         deserializer:\n\
         \x20 avro:\n\
         \x20   mode: raw\n\
         \x20   schema:\n\
         \x20     inline: '{GROUPS}'\n\
         sink: {{ capture: {{}} }}\n"
    );
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml).unwrap()).unwrap();
    let section = pipeline.config().deserializer.clone().unwrap();
    let deser = build(
        &AvroDeserializerBuilder::from_component_config(&section, &pipeline.io_handle()).unwrap(),
    );
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let runtime = pipeline
        .sink(sink)
        .unwrap()
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<T, _>(deser.clone())
                .map(move |rec: T| ticks(&rec).to_le_bytes().to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .unwrap();
    let shutdown = runtime.shutdown_handle();
    let running = PipelineRun::spawn(move || runtime.run());
    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);

    let mut peaks = Vec::new();
    for (groups, ticks) in [(1, 1 << 20), (10, 1 << 20), (1, ADMITTED_TICKS)] {
        let base = reset();
        let at = handle.push(p, None, &batch(groups, ticks));
        assert!(
            handle.wait_committed(p, at + 1, HANG),
            "committed past {groups} groups of {ticks}"
        );
        peaks.push(PEAK.load(Ordering::Relaxed) - base);
    }
    shutdown.trigger();
    running
        .wait_exit(HANG)
        .expect("run returns")
        .expect("clean exit");
    let delivered = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .map(|row| u64::from_le_bytes(row.try_into().unwrap()))
        .collect();
    let [one, ten, admitted] = peaks[..] else {
        unreachable!()
    };
    ItemsRun {
        one,
        ten,
        admitted,
        delivered,
    }
}

/// The claims are refused without growing the heap with them, and the datum
/// at the budget is delivered within a bounded growth.
pub(crate) fn assert_bounded(run: &ItemsRun) {
    let ItemsRun {
        one,
        ten,
        admitted,
        delivered,
    } = run;
    assert!(
        *ten <= one + (1 << 20),
        "peak heap growth {one} bytes for 1 group, {ten} bytes for 10: it grows with the claim"
    );
    assert_eq!(
        delivered,
        &[ADMITTED_TICKS.cast_unsigned()],
        "only the datum within the budget is delivered"
    );
    assert!(
        *admitted < 8 << 20,
        "peak heap growth {admitted} bytes for {ADMITTED_TICKS} ticks"
    );
}
