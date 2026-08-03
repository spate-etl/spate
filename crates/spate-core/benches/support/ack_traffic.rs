//! The checkpoint bench rig: a fixed corpus of source poll batches, issued
//! and resolved through the public acknowledgement path and drained into
//! per-partition watermarks a commit tick at a time.
//!
//! Everything here goes through `spate_core::checkpoint`'s public API —
//! [`AckIssuer::issue`], `AckRef`'s drop, [`Checkpointer::drain`] and
//! [`Checkpointer::take_watermarks`] — because that is the whole of what a
//! pipeline thread and the runtime touch. The module is synchronous and
//! tokio-free by invariant, which is exactly what makes it countable: there
//! is no executor between the call and the work.
//!
//! Nothing else compiles this file — the checkpointer has no wall-clock
//! sibling — so the `#[path]` include has exactly one consumer today.

use spate_core::checkpoint::{AckIssuer, AckRef, Checkpointer};
use spate_core::record::PartitionId;

/// Source poll batches one case issues and resolves in total.
///
/// The corpus is the batches, not the number of times one batch is replayed:
/// every batch here carries its own partition, sequence and offset, and the
/// tracker's state after it is different from the state before it.
pub(crate) const BATCHES: usize = 8192;

/// Partitions the epoch covers, and so the number of trackers a drain
/// dispatches across and a commit tick sweeps.
///
/// Small on purpose. The per-batch cost is a hash lookup either way, and a
/// wide assignment would put the count under the `HashMap` sizing rather than
/// under the tracker; four is a plausible per-thread lane count and keeps the
/// map in one bucket group.
pub(crate) const PARTITIONS: usize = 4;

/// Source offsets one batch covers. Only the arithmetic depends on it — the
/// tracker stores the last offset and reports `last_offset + 1` — but a
/// realistic stride keeps the watermarks strictly increasing per partition,
/// as a real source's would be.
const OFFSETS_PER_BATCH: i64 = 100;

/// How a tick's resolutions are ordered relative to the order the tick
/// issued them.
#[derive(Clone, Copy)]
pub(crate) enum Order {
    /// Resolved in issue order, which is what a pipeline whose sinks
    /// complete in flush order produces.
    Issued,
    /// Resolved in a fixed scrambled permutation — the same shape the
    /// `watermark_is_monotonic` unit test uses, `(step * 37) % n`, chosen
    /// there and reused here rather than invented. 37 is coprime with every
    /// tick size this rig uses, so the walk is a permutation and every batch
    /// resolves exactly once.
    Scrambled,
}

impl Order {
    /// The index, within a tick of `n` batches, that resolves at `step`.
    fn at(self, step: usize, n: usize) -> usize {
        match self {
            Order::Issued => step,
            Order::Scrambled => (step * 37) % n,
        }
    }
}

/// A checkpointer mid-epoch, its issuer, and the shape of the traffic one
/// drive puts through them.
pub(crate) struct Rig {
    checkpointer: Checkpointer,
    issuer: AckIssuer,
    partitions: Vec<PartitionId>,
    /// Batches one commit tick issues and resolves before draining.
    per_tick: usize,
    order: Order,
    /// Live acknowledgement handles for the tick under way. Held so the tick
    /// can resolve them in an order of its choosing; reused across ticks so
    /// the drive does not measure a `Vec` growing.
    live: Vec<Option<AckRef>>,
    /// Next offset to hand out, per partition. Keeps each partition's
    /// watermark strictly increasing across the whole drive.
    next_offset: Vec<i64>,
    /// Watermark pairs the drive must report — one per partition per tick,
    /// since every batch a tick issues also resolves inside it. Asserted
    /// rather than returned unchecked, so a fixture that silently stopped
    /// advancing a partition could not pass as a fast one.
    pub(crate) expect_watermarks: usize,
}

impl Rig {
    /// One drive: [`BATCHES`] source batches issued, resolved and committed,
    /// [`Rig::per_tick`] of them per commit tick. Returns the number of
    /// watermark pairs the ticks produced so a caller can keep the work
    /// observable.
    ///
    /// The shape mirrors the runtime's: a pipeline thread issues an
    /// [`AckRef`] per poll batch and drops it when the batch's records have
    /// all resolved, and the controller thread drains and takes watermarks on
    /// its commit interval. Both channels are unbounded, so nothing here can
    /// block — the acknowledgement path never waits on data, by invariant.
    pub(crate) fn drive(&mut self) -> usize {
        let mut watermarks = 0;
        for tick in 0..BATCHES / self.per_tick {
            for i in 0..self.per_tick {
                let p = (tick * self.per_tick + i) % PARTITIONS;
                self.next_offset[p] += OFFSETS_PER_BATCH;
                self.live.push(Some(
                    self.issuer.issue(self.partitions[p], self.next_offset[p]),
                ));
            }
            for step in 0..self.per_tick {
                // Dropping the last handle of a batch is what resolves it, so
                // the slot order here is the order resolutions reach the
                // checkpointer's channel.
                self.live[self.order.at(step, self.per_tick)] = None;
            }
            self.live.clear();
            let _ = self.checkpointer.drain();
            watermarks += self.checkpointer.take_watermarks().len();
        }
        watermarks
    }

    /// Batches still unadvanced across every partition. Zero after a drive:
    /// every batch a tick issues is resolved inside that tick, so each
    /// tracker's ring empties before the next one starts.
    pub(crate) fn pending(&self) -> usize {
        self.checkpointer.max_pending()
    }
}

/// A rig committing `per_tick` batches per tick, resolved in `order`.
///
/// # Panics
///
/// Panics unless `per_tick` divides [`BATCHES`] and is coprime with the
/// scramble's stride — a tick that did not divide the corpus would leave a
/// ragged last tick, and a stride sharing a factor with the tick size would
/// resolve some batches twice and others never.
pub(crate) fn rig(per_tick: usize, order: Order) -> Rig {
    assert!(
        BATCHES.is_multiple_of(per_tick),
        "{per_tick} batches per tick do not divide the {BATCHES}-batch corpus"
    );
    assert!(
        !per_tick.is_multiple_of(37),
        "a tick size divisible by the scramble stride is not a permutation"
    );
    assert!(
        per_tick.is_multiple_of(PARTITIONS),
        "a tick that does not divide evenly across {PARTITIONS} partitions \
         leaves some of them without a batch, and so without a watermark"
    );
    let partitions: Vec<_> = (0..PARTITIONS).map(|p| PartitionId(p as u32)).collect();
    let mut checkpointer = Checkpointer::new();
    checkpointer.begin_epoch(&partitions, 1);
    let issuer = checkpointer.handle();
    Rig {
        checkpointer,
        issuer,
        partitions,
        per_tick,
        order,
        live: Vec::with_capacity(per_tick),
        next_offset: vec![0; PARTITIONS],
        expect_watermarks: (BATCHES / per_tick) * PARTITIONS,
    }
}
