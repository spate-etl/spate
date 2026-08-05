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
//! Two drivers over one schedule. [`rig`] issues and resolves on the calling
//! thread, and is what the counted tier measures. [`threaded`] spreads the
//! same schedule across a pinned number of worker threads, and is what the
//! wall tier measures: what a thread count moves is contention on the two
//! unbounded channels rather than arithmetic, and callgrind serialises
//! threads, so the counted tier cannot see it at all.
//!
//! Included with `#[path]` by `checkpoint_gungraun.rs`, `control_plane_wall.rs`
//! and `tests/bench_fixtures.rs`. A bench target is its own crate, so several
//! targets can only agree on a workload by compiling the same source.

#![allow(dead_code, reason = "each target uses a different subset")]

use spate_core::checkpoint::{AckIssuer, AckRef, Checkpointer};
use spate_core::record::PartitionId;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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
///
/// Held *fixed* across the thread axis rather than widened per thread. The
/// partition count enters this path only as a `HashMap` key, so widening it
/// alongside the thread count would put two things in one axis and leave a
/// contention reading standing for a hashing one. Every thread count
/// [`threaded`] accepts divides it instead.
pub(crate) const PARTITIONS: usize = 4;

/// Source offsets one batch covers. Only the arithmetic depends on it — the
/// tracker stores the last offset and reports `last_offset + 1` — but a
/// realistic stride keeps the watermarks strictly increasing per partition
/// within a drive, as a real source's would be.
const OFFSETS_PER_BATCH: i64 = 100;

/// Bytes one [`Entry`] contributes to the corpus digest: a `u32` and an `i64`.
const ENTRY_BYTES: usize = 4 + 8;

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
    /// window size this rig accepts, so the walk is a permutation and every
    /// batch resolves exactly once.
    Scrambled,
}

impl Order {
    /// The index, within a resolution window of `n` batches, that resolves at
    /// `step`.
    fn at(self, step: usize, n: usize) -> usize {
        match self {
            Order::Issued => step,
            Order::Scrambled => (step * 37) % n,
        }
    }
}

/// One source poll batch: the partition it belongs to, and the highest offset
/// it covers.
#[derive(Clone, Copy)]
pub(crate) struct Entry {
    partition: u32,
    last_offset: i64,
}

/// Every batch a drive issues, in issue order.
///
/// Materialised here rather than computed inside a drive, for two reasons —
/// the second is what earns the 96 KiB. A drive then walks a slice instead of
/// carrying the rig's own modular arithmetic, so neither tier charges the
/// checkpointer for work the fixture does; and the schedule becomes *bytes*,
/// which is what the wall harness folds into its corpus digest to prove two
/// legs drove the same batches. A rig whose corpus cannot be handed over
/// compares equal to one that changed.
///
/// A pure function of nothing but the constants above — no RNG, no clock, no
/// hash iteration order — so two checkouts straddling a dependency bump build
/// it byte for byte alike, and the harness's seed argument is genuinely
/// unused. `tests/bench_fixtures.rs` pins the result.
///
/// Independent of the tick width: a batch's partition is its global index
/// modulo [`PARTITIONS`], and every tick this rig accepts covers a whole
/// number of partitions, so regrouping the same batches into wider or
/// narrower ticks does not move any of them. The tick width is a property of
/// the driver, not of the corpus, and `ack_wide_*` and `ack_narrow_*`
/// therefore share a digest.
fn schedule() -> Vec<Entry> {
    let mut next_offset = [0i64; PARTITIONS];
    (0..BATCHES)
        .map(|i| {
            let partition = i % PARTITIONS;
            next_offset[partition] += OFFSETS_PER_BATCH;
            Entry {
                partition: partition as u32,
                last_offset: next_offset[partition],
            }
        })
        .collect()
}

/// The schedule as bytes, for the wall harness's corpus digest.
fn schedule_bytes(schedule: &[Entry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(schedule.len() * ENTRY_BYTES);
    for entry in schedule {
        out.extend_from_slice(&entry.partition.to_le_bytes());
        out.extend_from_slice(&entry.last_offset.to_le_bytes());
    }
    out
}

/// The shape constraints every driver shares, where `window` is the number of
/// batches one resolution order runs over.
///
/// Checked in the builders, which every caller runs outside its measured
/// region — gungraun evaluates a `#[bench]` argument expression before it
/// starts collecting, and the wall harness builds the rig in a case's
/// `setup`. A drive over a ragged corpus therefore fails loudly instead of
/// quietly reporting a number for work that is not happening.
fn assert_shape(per_tick: usize, window: usize) {
    assert!(
        BATCHES.is_multiple_of(per_tick),
        "{per_tick} batches per tick do not divide the {BATCHES}-batch corpus"
    );
    assert!(
        per_tick.is_multiple_of(PARTITIONS),
        "a tick that does not divide evenly across {PARTITIONS} partitions \
         leaves some of them without a batch, and so without a watermark"
    );
    // Cannot fire as the constants stand, and is kept for the edit that would
    // change that rather than as a live guard. `BATCHES` is a power of two and
    // `per_tick` must divide it, `threads` must divide `PARTITIONS`, so every
    // reachable window is a power of two and 37 divides none of them. Move
    // `BATCHES` off a power of two and this becomes the only thing standing
    // between a scramble and a walk that resolves some batches twice and
    // others never — with no evidence, at that point, that it ever worked.
    // `tests/bench_fixtures.rs` is where that evidence lives instead.
    assert!(
        !window.is_multiple_of(37),
        "a resolution window of {window} is divisible by the scramble stride, \
         so the walk is not a permutation: some batches would resolve twice \
         and others never"
    );
}

/// Watermark pairs a drive must report: one per partition per commit tick,
/// since every batch a tick issues also resolves inside it.
fn expected_watermarks(per_tick: usize) -> usize {
    (BATCHES / per_tick) * PARTITIONS
}

// ---------------------------------------------------------------------------
// The single-threaded driver
// ---------------------------------------------------------------------------

/// A checkpointer mid-epoch, its issuer, and the shape of the traffic one
/// drive puts through them.
pub(crate) struct Rig {
    checkpointer: Checkpointer,
    issuer: AckIssuer,
    partitions: Vec<PartitionId>,
    schedule: Vec<Entry>,
    /// Batches one commit tick issues and resolves before draining.
    per_tick: usize,
    order: Order,
    /// Live acknowledgement handles for the tick under way. Held so the tick
    /// can resolve them in an order of its choosing; reused across ticks so
    /// the drive does not measure a `Vec` growing.
    live: Vec<Option<AckRef>>,
    /// Watermark pairs the drive must report. Asserted rather than returned
    /// unchecked, so a fixture that silently stopped advancing a partition
    /// could not pass as a fast one.
    pub(crate) expect_watermarks: usize,
}

impl Rig {
    /// The bytes this rig drives, for a caller that has to prove two builds
    /// measured the same ones — the wall tier folds these into its corpus
    /// digest, which is what demotes a pair of legs whose corpora drifted.
    pub(crate) fn corpus(&self) -> Vec<u8> {
        schedule_bytes(&self.schedule)
    }

    /// One drive: [`BATCHES`] source batches issued, resolved and committed,
    /// `per_tick` of them per commit tick. Returns the number of watermark
    /// pairs the ticks produced, so a caller can keep the work observable.
    ///
    /// The shape mirrors the runtime's: a pipeline thread issues an
    /// [`AckRef`] per poll batch and drops it when the batch's records have
    /// all resolved, and the controller thread drains and takes watermarks on
    /// its commit interval. Both channels are unbounded, so nothing here can
    /// block — the acknowledgement path never waits on data, by invariant.
    ///
    /// **Repeatable**, which the wall tier requires and the counted tier does
    /// not: every batch a tick issues resolves inside that tick, so each
    /// tracker's ring empties before the next tick starts and the hundredth
    /// drive is the same work as the first. Offsets repeat across drives
    /// rather than climbing — `PartitionTracker::advance` reports
    /// `last_offset + 1` off the delivered prefix without comparing it to
    /// anything, so a repeated offset costs the same pops and the same
    /// comparisons as a fresh one, and a schedule that is a pure function of
    /// its constants is worth more here than the realism of an ever-climbing
    /// offset.
    pub(crate) fn drive(&mut self) -> usize {
        let mut watermarks = 0;
        for entries in self.schedule.chunks(self.per_tick) {
            for entry in entries {
                self.live.push(Some(
                    self.issuer
                        .issue(self.partitions[entry.partition as usize], entry.last_offset),
                ));
            }
            for step in 0..entries.len() {
                // Dropping the last handle of a batch is what resolves it, so
                // the slot order here is the order resolutions reach the
                // checkpointer's channel.
                self.live[self.order.at(step, entries.len())] = None;
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
/// See [`assert_shape`].
pub(crate) fn rig(per_tick: usize, order: Order) -> Rig {
    assert_shape(per_tick, per_tick);
    let partitions: Vec<_> = (0..PARTITIONS).map(|p| PartitionId(p as u32)).collect();
    let mut checkpointer = Checkpointer::new();
    checkpointer.begin_epoch(&partitions, 1);
    let issuer = checkpointer.handle();
    Rig {
        checkpointer,
        issuer,
        partitions,
        schedule: schedule(),
        per_tick,
        order,
        live: Vec::with_capacity(per_tick),
        expect_watermarks: expected_watermarks(per_tick),
    }
}

// ---------------------------------------------------------------------------
// The multi-threaded driver
// ---------------------------------------------------------------------------

/// How many times a party spins before falling back to `yield_now`.
///
/// The waits here are a worker's share of a tick and the controller's drain —
/// microseconds, with every party runnable throughout. Spinning that out
/// costs far less than a kernel wake, and there are two waits per commit
/// tick, so a parking primitive would be the same order of magnitude as the
/// work being measured. `yield_now` is the escape hatch for a party that has
/// been preempted, not the steady state.
///
/// Kept modest rather than tuned: these cases do not spend their time here,
/// and a machine with fewer cores than parties should reach the fallback
/// promptly instead of burning a scheduling quantum first.
const SPIN_LIMIT: u32 = 1024;

/// How long a party waits before deciding the other side is never coming.
///
/// A bench process that hangs is the worst outcome this rig has: it produces
/// no record, so the leg has a missing replicate and the whole comparison
/// aborts — but only after somebody notices. [`ArriveOnUnwind`] handles the
/// realistic cause, a worker panicking mid-tick, and reports it by name; this
/// is the backstop for everything else.
const GATE_DEADLINE: Duration = Duration::from_secs(30);

/// Bytes of padding that keep two atomics off one cache line.
///
/// The two counters below are written by opposite sides on every tick. Shared
/// they would ping one line between the controller and every worker; apart
/// they cost one line each. 128 rather than 64 because Apple Silicon pairs
/// lines for some coherence traffic.
#[repr(align(128))]
struct Padded<T>(T);

/// Released generation meaning "stop", rather than "run tick N".
const SHUTDOWN: usize = usize::MAX;

/// The controller/worker rendezvous for one commit tick.
///
/// Deliberately *not* a barrier. Workers never wait on each other — only the
/// controller waits for workers, and only workers wait for the controller —
/// so a tick costs one plain store to release the workers plus one
/// read-modify-write per worker to report, instead of two full symmetric
/// crossings. Both counters are monotonic for the life of the rig and are
/// never reset, so the generation a party is waiting for is unambiguous and
/// there is no reuse hazard to guard against.
struct Gate {
    /// Controller to workers: generations `0..release` have been released.
    release: Padded<AtomicUsize>,
    /// Workers to controller: total worker-ticks completed, ever.
    arrived: Padded<AtomicUsize>,
    /// Set by a worker unwinding out of a tick. Without it a dead worker is
    /// indistinguishable from a slow one until the deadline expires, and the
    /// deadline's message describes the symptom rather than the cause.
    poisoned: AtomicBool,
    threads: usize,
}

impl Gate {
    fn new(threads: usize) -> Self {
        Gate {
            release: Padded(AtomicUsize::new(0)),
            arrived: Padded(AtomicUsize::new(0)),
            poisoned: AtomicBool::new(false),
            threads,
        }
    }

    /// Spin until `ready` says the other side has caught up, or give up.
    fn spin_until(&self, what: &str, mut ready: impl FnMut() -> bool) {
        let mut spins = 0u32;
        let start = Instant::now();
        while !ready() {
            if spins < SPIN_LIMIT {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
                assert!(
                    start.elapsed() < GATE_DEADLINE,
                    "the ack rig's {what} did not complete within {GATE_DEADLINE:?}; \
                     a worker has died or the gate's accounting is wrong"
                );
            }
        }
    }

    /// Controller: release generation `generation`, then wait for every
    /// worker to report it done.
    ///
    /// # Panics
    ///
    /// Panics as soon as a worker reports it has unwound, naming that rather
    /// than waiting out the deadline — a worker that dies on the first of a
    /// drive's ticks would otherwise leave the controller short by one more
    /// arrival every tick, and the run would end thirty seconds later
    /// complaining about a timeout.
    fn run_tick(&self, generation: usize) {
        self.release.0.store(generation + 1, Ordering::Release);
        let target = (generation + 1) * self.threads;
        self.spin_until("wait for its workers", || {
            assert!(
                !self.poisoned.load(Ordering::Acquire),
                "an acking thread panicked; its own message is above this one"
            );
            self.arrived.0.load(Ordering::Acquire) >= target
        });
    }

    /// Worker: wait to be released into generation `generation`. Returns
    /// false when the rig is shutting down.
    fn await_tick(&self, generation: usize) -> bool {
        let mut shutdown = false;
        self.spin_until("release a worker", || {
            let released = self.release.0.load(Ordering::Acquire);
            shutdown = released == SHUTDOWN;
            shutdown || released > generation
        });
        !shutdown
    }
}

/// Reports a worker's tick as done however the tick ends, and says so when it
/// ended badly.
///
/// A worker that panics mid-tick never increments `arrived` on its own, and
/// every later tick leaves the controller short by one more — so the run ends
/// at the deadline, reporting a timeout, which describes the symptom and not
/// the cause. Reporting from a destructor closes the current tick; setting
/// the poison flag is what makes the *next* one fail immediately and say
/// which thing went wrong. The two together are why a dead worker surfaces
/// promptly rather than thirty seconds later.
struct ArriveOnUnwind<'a>(&'a Gate);

impl Drop for ArriveOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.poisoned.store(true, Ordering::Release);
        }
        self.0.arrived.0.fetch_add(1, Ordering::Release);
    }
}

/// What one worker thread needs to run its share of every tick.
struct Worker {
    issuer: AckIssuer,
    /// The partitions this worker owns, as `PartitionId`s. Disjoint from
    /// every other worker's: `AckIssuer` numbers sequences per issuer, and
    /// `PartitionTracker::register` panics on a sequence gap, so a partition
    /// issued from two issuers inside one epoch is a crash rather than a
    /// slow path.
    partitions: Vec<PartitionId>,
    /// The global partition index this worker's run starts at, so a
    /// schedule entry's global partition maps onto `partitions` by
    /// subtraction rather than by a modulo that only happens to work.
    first_partition: usize,
    /// This worker's batches within one tick, as indices into the tick.
    /// Precomputed, so the measured region does not carry the stride
    /// arithmetic that selects them.
    slots: Vec<usize>,
}

/// A checkpointer driven by a pinned number of acking threads.
///
/// The main thread owns the [`Checkpointer`] — `drain` and `take_watermarks`
/// take `&mut self`, and the runtime owns it the same way — while each worker
/// owns a cloned [`AckIssuer`] and a disjoint slice of the partitions. One
/// commit tick is two barrier crossings: the first releases the workers, the
/// second waits for them, and the main thread drains between the second and
/// the next tick's first while the workers are already blocked.
///
/// *A simplification worth knowing.* Each worker drops its own handles, so a
/// batch is resolved by the thread that issued it. In production a sink
/// worker drops handles a source thread issued, and resolution is
/// cross-thread. What this rig reproduces is the part the thread count moves
/// — several producers on one unbounded MPSC sender — not the handoff.
pub(crate) struct Threaded {
    checkpointer: Checkpointer,
    workers: Vec<JoinHandle<()>>,
    gate: Arc<Gate>,
    schedule: Vec<Entry>,
    per_tick: usize,
    /// Generations released so far, kept across drives so the controller and
    /// the workers stay in step without either resetting a counter.
    generation: usize,
    /// Watermark pairs the drive must report, asserted for the same reason
    /// [`Rig::expect_watermarks`] is.
    pub(crate) expect_watermarks: usize,
}

impl Threaded {
    /// The bytes this rig drives — the same schedule the single-threaded
    /// driver walks, so a wall case and its counted sibling fold the same
    /// corpus.
    pub(crate) fn corpus(&self) -> Vec<u8> {
        schedule_bytes(&self.schedule)
    }

    /// One drive: the whole schedule, `per_tick` batches per commit tick,
    /// with each tick's batches issued and resolved across the workers.
    /// Returns the watermark-pair count.
    ///
    /// Repeatable for the same reason [`Rig::drive`] is, and deterministic in
    /// the work it performs: the tick count, the batches per worker per tick,
    /// the drain count and the watermark count are all fixed by the schedule.
    /// What the threads decide is the interleaving of the sends, which is the
    /// thing being measured.
    ///
    /// The gate is also what keeps `drain` on one code path. Every worker has
    /// reported before the controller drains, so a registration is always
    /// already in its channel when the matching resolution is applied and
    /// `drain`'s deferred-retry arm never fires. A controller draining
    /// concurrently with issuing threads would take that arm at a rate the
    /// machine decides, which is a second code path inside the measured
    /// region appearing and disappearing between replicates.
    pub(crate) fn drive(&mut self) -> usize {
        let mut watermarks = 0;
        for _ in 0..BATCHES / self.per_tick {
            self.gate.run_tick(self.generation);
            self.generation += 1;
            let _ = self.checkpointer.drain();
            watermarks += self.checkpointer.take_watermarks().len();
        }
        watermarks
    }

    /// Batches still unadvanced across every partition. Zero after a drive.
    pub(crate) fn pending(&self) -> usize {
        self.checkpointer.max_pending()
    }
}

impl Drop for Threaded {
    fn drop(&mut self) {
        // Workers are waiting to be released into the next generation, so
        // publishing the sentinel is the whole of the protocol. Joining here
        // rather than leaving it to field order is what guarantees no worker
        // can still be issuing into a checkpointer whose channels have been
        // dropped — and it runs on an unwind too, so a failed assertion
        // inside a measured region tears the rig down rather than leaking
        // four threads per replicate.
        self.gate.release.0.store(SHUTDOWN, Ordering::Release);
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

/// A rig committing `per_tick` batches per tick across `threads` acking
/// threads, each resolving its own share in `order`.
///
/// `threads` must divide [`PARTITIONS`], because a partition belongs to
/// exactly one issuer within an epoch. The resolution window is therefore
/// `per_tick / threads` — one worker's share of a tick — and that is what the
/// scramble stride has to be coprime with.
///
/// # Panics
///
/// See [`assert_shape`], plus the divisibility of [`PARTITIONS`] by `threads`.
pub(crate) fn threaded(per_tick: usize, order: Order, threads: usize) -> Threaded {
    assert!(threads > 0, "a rig needs at least one acking thread");
    assert!(
        PARTITIONS.is_multiple_of(threads),
        "{threads} threads do not divide the {PARTITIONS} partitions evenly, \
         so some thread would own none and the tick would be ragged"
    );
    assert_shape(per_tick, per_tick / threads);

    let partitions: Vec<_> = (0..PARTITIONS).map(|p| PartitionId(p as u32)).collect();
    let mut checkpointer = Checkpointer::new();
    checkpointer.begin_epoch(&partitions, 1);

    let schedule = schedule();
    // Shared rather than cloned per worker: the schedule is read-only for the
    // whole life of the rig, so every worker reading the same pages costs
    // nothing beyond filling them, and four copies would be four times the
    // cache footprint for no reason.
    let shared = Arc::new(schedule.clone());
    let per_thread = PARTITIONS / threads;
    let gate = Arc::new(Gate::new(threads));

    let workers = (0..threads)
        .map(|k| {
            let owned: Vec<_> = partitions[k * per_thread..(k + 1) * per_thread].to_vec();
            // A tick's batch `j` carries partition `j % PARTITIONS`: the tick
            // width is a multiple of `PARTITIONS`, so every tick starts on a
            // partition boundary and the pattern repeats within it.
            let slots: Vec<usize> = (0..per_tick)
                .filter(|j| {
                    let p = j % PARTITIONS;
                    p >= k * per_thread && p < (k + 1) * per_thread
                })
                .collect();
            // Every batch this worker will touch, across every tick, falls in
            // the partition run it owns. The worker maps a global partition
            // onto its own list by subtracting `first_partition`, which
            // underflows on a thread if the schedule's layout and the slot
            // filter ever stop agreeing — and an underflow there surfaces as
            // the gate's poison, several frames from the cause. Checked once
            // in the builder, outside every measured region, so a shape change
            // fails here instead.
            let first_partition = k * per_thread;
            for tick in 0..BATCHES / per_tick {
                for &slot in &slots {
                    let partition = schedule[tick * per_tick + slot].partition as usize;
                    assert!(
                        partition >= first_partition && partition < first_partition + per_thread,
                        "thread {k} owns partitions {first_partition}..{} but its slot {slot} \
                         in tick {tick} carries partition {partition}",
                        first_partition + per_thread
                    );
                }
            }

            let worker = Worker {
                issuer: checkpointer.handle(),
                partitions: owned,
                first_partition,
                slots,
            };
            spawn_worker(
                worker,
                Arc::clone(&gate),
                Arc::clone(&shared),
                per_tick,
                order,
            )
        })
        .collect();

    Threaded {
        checkpointer,
        workers,
        gate,
        schedule,
        per_tick,
        generation: 0,
        expect_watermarks: expected_watermarks(per_tick),
    }
}

/// One worker's loop: wait to be released into a tick, issue and resolve its
/// share, report, repeat.
fn spawn_worker(
    worker: Worker,
    gate: Arc<Gate>,
    schedule: Arc<Vec<Entry>>,
    per_tick: usize,
    order: Order,
) -> JoinHandle<()> {
    let Worker {
        mut issuer,
        partitions,
        first_partition,
        slots,
    } = worker;
    std::thread::spawn(move || {
        let window = slots.len();
        // Sized once, here, and only ever cleared: a drive must not measure a
        // `Vec` growing, and this is outside every measured region because
        // the rig is built in a case's setup.
        let mut live: Vec<Option<AckRef>> = Vec::with_capacity(window);
        let ticks = BATCHES / per_tick;
        let mut generation = 0usize;
        while gate.await_tick(generation) {
            // Reports this tick done however it ends, so a panic in the body
            // below surfaces as a wrong watermark count rather than a hang.
            let _arrive = ArriveOnUnwind(&gate);
            // Wrapping rather than counting drives is what makes the rig
            // repeatable: a drive is exactly `ticks` generations, so the next
            // drive starts back at the schedule's first tick.
            let base = (generation % ticks) * per_tick;
            for &slot in &slots {
                let entry = schedule[base + slot];
                // A schedule entry names a *global* partition; this worker
                // owns a contiguous run of them, so its own index is the
                // offset from where that run starts.
                let local = entry.partition as usize - first_partition;
                live.push(Some(issuer.issue(partitions[local], entry.last_offset)));
            }
            for step in 0..window {
                // Dropping the last handle of a batch is what resolves it.
                live[order.at(step, window)] = None;
            }
            live.clear();
            generation += 1;
        }
    })
}
