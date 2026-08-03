//! Peak resident set size, read from the kernel's own accounting.
//!
//! The instruction-count tier already measures heap shape — allocated blocks
//! and the t-gmax peak — under DHAT, deterministically, on the pull requests
//! that select a benched crate — a change confined to `benchmarks/` selects
//! none, so this number sits behind no gate at all. It is not that measure and
//! does not replace it.
//! DHAT measures a micro-benchmark calling one function; a pipeline's memory is
//! decided by things no micro-benchmark contains: the in-flight budget, queue
//! depth, how many chunks are sealed but unacknowledged, and how much the
//! allocator is holding rather than returning. Those move with a configuration
//! change and leave every instruction count where it was.
//!
//! What this reports is the high-water mark of the process's resident set —
//! what the machine actually had to find. That makes it a blunter number than
//! DHAT's and a more operational one: it is the quantity a container limit
//! bounds. Note the units differ when comparing the two — this is SI megabytes,
//! matching the rest of the record schema, while `memory: 256Mi` and
//! `--memory=256m` are binary, so a limit set from this figure at face value
//! sits 4.9% higher than intended.
//!
//! # Which rigs report it
//!
//! The three whose measured region is the whole process: the object-storage
//! backfill, the synthetic pipeline, and the cross-format decode rig. The
//! container-backed rigs (`ch_sink_saturation`, `kafka_sink_saturation`,
//! `e2e_kafka_clickhouse`, `multi_table_split`, `kafka_topology`) satisfy the
//! one-arm-per-process condition and are deliberately not wired: their numbers
//! belong to a comparison suite that runs elsewhere, and adding a metric to
//! them here would be coverage nothing consumes. `s3_backfill_coordinated`
//! cannot qualify at all — it stages into a fresh directory every run, so it
//! always builds its corpus.
//!
//! # Why a watch rather than a reading
//!
//! `ru_maxrss` is monotonic for the life of the process and cannot be reset, so
//! a bare reading answers "what did this *process* peak at", which is rarely the
//! question. Two things contaminate it:
//!
//! - **Setup.** A rig that stages a corpus before measuring may peak while
//!   building it. Measured on `s3_backfill` at four objects of 400k records,
//!   the staging run reported 225 MB against 123 MB for a run that reused the
//!   same corpus — an 83% inflation with wall time unchanged, attributed to
//!   whichever build happened to stage.
//! - **Other arms.** A rig that sweeps arms in one process reports the same
//!   high-water mark for all of them, whichever arm reached it.
//!
//! [`PeakWatch`] is a **validity gate, not a subtraction**. It takes a baseline
//! when setup is done and reports the process peak only if the process rose
//! above that baseline afterwards. What that buys is narrow and worth stating
//! precisely: it proves the measured region set the mark, so the number is
//! about the region rather than about whatever preceded it. It does not remove
//! the baseline from the figure — the value reported is the process peak, and
//! anything still resident from setup is inside it.
//!
//! That distinction decides which setup is disqualifying. A decode fixture is
//! resident *while* the arm runs, so it is part of what the arm needs and
//! belongs in the number. A staged corpus is written to disk and then dropped:
//! it is not resident during the measurement, but building it grew allocator
//! arenas that are never returned, so a process that staged carries a resident
//! set unrelated to the pipeline. The gate cannot tell those apart, which is
//! why the object-storage rig additionally refuses to report from a run that
//! staged.
//!
//! The second contaminant is the caller's to handle, by measuring one arm — and
//! one repetition — per process. A max over ten repetitions is not comparable
//! with a max over one, and neither `REPS` nor its equivalents are part of a
//! record's variant identity.

use crate::report::Metric;

/// Peak resident set size of this process, in bytes, or `None` if the kernel
/// declined to say.
///
/// # The unit differs by platform, and getting it wrong is a 1024× error
///
/// `getrusage` returns `ru_maxrss` in **kilobytes on Linux** and in **bytes on
/// macOS**. Both are `long`, both are plausible-looking numbers, and a rig that
/// picked one convention would report a figure 1024× wrong on the other
/// platform while looking entirely healthy — the same class of mistake as the
/// `MB/s` divisor split `Metric::bytes_per_s` exists to prevent, where four call
/// sites across two conventions all emitted the string `MB/s`. The `cfg` below
/// is why this function exists rather than three lines inline in a rig.
#[must_use]
pub fn peak_rss_bytes() -> Option<u64> {
    // SAFETY: `rusage` is a plain struct of integer fields — no references, no
    // niches, no padding an all-zero pattern could make invalid — so zero is a
    // valid inhabitant of the type and this is a sound way to obtain one to
    // write into.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `getrusage` takes a `who` selector and a pointer it initialises
    // for a valid selector. `usage` is a live, correctly-aligned, unaliased
    // `rusage` for the whole call, and `RUSAGE_SELF` is valid. Soundness does
    // not rest on the return value: `mem::zeroed` above already left `usage`
    // fully initialised, so reading it would be defined whatever `getrusage`
    // did. The error branch discards it because a partially-written value is
    // not a measurement, not because reading it would be unsound.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) } != 0 {
        return None;
    }

    let raw = u64::try_from(usage.ru_maxrss).ok()?;

    // Enumerated rather than split on one platform with a catch-all, because a
    // catch-all here is the 1024x error this function exists to prevent, just
    // deferred to whichever platform nobody thought of. `ru_maxrss` is bytes on
    // every Darwin kernel, kibibytes on Linux, and *pages* on Solaris and
    // illumos — where a `* 1024` fallback would under-report by 4-8x while
    // compiling perfectly. An unknown platform reports nothing, which the rigs
    // already handle, rather than a number nobody has checked. `RunMeta`'s CPU
    // detection sets the same precedent: name the platforms, fall through to
    // "unknown".
    #[cfg(target_vendor = "apple")]
    {
        Some(raw)
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Kibibytes — this is the kernel's `VmHWM`, which `/proc/self/status`
        // prints as `kB` and means KiB.
        Some(raw * 1024)
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    {
        let _ = raw;
        None
    }
}

/// A gate that reports the process peak only when the measured region set it.
///
/// Construct it when setup is finished and the thing worth measuring is about
/// to begin. The figure it yields is still the *process* peak — see the module
/// docs for why it is a gate and not a subtraction.
#[derive(Debug, Clone, Copy)]
pub struct PeakWatch {
    baseline: Option<u64>,
}

impl PeakWatch {
    /// Takes the baseline. Call this once setup and any warm-up are done, so a
    /// peak reached before this point disqualifies the run rather than being
    /// reported as a measurement.
    #[must_use]
    pub fn start() -> Self {
        Self {
            baseline: peak_rss_bytes(),
        }
    }

    /// The process peak in SI megabytes, if the process rose above the baseline.
    ///
    /// The value is the peak itself, not the growth since the baseline: a
    /// high-water mark minus another high-water mark is not a quantity anything
    /// consumes. `None` means the measured region never set the mark, so the
    /// only figure available describes what came before it — and a rig reports
    /// nothing rather than that.
    #[must_use]
    pub fn peak_mb(self) -> Option<f64> {
        let baseline = self.baseline?;
        let peak = peak_rss_bytes()?;
        if peak <= baseline {
            return None;
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "an f64 is exact to 2^53 bytes; a resident set that large is \
                      not a measurement this rig could have taken"
        )]
        Some(peak as f64 / 1e6)
    }

    /// The metric a rig emits, fully formed.
    ///
    /// Built here rather than at each call site so the name, unit and direction
    /// cannot drift between rigs — the same reason `Metric::bytes_per_s` exists.
    /// `n` is 1 and there is no interval: this is one observation of a
    /// high-water mark, not a mean over repetitions, and the process cannot
    /// produce a second sample without being a second process.
    #[must_use]
    pub fn metric(self) -> Option<Metric> {
        self.peak_mb()
            .map(|mb| Metric::minimize(mb, "MB").with_n(1))
    }

    /// The metric key every rig uses for it.
    pub const KEY: &'static str = "peak_rss_mb";
}

#[cfg(test)]
mod tests {
    use super::{PeakWatch, peak_rss_bytes};

    /// One test rather than several, because they would interfere. `make test`
    /// runs nextest, which gives each test its own process — but a bare
    /// `cargo test -p benchmarks --lib` does not, and there the quantity under
    /// test is process-wide and monotonic, so a sibling that allocated more
    /// would decide this one's answer. Everything the module claims is asserted
    /// against a single ballast allocation, in order, for that reason.
    #[test]
    fn the_peak_is_a_rising_high_water_mark_in_bytes() {
        let before = peak_rss_bytes().expect("the kernel tracks a peak for every process");
        assert!(before > 0, "a running process has a non-zero resident set");

        let watch = PeakWatch::start();

        // Touched, not merely allocated: an untouched allocation may never be
        // faulted in and would not move a resident-set figure at all.
        const BALLAST: usize = 256 << 20;
        let mut ballast: Vec<u8> = vec![0; BALLAST];
        for page in ballast.chunks_mut(4096) {
            page[0] = 1;
        }
        std::hint::black_box(&ballast);

        let peak = peak_rss_bytes().expect("readable");
        let grew = peak.saturating_sub(before);

        // The unit, pinned in BOTH directions — which a plausible-range check
        // on the absolute value cannot do. A test binary's own resident set is
        // a few megabytes, so reading kilobytes as bytes (or the reverse) still
        // lands inside any range wide enough to hold both platforms. Measuring
        // the *growth* against a known allocation does not have that problem:
        // 256 MiB touched must move the mark by roughly 256 MiB, and either
        // 1024x error lands three orders of magnitude away.
        assert!(
            grew >= (BALLAST as u64) / 2,
            "touching {BALLAST} bytes moved the peak by only {grew} — the unit is \
             probably being read as larger than it is"
        );
        assert!(
            grew <= (BALLAST as u64) * 4,
            "touching {BALLAST} bytes moved the peak by {grew} — the unit is \
             probably being read as smaller than it is"
        );

        // Rising above the baseline is what makes a figure attributable.
        let mb = watch.metric().expect("the ballast rose above the baseline");
        assert!(!mb.higher_is_better);
        assert_eq!(mb.unit, "MB");
        assert_eq!(mb.n, Some(1));
        // SI, not MiB: 1e6 rather than 1 << 20.
        #[expect(clippy::cast_precision_loss, reason = "test-scale values only")]
        let expected = peak as f64 / 1e6;
        assert!(
            (mb.value - expected).abs() < expected * 0.05,
            "{} MB is not the SI reading of {peak} bytes",
            mb.value
        );

        drop(ballast);
        assert!(
            peak_rss_bytes().expect("readable") >= peak,
            "the peak fell after a free, so this is a reading and not a high-water mark"
        );

        // A watch started now — after the ballast has already set the mark —
        // sees no rise and reports nothing. That is what keeps a rig's
        // corpus-staging cost from being emitted as if it were a measurement.
        // Asserted here rather than in a test of its own: the quantity is
        // process-wide and monotonic, so two tests allocating against it would
        // decide each other's answers depending on which ran first.
        let after_the_fact = PeakWatch::start();
        assert!(
            after_the_fact.peak_mb().is_none(),
            "a region that never became the high-water mark must report nothing"
        );
        assert!(after_the_fact.metric().is_none());
    }
}
