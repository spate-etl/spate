//! Peak resident set and CPU time, read from the kernel's own accounting.
//!
//! One `getrusage` call answers both. They are the two figures a wall-clock
//! number cannot supply on its own: how much memory the machine had to find,
//! and how much of the elapsed time was work rather than waiting.
//!
//! They are taken at different moments, though, and the difference matters.
//! CPU time is a running total, so it is read at the two ends of the measured
//! region and differenced. The resident set is a high-water mark that cannot be
//! reset, so it needs the gate below instead.
//!
//! # Why a watch rather than a reading
//!
//! `ru_maxrss` is monotonic for the life of the process and cannot be reset, so
//! a bare reading answers "what did this *process* peak at", which is rarely the
//! question. A case that builds a corpus before measuring may peak while
//! building it, and that peak has nothing to do with the code under test.
//!
//! [`PeakWatch`] is a **validity gate, not a subtraction**. It takes a baseline
//! when setup is done — before any warm-up, since a warm-up runs the same
//! routine and would otherwise set the mark the measurement is then judged
//! against — and reports the process peak only if the process rose above that
//! baseline afterwards. What that buys is narrow and worth stating
//! precisely: it proves running the case set the mark, so the number is about
//! the case rather than about whatever preceded it. It does not remove the
//! baseline from the figure — the value reported is the process peak, and
//! anything still resident from setup is inside it.
//!
//! A case that never sets the mark reports no `peak_rss_bytes` at all and says
//! so in a note. An absent metric is not a zero: the comparator leaves it out
//! of the table rather than rendering a hundred-percent drop.
//!
//! # One record per process
//!
//! The harness runs one process per (case, replicate) for this reason. A peak
//! taken over ten replicates is not comparable with a peak taken over one, and
//! a second case in the same process would inherit the first one's high-water
//! mark.

/// Peak resident set size of this process, in bytes, or `None` if the kernel
/// declined to say.
///
/// # The unit differs by platform, and getting it wrong is a 1024× error
///
/// `getrusage` returns `ru_maxrss` in **kilobytes on Linux** and in **bytes on
/// macOS**. Both are `long`, both are plausible-looking numbers, and a harness
/// that picked one convention would report a figure 1024× wrong on the other
/// platform while looking entirely healthy. The `cfg` below is why this
/// function exists rather than three lines inline at the call site, and
/// `the_peak_is_a_rising_high_water_mark_in_bytes` pins the unit in both
/// directions.
#[must_use]
pub fn peak_rss_bytes() -> Option<u64> {
    let usage = raw_usage()?;
    let raw = u64::try_from(usage.ru_maxrss).ok()?;

    // Enumerated rather than split on one platform with a catch-all, because a
    // catch-all here is the 1024× error this function exists to prevent, just
    // deferred to whichever platform nobody thought of. `ru_maxrss` is bytes on
    // every Darwin kernel, kibibytes on Linux, and *pages* on Solaris and
    // illumos — where a `* 1024` fallback would under-report by 4-8× while
    // compiling perfectly. An unknown platform reports nothing, which the
    // record schema already handles, rather than a number nobody has checked.
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

/// CPU time this process has consumed, user plus system, in nanoseconds.
///
/// Unlike the resident set this is a running total rather than a high-water
/// mark, so a difference between two readings is a real quantity and no gate is
/// needed.
#[must_use]
pub fn cpu_ns() -> Option<u64> {
    let usage = raw_usage()?;
    let total = [usage.ru_utime, usage.ru_stime]
        .iter()
        .map(|tv| {
            let secs = u64::try_from(tv.tv_sec).unwrap_or(0);
            let usecs = u64::try_from(tv.tv_usec).unwrap_or(0);
            secs.saturating_mul(1_000_000_000)
                .saturating_add(usecs.saturating_mul(1_000))
        })
        .fold(0u64, u64::saturating_add);
    Some(total)
}

fn raw_usage() -> Option<libc::rusage> {
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
    Some(usage)
}

/// A gate that reports the process peak only when running the case set it.
///
/// Construct it when setup is finished and the thing worth measuring is about
/// to begin — **before any warm-up**, not after. `ru_maxrss` is monotonic and a
/// warm-up runs the same routine, so a baseline taken afterwards finds the mark
/// already set by the routine's own first pass and reports nothing. Starting
/// before the warm-up keeps what the gate is actually for: excluding a case
/// whose *corpus building* set the mark, so the figure describes running the
/// case rather than preparing it.
#[derive(Debug, Clone, Copy)]
pub struct PeakWatch {
    baseline_rss: Option<u64>,
}

impl PeakWatch {
    /// Takes the baseline. Call this once setup is done and before the warm-up,
    /// so a peak reached while building the corpus disqualifies the figure
    /// rather than being reported as a measurement.
    #[must_use]
    pub fn start() -> Self {
        Self {
            baseline_rss: peak_rss_bytes(),
        }
    }

    /// The process peak in bytes, if the process rose above the baseline.
    ///
    /// The value is the peak itself, not the growth since the baseline: a
    /// high-water mark minus another high-water mark is not a quantity anything
    /// consumes. `None` means running the case never took the process above
    /// what setup had already made resident, so the only figure available
    /// describes the setup — and the harness reports nothing rather than that.
    #[must_use]
    pub fn peak_rss_bytes(self) -> Option<u64> {
        let baseline = self.baseline_rss?;
        let peak = peak_rss_bytes()?;
        (peak > baseline).then_some(peak)
    }
}

#[cfg(test)]
mod tests {
    use super::{PeakWatch, cpu_ns, peak_rss_bytes};

    /// One test rather than several, because they would interfere. `make test`
    /// runs nextest, which gives each test its own process — but a bare
    /// `cargo test -p spate-bench --lib` does not, and there the quantity under
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
        // 1024× error lands three orders of magnitude away.
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

        // Rising above the baseline is what makes a figure attributable, and
        // the figure is the peak itself rather than the growth.
        let reported = watch
            .peak_rss_bytes()
            .expect("the ballast rose above the baseline");
        assert_eq!(reported, peak);

        drop(ballast);
        assert!(
            peak_rss_bytes().expect("readable") >= peak,
            "the peak fell after a free, so this is a reading and not a high-water mark"
        );

        // A watch started now — after the ballast has already set the mark —
        // sees no rise and reports nothing. That is what keeps a case's
        // corpus-building cost from being emitted as if it were a measurement.
        // Asserted here rather than in a test of its own: the quantity is
        // process-wide and monotonic, so two tests allocating against it would
        // decide each other's answers depending on which ran first.
        let after_the_fact = PeakWatch::start();
        assert!(
            after_the_fact.peak_rss_bytes().is_none(),
            "a region that never became the high-water mark must report nothing"
        );
    }

    #[test]
    fn cpu_time_is_a_running_total_that_only_grows() {
        let first = cpu_ns().expect("the kernel accounts CPU time for every process");

        let mut acc = 0u64;
        for i in 0..5_000_000u64 {
            acc = acc.wrapping_add(i.wrapping_mul(2_654_435_761));
        }
        std::hint::black_box(acc);

        let second = cpu_ns().expect("readable");
        assert!(second >= first, "CPU time went backwards");
        assert!(
            second > first,
            "five million multiplies consumed no measurable CPU time"
        );
    }
}
