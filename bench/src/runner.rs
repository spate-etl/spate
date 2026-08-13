//! Driving a compiled bench binary over the runner protocol.
//!
//! One process per (case, replicate). That is not an efficiency choice: the
//! peak resident set is a process-wide high-water mark and cannot be reset, so
//! a second case in the same process would inherit the first one's mark, and a
//! second replicate would report a maximum over both. One record per process is
//! what makes the figure attributable.
//!
//! Stdout is captured and parsed; stderr is inherited, so a panic inside a case
//! reaches the terminal as it happens rather than being reassembled afterwards.
//!
//! A call that has not returned after [`slow_period`] is reported on stderr, and
//! reported again at every period after that. Nothing is killed: the driver names
//! the child so an operator — or the log of a run bounded from outside — says
//! which leg and which case is still running, and stopping it stays the
//! operator's decision.
//!
//! The build fingerprint travels *down* to the child through the environment so
//! a record is self-describing wherever it is written, and is stamped back onto
//! every record the driver collects — see [`Runner::measure`] for why the round
//! trip cannot be trusted across two checkouts.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::fingerprint::{BuildFingerprint, FINGERPRINT_ENV};
use crate::note;
use crate::protocol::{Calibration, Listing, PROTOCOL_VERSION, ProtocolVersion};
use crate::record::Record;

/// The shortest interval between reports of an outstanding call, and the floor
/// [`slow_period`] derives against.
const SLOW_PERIOD: Duration = Duration::from_secs(30);

/// How much of a case's own budget a child may spend before it is reported.
const PERIOD_MULTIPLE: u64 = 20;

/// How long a protocol call runs before the driver reports it, and the interval
/// between reports after that.
///
/// Twenty times `target_ms` + `warmup_ms` — what a child was asked to spend
/// inside its region and warming up for it — and never less than thirty
/// seconds. The multiple is the headroom for everything the child does outside
/// that: process start, and building the corpus, which nothing bounds.
#[must_use]
pub fn slow_period(target_ms: u64, warmup_ms: u64) -> Duration {
    Duration::from_millis(
        target_ms
            .saturating_add(warmup_ms)
            .saturating_mul(PERIOD_MULTIPLE),
    )
    .max(SLOW_PERIOD)
}

/// A compiled bench binary, checked to speak this protocol.
#[derive(Debug, Clone)]
pub struct Runner {
    binary: PathBuf,
    dir: PathBuf,
    fingerprint: BuildFingerprint,
    fingerprint_json: String,
    period: Duration,
}

impl Runner {
    /// Wraps a binary and checks its protocol version.
    ///
    /// `dir` is the leg's own tree, which every child runs in. `period` is how
    /// long one of this binary's calls runs before the driver reports it, from
    /// [`slow_period`].
    ///
    /// The version check happens once, before anything is measured, because the
    /// two legs of an A/B run are compiled from different checkouts and a
    /// mismatch is the one failure that would otherwise show up as a parse
    /// error halfway through a ten-minute run.
    ///
    /// # Errors
    ///
    /// When the binary cannot be run, does not answer with JSON, or answers
    /// with a protocol this driver does not speak.
    pub fn open(
        binary: &Path,
        dir: &Path,
        fingerprint: &BuildFingerprint,
        period: Duration,
    ) -> Result<Self, String> {
        let fingerprint_json = serde_json::to_string(fingerprint)
            .map_err(|e| format!("the build fingerprint does not serialise: {e}"))?;
        let runner = Self {
            binary: binary.to_owned(),
            dir: dir.to_owned(),
            fingerprint: fingerprint.clone(),
            fingerprint_json,
            period,
        };
        let version: ProtocolVersion = runner.ask(&["--protocol-version".to_owned()])?;
        if version.protocol != PROTOCOL_VERSION {
            return Err(format!(
                "{} speaks protocol {} and this driver speaks {PROTOCOL_VERSION}",
                binary.display(),
                version.protocol
            ));
        }
        Ok(runner)
    }

    /// The binary's cases.
    ///
    /// # Errors
    ///
    /// As [`Runner::open`].
    pub fn list(&self) -> Result<Listing, String> {
        self.ask(&["--list-cases".to_owned()])
    }

    /// The iteration count this case needs to run for about `target_ms`.
    ///
    /// # Errors
    ///
    /// As [`Runner::open`].
    pub fn calibrate(&self, case: &str, seed: u64, target_ms: u64) -> Result<u64, String> {
        let answer: Calibration = self.ask(&[
            "--calibrate".to_owned(),
            case.to_owned(),
            "--seed".to_owned(),
            seed.to_string(),
            "--target-ms".to_owned(),
            target_ms.to_string(),
        ])?;
        Ok(answer.iters)
    }

    /// One measured replicate.
    ///
    /// # Errors
    ///
    /// As [`Runner::open`].
    pub fn measure(&self, request: &Measurement<'_>) -> Result<Record, String> {
        let mut args = vec![
            "--run".to_owned(),
            request.case.to_owned(),
            "--seed".to_owned(),
            request.seed.to_string(),
            "--iters".to_owned(),
            request.iters.to_string(),
            "--replicate".to_owned(),
            request.replicate.to_string(),
            "--warmup-ms".to_owned(),
            request.warmup_ms.to_string(),
        ];
        if request.priming {
            args.push("--priming".to_owned());
        }
        let mut record: Record = self.ask(&args)?;
        // The driver's fingerprint wins over the one that came back, which is
        // the same value round-tripped through the child. The round trip is not
        // lossless across checkouts: a base leg compiled from an older commit
        // deserialises the fingerprint with *its* struct definition, drops any
        // field it does not know, and emits a record missing it — which the
        // comparator then reads as the two legs disagreeing about a guarded
        // field. The driver knows what it built; the binary is only carrying
        // the value for a run nobody drove.
        record.build = self.fingerprint.clone();
        Ok(record)
    }

    /// How a child is named while it is still running: its leg, its target, and
    /// the call in full.
    fn label(&self, args: &[String]) -> String {
        format!(
            "{} {} {}",
            self.fingerprint.leg,
            target_name(&self.binary),
            args.join(" ")
        )
    }

    /// Runs the binary and parses its single line of stdout.
    ///
    /// A call outstanding longer than this runner's period is reported on
    /// stderr, once per period, until it returns.
    fn ask<T: DeserializeOwned>(&self, args: &[String]) -> Result<T, String> {
        // Bound to a name, not to `_`. A `let _ =` drops the guard here rather
        // than at the end of the call, which stops the thread before it can
        // report anything and leaves no trace that it did.
        let _watchdog = Watchdog::start(self.period, self.label(args), note);

        let output = Command::new(&self.binary)
            .args(args)
            // In its own leg's tree. No case reads a file today, but the day
            // one does, both legs inheriting the driver's directory would read
            // the *head* tree's fixture — and the corpus digests would agree,
            // so the report would be clean, tight and entirely wrong.
            .current_dir(&self.dir)
            .env(FINGERPRINT_ENV, &self.fingerprint_json)
            .stderr(Stdio::inherit())
            .output()
            .map_err(|e| format!("could not run {}: {e}", self.binary.display()))?;

        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| format!("{} printed invalid UTF-8: {e}", self.binary.display()))?;

        // The *last* non-empty line, not the whole of stdout. A `println!` left
        // in a benched routine would otherwise take the whole run down with a
        // parse error, and stdout belongs to the protocol by convention rather
        // than by anything that can enforce it.
        let line = stdout.lines().map(str::trim).rfind(|l| !l.is_empty());

        match line {
            // A protocol answer is a protocol answer whatever the exit status,
            // but a non-zero status alongside one is still a failure.
            Some(text) if output.status.success() => {
                serde_json::from_str(text).map_err(|e| hint(&self.binary, args, &e.to_string()))
            }
            // An interrupt reaches the whole process group, so the child dies
            // non-zero and would otherwise earn a manifest hint that has
            // nothing to do with what happened.
            _ if crate::worktree::interrupted() => {
                Err("interrupted; the run is unwinding".to_owned())
            }
            // The target spoke the protocol and refused the call. Its reason is
            // already on the terminal — stderr is inherited — so the manifest
            // advice below would only point somewhere the reader has no reason
            // to look, about a binary that has just demonstrated it parses these
            // arguments fine.
            _ if output.status.code() == Some(crate::protocol::ERROR_EXIT) => Err(format!(
                "{} refused '{}': its reason is on stderr, above.",
                target_name(&self.binary),
                args.join(" ")
            )),
            _ => Err(hint(
                &self.binary,
                args,
                &format!(
                    "it exited with {} and its last line of stdout was {}",
                    output.status,
                    line.map_or_else(|| "absent".to_owned(), |l| format!("{l:?}"))
                ),
            )),
        }
    }
}

/// One replicate to measure.
#[derive(Debug, Clone, Copy)]
pub struct Measurement<'a> {
    /// The case id.
    pub case: &'a str,
    /// The corpus seed, identical on both legs and across replicates.
    pub seed: u64,
    /// The iteration count, calibrated once on the base leg and pinned for
    /// both.
    pub iters: u64,
    /// The replicate index — the key the comparator pairs on.
    pub replicate: u32,
    /// Whether this is the discarded priming pass.
    pub priming: bool,
    /// Milliseconds of unmeasured warm-up.
    pub warmup_ms: u64,
}

/// Reports a call that is still running, once per period, until it is dropped.
///
/// The reporting thread stops when the guard drops, which is what makes the
/// guard's scope the call's lifetime.
struct Watchdog {
    // Both fields are `Option` so `Drop` can drop the sender before joining the
    // thread. Joining first waits for a thread whose exit condition is that
    // drop, which never comes.
    done: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Watchdog {
    /// Starts reporting `label` every `period` until the guard is dropped.
    ///
    /// Nothing is ever sent on the channel. The sender's lifetime is the
    /// signal: dropping it disconnects the receiver, which ends the loop at
    /// once rather than at the end of the period it is sitting in.
    fn start<R>(period: Duration, label: String, report: R) -> Self
    where
        R: Fn(&str) + Send + 'static,
    {
        let (done, idle) = mpsc::channel::<()>();
        let thread = thread::spawn(move || {
            let mut periods: u32 = 0;
            while let Err(mpsc::RecvTimeoutError::Timeout) = idle.recv_timeout(period) {
                periods = periods.saturating_add(1);
                report(&slow_line(&label, period.saturating_mul(periods)));
            }
        });
        Self {
            done: Some(done),
            thread: Some(thread),
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.done.take());
        if let Some(thread) = self.thread.take() {
            // A watchdog that panicked has already lost its thread; taking its
            // payload here would panic a second time, and during an unwind that
            // aborts.
            let _ = thread.join();
        }
    }
}

/// The line an outstanding call earns.
///
/// `elapsed` is the period times the number of them that have passed, so the
/// figure is always a threshold the call has crossed rather than a measurement
/// of it — a reader can tell the reporting interval from any single line.
fn slow_line(label: &str, elapsed: Duration) -> String {
    format!("SLOW [> {:.3}s] {label}", elapsed.as_secs_f64())
}

/// The bench target's name, from the binary cargo built for it.
///
/// Cargo appends a hash to the file name, which is noise in a message about the
/// target.
fn target_name(binary: &Path) -> &str {
    binary
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.rsplit_once('-').map(|(stem, _)| stem))
        .unwrap_or("<target>")
}

/// The error any binary that did not answer the protocol earns, naming the
/// likeliest cause.
///
/// A `*_wall.rs` without `harness = false` compiles fine and is run by cargo
/// under libtest, which rejects `--list-cases` and exits 101 before the
/// target's own `main` is reached. So this covers a non-zero exit as well as
/// unparseable output: both are "no record came back", and the manifest stanza
/// is the one cause a message can do something about. The binary's own
/// diagnostics have already reached the terminal — stderr is inherited — so
/// this adds the part they cannot know.
fn hint(binary: &Path, args: &[String], why: &str) -> String {
    format!(
        "{} {} did not answer the runner protocol ({why}).\n\
         If this target is a `*_wall.rs`, check its package declares\n\
         \n    [[bench]]\n    name = \"{}\"\n    harness = false\n\n\
         Without it cargo runs the target under libtest, which rejects these arguments.",
        binary.display(),
        args.join(" "),
        target_name(binary)
    )
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant};

    use super::{PERIOD_MULTIPLE, Runner, SLOW_PERIOD, Watchdog, hint, slow_line, slow_period};
    use crate::fingerprint::BuildFingerprint;
    use crate::protocol::{PROTOCOL_VERSION, ProtocolVersion};

    /// A runner over a stand-in binary, built by struct literal.
    ///
    /// [`Runner::open`] asks the binary for its protocol version, which nothing
    /// a test can point at answers, so a test that needs a runner rather than a
    /// protocol builds one directly.
    fn stub(period: Duration) -> Runner {
        let mut fingerprint = BuildFingerprint::local();
        fingerprint.leg = "base".to_owned();
        Runner {
            binary: PathBuf::from("/bin/sh"),
            dir: std::env::temp_dir(),
            fingerprint,
            fingerprint_json: "{}".to_owned(),
            period,
        }
    }

    #[test]
    fn the_hint_names_the_manifest_stanza_and_strips_the_hash_suffix() {
        let message = hint(
            Path::new("/t/bench/decode_wall-9f3c1a"),
            &["--list-cases".to_owned()],
            "expected value at line 1",
        );
        assert!(message.contains("harness = false"), "{message}");
        assert!(message.contains("name = \"decode_wall\""), "{message}");
        assert!(message.contains("--list-cases"), "{message}");
    }

    /// The reported figure is the threshold crossed, so the count is one-sided:
    /// a loaded machine may deliver fewer reports than the elapsed time allows,
    /// and asserting an exact number would make this a test of the scheduler.
    #[test]
    fn a_call_that_outlives_a_period_is_reported_once_per_period() {
        let (tx, rx) = mpsc::channel();
        let watchdog = Watchdog::start(
            Duration::from_millis(20),
            "base decode_wall --run decode/small".to_owned(),
            move |line: &str| {
                let _ = tx.send(line.to_owned());
            },
        );
        std::thread::sleep(Duration::from_millis(110));
        drop(watchdog);

        let lines: Vec<String> = rx.iter().collect();
        assert!(!lines.is_empty(), "no report after five periods");
        assert_eq!(
            lines[0],
            "SLOW [> 0.020s] base decode_wall --run decode/small"
        );
        for (index, line) in lines.iter().enumerate() {
            let elapsed = 0.020 * (index + 1) as f64;
            assert!(
                line.starts_with(&format!("SLOW [> {elapsed:.3}s] ")),
                "report {index} is {line}"
            );
        }
    }

    /// What makes the watchdog safe to arm on every call: a child that answers
    /// promptly is never mentioned.
    #[test]
    fn a_call_that_returns_before_the_first_period_is_never_reported() {
        let (tx, rx) = mpsc::channel();
        let watchdog = Watchdog::start(
            Duration::from_secs(5),
            "base decode_wall --list-cases".to_owned(),
            move |line: &str| {
                let _ = tx.send(line.to_owned());
            },
        );
        drop(watchdog);

        assert!(rx.try_recv().is_err());
    }

    /// A `Drop` that joined before dropping the sender would wait for a thread
    /// whose exit condition is that drop, so this hangs rather than fails if the
    /// order is ever reversed.
    #[test]
    fn dropping_the_watchdog_stops_and_joins_its_thread() {
        let marker = Arc::new(());
        let held = Arc::clone(&marker);
        let watchdog = Watchdog::start(Duration::from_millis(5), "base t --run c".to_owned(), {
            move |_: &str| {
                let _ = &held;
            }
        });
        std::thread::sleep(Duration::from_millis(20));
        drop(watchdog);

        assert_eq!(Arc::strong_count(&marker), 1);
    }

    /// The watchdog wraps the call rather than changing it: an answer parses as
    /// it did before, and a period nothing reaches costs nothing.
    #[test]
    fn a_prompt_answer_parses_exactly_as_it_did_unwatched() {
        let runner = stub(Duration::from_secs(5));
        let started = Instant::now();
        let answer: ProtocolVersion = runner
            .ask(&[
                "-c".to_owned(),
                format!("echo '{{\"protocol\":{PROTOCOL_VERSION}}}'"),
            ])
            .expect("the stub answers the protocol");

        assert_eq!(answer.protocol, PROTOCOL_VERSION);
        // Well inside the 5s period: the failure this rules out is a guard that
        // waits the period out instead of disconnecting, and the margin is wide
        // enough that a loaded machine is not what fails it.
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn the_slow_line_names_the_leg_the_target_and_the_call() {
        let mut runner = stub(SLOW_PERIOD);
        runner.binary = PathBuf::from("/t/bench/decode_wall-9f3c1a");
        let label = runner.label(&["--run".to_owned(), "decode/small".to_owned()]);

        assert_eq!(label, "base decode_wall --run decode/small");
        assert_eq!(
            slow_line(&label, Duration::from_secs(30)),
            "SLOW [> 30.000s] base decode_wall --run decode/small"
        );
    }

    /// The floor, the multiple, and the saturation the multiple could otherwise
    /// overflow through.
    #[test]
    fn the_period_is_the_larger_of_the_floor_and_the_case_budget() {
        assert_eq!(slow_period(50, 50), SLOW_PERIOD);
        assert_eq!(slow_period(1, 0), SLOW_PERIOD);
        assert_eq!(slow_period(5_000, 1_000), Duration::from_secs(120));
        assert_eq!(
            slow_period(u64::MAX, u64::MAX),
            Duration::from_millis(u64::MAX)
        );
        assert_eq!(PERIOD_MULTIPLE, 20);
        assert_eq!(SLOW_PERIOD, Duration::from_secs(30));
    }
}
