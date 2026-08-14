//! The runner protocol a bench binary speaks, version 1.
//!
//! The driver never links a bench target; it runs it. That keeps a crate's
//! benchmarks buildable with `cargo bench --no-run` alone and lets one CLI
//! drive targets compiled from two different checkouts, which is what an A/B
//! run is.
//!
//! **JSON on stdout, humans on stderr.** Every mode below writes one JSON
//! value to stdout and nothing else, so the driver parses the stream rather
//! than scraping it. Diagnostics, panics and cargo's own noise go to stderr.
//!
//! | Invocation | Answer |
//! | --- | --- |
//! | `--protocol-version` | `{"protocol":1}` |
//! | `--list-cases` | the target's cases |
//! | `--calibrate <case> --seed N --target-ms M` | `{"iters":N}` |
//! | `--run <case> --seed N --iters M --replicate K [--priming] [--warmup-ms W]` | one [`Record`] |
//! | no arguments, or cargo's own `--bench` / `--test` | usage on stderr, exit 0 |
//!
//! The last row is deliberate. `cargo bench` runs every bench target with
//! `--bench` appended and `cargo test` with `--test`, so a target that treated
//! either as an unknown argument would fail a plain `cargo bench -p <crate>`
//! for everybody. Both are accepted and ignored; with no mode alongside them
//! the target prints what it is and exits 0.
//!
//! # What the driver owns
//!
//! The seed, the iteration count, the replicate index and the priming flag all
//! arrive from outside, because all four have to be identical on both legs and
//! a binary cannot know what the other leg did. The build fingerprint arrives
//! through the environment; see [`crate::fingerprint`].
//!
//! `--seed` seeds the corpus and nothing else. `--replicate` seeds nothing at
//! all: it is an index, so the comparator can pair replicate *k* of one leg
//! with replicate *k* of the other.

use std::io::Write as _;

use serde::{Deserialize, Serialize};

use crate::case::{RunOptions, Suite};
use crate::fingerprint::{BuildFingerprint, Host};
use crate::record::{CaseId, Record, SCHEMA_VERSION};

/// The exit code a target uses when it understood the call and refused it.
///
/// Distinct from the codes a target exits with for reasons of its own (libtest
/// panics with 101 when a `*_wall.rs` is missing its `harness = false`) so the
/// driver can tell "this binary does not speak the protocol" from "this binary
/// spoke it and said no". The reason goes to stderr, which is inherited, so it
/// has already reached the operator by the time the driver sees this.
pub const ERROR_EXIT: i32 = 2;

/// The protocol version this crate speaks.
///
/// A driver refuses a binary answering with anything else. The two sides of an
/// A/B run are compiled from different checkouts, so this is the one number
/// that says whether they understand each other.
pub const PROTOCOL_VERSION: u32 = 1;

/// The answer to `--protocol-version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    /// The version the binary speaks.
    pub protocol: u32,
}

/// One case, as `--list-cases` describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseSummary {
    /// The case's id within its target.
    pub id: String,
    /// Why the case declared itself noisy, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub erratic: Option<String>,
    /// The iteration count the case pinned, if it pinned one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iters_hint: Option<u64>,
}

/// The answer to `--list-cases`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listing {
    /// The protocol the binary speaks, repeated here so one call answers both
    /// questions on the common path.
    pub protocol: u32,
    /// The package the target belongs to.
    #[serde(rename = "crate")]
    pub krate: String,
    /// The bench target's name.
    pub target: String,
    /// Its cases, in declaration order.
    pub cases: Vec<CaseSummary>,
}

/// The answer to `--calibrate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Calibration {
    /// The iteration count the driver should pin for both legs.
    pub iters: u64,
}

/// Runs a bench target's side of the protocol.
///
/// Returns the process exit code. Use [`crate::bench_main!`] rather than
/// calling this directly; the macro also installs the counting allocator,
/// without which every record reports absent allocation metrics.
///
/// `krate` and `target` are the compiling package and bench target;
/// `bench_main!` passes `CARGO_PKG_NAME` and `CARGO_CRATE_NAME`.
#[must_use]
pub fn run(suite: &Suite, krate: &str, target: &str, args: &[String]) -> i32 {
    match dispatch(suite, krate, target, args) {
        Ok(output) => {
            let mut stdout = std::io::stdout().lock();
            if let Some(line) = output
                && writeln!(stdout, "{line}").is_err()
            {
                return 1;
            }
            i32::from(stdout.flush().is_err())
        }
        Err(message) => {
            let _ = writeln!(std::io::stderr(), "{target}: {message}");
            ERROR_EXIT
        }
    }
}

/// The protocol proper: arguments in, one line of JSON out (or nothing).
fn dispatch(
    suite: &Suite,
    krate: &str,
    target: &str,
    args: &[String],
) -> Result<Option<String>, String> {
    // A suite naming a package cargo does not know produces records that
    // intersect with nothing on the other leg, and the report would say the
    // case exists on neither side rather than that the name is wrong.
    if suite.krate() != krate {
        return Err(format!(
            "the suite calls itself '{}' but cargo compiled it in '{krate}'; \
             spate_bench::suite() takes the package name",
            suite.krate()
        ));
    }

    if args.is_empty() {
        let _ = writeln!(std::io::stderr(), "{}", usage(krate, target));
        return Ok(None);
    }

    let mut opts = Args::parse(args)?;
    if opts.mode.is_none() && opts.cargo_selector {
        let _ = writeln!(std::io::stderr(), "{}", usage(krate, target));
        return Ok(None);
    }
    match opts.take_mode()? {
        Mode::Version => Ok(Some(json(&ProtocolVersion {
            protocol: PROTOCOL_VERSION,
        })?)),
        Mode::List => Ok(Some(json(&Listing {
            protocol: PROTOCOL_VERSION,
            krate: krate.to_owned(),
            target: target.to_owned(),
            cases: suite
                .cases()
                .iter()
                .map(|case| CaseSummary {
                    id: case.id().to_owned(),
                    erratic: case.erratic().map(str::to_owned),
                    iters_hint: case.iters_hint(),
                })
                .collect(),
        })?)),
        Mode::Calibrate(id) => {
            let case = find(suite, &id)?;
            let iters = case.calibrate(opts.require("--seed")?, opts.target_ms())?;
            Ok(Some(json(&Calibration { iters })?))
        }
        Mode::Run(id) => {
            let case = find(suite, &id)?;
            let seed = opts.require("--seed")?;
            // Clamped here rather than inside the measurement loop, so the
            // record's `iters` is the count that actually ran. A record saying
            // zero while one iteration ran would divide every metric by the
            // wrong number on the other side of the comparison.
            let iters = opts.require("--iters")?.max(1);
            let replicate = u32::try_from(opts.require("--replicate")?)
                .map_err(|_| "--replicate is out of range".to_owned())?;
            let outcome = case.measure(&RunOptions {
                seed,
                iters,
                warmup_ms: opts.warmup_ms(),
            })?;
            let mut notes = outcome.notes;
            // The reason travels with the record rather than only with the case
            // list, so a report rendered from a leg directory alone can say why
            // a case is informational.
            if let Some(why) = case.erratic() {
                notes.push(format!("erratic: {why}"));
            }
            let record = Record {
                schema: SCHEMA_VERSION,
                case: CaseId {
                    krate: krate.to_owned(),
                    target: target.to_owned(),
                    case: id,
                },
                replicate,
                priming: opts.priming,
                iters,
                erratic: case.erratic().is_some(),
                seed,
                corpus_digest: outcome.corpus_digest,
                build_digest: outcome.build_digest,
                metrics: outcome.metrics,
                notes,
                build: BuildFingerprint::from_env()?,
                host: Host::detect(),
                ts_ms: Record::now_ms(),
            };
            Ok(Some(record.to_line().map_err(|e| e.to_string())?))
        }
    }
}

fn find<'s>(suite: &'s Suite, id: &str) -> Result<&'s crate::case::Case, String> {
    suite.find(id).ok_or_else(|| {
        let known: Vec<&str> = suite.cases().iter().map(crate::case::Case::id).collect();
        format!("no case '{id}'; this target declares: {}", known.join(", "))
    })
}

fn json<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}

fn usage(krate: &str, target: &str) -> String {
    format!(
        "{target} ({krate}) is a Spate wall-clock bench target, driven by the `bench` CLI \
         rather than run directly.\n\
         \n\
           cargo run -p spate-bench --features driver --bin bench -- list\n\
           cargo run -p spate-bench --features driver --bin bench -- ab main\n\
         \n\
         Protocol {PROTOCOL_VERSION}: --protocol-version | --list-cases | \
         --calibrate <case> --seed N --target-ms M | \
         --run <case> --seed N --iters M --replicate K [--priming] [--warmup-ms W]"
    )
}

/// What the driver asked for.
#[derive(Debug)]
enum Mode {
    Version,
    List,
    Calibrate(String),
    Run(String),
}

/// The argument following `args[at]`, or an error naming the flag that wanted
/// it.
fn value_after(args: &[String], at: usize, flag: &str) -> Result<String, String> {
    args.get(at + 1)
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

/// A hand-rolled parser, because clap is behind the `driver` feature and a
/// bench target must not compile it. The grammar is five flags wide and every
/// caller is this crate's own runner, so the parser's job is to reject rather
/// than to guide.
#[derive(Debug, Default)]
struct Args {
    mode: Option<Mode>,
    cargo_selector: bool,
    seed: Option<u64>,
    iters: Option<u64>,
    replicate: Option<u64>,
    target_ms: Option<u64>,
    warmup_ms: Option<u64>,
    priming: bool,
}

impl Args {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut out = Self::default();
        let mut i = 0;
        while i < args.len() {
            let flag = args[i].as_str();
            match flag {
                "--protocol-version" => {
                    out.set_mode(Mode::Version)?;
                    i += 1;
                }
                "--list-cases" => {
                    out.set_mode(Mode::List)?;
                    i += 1;
                }
                "--calibrate" => {
                    let case = value_after(args, i, flag)?;
                    out.set_mode(Mode::Calibrate(case))?;
                    i += 2;
                }
                "--run" => {
                    let case = value_after(args, i, flag)?;
                    out.set_mode(Mode::Run(case))?;
                    i += 2;
                }
                "--priming" => {
                    out.priming = true;
                    i += 1;
                }
                // Cargo appends these when it runs a bench or test target
                // itself. Accepted and ignored, so `cargo bench -p <crate>`
                // reaches the usage message rather than an argument error.
                "--bench" | "--test" => {
                    out.cargo_selector = true;
                    i += 1;
                }
                "--seed" | "--iters" | "--replicate" | "--target-ms" | "--warmup-ms" => {
                    let raw = value_after(args, i, flag)?;
                    let parsed = raw
                        .parse::<u64>()
                        .map_err(|_| format!("{flag} takes a non-negative integer, got '{raw}'"))?;
                    match flag {
                        "--seed" => out.seed = Some(parsed),
                        "--iters" => out.iters = Some(parsed),
                        "--replicate" => out.replicate = Some(parsed),
                        "--target-ms" => out.target_ms = Some(parsed),
                        _ => out.warmup_ms = Some(parsed),
                    }
                    i += 2;
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(out)
    }

    fn set_mode(&mut self, mode: Mode) -> Result<(), String> {
        if self.mode.is_some() {
            return Err("only one of --protocol-version, --list-cases, --calibrate, --run".into());
        }
        self.mode = Some(mode);
        Ok(())
    }

    fn take_mode(&mut self) -> Result<Mode, String> {
        self.mode.take().ok_or_else(|| {
            "no mode given; expected one of --protocol-version, --list-cases, --calibrate, --run"
                .to_owned()
        })
    }

    fn require(&self, flag: &str) -> Result<u64, String> {
        let slot = match flag {
            "--seed" => self.seed,
            "--iters" => self.iters,
            "--replicate" => self.replicate,
            _ => None,
        };
        slot.ok_or_else(|| format!("{flag} is required here"))
    }

    /// Calibration target, defaulted so a hand-run probe needs one flag fewer.
    fn target_ms(&self) -> u64 {
        self.target_ms.unwrap_or(50)
    }

    /// Warm-up budget. Zero is a legitimate value and is not the default.
    fn warmup_ms(&self) -> u64 {
        self.warmup_ms.unwrap_or(50)
    }
}

/// Declares a bench target's `main`, and installs the counting allocator.
///
/// Takes a function returning a [`Suite`]:
///
/// ```no_run
/// use spate_bench::{Suite, bench_main};
///
/// fn suite() -> Suite {
///     spate_bench::suite("spate-bench")
///         .case("noop", |_, _| (), |b, ()| b.iter(|| 1u8))
///         .done()
/// }
///
/// bench_main!(suite);
/// ```
///
/// The allocator is installed here rather than behind a feature: gating it
/// would mean the wall numbers and the allocation numbers come from two
/// different binaries. See [`crate::alloc`].
#[macro_export]
macro_rules! bench_main {
    ($suite:path) => {
        // One global allocator per process, claimed at the one place that knows
        // it is a bench binary.
        #[global_allocator]
        static SPATE_BENCH_ALLOCATOR: $crate::alloc::Counting = $crate::alloc::Counting;

        fn main() {
            let suite = $suite();
            let args: ::std::vec::Vec<::std::string::String> = ::std::env::args().skip(1).collect();
            // `env!` expands at this call site, so it reads the *bench target's*
            // package and crate name rather than this crate's.
            let code = $crate::protocol::run(
                &suite,
                env!("CARGO_PKG_NAME"),
                env!("CARGO_CRATE_NAME"),
                &args,
            );
            ::std::process::exit(code);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::{Calibration, Listing, PROTOCOL_VERSION, ProtocolVersion, dispatch};
    use crate::case::Suite;
    use crate::record::Record;

    fn suite() -> Suite {
        crate::suite("spate-bench")
            .case(
                "quick",
                |corpus, seed| {
                    let data: Vec<u64> = (0..512).map(|i| seed.wrapping_mul(i)).collect();
                    corpus.absorb(
                        "data",
                        &data
                            .iter()
                            .flat_map(|v| v.to_le_bytes())
                            .collect::<Vec<_>>(),
                    );
                    data
                },
                |b, data| b.iter(|| data.iter().fold(0u64, |a, v| a.wrapping_add(*v))),
            )
            .items(512)
            .done()
    }

    fn call(args: &[&str]) -> Result<Option<String>, String> {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        dispatch(&suite(), "spate-bench", "selftest_wall", &owned)
    }

    #[test]
    fn the_version_is_the_first_thing_a_driver_can_ask() {
        let line = call(&["--protocol-version"])
            .expect("dispatches")
            .expect("prints");
        let parsed: ProtocolVersion = serde_json::from_str(&line).expect("parses");
        assert_eq!(parsed.protocol, PROTOCOL_VERSION);
    }

    #[test]
    fn listing_names_the_package_the_target_and_the_cases() {
        let line = call(&["--list-cases"])
            .expect("dispatches")
            .expect("prints");
        let parsed: Listing = serde_json::from_str(&line).expect("parses");
        assert_eq!(parsed.krate, "spate-bench");
        assert_eq!(parsed.target, "selftest_wall");
        assert_eq!(parsed.protocol, PROTOCOL_VERSION);
        assert_eq!(parsed.cases.len(), 1);
        assert_eq!(parsed.cases[0].id, "quick");
        assert!(parsed.cases[0].erratic.is_none());
    }

    #[test]
    fn calibrate_answers_an_iteration_count() {
        let line = call(&["--calibrate", "quick", "--seed", "1", "--target-ms", "1"])
            .expect("dispatches")
            .expect("prints");
        let parsed: Calibration = serde_json::from_str(&line).expect("parses");
        assert!(parsed.iters >= 1);
    }

    #[test]
    fn run_answers_one_record_carrying_what_the_driver_decided() {
        let line = call(&[
            "--run",
            "quick",
            "--seed",
            "42",
            "--iters",
            "16",
            "--replicate",
            "3",
            "--priming",
            "--warmup-ms",
            "0",
        ])
        .expect("dispatches")
        .expect("prints");

        let record: Record = serde_json::from_str(&line).expect("parses");
        assert_eq!(record.case.krate, "spate-bench");
        assert_eq!(record.case.target, "selftest_wall");
        assert_eq!(record.case.case, "quick");
        assert_eq!(record.replicate, 3);
        assert!(record.priming);
        assert_eq!(record.iters, 16);
        assert_eq!(record.seed, 42);
        assert!(!record.erratic);
        assert!(record.metrics.contains_key(crate::record::WALL_NS_PER_ITER));
    }

    /// No arguments is what `cargo bench` does.
    #[test]
    fn no_arguments_prints_nothing_to_stdout_and_succeeds() {
        assert_eq!(call(&[]), Ok(None));
    }

    #[test]
    fn the_parser_rejects_rather_than_guesses() {
        assert!(call(&["--list-cases", "--run", "quick"]).is_err());
        assert!(call(&["--seed", "1"]).is_err());
        assert!(call(&["--nonsense"]).is_err());
        assert!(call(&["--calibrate"]).is_err());
        assert!(call(&["--run", "quick", "--seed", "x"]).is_err());
        assert!(call(&["--run", "quick", "--seed", "1"]).is_err());
        assert!(
            call(&[
                "--run",
                "nope",
                "--seed",
                "1",
                "--iters",
                "1",
                "--replicate",
                "0"
            ])
            .is_err()
        );
    }

    /// A suite whose declared package name is not the one cargo compiled it in
    /// would produce records that intersect with nothing on the other leg.
    #[test]
    fn a_mistyped_package_name_is_refused_at_start_up() {
        let owned = vec!["--list-cases".to_owned()];
        let err = dispatch(&suite(), "spate-jsonn", "decode_wall", &owned).expect_err("refused");
        assert!(err.contains("spate-jsonn"), "{err}");
    }
}
