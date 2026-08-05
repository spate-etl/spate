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
//! The build fingerprint travels *down* to the child through the environment so
//! a record is self-describing wherever it is written, and is stamped back onto
//! every record the driver collects — see [`Runner::measure`] for why the round
//! trip cannot be trusted across two checkouts.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::de::DeserializeOwned;

use crate::fingerprint::{BuildFingerprint, FINGERPRINT_ENV};
use crate::protocol::{Calibration, Listing, PROTOCOL_VERSION, ProtocolVersion};
use crate::record::Record;

/// A compiled bench binary, checked to speak this protocol.
#[derive(Debug, Clone)]
pub struct Runner {
    binary: PathBuf,
    dir: PathBuf,
    fingerprint: BuildFingerprint,
    fingerprint_json: String,
}

impl Runner {
    /// Wraps a binary and checks its protocol version.
    ///
    /// `dir` is the leg's own tree, which every child runs in.
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
    pub fn open(binary: &Path, dir: &Path, fingerprint: &BuildFingerprint) -> Result<Self, String> {
        let fingerprint_json = serde_json::to_string(fingerprint)
            .map_err(|e| format!("the build fingerprint does not serialise: {e}"))?;
        let runner = Self {
            binary: binary.to_owned(),
            dir: dir.to_owned(),
            fingerprint: fingerprint.clone(),
            fingerprint_json,
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

    /// Runs the binary and parses its single line of stdout.
    fn ask<T: DeserializeOwned>(&self, args: &[String]) -> Result<T, String> {
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
        binary
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.rsplit_once('-').map(|(stem, _)| stem))
            .unwrap_or("<target>")
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::hint;

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
}
