//! The example image, built from its Dockerfile and started once, so a change
//! that leaves the runtime image unable to load the binary fails the pull
//! request that made it.
//!
//! The example reads its config before it touches any server, so a missing
//! config file makes it exit 1 after the loader has started it. A loader
//! failure exits 127 instead, and the loader's line on stderr names the library
//! the runtime image lacks.

use std::path::Path;
use std::time::Duration;

use crate::run::{self, Error, Outcome, Step};

/// The Dockerfile the image is built from, relative to the repository root.
const DOCKERFILE: &str = "examples/docker/Dockerfile";

/// The tag the build and the start share.
const TAG: &str = "spate-example-image:xtask";

/// A config path that does not exist, so the example stops at its config and
/// never reaches a server.
const MISSING_CONFIG: &str = "SPATE_CONFIG=/nonexistent/spate-example-image.yaml";

/// How long the start may run. The example exits at once on a missing config,
/// so a run this long is hung, not slow.
const HANG: Duration = Duration::from_secs(120);

/// The status of an example that loaded and rejected its config. `main`
/// returned an error, which the Rust runtime reports as exit 1.
const CONFIG_REJECTED: i32 = 1;

/// Builds the image, then starts it and checks how the start ended.
pub(crate) fn run(root: &Path, explain: bool) -> Outcome {
    run::run(root, explain, &build())?;
    let start = start();
    if explain {
        println!("{}", start.display());
        return Ok(());
    }
    let done = run::complete_within(root, &start, HANG)?;
    verdict(done.completed.code, done.stopped)
}

fn build() -> Step<'static> {
    Step::new("docker", ["build", "-f", DOCKERFILE, "-t", TAG, "."])
}

fn start() -> Step<'static> {
    Step::new("docker", ["run", "--rm", "-e", MISSING_CONFIG, TAG])
}

/// Passes only when the example loaded and rejected its config. A run the bound
/// stopped, a loader failure, any other exit and a status from a signal all
/// fail, each with the status it ended on.
fn verdict(code: i32, stopped: bool) -> Outcome {
    if stopped {
        return Err(Error::msg(format!(
            "the example image ran past {HANG:?} and was stopped"
        )));
    }
    match code {
        CONFIG_REJECTED => Ok(()),
        126 | 127 => Err(Error::msg(format!(
            "the example image did not start (exit {code}): the binary could not be loaded, and the loader line above names the library the runtime image lacks"
        ))),
        _ => Err(Error::msg(format!(
            "the example image exited {code} for a missing config, not {CONFIG_REJECTED}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rejected_config_passes() {
        assert!(verdict(CONFIG_REJECTED, false).is_ok());
    }

    /// Every other end fails: a loader failure, an exec failure, a clean exit,
    /// a signal, and a run the bound stopped even when it reports 1.
    #[test]
    fn every_other_end_fails() {
        for code in [0, 2, 125, 126, 127, 130, 137] {
            assert!(verdict(code, false).is_err(), "exit {code} passed");
        }
        assert!(verdict(CONFIG_REJECTED, true).is_err());
    }

    /// A loader failure names its own status, so the report separates it from a
    /// config error.
    #[test]
    fn a_loader_failure_says_so() {
        let message = verdict(127, false).unwrap_err().message;
        assert!(message.contains("exit 127"), "{message}");
        assert!(message.contains("could not be loaded"), "{message}");
    }

    #[test]
    fn the_build_and_start_name_the_dockerfile_and_the_missing_config() {
        assert_eq!(build().args, ["build", "-f", DOCKERFILE, "-t", TAG, "."]);
        assert_eq!(start().args, ["run", "--rm", "-e", MISSING_CONFIG, TAG]);
    }
}
