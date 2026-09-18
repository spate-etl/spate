//! The libFuzzer targets in `fuzz/`, which build only on nightly.

use std::path::Path;

use clap::Subcommand;

use crate::run::{self, Error, Outcome, Step};

#[derive(Subcommand)]
pub(crate) enum Fuzz {
    /// Install cargo-fuzz
    Install,

    /// Build every fuzz target
    Build {
        #[command(flatten)]
        toolchain: Toolchain,
    },

    /// Fuzz one target
    Run {
        #[arg(value_name = "TARGET")]
        target: String,
        /// Seconds to run for
        #[arg(long, default_value_t = 60)]
        secs: u32,
        #[command(flatten)]
        toolchain: Toolchain,
    },
}

/// A dated channel and the bare one are different toolchains to rustup, so a
/// pinned build has to pass the name it installed.
#[derive(clap::Args)]
pub(crate) struct Toolchain {
    #[arg(long, default_value = "nightly", value_name = "CHANNEL")]
    nightly: String,
}

pub(crate) fn dispatch(root: &Path, explain: bool, cmd: Fuzz) -> Outcome {
    match cmd {
        Fuzz::Install => run::run(
            root,
            explain,
            &Step::new("cargo", ["install", "cargo-fuzz", "--locked"]),
        ),
        Fuzz::Build { toolchain } => run::run(
            root,
            explain,
            &Step::new("cargo", [&format!("+{}", toolchain.nightly)]).args(["fuzz", "build"]),
        ),
        Fuzz::Run {
            target,
            secs,
            toolchain,
        } => {
            let local = format!("fuzz/target/corpus/{target}");
            let seeds = format!("fuzz/corpus/{target}");
            if explain {
                println!("mkdir -p {seeds} {local}");
            } else {
                for dir in [&seeds, &local] {
                    std::fs::create_dir_all(root.join(dir))
                        .map_err(|e| Error::msg(format!("{dir}: {e}")))?;
                }
            }
            // libFuzzer writes new finds to the first corpus path and reads the
            // rest. The first sits under the ignored `target/`, so a local run
            // leaves the committed seeds alone.
            run::run(
                root,
                explain,
                &Step::new("cargo", [&format!("+{}", toolchain.nightly)])
                    .args(["fuzz", "run", &target, &local, &seeds, "--"])
                    .arg(format!("-max_total_time={secs}")),
            )
        }
    }
}
