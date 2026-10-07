//! One fault-run worker: reads the JSON config named by its only argument,
//! runs the coordinated pipeline, and exits 0 when it completes, 3 when a
//! journal line cannot be written, and 2 otherwise, with the reason on stderr.

#![allow(clippy::print_stderr)]

use std::process::ExitCode;

use spate_faults::worker::{self, WorkerConfig};

fn main() -> ExitCode {
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("spate-faults-worker: {e}");
            ExitCode::from(2)
        }
    }
}

fn start() -> Result<(), String> {
    let path = std::path::PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("usage: spate-faults-worker <config.json>")?,
    );
    let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let config: WorkerConfig = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    worker::run(&config)
}
