//! Repository automation for this workspace.
//!
//! `cargo xtask` is an alias for this binary, declared in `.cargo/config.toml`.
//! `cargo xtask help` lists the commands.

// A command-line tool writes to stdout and stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod ci;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

const HELP: &str = "\
spate repository automation

Usage:
  cargo xtask <command> [options]

Commands:
  ci-changes    Decide which CI jobs a change needs

Options for ci-changes:
  --classify-paths FILE   Classify a NUL-separated path list instead of a diff
  -h, --help              Show this message
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("ci-changes") => match ci::changes(&args[1..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("::error::xtask: {e}");
                ExitCode::FAILURE
            }
        },
        Some("-h" | "--help" | "help") | None => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("xtask: unknown command '{other}'\n\n{HELP}");
            ExitCode::FAILURE
        }
    }
}

fn repo_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask/ has no parent".to_string())
}
