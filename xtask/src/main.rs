//! Repository automation for this workspace.
//!
//! `cargo xtask` is an alias for this binary, declared in `.cargo/config.toml`.
//! `cargo xtask --help` lists the commands.

// A command-line tool writes to stdout and stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod ci;
mod commands;
mod run;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;

use commands::Command;

#[derive(Parser)]
#[command(
    name = "cargo xtask",
    about = "spate repository automation",
    disable_help_subcommand = true,
    subcommand_required = true,
    arg_required_else_help = true
)]
pub(crate) struct Cli {
    /// Print each child process this would run, and run none of them.
    #[arg(long, global = true)]
    explain: bool,

    #[command(subcommand)]
    command: Command,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let root = match repo_root() {
        Ok(root) => root,
        Err(e) => {
            eprintln!("{}xtask: {e}", annotation());
            return ExitCode::FAILURE;
        }
    };
    match commands::dispatch(&root, cli.explain, cli.command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}xtask: {}", annotation(), e.message);
            let code = e.code.unwrap_or(1);
            ExitCode::from(u8::try_from(code).unwrap_or(1))
        }
    }
}

/// The `::error::` prefix a workflow annotation needs, empty off a runner.
fn annotation() -> &'static str {
    if std::env::var_os("GITHUB_ACTIONS").is_some() {
        "::error::"
    } else {
        ""
    }
}

fn repo_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask/ has no parent".to_string())
}
