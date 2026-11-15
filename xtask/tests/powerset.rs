//! `cargo xtask powerset`, run the way the weekly workflow runs it.
//!
//! A fake `cargo` stands first on `PATH`. For `hack check` it lists 13,000
//! checks, which makes two shards at `PER_SHARD_CHECKS`, and it refuses any
//! other command, so the planner runs without a toolchain or a build.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

const CARGO: &str = "#!/bin/sh\n\
if [ \"$1\" = hack ] && [ \"$2\" = check ]; then\n\
  echo 'info: --no-dev-deps modifies real Cargo.toml while cargo-hack is running'\n\
  i=0\n\
  while [ $i -lt 13000 ]; do echo \"cargo check --features f$i\"; i=$((i+1)); done\n\
  exit 0\n\
fi\n\
echo \"unexpected: $*\" >&2\n\
exit 1\n";

/// A directory holding the fake `cargo` in `bin`.
struct Shim(PathBuf);

impl Shim {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-powerset-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(dir.join("bin")).unwrap();
        let path = dir.join("bin").join("cargo");
        std::fs::write(&path, CARGO).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        Self(dir)
    }

    fn bin(&self) -> PathBuf {
        self.0.join("bin")
    }
}

/// Runs the built `spate-xtask` with the fake `cargo` first on `PATH`, and with
/// `GITHUB_OUTPUT` set to `output` when one is given.
fn xtask(shim: &Shim, args: &[&str], output: Option<&PathBuf>) -> Output {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![shim.bin()];
    paths.extend(std::env::split_paths(&path));
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command
        .args(args)
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env_remove("GITHUB_OUTPUT");
    if let Some(file) = output {
        command.env("GITHUB_OUTPUT", file);
    }
    command.output().unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The planner writes `shards=` to the step-output file, after what an earlier
/// step wrote there, and reports the count on stderr.
#[test]
fn the_planner_writes_the_key_the_workflow_reads() {
    let shim = Shim::new("output");
    let file = shim.0.join("step_output");
    std::fs::write(&file, "earlier=kept\n").unwrap();
    let out = xtask(&shim, &["powerset", "plan"], Some(&file));
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stderr(&out), "powerset: 13000 checks in 2 shards\n");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "earlier=kept\nshards=[\"1/2\",\"2/2\"]\n"
    );
}

/// Off a runner the list goes to stdout, and no file is touched.
#[test]
fn off_a_runner_the_planner_prints_the_list() {
    let shim = Shim::new("stdout");
    let out = xtask(&shim, &["powerset", "plan"], None);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "[\"1/2\",\"2/2\"]\n");
}

/// A leg checks the shard it names: the argument reaches cargo-hack as
/// `--partition`, so a leg cannot run the whole powerset.
#[test]
fn a_leg_checks_only_its_shard() {
    let shim = Shim::new("leg");
    let out = xtask(&shim, &["--explain", "powerset", "run", "2/11"], None);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let shown = String::from_utf8_lossy(&out.stdout);
    assert!(shown.trim_end().ends_with(" --partition 2/11"), "{shown}");
}

/// A shard outside `M/N` is refused before any check starts. The fake `cargo`
/// would list checks and succeed for `hack`, so a success here means the check
/// ran.
#[test]
fn a_shard_outside_the_list_is_refused_before_a_check_runs() {
    let shim = Shim::new("refused");
    for shard in ["0/2", "3/2", "1/0", "x"] {
        let out = xtask(&shim, &["powerset", "run", shard], None);
        assert_ne!(out.status.code(), Some(0), "{shard}");
        assert!(stderr(&out).contains("M/N"), "{shard}: {}", stderr(&out));
    }
}
