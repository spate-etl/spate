//! Re-running one test of the current test binary in a child process.

use std::process::Command;

/// Runs the test `name`, given as its full libtest path, in a new process of
/// the current test binary, and panics unless it ran and passed.
///
/// `configure` sets the child's environment before it starts. The call blocks
/// until the child exits, so an async test serving an endpoint the child
/// connects to calls it inside `tokio::task::block_in_place`.
///
/// # Panics
///
/// Panics when the child cannot be spawned, fails, or runs no test. The
/// message carries the command and the child's output.
#[track_caller]
pub fn run_in_child(name: &str, configure: impl FnOnce(&mut Command) -> &mut Command) {
    let exe = std::env::current_exe().expect("the test binary's path");
    let mut command = Command::new(exe);
    command.args(["--exact", name]);
    configure(&mut command);
    let out = command
        .output()
        .unwrap_or_else(|e| panic!("spawn {command:?}: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout);
    // A filter that matches nothing also exits 0.
    assert!(
        out.status.success() && stdout.contains("test result: ok. 1 passed;"),
        "{command:?}\n{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHILD: &str = "SPATE_TEST_SUPPORT_CHILD";

    /// The named test runs in the child with the environment `configure` set.
    #[test]
    fn the_named_test_runs_in_the_child() {
        if let Some(value) = std::env::var_os(CHILD) {
            assert_eq!(value, "set");
            return;
        }
        run_in_child("child::tests::the_named_test_runs_in_the_child", |child| {
            child.env(CHILD, "set")
        });
    }

    /// A name that matches no test fails, although the child exits 0.
    #[test]
    #[should_panic(expected = "--exact")]
    fn a_name_matching_no_test_fails() {
        run_in_child("child::tests::no_such_test", |child| child);
    }
}
