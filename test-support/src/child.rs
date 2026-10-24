//! Re-running one test of the current test binary in a child process.

use std::io::{BufRead as _, BufReader};
use std::process::{Child, Command, ExitStatus};
use std::sync::mpsc;

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

/// A test of the current test binary running in a child process, which the
/// caller kills as a crash would end it. Dropping it kills the child too.
#[derive(Debug)]
pub struct ChildTest {
    child: Child,
}

impl ChildTest {
    /// Starts the test `name`, given as its full libtest path, in a new
    /// process of the current test binary, and returns once the child has
    /// started it.
    ///
    /// `configure` sets the child's arguments and environment before it
    /// starts; an `#[ignore]`d test needs `--ignored`. The child's output is
    /// discarded.
    ///
    /// # Panics
    ///
    /// Panics when the child cannot be spawned, or when `name` does not
    /// select exactly one test in it.
    #[track_caller]
    pub fn spawn(name: &str, configure: impl FnOnce(&mut Command) -> &mut Command) -> ChildTest {
        let exe = std::env::current_exe().expect("the test binary's path");
        let mut command = Command::new(exe);
        command
            .args(["--exact", name, "--nocapture"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        configure(&mut command);
        let mut child = command
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {command:?}: {e}"));
        let stdout = child.stdout.take().expect("the child's stdout");
        let (tx, rx) = mpsc::channel();
        // libtest prints `running N tests` before it starts any. The rest of
        // the output is read and dropped, so a full pipe never blocks the
        // child.
        std::thread::spawn(move || {
            let mut tx = Some(tx);
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(count) = line.strip_prefix("running ")
                    && let Some(tx) = tx.take()
                {
                    let _ = tx.send(count.split(' ').next().unwrap_or_default().to_owned());
                }
            }
        });
        match rx.recv() {
            Ok(count) if count == "1" => ChildTest { child },
            // A child running no test exits by itself, so it is waited for.
            // A kill could land while an instrumented build writes its
            // profile at exit, and the truncated file fails the coverage
            // merge.
            Ok(count) if count == "0" => {
                let status = child.wait().expect("reap the child");
                panic!("{command:?} matched 0 tests, not 1, and exited with {status}")
            }
            Ok(count) => {
                drop(ChildTest { child });
                panic!("{command:?} matched {count} tests, not 1")
            }
            Err(_) => {
                let status = child.wait().expect("reap the child");
                panic!("{command:?} exited with {status} before it started a test")
            }
        }
    }

    /// Whether the child has not exited yet.
    ///
    /// # Panics
    ///
    /// Panics when the child's state cannot be read.
    pub fn running(&mut self) -> bool {
        self.child.try_wait().expect("the child's state").is_none()
    }

    /// Kills the child with `SIGKILL` on Unix, so nothing in it runs again:
    /// no destructor, no exit handler, no buffered write. Returns its exit
    /// status once the system has reaped it.
    ///
    /// # Panics
    ///
    /// Panics when the child exited before the kill, or cannot be reaped.
    pub fn kill(mut self) -> ExitStatus {
        assert!(self.running(), "the child exited before it was killed");
        self.child.kill().expect("kill the child");
        self.child.wait().expect("reap the child")
    }
}

impl Drop for ChildTest {
    fn drop(&mut self) {
        // Already reaped after `kill`; otherwise a test that failed first
        // leaves no child behind.
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
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

    /// Waits for ever when `SPATE_TEST_SUPPORT_CHILD` is set. Runs only as
    /// the child of `a_killed_child_ends_by_its_signal`.
    #[test]
    fn a_child_that_waits_to_be_killed() {
        if std::env::var_os(CHILD).is_none() {
            return;
        }
        loop {
            std::thread::park();
        }
    }

    /// A child that would never exit on its own is running, and a kill ends
    /// it by `SIGKILL` on Unix.
    #[test]
    fn a_killed_child_ends_by_its_signal() {
        let mut child = ChildTest::spawn("child::tests::a_child_that_waits_to_be_killed", |c| {
            c.env(CHILD, "wait")
        });
        assert!(child.running());
        let status = child.kill();
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9), "{status:?}");
        }
    }

    /// A name that matches no test fails, although the child exits 0.
    #[test]
    #[should_panic(expected = "--exact")]
    fn a_name_matching_no_test_fails() {
        run_in_child("child::tests::no_such_test", |child| child);
    }

    /// A spawned name that matches no test fails, and the child is reaped
    /// after it exits 0 by itself, never killed. Regression for #1261.
    #[test]
    #[should_panic(expected = "matched 0 tests, not 1, and exited with exit status: 0")]
    fn a_spawned_name_matching_no_test_fails() {
        let _child = ChildTest::spawn("child::tests::no_such_test", |child| child);
    }

    /// A spawned filter that matches two tests fails, and the child, which
    /// could run for ever, is killed.
    #[test]
    fn a_spawned_filter_matching_two_tests_kills_the_child() {
        use std::io::Read as _;
        let (mut reader, writer) = std::io::pipe().expect("a pipe");
        let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            ChildTest::spawn("child::tests::a_child_that_waits_to_be_killed", |child| {
                child
                    .arg("child::tests::the_named_test_runs_in_the_child")
                    .env(CHILD, "wait")
                    .stderr(writer)
            })
        }));
        let message = *spawned
            .expect_err("two tests match")
            .downcast::<String>()
            .expect("a message");
        assert!(message.contains("matched 2 tests, not 1"), "{message}");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = reader.read_to_end(&mut Vec::new());
            let _ = tx.send(());
        });
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .expect("the child's stderr closes once it is killed");
    }
}
