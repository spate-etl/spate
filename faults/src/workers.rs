//! The guard that owns every worker process a run starts.

use std::fs::File;
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::outcome::WorkerExit;

/// How often [`Workers::wait`] polls for exits.
const POLL: Duration = Duration::from_millis(50);

/// One worker process.
#[derive(Debug)]
struct Worker {
    instance: String,
    child: Child,
    exit: Option<ExitStatus>,
}

/// Owns every worker process. On drop it kills and reaps each one still
/// running, so a stopped or wedged worker never outlives the run.
#[derive(Debug, Default)]
pub struct Workers {
    workers: Vec<Worker>,
}

impl Workers {
    /// Starts `program` with `args` as `instance`, with stderr to `stderr` and
    /// stdout discarded, and returns its pid.
    ///
    /// The child inherits the environment, except that `HTTP_PROXY` and
    /// `HTTPS_PROXY` are removed and `NO_PROXY` covers the loopback address.
    ///
    /// # Errors
    ///
    /// Fails when the process cannot be started.
    pub fn spawn(
        &mut self,
        instance: &str,
        program: &Path,
        args: &[&std::ffi::OsStr],
        stderr: File,
    ) -> std::io::Result<u32> {
        let child = Command::new(program)
            .args(args)
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()?;
        let pid = child.id();
        self.workers.push(Worker {
            instance: instance.to_owned(),
            child,
            exit: None,
        });
        Ok(pid)
    }

    /// Records the exit of every worker that has exited, and returns whether
    /// all of them have.
    ///
    /// # Errors
    ///
    /// Fails when a child's status cannot be read.
    pub fn try_wait(&mut self) -> std::io::Result<bool> {
        let mut all = true;
        for worker in &mut self.workers {
            if worker.exit.is_none() {
                worker.exit = worker.child.try_wait()?;
            }
            all &= worker.exit.is_some();
        }
        Ok(all)
    }

    /// Waits up to `deadline` for every worker to exit and returns whether
    /// one was still running at the deadline. It never kills a worker.
    ///
    /// # Errors
    ///
    /// Fails when a child's status cannot be read.
    pub fn wait(&mut self, deadline: Duration) -> std::io::Result<bool> {
        let until = Instant::now() + deadline;
        loop {
            if self.try_wait()? {
                return Ok(false);
            }
            if Instant::now() >= until {
                return Ok(true);
            }
            std::thread::sleep(POLL);
        }
    }

    /// How each worker ended, as of the last [`Workers::try_wait`]; a worker
    /// still running has neither a code nor a signal.
    #[must_use]
    pub fn exits(&self) -> Vec<WorkerExit> {
        self.workers
            .iter()
            .map(|w| WorkerExit {
                instance: w.instance.clone(),
                pid: w.child.id(),
                code: w.exit.and_then(|s| s.code()),
                signal: w.exit.and_then(|s| s.signal()),
                scheduled: false,
            })
            .collect()
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            if matches!(worker.child.try_wait(), Ok(None)) {
                let _ = worker.child.kill();
                let _ = worker.child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleeper(workers: &mut Workers) -> libc::pid_t {
        let dir = tempfile::tempdir().unwrap();
        let stderr = File::create(dir.path().join("w0-1.stderr")).unwrap();
        let pid = workers
            .spawn("w0", Path::new("sleep"), &["3600".as_ref()], stderr)
            .unwrap();
        libc::pid_t::try_from(pid).unwrap()
    }

    /// Dropping the guard kills and reaps a stopped child.
    #[test]
    fn guard_kills_and_reaps_a_stopped_child_on_drop() {
        let mut workers = Workers::default();
        let pid = sleeper(&mut workers);
        // SAFETY: `kill` takes no pointers; `pid` is our unreaped child.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(workers);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the guard's drop returned");
        // SAFETY: as above; signal 0 only checks that the pid exists.
        let alive = unsafe { libc::kill(pid, 0) };
        assert_eq!(alive, -1, "the child was reaped");
    }

    /// A wait that reaches its deadline reports it and leaves the worker
    /// running, without panicking.
    #[test]
    fn wait_for_exit_returns_timed_out_without_panicking() {
        let mut workers = Workers::default();
        let pid = sleeper(&mut workers);
        assert!(workers.wait(Duration::from_millis(200)).unwrap());
        assert_eq!(workers.exits()[0].code, None);
        // SAFETY: `kill` takes no pointers; signal 0 only checks the pid.
        assert_eq!(unsafe { libc::kill(pid, 0) }, 0, "still running");
    }

    /// A worker's exit code is recorded once it exits.
    #[test]
    fn exits_record_the_code() {
        let mut workers = Workers::default();
        let dir = tempfile::tempdir().unwrap();
        let stderr = File::create(dir.path().join("w1-1.stderr")).unwrap();
        workers
            .spawn(
                "w1",
                Path::new("sh"),
                &["-c".as_ref(), "exit 2".as_ref()],
                stderr,
            )
            .unwrap();
        assert!(!workers.wait(Duration::from_secs(30)).unwrap());
        let exit = &workers.exits()[0];
        assert_eq!(
            (exit.instance.as_str(), exit.code, exit.signal),
            ("w1", Some(2), None)
        );
    }

    /// A worker ended by a signal records the signal and no code.
    #[test]
    fn exits_record_the_signal() {
        let mut workers = Workers::default();
        let pid = sleeper(&mut workers);
        // SAFETY: `kill` takes no pointers; `pid` is our unreaped child.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        assert!(!workers.wait(Duration::from_secs(30)).unwrap());
        let exit = &workers.exits()[0];
        assert_eq!((exit.code, exit.signal), (None, Some(libc::SIGKILL)));
    }

    /// A worker's environment carries `NO_PROXY` covering the loopback address.
    #[test]
    fn spawn_sets_no_proxy_for_loopback() {
        let mut workers = Workers::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w2-1.stderr");
        workers
            .spawn(
                "w2",
                Path::new("sh"),
                &["-c".as_ref(), "printf %s \"$NO_PROXY\" >&2".as_ref()],
                File::create(&path).unwrap(),
            )
            .unwrap();
        assert!(!workers.wait(Duration::from_secs(30)).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "127.0.0.1,localhost"
        );
    }
}
