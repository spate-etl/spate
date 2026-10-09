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
    /// The harness killed it.
    scheduled: bool,
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
            scheduled: false,
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

    /// The pid of `instance`'s latest process, when it is still running.
    ///
    /// # Errors
    ///
    /// Fails when the child's status cannot be read.
    pub fn live(&mut self, instance: &str) -> std::io::Result<Option<u32>> {
        let Some(worker) = self.workers.iter_mut().rfind(|w| w.instance == instance) else {
            return Ok(None);
        };
        if worker.exit.is_none() {
            worker.exit = worker.child.try_wait()?;
        }
        Ok(worker.exit.is_none().then(|| worker.child.id()))
    }

    /// Sends SIGKILL to `instance`'s latest process and reaps it, recording
    /// the exit as scheduled. Does nothing when that process has exited.
    ///
    /// # Errors
    ///
    /// Fails when the signal cannot be sent or the child cannot be reaped.
    pub fn kill(&mut self, instance: &str) -> std::io::Result<()> {
        if self.live(instance)?.is_none() {
            return Ok(());
        }
        let worker = self
            .workers
            .iter_mut()
            .rfind(|w| w.instance == instance)
            .expect("a live worker");
        worker.child.kill()?;
        worker.exit = Some(worker.child.wait()?);
        worker.scheduled = true;
        Ok(())
    }

    /// Sends SIGSTOP to `instance`'s latest process and waits up to
    /// `confirm` for it to report stopped. Returns its pid once it has, and
    /// `None` when that process has exited.
    ///
    /// # Errors
    ///
    /// Fails when the signal cannot be sent, the child's status cannot be
    /// read, or the stop is not confirmed in time.
    pub fn stop(&mut self, instance: &str, confirm: Duration) -> std::io::Result<Option<u32>> {
        let Some(pid) = self.live(instance)? else {
            return Ok(None);
        };
        signal(pid, libc::SIGSTOP)?;
        if self.confirm_stopped(instance, confirm)? {
            return Ok(Some(pid));
        }
        if self.live(instance)?.is_none() {
            return Ok(None);
        }
        Err(std::io::Error::other(format!(
            "{instance} (pid {pid}) did not report stopped within {confirm:?}"
        )))
    }

    /// Waits up to `deadline` for `instance`'s latest process to report
    /// stopped, and returns whether it did. An exited process never does, and
    /// stays waitable.
    ///
    /// # Errors
    ///
    /// Fails when the child's status cannot be read.
    pub fn confirm_stopped(&mut self, instance: &str, deadline: Duration) -> std::io::Result<bool> {
        let until = Instant::now() + deadline;
        loop {
            let Some(pid) = self.live(instance)? else {
                return Ok(false);
            };
            if stopped(pid) {
                return Ok(true);
            }
            if Instant::now() >= until {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Sends SIGCONT to `instance`'s latest process when it is `pid` and has
    /// not exited, and returns whether it did.
    ///
    /// # Errors
    ///
    /// Fails when the signal cannot be sent or the child's status cannot be
    /// read.
    pub fn resume(&mut self, instance: &str, pid: u32) -> std::io::Result<bool> {
        if self.live(instance)? != Some(pid) {
            return Ok(false);
        }
        signal(pid, libc::SIGCONT)?;
        Ok(true)
    }

    /// The signal that ended `instance`'s latest process, as of the last
    /// status read.
    #[must_use]
    pub fn signal(&self, instance: &str) -> Option<i32> {
        self.workers
            .iter()
            .rfind(|w| w.instance == instance)
            .and_then(|w| w.exit)
            .and_then(|s| s.signal())
    }

    /// Records `instance`'s latest process as ended by the schedule.
    pub fn mark_scheduled(&mut self, instance: &str) {
        if let Some(worker) = self.workers.iter_mut().rfind(|w| w.instance == instance) {
            worker.scheduled = true;
        }
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
                scheduled: w.scheduled,
            })
            .collect()
    }
}

fn signal(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    let pid = libc::pid_t::try_from(pid).map_err(std::io::Error::other)?;
    // SAFETY: `kill` takes no pointers; `pid` is an unreaped child.
    if unsafe { libc::kill(pid, signal) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Whether the unreaped child `pid` is stopped.
pub(crate) fn stopped(pid: u32) -> bool {
    // SAFETY: `siginfo_t` is plain data, valid when zeroed.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // `WNOWAIT` leaves the child waitable, so `Child::try_wait` still reads
    // its exit. Do not replace this with `waitpid(WUNTRACED)`, which reaps a
    // child that has exited.
    // SAFETY: `info` is a valid, writable `siginfo_t`.
    let r = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            &raw mut info,
            libc::WSTOPPED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    r == 0 && info.si_code == libc::CLD_STOPPED
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

    /// A kill ends the instance's latest process, reaps it and records the
    /// exit as scheduled; an earlier process of the instance is left alone.
    #[test]
    fn kill_ends_the_latest_process_as_scheduled() {
        let mut workers = Workers::default();
        let first = sleeper(&mut workers);
        let second = sleeper(&mut workers);
        assert_eq!(
            workers.live("w0").unwrap(),
            Some(u32::try_from(second).unwrap())
        );
        workers.kill("w0").unwrap();
        assert_eq!(workers.live("w0").unwrap(), None);
        // SAFETY: `kill` takes no pointers; signal 0 only checks the pid.
        assert_eq!(unsafe { libc::kill(first, 0) }, 0, "the first still runs");
        let exits = workers.exits();
        assert_eq!(
            (exits[1].signal, exits[1].scheduled),
            (Some(libc::SIGKILL), true)
        );
        assert!(!exits[0].scheduled);
        workers.kill("w9").unwrap();
    }

    /// A running child is not reported stopped, a stop returns once the
    /// child is stopped, and a resume goes only to the pid it names; a child
    /// that has exited reports no stop and keeps its exit status for
    /// `try_wait`.
    #[test]
    fn stop_confirm_leaves_an_exited_child_waitable() {
        let mut workers = Workers::default();
        let pid = sleeper(&mut workers);
        let pid = u32::try_from(pid).unwrap();
        let confirm = Duration::from_secs(10);
        assert!(!workers.confirm_stopped("w0", POLL).unwrap(), "running");
        assert_eq!(workers.stop("w0", confirm).unwrap(), Some(pid));
        assert!(stopped(pid), "stopped when `stop` returns");
        assert!(workers.resume("w0", pid).unwrap());
        assert!(!workers.resume("w0", pid + 1).unwrap(), "another pid");

        let dir = tempfile::tempdir().unwrap();
        let stderr = File::create(dir.path().join("w1-1.stderr")).unwrap();
        let exited = workers
            .spawn(
                "w1",
                Path::new("sh"),
                &["-c".as_ref(), "exit 7".as_ref()],
                stderr,
            )
            .unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        while !exited_unreaped(exited) {
            assert!(Instant::now() < until, "the child never exited");
            std::thread::sleep(POLL);
        }
        assert!(!stopped(exited));
        workers.try_wait().unwrap();
        assert_eq!(workers.exits()[1].code, Some(7));
        assert!(!workers.confirm_stopped("w1", Duration::ZERO).unwrap());
    }

    /// Whether the child `pid` has exited, leaving it unreaped.
    fn exited_unreaped(pid: u32) -> bool {
        // SAFETY: `siginfo_t` is plain data, valid when zeroed.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is a valid, writable `siginfo_t`.
        let r = unsafe {
            libc::waitid(
                libc::P_PID,
                pid,
                &raw mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        r == 0 && info.si_code == libc::CLD_EXITED
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
