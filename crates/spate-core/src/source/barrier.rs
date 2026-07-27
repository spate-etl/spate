//! Drain barrier: revocation and shutdown share one choreography.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    done: Mutex<bool>,
    cv: Condvar,
}

/// A countdown barrier the controller waits on while pipeline threads
/// drain. Created with the number of arriving parties (the threads that own
/// the lanes being stopped); each thread calls [`DrainBarrier::arrive`]
/// exactly once after it has stopped the lanes and flushed its in-flight
/// records; the controller's [`DrainBarrier::wait`] returns `true` once all
/// parties arrived, or `false` on timeout (drain deadline exceeded — the
/// caller proceeds and unflushed data replays after restart; at-least-once
/// holds either way).
#[derive(Clone, Debug)]
pub struct DrainBarrier {
    inner: Arc<Inner>,
}

impl DrainBarrier {
    /// A barrier expecting `parties` arrivals. `parties == 0` is complete
    /// immediately.
    #[must_use]
    pub fn new(parties: usize) -> Self {
        DrainBarrier {
            inner: Arc::new(Inner {
                remaining: AtomicUsize::new(parties),
                done: Mutex::new(parties == 0),
                cv: Condvar::new(),
            }),
        }
    }

    /// Record this party's drain as complete. Idempotence is the caller's
    /// responsibility: call exactly once per party.
    pub fn arrive(&self) {
        let prev = self.inner.remaining.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "more arrivals than barrier parties");
        if prev == 1 {
            let mut done = self.inner.done.lock().expect("barrier lock");
            *done = true;
            self.inner.cv.notify_all();
        }
    }

    /// Parties still outstanding.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.inner.remaining.load(Ordering::Acquire)
    }

    /// Block until every party arrived or `timeout` elapsed. Returns
    /// whether the drain completed.
    #[must_use]
    pub fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut done = self.inner.done.lock().expect("barrier lock");
        while !*done {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _timeout) = self
                .inner
                .cv
                .wait_timeout(done, deadline - now)
                .expect("barrier lock");
            done = guard;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completes_when_all_arrive() {
        let barrier = DrainBarrier::new(2);
        let b2 = barrier.clone();
        let t = std::thread::spawn(move || {
            b2.arrive();
            b2.arrive();
        });
        assert!(barrier.wait(Duration::from_secs(5)));
        assert_eq!(barrier.remaining(), 0);
        t.join().unwrap();
    }

    #[test]
    fn times_out_when_a_party_never_arrives() {
        let barrier = DrainBarrier::new(2);
        barrier.arrive();
        assert!(!barrier.wait(Duration::from_millis(50)));
        assert_eq!(barrier.remaining(), 1);
    }

    #[test]
    fn zero_parties_is_immediately_complete() {
        let barrier = DrainBarrier::new(0);
        assert!(barrier.wait(Duration::ZERO));
    }

    #[test]
    fn wait_after_completion_returns_immediately() {
        let barrier = DrainBarrier::new(1);
        barrier.arrive();
        assert!(barrier.wait(Duration::ZERO));
        assert!(barrier.wait(Duration::from_secs(1)));
    }
}
