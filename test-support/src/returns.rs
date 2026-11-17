//! A bounded wait on a call that may never return.

use std::sync::mpsc;
use std::time::Duration;

/// Runs `f` on a thread of its own and reports whether it returned within
/// `limit`. A call that does not return leaves its thread running.
#[expect(
    clippy::disallowed_methods,
    reason = "a thread that never returns is the failure; the caller fails instead of joining it"
)]
pub fn returns_within(limit: Duration, f: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(limit).is_ok()
}

/// Runs `f` within `limit` and hands `keep` back once it returned.
///
/// # Panics
///
/// When `f` does not return within `limit`, naming `what`. `keep` is left
/// undropped first: a value whose clients `f` still holds would wait on them
/// in its drop.
pub fn assert_returns<K>(
    keep: K,
    limit: Duration,
    what: &str,
    f: impl FnOnce() + Send + 'static,
) -> K {
    if !returns_within(limit, f) {
        std::mem::forget(keep);
        panic!("{what} did not return");
    }
    keep
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Bounds a hang only.
    const HANG: Duration = Duration::from_secs(60);

    /// Sets its flag when dropped.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A call that blocks on a channel nothing sends on, and the sender that
    /// keeps it blocked.
    fn never_returns() -> (mpsc::Sender<()>, impl FnOnce() + Send + 'static) {
        let (tx, rx) = mpsc::channel::<()>();
        (tx, move || {
            let _ = rx.recv();
        })
    }

    #[test]
    fn a_call_that_returns_is_reported_as_returned() {
        assert!(returns_within(HANG, || {}));
    }

    #[test]
    fn a_call_that_blocks_past_the_limit_is_reported_as_not_returned() {
        let (_held, f) = never_returns();
        assert!(!returns_within(Duration::ZERO, f));
    }

    #[test]
    fn assert_returns_hands_back_what_it_keeps() {
        let dropped = Arc::new(AtomicBool::new(false));
        let kept = assert_returns(Dropped(Arc::clone(&dropped)), HANG, "a call", || {});
        assert!(
            !dropped.load(Ordering::SeqCst),
            "dropped before it was handed back"
        );
        drop(kept);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn assert_returns_forgets_what_it_keeps_on_a_hang() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (_held, f) = never_returns();
        let keep = Dropped(Arc::clone(&dropped));
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            assert_returns(keep, Duration::ZERO, "the close", f);
        }))
        .unwrap_err();
        assert_eq!(
            panicked.downcast_ref::<String>().map(String::as_str),
            Some("the close did not return")
        );
        assert!(
            !dropped.load(Ordering::SeqCst),
            "a hung call's value was dropped"
        );
    }
}
