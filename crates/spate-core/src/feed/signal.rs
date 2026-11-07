//! [`ReaderSignal`]: the reader-side state a [`Feed`](super::Feed) keeps for
//! its reader.

use super::{FeedError, FeedErrorKind, FeedWaker};

/// A feed's reader waker, its fatal latch and a one-shot retryable error.
///
/// It takes no lock of its own: a feed keeps it in the state its own lock
/// guards, so a latch and the close or publishes it rejects change together.
/// Every method that wakes the reader does so through
/// [`FeedWaker::wake`], which never blocks, so it is safe under that lock.
///
/// # Examples
///
/// ```
/// use spate_core::feed::{FeedErrorKind, ReaderSignal, wake_channel};
/// use std::time::Duration;
///
/// let mut signal = ReaderSignal::default();
/// let (waker, park) = wake_channel();
/// signal.set_waker(waker, false);
/// let (error, first) = signal.latch("the queue was deleted");
/// assert!(first);
/// assert_eq!(error.kind, FeedErrorKind::Fatal);
/// assert!(park.park(Duration::ZERO));
/// assert_eq!(signal.check(), Err(error));
/// ```
#[derive(Debug, Default)]
pub struct ReaderSignal {
    waker: Option<FeedWaker>,
    fatal: Option<FeedError>,
    retryable: Option<FeedError>,
}

impl ReaderSignal {
    /// Hold `waker` in place of any earlier one, and wake it at once when
    /// `ready`, so a unit that arrived before the waker still ends a park.
    pub fn set_waker(&mut self, waker: FeedWaker, ready: bool) {
        if ready {
            waker.wake();
        }
        self.waker = Some(waker);
    }

    /// Wake the reader, if a waker is set.
    pub fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake();
        }
    }

    /// Latch a [`Fatal`](FeedErrorKind::Fatal) error built from `reason`,
    /// and wake the reader.
    ///
    /// The first latch wins: a later call keeps the first error and drops its
    /// own `reason`. Returns the latched error, and whether this call latched
    /// it, so the feed logs its stop once.
    pub fn latch(&mut self, reason: &str) -> (FeedError, bool) {
        let first = self.fatal.is_none();
        let error = self
            .fatal
            .get_or_insert_with(|| FeedError::new(FeedErrorKind::Fatal, reason))
            .clone();
        self.wake();
        (error, first)
    }

    /// The latched error, if any.
    #[must_use]
    pub fn latched(&self) -> Option<&FeedError> {
        self.fatal.as_ref()
    }

    /// Hold a [`Retryable`](FeedErrorKind::Retryable) error built from
    /// `reason` for the next [`check`](Self::check), in place of any one not
    /// yet returned, and wake the reader.
    pub fn fail_soon(&mut self, reason: String) {
        self.retryable = Some(FeedError::new(FeedErrorKind::Retryable, reason));
        self.wake();
    }

    /// The guard at the top of a feed's `receive`: the latched error on every
    /// call, otherwise the one-shot error once.
    ///
    /// # Errors
    ///
    /// The latched [`Fatal`](FeedErrorKind::Fatal) error while one is held,
    /// then the error [`fail_soon`](Self::fail_soon) set, which this call
    /// takes.
    pub fn check(&mut self) -> Result<(), FeedError> {
        if let Some(fatal) = &self.fatal {
            return Err(fatal.clone());
        }
        self.retryable.take().map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::wake_channel;
    use std::time::Duration;

    /// Pins that `set_waker` wakes at once only when units are ready.
    #[test]
    fn set_waker_wakes_at_once_only_when_ready() {
        let mut signal = ReaderSignal::default();
        let (idle, idle_park) = wake_channel();
        signal.set_waker(idle, false);
        assert!(!idle_park.park(Duration::ZERO), "woke with nothing ready");
        let (ready, ready_park) = wake_channel();
        signal.set_waker(ready, true);
        assert!(ready_park.park(Duration::ZERO), "missed the ready units");
    }

    /// Pins that a later `set_waker` replaces the waker `wake` signals.
    #[test]
    fn a_later_waker_replaces_the_earlier_one() {
        let mut signal = ReaderSignal::default();
        let (first, first_park) = wake_channel();
        let (second, second_park) = wake_channel();
        signal.set_waker(first, false);
        signal.set_waker(second, false);
        signal.wake();
        assert!(!first_park.park(Duration::ZERO), "woke the replaced waker");
        assert!(second_park.park(Duration::ZERO), "missed the current waker");
    }

    /// Pins that the first latch wins, reports itself first, and wakes the
    /// reader, and that a later latch keeps the first error.
    #[test]
    fn the_first_latch_wins_and_wakes_the_reader() {
        let mut signal = ReaderSignal::default();
        let (waker, park) = wake_channel();
        signal.set_waker(waker, false);
        let (error, first) = signal.latch("first");
        assert!(first);
        assert_eq!(error, FeedError::new(FeedErrorKind::Fatal, "first"));
        assert!(park.park(Duration::ZERO), "the latch did not wake");
        let (again, first) = signal.latch("second");
        assert!(!first);
        assert_eq!(again, error);
        assert_eq!(signal.latched(), Some(&error));
    }

    /// Pins that a one-shot error wakes the reader, is returned once, and
    /// that the newest one replaces an earlier one not yet returned.
    #[test]
    fn a_one_shot_error_is_returned_once() {
        let mut signal = ReaderSignal::default();
        let (waker, park) = wake_channel();
        signal.set_waker(waker, false);
        assert_eq!(signal.check(), Ok(()));
        signal.fail_soon("older".to_owned());
        signal.fail_soon("newer".to_owned());
        assert!(park.park(Duration::ZERO), "fail_soon did not wake");
        assert_eq!(
            signal.check(),
            Err(FeedError::new(FeedErrorKind::Retryable, "newer"))
        );
        assert_eq!(signal.check(), Ok(()));
    }

    /// Pins that a latched error comes before a one-shot error, on every
    /// call.
    #[test]
    fn a_latched_error_comes_before_a_one_shot_error() {
        let mut signal = ReaderSignal::default();
        signal.fail_soon("transient".to_owned());
        let (error, _) = signal.latch("stopped");
        assert_eq!(signal.check(), Err(error.clone()));
        assert_eq!(signal.check(), Err(error));
        assert_eq!(signal.latched().map(|e| e.kind), Some(FeedErrorKind::Fatal));
    }
}
