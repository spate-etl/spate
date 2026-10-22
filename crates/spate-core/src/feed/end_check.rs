//! The end check: when a bounded job on a feed has read all of its work.
//!
//! [`EndCheck`] decides from the reads a source reports, and does no I/O
//! itself. The source asks it what to read next with
//! [`next`](EndCheck::next), starts that read, and reports the answer. It
//! holds no clock: every call takes the instant the source reads from its
//! own clock.

use super::{Depth, DepthOf, FeedError};
use crate::error::ErrorClass;
use std::time::{Duration, Instant};

/// The longest gap between two reads of the feed inside a settling window,
/// and between a read and its answer.
pub const END_CHECK_INTERVAL: Duration = Duration::from_secs(20);

/// The step between scheduled reads. Shorter than [`END_CHECK_INTERVAL`], so
/// a read taken on a poll that comes a little after its time still falls
/// within the bound.
const READ_EVERY: Duration = Duration::from_secs(15);

/// What a read of a bounded job's end marker found.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MarkerRead {
    /// The marker exists and names this lister run.
    Present(String),
    /// No marker exists.
    Absent,
    /// The read failed and may succeed later. A source ends its run itself
    /// on a read that cannot succeed.
    Failed,
}

/// The next step an [`EndCheck`] asks of its source.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EndAction {
    /// Start nothing. With an instant, call [`next`](EndCheck::next) again
    /// at or after it; with `None`, a read is outstanding and its answer
    /// comes first.
    Wait(Option<Instant>),
    /// Read the end marker and report it with
    /// [`marker_read`](EndCheck::marker_read).
    ReadMarker,
    /// Read the depth named and report it with
    /// [`depth_read`](EndCheck::depth_read).
    ReadDepth(DepthOf),
    /// Receive from the feed once and report whether it took a unit with
    /// [`received`](EndCheck::received).
    Receive,
    /// The job has ended.
    Ended(EndOutcome),
}

/// How a bounded job ended.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EndOutcome {
    /// The feed stayed empty over the window and its dead-letter destination
    /// holds nothing.
    Completed,
    /// The feed's dead-letter destination holds units, counted here.
    Stalled(Depth),
    /// A read of the feed's depth failed with an error that sending it again
    /// cannot clear, such as a missing permission.
    Failed(FeedError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Read {
    Marker,
    Depth(DepthOf),
    Receive,
}

#[derive(Debug)]
enum Phase {
    /// Waiting to open a window: read the marker on a windowed feed, the
    /// feed itself on an exact one.
    Opening {
        due: Instant,
    },
    /// A window opened by a read of `run`'s marker. It spans from `start`,
    /// its first read of the feed, unset until that read. The next read is
    /// due at `due`, and must be taken within [`END_CHECK_INTERVAL`] of
    /// `last`, the window's last read.
    Window {
        run: String,
        start: Option<Instant>,
        last: Instant,
        due: Instant,
    },
    /// The window has passed with every read empty; the marker is read again.
    Closing {
        run: String,
    },
    /// One receive that must take nothing.
    Receiving,
    /// The dead-letter destination's read, due at `due`.
    DeadLetters {
        due: Instant,
    },
    Ended(EndOutcome),
}

/// Decides when a bounded job on a feed has ended.
///
/// A windowed check opens a window on a read of the end marker. The window
/// spans from its first read of the feed. Inside it the feed is read every
/// 15 s, each read taken within [`END_CHECK_INTERVAL`] of the one before and
/// answered within it of its request, and every read must find the feed
/// empty. A read requested a window or more after the first that finds it
/// empty is followed by a second read of the marker: the same run
/// passes, another run opens a new window, and no marker restarts the
/// check. An exact check, for a feed whose counts are exact and whose lister
/// has finished, reads the feed once with no window or marker. Either way
/// one receive that takes nothing follows, and then the dead-letter
/// destination decides between [`Completed`](EndOutcome::Completed) and
/// [`Stalled`](EndOutcome::Stalled).
///
/// A non-empty read, a read that fails retryably, a read taken or answered
/// later than that bound, a marker read answered later than it included, or
/// a [`restart`](EndCheck::restart) sends a windowed check back to its marker
/// read, 15 s after its previous read at most. A dead-letter read that fails
/// retryably is retried 15 s later and ends nothing. A depth read that fails
/// with a [`Fatal`](ErrorClass::Fatal) class ends the check
/// [`Failed`](EndOutcome::Failed). The source calls `restart` whenever its reader holds
/// any work, so every read that counts was answered while it held none.
///
/// ```
/// use spate_core::feed::{Depth, DepthOf, EndAction, EndCheck, EndOutcome, MarkerRead};
/// use std::time::{Duration, Instant};
///
/// let start = Instant::now();
/// let mut check = EndCheck::windowed(Duration::from_secs(30), start);
/// assert_eq!(check.next(start), EndAction::ReadMarker);
/// check.marker_read(start, MarkerRead::Present("run-a".into()));
///
/// let empty = || Ok(Depth::new("queue", false));
/// for at in [0, 15, 30] {
///     let now = start + Duration::from_secs(at);
///     assert_eq!(check.next(now), EndAction::ReadDepth(DepthOf::Feed));
///     check.depth_read(now, empty());
/// }
/// let now = start + Duration::from_secs(30);
/// assert_eq!(check.next(now), EndAction::ReadMarker);
/// check.marker_read(now, MarkerRead::Present("run-a".into()));
/// assert_eq!(check.next(now), EndAction::Receive);
/// check.received(now, false);
/// assert_eq!(check.next(now), EndAction::ReadDepth(DepthOf::DeadLetters));
/// check.depth_read(now, empty());
/// assert_eq!(check.next(now), EndAction::Ended(EndOutcome::Completed));
/// ```
#[derive(Debug)]
pub struct EndCheck {
    /// `None` on an exact check.
    window: Option<Duration>,
    phase: Phase,
    /// The read outstanding, and when it was requested.
    pending: Option<(Read, Instant)>,
    /// When the last read of the feed or the marker was requested, so a
    /// restart waits 15 s from it.
    last_request: Option<Instant>,
    /// The run whose marker last opened a window, so a window reopened on
    /// the same run is not announced again.
    announced: Option<String>,
}

impl EndCheck {
    /// A check that ends a job once the feed stays empty over `window`
    /// between two reads of the same lister run's end marker. Its first read
    /// is due at `now`.
    #[must_use]
    pub fn windowed(window: Duration, now: Instant) -> EndCheck {
        EndCheck {
            window: Some(window),
            phase: Phase::Opening { due: now },
            pending: None,
            last_request: None,
            announced: None,
        }
    }

    /// A check for a feed whose counts are exact and whose lister has
    /// finished: one empty read of the feed passes, with no window and no
    /// marker. Its first read is due at `now`.
    #[must_use]
    pub fn exact(now: Instant) -> EndCheck {
        EndCheck {
            window: None,
            phase: Phase::Opening { due: now },
            pending: None,
            last_request: None,
            announced: None,
        }
    }

    /// What to do at `now`. A read returned here is outstanding until its
    /// answer is reported; until then `next` returns `Wait(None)`.
    pub fn next(&mut self, now: Instant) -> EndAction {
        if self.pending.is_some() {
            return EndAction::Wait(None);
        }
        if let Phase::Window { last, .. } = self.phase
            && now > last + END_CHECK_INTERVAL
        {
            tracing::info!(
                gap = ?now - last,
                "the feed went unread longer than the read interval; restarting the end check"
            );
            self.reopen(now);
        }
        let read = match &self.phase {
            Phase::Ended(outcome) => return EndAction::Ended(outcome.clone()),
            Phase::Opening { due } | Phase::Window { due, .. } | Phase::DeadLetters { due }
                if now < *due =>
            {
                return EndAction::Wait(Some(*due));
            }
            Phase::Opening { .. } if self.window.is_none() => Read::Depth(DepthOf::Feed),
            Phase::Opening { .. } | Phase::Closing { .. } => Read::Marker,
            Phase::Window { .. } => Read::Depth(DepthOf::Feed),
            Phase::Receiving => Read::Receive,
            Phase::DeadLetters { .. } => Read::Depth(DepthOf::DeadLetters),
        };
        self.pending = Some((read, now));
        if matches!(read, Read::Marker | Read::Depth(DepthOf::Feed)) {
            self.last_request = Some(now);
        }
        match read {
            Read::Marker => EndAction::ReadMarker,
            Read::Depth(of) => EndAction::ReadDepth(of),
            Read::Receive => EndAction::Receive,
        }
    }

    /// Report the end marker read that [`next`](Self::next) asked for. An
    /// answer nothing asked for is ignored.
    pub fn marker_read(&mut self, now: Instant, read: MarkerRead) {
        // The window and the next read count from the request, so an answer
        // the source notices late does not shift them.
        let Some(requested) = self.take_pending(Read::Marker) else {
            return;
        };
        if now > requested + END_CHECK_INTERVAL {
            tracing::info!(
                late = ?now - requested,
                "an end marker read answered after the read interval; restarting the end check"
            );
            self.reopen(now);
            return;
        }
        let window = self.window.unwrap_or_default();
        let closing = match &self.phase {
            Phase::Closing { run } => Some(run.clone()),
            _ => None,
        };
        match (read, closing) {
            (MarkerRead::Present(seen), Some(run)) if seen == run => {
                self.phase = Phase::Receiving;
            }
            (MarkerRead::Present(seen), closing) => {
                if closing.is_some() {
                    tracing::info!(
                        run = %seen,
                        "a newer lister run's end marker replaced the window's; \
                         opening a new settling window"
                    );
                } else if self.announced.as_deref() != Some(seen.as_str()) {
                    tracing::info!(
                        run = %seen,
                        window = ?window,
                        "end marker found; the job ends once the feed stays empty over \
                         the settling window"
                    );
                }
                self.announced = Some(seen.clone());
                self.phase = Phase::Window {
                    run: seen,
                    start: None,
                    last: requested,
                    due: now,
                };
            }
            (MarkerRead::Absent, closing) => {
                self.announced = None;
                if closing.is_some() {
                    tracing::info!(
                        "the end marker is gone at the settling window's end; waiting for a \
                         lister run's marker"
                    );
                }
                self.phase = Phase::Opening {
                    due: requested + READ_EVERY,
                };
            }
            (MarkerRead::Failed, _) => {
                tracing::warn!("reading the end marker failed; reading it again");
                self.phase = Phase::Opening {
                    due: requested + READ_EVERY,
                };
            }
        }
    }

    /// Report the depth read that [`next`](Self::next) asked for. An answer
    /// nothing asked for is ignored.
    pub fn depth_read(&mut self, now: Instant, read: Result<Depth, FeedError>) {
        let Some((Read::Depth(of), requested)) = self.pending else {
            return;
        };
        self.pending = None;
        let due = requested + READ_EVERY;
        match (of, read) {
            (_, Err(e)) if e.class() == ErrorClass::Fatal => {
                self.phase = Phase::Ended(EndOutcome::Failed(e));
            }
            (DepthOf::DeadLetters, Ok(depth)) if depth.is_empty() => {
                self.phase = Phase::Ended(EndOutcome::Completed);
            }
            (DepthOf::DeadLetters, Ok(depth)) => {
                self.phase = Phase::Ended(EndOutcome::Stalled(depth));
            }
            (DepthOf::DeadLetters, Err(e)) => {
                tracing::warn!(error = %e, "reading the dead-letter destination failed; reading it again");
                self.phase = Phase::DeadLetters { due };
            }
            (DepthOf::Feed, Err(e)) => {
                tracing::warn!(error = %e, "reading the feed's depth failed; restarting the end check");
                self.reopen(due);
            }
            (DepthOf::Feed, Ok(depth)) if !depth.is_empty() => {
                // Every interval while peers still read, so not at INFO.
                tracing::debug!(%depth, "the feed is not empty; restarting the end check");
                self.reopen(due);
            }
            (DepthOf::Feed, Ok(_))
                if self.window.is_some() && now > requested + END_CHECK_INTERVAL =>
            {
                tracing::info!(
                    late = ?now - requested,
                    "a depth read answered after the read interval; restarting the end check"
                );
                self.reopen(now);
            }
            (DepthOf::Feed, Ok(_)) => self.advance(requested),
        }
    }

    /// Report whether the receive that [`next`](Self::next) asked for took a
    /// unit. One that did restarts the check.
    pub fn received(&mut self, now: Instant, took: bool) {
        if self.take_pending(Read::Receive).is_none() {
            return;
        }
        if took {
            self.restart(now);
        } else {
            self.phase = Phase::DeadLetters { due: now };
        }
    }

    /// Send the check back to its opening read, because the reader holds
    /// work. A windowed check reads again 15 s after its last read; an exact
    /// one at once. A check that has ended stays ended.
    ///
    /// Drop every read outstanding when calling it. The check tells answers
    /// apart only by kind, so an answer reported once the next read of its
    /// kind has started counts as that read's.
    pub fn restart(&mut self, now: Instant) {
        if matches!(self.phase, Phase::Ended(_)) {
            return;
        }
        if !matches!(self.phase, Phase::Opening { .. }) {
            tracing::info!("the reader took work; restarting the end check");
        }
        self.pending = None;
        let due = match (self.window, self.last_request) {
            (Some(_), Some(last)) => (last + READ_EVERY).max(now),
            _ => now,
        };
        self.phase = Phase::Opening { due };
    }

    /// Whether the check has ended.
    #[must_use]
    pub fn outcome(&self) -> Option<&EndOutcome> {
        match &self.phase {
            Phase::Ended(outcome) => Some(outcome),
            _ => None,
        }
    }

    /// Take the outstanding read if it is `read`, returning when it was
    /// requested.
    fn take_pending(&mut self, read: Read) -> Option<Instant> {
        match self.pending {
            Some((pending, requested)) if pending == read => {
                self.pending = None;
                Some(requested)
            }
            _ => None,
        }
    }

    fn reopen(&mut self, due: Instant) {
        self.phase = Phase::Opening { due };
    }

    /// An empty, timely read of the feed, requested at `requested`.
    fn advance(&mut self, requested: Instant) {
        match (&self.phase, self.window) {
            (Phase::Opening { .. }, None) => self.phase = Phase::Receiving,
            (Phase::Window { run, start, .. }, Some(window)) => {
                // The window spans the feed's reads, so its first read starts
                // it. A window too long to add to an instant never closes.
                let start = start.unwrap_or(requested);
                let closes = start
                    .checked_add(window)
                    .is_some_and(|end| requested >= end);
                self.phase = if closes {
                    Phase::Closing { run: run.clone() }
                } else {
                    Phase::Window {
                        run: run.clone(),
                        start: Some(start),
                        last: requested,
                        due: requested + READ_EVERY,
                    }
                };
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::FeedErrorKind;

    const WINDOW: Duration = Duration::from_secs(60);

    fn at(start: Instant, secs: u64) -> Instant {
        start + Duration::from_secs(secs)
    }

    fn empty() -> Result<Depth, FeedError> {
        Ok(Depth::new("queue", true))
    }

    fn visible(n: u64) -> Result<Depth, FeedError> {
        let mut depth = Depth::new("queue", true);
        depth.visible = n;
        Ok(depth)
    }

    fn failed() -> Result<Depth, FeedError> {
        Err(FeedError::new(FeedErrorKind::Retryable, "throttled"))
    }

    fn present(run: &str) -> MarkerRead {
        MarkerRead::Present(run.into())
    }

    /// Opens a window on `run` at `start` and returns the check.
    fn opened(start: Instant, run: &str) -> EndCheck {
        let mut check = EndCheck::windowed(WINDOW, start);
        assert_eq!(check.next(start), EndAction::ReadMarker);
        check.marker_read(start, present(run));
        check
    }

    /// Answers every empty read of the feed due from `from` to `to`, in
    /// 15-second steps, and returns how many it answered.
    fn empty_reads(check: &mut EndCheck, start: Instant, from: u64, to: u64) -> usize {
        let mut reads = 0;
        for secs in (from..=to).step_by(15) {
            let now = at(start, secs);
            assert_eq!(
                check.next(now),
                EndAction::ReadDepth(DepthOf::Feed),
                "read at {secs}s"
            );
            check.depth_read(now, empty());
            reads += 1;
        }
        reads
    }

    /// Takes a check whose closing marker passed through the receive and
    /// the dead-letter read, answering the latter with `dead`.
    fn finish(check: &mut EndCheck, now: Instant, dead: Result<Depth, FeedError>) -> EndAction {
        assert_eq!(check.next(now), EndAction::Receive);
        check.received(now, false);
        assert_eq!(check.next(now), EndAction::ReadDepth(DepthOf::DeadLetters));
        check.depth_read(now, dead);
        check.next(now)
    }

    /// Pins that a window ends one window after its first read of the feed:
    /// five empty reads at 0, 15, 30, 45 and 60 s, then the closing marker
    /// read.
    #[test]
    fn a_window_ends_one_window_after_its_opening_marker_read() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        assert_eq!(empty_reads(&mut check, start, 0, 45), 4);
        assert_eq!(
            check.next(at(start, 59)),
            EndAction::Wait(Some(at(start, 60)))
        );
        empty_reads(&mut check, start, 60, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(
            finish(&mut check, at(start, 60), empty()),
            EndAction::Ended(EndOutcome::Completed)
        );
    }

    /// Pins that one non-empty read inside a window, as an approximate count
    /// can give between zeros, restarts it from a new marker read.
    #[test]
    fn a_nonzero_read_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 15);
        assert_eq!(
            check.next(at(start, 30)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 30), visible(1));
        assert_eq!(
            check.next(at(start, 44)),
            EndAction::Wait(Some(at(start, 45)))
        );
        assert_eq!(check.next(at(start, 45)), EndAction::ReadMarker);
        check.marker_read(at(start, 45), present("a"));
        empty_reads(&mut check, start, 45, 90);
        assert_eq!(
            check.next(at(start, 105)),
            EndAction::ReadDepth(DepthOf::Feed),
            "the window counts from the marker read at 45 s"
        );
        check.depth_read(at(start, 105), empty());
        assert_eq!(check.next(at(start, 105)), EndAction::ReadMarker);
    }

    /// Pins that a failed read of the feed restarts the window and ends
    /// nothing.
    #[test]
    fn a_failed_read_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 30);
        assert_eq!(
            check.next(at(start, 45)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 45), failed());
        assert_eq!(
            check.next(at(start, 59)),
            EndAction::Wait(Some(at(start, 60))),
            "a failed read is followed 15 s after its request"
        );
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        assert!(check.outcome().is_none());
    }

    /// Pins the answer bound: an empty read answered more than 20 s after it
    /// was requested restarts the window, and one answered at 20 s does not.
    #[test]
    fn a_read_answered_late_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 15);
        assert_eq!(
            check.next(at(start, 30)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 51), empty());
        assert_eq!(check.next(at(start, 51)), EndAction::ReadMarker);

        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(at(start, 20), empty());
        assert_eq!(
            check.next(at(start, 20)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        // The read that would close the window, answered late, opens a new
        // window from the next marker read and does not pass to the receive.
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 45);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 81), empty());
        assert_eq!(check.next(at(start, 81)), EndAction::ReadMarker);
        check.marker_read(at(start, 81), present("a"));
        assert_eq!(
            check.next(at(start, 81)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
    }

    /// Pins the gap bound: a read taken more than 20 s after the window's last
    /// one, as a clock that jumped or a poll that stalled gives, restarts the
    /// window from a marker read, and one taken at 20 s does not.
    #[test]
    fn a_read_taken_after_a_gap_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 15);
        assert_eq!(check.next(at(start, 36)), EndAction::ReadMarker);
        assert!(check.outcome().is_none());

        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 15);
        assert_eq!(
            check.next(at(start, 35)),
            EndAction::ReadDepth(DepthOf::Feed),
            "20 s after the last read is within the bound"
        );

        // A jump of a whole window cannot close it.
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 0);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
    }

    /// Pins that a closing marker naming another run opens a new window on
    /// that run instead of ending.
    #[test]
    fn a_closing_marker_of_another_run_opens_a_new_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("b"));
        empty_reads(&mut check, start, 60, 120);
        assert_eq!(check.next(at(start, 120)), EndAction::ReadMarker);
        check.marker_read(at(start, 120), present("b"));
        assert_eq!(check.next(at(start, 120)), EndAction::Receive);
    }

    /// Pins that a marker gone at the window's end ends nothing, and the
    /// check reads it again 15 s later.
    #[test]
    fn an_absent_closing_marker_does_not_end() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), MarkerRead::Absent);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::Wait(Some(at(start, 75))),
            "an absent marker is read again 15 s later"
        );
        assert_eq!(check.next(at(start, 75)), EndAction::ReadMarker);
        check.marker_read(at(start, 75), MarkerRead::Absent);
        assert!(check.outcome().is_none());

        // A failed closing read ends nothing either.
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), MarkerRead::Failed);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::Wait(Some(at(start, 75))),
            "a failed marker read is retried 15 s later"
        );
        assert_eq!(check.next(at(start, 75)), EndAction::ReadMarker);
    }

    /// Pins that a marker read's schedule counts from its request, so an
    /// answer noticed late does not delay the next read, and that the window
    /// counts from its first read of the feed.
    #[test]
    fn a_marker_answer_noticed_late_keeps_the_schedule() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(WINDOW, start);
        assert_eq!(check.next(start), EndAction::ReadMarker);
        check.marker_read(at(start, 10), MarkerRead::Absent);
        assert_eq!(
            check.next(at(start, 10)),
            EndAction::Wait(Some(at(start, 15)))
        );
        assert_eq!(check.next(at(start, 15)), EndAction::ReadMarker);
        // Answered at 25 s, and the first read of the feed taken at 30 s:
        // that read starts the window.
        check.marker_read(at(start, 25), present("a"));
        empty_reads(&mut check, start, 30, 75);
        assert_eq!(
            check.next(at(start, 75)),
            EndAction::Wait(Some(at(start, 90))),
            "the read at 75 s is short of a window after the first read"
        );
        empty_reads(&mut check, start, 90, 90);
        assert_eq!(check.next(at(start, 90)), EndAction::ReadMarker);
    }

    /// Pins that a window at the 20 s floor spans 20 s of reads even when its
    /// marker read was answered at the 20 s bound: the first read of the feed
    /// starts it, and the read 20 s or more later closes it.
    #[test]
    fn a_window_spans_its_reads_from_the_first() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(END_CHECK_INTERVAL, start);
        assert_eq!(check.next(start), EndAction::ReadMarker);
        check.marker_read(at(start, 20), present("a"));
        empty_reads(&mut check, start, 20, 35);
        assert_eq!(
            check.next(at(start, 35)),
            EndAction::Wait(Some(at(start, 50))),
            "15 s of reads do not close a 20 s window"
        );
        empty_reads(&mut check, start, 50, 50);
        assert_eq!(check.next(at(start, 50)), EndAction::ReadMarker);
    }

    /// Pins that an answer of a kind the check did not ask for is ignored and
    /// leaves the outstanding read outstanding.
    #[test]
    fn an_answer_nothing_asked_for_is_ignored() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.marker_read(start, present("b"));
        check.received(start, true);
        assert_eq!(check.next(start), EndAction::Wait(None));
        check.depth_read(start, empty());
        assert_eq!(
            check.next(start),
            EndAction::Wait(Some(at(start, 15))),
            "the window on run a carries on"
        );
        // With nothing outstanding, a depth answer is ignored too.
        check.depth_read(start, visible(1));
        assert_eq!(
            check.next(at(start, 15)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
    }

    /// Everything the check logs while `f` runs, as text.
    fn logs(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    /// Pins the transitions an operator sees at INFO: a window opening with
    /// its run, and a restart once a window is open. A restart before any
    /// window opens, as every poll of a busy reader makes, logs nothing.
    #[test]
    fn the_check_logs_a_window_opening_and_its_restart() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(WINDOW, start);
        let quiet = logs(|| check.restart(start));
        assert!(quiet.is_empty(), "{quiet}");
        assert_eq!(check.next(start), EndAction::ReadMarker);
        let opened = logs(|| check.marker_read(start, present("run-a")));
        assert!(
            opened.contains("INFO")
                && opened.contains("end marker found")
                && opened.contains("run-a"),
            "{opened}"
        );
        let restarted = logs(|| check.restart(start));
        assert!(
            restarted.contains("INFO") && restarted.contains("restarting the end check"),
            "{restarted}"
        );

        // The same run's marker opening the window again is not announced
        // again; a busy feed restarts the check at DEBUG only.
        assert_eq!(check.next(at(start, 15)), EndAction::ReadMarker);
        let again = logs(|| check.marker_read(at(start, 15), present("run-a")));
        assert!(again.is_empty(), "{again}");
        assert_eq!(
            check.next(at(start, 15)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        let busy = logs(|| check.depth_read(at(start, 15), visible(1)));
        assert!(!busy.contains("INFO") && !busy.contains("WARN"), "{busy}");

        // An absent marker is no failure; a failed read warns.
        assert_eq!(check.next(at(start, 30)), EndAction::ReadMarker);
        let absent = logs(|| check.marker_read(at(start, 30), MarkerRead::Absent));
        assert!(!absent.contains("WARN"), "{absent}");
        assert_eq!(check.next(at(start, 45)), EndAction::ReadMarker);
        let failed = logs(|| check.marker_read(at(start, 45), MarkerRead::Failed));
        assert!(
            failed.contains("WARN") && failed.contains("reading the end marker failed"),
            "{failed}"
        );
    }

    /// Pins that an exact check's wording names no settling window, which
    /// the in-process feed does not have.
    #[test]
    fn an_exact_check_logs_no_settling_window() {
        let start = Instant::now();
        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        let failed = logs(|| check.depth_read(start, failed()));
        assert!(failed.contains("restarting the end check"), "{failed}");
        let shown = failed + &logs(|| check.restart(start));
        assert!(!shown.contains("settling window"), "{shown}");
    }

    /// Pins that no window opens before a marker is found: the feed is not
    /// read until a marker is.
    #[test]
    fn no_window_opens_before_a_marker_is_found() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(WINDOW, start);
        for secs in [0, 15, 30] {
            assert_eq!(check.next(at(start, secs)), EndAction::ReadMarker);
            check.marker_read(at(start, secs), MarkerRead::Absent);
        }
        assert_eq!(check.next(at(start, 45)), EndAction::ReadMarker);
        check.marker_read(at(start, 45), present("a"));
        assert_eq!(
            check.next(at(start, 45)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
    }

    /// Pins that the closing marker is read only after the read of the feed
    /// that closes the window, never at the window's end alone.
    #[test]
    fn the_closing_marker_is_read_after_the_last_depth_read() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 45);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        assert_eq!(check.next(at(start, 61)), EndAction::Wait(None));
        check.depth_read(at(start, 61), empty());
        assert_eq!(check.next(at(start, 61)), EndAction::ReadMarker);
    }

    /// Pins that a receive taking a unit after the closing marker restarts
    /// the check, and that a restart waits 15 s from the last read.
    #[test]
    fn a_receive_that_takes_a_unit_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(check.next(at(start, 62)), EndAction::Receive);
        check.received(at(start, 62), true);
        assert_eq!(
            check.next(at(start, 62)),
            EndAction::Wait(Some(at(start, 75))),
            "15 s from the closing marker read, the last read of the marker or feed"
        );
        assert_eq!(check.next(at(start, 75)), EndAction::ReadMarker);
    }

    /// Pins that a restart drops the outstanding read: its answer is ignored.
    #[test]
    fn a_restart_ignores_the_outstanding_read() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.restart(at(start, 5));
        check.depth_read(at(start, 5), empty());
        assert_eq!(
            check.next(at(start, 5)),
            EndAction::Wait(Some(at(start, 15)))
        );
        assert_eq!(check.next(at(start, 15)), EndAction::ReadMarker);
    }

    /// Pins that a failed dead-letter read is retried 15 s later and ends
    /// the job neither way.
    #[test]
    fn a_failed_dead_letter_read_neither_drains_nor_stalls() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(
            finish(&mut check, at(start, 60), failed()),
            EndAction::Wait(Some(at(start, 75)))
        );
        assert!(check.outcome().is_none());
        assert_eq!(
            check.next(at(start, 75)),
            EndAction::ReadDepth(DepthOf::DeadLetters)
        );
        check.depth_read(at(start, 75), empty());
        assert_eq!(check.outcome(), Some(&EndOutcome::Completed));
    }

    /// Pins the dead-letter verdict at its boundary: one unit stalls the job
    /// with the depth read, none completes it.
    #[test]
    fn one_dead_letter_stalls_and_none_completes() {
        let start = Instant::now();
        for (field, set) in [
            ("visible", (|d: &mut Depth| d.visible = 1) as fn(&mut Depth)),
            ("delayed", |d: &mut Depth| d.delayed = 1),
            ("in_flight", |d: &mut Depth| d.in_flight = 1),
        ] {
            let mut check = EndCheck::exact(start);
            assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
            check.depth_read(start, empty());
            let mut dead = Depth::new("dead letters", true);
            set(&mut dead);
            assert_eq!(
                finish(&mut check, start, Ok(dead.clone())),
                EndAction::Ended(EndOutcome::Stalled(dead)),
                "{field}"
            );
        }
        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, empty());
        assert_eq!(
            finish(&mut check, start, empty()),
            EndAction::Ended(EndOutcome::Completed)
        );
    }

    /// Pins that an exact check ends on its first empty read with no window,
    /// no marker and no clock advance, and that a non-empty read holds it.
    #[test]
    fn an_exact_feed_ends_on_its_first_empty_read() {
        let start = Instant::now();
        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, visible(1));
        assert!(check.outcome().is_none());
        check.restart(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, empty());
        assert_eq!(
            finish(&mut check, start, empty()),
            EndAction::Ended(EndOutcome::Completed)
        );
    }

    /// Pins that a depth read failing with a fatal class ends the check
    /// `Failed` with that error, for the feed and for its dead letters, and
    /// that a retryable failure does not.
    #[test]
    fn a_fatal_depth_read_ends_the_check_failed() {
        let start = Instant::now();
        let fatal = FeedError::new(FeedErrorKind::Fatal, "access denied");
        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, Err(fatal.clone()));
        assert_eq!(
            check.next(start),
            EndAction::Ended(EndOutcome::Failed(fatal.clone()))
        );

        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, empty());
        assert_eq!(check.next(start), EndAction::Receive);
        check.received(start, false);
        assert_eq!(
            check.next(start),
            EndAction::ReadDepth(DepthOf::DeadLetters)
        );
        check.depth_read(start, Err(fatal.clone()));
        assert_eq!(check.outcome(), Some(&EndOutcome::Failed(fatal)));

        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, failed());
        assert!(check.outcome().is_none());
    }

    /// Pins that a marker read answered more than 20 s after its request
    /// restarts the check: a closing read that comes back late does not pass
    /// on to the receive.
    #[test]
    fn a_marker_read_answered_late_restarts_the_check() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 81), present("a"));
        assert_eq!(check.next(at(start, 81)), EndAction::ReadMarker);

        // At 20 s it is on time.
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 80), present("a"));
        assert_eq!(check.next(at(start, 80)), EndAction::Receive);
    }

    /// Pins that an exact check takes an empty read answered late, since its
    /// counts are exact and it has no window to keep.
    #[test]
    fn an_exact_check_takes_a_read_answered_late() {
        let start = Instant::now();
        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(at(start, 100), empty());
        assert_eq!(check.next(at(start, 100)), EndAction::Receive);
    }

    /// Pins that a restart after the check has ended keeps its outcome.
    #[test]
    fn a_restart_after_the_end_keeps_the_outcome() {
        let start = Instant::now();
        let mut check = EndCheck::exact(start);
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(start, empty());
        assert_eq!(
            finish(&mut check, start, empty()),
            EndAction::Ended(EndOutcome::Completed)
        );
        check.restart(at(start, 1));
        assert_eq!(
            check.next(at(start, 1)),
            EndAction::Ended(EndOutcome::Completed)
        );
    }

    /// Pins that a window too long to add to an instant never closes, and
    /// does not panic.
    #[test]
    fn a_window_too_long_for_an_instant_never_closes() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(Duration::MAX, start);
        assert_eq!(check.next(start), EndAction::ReadMarker);
        check.marker_read(start, present("a"));
        empty_reads(&mut check, start, 0, 45);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
    }

    /// Pins that every count of the feed's depth, the unsent one included,
    /// holds the window open.
    #[test]
    fn each_count_holds_the_window_open() {
        let start = Instant::now();
        let setters: [fn(&mut Depth); 4] = [
            |d| d.visible = 1,
            |d| d.delayed = 1,
            |d| d.in_flight = 1,
            |d| d.unsent = 1,
        ];
        for set in setters {
            let mut check = opened(start, "a");
            assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
            let mut depth = Depth::new("queue", true);
            set(&mut depth);
            check.depth_read(start, Ok(depth));
            assert_eq!(check.next(at(start, 15)), EndAction::ReadMarker);
        }
    }
}
