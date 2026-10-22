//! The end check: when a bounded job on a feed has read all of its work.
//!
//! [`EndCheck`] decides from the reads a source reports, and does no I/O
//! itself. The source asks it what to read next with
//! [`next`](EndCheck::next), starts that read, and reports the answer. It
//! holds no clock: every call takes the instant the source reads from its
//! own clock.

use super::{Depth, DepthOf, FeedError};
use std::time::{Duration, Instant};

/// The longest gap between two reads of the feed inside a settling window.
pub const END_CHECK_INTERVAL: Duration = Duration::from_secs(20);

/// What a read of a bounded job's end marker found.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MarkerRead {
    /// The marker exists and names this lister run.
    Present(String),
    /// No marker exists.
    Absent,
    /// The read failed and may succeed later.
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
    Opening { due: Instant },
    /// A window opened by a read of `run`'s marker at `start`; the next read
    /// of the feed is due at `due`.
    Window {
        run: String,
        start: Instant,
        due: Instant,
    },
    /// The window has passed with every read empty; the marker is read again.
    Closing { run: String },
    /// One receive that must take nothing.
    Receiving,
    /// The dead-letter destination's read, due at `due`.
    DeadLetters { due: Instant },
    Ended(EndOutcome),
}

/// Decides when a bounded job on a feed has ended.
///
/// A windowed check opens a window on a read of the end marker. Inside it
/// the feed is read at most [`END_CHECK_INTERVAL`] apart, and every read
/// must find it empty. A read requested at or after the window's end that
/// finds it empty is followed by a second read of the marker: the same run
/// passes, another run opens a new window, and no marker restarts the
/// check. An exact check, for a feed whose counts are exact and whose lister
/// has finished, reads the feed once with no window or marker. Either way
/// one receive that takes nothing follows, and then the dead-letter
/// destination decides between [`Completed`](EndOutcome::Completed) and
/// [`Stalled`](EndOutcome::Stalled).
///
/// A non-empty read, a failed read, a read answered more than one interval
/// after it was requested, or a [`restart`](EndCheck::restart) sends a
/// windowed check back to its marker read, at most one interval after its
/// previous read. A failed dead-letter read is retried one interval later
/// and ends nothing. The source calls `restart` whenever its reader holds
/// any work, so every read that counts was answered while it held none.
///
/// ```
/// use spate_core::feed::{Depth, DepthOf, EndAction, EndCheck, EndOutcome, MarkerRead};
/// use std::time::{Duration, Instant};
///
/// let start = Instant::now();
/// let mut check = EndCheck::windowed(Duration::from_secs(20), start);
/// assert_eq!(check.next(start), EndAction::ReadMarker);
/// check.marker_read(start, MarkerRead::Present("run-a".into()));
///
/// let empty = || Ok(Depth::new("queue", false));
/// for at in [0, 20] {
///     let now = start + Duration::from_secs(at);
///     assert_eq!(check.next(now), EndAction::ReadDepth(DepthOf::Feed));
///     check.depth_read(now, empty());
/// }
/// let now = start + Duration::from_secs(20);
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
    /// restart waits out one interval from it.
    last_request: Option<Instant>,
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
        }
    }

    /// What to do at `now`. A read returned here is outstanding until its
    /// answer is reported; until then `next` returns `Wait(None)`.
    pub fn next(&mut self, now: Instant) -> EndAction {
        if self.pending.is_some() {
            return EndAction::Wait(None);
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
        if !self.take_pending(Read::Marker) {
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
                } else {
                    tracing::info!(
                        run = %seen,
                        window = ?window,
                        "end marker found; the job ends once the feed stays empty over \
                         the settling window"
                    );
                }
                self.phase = Phase::Window {
                    run: seen,
                    start: now,
                    due: now,
                };
            }
            (MarkerRead::Absent, closing) => {
                if closing.is_some() {
                    tracing::info!(
                        "the end marker is gone at the settling window's end; waiting for a \
                         lister run's marker"
                    );
                }
                self.phase = Phase::Opening {
                    due: now + END_CHECK_INTERVAL,
                };
            }
            (MarkerRead::Failed, _) => {
                tracing::warn!("reading the end marker failed; reading it again");
                self.phase = Phase::Opening {
                    due: now + END_CHECK_INTERVAL,
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
        let due = requested + END_CHECK_INTERVAL;
        match (of, read) {
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
                tracing::warn!(error = %e, "reading the feed's depth failed; restarting the settling window");
                self.reopen(due);
            }
            (DepthOf::Feed, Ok(depth)) if !depth.is_empty() => {
                if matches!(self.phase, Phase::Window { .. }) {
                    tracing::info!(%depth, "the feed is not empty; restarting the settling window");
                }
                self.reopen(due);
            }
            (DepthOf::Feed, Ok(_)) if self.window.is_some() && now > due => {
                tracing::info!(
                    late = ?now - requested,
                    "a depth read answered after the read interval; restarting the settling window"
                );
                self.reopen(now);
            }
            (DepthOf::Feed, Ok(_)) => self.advance(requested),
        }
    }

    /// Report whether the receive that [`next`](Self::next) asked for took a
    /// unit. One that did restarts the check.
    pub fn received(&mut self, now: Instant, took: bool) {
        if !self.take_pending(Read::Receive) {
            return;
        }
        if took {
            self.restart(now);
        } else {
            self.phase = Phase::DeadLetters { due: now };
        }
    }

    /// Send the check back to its opening read, because the reader holds
    /// work. Any outstanding read's answer is then ignored. A windowed check
    /// reads again one interval after its last read; an exact one at once.
    pub fn restart(&mut self, now: Instant) {
        if matches!(self.phase, Phase::Ended(_)) {
            return;
        }
        if !matches!(self.phase, Phase::Opening { .. }) {
            tracing::info!("the reader took work; restarting the settling window");
        }
        self.pending = None;
        let due = match (self.window, self.last_request) {
            (Some(_), Some(last)) => (last + END_CHECK_INTERVAL).max(now),
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

    fn take_pending(&mut self, read: Read) -> bool {
        match self.pending {
            Some((pending, _)) if pending == read => {
                self.pending = None;
                true
            }
            _ => false,
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
                self.phase = if requested >= *start + window {
                    Phase::Closing { run: run.clone() }
                } else {
                    Phase::Window {
                        run: run.clone(),
                        start: *start,
                        due: requested + END_CHECK_INTERVAL,
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

    /// Opens a window on `run` at `now` and returns the check.
    fn opened(start: Instant, run: &str) -> EndCheck {
        let mut check = EndCheck::windowed(WINDOW, start);
        assert_eq!(check.next(start), EndAction::ReadMarker);
        check.marker_read(start, present(run));
        check
    }

    /// Answers every empty read of the feed due from `from` to `to`, in
    /// interval steps, and returns how many it answered.
    fn empty_reads(check: &mut EndCheck, start: Instant, from: u64, to: u64) -> usize {
        let mut reads = 0;
        for secs in (from..=to).step_by(20) {
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

    /// Pins that a window opened by a marker read ends one window later: four
    /// empty reads at 0, 20, 40 and 60 s, then the closing marker read.
    #[test]
    fn a_window_ends_one_window_after_its_opening_marker_read() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        assert_eq!(empty_reads(&mut check, start, 0, 40), 3);
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
        empty_reads(&mut check, start, 0, 20);
        assert_eq!(
            check.next(at(start, 40)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 40), visible(1));
        assert_eq!(
            check.next(at(start, 59)),
            EndAction::Wait(Some(at(start, 60)))
        );
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        empty_reads(&mut check, start, 60, 100);
        assert_eq!(
            check.next(at(start, 120)),
            EndAction::ReadDepth(DepthOf::Feed),
            "the window counts from the marker read at 60 s"
        );
        check.depth_read(at(start, 120), empty());
        assert_eq!(check.next(at(start, 120)), EndAction::ReadMarker);
    }

    /// Pins that a failed read of the feed restarts the window and ends
    /// nothing.
    #[test]
    fn a_failed_read_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 40);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 60), failed());
        assert_eq!(check.next(at(start, 80)), EndAction::ReadMarker);
        assert!(check.outcome().is_none());
    }

    /// Pins that an empty read answered more than one interval after it was
    /// requested leaves a gap in the window and restarts it.
    #[test]
    fn a_read_answered_late_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 20);
        assert_eq!(
            check.next(at(start, 40)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 65), empty());
        assert_eq!(check.next(at(start, 65)), EndAction::ReadMarker);

        // An answer exactly one interval late is still on time.
        let mut check = opened(start, "a");
        assert_eq!(check.next(start), EndAction::ReadDepth(DepthOf::Feed));
        check.depth_read(at(start, 20), empty());
        assert_eq!(
            check.next(at(start, 20)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
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
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        check.depth_read(at(start, 60), empty());
        empty_reads(&mut check, start, 80, 120);
        assert_eq!(check.next(at(start, 120)), EndAction::ReadMarker);
        check.marker_read(at(start, 120), present("b"));
        assert_eq!(check.next(at(start, 120)), EndAction::Receive);
    }

    /// Pins that a marker gone at the window's end ends nothing, and the
    /// check waits for a marker.
    #[test]
    fn an_absent_closing_marker_does_not_end() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), MarkerRead::Absent);
        assert_eq!(check.next(at(start, 80)), EndAction::ReadMarker);
        check.marker_read(at(start, 80), MarkerRead::Absent);
        assert!(check.outcome().is_none());

        // A failed closing read ends nothing either.
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), MarkerRead::Failed);
        assert_eq!(check.next(at(start, 80)), EndAction::ReadMarker);
    }

    /// Pins that no marker at the start opens no window: the feed is not read
    /// until a marker is found.
    #[test]
    fn no_window_opens_before_a_marker_is_found() {
        let start = Instant::now();
        let mut check = EndCheck::windowed(WINDOW, start);
        for secs in [0, 20, 40] {
            assert_eq!(check.next(at(start, secs)), EndAction::ReadMarker);
            check.marker_read(at(start, secs), MarkerRead::Absent);
        }
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
    }

    /// Pins that the closing marker is read only after the read of the feed
    /// that closes the window, never at the window's end alone.
    #[test]
    fn the_closing_marker_is_read_after_the_last_depth_read() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 40);
        assert_eq!(
            check.next(at(start, 60)),
            EndAction::ReadDepth(DepthOf::Feed)
        );
        assert_eq!(check.next(at(start, 61)), EndAction::Wait(None));
        check.depth_read(at(start, 61), empty());
        assert_eq!(check.next(at(start, 61)), EndAction::ReadMarker);
    }

    /// Pins that a receive taking a unit after the closing marker restarts
    /// the check, and that a restart waits one interval from the last read.
    #[test]
    fn a_receive_that_takes_a_unit_restarts_the_window() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(check.next(at(start, 60)), EndAction::Receive);
        check.received(at(start, 61), true);
        assert_eq!(
            check.next(at(start, 61)),
            EndAction::Wait(Some(at(start, 80)))
        );
        assert_eq!(check.next(at(start, 80)), EndAction::ReadMarker);
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
            EndAction::Wait(Some(at(start, 20)))
        );
        assert_eq!(check.next(at(start, 20)), EndAction::ReadMarker);
    }

    /// Pins that a failed dead-letter read is retried one interval later and
    /// ends the job neither way.
    #[test]
    fn a_failed_dead_letter_read_neither_drains_nor_stalls() {
        let start = Instant::now();
        let mut check = opened(start, "a");
        empty_reads(&mut check, start, 0, 60);
        assert_eq!(check.next(at(start, 60)), EndAction::ReadMarker);
        check.marker_read(at(start, 60), present("a"));
        assert_eq!(
            finish(&mut check, at(start, 60), failed()),
            EndAction::Wait(Some(at(start, 80)))
        );
        assert!(check.outcome().is_none());
        assert_eq!(
            check.next(at(start, 80)),
            EndAction::ReadDepth(DepthOf::DeadLetters)
        );
        check.depth_read(at(start, 80), empty());
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
            assert_eq!(check.next(at(start, 20)), EndAction::ReadMarker);
        }
    }
}
