//! The end of a bounded feed job: [`JobEnd`] drives the feed's
//! [`EndCheck`] from the unit reader's idle polls.
//!
//! A job with a job location ends on its lister run's end marker, read at
//! the start and the end of a settling window over which the feed reads
//! empty. A solo run on the in-process feed the runtime built ends once its
//! lister has finished and the feed reads empty, with no window. Either way
//! the feed's dead-letter destination then decides between a completed and a
//! stalled job. Every read runs on the I/O runtime or the feed's own task,
//! and the reader only checks its outcome, so no poll waits on it.

use crate::location::{Location, read_end};
use object_store::ObjectStore;
use spate_core::clock::tokio::Clock;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::{
    DepthOf, DepthReading, EndAction, EndCheck, EndOutcome, Feed, FeedWaker, MarkerRead,
};
use spate_core::source::StopSignal;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// What tells a job that its lister has finished.
pub(crate) enum Finish {
    /// The end marker at a job location, read by every worker.
    Marker {
        store: Arc<dyn ObjectStore>,
        location: Location,
        window: Duration,
    },
    /// The solo run's own lister, which reports its completion here.
    Solo { finished: bool },
}

/// The end check of one bounded job, held by its unit reader.
pub(crate) struct JobEnd {
    finish: Finish,
    /// Built at the first idle poll that may start it: at once on a job
    /// location, after the lister has finished on a solo run.
    check: Option<EndCheck>,
    clock: Arc<dyn Clock>,
    handle: tokio::runtime::Handle,
    stop: StopSignal,
    /// Signalled when a marker read finishes, so the reader's park ends.
    waker: Option<FeedWaker>,
    /// The marker read's answer, sent before its task signals the waker.
    marker_read: Option<oneshot::Receiver<Result<Option<String>, SourceError>>>,
    depth_read: Option<DepthReading>,
    /// What the last depth read asked for, so a failure names it.
    depth_of: Option<DepthOf>,
    /// When the check's next read falls due, so the reader's park ends by
    /// then. `None` while a read is outstanding or the reader holds work.
    due: Option<Instant>,
    /// The check's closing receive took nothing, so until it ends only the
    /// reader's own receives watch the feed.
    closed: bool,
}

impl JobEnd {
    pub(crate) fn new(
        finish: Finish,
        clock: Arc<dyn Clock>,
        handle: tokio::runtime::Handle,
        stop: StopSignal,
    ) -> JobEnd {
        JobEnd {
            finish,
            check: None,
            clock,
            handle,
            stop,
            waker: None,
            marker_read: None,
            depth_read: None,
            depth_of: None,
            due: None,
            closed: false,
        }
    }

    /// The longest the reader may park: `timeout`, cut short at the check's
    /// next read.
    pub(crate) fn park_within(&self, timeout: Duration) -> Duration {
        self.due.map_or(timeout, |due| {
            timeout.min(due.saturating_duration_since(self.clock.now().into_std()))
        })
    }

    /// Hand over the waker a finished marker read signals.
    pub(crate) fn set_waker(&mut self, waker: FeedWaker) {
        self.waker = Some(waker);
    }

    /// Record that the solo run's lister has listed and published the whole
    /// prefix.
    pub(crate) fn lister_finished(&mut self) {
        if let Finish::Solo { finished } = &mut self.finish {
            *finished = true;
        }
    }

    /// The reader holds work: send the check back to its opening read, and
    /// drop any read outstanding, as `EndCheck::restart` asks. A marker read
    /// still running finishes on its own, unread.
    pub(crate) fn busy(&mut self) {
        self.due = None;
        self.closed = false;
        if let Some(check) = &mut self.check {
            check.restart(self.clock.now().into_std());
            self.marker_read = None;
            self.depth_read = None;
        }
    }

    /// A receive of the reader's own failed and may succeed later. Past the
    /// closing receive it restarts the check, since no read of the feed
    /// follows that could find a unit the receive missed.
    pub(crate) fn receive_failed(&mut self) {
        if self.closed {
            self.busy();
        }
    }

    /// Advance the check on an idle poll, the reader holding no work.
    /// `receive` receives once and returns how many units it took, or `None`
    /// when the receive failed and may succeed later. Returns `true` once the
    /// job has completed.
    ///
    /// # Errors
    ///
    /// Fatal when the dead-letter destination holds units (the job is
    /// stalled), when the feed reports no depth, on a fatal feed or marker
    /// read error, and as `receive` returns.
    pub(crate) fn idle(
        &mut self,
        feed: &dyn Feed,
        mut receive: impl FnMut() -> Result<Option<usize>, SourceError>,
    ) -> Result<bool, SourceError> {
        self.due = None;
        if self.stop.is_set() {
            return Ok(false);
        }
        let now = self.clock.now().into_std();
        if self.check.is_none() {
            self.check = Some(match &self.finish {
                Finish::Marker { window, .. } => EndCheck::windowed(*window, now),
                Finish::Solo { finished: true } => EndCheck::exact(now),
                Finish::Solo { finished: false } => return Ok(false),
            });
        }
        let Some(check) = &mut self.check else {
            return Ok(false);
        };
        if let Some(answer) = &mut self.marker_read
            && let Some(outcome) = match answer.try_recv() {
                Ok(outcome) => Some(Some(outcome)),
                Err(oneshot::error::TryRecvError::Empty) => None,
                Err(oneshot::error::TryRecvError::Closed) => Some(None),
            }
        {
            self.marker_read = None;
            let read = match outcome {
                Some(Ok(Some(run))) => MarkerRead::Present(run),
                Some(Ok(None)) => MarkerRead::Absent,
                Some(Err(e)) if is_retryable(&e) => {
                    tracing::warn!(error = %e, "reading the end marker failed; reading it again");
                    MarkerRead::Failed
                }
                Some(Err(e)) => return Err(e),
                None => {
                    tracing::warn!("the end marker read ended unanswered; reading it again");
                    MarkerRead::Failed
                }
            };
            check.marker_read(now, read);
        }
        if let Some(reading) = &mut self.depth_read
            && let Some(outcome) = reading.try_outcome()
        {
            self.depth_read = None;
            check.depth_read(now, outcome);
        }
        loop {
            match check.next(now) {
                EndAction::ReadMarker => {
                    let Finish::Marker {
                        store, location, ..
                    } = &self.finish
                    else {
                        unreachable!("an exact check reads no marker");
                    };
                    let store = Arc::clone(store);
                    let location = location.clone();
                    let waker = self.waker.clone();
                    let (answer, answered) = oneshot::channel();
                    self.marker_read = Some(answered);
                    // The answer is sent before the wake, so the poll the
                    // wake starts finds it.
                    drop(self.handle.spawn(async move {
                        let _ = answer.send(read_end(store.as_ref(), &location).await);
                        if let Some(waker) = waker {
                            waker.wake();
                        }
                    }));
                }
                EndAction::ReadDepth(of) => {
                    self.depth_of = Some(of);
                    let Some(mut reading) = feed.read_depth(of) else {
                        return Err(no_depth(feed));
                    };
                    match reading.try_outcome() {
                        Some(outcome) => check.depth_read(now, outcome),
                        None => self.depth_read = Some(reading),
                    }
                }
                EndAction::Receive => {
                    // A receive that failed did not find the feed empty.
                    let took = receive()?.is_none_or(|took| took > 0);
                    check.received(now, took);
                    if took {
                        return Ok(false);
                    }
                    self.closed = true;
                }
                EndAction::Ended(EndOutcome::Completed) => {
                    tracing::info!(
                        "the bounded job has read all of its work and its dead-letter \
                         destination is empty; ending the run"
                    );
                    return Ok(true);
                }
                EndAction::Ended(EndOutcome::Stalled(depth)) => {
                    // A solo run's dead letters leave with the process, and
                    // its re-run lists the whole prefix again.
                    let next = match self.finish {
                        Finish::Marker { .. } => "copy them into a re-run's feed",
                        Finish::Solo { .. } => "run the job again",
                    };
                    return Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!(
                            "bounded feed job stalled: the dead-letter destination holds units \
                             ({depth}); fix their cause, then {next}"
                        ),
                    });
                }
                EndAction::Ended(EndOutcome::Failed(e)) => {
                    return Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: if self.depth_of == Some(DepthOf::DeadLetters) {
                            format!("reading the depth of the feed's dead-letter destination: {e}")
                        } else {
                            format!("reading the feed's depth: {e}")
                        },
                    });
                }
                EndAction::Wait(due) => {
                    self.due = due;
                    return Ok(false);
                }
                // `EndAction` is non-exhaustive.
                _ => return Ok(false),
            }
        }
    }
}

/// The error a feed that cannot count gives a bounded job.
pub(crate) fn no_depth(feed: &dyn Feed) -> SourceError {
    SourceError::Client {
        class: ErrorClass::Fatal,
        reason: format!(
            "the `{}` feed reports no depth, so a bounded job on it cannot tell when its work \
             is done; use a feed that counts its units, or run the job without `location`",
            feed.backend()
        ),
    }
}

fn is_retryable(e: &SourceError) -> bool {
    matches!(
        e,
        SourceError::Client {
            class: ErrorClass::Retryable,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::clock::tokio::TestClock;
    use spate_core::feed::wake_channel;
    use std::cell::Cell;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn solo(rt: &tokio::runtime::Runtime) -> JobEnd {
        let mut end = JobEnd::new(
            Finish::Solo { finished: true },
            TestClock::frozen(),
            rt.handle().clone(),
            StopSignal::default(),
        );
        end.lister_finished();
        end
    }

    /// Pins the closing receive: a unit it takes restarts the check and
    /// ends the poll after one receive, and a receive that takes nothing lets
    /// the job complete.
    #[test]
    fn a_closing_receive_that_takes_a_unit_restarts_the_check() {
        let rt = runtime();
        let (feed, _script) = spate_test::scripted_feed();
        let mut end = solo(&rt);
        let receives = Cell::new(0);
        let took_one = end.idle(&feed, || {
            receives.set(receives.get() + 1);
            Ok(Some(1))
        });
        assert!(
            !took_one.unwrap(),
            "completed though the receive took a unit"
        );
        assert_eq!(receives.get(), 1, "one receive per poll");
        let took_none = end.idle(&feed, || {
            receives.set(receives.get() + 1);
            Ok(Some(0))
        });
        assert!(took_none.unwrap(), "an empty receive lets the job complete");
        assert_eq!(receives.get(), 2);
    }

    /// Pins that a marker read signals the reader's waker when it finishes,
    /// so a reader parked on the feed reads the answer without waiting out
    /// its poll timeout.
    #[test]
    fn a_finished_marker_read_wakes_the_reader() {
        let rt = runtime();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        let mut end = JobEnd::new(
            Finish::Marker {
                store,
                location,
                window: Duration::from_secs(60),
            },
            TestClock::frozen(),
            rt.handle().clone(),
            StopSignal::default(),
        );
        let (waker, park) = wake_channel();
        end.set_waker(waker);
        let (feed, _script) = spate_test::scripted_feed();
        assert!(!end.idle(&feed, || Ok(Some(0))).unwrap());
        assert!(
            park.park(Duration::from_secs(30)),
            "the marker read did not wake the reader"
        );
    }

    /// Pins that the poll a marker read's wake starts finds the read's
    /// answer and reads the feed, and that the reader then parks no later
    /// than the check's next read until it holds work.
    #[test]
    fn the_poll_a_marker_read_wakes_reads_its_answer() {
        use object_store::ObjectStoreExt as _;
        let rt = runtime();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        rt.block_on(store.put(
            &location.end(),
            b"run=0123456789abcdef0123456789abcdef\n".to_vec().into(),
        ))
        .unwrap();
        let mut end = JobEnd::new(
            Finish::Marker {
                store,
                location,
                window: Duration::from_secs(60),
            },
            TestClock::frozen(),
            rt.handle().clone(),
            StopSignal::default(),
        );
        let (waker, park) = wake_channel();
        end.set_waker(waker);
        let (feed, script) = spate_test::scripted_feed();
        assert!(!end.idle(&feed, || Ok(Some(0))).unwrap());
        assert!(
            park.park(Duration::from_secs(30)),
            "the marker read finishes"
        );
        assert!(!end.idle(&feed, || Ok(Some(0))).unwrap());
        assert_eq!(
            script.depth_reads(DepthOf::Feed),
            1,
            "the woken poll did not find the marker read's answer"
        );
        let long = Duration::from_secs(60);
        let parked = end.park_within(long);
        assert!(
            parked > Duration::ZERO && parked <= Duration::from_secs(15),
            "{parked:?}"
        );
        end.busy();
        assert_eq!(end.park_within(long), long);
    }

    /// Pins that an end marker read that fails for good, such as a body that
    /// is not a marker, fails the run, where a retryable failure is read
    /// again.
    #[test]
    fn a_fatal_marker_read_fails_the_run() {
        use object_store::ObjectStoreExt as _;
        let rt = runtime();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let location = Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        rt.block_on(store.put(&location.end(), b"not a marker".to_vec().into()))
            .unwrap();
        let mut end = JobEnd::new(
            Finish::Marker {
                store,
                location,
                window: Duration::from_secs(60),
            },
            TestClock::frozen(),
            rt.handle().clone(),
            StopSignal::default(),
        );
        let (waker, park) = wake_channel();
        end.set_waker(waker);
        let (feed, _script) = spate_test::scripted_feed();
        assert!(!end.idle(&feed, || Ok(Some(0))).unwrap());
        assert!(
            park.park(Duration::from_secs(30)),
            "the marker read finishes"
        );
        let err = end.idle(&feed, || Ok(Some(0))).unwrap_err().to_string();
        assert!(err.contains("not a lister run's end marker"), "{err}");
    }
}
