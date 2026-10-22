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
use futures_util::FutureExt as _;
use object_store::ObjectStore;
use spate_core::clock::tokio::Clock;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::{
    DepthReading, EndAction, EndCheck, EndOutcome, Feed, FeedWaker, MarkerRead,
};
use spate_core::source::StopSignal;
use std::sync::Arc;
use std::time::Duration;

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
    marker_read: Option<tokio::task::JoinHandle<Result<Option<String>, SourceError>>>,
    depth_read: Option<DepthReading>,
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
        }
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
        if let Some(check) = &mut self.check {
            check.restart(self.clock.now().into_std());
            self.marker_read = None;
            self.depth_read = None;
        }
    }

    /// Advance the check on an idle poll, the reader holding no work.
    /// `receive` receives once and returns how many units it took. Returns
    /// `true` once the job has completed.
    ///
    /// # Errors
    ///
    /// Fatal when the dead-letter destination holds units (the job is
    /// stalled), when the feed reports no depth, on a fatal feed or marker
    /// read error, and as `receive` returns.
    pub(crate) fn idle(
        &mut self,
        feed: &dyn Feed,
        mut receive: impl FnMut() -> Result<usize, SourceError>,
    ) -> Result<bool, SourceError> {
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
        if let Some(task) = &mut self.marker_read
            && let Some(outcome) = (&mut *task).now_or_never()
        {
            self.marker_read = None;
            let read = match outcome {
                Ok(Ok(Some(run))) => MarkerRead::Present(run),
                Ok(Ok(None)) => MarkerRead::Absent,
                Ok(Err(e)) if is_retryable(&e) => {
                    tracing::warn!(error = %e, "reading the end marker failed; reading it again");
                    MarkerRead::Failed
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "the end marker read ended; reading it again");
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
                    self.marker_read = Some(self.handle.spawn(async move {
                        let read = read_end(store.as_ref(), &location).await;
                        if let Some(waker) = waker {
                            waker.wake();
                        }
                        read
                    }));
                }
                EndAction::ReadDepth(of) => {
                    let Some(mut reading) = feed.read_depth(of) else {
                        return Err(no_depth(feed));
                    };
                    match reading.try_outcome() {
                        Some(outcome) => check.depth_read(now, outcome),
                        None => self.depth_read = Some(reading),
                    }
                }
                EndAction::Receive => {
                    let took = receive()?;
                    check.received(now, took > 0);
                    if took > 0 {
                        return Ok(false);
                    }
                }
                EndAction::Ended(EndOutcome::Completed) => {
                    tracing::info!(
                        "the bounded job has read all of its work and its dead-letter \
                         destination is empty; ending the run"
                    );
                    return Ok(true);
                }
                EndAction::Ended(EndOutcome::Stalled(depth)) => {
                    return Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!(
                            "bounded feed job stalled: the dead-letter destination holds units \
                             ({depth}); fix their cause, then copy them into a re-run's feed"
                        ),
                    });
                }
                EndAction::Ended(EndOutcome::Failed(e)) => {
                    return Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("reading the feed's depth: {e}"),
                    });
                }
                // `Wait`: nothing to start until a read answers or falls due.
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
            Ok(1)
        });
        assert!(
            !took_one.unwrap(),
            "completed though the receive took a unit"
        );
        assert_eq!(receives.get(), 1, "one receive per poll");
        let took_none = end.idle(&feed, || {
            receives.set(receives.get() + 1);
            Ok(0)
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
        assert!(!end.idle(&feed, || Ok(0)).unwrap());
        assert!(
            park.park(Duration::from_secs(30)),
            "the marker read did not wake the reader"
        );
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
        assert!(!end.idle(&feed, || Ok(0)).unwrap());
        assert!(
            park.park(Duration::from_secs(30)),
            "the marker read finishes"
        );
        let err = end.idle(&feed, || Ok(0)).unwrap_err().to_string();
        assert!(err.contains("not a lister run's end marker"), "{err}");
    }
}
