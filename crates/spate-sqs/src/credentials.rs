//! The credentials provider the SDK client loads through: it runs each load
//! on a task of its own, so a load outlives the call that started it.

use crate::error::chain;
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, future};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::Instrument as _;

/// How long one load runs before it ends as timed out: the longest bound
/// of any call the client makes.
pub(crate) const LOAD_BOUND: Duration = crate::client::RECEIVE_OPERATION_TIMEOUT;

/// The SDK identity cache's refresh buffer. A finished load nobody awaited
/// is handed out only while the cache would keep its credentials.
const REFRESH_BUFFER: Duration = Duration::from_secs(10);

type Outcome = Result<Credentials, Arc<CredentialsError>>;

/// A load under way or finished, until a caller takes its outcome.
struct Load {
    outcome: watch::Receiver<Option<Outcome>>,
    task: AbortHandle,
}

/// What a call does with the slot: await a load, or take credentials a
/// load nobody awaited left behind.
enum Joined {
    Await(watch::Receiver<Option<Outcome>>),
    Ready(Credentials),
}

/// Wraps the client's credentials provider so that each load runs on a task
/// of its own.
///
/// The SDK runs a load inside the call that needs credentials, and a call
/// given up at its timeout drops it. Here a dropped call leaves the load
/// running, and the next call awaits it. At most one load runs at a time,
/// on the runtime that polled the call that started it. A load ends when it
/// finishes, after [`LOAD_BOUND`], or when this provider drops. A finished
/// load is handed out once: to every call awaiting it, or else to the next
/// call, and the call after that starts a new load.
pub(crate) struct SharedLoad {
    inner: SharedCredentialsProvider,
    now: fn() -> SystemTime,
    slot: Mutex<Option<Load>>,
}

impl SharedLoad {
    pub(crate) fn new(inner: SharedCredentialsProvider) -> Self {
        Self::with_clock(inner, SystemTime::now)
    }

    fn with_clock(inner: SharedCredentialsProvider, now: fn() -> SystemTime) -> Self {
        Self {
            inner,
            now,
            slot: Mutex::new(None),
        }
    }

    /// Joins the load under way, takes the one left behind, or starts one.
    fn join_or_start(&self) -> Joined {
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(load) = slot.take() {
            // A closed channel with no outcome is a load its runtime dropped.
            let running = load.outcome.borrow().is_none() && load.outcome.has_changed().is_ok();
            if running {
                let outcome = load.outcome.clone();
                *slot = Some(load);
                return Joined::Await(outcome);
            }
            if let Some(Ok(credentials)) = &*load.outcome.borrow()
                && credentials
                    .expiry()
                    .is_none_or(|expiry| expiry > (self.now)() + REFRESH_BUFFER)
            {
                return Joined::Ready(credentials.clone());
            }
        }
        let (send, outcome) = watch::channel(None);
        let task =
            tokio::spawn(load(self.inner.clone(), send).instrument(tracing::Span::current()))
                .abort_handle();
        *slot = Some(Load {
            outcome: outcome.clone(),
            task,
        });
        Joined::Await(outcome)
    }

    /// Clears the slot if it still holds the load `outcome` came from.
    fn taken(&self, outcome: &watch::Receiver<Option<Outcome>>) {
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        if slot
            .as_ref()
            .is_some_and(|load| load.outcome.same_channel(outcome))
        {
            *slot = None;
        }
    }
}

impl ProvideCredentials for SharedLoad {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async move {
            let mut outcome = match self.join_or_start() {
                Joined::Await(outcome) => outcome,
                Joined::Ready(credentials) => return Ok(credentials),
            };
            let got = outcome
                .wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|got| got.clone());
            self.taken(&outcome);
            match got {
                Some(Ok(credentials)) => Ok(credentials),
                Some(Err(e)) => Err(CredentialsError::provider_error(Shared(e))),
                None => Err(CredentialsError::provider_error(
                    "the credentials load was cancelled",
                )),
            }
        })
    }

    fn fallback_on_interrupt(&self) -> Option<Credentials> {
        fallback(&self.inner)
    }
}

impl Drop for SharedLoad {
    fn drop(&mut self) {
        let slot = self.slot.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let Some(load) = slot.take() {
            load.task.abort();
        }
    }
}

impl fmt::Debug for SharedLoad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedLoad")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// Aborts the task it holds when dropped, so aborting a load ends the
/// provider's own future too.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Runs one load of `inner` and sends its outcome. The provider runs on a
/// task of its own so that a panic in it reaches the callers as an error.
async fn load(inner: SharedCredentialsProvider, send: watch::Sender<Option<Outcome>>) {
    let provider = inner.clone();
    let mut call = AbortOnDrop(tokio::spawn(
        async move { provider.provide_credentials().await },
    ));
    let outcome = match tokio::time::timeout(LOAD_BOUND, &mut call.0).await {
        Ok(Ok(result)) => result,
        Ok(Err(join)) => Err(CredentialsError::provider_error(ended(join))),
        Err(_) => match fallback(&inner) {
            Some(credentials) => {
                tracing::warn!(
                    "loading AWS credentials took longer than {LOAD_BOUND:?}; using the \
                     provider's fallback credentials"
                );
                Ok(credentials)
            }
            None => Err(CredentialsError::provider_timed_out(LOAD_BOUND)),
        },
    };
    if let Err(e) = &outcome {
        tracing::warn!("loading AWS credentials failed: {}", chain(e));
    }
    send.send_replace(Some(outcome.map_err(Arc::new)));
}

/// The fallback credentials of the provider `shared` wraps.
fn fallback(shared: &SharedCredentialsProvider) -> Option<Credentials> {
    // Through the wrapped provider: `SharedCredentialsProvider`'s own
    // `fallback_on_interrupt` is the trait default, `None`.
    shared.as_ref().fallback_on_interrupt()
}

/// What a provider task that did not return says of how it ended.
fn ended(join: tokio::task::JoinError) -> String {
    if !join.is_panic() {
        return "the credentials load was cancelled".to_owned();
    }
    let payload = join.into_panic();
    let text = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a non-string payload");
    format!("the credentials provider panicked: {text}")
}

/// One load's error, handed to each caller that awaited the load. Its text
/// and sources are the load's own, so the caller's error classifies as the
/// load's would.
#[derive(Debug)]
struct Shared(Arc<CredentialsError>);

impl fmt::Display for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for Shared {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_stub::{ScriptedCredentials, keys};
    use std::time::UNIX_EPOCH;
    use tokio::time::timeout;

    const MS: Duration = Duration::from_millis(1);

    /// Answers each load with keys that never expire after `d`.
    fn every_load_takes(d: Duration) -> Arc<ScriptedCredentials> {
        ScriptedCredentials::new(move |_| {
            Box::pin(async move {
                tokio::time::sleep(d).await;
                Ok(keys(None))
            })
        })
    }

    /// Pins that a call given up during a load leaves it running, that the
    /// calls after it share that one load, and that the call after those
    /// starts a new one.
    #[tokio::test(start_paused = true)]
    async fn overlapping_calls_share_one_inner_load() {
        let inner = every_load_takes(1_200 * MS);
        let wrapper = SharedLoad::new(inner.provider());
        assert!(
            timeout(500 * MS, wrapper.provide_credentials())
                .await
                .is_err()
        );
        let (b, c) = tokio::join!(wrapper.provide_credentials(), wrapper.provide_credentials());
        assert_eq!(b.unwrap().access_key_id(), "AKIDSCRIPTED");
        assert_eq!(c.unwrap().access_key_id(), "AKIDSCRIPTED");
        assert_eq!(inner.started(), 1);
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);
    }

    /// Pins that a load nobody awaits when it finishes is handed to the
    /// next call, once, even past the SDK cache's own load timeout.
    #[tokio::test(start_paused = true)]
    async fn a_finished_load_is_handed_out_once() {
        let inner = every_load_takes(Duration::from_secs(40));
        let wrapper = SharedLoad::new(inner.provider());
        assert!(
            timeout(Duration::from_secs(1), wrapper.provide_credentials())
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_secs(40)).await;
        assert_eq!(inner.finished(), 1);
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 1);
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);
    }

    /// Fails the first load with "no profile" after `d`; later loads
    /// answer at once.
    fn first_load_fails_after(d: Duration) -> Arc<ScriptedCredentials> {
        ScriptedCredentials::new(move |n| {
            Box::pin(async move {
                if n == 0 {
                    tokio::time::sleep(d).await;
                    return Err(CredentialsError::provider_error("no profile"));
                }
                Ok(keys(None))
            })
        })
    }

    /// Pins that a failed load reaches the call awaiting it with its cause,
    /// and that a failure nobody awaited is never handed to a later call.
    #[tokio::test(start_paused = true)]
    async fn a_failed_load_reaches_its_caller_and_is_not_kept() {
        let inner = first_load_fails_after(Duration::ZERO);
        let wrapper = SharedLoad::new(inner.provider());
        let e = wrapper.provide_credentials().await.unwrap_err();
        assert_eq!(
            chain(&e),
            "an error occurred while loading credentials: an error occurred while loading \
             credentials: no profile"
        );
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);

        let inner = first_load_fails_after(Duration::from_secs(1));
        let wrapper = SharedLoad::new(inner.provider());
        assert!(
            timeout(500 * MS, wrapper.provide_credentials())
                .await
                .is_err()
        );
        inner.until_finished(1).await;
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);
    }

    /// Pins that a provider that panics fails the call with the panic's
    /// text, and that the next call starts a new load.
    #[tokio::test(start_paused = true)]
    async fn a_panicking_load_reads_as_an_error() {
        let inner = ScriptedCredentials::new(|n| {
            Box::pin(async move {
                assert!(n > 0, "boom");
                Ok(keys(None))
            })
        });
        let wrapper = SharedLoad::new(inner.provider());
        let e = wrapper.provide_credentials().await.unwrap_err();
        assert!(chain(&e).contains("panicked: boom"), "{}", chain(&e));
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);
    }

    /// The first load never answers; later loads answer at once.
    fn first_load_hangs(fallback: Option<Credentials>) -> Arc<ScriptedCredentials> {
        ScriptedCredentials::with_fallback(
            |n| {
                Box::pin(async move {
                    if n == 0 {
                        std::future::pending::<()>().await;
                    }
                    Ok(keys(None))
                })
            },
            fallback,
        )
    }

    /// Pins that a call joins a load that has not answered within its
    /// bound, and that past [`LOAD_BOUND`] the next call starts a new one.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_load_is_started_over_after_its_bound() {
        let inner = first_load_hangs(None);
        let wrapper = SharedLoad::new(inner.provider());
        let second = Duration::from_secs(1);
        assert!(
            timeout(second, wrapper.provide_credentials())
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_secs(29)).await;
        assert!(
            timeout(second, wrapper.provide_credentials())
                .await
                .is_err()
        );
        assert_eq!(inner.started(), 1, "a call inside the bound joins the load");
        tokio::time::sleep(LOAD_BOUND - Duration::from_secs(30)).await;
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2);
    }

    /// Pins that a load whose runtime was dropped is started over on the
    /// next call, which does not wait on the dead load.
    #[test]
    fn a_load_its_runtime_dropped_is_started_over() {
        let paused = || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
        };
        let inner = every_load_takes(Duration::from_secs(1));
        let wrapper = SharedLoad::new(inner.provider());
        paused().block_on(async {
            assert!(
                timeout(500 * MS, wrapper.provide_credentials())
                    .await
                    .is_err()
            );
        });
        let got = paused().block_on(wrapper.provide_credentials());
        assert_eq!(got.unwrap().access_key_id(), "AKIDSCRIPTED");
        assert_eq!(inner.started(), 2);
    }

    /// The fixed instant the clock-taking tests read as now.
    fn fixed_now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000_000)
    }

    /// Pins that a load nobody awaited is handed out only while its keys
    /// have more than the refresh buffer left.
    #[tokio::test(start_paused = true)]
    async fn a_kept_result_inside_the_refresh_buffer_is_not_handed_out() {
        for (left, handed_out) in [(9, false), (10, false), (11, true)] {
            let expiry = fixed_now() + Duration::from_secs(left);
            let inner = ScriptedCredentials::new(move |_| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(keys(Some(expiry)))
                })
            });
            let wrapper = SharedLoad::with_clock(inner.provider(), fixed_now);
            assert!(
                timeout(500 * MS, wrapper.provide_credentials())
                    .await
                    .is_err()
            );
            inner.until_finished(1).await;
            wrapper.provide_credentials().await.unwrap();
            let loads = if handed_out { 1 } else { 2 };
            assert_eq!(inner.started(), loads, "{left} s left");
        }
    }

    /// Pins that the provider's fallback is forwarded, and that a load
    /// stuck past [`LOAD_BOUND`] hands it to the call awaiting the load.
    #[tokio::test(start_paused = true)]
    async fn the_fallback_on_interrupt_is_forwarded() {
        let fallback = Credentials::new("AKIDFALLBACK", "x", None, None, "fallback");
        let inner = first_load_hangs(Some(fallback));
        let wrapper = SharedLoad::new(inner.provider());
        assert_eq!(
            wrapper.fallback_on_interrupt().unwrap().access_key_id(),
            "AKIDFALLBACK"
        );
        let got = wrapper.provide_credentials().await.unwrap();
        assert_eq!(got.access_key_id(), "AKIDFALLBACK");
        assert_eq!(inner.started(), 1);
    }

    /// Pins one warning per failed load, however many calls gave up on it.
    #[test]
    fn a_failed_credentials_load_warns_once() {
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(async {
                    let inner = first_load_fails_after(Duration::from_secs(1));
                    let wrapper = SharedLoad::new(inner.provider());
                    for _ in 0..2 {
                        assert!(
                            timeout(200 * MS, wrapper.provide_credentials())
                                .await
                                .is_err()
                        );
                    }
                    wrapper.provide_credentials().await.unwrap_err();
                    assert_eq!(inner.started(), 1);
                });
        });
        assert_eq!(
            lines,
            [
                " WARN spate_sqs::credentials: loading AWS credentials failed: an error occurred \
              while loading credentials: no profile"
            ]
        );
    }

    /// Pins that a call taking a load's outcome late leaves a newer load in
    /// the slot, so a call during that newer load joins it.
    #[tokio::test(start_paused = true)]
    async fn a_late_taker_leaves_a_newer_load_in_place() {
        use std::future::{Future as _, poll_fn};
        use std::task::Poll;
        let inner = every_load_takes(Duration::from_secs(1));
        let wrapper = SharedLoad::new(inner.provider());
        let mut late = std::pin::pin!(wrapper.provide_credentials());
        let mut pending = std::pin::pin!(wrapper.provide_credentials());
        assert!(poll_fn(|cx| Poll::Ready(late.as_mut().poll(cx).is_pending())).await);
        inner.until_finished(1).await;
        wrapper.provide_credentials().await.unwrap();
        assert!(poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(inner.started(), 2);
        late.await.unwrap();
        wrapper.provide_credentials().await.unwrap();
        assert_eq!(inner.started(), 2, "a call during the second load joins it");
    }

    /// Pins the one warning a load stuck past [`LOAD_BOUND`] logs when it
    /// answers from the provider's fallback.
    #[test]
    fn a_load_answered_from_the_fallback_warns_once() {
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(async {
                    let fallback = Credentials::new("AKIDFALLBACK", "x", None, None, "fallback");
                    let inner = first_load_hangs(Some(fallback));
                    let wrapper = SharedLoad::new(inner.provider());
                    let got = wrapper.provide_credentials().await.unwrap();
                    assert_eq!(got.access_key_id(), "AKIDFALLBACK");
                });
        });
        assert_eq!(
            lines,
            [
                " WARN spate_sqs::credentials: loading AWS credentials took longer than 60s; using \
              the provider's fallback credentials"
            ]
        );
    }

    /// Pins that a load stuck past [`LOAD_BOUND`] with no fallback fails the
    /// call awaiting it as timed out.
    #[tokio::test(start_paused = true)]
    async fn a_load_stuck_past_its_bound_fails_as_timed_out() {
        let inner = first_load_hangs(None);
        let wrapper = SharedLoad::new(inner.provider());
        let e = wrapper.provide_credentials().await.unwrap_err();
        assert_eq!(
            chain(&e),
            "an error occurred while loading credentials: credentials provider timed out after \
             60 seconds"
        );
    }

    /// Pins that a panic with a formatted message, as `unwrap` and `expect`
    /// raise, reaches the call with its text.
    #[tokio::test(start_paused = true)]
    async fn a_formatted_panic_reads_as_its_text() {
        let inner = ScriptedCredentials::new(|n| {
            Box::pin(async move {
                assert!(n > 0, "boom on load {n}");
                Ok(keys(None))
            })
        });
        let wrapper = SharedLoad::new(inner.provider());
        let e = wrapper.provide_credentials().await.unwrap_err();
        assert!(
            chain(&e).contains("panicked: boom on load 0"),
            "{}",
            chain(&e)
        );
    }
}
