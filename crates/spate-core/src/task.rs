//! Spawning tokio tasks and OS threads through handles that own them.
//!
//! [`spawn`] and [`spawn_on`] start a tokio task and return a [`TaskHandle`],
//! which aborts the task when dropped. [`spawn_thread`] starts a named OS
//! thread and returns a [`ThreadHandle`], which joins the thread when dropped.
//! Each spawn takes a name, and a panic in the task or thread is logged at
//! `ERROR` with that name. The panic still reaches a caller that awaits or
//! joins the handle. [`ThreadBound`] holds a future to a limit timed on such a
//! thread.

use std::any::Any;
use std::borrow::Cow;
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crossbeam_channel::RecvTimeoutError;
use tokio::task::{JoinError, JoinHandle, coop};

use crate::clock::Clock;

#[cfg(not(loom))]
mod bound;

#[cfg(not(loom))]
pub use bound::{BoundError, ThreadBound};

/// An owned tokio task, aborted when the handle drops.
///
/// Dropping the handle requests cancellation and returns at once: the task's
/// future is dropped at its next yield point, possibly after the drop
/// returns. A task blocking its thread, never yielding, is not stopped.
///
/// Awaiting the handle yields the task's output, or a [`JoinError`] when the
/// task panicked or was aborted. The await is cancel safe: polling stops
/// without losing the output, and the handle can be awaited again.
#[must_use = "dropping a TaskHandle aborts its task"]
pub struct TaskHandle<T> {
    name: &'static str,
    inner: JoinHandle<T>,
    abort_on_drop: bool,
}

impl<T> TaskHandle<T> {
    /// Request cancellation of the task, as dropping the handle does.
    pub fn abort(&self) {
        self.inner.abort();
    }

    /// Whether the task has ended, by returning, panicking or being aborted.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// Let the task run on with no owner.
    ///
    /// It then ends only when its future returns or its runtime shuts down. A
    /// panic in it is still logged.
    pub fn detach(mut self) {
        self.abort_on_drop = false;
    }
}

impl<T> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        if self.abort_on_drop {
            self.inner.abort();
        }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> std::fmt::Debug for TaskHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("name", &self.name)
            .field("id", &self.inner.id())
            .finish_non_exhaustive()
    }
}

/// Spawn `future` as a task named `name` on the current tokio runtime.
///
/// Returns without awaiting or waiting on the task, so a non-blocking path may
/// call it; tokio takes a brief internal lock to queue the task. A panic in
/// the task is logged at `ERROR` with `name`.
///
/// # Panics
///
/// When called outside a tokio runtime, as [`tokio::spawn`] does.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let handle = spate_core::task::spawn("answer", async { 6 * 7 });
/// assert_eq!(handle.await.unwrap(), 42);
///
/// // Dropping a handle aborts its task.
/// drop(spate_core::task::spawn("forever", std::future::pending::<()>()));
/// # }
/// ```
#[expect(
    clippy::disallowed_methods,
    reason = "the returned `TaskHandle` aborts the task when dropped"
)]
pub fn spawn<F>(name: &'static str, future: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    owned(name, tokio::spawn(logged(name, future)))
}

/// Spawn `future` as a task named `name` on the runtime behind `handle`.
///
/// Returns without awaiting or waiting on the task, as [`spawn`] does, and may
/// be called from outside any runtime.
#[expect(
    clippy::disallowed_methods,
    reason = "the returned `TaskHandle` aborts the task when dropped"
)]
pub fn spawn_on<F>(
    name: &'static str,
    future: F,
    handle: &tokio::runtime::Handle,
) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    owned(name, handle.spawn(logged(name, future)))
}

fn owned<T>(name: &'static str, inner: JoinHandle<T>) -> TaskHandle<T> {
    TaskHandle {
        name,
        inner,
        abort_on_drop: true,
    }
}

/// `future`, logging a panic in any poll before the panic resumes.
///
/// The resumed panic keeps `JoinError::is_panic` for a caller awaiting the
/// task. An abort drops the future without unwinding, so it logs nothing.
async fn logged<F: Future>(name: &'static str, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(payload) => {
                tracing::error!(
                    task = name,
                    panic = panic_text(&*payload),
                    "background task panicked"
                );
                resume_unwind(payload)
            }
        },
    )
    .await
}

/// Runs `fut` until it ends, or until `stop` ends first with `Err`.
///
/// `fut` is polled first, so an output ready as `stop` fires counts. As
/// `tokio::time::timeout` does with its deadline, `stop` is polled outside
/// tokio's coop budget when `fut` spent the budget on that poll, so a `fut`
/// that spends it on every poll still ends.
pub(crate) async fn first<F: Future, S: Future>(fut: F, stop: S) -> Result<F::Output, S::Output> {
    let mut fut = pin!(fut);
    let mut stop = pin!(stop);
    poll_fn(|cx| {
        let had_budget = coop::has_budget_remaining();
        if let Poll::Ready(output) = fut.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        // With no budget on entry, `fut` may be ready on the next poll, so
        // `stop` stays inside the budget and waits for it.
        if had_budget && !coop::has_budget_remaining() {
            pin!(coop::unconstrained(stop.as_mut())).poll(cx).map(Err)
        } else {
            stop.as_mut().poll(cx).map(Err)
        }
    })
    .await
}

/// The text of a panic payload: the message of a `&str` or `String` payload,
/// and a fixed placeholder for any other type.
#[must_use]
pub fn panic_text(payload: &(dyn Any + Send)) -> &str {
    if let Some(text) = payload.downcast_ref::<&'static str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text
    } else {
        "non-string panic payload"
    }
}

/// How often a wait on a thread's end reads its clock.
const TICK: Duration = Duration::from_millis(50);

/// How [`wait_for`] ended.
enum Waited<R> {
    /// The thread signalled its end.
    Done,
    /// The check returned a value.
    Checked(R),
    /// The deadline passed.
    Elapsed,
}

/// Wait until `done` disconnects, `check` returns a value, or `clock` reads
/// `deadline`, reading the clock at least once a `tick` of real time.
///
/// The end is looked for before `check` runs and before the deadline is
/// read, so a thread that has ended by the deadline counts as done.
fn wait_for<R>(
    done: &crossbeam_channel::Receiver<()>,
    deadline: Option<Instant>,
    clock: &dyn Clock,
    tick: Duration,
    mut check: impl FnMut() -> Option<R>,
) -> Waited<R> {
    loop {
        let left = deadline.map_or(tick, |d| d.saturating_duration_since(clock.now()));
        if done.recv_timeout(left.min(tick)) != Err(RecvTimeoutError::Timeout) {
            return Waited::Done;
        }
        if let Some(value) = check() {
            return Waited::Checked(value);
        }
        if deadline.is_some_and(|d| clock.now() >= d) {
            return Waited::Elapsed;
        }
    }
}

/// An owned OS thread, joined when the handle drops.
///
/// Dropping the handle blocks until the thread ends, so an owner whose thread
/// might not end waits with [`join_by`](Self::join_by) instead. A panic in the
/// thread was logged when it happened; the drop discards it.
#[must_use = "dropping a ThreadHandle blocks until its thread ends"]
pub struct ThreadHandle<T> {
    inner: Option<std::thread::JoinHandle<T>>,
    /// Disconnects when the thread's function returns or unwinds.
    done: crossbeam_channel::Receiver<()>,
}

impl<T> ThreadHandle<T> {
    /// Wait for the thread to end and return its result.
    ///
    /// # Errors
    ///
    /// The panic payload when the thread panicked.
    pub fn join(mut self) -> std::thread::Result<T> {
        match self.inner.take() {
            Some(inner) => inner.join(),
            None => unreachable!("only `join`, `join_by` and `drop` take the handle, each once"),
        }
    }

    /// Whether the thread has ended.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }

    /// Wait for the thread to end until `clock` reads `deadline`, and return
    /// its result.
    ///
    /// The wait returns as soon as the thread ends, and reads `clock` every
    /// 50 ms of real time, so it can return up to that long after the
    /// deadline. A thread that has ended by then is joined. One that has not
    /// is left running with no owner, holding everything it owns, and `None`
    /// is returned.
    ///
    /// # Errors
    ///
    /// `Some(Err)` carries the panic payload when the thread panicked.
    pub fn join_by(
        mut self,
        deadline: Instant,
        clock: &dyn Clock,
    ) -> Option<std::thread::Result<T>> {
        match wait_for(
            &self.done,
            Some(deadline),
            clock,
            TICK,
            || None::<Infallible>,
        ) {
            Waited::Done => Some(self.join()),
            Waited::Checked(never) => match never {},
            Waited::Elapsed => {
                // A `JoinHandle` dropped unjoined detaches its thread.
                drop(self.inner.take());
                None
            }
        }
    }
}

impl<T> Drop for ThreadHandle<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.join();
        }
    }
}

impl<T> std::fmt::Debug for ThreadHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadHandle")
            .field("name", &self.inner.as_ref().and_then(|h| h.thread().name()))
            .finish_non_exhaustive()
    }
}

/// Spawn `f` on a new OS thread named `name`.
///
/// The thread logs to the `tracing` subscriber that is the default where
/// `spawn_thread` is called, or to the global default when none is set yet.
/// A panic in `f` is logged at `ERROR` with `name`
/// and returned by [`ThreadHandle::join`].
///
/// # Errors
///
/// The OS error when the thread cannot be created.
pub fn spawn_thread<F, T>(name: &'static str, f: F) -> std::io::Result<ThreadHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_named(Cow::Borrowed(name), f)
}

/// [`spawn_thread`] with a name built at run time.
pub(crate) fn spawn_named_thread<F, T>(name: String, f: F) -> std::io::Result<ThreadHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_named(Cow::Owned(name), f)
}

fn spawn_named<F, T>(name: Cow<'static, str>, f: F) -> std::io::Result<ThreadHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // With no subscriber set, the thread looks the default up when it logs,
    // so a global subscriber installed after the spawn still applies.
    let dispatch = tracing::dispatcher::get_default(|d| {
        (!d.is::<tracing::subscriber::NoSubscriber>()).then(|| d.clone())
    });
    let (finished, done) = crossbeam_channel::bounded::<()>(0);
    #[expect(
        clippy::disallowed_methods,
        reason = "the returned `ThreadHandle` joins the thread when dropped"
    )]
    let inner = std::thread::Builder::new()
        .name(name.clone().into_owned())
        .spawn(move || {
            // Dropped when this closure returns or unwinds, which wakes `join_by`.
            let _finished = finished;
            let run = move || match catch_unwind(AssertUnwindSafe(f)) {
                Ok(output) => output,
                Err(payload) => {
                    tracing::error!(
                        thread = &*name,
                        panic = panic_text(&*payload),
                        "background thread panicked"
                    );
                    resume_unwind(payload)
                }
            };
            match dispatch {
                Some(dispatch) => tracing::dispatcher::with_default(&dispatch, run),
                None => run(),
            }
        })?;
    Ok(ThreadHandle {
        inner: Some(inner),
        done,
    })
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;
    use tokio::sync::oneshot;

    /// How long a test waits on a signal. Time is paused, so a signal that
    /// never comes fails the test at once.
    const DEADLINE: Duration = Duration::from_secs(10);

    /// Sends on its channel when dropped, which shows the future holding it
    /// was dropped.
    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    fn drop_signal() -> (DropSignal, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (DropSignal(Some(tx)), rx)
    }

    async fn signalled(rx: oneshot::Receiver<()>) {
        tokio::time::timeout(DEADLINE, rx)
            .await
            .expect("signal within the deadline")
            .expect("signal sent");
    }

    /// The `ERROR` lines logged while `f` runs.
    fn error_lines<R>(f: impl FnOnce() -> R) -> (R, Vec<String>) {
        let capture = spate_test::LogCapture::new();
        let out = capture.during(tracing::Level::ERROR, f);
        (out, capture.lines())
    }

    /// Runs `f` on a paused current-thread runtime on this thread, so its
    /// tasks log to the subscriber [`error_lines`] installs.
    fn paused<F: Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(f)
    }

    /// Pins that dropping a handle aborts its task: the task's future is
    /// dropped although it never completes.
    #[test]
    fn dropping_the_handle_aborts_the_task() {
        paused(async {
            let (guard, dropped) = drop_signal();
            let handle = spawn("pending", async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            tokio::task::yield_now().await;
            assert!(!handle.is_finished());
            drop(handle);
            signalled(dropped).await;
        });
    }

    /// Pins that a handle dropped on a thread outside the runtime still
    /// aborts its task.
    #[test]
    fn dropping_the_handle_off_the_runtime_aborts_the_task() {
        paused(async {
            let (guard, dropped) = drop_signal();
            let handle = spawn("pending", async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            tokio::task::yield_now().await;
            std::thread::scope(|s| {
                s.spawn(move || drop(handle));
            });
            signalled(dropped).await;
        });
    }

    /// Pins that dropping the handle of a task that returned logs nothing.
    #[test]
    fn dropping_a_finished_handle_logs_nothing() {
        let ((), errors) = error_lines(|| {
            paused(async {
                let mut handle = spawn("finished", async { 1 });
                assert_eq!((&mut handle).await.unwrap(), 1);
                assert!(handle.is_finished());
                drop(handle);
            });
        });
        assert_eq!(errors, Vec::<String>::new());
    }

    /// Pins that a handle dropped after its runtime shut down neither panics
    /// nor logs.
    #[test]
    fn a_handle_outliving_its_runtime_drops_quietly() {
        let ((), errors) = error_lines(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let handle = spawn_on("orphan", std::future::pending::<()>(), runtime.handle());
            drop(runtime);
            drop(handle);
        });
        assert_eq!(errors, Vec::<String>::new());
    }

    /// Pins that awaiting a handle yields the task's output.
    #[test]
    fn awaiting_the_handle_yields_the_output() {
        let output = paused(async { spawn("answer", async { 6 * 7 }).await.unwrap() });
        assert_eq!(output, 42);
    }

    /// Pins that a detached task keeps running after its handle is gone.
    #[test]
    fn a_detached_task_runs_to_completion() {
        paused(async {
            let (go_tx, go_rx) = oneshot::channel::<()>();
            let (done_tx, done_rx) = oneshot::channel::<()>();
            spawn("detached", async move {
                go_rx.await.unwrap();
                done_tx.send(()).unwrap();
            })
            .detach();
            tokio::task::yield_now().await;
            go_tx.send(()).unwrap();
            signalled(done_rx).await;
        });
    }

    /// Pins that a panic in a task nobody awaits is logged once at `ERROR`,
    /// with the task's name and the panic's text, for a `&str` and a
    /// `String` payload.
    #[test]
    fn a_dropped_handles_panic_is_logged_with_its_name() {
        let ((), errors) = error_lines(|| {
            paused(async {
                let (guard, dropped) = drop_signal();
                let handle = spawn("static-panic", async move {
                    let _guard = guard;
                    panic!("boom");
                });
                signalled(dropped).await;
                drop(handle);

                let (guard, dropped) = drop_signal();
                let handle = spawn("formatted-panic", async move {
                    let _guard = guard;
                    panic!("boom {}", 1);
                });
                signalled(dropped).await;
                drop(handle);
            });
        });
        assert_eq!(
            errors,
            vec![
                r#"ERROR spate_core::task: background task panicked task="static-panic" panic="boom""#.to_owned(),
                r#"ERROR spate_core::task: background task panicked task="formatted-panic" panic="boom 1""#
                    .to_owned(),
            ]
        );
    }

    /// Pins that aborting a task is not reported as a panic.
    #[test]
    fn an_aborted_task_logs_no_panic() {
        let ((), errors) = error_lines(|| {
            paused(async {
                let (guard, dropped) = drop_signal();
                let handle = spawn("aborted", async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                });
                tokio::task::yield_now().await;
                handle.abort();
                signalled(dropped).await;
                assert!(handle.await.unwrap_err().is_cancelled());
            });
        });
        assert_eq!(errors, Vec::<String>::new());
    }

    /// Pins that an awaited panic reaches the awaiting caller as a panic
    /// `JoinError`, and is logged once, by the helper.
    #[test]
    fn an_awaited_panic_is_returned_and_logged_once() {
        let (joined, errors) = error_lines(|| {
            paused(async {
                spawn("awaited-panic", async {
                    panic!("awaited boom");
                })
                .await
            })
        });
        let err = joined.unwrap_err();
        assert!(err.is_panic());
        assert_eq!(panic_text(&*err.into_panic()), "awaited boom");
        assert_eq!(
            errors,
            vec![
                r#"ERROR spate_core::task: background task panicked task="awaited-panic" panic="awaited boom""#
                    .to_owned()
            ]
        );
    }

    /// Pins that a spawned thread carries its name and that dropping its
    /// handle returns only once the thread has ended.
    #[test]
    fn a_spawned_thread_is_named_and_joined_on_drop() {
        let name = Arc::new(Mutex::new(None));
        let ended = Arc::new(AtomicBool::new(false));
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let handle = {
            let name = Arc::clone(&name);
            let ended = Arc::clone(&ended);
            spawn_thread("spate-test-thread", move || {
                *name.lock().unwrap() = std::thread::current().name().map(str::to_owned);
                // Returns once the sender drops.
                let _ = go_rx.recv();
                // Widens the window in which a drop that does not join
                // would see the flag unset.
                for _ in 0..1000 {
                    std::thread::yield_now();
                }
                ended.store(true, Ordering::SeqCst);
            })
            .unwrap()
        };
        drop(go_tx);
        drop(handle);
        assert!(
            ended.load(Ordering::SeqCst),
            "drop returned before the thread ended"
        );
        assert_eq!(name.lock().unwrap().as_deref(), Some("spate-test-thread"));
    }

    /// Pins that each handle's `Debug` names what it owns.
    #[test]
    fn a_handle_debugs_as_its_name() {
        let task = paused(async {
            let handle = spawn("debugged-task", std::future::pending::<()>());
            format!("{handle:?}")
        });
        assert!(
            task.starts_with(r#"TaskHandle { name: "debugged-task", id: "#),
            "{task}"
        );
        let thread = format!("{:?}", spawn_thread("debugged-thread", || ()).unwrap());
        assert_eq!(
            thread,
            r#"ThreadHandle { name: Some("debugged-thread"), .. }"#
        );
    }

    /// Pins that a thread's panic is logged once at `ERROR` with its name,
    /// and returned by `join`.
    #[test]
    fn a_panicked_thread_is_logged_once() {
        let (joined, errors) = error_lines(|| {
            spawn_thread("panicking-thread", || -> () { panic!("thread boom") })
                .unwrap()
                .join()
        });
        let payload = joined.unwrap_err();
        assert_eq!(panic_text(&*payload), "thread boom");
        assert_eq!(
            errors,
            vec![
                r#"ERROR spate_core::task: background thread panicked thread="panicking-thread" panic="thread boom""#
                    .to_owned()
            ]
        );
    }

    /// Pins that each spawn path `clippy.toml` bans with no call site of its
    /// own resolves: clippy reports a banned call only at a path it resolves,
    /// so an entry naming no item leaves its `expect` here unfulfilled.
    #[test]
    fn banned_spawn_paths_resolve() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        #[expect(
            clippy::disallowed_methods,
            reason = "proves the `Runtime::spawn_blocking` entry resolves; awaited below"
        )]
        let on_runtime = runtime.spawn_blocking(|| 1);
        let total = runtime.block_on(async {
            let handle = tokio::runtime::Handle::current();
            #[expect(
                clippy::disallowed_methods,
                reason = "proves the `Handle::spawn_blocking` entry resolves; awaited below"
            )]
            let on_handle = handle.spawn_blocking(|| 2);
            let mut set = tokio::task::JoinSet::new();
            #[expect(
                clippy::disallowed_methods,
                reason = "proves the `JoinSet::spawn_on` entry resolves; joined below"
            )]
            set.spawn_on(async { 3 }, &handle);
            #[expect(
                clippy::disallowed_methods,
                reason = "proves the `JoinSet::spawn_blocking` entry resolves; joined below"
            )]
            set.spawn_blocking(|| 4);
            #[expect(
                clippy::disallowed_methods,
                reason = "proves the `JoinSet::spawn_blocking_on` entry resolves; joined below"
            )]
            set.spawn_blocking_on(|| 5, &handle);
            let mut total = on_runtime.await.unwrap() + on_handle.await.unwrap();
            while let Some(joined) = set.join_next().await {
                total += joined.unwrap();
            }
            total
        });
        assert_eq!(total, 15);
    }

    /// A frozen test clock that counts the reads made on one thread, so a test
    /// knows a waiter there has read the time it set.
    struct CountedClock {
        clock: Arc<crate::clock::TestClock>,
        reader: Mutex<Option<std::thread::ThreadId>>,
        reads: AtomicUsize,
    }

    impl CountedClock {
        fn new() -> Arc<Self> {
            Arc::new(CountedClock {
                clock: crate::clock::TestClock::frozen(),
                reader: Mutex::new(None),
                reads: AtomicUsize::new(0),
            })
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }

        /// Wait until the reader has read the clock again.
        fn wait_for_a_read(&self) {
            let seen = self.reads();
            spate_test::wait_until(spate_test::HANG, "the waiter reads the clock", || {
                self.reads() > seen + 1
            });
        }
    }

    impl Clock for CountedClock {
        fn now(&self) -> Instant {
            let mut reader = self.reader.lock().unwrap();
            let me = std::thread::current().id();
            if *reader.get_or_insert(me) == me {
                self.reads.fetch_add(1, Ordering::SeqCst);
            }
            self.clock.now()
        }
    }

    /// A thread that runs until its sender drops, then reports its end.
    fn held_thread() -> (ThreadHandle<u8>, mpsc::Sender<()>, mpsc::Receiver<()>) {
        let (release, released) = mpsc::channel::<()>();
        let (ended_tx, ended) = mpsc::channel::<()>();
        let handle = spawn_thread("spate-held", move || {
            let _ = released.recv();
            let _ = ended_tx.send(());
            7
        })
        .unwrap();
        (handle, release, ended)
    }

    /// `is_finished` reads false while the thread runs and true once it ends.
    #[test]
    fn is_finished_turns_true_when_the_thread_ends() {
        let (handle, release, _ended) = held_thread();
        assert!(!handle.is_finished());
        drop(release);
        spate_test::wait_until(spate_test::HANG, "the thread to end", || {
            handle.is_finished()
        });
        assert_eq!(handle.join().unwrap(), 7);
    }

    /// The clock never moves, so only the thread's end can return the wait.
    #[test]
    fn join_by_returns_the_result_of_a_thread_that_ends_in_time() {
        let clock = CountedClock::new();
        let (handle, release, _ended) = held_thread();
        let deadline = clock.now() + Duration::from_secs(1);
        drop(release);
        let joined = handle.join_by(deadline, &*clock);
        assert_eq!(joined.map(Result::unwrap), Some(7));
    }

    /// The wait holds until the clock reaches the deadline, then leaves the
    /// thread running on its own.
    #[test]
    fn join_by_gives_up_at_the_deadline_and_leaves_the_thread_running() {
        let clock = CountedClock::new();
        let (handle, release, ended) = held_thread();
        let deadline = clock.clock.now() + Duration::from_secs(1);
        let waiter = {
            let clock = Arc::clone(&clock);
            spawn_thread("spate-waiter", move || {
                handle.join_by(deadline, &*clock).map(Result::unwrap)
            })
            .unwrap()
        };
        // Bound after the waiter so it drops first: a failed assertion then
        // releases the thread, and the waiter it unblocks can be joined.
        let release = release;
        clock.wait_for_a_read();
        clock
            .clock
            .advance(Duration::from_secs(1) - Duration::from_millis(1));
        clock.wait_for_a_read();
        assert!(!waiter.is_finished(), "the wait ended before its deadline");
        clock.clock.advance(Duration::from_millis(1));
        assert_eq!(waiter.join().unwrap(), None);
        assert!(
            ended.try_recv().is_err(),
            "the thread ended before it was released"
        );
        drop(release);
        ended
            .recv_timeout(spate_test::HANG)
            .expect("the thread left running ends once released");
    }

    /// A thread that has ended is joined even when the clock already reads
    /// past the deadline.
    #[test]
    fn join_by_joins_a_thread_that_ended_by_the_deadline() {
        let clock = CountedClock::new();
        let (handle, release, _ended) = held_thread();
        let deadline = clock.now();
        drop(release);
        spate_test::wait_until(spate_test::HANG, "the thread to end", || {
            handle.is_finished()
        });
        clock.clock.advance(Duration::from_secs(1));
        assert_eq!(
            handle.join_by(deadline, &*clock).map(Result::unwrap),
            Some(7)
        );
    }

    /// With a tick far longer than the hang bound, only the end signal can
    /// return the wait in time.
    #[test]
    fn wait_for_returns_when_the_end_is_signalled() {
        let clock = CountedClock::new();
        let (finished, done) = crossbeam_channel::bounded::<()>(0);
        let (result_tx, result) = mpsc::channel();
        let waiter = spawn_thread("spate-waiter", move || {
            let waited = wait_for(&done, None, &*clock, spate_test::HANG * 10, || None::<()>);
            let _ = result_tx.send(matches!(waited, Waited::Done));
        })
        .unwrap();
        drop(finished);
        let waited = result.recv_timeout(spate_test::HANG);
        if waited.is_err() {
            // Joining would hold the failure for the waiter's whole tick.
            let clock = crate::clock::TestClock::frozen();
            let _ = waiter.join_by(clock.now(), &*clock);
            panic!("the wait did not return on the end signal");
        }
        assert_eq!(waited, Ok(true));
        waiter.join().unwrap();
    }
}
