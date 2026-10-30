//! Spawning tokio tasks and OS threads through handles that own them.
//!
//! [`spawn`] and [`spawn_on`] start a tokio task and return a [`TaskHandle`],
//! which aborts the task when dropped. [`spawn_thread`] starts a named OS
//! thread and returns a [`ThreadHandle`], which joins the thread when dropped.
//! Each spawn takes a name, and a panic in the task or thread is logged at
//! `ERROR` with that name. The panic still reaches a caller that awaits or
//! joins the handle.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::{JoinError, JoinHandle};

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

/// An owned OS thread, joined when the handle drops.
///
/// Dropping the handle blocks until the thread ends, so only an owner whose
/// thread ends on a bounded signal holds one. A panic in the thread was
/// logged when it happened; the drop discards it.
#[must_use = "dropping a ThreadHandle blocks until its thread ends"]
pub struct ThreadHandle<T> {
    inner: Option<std::thread::JoinHandle<T>>,
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
            None => unreachable!("only `join` and `drop` take the handle, each once"),
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
    // With no subscriber set, the thread looks the default up when it logs,
    // so a global subscriber installed after the spawn still applies.
    let dispatch = tracing::dispatcher::get_default(|d| {
        (!d.is::<tracing::subscriber::NoSubscriber>()).then(|| d.clone())
    });
    #[expect(
        clippy::disallowed_methods,
        reason = "the returned `ThreadHandle` joins the thread when dropped"
    )]
    let inner = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let run = move || match catch_unwind(AssertUnwindSafe(f)) {
                Ok(output) => output,
                Err(payload) => {
                    tracing::error!(
                        thread = name,
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
    Ok(ThreadHandle { inner: Some(inner) })
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
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

    /// The `ERROR` lines logged while `f` runs, without time or target.
    fn error_lines<R>(f: impl FnOnce() -> R) -> (R, Vec<String>) {
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
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
            .without_time()
            .with_target(false)
            .finish();
        let out = tracing::subscriber::with_default(subscriber, f);
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines = text
            .lines()
            .filter(|l| l.trim_start().starts_with("ERROR"))
            .map(|l| l.trim().to_owned())
            .collect();
        (out, lines)
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
                r#"ERROR background task panicked task="static-panic" panic="boom""#.to_owned(),
                r#"ERROR background task panicked task="formatted-panic" panic="boom 1""#
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
                r#"ERROR background task panicked task="awaited-panic" panic="awaited boom""#
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
                r#"ERROR background thread panicked thread="panicking-thread" panic="thread boom""#
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
}
