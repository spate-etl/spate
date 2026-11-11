//! Replicas that stop answering, and a clock moved past the startup bound,
//! for the crate's unit tests.

use spate_core::clock::{SyncClock, TestClock};
use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

/// A runtime with one worker, so a spawned call runs while the test's thread
/// waits on it.
pub(crate) fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build a runtime")
}

/// A frozen clock, and the same clock in the form a bound takes.
pub(crate) fn frozen() -> (Arc<TestClock>, SyncClock) {
    let clock = TestClock::frozen();
    let given = SyncClock::new(Arc::clone(&clock) as _);
    (clock, given)
}

/// A replica on `127.0.0.1` that relays its first `answered` connections to
/// `mock` and holds every later one open with no answer. The mock closes a
/// connection after one request, so a connection is a request.
///
/// Returns the replica's URL, on the mock's host, and a receiver that yields once per held
/// connection.
pub(crate) fn silent_after(
    io: &Runtime,
    mock: &clickhouse::test::Mock,
    answered: usize,
) -> (String, mpsc::Receiver<()>) {
    let (mocked, port) = mock.url().rsplit_once(':').expect("a mock URL has a port");
    let upstream = format!("127.0.0.1:{port}");
    let (holding, held) = mpsc::channel();
    let listener = io
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("bind a local port");
    // The mock's host, so the client reads a relayed answer as it reads the
    // mock's own.
    let url = format!(
        "{mocked}:{}",
        listener.local_addr().expect("local address").port()
    );
    io.spawn(async move {
        let mut silent = Vec::new();
        for seen in 0.. {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            if seen < answered {
                let mut to = TcpStream::connect(&upstream).await.expect("reach the mock");
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut to).await;
            } else {
                silent.push(stream);
                let _ = holding.send(());
            }
        }
    });
    (url, held)
}

/// `call`'s output, with `clock` moved past `limit` once `held` has yielded.
pub(crate) fn past_the_bound<T: Send + 'static>(
    io: &Runtime,
    clock: &TestClock,
    limit: Duration,
    held: &mpsc::Receiver<()>,
    call: impl Future<Output = T> + Send + 'static,
) -> T {
    let (tx, done) = mpsc::channel();
    io.spawn(async move {
        let _ = tx.send(call.await);
    });
    held.recv_timeout(spate_test::HANG)
        .expect("the request is held");
    let mut got = None;
    spate_test::wait_until(spate_test::HANG, "the call ends", || {
        clock.advance(limit);
        got = done.recv_timeout(Duration::from_millis(100)).ok();
        got.is_some()
    });
    got.expect("the call ended")
}
