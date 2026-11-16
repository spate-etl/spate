//! A local TCP server that hands each connection to a handler on a thread of
//! its own.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

/// Binds a port on `127.0.0.1` and serves it with [`serve_each_on`].
///
/// # Panics
///
/// Panics when no local port can be bound.
pub fn serve_each(handle: impl Fn(TcpStream) + Send + Sync + 'static) -> SocketAddr {
    serve_each_on(
        TcpListener::bind("127.0.0.1:0").expect("bind a local port"),
        handle,
    )
}

/// Accepts on `listener` from a detached thread and runs `handle` on a new
/// thread for each connection, skipping a failed accept. Returns the
/// listener's address. The threads serve until the process exits.
///
/// # Panics
///
/// Panics when the listener has no local address.
#[expect(
    clippy::disallowed_methods,
    reason = "the server's threads serve until the test process exits"
)]
pub fn serve_each_on(
    listener: TcpListener,
    handle: impl Fn(TcpStream) + Send + Sync + 'static,
) -> SocketAddr {
    let addr = listener.local_addr().expect("local address");
    let handle = Arc::new(handle);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || handle(stream));
        }
    });
    addr
}
