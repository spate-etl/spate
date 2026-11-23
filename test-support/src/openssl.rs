//! OpenSSL's exit handler in a test process that builds Kafka clients.

/// Leaves OpenSSL's exit handler unregistered, as spate-kafka's client
/// constructor does. librdkafka seeds its random numbers through OpenSSL in the
/// first client a process builds, and that call registers the handler unless
/// OpenSSL was initialised without it first. The handler then frees state
/// librdkafka's threads still use, which kills a process that exits with a
/// client alive.
///
/// A test calls it before it builds a Kafka client of its own. It does nothing
/// without `kafka-tls`.
pub fn leave_openssl_running() {
    #[cfg(feature = "kafka-tls")]
    openssl_sys::init();
}

/// Runs the test `name` again in a child process, where `build` builds and
/// drops a client after the child registers an exit handler of its own. That
/// handler runs after any OpenSSL registers later, and aborts if OpenSSL
/// refuses to initialise because its cleanup has run.
///
/// # Panics
///
/// Panics when the child fails, as [`run_in_child`](crate::run_in_child)
/// does, and in the child when the exit handler cannot be registered.
#[cfg(feature = "kafka-tls")]
pub fn assert_openssl_runs_through_exit(name: &str, build: impl FnOnce()) {
    use std::ffi::c_int;

    const CHILD: &str = "SPATE_TEST_OPENSSL_EXIT";

    unsafe extern "C" {
        fn atexit(handler: extern "C" fn()) -> c_int;
    }

    extern "C" fn abort_after_openssl_cleanup() {
        // SAFETY: a null settings pointer selects OpenSSL's defaults.
        if unsafe { openssl_sys::OPENSSL_init_ssl(0, std::ptr::null()) } != 1 {
            std::process::abort();
        }
    }

    if std::env::var_os(CHILD).is_none() {
        crate::run_in_child(name, |child| child.env(CHILD, "1"));
        return;
    }
    // SAFETY: the handler is a function item, valid for the whole process.
    assert_eq!(unsafe { atexit(abort_after_openssl_cleanup) }, 0);
    build();
}
