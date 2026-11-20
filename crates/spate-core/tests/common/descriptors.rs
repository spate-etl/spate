//! The process's descriptor table, for the admin server tests that fill it or
//! need more of it. Each test that changes the limit runs in a binary of its
//! own, since the limit belongs to the process.

use std::fs::File;

/// Descriptors this process holds open.
pub(crate) fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd").expect("/dev/fd").count()
}

fn limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid, writable `rlimit`.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    assert_eq!(rc, 0, "getrlimit");
    limit
}

fn set_soft_limit(soft: libc::rlim_t) {
    let limit = libc::rlimit {
        rlim_cur: soft,
        rlim_max: limit().rlim_max,
    };
    // SAFETY: `limit` is a valid `rlimit` whose soft value the callers keep
    // at or below the hard one.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) };
    assert_eq!(rc, 0, "setrlimit");
}

/// Raises the soft descriptor limit to at least `wanted`, or fails the test
/// when the hard limit is lower.
#[allow(
    dead_code,
    reason = "used by one of the binaries that include this module"
)]
pub(crate) fn raise_to(wanted: usize) {
    let hard = limit().rlim_max;
    let wanted = libc::rlim_t::try_from(wanted).expect("a descriptor count");
    assert!(
        hard >= wanted,
        "the test needs {wanted} descriptors, the hard limit is {hard}"
    );
    set_soft_limit(wanted);
}

/// Every descriptor the process may open, held but the one [`Full::take_last`]
/// leaves for the caller.
#[allow(dead_code, reason = "used by the binaries that include this module")]
pub(crate) struct Full {
    fillers: Vec<File>,
}

/// Lowers the soft descriptor limit a little above what is open, and opens
/// `/dev/null` until the process is out of descriptors.
#[allow(dead_code, reason = "used by the binaries that include this module")]
pub(crate) fn fill() -> Full {
    let open = libc::rlim_t::try_from(open_fds()).expect("a descriptor count");
    set_soft_limit(open + 16);
    let mut fillers = Vec::new();
    loop {
        match File::open("/dev/null") {
            Ok(file) => fillers.push(file),
            Err(err) if err.raw_os_error() == Some(libc::EMFILE) => break,
            Err(err) => panic!("open /dev/null: {err}"),
        }
    }
    assert!(!fillers.is_empty(), "the lowered limit left room to fill");
    Full { fillers }
}

#[allow(dead_code, reason = "used by the binaries that include this module")]
impl Full {
    /// Frees one descriptor, for the caller's next open to take. The process
    /// is out of descriptors again once it does.
    pub(crate) fn take_last(&mut self) {
        self.fillers.pop().expect("a filler to free");
    }

    /// Frees every descriptor the fill took.
    pub(crate) fn release(self) {
        drop(self.fillers);
    }
}
