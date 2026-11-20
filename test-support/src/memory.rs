//! This process's memory, as the operating system reports it.

use std::process::Command;

/// This process's resident memory in KiB, from `ps`. It holds the code under
/// test and the test's own clients, so it shows growth between two readings
/// and says nothing of one component's footprint.
///
/// # Panics
///
/// Panics when `ps` cannot be run, fails, or prints no number.
#[must_use]
pub fn resident_kib() -> u64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .expect("run ps");
    assert!(out.status.success(), "ps failed: {out:?}");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("a resident size")
}

#[cfg(test)]
mod tests {
    use super::resident_kib;

    /// A test process holds more than a mebibyte, so a reading below that is
    /// no reading.
    #[test]
    fn a_test_process_holds_over_a_mebibyte() {
        assert!(resident_kib() > 1024, "{} KiB", resident_kib());
    }
}
