//! The tail of a container's output, as a failed start or an exited node
//! reports it.

use std::fmt;

/// The lines of each container stream a report keeps. A service's reason
/// for stopping is the last thing it writes.
const LOG_TAIL: usize = 200;

/// A container stream as text, or why it could not be read.
pub(crate) fn container_output<E: fmt::Display>(read: Result<Vec<u8>, E>) -> String {
    match read {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(e) => format!("(unreadable: {e})"),
    }
}

/// The last [`LOG_TAIL`] lines of a container's stdout and stderr, each
/// under a heading.
pub(crate) fn container_tails(what: &str, stdout: &str, stderr: &str) -> String {
    format!(
        "---- last {LOG_TAIL} lines of {what} stdout ----\n{}\n\
         ---- last {LOG_TAIL} lines of {what} stderr ----\n{}",
        tail(stdout),
        tail(stderr),
    )
}

/// The last [`LOG_TAIL`] lines of `log`.
fn tail(log: &str) -> String {
    let lines: Vec<&str> = log.lines().collect();
    lines[lines.len().saturating_sub(LOG_TAIL)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A container stream reads as its text, or as why it could not be read.
    #[test]
    fn a_container_stream_reads_as_text_or_its_error() {
        let read: Result<Vec<u8>, String> = Ok(b"a line".to_vec());
        assert_eq!(container_output(read), "a line");
        let unread: Result<Vec<u8>, String> = Err("no such container".to_owned());
        assert_eq!(container_output(unread), "(unreadable: no such container)");
    }

    /// The report after a failed start carries each stream under its heading.
    #[test]
    fn the_container_tails_carry_both_streams() {
        assert_eq!(
            container_tails("Garage", "out line", "err line"),
            "---- last 200 lines of Garage stdout ----\nout line\n\
             ---- last 200 lines of Garage stderr ----\nerr line"
        );
    }

    /// A container log longer than the tail keeps only its last lines.
    #[test]
    fn a_long_log_keeps_its_last_lines() {
        let log: Vec<String> = (0..=LOG_TAIL).map(|n| n.to_string()).collect();
        let kept = tail(&log.join("\n"));
        assert_eq!(kept, log[1..].join("\n"));
        assert_eq!(tail("one\ntwo"), "one\ntwo");
    }
}
