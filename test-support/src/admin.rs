//! The address a pipeline's admin server bound, read off its start log.

use std::net::SocketAddr;

/// The message a pipeline logs once its admin server has bound.
const LISTENING: &str = "admin server listening";

/// The `addr` field of the first `admin server listening` line in `lines`,
/// or `None` while no such line has been written.
///
/// `lines` are what a `tracing` fmt subscriber writes, such as
/// `spate_test::LogCapture::lines`. A capture that saw two pipelines start
/// holds two such lines, and this returns the first, so give each pipeline a
/// capture of its own.
///
/// # Panics
///
/// When the line has no `addr=` field that parses as a socket address. Read
/// as "not yet", a changed log format would turn into a hang.
pub fn admin_addr(lines: &[String]) -> Option<SocketAddr> {
    let line = lines.iter().find(|line| line.contains(LISTENING))?;
    let addr = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("addr="))
        .unwrap_or_else(|| panic!("no `addr` field on the admin server's line: {line}"));
    let addr = addr
        .parse()
        .unwrap_or_else(|e| panic!("`addr={addr}` is not a socket address ({e}): {line}"));
    Some(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|line| (*line).to_owned()).collect()
    }

    #[test]
    fn reads_the_address_off_the_listening_line() {
        let lines = lines(&[
            " INFO spate_core::telemetry::cpu: cpu features arch=\"aarch64\"",
            " INFO spate_core::pipeline::runtime: admin server listening addr=127.0.0.1:41234",
        ]);
        assert_eq!(admin_addr(&lines), Some("127.0.0.1:41234".parse().unwrap()));
    }

    #[test]
    fn an_addr_field_under_another_message_is_not_the_admin_address() {
        let lines = lines(&[
            " INFO spate_kafka::source: connected addr=127.0.0.1:9092",
            " WARN spate_clickhouse::writer: replica slow addr=127.0.0.1:8123",
        ]);
        assert_eq!(admin_addr(&lines), None);
    }

    #[test]
    fn two_listening_lines_give_the_first_address() {
        let lines = lines(&[
            " INFO spate_core::pipeline::runtime: admin server listening addr=127.0.0.1:41234",
            " INFO spate_core::pipeline::runtime: admin server listening addr=127.0.0.1:41235",
        ]);
        assert_eq!(admin_addr(&lines), Some("127.0.0.1:41234".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "no `addr` field on the admin server's line")]
    fn a_listening_line_without_an_addr_field_panics() {
        admin_addr(&lines(&[
            " INFO spate_core::pipeline::runtime: admin server listening",
        ]));
    }

    #[test]
    #[should_panic(expected = "`addr=port-9090` is not a socket address")]
    fn an_addr_that_is_no_socket_address_panics() {
        admin_addr(&lines(&[
            " INFO spate_core::pipeline::runtime: admin server listening addr=port-9090",
        ]));
    }
}
