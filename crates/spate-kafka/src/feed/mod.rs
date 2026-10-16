//! The Kafka work feed.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the feed backend in the next change reads it")
)]
mod config;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the feed backend in the next change returns it")
)]
mod error;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the feed backend in the next change tracks offsets with it"
    )
)]
mod inflight;
