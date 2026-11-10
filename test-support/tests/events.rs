//! The event corpus is pinned and every line is one typed record.

use spate_test_support::events::{Event, event_line};
use spate_test_support::pin;

/// Lines the tests generate.
const LINES: u64 = 4_096;

fn corpus() -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..LINES {
        event_line(i, &mut out);
    }
    out
}

/// Pins the bytes of the first 4,096 lines, so a benchmark that generates the
/// corpus on two revisions measures the same input.
#[test]
fn the_corpus_bytes_are_pinned() {
    assert_eq!(pin(&corpus()), (2_567_516, 16_179_727_331_478_610_702));
}

/// Every line ends in its only newline and decodes into [`Event`].
#[test]
fn every_line_is_one_event() {
    let corpus = corpus();
    let lines: Vec<&[u8]> = corpus.split_inclusive(|b| *b == b'\n').collect();
    assert_eq!(lines.len() as u64, LINES);
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line.iter().filter(|b| **b == b'\n').count(), 1, "line {i}");
        let event: Event = serde_json::from_slice(line)
            .unwrap_or_else(|e| panic!("line {i} does not decode: {e}"));
        assert!((30..=100).contains(&event.message.len()), "line {i}");
        assert!(!event.tags.is_empty() && event.tags.len() <= 4, "line {i}");
        assert!((2..=4).contains(&event.labels.len()), "line {i}");
    }
}

/// A line averages about 600 bytes, the size the corpus is meant to have.
#[test]
fn a_line_averages_about_600_bytes() {
    let mean = corpus().len() as u64 / LINES;
    assert!((560..=640).contains(&mean), "mean line length {mean}");
}
