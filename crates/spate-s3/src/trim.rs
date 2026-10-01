//! The byte trim for a split that reads a [`SplitRange`] of one object.
//!
//! A range `[start, end)` owns the records whose first byte lies inside it,
//! where a record starts at byte 0 and after every delimiter. [`RangeTrim`]
//! takes the object's bytes in contiguous windows and yields exactly the
//! owned bytes: everything from the first owned record's first byte through
//! the delimiter that ends the last owned record, or through the object's end.

use crate::split::SplitRange;
use bytes::Bytes;

/// Where a [`RangeTrim`] is within its range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Dropping bytes through the first delimiter in `[start - 1, end - 1)`.
    Seek,
    /// Forwarding owned bytes.
    Emit,
    /// Every owned byte has been forwarded.
    Done,
}

/// Cuts one range's owned bytes out of contiguous windows of its object.
///
/// The bytes it yields depend only on the object's bytes, the range and the
/// delimiter, never on where the windows start or end.
#[derive(Debug)]
pub(crate) struct RangeTrim {
    start: u64,
    end: u64,
    delimiter: u8,
    size: u64,
    phase: Phase,
}

impl RangeTrim {
    /// The trim for `range` of an object of `size` bytes, where
    /// `range.start < range.end <= size`.
    pub(crate) fn new(range: SplitRange, size: u64) -> RangeTrim {
        debug_assert!(range.start < range.end && range.end <= size);
        RangeTrim {
            start: range.start,
            end: range.end,
            delimiter: range.delimiter,
            size,
            phase: if range.start == 0 {
                Phase::Emit
            } else {
                Phase::Seek
            },
        }
    }

    /// The first byte to read: `start - 1`, so a delimiter there marks a
    /// record starting at `start`.
    pub(crate) fn first_byte(&self) -> u64 {
        self.start.saturating_sub(1)
    }

    /// The end of the next window to read from `pos`, or `None` once every
    /// owned byte is out or the object is exhausted.
    ///
    /// A window is `range_bytes` long until it reaches `end`; the window that
    /// does ends `chunk_bytes` past `end`, and later ones are `chunk_bytes`
    /// long. Each is cut at the object's end and is never empty.
    pub(crate) fn next_window(&self, pos: u64, range_bytes: u64, chunk_bytes: u64) -> Option<u64> {
        if self.phase == Phase::Done || pos >= self.size {
            return None;
        }
        let (range_bytes, chunk_bytes) = (range_bytes.max(1), chunk_bytes.max(1));
        // Saturating: `size` and the range come from a descriptor another
        // process wrote, and may sit close to `u64::MAX`.
        let end = if pos >= self.end {
            pos.saturating_add(chunk_bytes)
        } else if pos.saturating_add(range_bytes) >= self.end {
            self.end.saturating_add(chunk_bytes)
        } else {
            pos + range_bytes
        };
        Some(end.min(self.size))
    }

    /// The owned bytes of `window`, the object's bytes from `pos`. Windows
    /// arrive in order, each starting where the previous one ended.
    pub(crate) fn feed(&mut self, pos: u64, window: Bytes) -> Bytes {
        let len = window.len() as u64;
        let mut from = 0;
        if self.phase == Phase::Seek {
            debug_assert!(pos >= self.first_byte(), "window before the range");
            // A delimiter before `end - 1` starts a record inside the range.
            let seek_len = (self.end - 1).saturating_sub(pos).min(len) as usize;
            match window[..seek_len].iter().position(|&b| b == self.delimiter) {
                Some(at) => {
                    self.phase = Phase::Emit;
                    from = at + 1;
                }
                None => {
                    if pos + len >= self.end - 1 {
                        // No record starts inside the range.
                        self.phase = Phase::Done;
                    }
                    return Bytes::new();
                }
            }
        }
        if self.phase != Phase::Emit {
            return Bytes::new();
        }
        // The first delimiter at or after `end - 1` ends the last owned record.
        let tail_from = ((self.end - 1).saturating_sub(pos).min(len) as usize).max(from);
        match window[tail_from..]
            .iter()
            .position(|&b| b == self.delimiter)
        {
            Some(at) => {
                self.phase = Phase::Done;
                window.slice(from..=tail_from + at)
            }
            None => window.slice(from..),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Drive a trim over `object` in windows cut at `cuts` (sorted offsets
    /// past the first byte), stopping once it is done.
    fn trimmed(object: &[u8], range: SplitRange, cuts: &[usize]) -> Vec<u8> {
        let mut trim = RangeTrim::new(range, object.len() as u64);
        let mut pos = trim.first_byte() as usize;
        let mut out = Vec::new();
        let first = pos;
        let ends = cuts
            .iter()
            .copied()
            .filter(|&c| c > first && c < object.len())
            .chain([object.len()]);
        for end in ends {
            if trim.phase == Phase::Done {
                break;
            }
            let window = Bytes::copy_from_slice(&object[pos..end]);
            out.extend_from_slice(&trim.feed(pos as u64, window));
            pos = end;
        }
        out
    }

    /// Drive a trim the way the fetcher does, through `next_window`.
    fn scheduled(object: &[u8], range: SplitRange, range_bytes: u64, chunk: u64) -> Vec<u8> {
        let mut trim = RangeTrim::new(range, object.len() as u64);
        let mut pos = trim.first_byte();
        let mut out = Vec::new();
        while let Some(end) = trim.next_window(pos, range_bytes, chunk) {
            assert!(end > pos, "an empty window at {pos}");
            let window = Bytes::copy_from_slice(&object[pos as usize..end as usize]);
            out.extend_from_slice(&trim.feed(pos, window));
            pos = end;
        }
        out
    }

    fn range(start: u64, end: u64) -> SplitRange {
        SplitRange::new(start, end, b'\n')
    }

    #[test]
    fn a_delimiter_at_start_minus_one_starts_the_range_on_a_record() {
        // Records start at 0, 3 and 6.
        let object = b"ab\ncd\nef\n";
        assert_eq!(trimmed(object, range(3, 6), &[]), b"cd\n");
        assert_eq!(trimmed(object, range(0, 3), &[]), b"ab\n");
        assert_eq!(trimmed(object, range(6, 9), &[]), b"ef\n");
    }

    #[test]
    fn a_record_straddling_the_end_is_read_to_its_delimiter() {
        let object = b"ab\ncdef\ngh";
        assert_eq!(trimmed(object, range(0, 5), &[]), b"ab\ncdef\n");
        assert_eq!(trimmed(object, range(5, 10), &[]), b"gh");
    }

    #[test]
    fn a_range_inside_one_record_owns_nothing() {
        let object = b"a\nbcdefgh\ni";
        assert_eq!(trimmed(object, range(3, 7), &[]), b"");
        assert_eq!(trimmed(object, range(0, 3), &[]), b"a\nbcdefgh\n");
        assert_eq!(trimmed(object, range(7, 11), &[]), b"i");
    }

    #[test]
    fn scheduled_windows_never_run_past_the_terminator_or_the_object() {
        let object = b"0123456789\nabcdefghij\n";
        let trim = RangeTrim::new(range(5, 15), object.len() as u64);
        assert_eq!(trim.first_byte(), 4);
        // The window reaching `end` extends `chunk_bytes` past it.
        assert_eq!(trim.next_window(4, 8, 3), Some(12));
        assert_eq!(trim.next_window(12, 8, 3), Some(18));
        assert_eq!(trim.next_window(15, 8, 3), Some(18));
        assert_eq!(trim.next_window(21, 8, 3), Some(22));
        assert_eq!(trim.next_window(22, 8, 3), None);
        assert_eq!(scheduled(object, range(5, 15), 8, 3), b"abcdefghij\n");
    }

    /// An object of short lines, with `\r\n`, whitespace-only and long lines,
    /// and an unterminated final line.
    fn arb_object() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            prop_oneof![Just(b'\n'), Just(b'\r'), Just(b' '), Just(b'x'), Just(b'y')],
            1..200,
        )
    }

    /// Sorted, deduplicated cut points strictly inside `1..len`.
    fn cut_points(seeds: &[usize], len: usize) -> Vec<usize> {
        let mut cuts: Vec<usize> = seeds
            .iter()
            .filter(|_| len > 1)
            .map(|s| 1 + s % (len - 1))
            .collect();
        cuts.sort_unstable();
        cuts.dedup();
        cuts
    }

    proptest! {
        /// Any partition of any object into ranges, read in any windows,
        /// yields owned streams that concatenate to the object, and each
        /// non-empty one starts on a record.
        #[test]
        fn owned_streams_tile_the_object(
            object in arb_object(),
            range_seeds in proptest::collection::vec(any::<usize>(), 0..12),
            window_seeds in proptest::collection::vec(any::<usize>(), 0..24),
        ) {
            let bounds: Vec<usize> = std::iter::once(0)
                .chain(cut_points(&range_seeds, object.len()))
                .chain([object.len()])
                .collect();
            let windows = cut_points(&window_seeds, object.len());
            let mut joined = Vec::new();
            for pair in bounds.windows(2) {
                let owned = trimmed(&object, range(pair[0] as u64, pair[1] as u64), &windows);
                if !owned.is_empty() {
                    let at = joined.len();
                    prop_assert!(at == 0 || object[at - 1] == b'\n', "owned stream at {} is mid-record", at);
                }
                joined.extend_from_slice(&owned);
            }
            prop_assert_eq!(joined, object);
        }

        /// The fetcher's schedule yields the same owned bytes as arbitrary
        /// windows, for any window and chunk size.
        #[test]
        fn the_window_schedule_does_not_change_the_owned_bytes(
            object in arb_object(),
            a in any::<usize>(),
            b in any::<usize>(),
            range_bytes in 1u64..40,
            chunk in 1u64..16,
        ) {
            let (x, y) = (a % object.len(), b % object.len());
            let r = range(x.min(y) as u64, x.max(y) as u64 + 1);
            prop_assert_eq!(scheduled(&object, r, range_bytes, chunk), trimmed(&object, r, &[]));
        }
    }
}
