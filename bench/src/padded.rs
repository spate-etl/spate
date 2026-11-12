//! A value on cache lines of its own.

/// Holds a value on cache lines no other `Padded` value shares.
///
/// Two atomics that different threads write on every step cost one cache line
/// each when they sit apart, and move one shared line between the cores when
/// they sit together. The alignment is 128 rather than 64 because Intel's
/// spatial prefetcher pulls lines in pairs and Apple Silicon's lines are 128
/// bytes, the figure `crossbeam_utils::CachePadded` uses on both.
#[derive(Debug, Default)]
#[repr(align(128))]
pub struct Padded<T>(pub T);

const _: () = assert!(align_of::<Padded<u8>>() == 128);
