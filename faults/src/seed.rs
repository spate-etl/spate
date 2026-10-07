//! The seeded generator every fault-run draw comes from.

/// SplitMix64. The same seed yields the same stream on any build and
/// toolchain. Not cryptographic.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// A generator starting at `seed`.
    #[must_use]
    pub fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }

    /// The generator for `scenario` under the run seed `seed`.
    #[must_use]
    pub fn for_scenario(seed: u64, scenario: &str) -> SplitMix64 {
        SplitMix64::new(seed ^ spate_test_support::fnv1a(scenario.as_bytes()))
    }

    /// The next value in the stream.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `[low, high]`, biased when the span is not a power of two,
    /// negligibly only for spans far below 2^64.
    ///
    /// # Panics
    ///
    /// Panics when `low > high`.
    pub fn in_range(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "empty range {low}..={high}");
        match (high - low).checked_add(1) {
            Some(span) => low + self.next_u64() % span,
            None => self.next_u64(),
        }
    }
}

/// Parses a run seed written in decimal or as `0x`-prefixed hex.
#[must_use]
pub fn parse(text: &str) -> Option<u64> {
    let text = text.trim();
    match text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => text.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seed reads the same in decimal and in `0x` hex.
    #[test]
    fn seed_parses_decimal_and_hex() {
        assert_eq!(parse("255"), Some(255));
        assert_eq!(parse("0xff"), Some(255));
        assert_eq!(parse("0XFF"), Some(255));
        assert_eq!(parse("ff"), None);
        assert_eq!(parse(""), None);
    }

    /// The stream matches the reference SplitMix64 outputs for seed 1234567,
    /// and a scenario's stream depends on its name.
    #[test]
    fn splitmix_output_is_pinned() {
        let mut rng = SplitMix64::new(1_234_567);
        let got: Vec<u64> = (0..5).map(|_| rng.next_u64()).collect();
        assert_eq!(
            got,
            [
                6_457_827_717_110_365_317,
                3_203_168_211_198_807_973,
                9_817_491_932_198_370_423,
                4_593_380_528_125_082_431,
                16_408_922_859_458_223_821,
            ]
        );

        let mut a = SplitMix64::for_scenario(7, "nats_one_instance");
        let mut b = SplitMix64::for_scenario(7, "nats_three_instances");
        assert_ne!(a.next_u64(), b.next_u64());
        let mut rng = SplitMix64::new(9);
        assert!((0..1000).all(|_| (3..=5).contains(&rng.in_range(3, 5))));
        assert_eq!(SplitMix64::new(9).in_range(4, 4), 4);
    }

    /// A scenario's stream is the stream at `seed ^ fnv1a(scenario)`, so it
    /// changes with the run seed.
    #[test]
    fn scenario_stream_depends_on_the_seed() {
        let mut got = SplitMix64::for_scenario(7, "nats_one_instance");
        let mut want = SplitMix64::new(7 ^ spate_test_support::fnv1a(b"nats_one_instance"));
        assert_eq!(got.next_u64(), want.next_u64());
        assert_ne!(
            SplitMix64::for_scenario(7, "nats_one_instance").next_u64(),
            SplitMix64::for_scenario(8, "nats_one_instance").next_u64()
        );
    }

    /// `in_range` over the whole `u64` span returns the next raw value.
    #[test]
    fn in_range_over_the_full_span_is_the_raw_value() {
        assert_eq!(
            SplitMix64::new(9).in_range(0, u64::MAX),
            SplitMix64::new(9).next_u64()
        );
    }
}
