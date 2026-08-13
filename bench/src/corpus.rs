//! The digest that proves two legs measured the same bytes, and the one that
//! says what compiled.
//!
//! Every case builds its own input in `setup`, on both legs, from the same
//! seed. That is a claim rather than a guarantee: a change to a generator, a
//! `HashMap` iteration order leaking into the build, a corpus sized from
//! something environmental — any of those makes the two legs different
//! workloads, and the timing difference that follows reads exactly like a
//! performance change.
//!
//! [`Corpus`] is how the claim is checked. A case absorbs each input it built,
//! the digest travels in the record, and the comparator refuses to pair two
//! records whose digests differ. It is a streaming hash, so absorbing is cheap
//! enough to do unconditionally and the result is sensitive to the *order* of
//! the inputs as well as their contents.
//!
//! # Two channels, because they answer opposite questions
//!
//! [`absorb`](Corpus::absorb) takes what the measured region *consumed*, and
//! the comparator requires it to match: two legs that read different bytes are
//! not measuring one thing.
//!
//! [`declare`](Corpus::declare) takes what *compiled* — a `cfg`-selected
//! constant naming the subject a feature arm swapped in. It feeds a separate
//! digest, and the comparator's requirement inverts with the axis: two builds
//! of one commit must declare the same subject, and two feature arms must
//! declare different ones. Folded into the corpus digest instead, a
//! feature-arm comparison could only proceed by waiving the guard on the bytes,
//! which is the guard that most needs to hold in exactly that run.

use std::hash::Hasher;

use twox_hash::XxHash64;

/// A running digest over everything a case fed its measured region, and a
/// second over what it declared about the build.
///
/// Sensitive to the label, the length and the order of every absorbed input —
/// see `digest_is_sensitive_to_label_length_and_order`.
#[derive(Debug)]
pub struct Corpus {
    hasher: XxHash64,
    inputs: u32,
    bytes: u64,
    /// Absent until something is declared, so a case that declares nothing
    /// asserts nothing. `Some` of an empty stream and `None` are different
    /// claims, and only the second is "this case has no compiled subject to
    /// compare".
    build: Option<XxHash64>,
}

impl Default for Corpus {
    fn default() -> Self {
        Self::new()
    }
}

impl Corpus {
    /// An empty corpus.
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Seed 0: the digest is compared against another run of this same
            // code, never published or looked up, so there is nothing a seed
            // would buy.
            hasher: XxHash64::with_seed(0),
            inputs: 0,
            bytes: 0,
            build: None,
        }
    }

    /// Folds one labelled input into the digest.
    ///
    /// Both the label and the length are hashed as well as the bytes. Without
    /// the lengths, two inputs absorbed back to back are indistinguishable from
    /// one input of their concatenation, and a case that split its corpus
    /// differently between the legs would pair happily.
    pub fn absorb(&mut self, label: &str, bytes: &[u8]) {
        fold(&mut self.hasher, label, bytes);
        self.inputs += 1;
        self.bytes += bytes.len() as u64;
    }

    /// Folds one labelled input into the *build* digest instead.
    ///
    /// For what a feature arm swapped in rather than for what the region read:
    /// a `cfg`-selected constant naming the compiled subject. It does not move
    /// [`digest`](Self::digest) and it is not counted in
    /// [`inputs`](Self::inputs) or [`bytes`](Self::bytes) — those describe the
    /// workload, and the workload is what has to match across a feature arm.
    ///
    /// Declare a value the compiler chooses, never one passed in by hand: a
    /// label the caller types agrees across two arms whatever was actually
    /// built, which is the failure this exists to catch.
    pub fn declare(&mut self, label: &str, bytes: &[u8]) {
        fold(
            self.build.get_or_insert_with(|| XxHash64::with_seed(0)),
            label,
            bytes,
        );
    }

    /// The digest so far.
    #[must_use]
    pub fn digest(&self) -> u64 {
        self.hasher.finish()
    }

    /// The digest as the fixed-width hex the record carries.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        format!("{:016x}", self.digest())
    }

    /// The build digest as hex, or `None` when the case declared nothing.
    #[must_use]
    pub fn build_digest_hex(&self) -> Option<String> {
        self.build
            .as_ref()
            .map(|hasher| format!("{:016x}", hasher.finish()))
    }

    /// How many inputs have been absorbed.
    #[must_use]
    pub const fn inputs(&self) -> u32 {
        self.inputs
    }

    /// How many bytes have been absorbed, across every input.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Folds one labelled input into a hasher, identically for both channels.
///
/// Lengths are written as explicit little-endian bytes rather than through
/// `write_u64`, whose default implementation is native-endian. The two legs of
/// a comparison always share a host, so this cannot change an answer today —
/// but a digest that quietly depended on the byte order of the machine would be
/// a surprise waiting for whoever compares across one.
fn fold(hasher: &mut XxHash64, label: &str, bytes: &[u8]) {
    hasher.write(&(label.len() as u64).to_le_bytes());
    hasher.write(label.as_bytes());
    hasher.write(&(bytes.len() as u64).to_le_bytes());
    hasher.write(bytes);
}

#[cfg(test)]
mod tests {
    use super::Corpus;

    fn digest_of(pairs: &[(&str, &[u8])]) -> String {
        let mut corpus = Corpus::new();
        for (label, bytes) in pairs {
            corpus.absorb(label, bytes);
        }
        corpus.digest_hex()
    }

    /// The three ways a corpus can differ while a naive digest stays equal.
    /// Each one has happened to somebody: a renamed input, a re-split buffer,
    /// and two inputs swapped by a refactor.
    #[test]
    fn digest_is_sensitive_to_label_length_and_order() {
        let base = digest_of(&[("keys", b"abc"), ("values", b"def")]);

        assert_ne!(
            base,
            digest_of(&[("Keys", b"abc"), ("values", b"def")]),
            "the label is not part of the digest"
        );
        assert_ne!(
            base,
            digest_of(&[("values", b"def"), ("keys", b"abc")]),
            "the order is not part of the digest"
        );
        assert_ne!(
            base,
            digest_of(&[("keys", b"abcdef")]),
            "a re-split corpus hashes the same as the original"
        );
        assert_ne!(
            base,
            digest_of(&[("keys", b"abc"), ("values", b"defg")]),
            "the contents are not part of the digest"
        );

        // And the property the comparator relies on: the same inputs in the
        // same order always agree.
        assert_eq!(base, digest_of(&[("keys", b"abc"), ("values", b"def")]));
    }

    /// The property the whole split exists for: a feature arm declares a
    /// different subject and the two legs still agree about the bytes they
    /// measured. Folded into one digest, this case could only be compared by
    /// waiving the guard on the bytes.
    #[test]
    fn declaring_moves_the_build_digest_and_not_the_corpus() {
        let mut serde = Corpus::new();
        serde.absorb("payload", b"{}");
        serde.declare("backend", b"serde_json");

        let mut simd = Corpus::new();
        simd.absorb("payload", b"{}");
        simd.declare("backend", b"simd-json");

        assert_eq!(
            serde.digest_hex(),
            simd.digest_hex(),
            "declaring a subject moved the digest over the measured bytes"
        );
        assert_ne!(serde.build_digest_hex(), simd.build_digest_hex());
        assert_eq!(serde.digest_hex(), digest_of(&[("payload", b"{}")]));
    }

    /// A case that declares nothing asserts nothing, which is what keeps a
    /// target with no feature axis comparable on either axis. `None` and a
    /// digest of an empty stream are different claims.
    #[test]
    fn a_case_that_declares_nothing_has_no_build_digest() {
        let mut corpus = Corpus::new();
        assert_eq!(corpus.build_digest_hex(), None);
        corpus.absorb("payload", b"abc");
        assert_eq!(corpus.build_digest_hex(), None);
        corpus.declare("backend", b"");
        assert_eq!(corpus.build_digest_hex().map(|d| d.len()), Some(16));
    }

    /// The build digest is folded the same way as the corpus digest, so the
    /// three confusions the corpus test names cannot reach it either.
    #[test]
    fn the_build_digest_is_sensitive_to_label_length_and_order() {
        let build_of = |pairs: &[(&str, &[u8])]| {
            let mut corpus = Corpus::new();
            for (label, bytes) in pairs {
                corpus.declare(label, bytes);
            }
            corpus.build_digest_hex().expect("declared")
        };

        let base = build_of(&[("backend", b"a"), ("guard", b"b")]);
        assert_ne!(base, build_of(&[("Backend", b"a"), ("guard", b"b")]));
        assert_ne!(base, build_of(&[("guard", b"b"), ("backend", b"a")]));
        assert_ne!(base, build_of(&[("backend", b"ab")]));
        assert_eq!(base, build_of(&[("backend", b"a"), ("guard", b"b")]));
    }

    /// Declared inputs are not workload, so they must not reach the counters a
    /// case's throughput is derived against.
    #[test]
    fn declaring_does_not_count_towards_the_absorbed_totals() {
        let mut corpus = Corpus::new();
        corpus.absorb("payload", &[0; 10]);
        corpus.declare("backend", &[0; 99]);
        assert_eq!(corpus.inputs(), 1);
        assert_eq!(corpus.bytes(), 10);
    }

    #[test]
    fn an_empty_corpus_still_has_a_digest() {
        let empty = Corpus::new();
        assert_eq!(empty.digest_hex().len(), 16);
        assert_eq!(empty.inputs(), 0);
        assert_eq!(empty.bytes(), 0);
    }

    #[test]
    fn counters_track_what_was_absorbed() {
        let mut corpus = Corpus::new();
        corpus.absorb("a", &[0; 10]);
        corpus.absorb("b", &[0; 5]);
        assert_eq!(corpus.inputs(), 2);
        assert_eq!(corpus.bytes(), 15);
    }
}
