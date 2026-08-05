//! The digest that proves two legs measured the same bytes.
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

use std::hash::Hasher;

use twox_hash::XxHash64;

/// A running digest over everything a case fed its measured region.
///
/// Sensitive to the label, the length and the order of every absorbed input —
/// see `digest_is_sensitive_to_label_length_and_order`.
#[derive(Debug)]
pub struct Corpus {
    hasher: XxHash64,
    inputs: u32,
    bytes: u64,
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
        }
    }

    /// Folds one labelled input into the digest.
    ///
    /// Both the label and the length are hashed as well as the bytes. Without
    /// the lengths, two inputs absorbed back to back are indistinguishable from
    /// one input of their concatenation, and a case that split its corpus
    /// differently between the legs would pair happily.
    pub fn absorb(&mut self, label: &str, bytes: &[u8]) {
        // Lengths written as explicit little-endian bytes rather than through
        // `write_u64`, whose default implementation is native-endian. The two
        // legs of a comparison always share a host, so this cannot change an
        // answer today — but a digest that quietly depended on the byte order
        // of the machine would be a surprise waiting for whoever compares
        // across one.
        self.hasher.write(&(label.len() as u64).to_le_bytes());
        self.hasher.write(label.as_bytes());
        self.hasher.write(&(bytes.len() as u64).to_le_bytes());
        self.hasher.write(bytes);
        self.inputs += 1;
        self.bytes += bytes.len() as u64;
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
