//! Wall time for the wire parsers: the header split every payload pays before
//! a decoder sees it.
//!
//! [`parse_confluent`](spate_avro::parse_confluent) and
//! [`parse_single_object`](spate_avro::parse_single_object) are this crate's
//! own code, with no `apache-avro` counterpart, so neither case carries a
//! floor.
//!
//! Read against `raw_flat15_value` in `decode_paths_wall.rs`, which walks the
//! same datums: the pair says what share of a Confluent payload's cost is the
//! framing rather than the decode.
//!
//! The schema cache sits behind these parsers and is `pub(crate)`, so no case
//! here reaches it. `decode_paths_wall.rs`'s `mode_confluent_warm` and
//! `mode_confluent_mixed_ids` are the pair that prices it: same datum bodies,
//! one schema id against eight.
//!
//! ```sh
//! make bench-ab REF=main FILTER=wire_
//! ```

use spate_bench::{Corpus, Suite, bench_main};

#[path = "support/corpora.rs"]
mod corpora;
#[path = "support/orders.rs"]
mod orders;

/// Payloads in each corpus, the extent a case declares.
const RECORDS: u64 = corpora::BATCH as u64;

fn suite() -> Suite {
    spate_bench::suite("spate-avro")
        .case(
            "wire_parse_confluent",
            |corpus, _seed| {
                let payloads = corpora::confluent_orders(corpora::READY_ID);
                absorb(corpus, &payloads);
                payloads
            },
            |b, payloads: &Vec<Vec<u8>>| {
                b.iter(|| {
                    let mut bodies = 0usize;
                    for payload in payloads {
                        let (id, datum) =
                            spate_avro::parse_confluent(payload).expect("the fixture is framed");
                        std::hint::black_box((id, datum));
                        bodies += datum.len();
                    }
                    bodies
                });
            },
        )
        .items(RECORDS)
        .bytes_of(|payloads: &Vec<Vec<u8>>| corpus_bytes(payloads))
        .done()
        .case(
            "wire_parse_single_object",
            |corpus, _seed| {
                let payloads = corpora::matching_single_object();
                absorb(corpus, &payloads);
                payloads
            },
            |b, payloads: &Vec<Vec<u8>>| {
                b.iter(|| {
                    let mut bodies = 0usize;
                    for payload in payloads {
                        let (fingerprint, datum) = spate_avro::parse_single_object(payload)
                            .expect("the fixture is framed");
                        std::hint::black_box((fingerprint, datum));
                        bodies += datum.len();
                    }
                    bodies
                });
            },
        )
        .items(RECORDS)
        .bytes_of(|payloads: &Vec<Vec<u8>>| corpus_bytes(payloads))
        .done()
}

bench_main!(suite);

/// Absorb every byte the region reads, so the digest proves both legs parsed
/// the same corpus.
fn absorb(corpus: &mut Corpus, payloads: &[Vec<u8>]) {
    for payload in payloads {
        corpus.absorb("payload", payload);
    }
}

fn corpus_bytes(payloads: &[Vec<u8>]) -> u64 {
    payloads.iter().map(|p| p.len() as u64).sum()
}
