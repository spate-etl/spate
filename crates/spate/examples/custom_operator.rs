//! Writing custom operators.
//!
//! Operators in `spate` are **stateful closures** over the push model: `map`,
//! `filter`, `flat_map`, and `try_map` compose statically into one loop, and
//! a closure capturing its own state (here: a dedup set) is a full-blown
//! custom operator — no trait to implement. (The underlying `Collector` /
//! `StageLifecycle` traits in [`spate::ops`] are public for advanced stages,
//! but closures are the intended API.)
//!
//! Two chains are driven here: one over owned records, where bare closures
//! infer everywhere, and one over records that borrow the source buffer,
//! which is what the `map_rec` tier exists for.
//!
//! This example drives a chain by hand — poll a batch, push it through,
//! flush — which is exactly what a pipeline thread does in production. It
//! deliberately bypasses the runtime; for a full assembly around a chain
//! like this one, see `memory_pipeline.rs` and `spate::pipeline::Pipeline`:
//!
//! ```sh
//! cargo run -p spate --example custom_operator
//! ```

// The examples index renders these fields; see scripts/examples-index.sh.
// INDEX-TIER:  extending
// INDEX-GOAL:  drive a chain of stateful operators over owned and borrowed records by hand
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::backpressure::InflightBudget;
use spate::checkpoint::{AckRef, Checkpointer};
use spate::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate::error::{DeserError, ErrorPolicy};
use spate::ops::{ChunkConfig, PushOutcome, chain_owned};
use spate::record::{PartitionId, RawPayload, Record};
use spate::sink::{KeyHashRouter, shard_queues};
use spate::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::{TestDeserializer, TestEncoder, memory_source};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

// ─── A borrowing record family ──────────────────────────────────────────

/// One order line, still pointing into the payload buffer the source lane
/// handed the chain — decoding copies nothing out of it.
#[derive(Debug)]
struct OrderLine<'buf> {
    order_id: &'buf str,
    customer_id: &'buf str,
}

/// The family tag: a type-level function from a buffer lifetime to the
/// record type. It is what lets a lifetime-parameterized record cross the
/// chain's generic boundaries.
struct OrderLineF;

impl RecFamily for OrderLineF {
    type Rec<'buf> = OrderLine<'buf>;
}

/// Splits `<order_id>|<customer_id>` payloads into borrowed order lines.
struct OrderLineDeser;

impl Deserializer<OrderLineF> for OrderLineDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, OrderLine<'buf>>,
    ) -> Result<(), DeserError> {
        let text = std::str::from_utf8(raw.bytes).map_err(|e| DeserError::Malformed {
            reason: e.to_string(),
        })?;
        let (order_id, customer_id) = text.split_once('|').ok_or(DeserError::Malformed {
            reason: "order line has no customer field".to_string(),
        })?;
        let _ = out.emit(Record {
            payload: OrderLine {
                order_id,
                customer_id,
            },
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// The `map_rec` stage: a borrowed order line in, the owned billing key the
/// sink stores out. A `fn` item, which satisfies the stage's higher-ranked
/// bound at every buffer lifetime — the call site explains the bound.
fn billing_key(line: OrderLine<'_>) -> Vec<u8> {
    format!("{}/{}", line.customer_id, line.order_id).into_bytes()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // A chain: dedup (stateful flat_map) → validate (try_map with a
    // per-stage error policy) → uppercase (map).
    let (queues, mut receivers) = shard_queues(1, 16);
    let budget = Arc::new(InflightBudget::new());

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut chain = chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
        // Custom operator #1: stateful deduplication. `flat_map` emits
        // 0..N records; emitting zero drops the duplicate (its ack share
        // is released — dropping counts as success, not loss).
        .flat_map::<Owned<Vec<u8>>, _>(move |word, out| {
            if seen.insert(word.clone()) {
                out.emit(word);
            }
        })
        // Custom operator #2: record-level validation. `Skip` drops bad
        // records and counts them in metrics; `Fail` would stop the
        // pipeline instead.
        .try_map(
            |word: Vec<u8>| {
                if word.iter().all(u8::is_ascii_alphabetic) {
                    Ok(word)
                } else {
                    Err("non-alphabetic word")
                }
            },
            ErrorPolicy::Skip,
        )
        .map(|word: Vec<u8>| word.to_ascii_uppercase())
        .sink(
            TestEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            budget,
        )
        .build();

    // Drive it exactly like a pipeline thread: poll a batch from a lane,
    // push it through the chain, flush the terminal buffers.
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle()))?;
    let p0 = PartitionId(0);
    cp.begin_epoch(&[p0], 1);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let SourceEvent::LanesAssigned(mut lanes) = source.poll_events(Duration::from_millis(100))?
    else {
        panic!("expected assignment");
    };

    for word in ["hello", "world", "hello", "rust", "n0pe", "world"] {
        handle.push(p0, None, word.as_bytes());
    }

    let mut batch = lanes[0]
        .poll(64, Duration::from_millis(200))?
        .expect("records queued");
    assert!(matches!(chain.push_batch(&mut batch, 0), PushOutcome::Done));
    drop(batch);
    assert!(matches!(chain.flush(), PushOutcome::Done));

    // What came out the other end: duplicates and the invalid word gone.
    let mut rows = Vec::new();
    while let Ok(chunk) = receivers[0].try_recv() {
        rows.extend(
            spate_test::decode_rows(&chunk.frame)
                .into_iter()
                .map(|r| String::from_utf8_lossy(&r).into_owned()),
        );
    }
    println!("deduped + validated + uppercased: {rows:?}");
    assert_eq!(rows, ["HELLO", "WORLD", "RUST"]);

    // ─── The borrowed tier: what `map_rec` is for ───────────────────────
    //
    // The chain above is over an *owned* family: `Owned<Vec<u8>>` means
    // `Rec<'buf> = Vec<u8>` whatever `'buf` is, so `map`'s bound is a plain
    // `FnMut(T) -> U` with no lifetime to quantify over and bare closures
    // infer. The chain below decodes into records that borrow the lane's
    // payload buffer, and there `map`/`try_map` are not offered at all:
    // both are defined only on a builder whose current family is
    // `Owned<T>`, because the family-generic bound they would need,
    //
    //     G: for<'buf> FnMut(CurF::Rec<'buf>) -> NF::Rec<'buf>
    //
    // puts `'buf` only inside associated types and rustc rejects it at the
    // *definition* site with E0582 — the `spate::ops` module docs carry the
    // desugaring. Nothing to do with ownership or with borrowck: the bound
    // cannot be written.
    //
    // A borrowing family transforms through `map_rec`/`try_map_rec`
    // instead, whose bound goes through `spate::ops::MapFn<In, Out>`: `In`
    // and `Out` are ordinary trait parameters rather than an
    // associated-type binding, so `for<'buf>` over them is legal. Same
    // transformation, a bound the compiler accepts.
    //
    // Pass a `fn` item, as the builder's docs advise: it is higher-ranked
    // by construction and satisfies the bound at every lifetime, where a
    // closure does so only when the compiler infers a higher-ranked
    // signature for it. `filter`, `inspect` and `flat_map` serve both
    // tiers from one method — their output family, where they have one,
    // sits in an argument type (`&mut Emitter<'_, OutF>`) rather than in
    // an `Output` binding, so the same `for<'buf>` bound stays legal and
    // the `filter` below takes an ordinary closure.
    let (order_queues, mut order_receivers) = shard_queues(1, 16);
    let order_budget = Arc::new(InflightBudget::new());
    // (`chain` by its full path: the owned chain above already took the
    // name.)
    let mut orders = spate::ops::chain::<OrderLineF, _>(OrderLineDeser)
        .filter(|line: &OrderLine<'_>| !line.order_id.is_empty())
        .map_rec::<Owned<Vec<u8>>, _>(billing_key)
        .sink(
            TestEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            order_queues,
            order_budget,
        )
        .build();

    // Same drive loop, same lane: three order lines, the middle one with no
    // order id for the `filter` to drop.
    for line in ["o-17|cust-2", "|cust-2", "o-18|cust-3"] {
        handle.push(p0, None, line.as_bytes());
    }
    let mut batch = lanes[0]
        .poll(64, Duration::from_millis(200))?
        .expect("order lines queued");
    assert!(matches!(
        orders.push_batch(&mut batch, 0),
        PushOutcome::Done
    ));
    drop(batch);
    assert!(matches!(orders.flush(), PushOutcome::Done));

    let mut billed = Vec::new();
    while let Ok(chunk) = order_receivers[0].try_recv() {
        billed.extend(
            spate_test::decode_rows(&chunk.frame)
                .into_iter()
                .map(|r| String::from_utf8_lossy(&r).into_owned()),
        );
    }
    println!("billing keys off borrowed records: {billed:?}");
    assert_eq!(billed, ["cust-2/o-17", "cust-3/o-18"]);

    // The checkpoint side saw everything resolve (drops included).
    drop(chain); // releases the acks parked in received-but-undropped chunks
    drop(orders);
    cp.drain();
    println!("committable watermarks: {:?}", cp.take_watermarks());
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
