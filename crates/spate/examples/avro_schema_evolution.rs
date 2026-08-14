//! Evolving a producer's Avro schema without stopping the pipeline, with no
//! broker, no registry and no server.
//!
//! "The producer added a field" is the question that decides whether a team
//! commits to a typed pipeline. Avro answers it with *schema resolution*: the
//! payload was written in the **writer** schema's shape, a configured
//! `reader_schema` states the shape you want, and the decoder reconciles the
//! two per record. This runs the whole thing in memory: bytes from the old
//! producer in, evolved records out, asserted:
//!
//! ```sh
//! cargo run -p spate --features avro --example avro_schema_evolution
//! ```
//!
//! Four mechanisms, labeled § 1 to § 4 throughout: a **new field with a
//! default**, a **renamed record**, an **`int`→`long` promotion**, and
//! `#[serde(default)]` on the Rust type, which solves an overlapping problem
//! one layer further up. All four ride the one pipeline decode; § 2 and § 3
//! carry standalone sections after it for the cases a decode that succeeds
//! cannot show.
//!
//! # What resolves, and what does not
//!
//! The decoder underneath is `apache-avro` 0.21. What its schema resolution
//! does, and where it departs from the Avro specification:
//!
//! - A reader field carrying a `default` fills in for a writer that never
//!   wrote it. Works; § 1 asserts it.
//! - A renamed **record** resolves, though not because of its alias.
//!   Resolution never compares record names, so the reader's name and its `aliases`
//!   are ignored, and a payload resolves against any structurally compatible
//!   reader record whatever it is called. § 2 asserts that, and keeps the
//!   alias in the schema anyway: the specification and a registry's
//!   compatibility check both read it.
//! - A **field**-level alias does **not** resolve. A reader field that renames
//!   the producer's field and lists the old name in `aliases` fails *every*
//!   payload with `Missing field in record`, which under the default Skip
//!   policy drops and acks the whole stream. § 2 asserts that failure rather
//!   than describing it, and renames on the Rust side with `#[serde(alias)]`,
//!   which does work. Tracked as spate issue #74.
//! - `int`→`long` promotion resolves. Works; § 3 asserts it, and asserts what
//!   the *reverse* does, which is worse than failing.
//!
//! # A schema miss is not a drop
//!
//! Nothing here can miss: `mode: raw` pins both schemas inline, so no registry
//! is involved. In `confluent` mode a payload can arrive carrying a schema id
//! that is not cached yet. That is neither an error nor a drop: the
//! deserializer returns `DeserError::NotReady`, the chain converts it into a
//! retriable `Blocked`, the fetch runs on the I/O runtime, and the driver
//! re-pushes the same batch from the payload that blocked until the schema
//! lands. A not-ready wait is an upstream dependency rather than sink
//! pressure, so it does not engage the backpressure controller and does not
//! pause the source; it is counted on `spate_deser_not_ready_total`. Delivery
//! stays at-least-once (INV-1): no record is dropped, and the pipeline thread
//! never performs the I/O itself.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  add a field to a producer's schema without breaking a running pipeline
// INDEX-TECH:  Avro
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use apache_avro::{Schema, from_avro_datum, to_avro_datum};
use serde::Deserialize;
use spate::avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, SchemaSource};
use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{PipelineRun, TestEncoder, capture_sink, memory_source};
use std::error::Error;
use std::time::Duration;

/// **The producer, last year.** Three fields, `quantity` as an `int`, no
/// currency. Every byte on the wire in this example was written in this
/// shape, and the producer is not being redeployed to suit us.
const WRITER_V1: &str = r#"{"type":"record","name":"OrderPlaced","fields":[
    {"name":"order_id","type":"string"},
    {"name":"sku","type":"string"},
    {"name":"quantity","type":"int"}]}"#;

/// **What we want to read today.** Three evolutions at once, and old payloads
/// still decode:
///
/// - § 1 `currency` is new and carries a `default`, so a writer that never
///   wrote the field still produces a record that has it.
/// - § 2 the record is renamed `Order`. `aliases` carries the producer's name
///   because the specification asks for it; this decoder resolves the rename
///   without reading it, which `main` asserts.
/// - § 3 `quantity` widens to `long`.
const READER_V2: &str = r#"{"type":"record","name":"Order","aliases":["OrderPlaced"],"fields":[
    {"name":"order_id","type":"string"},
    {"name":"sku","type":"string"},
    {"name":"quantity","type":"long"},
    {"name":"currency","type":"string","default":"USD"}]}"#;

/// § 2, the schema we would rather have written: rename the *field* `sku` to
/// `item_code` and keep the old name as an alias. This one does not resolve,
/// and `main` asserts the failure.
const READER_FIELD_ALIAS: &str = r#"{"type":"record","name":"Order","aliases":["OrderPlaced"],"fields":[
    {"name":"order_id","type":"string"},
    {"name":"item_code","type":"string","aliases":["sku"]},
    {"name":"quantity","type":"long"},
    {"name":"currency","type":"string","default":"USD"}]}"#;

/// § 2, `READER_V2` with the alias dropped and the record renamed to something
/// the producer never heard of. Resolution accepts it: record names are not
/// compared.
const READER_UNRELATED_NAME: &str = r#"{"type":"record","name":"Unrelated","fields":[
    {"name":"order_id","type":"string"},
    {"name":"sku","type":"string"},
    {"name":"quantity","type":"long"},
    {"name":"currency","type":"string","default":"USD"}]}"#;

/// § 3, the promotion run backwards: one field, written wide, read narrow.
const WIDE_WRITER: &str =
    r#"{"type":"record","name":"Count","fields":[{"name":"quantity","type":"long"}]}"#;
/// § 3, the reader that narrows it. Avro forbids this direction; the decoder
/// does not.
const NARROW_READER: &str =
    r#"{"type":"record","name":"Count","fields":[{"name":"quantity","type":"int"}]}"#;

/// `pipeline.name` is the `pipeline` label on every series this run mints, and
/// a gauge series has one live owner per process (INV-10), so a run builds
/// one pipeline under one name.
///
/// This example asserts on decoded records rather than on the exposition, so
/// it asks for neither an exporter nor an admin server. A pipeline that names
/// no address takes `0.0.0.0:9090`, which examples running concurrently would
/// contend for.
const CONFIG: &str = r#"
pipeline: { name: avro-evolution-demo, threads: 1 }
admin: { listen: none }
checkpoint: { interval: 200ms }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
"#;

/// The shape the pipeline works in. Nothing here knows about Avro; the two
/// `serde` attributes are the Rust-side half of § 2 and § 4.
#[derive(Debug, Deserialize)]
struct Order {
    order_id: String,
    /// § 2, the half that works. `READER_V2` keeps the producer's field name,
    /// so resolution hands us `sku` and the rename happens one layer up: serde
    /// matches either name onto `item_code`. Renaming the field in the reader
    /// schema instead is the path that does not resolve, which `main` asserts.
    /// A serde alias is a *name* mapping only. It cannot invent a field,
    /// cannot promote a type, and does not survive the field changing meaning.
    #[serde(alias = "sku")]
    item_code: String,
    /// § 3. The payload holds a 4-byte `int`; `READER_V2` says `long`, and
    /// resolution widens it before serde ever sees it.
    quantity: i64,
    /// § 1. No `#[serde(default)]` here: if Avro's reader-schema default did
    /// not fire, serde would fail with `missing field currency` and this
    /// example would not run. The assertion below is about Avro's mechanism
    /// and nothing else.
    currency: String,
    /// § 4. In neither schema. Avro never produces this field, so there is
    /// nothing for resolution to do and serde fills it. An Avro default is
    /// *in the contract*, visible to every consumer of the schema and applied
    /// during decode; a serde default is private to this struct and applied
    /// after. Use the Avro default when the producer's schema is the thing
    /// that changed; use the serde default for a field the schema was never
    /// going to carry.
    #[serde(default = "direct_channel")]
    channel: String,
}

fn direct_channel() -> String {
    "direct".to_string()
}

/// One datum in the old producer's shape: a bare Avro record with no framing,
/// which is what `mode: raw` expects on the wire.
fn v1_datum(order_id: &str, sku: &str, quantity: i32) -> Result<Vec<u8>, Box<dyn Error>> {
    let schema = Schema::parse_str(WRITER_V1)?;
    let mut record =
        apache_avro::types::Record::new(&schema).ok_or("WRITER_V1 is not a record schema")?;
    record.put("order_id", order_id);
    record.put("sku", sku);
    record.put("quantity", quantity);
    Ok(to_avro_datum(&schema, record)?)
}

/// Resolve one datum against a reader schema, through the same
/// `from_avro_datum` call the deserializer makes internally, reached directly here so a section
/// can assert on one reader schema at a time without a pipeline around it.
fn resolve(writer: &str, reader: &str, datum: &[u8]) -> Result<AvroValue, apache_avro::Error> {
    let writer = Schema::parse_str(writer)?;
    let reader = Schema::parse_str(reader)?;
    from_avro_datum(&writer, &mut &datum[..], Some(&reader))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // ── The pipeline: § 1, § 2 and § 3 in one decode ────────────────────
    // `mode: raw` with both schemas inline, so the payload carries no schema
    // id and nothing contacts a registry. `build_serde` applies resolution;
    // the single-pass `build_serde_datum` rejects a reader schema at build
    // time, because it decodes in the writer's shape.
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(WRITER_V1)),
        reader_schema: Some(SchemaSource::inline(READER_V2)),
        ..AvroSettings::default()
    };
    let deserializer = AvroDeserializerBuilder::from_settings(&settings, &pipeline.io_handle())?
        .build_serde::<Order>()?;

    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    });

    let runtime = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Order, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .map(|order: Order| {
                    format!(
                        "{}|{}|{}|{}|{}",
                        order.order_id,
                        order.item_code,
                        order.quantity,
                        order.currency,
                        order.channel
                    )
                    .into_bytes()
                })
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    // Three orders, every one of them written by the old producer.
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let mut last = 0;
    for (order_id, sku, quantity) in [
        ("ord-1001", "SKU-COFFEE", 2),
        ("ord-1002", "SKU-MUG", 1),
        ("ord-1003", "SKU-BEANS", 12),
    ] {
        last = handle.push(
            p0,
            Some(order_id.as_bytes()),
            &v1_datum(order_id, sku, quantity)?,
        );
    }

    // Bounded on purpose, here and below: an unbounded wait turns a wedged
    // pipeline into a hung process rather than a failing one.
    assert!(
        handle.wait_committed(p0, last + 1, Duration::from_secs(30)),
        "every payload commits (last committed: {:?})",
        handle.last_committed(p0),
    );
    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect("the pipeline drains after shutdown")?;

    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();

    // Every payload here predates `currency`, predates the record's new name,
    // and wrote `quantity` four bytes narrower than it is read. All three
    // decode, carrying the values resolution yields.
    assert_eq!(rows.len(), 3, "every old-format payload must decode");
    assert!(
        rows.contains(&"ord-1001|SKU-COFFEE|2|USD|direct".to_string()),
        "{rows:?}"
    );
    assert!(
        rows.contains(&"ord-1002|SKU-MUG|1|USD|direct".to_string()),
        "{rows:?}"
    );
    assert!(
        rows.contains(&"ord-1003|SKU-BEANS|12|USD|direct".to_string()),
        "{rows:?}"
    );

    let datum = v1_datum("ord-1004", "SKU-TEA", 3)?;

    // ── § 2, what the record alias is and is not doing ──────────────────
    // The alias is not what matched `OrderPlaced` to `Order`: resolution never
    // compares record names, so the same payload resolves against a reader
    // named for nothing in particular and carrying no alias at all. Keep the
    // alias in the schema, which the specification and a registry's
    // compatibility check both read, and do not rely on this decoder to
    // enforce a name.
    resolve(WRITER_V1, READER_UNRELATED_NAME, &datum)
        .expect("record names are not compared during resolution");

    // ── § 2, the part that does not work ────────────────────────────────
    // The same payload, read through the schema a reader *would* write to
    // rename `sku` to `item_code`. Resolution matches reader fields by name
    // alone and never consults their aliases, so this fails per record, which
    // under the default Skip policy would drop and ack 100% of the stream,
    // counted on `spate_deser_records_dropped_total`. A dependency bump that
    // fixes it fails this assertion.
    let err = resolve(WRITER_V1, READER_FIELD_ALIAS, &datum)
        .expect_err("a reader field alias is documented to resolve, and does not");
    assert!(err.to_string().contains("item_code"), "{err}");

    // ── § 3, the promotion's forbidden direction ────────────────────────
    // Avro permits `int`→`long` and not the reverse. This decoder does not
    // enforce the rule: it narrows and wraps, so a quantity of five billion
    // arrives as seven hundred million, with no error anywhere. Widen a field
    // freely; do not narrow one.
    let wide_schema = Schema::parse_str(WIDE_WRITER)?;
    let mut wide =
        apache_avro::types::Record::new(&wide_schema).ok_or("WIDE_WRITER is not a record")?;
    wide.put("quantity", 5_000_000_000_i64);
    let wide = to_avro_datum(&wide_schema, wide)?;
    let AvroValue::Record(fields) = resolve(WIDE_WRITER, NARROW_READER, &wide)? else {
        return Err("expected a record value".into());
    };
    assert_eq!(
        fields[0].1,
        AvroValue::Int(705_032_704),
        "narrowing truncates silently; it must not be mistaken for a promotion"
    );

    println!("\npipeline exit: {:?}", report.state);
    println!("final watermarks: {:?}", report.final_watermarks);
    println!("rows written ({}): {rows:?}", rows.len());
    println!("reader field alias: {err}");
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
