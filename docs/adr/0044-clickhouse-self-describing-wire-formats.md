---
description: "Both ClickHouse wire formats carry the table's column names and types, so the sink always fetches the schema and the validate_schema key is removed."
---

# ADR-0044 — Both ClickHouse wire formats carry the schema, so it is always fetched

- **Status:** accepted
- **Date:** 2026-09-06
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The ClickHouse sink's default wire format, `RowBinary`, carries neither column
names nor types: the server maps the bytes to columns by position and reads
them under the table's current types. `crates/spate-clickhouse/src/schema/`
checks the row struct against the table when the sink is built and again at
each pipeline thread's first record, and then nothing looks at the table again.

Most of what an `ALTER` can do still fails loudly, because it changes a
column's width and the server's parse fails. What does not is a type change
that keeps the width: any `Decimal` scale change inside one precision band, any
`DateTime64` precision change, a signedness change. Those are accepted and
land rescaled values.

The Native encoder is not exposed to this. A Native block names each column and
its type, so the server compares them against the table on every insert.

The `system.columns` read that the check needs has no privilege cost: ClickHouse
filters that table by what the account can already see, so a writer holding
`INSERT` on its target already reads that target's columns, and one it holds no
grant on returns no rows.

## Considered options

- Send `RowBinaryWithNamesAndTypes` for the row format, and fetch the schema
  unconditionally so both formats have one.
- Keep the three `validate_schema` modes and change the default to `full`
  (the proposal in #283).
- Add a fourth `format` value so the header is opt-in alongside `rowbinary`.
- Do nothing: leave the drift to the retry ladder, the circuit breaker and the
  stalled-watermark alert.

## Decision outcome

Chosen option: "send `RowBinaryWithNamesAndTypes` and fetch the schema
unconditionally", because it moves the check from a moment to the wire. A
header travels with every batch, so the property holds for the life of the
pipeline rather than for as long as the table happens not to change.

The header is the names and types `system.columns` reported, written back
verbatim, once per request body ahead of the rows. The server compares them to
the table and rejects a mismatch with `INCORRECT_DATA` (117), which the writer
already classifies fatal. The two settings that gate that comparison,
`input_format_with_names_use_header` and `input_format_with_types_use_header`,
are sent per insert and refused in the user's `settings:` map: they default on,
and a server or profile default that turned them off would restore the silent
corruption without anything saying so.

**Defaulting `validate_schema` to `full` was rejected** because it leaves the
gap open. The client's check happens twice and the table can change afterwards;
no mode of a startup check closes a window that opens after startup.

**An opt-in fourth `format` value was rejected** because it makes correctness a
configuration choice, and the measured cost does not justify offering the
incorrect one: 83 bytes per request body for a six-column table, and 150 ms
against 148 ms over 1M rows.

**Doing nothing was rejected** because the failure is silent. The retry ladder
and the breaker surface writes that fail; this one succeeds.

### Consequences

- Good, because a table altered under a running pipeline is now caught. The
  client looked once; the header travels with every batch.
- Good, because the client-side type check is unconditional, so the footguns
  only `full` used to catch — a bare `Uuid`, a bare `Ipv4Addr`, a
  `Decimal64<2>` against a `Decimal(9, 2)` column — are caught for every
  pipeline. The server never sees the struct's types, only the table's, so
  these have no server-side counterpart.
- Good, because the two descriptions cannot be wired to different tables. The
  sink hands the encoder the schema it fetched.
- Bad, because every pipeline now reads `system.columns` from every replica of
  every shard at startup, and a replica that cannot answer fails the build. The
  sink writes to every replica, so a replica down at deploy time is a degraded
  deployment, but this is startup behaviour that used to be opt-in.
- Bad, because every existing pipeline breaks: `validate_schema` leaves the
  configuration, `with_row` becomes an async step, `ClickHouseSink::validate_schema`
  and `ClickHouseEncoder::new` are gone.
- Bad, because `NativeSchema::from_columns`, the static no-fetch path, now
  compares the row against the types the caller declared rather than only their
  names. Those are the strings its blocks put on the wire, so a disagreement it
  now rejects is one the server would have seen.
- Neutral, because the break is loud. `ClickHouseSinkConfig` carries
  `#[serde(deny_unknown_fields)]`, so a YAML still holding `validate_schema:`
  fails to load with an error naming the key, and every Rust call site is a
  compile error.

### Confirmation

A container test, `rowbinary_header::a_same_width_alter_is_rejected_rather_than_silently_miswritten`,
which is the reproduction from #410: it writes a batch, alters
`Decimal(18, 4)` to `Decimal(18, 2)` and `DateTime64(3)` to `DateTime64(6)`,
and asserts the next write fails with code 117. Reverting the format to plain
`RowBinary` makes that write succeed, which is the defect.

Structurally, `ClickHouseSink` is reachable only through `with_row`, which
fetches, so a sink whose writer has no header cannot be constructed.

## Evidence

Server-side insert time for 1M rows, six runs each against ClickHouse 26.8:
`RowBinary` averaged 148 ms (132–159) and `RowBinaryWithNamesAndTypes` 150 ms
(136–156), at 89 ms of server CPU either way. Re-measured on 26.3, six
interleaved runs each of a 1M-row six-column insert: 87 ms (78–96) against
90 ms (84–101), at 73 ms and 77 ms of server CPU. Both spike-measured and
hand-recorded on a machine that was not quiet; no committed rig. The ranges
overlap, and the arms differ only in the request body's first 78 bytes.

That second run also produced one 574 ms outlier, which followed the *position*
of the insert rather than its format: reversing the order moved it to the other
arm, and it does not appear in the figures above.

The header is once per request body, not per chunk: 83 bytes for the
six-column table #410 measured, 78 for the one above.

Instruction counts for the encode benches, HEAD against `main` under callgrind:
`encode_rowbinary_events` −0.14% and `encode_rowbinary_metrics` −0.37%, since
the bench encoder's branch is simpler than the `Option` it replaces. The three
Native cases rise 0.09% to 0.44%, which is the first-record type check that
`NativeSchema::from_columns` now runs, amortized over the 1,000-row corpus.

## More information

- Landed in #413. Tracked by #410, which folds in #283.
- [ADR-0007](0007-clickhouse-insert-path.md) — the pre-encoded frames and the
  deterministic deduplication token, which this record does not disturb: the
  frames are still RowBinary rows, the framework still owns the batch
  boundary, and a retry still re-sends identical bytes under an identical
  token. Only the format keyword and the body's prefix change.
- [ADR-0043](0043-clickhouse-columns-from-the-row-struct.md) — the row struct
  as the insert column list, which still holds. Its `Decision outcome` says
  "`with_row` stays separate from `validate_schema`, because
  `validate_schema: off` still needs the column list and still must issue no
  queries"; that sentence stops applying here, since there is no mode that
  issues no queries and the fetch moves into `with_row`.
- [Schema validation](../user-guide/04-connectors/sinks/clickhouse/schema-validation.mdx)
  — the three checks and when each runs.
- [Permissions](../user-guide/04-connectors/sinks/clickhouse/permissions.mdx)
  — the grant the `system.columns` read does not need.
