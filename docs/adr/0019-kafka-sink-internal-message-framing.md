# ADR-0019 — A connector-internal length-delimited framing inside the opaque chunk frames

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The frozen sink contract carries a sealed batch as a list of opaque byte frames:
the encoder produces them on the pipeline threads and the writer sends them. That
shape came from a row-oriented destination, where a frame is simply rows.

Kafka's unit is not a row. It is a message — a key, a set of headers, a payload,
and possibly a tombstone marker. Four fields that the contract has nowhere to
put.

## Considered options

- Widen the frozen contract so a sealed batch can carry structured messages
- Encode a connector-internal length-delimited framing into the opaque frames,
  and parse it back in the writer
- Bypass the pool and have the Kafka sink batch on its own

## Decision outcome

Chosen option: "Encode a connector-internal framing into the opaque frames",
because it absorbs the mismatch entirely inside one connector and leaves the
frozen contract untouched.

The encoder writes key, headers, payload and a tombstone bit with length
delimiters; the writer parses them back. Because the format never leaves the
connector — it is written and read by the same crate, in the same process, for
the duration of one batch — it carries no compatibility obligation and is free
to change.

Widening the contract was rejected because it would push a Kafka-shaped concept
into a trait every sink implements, for the benefit of one. Bypassing the pool
was rejected because it would give up batching, breaker quarantine, health
signalling and drain choreography — all of which work unchanged.

### Consequences

- Good, because the frozen contract stays frozen and every other sink is
  unaffected.
- Good, because the framing can evolve freely; nothing outside the crate can
  depend on it.
- Bad, because the connector encodes and immediately re-decodes its own data, so
  there is a serialisation round trip that exists only to satisfy the seam's
  shape.
- Bad, because a reader of the sealed batch cannot tell what is in it; the frames
  are genuinely opaque and only the owning connector can interpret them.

### Confirmation

Round-trip tests over the framing in the connector's own test suite. Nothing
outside the crate can observe the format, so there is nothing further to hold.

## More information

- Landed in `6af6861` (#24).
- [ADR-0020](0020-kafka-sink-oversized-records.md) — the size guard that runs at
  encode time, in this same encoder.
