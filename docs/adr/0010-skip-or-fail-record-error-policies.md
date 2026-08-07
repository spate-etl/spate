# ADR-0010 — Record error policies are Skip or Fail, with no dead-letter queue

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A record can be individually bad — a payload that will not deserialise, a `map`
that returns an error — while every other record in the batch is fine. Failing
the pipeline on each one makes it unusable against real data; ignoring them
silently loses data without anyone noticing. Some framework in this space would
route them to a dead-letter queue.

## Considered options

- Skip or Fail only, both surfaced through metrics
- Skip, Fail, or route to a dead-letter queue
- Fail only, with no per-record tolerance

## Decision outcome

Chosen option: "Skip or Fail only, both surfaced through metrics", because a
dead-letter queue is a *destination*, and owning one means owning its schema,
its retention, its credentials and its own delivery guarantees — a second sink
inside the framework, for a case the target environments already handle with a
topic they own.

Skip counts the record and continues; Fail stops the pipeline. Defaults follow
where the error came from: deserialisation defaults to Skip, because a malformed
payload on a shared topic is expected, and operators default to Fail, because a
`map` that errors is usually a bug in the pipeline rather than in the data.

Deliberately there is **no third policy that drops a record without counting
it.** A silent drop is indistinguishable from correct operation, and the whole
point of the taxonomy is that a skipped record leaves evidence.

### Consequences

- Good, because a bad record is always visible — `*_dropped_total{reason}` and
  `*_errors_total{error_type}` — so an alert can be written against it.
- Good, because the framework carries no destination it did not otherwise need.
- Bad, because recovering skipped records means reading them out of the source
  again; the framework keeps no copy.
- Bad, because operators who want dead-lettering have to build it themselves,
  typically as a second sink on a split branch.

### Confirmation

INV-7 — policies are Skip or Fail only, and both surface through metrics rather
than only logs.

## More information

- Landed in `c8973e6`.
- [Error handling](../user-guide/02-concepts/04-error-handling.mdx) — the
  taxonomy, the defaults, and what each policy does in operation.
