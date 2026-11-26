---
description: "The Kafka source pauses a partition at the consumer once its lane has been paused for 2 s with no poll of the partition between pauses, so short pauses with no poll between them add up."
---

# ADR-0116 — A Kafka source counts paused time with no poll toward its client pause

- **Status:** accepted
- **Date:** 2026-11-26
- **Supersedes:** [ADR-0115](0115-a-kafka-source-pauses-its-client-after-two-seconds.md)
- **Superseded by:** —

## Context and problem statement

ADR-0115 pauses a partition at the consumer once its lane has stayed paused for
2 s, and a resume before then clears the wait. A partition that is neither
paused at the consumer nor polled keeps fetching, and on a topic with aborted
transactions what it fetches is not counted against the client's queue limits
(ADR-0107). A poll frees it: librdkafka drops the records of an aborted
transaction when it parses the response, keeps the response for the
transaction's marker, and a poll serves the markers ahead of the next record
and frees their responses.

A pipeline thread polls one lane at a time, round-robin. Under a sink that
pauses and resumes the thread several times a second, a lane can be resumed
and paused again before its turn comes. Under ADR-0115 each such pause starts
a new 2 s wait, so a lane that is not reached across several short pauses
keeps fetching for their total length, and no client pause ends it. Before
ADR-0115 every pause purged the partition, which ended each stretch.

The question was what the 2 s counts.

## Considered options

- Time paused since the last poll of the partition, across pauses the runtime
  lifted
- Time paused in the current pause only, as ADR-0115 has it, with the gap
  documented
- Measure process memory under cycling pauses before deciding

## Decision outcome

Chosen option: "time paused since the last poll of the partition", because it
bounds every stretch in which a partition is neither polled nor paused at the
consumer by the same 2 s, and a pipeline that keeps polling its lanes still
keeps the prefetch.

Each lane counts its polls in the state it shares with the source. A resume
that finds a pause not yet at the consumer keeps it as lifted, with its instant
and the count at the resume. A pause before the count moves takes that instant
back, and one after it starts a new 2 s wait. Everything else is as ADR-0115
decided.

The measurement was not chosen: it could not become a test, and the bound this
decision sets can be asserted directly.

### Consequences

- Good, because a partition stops fetching once it has gone 2 s paused with no
  poll, however many pauses that spans.
- Good, because a lane polled between short pauses keeps its prefetch, as under
  ADR-0115.
- Neutral, because a lane poll adds one relaxed atomic increment, once per
  batch.
- Bad, because a pause can now reach the consumer sooner than 2 s after the
  runtime asks for it, when an earlier pause was lifted with no poll since.
- Bad, because a lane that is resumed and never polled keeps fetching with no
  bound in time, as it did before ADR-0115. Nothing pauses it.

### Confirmation

`a_pause_with_no_poll_since_the_last_keeps_its_instant` and
`a_poll_between_pauses_starts_the_wait_again` in
`crates/spate-kafka/src/source.rs` test the rule.
`a_poll_between_short_pauses_keeps_the_prefetch` in
`crates/spate-kafka/tests/mock_cluster.rs` shows a lane's poll restarting the
wait on a real client.

## Evidence

- librdkafka 2.16.0 (rdkafka-sys 4.11.0+2.16.0): an aborted transaction's
  MessageSet is skipped as the response is parsed
  (`src/rdkafka_msgset_reader.c:1082-1087`); a control record becomes an op
  that holds the whole response and counts no bytes
  (`src/rdkafka_op.c:951-1005`); a poll serves a control op and goes on
  (`src/rdkafka_op.c:1062-1066`).
- The pipeline thread polls one lane per pass, round-robin, from a cursor that
  persists across pauses (`crates/spate-core/src/pipeline/driver.rs:346-366`).
- No memory figure was taken under cycling pauses.

## More information

- Landed in the pull request that counts unpolled time toward the Kafka
  client pause.
- [ADR-0115](0115-a-kafka-source-pauses-its-client-after-two-seconds.md)
  holds the rest of the decision: the 2 s wait, the deferral and its costs.
- The behavior is described in the Kafka source's
  [Backpressure: pause and resume](../user-guide/04-connectors/sources/kafka/README.mdx#backpressure-pause-and-resume).
