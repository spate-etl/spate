---
description: "On Apache Kafka, a broker cut off from its cluster answers the Kafka feed's depth reads with stale ends; the feed page states the case and the feed adds no second client."
---

# ADR-0097 — A cut-off Kafka leader can end a bounded job early, and the feed page says so

- **Status:** accepted
- **Date:** 2026-11-10
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
ends a bounded job once the feed's depth stays at zero over a settling window
and the dead-letter read finds nothing. Its consequences accept one case where
a job ends `Completed` over a unit it never read: retention deleting the unit
first.

On Apache Kafka, a broker cut off from the controller and the other brokers
keeps answering as leader with the ends it held at the cut. The Kafka feed's
depth client lists each end at the leader its metadata names, and librdkafka
takes that metadata from the up broker it sent to most recently. When the
cut-off broker is the group's coordinator and leads the work, retry and
dead-letter topics, every depth read goes to it. A job then ends `Completed`
over units written to the new leaders since the cut, and a unit a peer
dead-lettered since the cut does not stall it
([#1767](https://github.com/spate-etl/spate/issues/1767)). Redpanda steps the
cut-off leader down, and the read fails.

## Considered options

- State the case on the Kafka feed page, and change no code
- Each depth read also lists ends through a short-lived client bootstrapped
  from one broker of the metadata's list, rotating per read, and keeps the
  larger end
- Each depth read lists ends through one short-lived client per broker, and
  keeps the answer with the highest leader epoch

## Decision outcome

Chosen option: "State the case on the Kafka feed page", because neither client
change covers a worker cut off on the same side as the broker. A worker in a
zone isolated with that broker reaches no survivor, so no client it creates
reads a newer end.

The rotating client covers a worker that reaches a survivor once the window
holds at least as many reads as the cluster has brokers. It costs a client
creation, a connection, and a TLS and SASL handshake on every read, and the
dead-letter read is a single read, so it needs every broker's view anyway.
Reading every broker on every read makes the answer deterministic whenever any
survivor is reachable. It also turns a broker that is down into a choice
between failing every read, which holds a job for as long as any broker is
down, and skipping that broker. rdkafka 0.39 exposes no leader epoch, so the
epoch comparison reads it through the C API. Both need a test tier that cuts a
broker's traffic with iptables, which CI does not run.

### Consequences

- Good, because the depth reader keeps one client and one code path, and CI
  needs no network-fault tier.
- Bad, because on Apache Kafka a bounded job can end `Completed`, exit 0, while
  a broker is cut off from its cluster. Units written since the cut stay above
  the group's commit until a later worker on the group reads them, and a unit a
  peer dead-lettered since the cut stays in the dead-letter topic uncounted.
- Bad, because the job's exit gives no sign of the cut. The operator has to
  know of the cut and check the group's lag and the dead-letter topic
  afterwards.
- Neutral, because Redpanda steps a cut-off leader down, so a read through it
  fails and the end check restarts.

### Confirmation

Nothing yet. No committed test cuts a broker off from its cluster.

## Evidence

Hand-run, with no committed rig. Three KRaft nodes, with the coordinator and
the work, retry and dead-letter leaders on one node, that node's traffic to its
peers dropped with iptables, and a unit confirmed at `acks=all` through the
survivors:

- Apache Kafka 4.4.0. The job exits `Completed`, code 0, with 2 of 4 rows. The
  group's commit stays at 1 against an end of 2, and after the cut heals a
  second worker on the group writes the two rows. An earlier run read the
  dead-letter topic's end at 50 against 100 written, on every read for 389 s.
- Redpanda v26.2.6, write caching off. The job writes 4 of 4 rows in four runs.
  From 0.5 s after the cut, the cut-off node answers ListOffsets with
  `NOT_LEADER_FOR_PARTITION`.

This does not establish how the case behaves on Redpanda with write caching on,
or after a crash.

## More information

- Landed with the pull request that closes #1767.
- [Kafka feed: A broker cut off from its cluster](../user-guide/04-connectors/feeds/kafka/README.mdx#a-broker-cut-off-from-its-cluster)
  states the behaviour.
- [ADR-0094](0094-a-kafka-feed-reads-its-end-at-the-high-watermark.md) sets the
  level the depth client lists ends at. Either level gives the same stale end.
