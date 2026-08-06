# ADR-0012 — One Kafka consumer per process, with split partition queues

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The pipeline runs several threads and wants each to own some partitions. The
straightforward way is one consumer per thread, each its own consumer-group
member. That makes a pod with eight threads eight members, so a fleet of
twenty pods is a group of a hundred and sixty — and consumer-group rebalances
get slower and more disruptive as the group grows.

librdkafka offers `split_partition_queue`, which detaches a partition's queue
from the main consumer queue so another thread can poll it directly. That allows
one member per pod with partitions mapped onto threads under local control, but
it puts the rebalance choreography in our hands rather than the client's.

## Considered options

- One consumer per pipeline thread; the client handles group membership
- One consumer per process with `split_partition_queue`, partitions mapped m:n
  onto pipeline threads
- One consumer per process, single-threaded consumption

## Decision outcome

Chosen option: "One consumer per process with `split_partition_queue`", because
group size becomes a function of pod count rather than pod count times thread
count, and because it gives one drain choreography to get right instead of one
per thread.

Consumption parallelism is unchanged — it is still bounded by partition count,
exactly as with per-thread consumers. What changes is group scale, and the fact
that shutdown, revocation and commit all happen once per process.

The choreography this obliged us to implement was verified on a spike before the
decision was taken, because the failure modes are not obvious from the API. As
understood at the time:

- **Assign.** The rebalance callback runs on the controller's main-queue poll and
  only receives a `&BaseConsumer`, so it pauses all assigned partitions there —
  stopping fetch before it starts, which is what prevents pre-split messages
  landing on the main queue. After the callback returns, the controller splits
  every partition queue, distributes them, and resumes. Anything that still
  arrives on the main queue is routed defensively rather than dropped.
- **Revoke.** Trip the drain barrier, flush and acknowledge, commit
  synchronously, then drop the revoked queues. Dropping a `PartitionQueue`
  restores forwarding to the main queue, so the drop has to come last. Queues are
  re-split after **every** rebalance; retained-partition queues happen to keep
  working across eager rebalances, but nothing relies on that.
- The consumer lives in an `Arc` from the start, because `split_partition_queue`
  requires it. `enable.partition.eof` is forced off.

A per-thread-consumer fallback remains a documented escape hatch behind the same
traits, so the decision is reversible per deployment.

### Consequences

- Good, because a twenty-pod fleet is a twenty-member group, and rebalances stay
  fast.
- Good, because split-queue polls reset `max.poll.interval.ms` on their own, so
  group liveness never depends on the controller thread keeping up.
- Bad, because the rebalance choreography is ours to maintain, and each step
  above is load-bearing in a way that is not apparent from the code.
- Bad, because idle lanes must block briefly on their queue — zero-timeout
  polling busy-spins.

### Confirmation

INV-2 — source threads never block on a channel send, which is what keeps the
poll loop servicing the group. Two failure modes found during bring-up are now
pinned by the implementation rather than by care: the consumer's own close-poll
triggers a final revoke inside `BaseConsumer::drop`, so a deferred revocation
intent there deadlocks teardown forever and the rebalance must complete inline;
and the startup deadline is checked *before* transport errors in `poll_events`,
because unreachable brokers otherwise surface as an endless stream of retryable
errors and the fail-fast never fires.

## Evidence

Validated empirically, first on a spike and then by an interleaved topology A/B:
at realistic per-record work, split queues trail per-thread consumers by a
couple of percent. Measured by a rig this repository no longer carries.

## More information

- Landed in `c8973e6`; the teardown and startup-deadline findings in `e062465`.
- **The choreography above is the 2026-07-05 understanding, not a current
  spec.** The implementation has since moved to deferred completion — the
  revocation spans two `poll_events` calls and finishes at `unassign()`, and a
  message that reaches the main queue is rewound with `seek` rather than merely
  routed. The living specification is the module documentation on
  `crates/spate-kafka/src/source.rs`, which is kept beside the code it
  describes; this record says why the topology was chosen, not how it currently
  works.
- [Kafka source](../user-guide/04-connectors/sources/kafka/README.mdx) — the
  connector's configuration and the trade as an operator sees it.
