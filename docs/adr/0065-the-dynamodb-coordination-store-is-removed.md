---
description: "The DynamoDB coordination store is removed before its first release, with the per-handle expiry judgment and revision floors only it used."
---

# ADR-0065 — The DynamoDB coordination store is removed before its first release

- **Status:** accepted
- **Date:** 2026-10-11
- **Supersedes:** [ADR-0051](0051-lease-expiry-judged-by-each-observer.md),
  [ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md),
  [ADR-0058](0058-a-dynamodb-ephemeral-write-leaves-its-predecessors-removal-revision.md),
  [ADR-0061](0061-a-watch-delete-orders-above-what-its-handle-has-seen.md)
  (DynamoDB re-create exception)
- **Superseded by:** —

## Context and problem statement

The DynamoDB store in `spate-coordination` landed after v0.2.0 and no release
carries it. Coordination for object-storage backfills is moving to a design fed
by a queue of storage events plus a resumable lister. Shipping the store in
v0.3.0 makes its configuration, its `spate` feature and the AWS SDK dependency
tree public, along with its emulator lane in CI and its fault scenarios.

## Considered options

- Remove the store before its first release
- Ship it in v0.3.0 and deprecate it once the queue-fed design lands

## Decision outcome

Chosen option: "Remove the store before its first release", because no
release carries it, so removing it breaks no user, and shipping it would hold
its API and dependency tree until a deprecation cycle ends.

Three parts of the store contract existed only for this store and go with it:
per-handle expiry judgment
([ADR-0051](0051-lease-expiry-judged-by-each-observer.md)), the ephemeral
revision floors and their re-create exception
([ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md),
[ADR-0058](0058-a-dynamodb-ephemeral-write-leaves-its-predecessors-removal-revision.md)),
and the `op_timeout` and `attach_metrics` store hooks with the `op="poll"`
series. The contract again states that ephemeral keys expire on the store's
clock and that a key's revisions strictly increase.

[ADR-0052](0052-dynamodb-store-polls-without-streams.md) and
[ADR-0053](0053-one-dynamodb-table-holds-every-job.md) record how the store
polls and how its table is laid out. Nothing replaces them, so they are
deprecated.

The polled watch
([ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md)) and the
delete-revision rule of
[ADR-0061](0061-a-watch-delete-orders-above-what-its-handle-has-seen.md) stay.
The polled test double in `spate-coordination`'s tests declares both, and the
regression tests that model a view lagging the store run over it.

### Consequences

- Good, because the AWS SDK crates leave the dependency graph.
- Good, because the store contract has one expiry clock and no exception to
  increasing revisions.
- Bad, because no built-in store that several processes share runs without a
  server of its own.
- Bad, because the fault scenarios lose the HTTP-level faults injected between
  the SDK and DynamoDB Local: throttling, 5xx replies, and a reply dropped
  after its write landed.
- Bad, because no fault scenario runs the polled watch. Every remaining
  scenario uses NATS, whose watch pushes, so only in-process tests over polled
  test doubles exercise it.
- Neutral, because the polled watch stays with no built-in store that declares
  it.

### Confirmation

Structural: `spate-coordination` has no `dynamodb` feature or module, and
`spate` has no `coordination-dynamodb` feature.

## More information

- Landed in the pull request titled `coordination: remove the DynamoDB store`.
- [Coordination stores](https://github.com/spate-etl/spate/blob/ab71fb0ceddf1cafa0d5ad4b3defc503603dbaa8/docs/user-guide/04-connectors/coordination/README.mdx)
  — the stores that remain.
