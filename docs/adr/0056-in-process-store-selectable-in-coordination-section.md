---
description: "The coordination: section can select the in-process store, so a solo run takes its tuning from the file; that store is never shared between processes."
---

# ADR-0056 — A solo run takes its coordinator tuning from the `coordination:` section by selecting the in-process store

- **Status:** accepted
- **Date:** 2026-10-01
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A coordinated source with no `coordination:` section runs solo over the
in-process store with `CoordinationConfig::default()`
([ADR-0032](0032-s3-always-coordinated.md)). The section
([ADR-0049](0049-coordination-section-in-the-pipeline-config.md)) requires a
`store:`, and `CoordinatorSpec` knew only the durable stores, so a solo run
could change its tuning only in code. `max_in_flight` is the tuning that
matters most there: it sets the object-storage source's read parallelism, and
its default of 8 leaves most pipeline threads on a large host without source
work. The components involved are `CoordinatorSpec` in `spate-coordination`
and the solo fallback in `spate-s3`.

## Considered options

- Keep solo tuning code-only, through `with_coordinator`
- A `coordination:` section with tuning and no `store:` means the in-process
  store
- A `memory` store kind under `store:`, with a body that takes no keys
- A source key for read parallelism, such as the removed `lanes`

## Decision outcome

Chosen option: "a `memory` store kind under `store:`", because it puts a solo
run's tuning in the file with the same keys a durable store takes, and keeps
`store:` required.

`store: { memory: {} }` builds a fresh in-process store for the pipeline and a
`StoreCoordinator` over it with the section's tuning. Building it logs a WARN
that no other process or pipeline shares the store and that a restart replays
the whole job. No tuning key is rejected. The store needs no crate feature,
unlike the new-store rule in ADR-0049, because `spate-coordination` links it
unconditionally for the solo fallback.

The store-less section was rejected because a deployment whose `store:` was
dropped would run every instance solo over the whole input, with only a log
line to show it. A required `store:` fails that file at load. A source key was
rejected because read parallelism is the coordinator's working set, and two
knobs for one budget would have to be reconciled in every coordinated source.
Code-only tuning is what the change replaces.

### Consequences

- Good, because a solo run sets `max_in_flight` and the rest without a rebuild,
  and moving it to a durable store changes one key.
- Good, because a section without a `store:` still fails at load.
- Bad, because a file naming `memory` can be deployed to several instances, and
  each one runs the whole job. Only the WARN and the documentation guard
  against it.
- Bad, because keys that act only between peers, such as `rebalance_delay`,
  `drain_deadline` and `instance_id`, are accepted with `memory` and have no
  effect.
- Neutral, because the in-process store now has a configured role, which
  ADR-0032 listed as missing. It was already the store of every solo run.

### Confirmation

`builds_a_memory_coordinator_and_warns` and
`memory_store_keys_are_rejected_with_the_store_path` in
`crates/spate-coordination/src/section.rs` pin the decode and the WARN.
`the_memory_store_section_sets_solo_read_parallelism` in
`crates/spate-s3/tests/request_shape.rs` pins that the section's
`max_in_flight` reaches a solo run's read parallelism.

## More information

- Requested in #866.
- [ADR-0049](0049-coordination-section-in-the-pipeline-config.md) defines the
  section, and [ADR-0032](0032-s3-always-coordinated.md) the solo fallback this
  store backs.
- [Coordination stores](../user-guide/04-connectors/coordination/README.mdx)
  lists the store and its key.
