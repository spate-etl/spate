---
description: "The plan record names the worker whose election wrote its generation, so a leader keeps an election whose bump reply was lost. A 0.2 worker cannot read it."
---

# ADR-0060 — The plan record names the worker whose election wrote its generation

- **Status:** accepted
- **Date:** 2026-10-04
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A newly elected leader in `spate-coordination` fences the plan record by
bumping its `generation` with a compare-and-set
([ADR-0026](0026-coordination-fencing.md)). When that write applies and its
reply is lost, the retry loses to the leader's own write, and the re-read shows
a record at the new generation that could equally be a racing successor's
bump. The leader gives leadership back, and the fleet holds one more election
and advances the generation a second time. No split progress is lost. The
question is how the leader recognises its own bump.

## Considered options

- The plan record names its elector, as `instance_id` and the per-process
  nonce, and a record without one reads as another worker's
- Adopt a re-read record that is byte-equal to the write
- Keep the demotion and the extra election
- Add the field and bump the record schema to 4
- Read the field in this release and write it from a later one
- Store the elector under its own key

## Decision outcome

Chosen option: "The plan record names its elector", because only this process
writes its `(instance_id, nonce)` pair, so a record at the election's
generation that names it is this process's bump. A bump whose re-read finds
such a record holds the fence at that revision. A won reply leaves the same
state. The record may come from an earlier election of the same process that
gave leadership back before it read its bump. Any other leader in between had
to win its own bump and so moved the generation past it, and adopting the
record skips no leader. A record at the same generation naming another
owner or another nonce, or naming no elector, demotes as before. The record
schema stays at 3.

A byte-equal check stops matching once a publish rewrites `updated_at_ms`, and
a wrong match would leave a deposed leader's pending publish unfenced. A
schema bump would make this build reject every record the previous release
wrote, and 0.2 workers would still exit on the new records. Writing the field
a release later keeps a mixed 0.2 fleet running at the cost of one more
release with the extra election. A separate key cannot be written in the same
compare-and-set as the bump, so it cannot name the bump's writer.

### Consequences

- Good, because a leader whose bump reply was lost keeps the election, with
  no extra store operation.
- Good, because this build reads every plan record 0.2 wrote. A record with
  no elector encodes to the same bytes it had in 0.2.
- Bad, because `PlanRecord` rejects unknown fields, and a 0.2 worker exits
  fatally on a plan record that names an elector. During a rolling upgrade the
  remaining 0.2 workers exit once an upgraded worker leads. Stopping every 0.2
  worker on a job before starting this version on it avoids that. Returning to
  0.2 needs a fresh job name, and a bounded job then runs again from the
  beginning.
- Bad, because the plan record grows by up to 194 bytes. The planner
  cursor's share of the portable value budget shrinks by the same amount.
- Neutral, because the identity is per process. A restarted process with the
  same `instance_id` draws a new nonce, so a bump its predecessor wrote at the
  same generation demotes it.

### Confirmation

`crates/spate-coordination/tests/unseen_generation_bump.rs` pins the
adoption and each mismatch that demotes: another nonce, another owner, no
elector, and this process's elector at an earlier generation.
`a_plan_record_without_an_elector_reads_as_none` and
`planner_cursor_budget_includes_the_escaped_fingerprint`
(`crates/spate-coordination/src/records.rs`) pin the 0.2 layout and the
envelope. The `coordination_records` fuzz target encodes plan records with and
without an elector.

## More information

- Landed in the pull request closing #895.
- [ADR-0026](0026-coordination-fencing.md) — the plan record's revision as the
  leader fence.
- [Scaling out](../user-guide/05-deployment/scaling-out.mdx) — the rule that
  every replica reads the records the others write.
