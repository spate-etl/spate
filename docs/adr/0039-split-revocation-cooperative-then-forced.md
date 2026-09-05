---
description: "Split revocation asks the current owner to drain cooperatively first, then forces release at a deadline, bounding a rebalance despite a stalled worker."
---

# ADR-0039 — Split revocation is cooperative first, forced second

- **Status:** accepted
- **Date:** 2026-07-21 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

When the leader moves a split from one worker to another
([ADR-0038](0038-leader-computed-sticky-assignment.md)), the current owner is
probably mid-read. Fencing it immediately is safe, since the compare-and-set
guarantees it cannot write, but it costs up to a commit interval of duplicated
work, because everything read since the last commit is re-read by the new owner.

So consent is worth asking for. But it cannot be worth waiting indefinitely for:
under the previous peer-negotiated design a worker could ignore a request
and the requester waited out a round budget, which was acceptable when the
request was **advisory**. A leader's revocation is a **decision**, and a worker
refusing it must not be able to pin a rebalance open.

## Considered options

- Fence immediately on every move; accept the duplicate window
- Cooperative only: ask, and wait however long the owner takes
- Cooperative first with a bounded wait, then forced

## Decision outcome

Chosen option: "Cooperative first with a bounded wait, then forced", because it
takes the cheap path when it is available and keeps the rebalance bounded when it
is not.

The leader stops assigning the split. The owner stops intake at a safe boundary,
chases its tail to a final fenced commit, and releases, **replaying nothing**. A
source that declines, or whose drain outruns the deadline, has its release forced
and its uncommitted tail replays under the next owner.

This deliberately reverses the earlier "no driver-side deadline" decision taken
with the negotiated handoff. That decision was correct while the request was
advisory; it is wrong once the request is a decision, because the thing the
deadline protects, the rebalance, did not exist as an obligation before.

Graceful-then-forced with a bounded wait between is the shape mature rebalancing
protocols converge on.

### Consequences

- Good, because a cooperative move costs no replay at all, which is the common
  case.
- Good, because a rebalance completes in bounded time regardless of how any
  individual worker behaves.
- Bad, because a slow but healthy drain gets forced at the deadline and pays a
  replay it would not have needed with a little more time.
- Bad, because the deadline is a tuning constant standing in for "how long is a
  reasonable drain", and it cannot be right for every workload.

### Confirmation

Correctness under forcing is unchanged and rests on
[ADR-0026](0026-coordination-fencing.md): a forced release fences the old owner,
so a late write loses its compare-and-set whatever the drain was doing.

## More information

- Landed in `e79dfc2` (#64).
- [ADR-0041](0041-cancel-a-withdrawn-revocation.md) — what happens when the
  leader takes the revocation back before the deadline.
- [Work assignment](../user-guide/02-concepts/08-work-assignment.mdx).
