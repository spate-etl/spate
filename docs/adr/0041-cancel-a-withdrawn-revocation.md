# ADR-0041 — A revocation the leader takes back is cancelled, leaving a no-progress timeout behind

- **Status:** accepted
- **Date:** 2026-07-22 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0039](0039-split-revocation-cooperative-then-forced.md) gives a revocation a
deadline: drain cooperatively, or be forced. But a leader can change its mind
mid-drain. It names the split for its current owner again, while that owner
still holds it, because a subsequent rebalance made the move unnecessary.

The pending forced release is now protecting a handoff with nothing waiting on
it. Letting it fire charges a replay for nothing.

## Considered options

- Let the forced release fire anyway; the deadline was set, so honor it
- Cancel the revocation and drop the pending forced release entirely
- Cancel the revocation but keep a weaker timeout over the drain it left behind

## Decision outcome

Chosen option: "Cancel the revocation but keep a weaker timeout", because the two
halves of the deadline serve different purposes and only one of them stops
mattering.

The deadline exists to serve the **rebalance**. Once the leader withdraws the
move there is no rebalance left to protect, so forcing would charge a replay for
a handoff with nothing waiting on it. Hence the cancel, whose only effect is on a
drain slower than the deadline, since a faster one was never going to be forced.

But cancelling ends the *revocation*, not necessarily the *drain*. Resuming
intake on a source that has already stopped is a seam the split-source trait
deliberately lacks, so a drain in flight still runs to completion and hands the
split back, after which the same worker re-claims it replay-free.

What the cancel must **not** drop is the obligation to keep the split readable. A
wedged drain with nothing over it would leave the split owned, leased and unread
forever, and a bounded job containing it could never finish. So the
deadline stays in a weaker form: a **no-progress timeout**. If nothing commits
for the deadline's duration, release it and let the same worker re-claim with a
fresh lane. A live drain commits as its tail acknowledges, so only a wedged one
trips it.

### Consequences

- Good, because a withdrawn move costs no replay, where previously it charged one
  for a handoff that was cancelled.
- Good, because a wedged drain still cannot strand a split, so bounded jobs stay
  able to finish.
- Bad, because there are now two timeout semantics over the same duration
  (a hard deadline while a revocation is live, a no-progress timeout after it is
  cancelled), and which applies depends on state that is not local to the timer.
- Bad, because a legitimately slow drain that commits nothing for the whole
  window is released and re-claimed, costing the replay the cancel was meant to
  avoid.

### Confirmation

The distinction is exercised directly: a cancelled revocation over a live drain
must not force, and a cancelled revocation over a wedged drain must release.
Both are in the coordination test suite, which is what caught the original defect.

## More information

- Landed in `9767280` (#68), amending
  [ADR-0039](0039-split-revocation-cooperative-then-forced.md) rather than
  replacing it. The cooperative-then-forced shape is unchanged, and this
  records what happens on the withdrawal path it did not cover.
