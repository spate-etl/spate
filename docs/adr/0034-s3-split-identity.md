# ADR-0034 — Split identity is a truncated digest over member keys, versions and a packing version

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0028](0028-deterministic-split-ids.md) requires split ids to be
deterministic and derived from content. For the object-storage source, "content"
is a list of object keys and their versions, which can be long, arbitrary, and
chosen by whoever writes to the bucket.

The id is **persisted identity**: progress is recorded against it. A collision
therefore does not produce a visible error; it silently merges two distinct
splits, and one of them is never processed while the job reports completion. So
the derivation has to hold against key names an adversary chose, not merely
against likely ones.

## Considered options

- Concatenate the member keys
- A truncated SHA-256 over the sorted member keys, their versions and a
  packing-version constant, encoded base64url
- A random id recorded in the plan

## Decision outcome

Chosen option: "A truncated SHA-256 over sorted keys, versions and a packing
version", because it is fixed-length regardless of member count, uses the
portable alphabet ids need, and is collision-resistant against chosen input.

Concatenation was rejected on both length and safety: keys can be long enough to
blow a store key limit, and a naive concatenation is trivially ambiguous. Two
different member sets can produce the same string when a key contains the
separator. That is exactly the adversarial case that matters here.

Including the **version** of each object means an overwrite produces a different
id, so re-uploaded content becomes new work rather than silently matching stale
progress. Including a **packing-version constant** means a change to the packing
algorithm is an explicit epoch: every id changes at once, deliberately, rather
than some splits coincidentally matching.

The derivation is public, so out-of-process producers (an event-driven planner,
a single-shot invocation) can mint identical ids for the same work.

### Consequences

- Good, because the id is fixed-length and portable whatever the member keys look
  like.
- Good, because an overwrite becomes new work instead of inheriting the previous
  object's progress.
- Bad, because the id is opaque: an operator looking at a stuck split cannot tell
  from its id which objects it covers, and has to look up the spec record.
- Bad, because a packing change orphans all existing progress. That is correct,
  since the splits are different, but it means a packing tweak is a full re-run,
  not a rolling change.

### Confirmation

The derivation is exposed as a public function, so the property that two
planners produce identical ids for identical input is testable directly rather
than being an internal claim.

## More information

- Landed in `84e1583` (#49).
- [ADR-0028](0028-deterministic-split-ids.md) — the general requirement this
  implements.
- [ADR-0033](0033-s3-split-packing.md) — the packing whose version participates
  in the digest.
