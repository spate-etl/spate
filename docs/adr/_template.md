---
description: "REPLACE-ME"
---

<!--
  The description is what a search result shows under the record's title: one
  or two sentences of 50 to 160 characters stating what was decided and the
  consequence that matters most. A superseded record says so here, since a
  reader arriving from a search must not take it for the current decision.
-->

<!--
  The ADR template. Copy it with `make adr-new SLUG=short-description`, which
  allocates the next number and fills in the date.

  This file is the documentation: the rules are stated inline, next to the
  section each one governs, and there is no separate how-to page to fall out of
  step with it. The underscore prefix keeps Docusaurus from rendering it.

  The format is MADR 4.0.0 (https://adr.github.io/madr/), minimal variant, with
  two additions of our own, `Confirmation` and `Evidence`. Where we depart from
  upstream MADR the departure is noted below, so the next person can tell a
  decision from a mistake.

  ---------------------------------------------------------------------------
  WHAT GETS AN ADR

  A decision earns a record when it affects the structure of the system, a key
  quality attribute (delivery semantics, throughput, memory, operability), or
  is hard to reverse.

  A decision does NOT earn one when it is none of those: a dependency bump,
  a rename, a formatting choice, a fix with only one sensible shape. Nor when
  an existing ADR already covers it. Amend nothing, write nothing, just cite
  it.

  This exclusion rule is the whole guard against decay. Decision logs die by
  filling with unneeded records, not by missing one; when every commit seems to
  want an ADR, people stop writing them at all. "No ADR" should be a decision
  this rule makes, not an oversight.

  ---------------------------------------------------------------------------
  THE RULES

  One decision per record. A record arguing three things is three records.

  Accepted records are immutable. Never rewrite the body of an accepted ADR to
  say something different. That is the failure that motivated moving off the
  decision-log table, where reversals were written over the decisions they
  reversed and the originals were lost. A changed decision is a NEW ADR.
  The old one keeps its body, gains `Superseded by`, and the new one names what
  it replaces. Both directions, always.

  Fixing a typo, a broken link or a wrong file path is not rewriting. Changing
  what the record claims was decided is.

  Numbers are monotonic and never reused, the same rule the invariants follow.
  A withdrawn ADR keeps its number.

  Every `REPLACE-ME` below must be gone before the record lands.
  `make ci-lint` fails while one remains.
-->

# ADR-NNNN — REPLACE-ME short title naming both the problem and the solution

- **Status:** accepted
- **Date:** YYYY-MM-DD
- **Supersedes:** —
- **Superseded by:** —

<!--
  Status is one of `accepted`, `superseded`, `deprecated`. Deliberately no
  `proposed` and no `rejected`: an ADR here is written once the call has been
  made, so `proposed` would never be true, and a rejected alternative belongs
  in `Considered options` below rather than in a file of its own.

  `deprecated` is for a decision that stopped applying without anything
  replacing it, such as a knob that was removed rather than changed.

  Status is a body line rather than a front-matter key, which is where
  upstream MADR 4 puts it. `docs/STYLE.md` §8 allows one front-matter key on a
  published page, `description`, and no `title:`, which alongside an H1 renders
  the title twice. MADR's own ADR-0008 evaluated the plain-text-line
  alternative and scored it well on every axis but one, so this is a trade we
  are making knowingly.

  Date is the date the decision LANDED IN THE TREE, citable to the commit named
  under `More information`. Not the day it was first discussed, which is not
  knowable, and not the day this file was written. Where a backfilled record is
  written long after the decision, say so:

      - **Date:** 2026-07-21 (recorded 2026-08-06 from the decision log)

  Both link fields hold `—` until they are needed, and a real relative link
  when they are: `[ADR-0009](0009-some-title.md)`. Extension-qualified, because
  `onBrokenLinks: 'throw'` means a stale one fails the site build.
-->

## Context and problem statement

REPLACE-ME

<!--
  Two or three sentences, or a question. What forces were in play, stated
  value-neutrally. The reader should be able to disagree with the outcome
  without disputing the context.

  Make the scope explicit by naming the components involved. "The sink worker's
  intake path" is scope; "the sink" is not.

  Do not explain the system here. A reader who needs that goes to the user
  guide, which this record links to rather than reproduces.
-->

## Considered options

- REPLACE-ME
- REPLACE-ME

<!--
  Every option that was genuinely on the table, including the one that was
  chosen and including "do nothing" where that was real. This is the section
  that makes a decision record worth more than a commit message, and it is the
  first thing lost when a record is written from memory.

  Do not pad it with an option that would never have been taken. An alternative
  included to make the chosen one look better is worse than listing one option
  honestly.
-->

## Decision outcome

Chosen option: "REPLACE-ME", because REPLACE-ME.

### Consequences

- Good, because REPLACE-ME
- Bad, because REPLACE-ME

<!--
  At least one of each. A record with no `Bad, because` line is either not
  finished or not honest. Every decision worth recording cost something, and
  naming the cost is what lets a future reader recognize when the trade has
  stopped paying.

  `Neutral, because` is available where an argument weighs neither way.
-->

### Confirmation

REPLACE-ME

<!--
  What enforces this decision — an invariant number, a property test, a
  specific gate, a compile-time impossibility.

  "Nothing yet" is a valid answer and a useful one: it marks a decision that
  rests on everybody remembering, which is the kind most likely to erode.
  Prefer it to a vague claim that review will catch it.

  Examples of the shape wanted:
    INV-3, and `crates/spate-core/src/checkpoint/` is loom-tested.
    `make check-adr`, run by `make ci-lint`.
    Structural — `ShardWorker::dispatch` is not `async`, so the await cannot
    be added without changing the signature.
    Nothing yet.
-->

## Evidence

<!--
  OPTIONAL — delete the heading entirely when the decision rests on no
  measurement. An empty Evidence section reads as though a number was lost.

  Measured claims only, each with its provenance. A figure with no provenance
  is an assertion, and this repository has learned to tell the difference:
  some of the numbers these decisions rest on came from rigs that no longer
  exist, and saying so is what keeps them usable. Acceptable provenance lines
  look like:

    Measured by `crates/spate-core/benches/chain_wall.rs`.
    Spike-measured, hand-recorded; no committed rig.
    Measured by a rig this repository no longer carries.

  State what the number does NOT establish where that has caught someone
  before. An A/B whose arms differ in more than one way measures the pair, not
  either arm.
-->

## More information

<!--
  The commit or pull request that landed the decision. This is what makes the
  `Date` above citable rather than asserted. Then related ADRs, and the user
  guide pages that describe the resulting behavior.

  Rationale belongs here; usage guidance does not. If a reader needs to know
  how to configure the thing, that is a user-guide page, and this section links
  to it.
-->

- Landed in REPLACE-ME
