<!--
Delete every section that does not apply, and every comment like this one. If
the commit message already carries the argument, a link and a sentence is a
complete description.

Contributions are accepted under Apache-2.0 §5. There is no CLA to sign.
-->

## What this changes, and why

<!--
`Closes #123.` on a line of its own first, if an issue tracks this.

One sentence: why this exists. Then what it does, and where its scope stops.
Open on the problem a user hits, not the symptom in the code: "the final batch
never reaches the sink on shutdown", not "`ChunkWriter` drops the last frame".

The commit message carries the argument for the approach, dated and attached to
the diff. Cite it rather than restating it here.

Say where this delivers less, more, or something other than the issue asked
for. That comparison is the one a reviewer cannot make alone.

Deferred work goes under `Anything else`, and so does evidence that needs more
than a clause.
-->

## Invariants

<!--
Name the numbers this change touches and say how each property still holds.
Touching one is not a problem. Answer "None." if it touches none.

docs/INVARIANTS.md states each invariant in full and is the only place that
does.
-->

## Semver

<!-- Delete all but one. Pre-1.0, a breaking change is fine if it is announced. -->

- [ ] Additive — nothing existing changes
- [ ] Breaking — and the commit subject carries `!`

## Checks

<!-- Tick what you ran. Nothing else belongs under this heading. -->

- [ ] `make gates`
- [ ] `make docs`, if this touches docs
- [ ] Conventional Commits, scoped to the crate touched, no AI attribution
      trailers
- [ ] A fragment under `changelog.d/`, if this reaches a crate somebody
      upgrading would care about. `changelog.d/README.md` says when

## Anything else

<!--
Most changes delete this section.

Keep it to point a reviewer at the lines worth their attention, to say how to
reject one part without blocking the rest, or to report something you verified
by hand that no automated test captures. A change with no runtime behavior has
nothing of that last kind to report.

A defect you found along the way and left alone belongs in its own issue rather
than here. If you measured something, say what you measured it on and how quiet
the machine was.
-->
