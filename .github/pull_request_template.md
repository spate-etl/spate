<!--
Delete every section that does not apply, and every comment like this one. If
the commit message already carries the argument, a link and a sentence is a
complete description.

Contributions are accepted under Apache-2.0 §5. There is no CLA to sign.
-->

## What this changes, and why

<!--
`Closes #123.` on a line of its own first, if an issue tracks this.

There is no need to repeat what that issue says. The diff shows the code and
the issue states the problem, so use this space for what neither can tell a
reviewer: the approach, the decisions that were not forced, and what someone
using the framework sees differently.

When no issue tracks this, state the problem here, in terms of what a user
hits. "`ChunkWriter` drops the last frame" is a symptom of the implementation.
"The final batch never reaches the sink on shutdown" is the user-visible
problem.

Say where this delivers less, more, or something other than the issue asked
for. That comparison is the one a reviewer cannot make alone.
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
