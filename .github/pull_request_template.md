<!--
Thank you for this. Delete whichever sections do not apply — a typo fix does
not need the invariants checklist.

Contributions are accepted under Apache-2.0 §5. There is no CLA to sign.
-->

## What this changes, and why

<!-- Which crates, and the problem it solves. Link the issue if there is one. -->

## Invariants

<!--
Tick the ones this change touches. Ticking one is not a problem — it means
the description above should say how the property still holds.

The numbers are canonical and defined in docs/INVARIANTS.md; CONTRIBUTING.md
states the same list with the reasoning attached.
-->

- [ ] **INV-1 — delivery is at-least-once.** A source watermark is never
      committed past unacknowledged data, including across rebalances and
      shutdown.
- [ ] **INV-2 — source threads never block on a channel send.** Backpressure is
      `try_send` + `Source::pause` + keep polling. A blocked poll loop gets the
      consumer evicted from its group.
- [ ] **INV-3 — the checkpoint tracker stays synchronous and free of async
      runtimes.** It is loom-tested and must remain so.
- [ ] **INV-4 — acks never block behind data.** The ack path is unbounded and
      atomic.
- [ ] **INV-5 — the sink worker's intake path never awaits outside its
      `select!`.** Anything it blocks on sits in a branch alongside the drain
      deadline, or shutdown deadlocks.
- [ ] **INV-6 — no connector types in `spate-core`'s public API**, and no 0.x
      dependency types in any public trait bound. The `metrics` facade is the
      one sanctioned exception.
- [ ] **INV-7 — record error policies are Skip or Fail only**, and both are
      surfaced through metrics rather than only logged.
- [ ] **INV-8 — metrics handles are pre-registered at build time** — never
      resolved on the per-record path.
- [ ] **INV-9 — every metric family lives under the `spate_` umbrella.**
- [ ] **INV-10 — a gauge series has exactly one live owner per process.**

## Semver

<!-- Delete all but one. Pre-1.0, a breaking change is fine; an unannounced one is not. -->

- [ ] Additive — nothing existing changes
- [ ] Breaking — and the commit subject carries `!`

## Checks

Tick by exit code, not by memory.

- [ ] `make gates` — formatting, clippy, check, tests, doctests, the feature
      matrix, licences and advisories, and the repository consistency checks
- [ ] Docs changed: `make docs`. It sets `CI=true`, without which the redirect
      validation silently does not run
- [ ] Any ad-hoc cargo command run with `--locked`, as CI does
- [ ] Dependency changes: covered by `make gates`. `THIRD-PARTY.md` is *not*
      required to be current here — it is checked nightly and regenerated at
      release — but `make attribution` is welcome if you are adding a dependency
      rather than bumping one
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org),
      scoped to the crate touched, and carry no AI attribution trailers
- [ ] Crate-reaching `feat`, `fix`, `perf`, `revert`, `build` — or any `!`
      whatever its scope: a fragment under `changelog.d/`, from
      `make changelog-new TYPE=… SLUG=…`. Scoping to a non-crate area is the
      exemption; leaving the scope off is not. A fix to a bug that was never
      released takes a `Changelog: none` trailer instead. **The title is what
      lands on `main`**, so it is the one that has to be right

## Anything else

<!--
If you measured something, say what you measured it on, and on how quiet a
machine. A figure only reaches the docs carrying how it was established — see
CONTRIBUTING.md — because a number nobody can place is one nobody can check.
-->
