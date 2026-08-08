---
description: Adversarial, claim-refuting review of the current working diff (branch vs main). Full-reproduce rigor; report, then walkthrough, then approved fixes.
argument-hint: "[base-ref]  # defaults to main"
---

# deep-review

A review is not a summary of the diff — it is an adversarial test of the diff's
own claims against the implementation and its integration points. Trust nothing
the PR asserts about itself until you have tried to refute it in the code.

Target: the **current working diff** — the checked-out branch versus `${1:-main}`.
This does **not** touch GitHub; it reads the local tree.

The review is **read-only through the report**: reading, running gates, and
verifying — never editing code — until the report is delivered. Code changes
happen only *after* the walkthrough, and only for findings you explicitly accept.
Never post PR comments, never commit, never push without being asked.

## Non-negotiables

- **Refute before you report.** Every candidate finding AND every claim the PR
  makes about itself must survive an active attempt to disprove it by reading
  the real implementation and the unchanged code it integrates with. A finding
  that cannot be grounded in a concrete failure scenario does not ship.
- **Full-reproduce rigor.** Run the repo's gates for real (below), including the
  Docker/testcontainers suite. Verify every gate by **explicit exit code**
  (redirect to a log, check `$?`) — piped `grep`/`tail` chains have masked real
  failures in this repo. Never report a gate as green off a scrolled-past tail.
- **Degrade loudly.** If some gate cannot run (no Docker daemon, a toolchain is
  missing, a build breaks before the point you needed), run what you can, fall
  back to read-and-reason for the rest, and mark every claim you could not
  execute as **UNVERIFIED** in the report. The review still completes; it never
  silently claims a rigor it didn't reach.
- **Use agents efficiently — group, never per-finding.** Fan out by review
  dimension/area, not by individual finding. One skeptic pass per dimension
  reviews all of that dimension's candidates together. Spawning an agent per
  finding is a defect in the process, not thoroughness.

## Process

### 1. Establish ground truth
Read, before judging anything: `docs/INVARIANTS.md` — the canonical list in
full, plus the records under `docs/adr/` the diff touches — then `AGENTS.md` (its
Invariants section carries the implementation detail) and `docs/METRICS.md`.
Findings are judged against *this repo's* invariants, not generic Rust intuition.
Cite them by number: INV-1 at-least-once delivery, INV-2 source threads never
block on send, INV-3 the checkpoint tracker stays sync/tokio-free and loom-clean,
INV-4 acks never block behind data, INV-5 the sink intake never awaits outside
its `select!`, INV-6 no connector/0.x types in `spate-core` public bounds,
INV-7 error policies Skip-or-Fail-only, INV-8 metrics pre-registered at build
time, INV-9 the `spate_` umbrella, INV-10 one live owner per gauge series.

### 2. Scope the diff and extract the claim set
- `git diff --stat ${1:-main}...HEAD` and `git log ${1:-main}..HEAD` for shape.
- Harvest every **falsifiable claim** from the PR/commit messages and from code
  comments in the diff (e.g. "no `ShardQueues` clone survives the drain",
  "zero cost on the data path", "e2e_basis now honored", "hot path unchanged",
  "adds no indirection"). Each claim is a review target, recorded in a **claims
  ledger**, not background narrative.

### 3. Map the diff into the system
For each changed area, read the **surrounding unchanged code it integrates with**
— the caller of a changed function, the select loop a branch was added to, the
trait the new bound sits under, the shutdown/drop ordering a fix depends on. Most
real defects live at the seam between the diff and the code it assumes.

### 4. Fan out finders — grouped by dimension
Spawn a small set of finder agents, each owning one dimension across the whole
diff (adjust to the diff; keep the set small):
1. **Correctness + invariants** — logic bugs and any violation of INV-1..INV-10
   above. Highest priority.
2. **API / semver surface** — public API design, semver hazards, the no-0.x-types
   -leak rule, `#[non_exhaustive]` correctness, additive-vs-breaking.
3. **Doc-vs-code accuracy** — do the guide pages, desugaring tables, config
   defaults, and stated guarantees match the actual implementation.
4. **Style / simplification / efficiency** — reuse, simplification, idiom,
   per-record-path cost. Lowest priority; suppress noise.
Each finder returns candidate findings with `file:line` and a proposed failure
scenario — not verdicts.

### 5. Verify: run the gates, then refute
Run the full-reproduce gates, checking each by **explicit exit code** — a piped
`grep`/`tail` reports the pipeline's last command and has masked real failures
here:
```sh
make gates          # lint, check, test, doctest, check-features, deny, ci-lint
make bench-ab REF=main  # if a wall-time claim is in the ledger
make loom           # if sync/loom code was touched
make test-docker    # the delivery-guarantee claims live here; opt-in, ~minutes
```
Every target passes `--locked`, as CI does. An ad-hoc cargo call added during
review needs it too, or it can resolve a different graph than the one under
review.
Then, **per dimension** (not per finding), run one adversarial verification pass
that tries to *disprove* each candidate and each ledger claim against the code
and the gate results. Assign each survivor a verdict:
- **CONFIRMED** — reproduced or provable from the implementation.
- **PLAUSIBLE** — a real hazard by inspection, not executed to proof.
- **UNVERIFIED** — could not be exercised (degraded gate); state what's missing.
Drop anything that gets refuted.

### 6. Report (written, terminal only)
Emit, most-severe-first:
- **Verdict-free summary** — what was reviewed, which gates ran green (by exit
  code) and which degraded to UNVERIFIED.
- **Confirmed defects** — each with: severity, category, `file:line`, the defect
  in one sentence, a concrete failure scenario (inputs → wrong outcome), and the
  refutation attempt that failed to clear it.
- **Claims ledger** — every harvested claim marked UPHELD / VIOLATED / UNVERIFIED
  with the evidence.
- **Lower-confidence / residual risks** — PLAUSIBLE items and anything a maintainer
  should eyeball.
Then **stop and hand control to the user.** Do not start fixing anything.

### 7. Walkthrough — user-driven
After the report is delivered, wait. The user drives: they raise the findings
they want to discuss or act on rather than you marching through every one. Do not
re-litigate the whole list unprompted. For each finding the user engages, record a
disposition:
- **Fix now** — accepted; queued for the fix pass.
- **Reject / dismiss** — dropped; record the user's reason in the report.
- **Defer to follow-up** — real but not this pass; record as a follow-up (a
  memory note and/or tracked item), not fixed now.
- **Needs more investigation** — verdict uncertain; go deeper (more reading, or a
  targeted repro) and bring it back before the user decides. Do not fix it yet.
Findings the user never raises stay in the report as-is; assume nothing about them.

### 8. Fixes — one batch, after explicit approval
Only once the user has explicitly approved the accepted set:
- Apply **every** accepted fix to the working tree in one pass, then show the
  **combined diff** for a single final review.
- Re-run the relevant gates (from step 5) on the result and report them by
  explicit exit code.
- **No commits and no push** unless the user asks. Deferred and rejected findings
  are not touched.
This command reports and, on approval, fixes the working tree; it never decides
merge, touches GitHub, or commits on its own.
