# Spate invariants

The properties the engine is arranged around. This list is canonical: the
statements below are what `AGENTS.md`, `CONTRIBUTING.md` and the pull request
template each restate for their own audience, and they cite these numbers so a
disagreement between them is a mismatched identifier rather than a difference in
phrasing somebody has to notice.

`scripts/check-invariants.sh` compares the *set of numbers* across those files.
It cannot compare their wording, so a restatement that drifts in substance while
citing the right number passes — the statements here are what the others must
mean, and any exception belongs in this list rather than only in the restatement.

Most changes touch none of these. A change that touches one is not thereby
wrong; it needs to say how the property still holds, and that is the review.

Numbering is append-only. A property that is ever retired keeps its number and
is marked retired rather than freeing it for reuse, so a pull request from a
year ago still means what it said.

- **INV-1 — delivery is at-least-once.** A source watermark is never committed
  past unacknowledged data, including across rebalances and shutdown. Everything
  else exists to make this one affordable.
- **INV-2 — source threads never block on a channel send.** Backpressure is
  `try_send` plus `Source::pause` plus continuing to poll. A blocked poll loop
  gets the consumer evicted from its group, which is a worse failure than the
  one it was avoiding.
- **INV-3 — the checkpoint tracker stays synchronous and free of async
  runtimes.** It is loom-tested, and loom can only model what stays this shape.
- **INV-4 — acks never block behind data.** The ack path is unbounded and
  atomic. An ack queued behind the data it acknowledges is a deadlock waiting
  for backpressure to arrive.
- **INV-5 — the sink worker's intake path never awaits outside its `select!`.**
  Anything it blocks on sits in a branch alongside the drain-deadline branch, or
  the deadline is not polled while it waits and shutdown deadlocks.
- **INV-6 — no connector types in `spate-core`'s public API**, and no 0.x
  dependency types in any public trait bound. Those cannot enter our semver
  surface. The `metrics` facade is the one sanctioned exception, because the
  instrumentation API *is* that facade.
- **INV-7 — record error policies are Skip or Fail only**, and both are surfaced
  through metrics rather than only logged. There is deliberately no third policy
  that drops a record without counting it.
- **INV-8 — metrics handles are pre-registered at build time.** A metric name or
  label resolved on the per-record path is a per-record allocation and a lookup
  in the one place neither is affordable.
- **INV-9 — every metric lives under the `spate_` umbrella.** The framework owns
  the reserved stage roots; connector and user families register through a
  `Meter`, which prefixes them and rejects a namespace shadowing a reserved
  root. The one sanctioned exception is a metric registered on the raw `metrics`
  facade, which is the deliberate opt-out for a name that must sit outside
  `spate_` — an exporter's own series, or one a downstream contract fixes.
- **INV-10 — a gauge series has exactly one live owner per process.** A
  duplicate claim on the same key is refused rather than shared. Assembly makes
  it fatal (`BuildError`/`StartError`); direct construction cannot fail a build,
  so it logs and the loser becomes a *shadow* — it still counts, since counters
  sum, but it publishes no gauge. Two live owners would be two writers racing to
  describe one piece of state, and the exposition cannot show that happened.

## Where the reasoning lives

An invariant states a property; it does not argue for it. The argument is in the
decision record that established it — [`adr/`](adr/README.mdx) indexes them, and
each record's `Confirmation` section names the invariant, test or gate that holds
it.

One documentation page is normative rather than descriptive:
[`user-guide/02-concepts/08-work-assignment.mdx`](user-guide/02-concepts/08-work-assignment.mdx).
Its own numbered invariants name the property tests that enforce them, and are
separate from these — changing the balancer means changing that page in the same
commit.
