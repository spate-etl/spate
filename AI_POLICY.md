# AI policy

AI tools are used on this project and their use is encouraged. What a
contribution has to withstand is the same however it was produced, and this page
says what that is.

## Getting oriented

An assistant is good at entering a large Rust workspace: ask it to trace a record
from a source poll to a committed watermark, or to explain what the checkpoint
tracker is protecting.

It will not give you the intuition, and this codebase punishes its absence. The
properties the engine holds to are mostly *negative*: what must never happen, on
a path that looks fine when it does. `docs/INVARIANTS.md` states them, each
numbered `INV-N` and cited everywhere else by that number. Read them yourself, in
the source, before changing engine behaviour.

## You own what you submit

Opening a pull request or an issue means vouching for it. Ideally that means you
can say what the change does, why it is correct, and how it fits the invariants
above. If a reviewer asks a question, answer it in your own words rather than
relaying a model's.

That applies whether you wrote every line, used an assistant, or anything
between. The bar is whether somebody understands the change.

## Keep conversations human

Issue threads, pull request descriptions and review discussion work best with a
person behind them. Polishing your grammar with a tool is fine. The ideas, the
judgement, and the answers to review questions should be yours.

## Quality over volume

One clear problem and one clear solution per pull request gets read and merged
faster than a large change that does several things.

In practice:

- Tie it to a need, ideally an open issue.
- Run `make gates` and check it by exit code.
- A diff past roughly 400 lines is worth splitting, or at least flagging.
- Skip the drive-by cleanup. A formatting sweep bundled with a fix makes the fix
  harder to review and harder to revert.

## Delivery-correctness changes need a reproduction

At-least-once is the promise everything else is arranged around. A change to
delivery semantics, meaning anything that could commit a watermark past
unacknowledged data, is judged on evidence rather than on reasoning that reads
well. A plausible explanation of why the fix is correct is not evidence. A
failing test that passes afterwards is.

`spate-test`'s in-memory source and capture sink reproduce most engine behaviour
with no infrastructure at all, so that test is cheap to write. It is the most
useful thing you can attach.

The same applies to performance claims: numbers are re-measured on reference
hardware under the published protocol before they are acted on.

## Agentic contributions

A pull request written by an agent is held to the bar above: understood by the
person submitting it, covered by tests, and validated against real
infrastructure where the change warrants it.

If an agent did most of the work, say so and name the model in the pull request
description. That is not required when a tool assisted, it is not held against
the change, and it gives whoever reviews it useful context.

Commits and pull request bodies carry no AI attribution trailers or footers: no
`Co-Authored-By` for a model, no "generated with" line. Commit messages likewise
do not reference plans or iterations.

## Licensing

Contributions are accepted under Apache-2.0 §5, inbound under the same terms as
outbound. There is no CLA. If you reproduce code from elsewhere, whether you
found it or a model produced it, you are responsible for its licence being
compatible, and for saying where it came from.

---

If something is not covered here, the principle underneath it is: be considerate
of reviewers' time, and take ownership of what you submit. Thank you for
contributing.
