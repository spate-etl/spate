# AI policy

AI tools are used on this project and their use is encouraged. This page is not
about restricting how you work. It is about what a contribution has to be able
to withstand, which does not change with how it was produced.

## Getting oriented

A large Rust workspace is genuinely hard to enter, and an assistant is good at
the entering: ask it to trace a record from a source poll to a committed
watermark, or to explain what the checkpoint tracker is protecting. That will
save you hours.

It will not give you the intuition, though, and this codebase punishes its
absence in a specific way. Almost everything load-bearing here is a *negative*
property — what must never happen, on a path that looks fine when it does.
`docs/INVARIANTS.md` states ten of them as INV-1 through INV-10. Read those
yourself, in the source, before changing engine behaviour. A model can tell you
they exist; only reading tells you why each one is shaped the way it is.

## You own what you submit

Opening a pull request or an issue means vouching for it. Ideally that means you
can say what the change does, why it is correct, and how it fits the invariants
above. If a reviewer asks a question, answer it in your own words rather than
relaying a model's.

That applies the same whether you wrote every line, used an assistant, or
anything between. The bar is not "how was this made" — it is "is there somebody
who understands it".

## Keep conversations human

Issue threads, pull request descriptions and review discussion work best with a
person behind them. Polishing your grammar with a tool is fine. The ideas, the
judgement, and the answers to review questions should be yours.

## Quality over volume

AI makes it easy to produce a lot of code quickly, and review capacity is the
thing that does not scale with it. One clear problem and one clear solution per
pull request gets read and merged faster than a large change that does several
things.

Some practical shape, most of which is in `CONTRIBUTING.md` too:

- Tie it to a real need — ideally an open issue.
- Run `make gates` and check it by exit code.
- A diff past roughly 400 lines is worth splitting, or at least flagging.
- Skip the drive-by cleanup. A formatting sweep bundled with a fix makes the fix
  harder to review and harder to revert.

## Delivery-correctness changes need a reproduction

This is the one place the bar is genuinely higher, and it is not about AI.

At-least-once is the promise everything else is arranged around, so a change to
delivery semantics — anything that could commit a watermark past unacknowledged
data — is judged on evidence rather than on reasoning that reads well. A
plausible explanation of why the fix is correct is not evidence. A failing test
that passes afterwards is.

`spate-test`'s in-memory source and capture sink reproduce most engine behaviour
with no infrastructure at all, which makes that test cheap to write. It is the
most useful thing you can attach.

The same applies to performance claims: numbers are re-measured on reference
hardware under the published protocol before they are acted on. That is about
comparability, not doubt.

## Agentic contributions

A pull request written by an agent is not a special category and is held to
exactly the bar above: understood by the person submitting it, covered by tests,
and validated against real infrastructure where the change warrants it.

If an agent did most of the work, say so and name the model in the pull request
description. It is not a requirement when a tool merely assisted, and it is not
held against the change — it is useful context for whoever reviews it.

Commits and pull request bodies should carry no AI attribution trailers or
footers: no `Co-Authored-By` for a model, no "generated with" line. That is a
formatting convention rather than a judgement, and it is the same reason commit
messages do not reference plans or iterations — the message is for somebody
reading the history later, and tooling metadata is noise to them.

## Licensing

Contributions are accepted under Apache-2.0 §5, inbound under the same terms as
outbound. There is no CLA. If you reproduce code from elsewhere — whether you
found it or a model produced it — you are responsible for its licence being
compatible, and for saying where it came from.

---

If something is not covered here, the principle underneath it is: be considerate
of reviewers' time, and take ownership of what you submit. Thank you for
contributing.
