---
description: "Adopts one-file-per-decision MADR-format ADRs, replacing the decision-log table so a reversal creates a new record instead of overwriting the old one."
---

# ADR-0001 — Record architecture decisions in ADRs

- **Status:** accepted
- **Date:** 2026-08-06
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Spate recorded its architectural decisions in a single markdown table, the
"Decision log" at the foot of `docs/DESIGN.md`, with the columns
`Decision | Choice | Why (short)`. It grew to 46 rows and roughly 17,000
characters. Individual cells reached 2,359 characters, and seven exceeded 500,
so "Why (short)" had stopped describing the column some time ago.

The format failed in a way that matters more than its size. Because a decision
occupied a fixed row, revising one meant **rewriting that row in place**, and
the superseded reasoning was overwritten rather than kept. Two rows say so
outright: the coordination-model row records that it "supersedes the dynamic
work-stealing model this row used to describe", and the split-revocation row
that it "deliberately reverses the 'no driver-side deadline' decision taken with
the negotiated handoff". In both cases the earlier decision (what was believed,
and why it was abandoned) survives only as a gesture inside the row that
replaced it. That is the history a decision record exists to keep.

The question is what structure keeps a reversal without destroying what it
reverses, at a scale one maintainer can sustain.

## Considered options

- Keep the decision-log table, and add a convention that reversals append
  rather than overwrite
- Architecture Decision Records, one file per decision, in the MADR format
- Architecture Decision Records in Michael Nygard's original format
- An RFC process — proposals reviewed and voted on before the work starts
- Formless: no decision documents; rationale lives in commit messages only

## Decision outcome

Chosen option: "Architecture Decision Records, one file per decision, in the
MADR format", because one decision per file makes supersession structural rather
than a convention to be honored by hand. A superseded record is a file that is
never edited, so it cannot be overwritten by the decision that replaced it,
which is the specific failure being fixed.

MADR 4.0.0 minimal is the variant, plus two sections of our own. `Confirmation`
names what enforces a decision, which fits a repository whose `AI_POLICY.md`
holds that a correctness claim is judged on a failing test rather than on
reasoning that reads well. `Evidence` carries measured claims with their
provenance, because several of the figures these decisions rest on came from
rigs that no longer exist, and a number is only reusable if it says so.

MADR over Nygard because its `Considered options` section is exactly the
dimension the table already argued well and Nygard's four sections lack. MADR
over an RFC process because the two answer different questions: an RFC gates a
decision *before* it is taken, which is what a project needs when a distributed
group holds veto power. Kafka's KIPs, Flink's FLIPs and Rust's RFCs all exist to
run a vote. Spate has one maintainer. There is no vote to run, so what is needed
is a record, not a process. Formless was rejected because commit messages,
which this repository does already require to be self-contained, are indexed by
change rather than by decision: they answer "what did this commit do" and not
"why is it this way", and no amount of discipline makes them answer the second.

### Consequences

- Good, because a reversal preserves what it reverses. The superseded record
  keeps its body and gains a pointer, so the reasoning that was abandoned stays
  readable alongside the reasoning that replaced it.
- Good, because one decision per file makes each one citable. `ADR-0012` works
  in a commit message, a code comment or a review the same way `INV-5` already
  does.
- Good, because `Considered options` gives the rejected alternatives a home.
  They were the least durable part of the table and are the most valuable part
  of a record.
- Bad, because the decisions are now spread across many files, and no single
  page can be read start to finish the way the table could. The index at
  `docs/adr/README.mdx` is a partial answer and not a complete one.
- Bad, because a per-decision format invites recording decisions that do not
  warrant one, which is how decision logs die. The exclusion rule in
  `docs/adr/_template.md` exists against that, and it is a rule requiring
  judgment rather than a gate that can enforce itself.
- Bad, because ADRs are uncommon in comparable projects, so the format is
  unfamiliar to most contributors arriving from other data-infrastructure
  codebases. The template carrying its own rules is the mitigation.

### Confirmation

`scripts/adr.sh --check`, run by `make ci-lint` and therefore by CI. It holds
the mechanical half: numbers unique and never reused, status values from the
permitted set, no unfilled `REPLACE-ME` placeholder, and every record present in
the index. The judgment half, whether a decision warranted a record at all and
whether `Considered options` is honest, is review, and cannot be automated.

## More information

- The exclusion rule, the section-by-section rules, and the departures from
  upstream MADR are stated in `docs/adr/_template.md`.
- MADR 4.0.0: https://adr.github.io/madr/ — dual MIT / CC0-1.0, so the template
  carries no attribution burden.
- Michael Nygard's original article, which is where the numbering and
  supersession rules come from:
  https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions
- Landed in the commit that created `docs/adr/`.
