# Prose failure modes

The catalog for step 3 of [SKILL.md](../SKILL.md). The rules are in
`docs/STYLE.md` § 7; this names the shapes they break in and how to tell each
from its false positive.

**Read the page before you read this file.** The reading pass is the finding;
the catalog only helps you name what you already felt. A reviewer who opens
the checklist first finds items on it, which is a different thing from finding
defects.

## The reading pass

Read top to bottom, once, checking nothing, as the reader § 7's preamble names:
fluent in async Rust, already holding at-least-once, consumer groups,
backpressure and partitioning as concepts, knowing nothing Spate-specific.

Mark every point where you would **stall** (a term you cannot resolve),
**re-read** (a sentence that parses two ways), or **leave** (give up and open
the rustdoc, or the source). Those three marks are the report. Everything below
is vocabulary for describing them.

## The reader is wrong for the page

**Re-teaching a shared concept.** A paragraph defining backpressure, or
at-least-once, or what a partition is, before getting to what *this* system
does. It reads as helpful and costs the reader the thing they came for. The
tell: delete the paragraph and ask whether the page still answers its title. If
it does, the paragraph was ballast.

*Not this:* a one-clause reminder that anchors a Spate-specific claim
("backpressure here pauses lanes rather than blocking the send, because …").
That is the claim, not a lesson.

**A Spate term used before it is defined.** `split`, `lane`, `shard`,
`assignment`, `fence`, `drain`, `seam` — every one is a word the reader knows
from elsewhere with a different meaning. Used cold, it does not read as unknown;
it reads as *known and wrong*, which is worse, because the reader does not stop.
§ 7 requires the term defined once in the glossary; check the page either links
it on first use or defines it inline.

**Assumed Spate knowledge with no path to it.** "As with the coordinated
sources" on a page that never says which sources those are, or what coordination
means here. The reader cannot follow the reference and cannot tell whether they
were supposed to.

## The page is in the wrong quadrant

§ 6 fixes the quadrant by directory; these are the leaks that show up *within* a
correctly-filed page. Diátaxis applies at sentence scale, so a single paragraph
counts.

**Explanation inside a tutorial.** The commonest failure in the whole tree, and
Diátaxis names its cause: the writer's urge to impart what they know. A tutorial
paragraph beginning "The reason for this is…" belongs in `02-concepts/` with a
link. The reader is mid-task; they cannot spend attention on why.

**Branching inside a tutorial.** "If you prefer Docker, instead…". A tutorial is
a managed path and must not fork — every alternative is a chance to end up
somewhere the rest of the lesson does not describe. How-to guides fork; tutorials
do not.

**Procedure inside reference.** A connector page or the appendix growing numbered
steps. Reference is consulted, not read; the steps belong in `03-guides/`.

**A reference table inside a tutorial or how-to** (§ 6). Link to the appendix.

**Explanation that considers no alternative.** Explanation is the one quadrant
where judgment is allowed and where trade-offs are the *content*. A page in
`02-concepts/` that describes only what the system does, with no sense of what
else it could have done, has become reference filed in the wrong place.

**Voice against the quadrant** (§ 7). "You get a retry" on a concepts or
connector page; "the reader configures the sink" in a guide. Check against the
directory, not against the surrounding paragraphs — a page drifts as a whole.

## The claim is not underwritten

**A performance figure with no provenance** (§ 7). A defect even when the figure
is true, because nobody downstream can check it or know when it expired.

**An unverifiable superlative.** "fastest", "guarantees", "the simplest way",
"scales to any load". Either it is measured — in which case it carries its
provenance — or it is a claim the project cannot stand behind. This is the one
lexical pattern worth watching for, because the words are load-bearing rather
than stylistic.

**A hedge standing in for an answer.** "may", "should generally", "in most
cases" where the behavior is in fact deterministic. The reader needs to know
what happens; if it genuinely depends, say what it depends on.

## The sentence works against the reader

**Buried lede.** The page's actual answer sits in paragraph three. Ask of the
first screen: is this what the title promised? This is the highest-yield single
question in the pass.

**Condescension.** "simply", "just", "easy", "obviously", "of course", "all you
need to do". Every one asserts the reader's experience for them, and is wrong
for whoever it was hard for. Cutting the word almost always improves the
sentence, which is the tell that it was carrying nothing.

**Conditions after the instruction** (§ 7). A reader who acts on the first half
has already acted.

**Future tense for current behavior** (§ 7). "The sink will retry" — it retries.

**Changelog voice** (§ 7). "now", "recently", "as of", "has been changed to".
Suspended in `docs/adr/` only.

**Self-referential opening** (§ 7). "This page explains…", "In this guide we
will…". The most valuable line in the document, spent on nothing.

**A count of the list beside it** (§ 7). "Two things to notice:", "Three
responses remain:", "## The two coordination latencies". The list counts itself,
and the next contributor adds a bullet without reading upward. Delete the
numeral; the sentence rarely loses anything else.

*Not this:* a number the source closes — an enum's variants, a `const`
array — which step 1 verifies instead. Nor a number that *constrains* the set: § 3's
"exactly three forms, no others" is the rule, and deleting the number deletes
it.

**Link text that names nothing** (§ 7). "here", "this page", "see this". Also
the near-miss: link text that names a *different* thing from what is on the
other side, and the path standing in for a name — `[docs/METRICS.md](…)` where
"Metrics" is what the reader is looking for.

**A repository path the reader cannot act on** (§ 7). A bare
`crates/spate/examples/memory_pipeline.rs` in a sentence. The site reader has no
checkout, so it names a layout only a contributor holds; § 7 gives the `repo:`
form. The build catches a path that does not resolve — what it cannot catch is a
path that resolves and was never made a link, which is why this is here.

*Not this:* a path in a fenced block, or in `CONTRIBUTING.md`, `DEVELOPING.md`,
`AGENTS.md` and `docs/STYLE.md` itself — contributor files, none of them
published, all read from a checkout.

**First-person plural** (§ 7). "we recommend", "our design".

**Terminology drift.** The same thing under two names across one page — "worker"
and "instance", "chunk" and "batch", "split" and "work unit". § 7 says define
once and stay consistent; a synonym reads as a second concept.

**Admonition misuse** (§ 7). Two stacked; one opening a section; `:::danger` on
something that costs time rather than data — which spends the one form reserved
for data loss and leaves nothing louder for when it matters.

## Not defects here

Do not report these. They are the lint-shaped noise this pass exists instead of,
and several have measured false-positive rates worse than 4:1 in the upstream
style packages they come from.

- **Passive voice**, as a blanket rule. § 7's own exemplar is passive ("Unknown
  fields are rejected") and is correct: the actor is the framework, and naming
  it adds nothing. Raise passive voice *only* where the actor is genuinely
  missing and the reader needs it — "the offsets are committed" when which
  component commits them is the question the paragraph exists to answer.
- **Adverbs and weasel words** as a category hunt.
- **Sentence length.** One idea per sentence is the real rule; length is a poor
  proxy for it. A long sentence carrying one idea is fine.
- **Contractions**, either way.
- **Oxford commas, spacing, quote style, en- versus em-dashes.** Style, not
  defects, and not worth the attention budget.
- **Anthropomorphism** ("the sink sees", "the source knows"). Standard technical
  register.
- **Repetition across pages.** Documentation is allowed to repeat itself; DRY is
  a code rule. § 1's one-home rule governs *vendor facts* specifically — it is
  not a license to deduplicate prose generally.
- **British spellings in `crates/spate-avro/benches/support/corpora.rs`** and in
  the `cancelled` metric label value — both deliberate (§ 7).
