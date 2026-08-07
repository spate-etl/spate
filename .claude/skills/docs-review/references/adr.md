# Decision records

Replaces steps 2 and 3 for `docs/adr/**`; steps 1 and 4 still apply.

The rules are `docs/STYLE.md` § 9 and `docs/adr/_template.md`, which § 9
declares **normative for a record's contents** — its guidance sits inline beside
the section each rule governs. Read it before judging a record's sections, and
cite it rather than paraphrasing it into a review comment.

## 1. Immutability — settle this before reading for quality

An accepted record is immutable (§ 9), and every judgment below is downstream of
it: against an accepted record, most demand an edit that is not available.

| The edit | Verdict |
|---|---|
| Typo, broken link, wrong or moved file path | Allowed — § 9 says so explicitly |
| `Status:` → `superseded`, filling `Superseded by:` | Allowed; the sanctioned change to an accepted record |
| Rewording an outcome, softening a claim, adding or dropping a consequence or option, changing a number | **New record**, not an edit |
| "It is out of date, the code does X now" | **New record.** A record is not a description of the system |

The test that settles the middle rows: **would a reader of the old text and a
reader of the new come away with a different account of what was decided, or of
why?** If yes it is a rewrite, whatever its size.

§ 9 names the failure this prevents, and it already happened here: the
decision-log table these records replaced recorded reversals by overwriting the
rows they reversed. "The old reasoning is wrong, so fix it" is the defect, not
the fix — ADR-0023 keeps a convergence claim that did not survive, and says so.

## 2. The supersede protocol

Both directions, always (`_template.md`). Check all five:

- New record: `Supersedes:` holds a relative extension-qualified link, and the
  body says why the old decision did not survive.
- Old record: `Status:` → `superseded`, `Superseded by:` filled, **body
  untouched**. That bars edits made *after* acceptance — not supersession
  content a backfilled record carried from birth. Several pairs here were
  written into the tree in one commit, so the older body was never touched at
  all; check when the file was created before treating this as violated.
- The new record takes the next number. Numbers are never reused.
- `docs/adr/README.mdx` gains a row, and the old row's `Status` column is
  updated by hand — `make check-adr` checks the link, not that column.
- Nothing replaces it? That is `deprecated`, not `superseded`.

## 3. Suspended rules — do not report these

§ 9's table in review terms — and the tree's largest false-positive source.

- **§ 1 vendor neutrality is suspended.** A vendor name, a vendor setting key,
  client-library behavior — none is a defect in a record.
- **§ 7 present tense and "never a changelog" is suspended.** Past tense, a
  date, "this replaced X", "at the time this was believed" — all correct here.
- **§ 8 no-YAML-frontmatter still applies.** Records are rendered pages; status
  is a body line.

§ 7's no-first-person-plural rule is not suspended, but accepted records use
"our" throughout. Raise it on a draft, never against an accepted record.

**Suspended is not encouraged.** The tree's practice is to state outside
evidence in its own terms — ADR-0038 writes "mature implementations of this
shape are unanimous the other way" rather than naming three products — and the
records are stronger for it. Never raise a vendor name in an accepted record;
when *authoring* one, reach for the neutral form first.

## 4. What is still a defect

- **Usage guidance.** § 1's closing paragraph and § 9 both draw this line: a
  record is "not documentation, and not a Diátaxis quadrant". A configuration
  key table, a YAML block written to be pasted, "to enable this, set…" — the
  record has become the wrong kind of document. Fix: a link from
  `## More information` to the guide, never an expansion in place.
- **The system explained in `Context and problem statement`** instead of the
  forces in play (`_template.md`).
- **`Consequences` with no `Bad, because`**; `Considered options` padded with an
  option nobody would have taken, or missing a real "do nothing".
- **A figure with no provenance** in `Evidence` (§ 7's last bullet, reached by
  step 1) — records are where the load-bearing figures live. A record resting on
  no measurement deletes the heading rather than emptying it (`_template.md`).
- **A load-bearing claim nobody can check.** § 7's provenance rule is worded for
  figures, and the practice just above pushes outside evidence into prose that
  names nothing — "mature implementations are unanimous", "the standard approach
  is". Stating it in our own terms is right; leaving it unfalsifiable is not. If
  such a claim carries a pillar of the decision, it needs saying what it rests
  on, exactly as a number would.
- **More than one decision per record**, and a vague `Confirmation`: "Nothing
  yet" is honest and useful, "review will catch it" is not.

## 5. Style that still applies

§ 9: "everything else follows the rest of this file."

- Sentence-case headings; identifiers backticked as the compiler spells them.
- Relative extension-qualified links, `[ADR-0026](0026-coordination-fencing.md)`
  (§ 8); `onBrokenLinks: 'throw'` fails the build on a stale one.
- The em-dash gloss (§ 7). Records carry no `## Related`; their cross-links sit
  under `## More information` and take the same gloss. The rule is that a link
  explains itself, not that the punctuation is a dash — a colon does the job.
  The landing-commit bullet is not a cross-link and needs none.
- American English (§ 7), with `cancelled` the deliberate exception — the metric
  label value is `outcome="cancelled"` and ADR-0040 and ADR-0041 match the
  exposition. Do not sweep it.

## 6. Did it deserve a record at all?

§ 9 assigns this to review; no gate can express it. The bar is `_template.md`'s:
structure, a key quality attribute (delivery semantics, throughput, memory,
operability), or hard to reverse. Excluded are a dependency bump, a rename, a
formatting choice, a fix with one sensible shape — and, most often missed,
**an existing record already covers it**: cite it, amend nothing.

Ask what a reader loses if the file does not exist. If the commit message
carries it there is no record here, and a needless one is a finding rather than
a nit — `_template.md` argues this exclusion is the whole guard against decay.
The rarer, worse miss is a change that clears the bar and lands without a
record, usually one quietly replacing an old design.

## 7. Gates

Step 4's `make check-adr` covers numbers unique and never reused, status from
the permitted set, no bare `REPLACE-ME`, and every record linked from
`README.mdx`. **Do not re-verify those by hand.**

It does not cover, and review must: reciprocal `Supersedes` / `Superseded by`,
the index row's `Status` and `Date` columns, whether the date is the date the
decision landed, and everything in §§ 1–6. It never reads prose for meaning, by
design — a gate with opinions about wording would demand edits to immutable
files.

Two scoping notes, both learned the expensive way:

- **Only the landing date is checkable.** Confirm `Date` against the commit
  named under `## More information`. The parenthetical recorded-date of a
  backfilled record is not verifiable from the tree and is not review's to
  confirm.
- **`make docs` is not always worth paying here.** It runs `npm ci` and a full
  site build, and on a record that adds no file and links only to sibling
  records it validates paths you can check by hand in seconds. Run it when a
  record is **added or renamed**, or when it links into `user-guide/`; otherwise
  verify the link targets directly and rely on CI's site job.

## Authoring a record

`make adr-new SLUG=short-description` allocates the next number and fills
today's date. Then:

1. Fill every `REPLACE-ME`, deleting the guidance comments as you go.
2. Write `Considered options` before `Decision outcome` — it is what makes the
   record worth more than a commit message, and the first thing lost when one is
   written from memory.
3. Set `Date` to the date the decision **landed in the tree**, citable to the
   commit named under `## More information`. A backfilled record says so:
   `2026-07-21 (recorded 2026-08-06 from the decision log)`.
4. Add the `README.mdx` row by hand — the scaffold does not.
5. Superseding something? § 2 above, both directions.
6. Run steps 1 and 4 against the draft, then §§ 3–6 here as your own reviewer.
