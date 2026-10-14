# The blog

The posts at <https://spate.kainth.dev/blog>. This page is for whoever writes
one. The site build leaves it out.

## The release post

Every minor release, `X.Y.0`, has a post. A patch release has none.

The file is `YYYY-MM-DD-spate-X-Y.md`, or `.mdx` when the post renders a
component, dated for the release day, with this front matter:

```yaml
---
title: Spate X.Y
description: "One sentence on what the release ships."
slug: releases/X.Y
authors: [marcus]
tags: [release]
draft: true
---
```

Merge the post on its own pull request before the release is dispatched, with
`draft: true` in place. A production build leaves a draft out, so the nightly
deploy from `main` does not publish it early. `npm start` shows drafts, so the
post can be read in place while it is written.

The rest is automatic. `cargo xtask release assemble` refuses an `X.Y.0`
unless exactly one post carries `slug: releases/X.Y`, and the release commit
removes the `draft: true` line. A `draft` key written any other way, a merge
key (`<<`), a key that is not a plain string, or front matter that does not
parse as YAML stops `assemble`. The post goes live at `/blog/releases/X.Y`
with the first site deploy after the release pull request merges, normally the
one the release dispatches, and the GitHub release body links it.
[`RELEASING.md`](../../RELEASING.md) has the release itself.

## Shape

- **The opener.** A paragraph or two that states the point of the release:
  what a user can do with it that they could not before. Then
  `<!-- truncate -->`. The blog list shows everything above the marker.
- **Breaking changes**, when there are any, as the first `##` section. Each
  one gets the migration as a snippet of the old form and the new.
- **One `##` per highlight**, five to ten of them. A highlight is one
  capability. Give it a line or two of why, a runnable YAML or Rust snippet,
  the output it really printed where output helps, and one line on what that
  output shows.
- **A performance claim** shows the before and the after on the same setup,
  names the hardware and the number of runs, and says which benchmark tier
  proves it. A few figures compared side by side can be drawn with the grouped
  bar chart in `src/components/charts/`, whose caption states them in words.
  The feeds drop every inline drawing and its legend, so a figure's caption
  must carry its content in words.
- **A fix worth telling** is a short arc in place of a feature section: the
  symptom, the cause, the fix, and the proof.
- **A credit line** under a highlight that someone other than the author
  wrote.
- **The close** links the full section in `CHANGELOG.md` and thanks the
  contributors by name.

Snippets are written by hand, because a dated post shows the code as it was at
the release ([`docs/STYLE.md`](../../docs/STYLE.md) § 11). Each one is built or
run against the release's crates before the post merges, and the output shown
is the output it printed. A dependency line carries the release's version, as
in `spate = { version = "X.Y", features = ["s3"] }`; the release version check
leaves the blog out.

## Voice

The posts already in this directory are the reference.

- First person singular. "I" for what the author built, measured or decided.
- Short, plain sentences. A long one is fine between short ones.
- Concrete numbers from real runs, including what did not work.
- No marketing: no pitch, no sign-up prompt, no emoji.
- Say what changed since the last release. "Now" and "will" are fine in a
  post.
- State what is. No "X, not Y" contrasts, no hedging, no adjectives that grade
  the work, and nothing that tells the reader how to feel about it.

Posts wrap at 80 columns, like the other Markdown in the repository.
