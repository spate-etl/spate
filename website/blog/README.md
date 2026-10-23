# The blog

The posts at <https://spate.kainth.dev/blog>. This page has the mechanics of
adding one. The site build leaves it out.
[`handbook/blog-posts.md`](../../handbook/blog-posts.md) has how a post is
written: the topic, the title and description, the shape of each kind of post,
evidence and voice.

## Front matter

A post is `YYYY-MM-DD-slug.md`, or `.mdx` when it renders a component, dated
for the day it goes live:

```yaml
---
title: Sentence-case title
description: "What the reader learns, in 160 characters or fewer."
slug: short-slug
authors: [marcus]
tags: [engineering]
image: /img/post-social.png
---
```

`image` is optional. Without it a link preview shows the site's own card.
`npm start` shows drafts, so a post can be read in place while it is written.

## The release post

Every minor release, `X.Y.0`, has a post. A patch release has none.

The file is `YYYY-MM-DD-spate-X-Y.md`, or `.mdx`, dated for the release day,
with this front matter:

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
deploy from `main` does not publish it early.

The rest is automatic. `cargo xtask release assemble` refuses an `X.Y.0`
unless exactly one post carries `slug: releases/X.Y`, and the release commit
removes the `draft: true` line. A `draft` key written any other way, a merge
key (`<<`), a key that is not a plain string, or front matter that does not
parse as YAML stops `assemble`. The post goes live at `/blog/releases/X.Y`
with the first site deploy after the release pull request merges, normally the
one the release dispatches, and the GitHub release body links it.
[`RELEASING.md`](../../RELEASING.md) has the release itself.

## Tags

An engineering post, on how something was built, measured or fixed, takes
`tags: [engineering]`. Every tag is declared in `tags.yml`, and the build fails
on a tag that is not, so a new tag goes into `tags.yml` first.

## Snippets and charts

Snippets are written by hand, because a dated post shows the code as it was on
its date ([`docs/STYLE.md`](../../docs/STYLE.md) § 11). A dependency line
carries the release's version, as in
`spate = { version = "X.Y", features = ["s3"] }`; the release version check
leaves the blog out.

A few figures compared side by side can be drawn with the grouped bar chart in
`src/components/charts/`, whose caption states them in words. The feeds drop
every inline drawing and its legend, so a figure's caption must carry its
content in words.

Posts wrap at 80 columns, like the other Markdown in the repository.
