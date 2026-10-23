# Blog posts

A post on <https://spate.kainth.dev/blog> exists for a reader who reaches it
from a search or a link. That reader has met a problem with moving a stream
into a warehouse, and should leave understanding the problem and what Spate
does about it. Every rule here serves that reader.

[`website/blog/README.md`](../website/blog/README.md) has the mechanics: the
file name, the front matter, the draft flag, tags and the chart component.

## Who reads it

Engineers who run pipelines from a broker or an object store into a warehouse,
and Rust engineers who build data systems. They know their own tools well. They
arrive with a symptom, a tuning question, or a choice between tools. Write for
someone who has met the problem and has not yet found its cause.

## Choosing a topic

A topic is worth a post when an engineer would ask you to explain it, when it
would have saved you time before you found the answer, or when a reader would
search for it: an error message, a throughput ceiling, a comparison between two
tools.

Spate's posts come from five places:

- why Spate exists, and which limits in other tools it removes;
- a bottleneck found, explained and fixed;
- a decision and the reasons for it;
- a measured comparison with another tool;
- a release.

## The kinds of post

**Engineering**, tagged `engineering`, is the arc of something built, measured
or fixed: the symptom, where the time or the bytes went, the fix, and the
proof, measured before and after on the same setup. A section on what the fix
costs belongs in it.

**Why Spate exists** sets out the options a reader has today and what each one
costs them, then the choice Spate makes and the evidence that it holds. The
cost of each option is concrete: the feature, the version and the measured
limit.

**Comparison** measures Spate against a named tool on one workload. It follows
[Writing about other tools](#writing-about-other-tools) in full.

**Release**, tagged `release`, announces a minor release. Its opener says what
a user can do that they could not before. Breaking changes come first, each
with the old form and the new. Then one `##` per highlight, five to ten of
them, each a capability with a line or two of why, a YAML or Rust snippet that
was run, the output it printed where output helps, and one line on what that
output shows. A fix worth telling is a short arc of symptom,
cause, fix and proof. A highlight someone else wrote carries a credit line. The
close links the release's section in `CHANGELOG.md` and thanks the contributors
by name.

## The title

The title is what a reader judges in a search result, a feed and a link
preview.

- Name what a reader would search for: the tools (ClickHouse, Kafka, S3, Rust)
  and the problem.
- State the claim, or the question the post answers. "Why zstd inserts into
  ClickHouse ran at 42% of lz4" tells a reader what they will learn.
- Use a number when the number is the claim. Hacker News asks submitters to
  crop "a gratuitous number or number + adjective".
- Keep it to 52 characters or fewer. The page title appends ` | Spate`, and
  search results cut a title at about 60.
- Use sentence case, and leave out the site's name.

A release post's title is `Spate X.Y`, so its description carries the search
terms.

## The description

The `description` field is the summary search results and link previews show.

- Keep it to 160 characters or fewer, which is about where search results cut
  it.
- Lead with what the reader learns, with the number when there is one.
- Name the tools the post is about.

## The social image

A link preview shows the `image` field, or the site's own card when a post has
none. A post with a figure worth sharing sets `image` to a 1280 by 640 PNG
under `website/static/img/`, with the figure's point readable at that size.

## The opening

The text above `<!-- truncate -->` is what the blog list shows, so it has to
stand on its own. A feed item carries the whole post, with the `description`
as its summary.

- The first two sentences carry the problem or the result, with its number.
- A claim about what Spate is for may open the post when the post carries the
  figure that backs it.
- Leave out project history and any sentence about the post itself.

## Headings and sections

A reader skims the headings before deciding to read. Each heading says what its
section shows: "Where the time went", "What it costs". A generic heading such
as "Background" or "Results" tells the skimmer nothing.

Each section answers one question, and makes sense lifted out on its own,
because search results and answer engines quote sections. Name the subject in
a section's first sentence. An "it" carried over from the section before means
nothing once the section is quoted alone.

## Evidence

- A number comes from a recorded run. A performance claim shows the before and
  the after on the same setup, names the hardware and the number of runs, and
  says which benchmark tier proves it.
- What did not work belongs in the post: the dead end, the result you cannot
  yet explain, the cost of the fix.
- An alternative you rejected gets its own statement of what you tried and what
  it cost.
- Every config key, default, metric name and figure is read from the tree the
  post describes. Every quantifier is checked for its exceptions, and the
  exceptions go in the sentence.
- A claim about another tool links to where it was measured: the
  [benchmark page](https://spate.kainth.dev/benchmarks) or a public run.

## Writing about other tools

The frustrations with other tools are the posts most likely to be shared, and
the easiest to dismiss as a vendor's view.

- Tell each frustration as your own experience: the tool, its version, the
  configuration, what you saw and what it cost you.
- Measure the other tool with its defaults, or publish the tuning beside the
  result. Link its configuration in the
  [benchmark repository](https://github.com/spate-etl/benchmark).
- Include the cases where the other tool does better, and what Spate does not
  do.
- State the conflict of interest. You build Spate and run the benchmark.
- Say how Spate handles the problem. A post that only lists another tool's
  faults reads as a rant, and This Week in Rust declines rants.
- When the other tool's maintainers correct a fact, fix the post and note the
  correction at its foot.

## Voice

Where a post already in `website/blog/` differs from these rules, the rules
hold.

- "I" for what you built, measured or decided. "You" for the reader's
  situation and what it costs them.
- Plain sentences, joined by cause: "This meant that", "which led to". Let a
  sentence be as long as its thought.
- Full sentences for a run's parameters: "five rounds, each 45 seconds long"
  where a clipped list would say "five rounds of 45 seconds".
- A decision states its reasons first: "I deprecated split coordination
  because …", before the mechanics.
- A removal gives its reason where a reader would ask why. Do not offer a way
  to keep the removed behavior.
- A change from the last release is marked "now", and what comes next "will".
- A claim about performance carries its number. Phrases that grade the work,
  such as "absolute performance" or "built from the ground up", come out.
- No "X, not Y" contrasts, no hedging, nothing that tells the reader how to
  feel, no emoji.
- American English, sentence-case headings, and link text that names its
  destination, as [`docs/STYLE.md`](../docs/STYLE.md) § 7 sets out.

## Links and the close

- Link three to five pages of the docs, each at the first mention of what it
  explains.
- Link the record behind a claim: the decision record, the issue or the pull
  request.
- Close with one next step: the quickstart, or the docs page for what the post
  covered. No sign-up, newsletter or star prompt.

## Proof and review

Snippets follow [`docs/STYLE.md`](../docs/STYLE.md) § 11, and the output
shown is the output a snippet printed. Every post merges through a pull
request and review, with `draft: true` until it is due to go live.

## Where a post is shared

Share a post under its own title, from your own accounts.

- **Hacker News.** Submit under the post's title. Do not ask anyone for votes;
  the site penalizes submissions that do. A blog post is a regular submission.
  Show HN is for something readers can try, and lists blog posts as off topic.
- **Lobsters.** Its rule of thumb is that self-promotion stays under a quarter
  of an account's stories and comments over a year.
- **This Week in Rust.** It does not accept an article written by an LLM, and
  its editors may decline one with excessive LLM output. A release post is a
  project update, which it takes only from posts on r/rust. Propose any other
  post by pull request against the issue in its `drafts/` folder. A post that
  shows how something is done in Rust code fits Rust Walkthroughs.
- **Personal accounts.** One or two sentences carrying the post's result, and
  the link.
