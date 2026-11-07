---
description: "The comment and rustdoc contract, the prose rules for comments and pull request bodies, and the reuse rule a change meets before it adds code."
---

# Code and comments

Comments are written for a senior Rust engineer who is new to this code. A
comment carries what the code beside it cannot show: the contract a caller
relies on, or the reason a maintainer needs before changing a line. `main`
squashes with an empty body, so a reason the next editor needs belongs in the
code.

## Rustdoc states the contract

- Open with one sentence saying what the item does. Add more only for what a
  caller relies on and the signature does not say: units, ordering, blocking,
  cancel safety, an invariant upheld.
- Add `# Errors`, `# Panics` and `# Safety` where they apply, and `# Examples`
  on public entry points.
- A module header names what the module provides and its role in the crate.

## Inline comments state why, where the code cannot

- A guardrail on a line that looks removable: a deliberate re-read, an
  `#[inline(never)]`, an ordering that must hold.
- A constraint the code relies on but does not show, such as a lock held or an
  upstream behavior.
- `// SAFETY:` on every `unsafe` block, naming the invariant that makes it
  sound.
- Self-evident code takes no comment. When a change removes what a comment
  defended, the comment goes too.

The rejected alternative and how the change came about go in the pull request
body.

**Never in a comment:** history (old behavior, "now", upstream versions or
advisory IDs), callers (which, how many, how often), a tour of visible control
flow, line numbers.

A test's doc says what the test pins;
[Writing a test](02-testing.md#a-tests-doc) has the one line it may add.

## Prose in comments and bodies

In comments, commit messages and pull request bodies, avoid the dramatic
em-dash, the colon reveal, antithesis framing, evaluative tails and empty
intensifiers. Swapping one for another is no fix.

## Reuse

Reuse is part of the change: when new code would repeat existing logic or a
similar type, share the original. Before adding a function, type, constant,
check, default or seam, search `crates/*/src` and `test-support/` for the one
that does the same job, by the trait it wraps, the type it holds and the terms
in its name. A copy within a crate moves onto one helper in the same change. A
mechanism two crates would share lives in `spate-core`, and a change that
would add the second copy moves the first there instead.

## Related

- [Writing a test](02-testing.md) — the rules a test follows, and what its doc
  says.
- [Reading a review](06-reading-a-review.md) — where a review checks the
  comments and the reuse this page asks for.
- [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) — commit subjects, pull request
  titles and bodies.
