---
description: "What rustdoc and an inline comment state, what never goes in a comment, keeping a change to one purpose, and the reuse rule a change meets before it adds code."
---

# Code and comments

Write a comment for a senior Rust engineer who is new to this code.
A comment carries what the code beside it cannot show: the contract a caller relies on, or the reason a maintainer needs before changing a line.

Pull requests are squash-merged into `main` with an empty body, so a reason the next editor needs goes in the code.

## Rustdoc states the contract

Open an item's rustdoc with one sentence saying what the item does.
Add more only for what a caller relies on and the signature does not say: units, ordering, blocking, cancel safety, or an invariant the item upholds.

Add `# Errors`, `# Panics` and `# Safety` sections where they apply, and `# Examples` on a public entry point.
Open a module with a header that names what the module provides and its role in the crate.

The workspace lints warn on `missing_docs`, and `cargo xtask clippy` denies warnings, so it fails on a public item with no rustdoc.

## An inline comment states why

Write an inline comment where the code cannot show its reason:

- A guard on a line that looks removable, such as a deliberate re-read, an `#[inline(never)]`, or an ordering that must hold.
- A constraint the code relies on and does not show, such as a lock held or an upstream behavior.
- A `// SAFETY:` comment on every `unsafe` block, naming the invariant that makes it sound.

The workspace lints warn on the clippy lint `undocumented_unsafe_blocks`, so `cargo xtask clippy` fails on an `unsafe` block with no `// SAFETY:` comment.

Leave self-evident code without a comment.
When a change removes what a comment defended, remove the comment too.

## What never goes in a comment

Keep these out of every comment:

- History: old behavior, "now", upstream versions or advisory IDs.
- Callers: which ones, how many, how often.
- A tour of the control flow a reader can see.
- Line numbers.

The rejected alternative and how the change came about go in the pull request body.

A test's doc comment says what the test pins.
[Write the test's doc comment](04-writing-a-test.md#write-the-tests-doc-comment) gives the one line it may add.

## Keep a change to one purpose

Make one logical change per commit.
Implement the smallest change that solves the problem, and leave polish to a follow-up.
Change a dependency, the formatting or unrelated code only when the task needs it.

## Reuse before you add

Before you add a function, type, constant, check, default or seam, search `crates/*/src` and `test-support/` for one that does the same job.
Search by the trait it wraps, the type it holds and the words in its name.

When new code would repeat existing logic or a similar type, share the original:

- A copy within a crate moves onto one helper in the same change.
- A copy across crates moves to where [The workspace](02-workspace.md#put-shared-code-in-spate-core-unless-its-api-names-a-connector-type) says shared code lives.

## Related

- [Writing a test](04-writing-a-test.md) — where a test lives, and what its doc comment says.
- [The workspace](02-workspace.md) — what belongs in `spate-core`.
- [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) — commit subjects, pull request titles and bodies, and the prose rules for comments, commit messages and pull request bodies.
