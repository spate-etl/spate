---
description: "Where a test lives in this repository, which double to reach for, and the conventions a test here follows."
---

# Writing a test

## Where a test lives

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronization primitives, rdkafka's `MockCluster` and the
ClickHouse mocks for connector behavior that does not need a container.

Helpers that only this repository's tests need, such as reading a pinned image,
belong in the unpublished `spate-test-support` crate under `test-support/`.

## The in-memory doubles

Framework users test their pipelines with `spate-test`'s in-memory source and
capture sink. Keep those first-class, and prefer them for reproductions. A test
written against them needs no infrastructure and runs in milliseconds.

## Process-wide state

One trap worth knowing: `cargo test` runs a binary's tests in one process, and
metric series ownership is process-wide (INV-10). Fixtures therefore need
per-test `pipeline` and `component` labels; a local recorder does not isolate
the claim.

## A test's doc

A test's doc says what the test pins. Add `Regression for #N.` when issue #N
reports a defect in the code the test exercises and the test fails without its
fix. A change that fixes the test itself, such as a flaky setup, adds no such
line.
