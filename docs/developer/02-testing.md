---
description: "Where a test lives, which harness fits the code, what the process-wide state in a test binary demands, and what each tool proves."
---

# Writing a test

A test here pins one property of the code and passes on every run in which
that property holds. This page covers where a test lives, the harnesses in the
order to reach for them, and what the process-wide state in a test binary
demands.

## Where a test lives

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronization primitives, the Kafka mock cluster from
[`MockKafka`](repo:crates/spate-kafka/tests/support/mock.rs) and the ClickHouse
mocks for connector behavior that does not need a container.

Helpers that only this repository's tests need, such as reading a pinned image,
belong in the unpublished `spate-test-support` crate under `test-support/`.

## Tests that do not flake

[Tests that do not flake](05-flaky-tests.md) has the rules that keep a test
passing on every CI runner, how CI differs from your machine, and what to do
when a test fails there.

## The harnesses

Reach for the lowest rung that holds the code under test. Each rung up costs
determinism, so a test that can run on the rung below is written there.

### The in-memory doubles

Framework users test their pipelines with `spate-test`'s in-memory source and
capture sink. Keep those first-class, and prefer them for reproductions. A test
written against them needs no infrastructure and runs in milliseconds.
[Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) walks the
source handle, the sink script and the committed watermark as the assertion.
Beyond what that page shows, `scripted_feed` plays a feed backend with no
queue and no clock, `LogCapture` returns a log line once it is written, and
`render_metrics` renders a text exposition under a recorder of the test's own.

### Paused tokio time

`#[tokio::test(start_paused = true)]` fits code on a current-thread runtime
that reads `tokio::time`. The clock stands still until the test advances it,
and every timer the code armed fires in order when it does. This is the rung
with no scheduling in it at all.

### A test clock

`TestClock`, in `spate_core::clock` and `spate_core::clock::tokio`, fits code
that holds a `SharedClock` on OS threads or on a multi-thread runtime, where
tokio time cannot be paused. It moves only when the test moves it, and the
tokio one records each deadline it was asked to sleep until, so a test can
wait for the timer before it advances. It does not reach a vendor client's own
timers, so a test of what librdkafka does with time sits on the next rung.

### The Kafka mock cluster

An in-process broker, with no container, for behavior that lives in the broker
protocol: assignment, rebalance, offsets, fetch errors. A test builds it with
[`MockKafka`](repo:crates/spate-kafka/tests/support/mock.rs), and clippy
refuses rdkafka's `MockCluster::new`. The mock holds a new group's first
rebalance for 3 s so that members starting together join one generation.
`MockKafka` removes that hold, and a test whose members start together and must
join one generation calls `hold_first_rebalance`. The hold lasts 3 s of real
time, and a member that reaches the coordinator once it has ended forms a later
generation. [Wait for a signal the code
publishes](05-flaky-tests.md#wait-for-a-signal-the-code-publishes) says how a
test with more than one consumer settles the group.

### A container

A real server, at a pinned image, for what no double can show: the client's
wire behavior, a store's semantics, a drain against a paused sink. A
container test carries `#[ignore = "requires Docker"]` and starts its image
through `spate_test_support::container_image`.
[Container suites](01-commands.md#container-suites) says how they run and
which lanes exist.

## Process-wide state

Under `cargo test`, a binary's tests share one process. Under nextest each test
has a process of its own, which `cargo xtask test`, `cargo xtask
integration-test` and cargo-mutants all use, while the loom models run under
`cargo test`. A test holds under both.

- A gauge series has one live owner per process, keyed on the pipeline and
  component labels ([INV-10](../INVARIANTS.md)). Fixtures therefore carry
  per-test `pipeline` and `component` labels from `spate_test::unique_name`,
  and a test filters an exposition on its own label before reading a value
  from it. A local recorder does not isolate the claim: `render_metrics`
  renders under a recorder local to the calling thread, with the handles
  built inside it, and the gauge claim stays process-wide.
- What a listener binds and what a `tracing` subscriber owns are process-wide
  too. [Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) says
  how a test takes a kernel-chosen port, or no port, and when a subscriber
  wants a binary of its own.
- The end-to-end scenarios under `crates/spate/tests` start their servers once
  per binary and share them across that binary's tests; the connector suites
  start a server inside each test. Either way, topics, tables, groups and
  buckets carry a `unique_name`.

## What each tool proves

### cargo-mutants

That a guard is reached by a test. `cargo xtask mutants --since origin/main`
mutates the diff and runs the suite once per mutant; a survivor is a branch no
test distinguishes. `.cargo/mutants.toml` excludes the S3 and ClickHouse seam
modules, and the forms the tool does not generate stay a review step.
[Mutants](01-commands.md#mutants) has the command.

### loom

Every interleaving of a synchronization primitive. A model runs the threads
under loom's scheduler and asserts what must hold on all of them:

```rust file=crates/spate-core/src/checkpoint/loom_tests.rs region=every_interleaving title="loom_tests.rs"
```

### proptest

An invariant over generated inputs, checked against a reference model that is
too simple to be wrong. The checkpoint tracker is held to one:

```rust file=crates/spate-core/src/checkpoint/tracker.rs region=reference_model title="tracker.rs"
```

### fuzz

A property at a boundary where bytes become records, under inputs nobody
wrote. [Fuzzing](04-benchmarks-and-fuzzing.md#fuzzing) has the targets and the
commands.

## A test's doc

A test's doc says what the test pins. Add `Regression for #N.` when issue #N
reports a defect in the code the test exercises and the test fails without its
fix. A change that fixes the test itself, such as a flaky setup, adds no such
line.

## Related

- [Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) — the
  `spate-test` doubles from the user's side.
- [Commands and gates](01-commands.md) — the runner, its profiles and the
  container suites.
- [Spate invariants](../INVARIANTS.md) — the properties a test's labels and
  waits answer to.
