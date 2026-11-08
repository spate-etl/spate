---
description: "Where a test lives, which harness to choose, what a test binary shares, the tools that check a test reaches the code, and how to read a failing test."
---

# Writing a test

A test pins one property of the code and passes on every run in which that property holds.
This guide's other test page, [Tests that do not flake](05-flaky-tests.md), gives the rules that keep it passing on every CI runner.
Read that page before your first test.

## Where a test lives

Put a unit test in a `#[cfg(test)]` module beside the code it tests.
Put an integration test in the crate's `tests/` directory, and a doc test on the public item it shows.

A helper that only this repository's tests need, such as reading a pinned image, goes in the unpublished `spate-test-support` crate under `test-support/`.

## Choose the harness

Use the first harness in this list that can run the code under test:

1. The in-memory doubles
2. Paused tokio time
3. A test clock
4. The Kafka mock cluster
5. A container

**Why:** each step down the list adds scheduling or real I/O, and each one makes a test less deterministic.
A test that can run on an earlier harness is written there.

### The in-memory doubles

Use `spate-test`'s in-memory source and capture sink for pipeline behavior.
A test written against them needs no infrastructure and runs in milliseconds.
[Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) walks through the source handle, the sink script, and the committed watermark as the assertion.

The crate also has three helpers that page does not cover:

- The `scripted_feed` helper plays a feed backend with no queue and no clock.
- The `LogCapture` type returns a log line once the code writes it.
- The `render_metrics` helper renders a metrics exposition under a recorder of the test's own.

### Paused tokio time

Use `#[tokio::test(start_paused = true)]` for code on a current-thread runtime that reads `tokio::time`.
The clock stands still until the test advances it.
`tokio::time::advance` completes every timer it moves past at once, in no fixed order.
To have the timers fire in order, call `tokio::time::sleep` in the test, and the paused runtime moves the clock to each timer in turn.

### A test clock

Use `TestClock`, from `spate_core::clock` and `spate_core::clock::tokio`, for code that holds a `SharedClock` on OS threads or on a multi-thread runtime.
Tokio time cannot be paused there.
A `TestClock` moves only when the test moves it.
The tokio one records each deadline the code sleeps until, so a test can wait for the timer before it advances.

A test clock does not reach a vendor client's own timers.
A test of what librdkafka does with time goes on the next harness.

### The Kafka mock cluster

Use the Kafka mock cluster for behavior that lives in the broker protocol: assignment, rebalance, offsets and fetch errors.
It is a broker that runs inside the test process, with no container.

Build it with [`MockKafka`](repo:crates/spate-kafka/tests/support/mock.rs).
Clippy rejects a direct call to rdkafka's `MockCluster::new`.

By default, librdkafka's mock waits 3 s before a new group's first rebalance, so that consumers starting together join one generation.
The `MockKafka` helper turns that wait off.
The first consumer then forms the group alone, and each later consumer rebalances it.
A test whose consumers must join one generation turns the wait back on with `hold_first_rebalance`.

### A container

Use a container for what no double can show: a client's wire behavior, a store's semantics, or a drain against a paused sink.
Mark the test `#[ignore = "requires Docker"]`, so that a normal run skips it.
Start its image through `spate_test_support::container_image`, which reads the image pinned under `ci/<service>/`.
[Container suites](01-commands.md#container-suites) says how to run them.

## Know what a test binary shares

Under nextest each test runs in a process of its own.
Under `cargo test` a binary's tests share one process.
Nextest runs under `cargo xtask test`, `cargo xtask integration-test` and cargo-mutants.
The `cargo xtask loom` command runs `cargo test`.
Write each test to pass under both.

Three kinds of state are process-wide:

- A metric gauge series has one live owner per process ([INV-10](../INVARIANTS.md)).
  The `render_metrics` helper renders under a recorder local to the calling thread, and the gauge claim still spans the process.
- A listener's port and a `tracing` subscriber belong to the process.
  [Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) says how to take a kernel-chosen port, or no port, and when a subscriber needs a binary of its own.
- The end-to-end scenarios under `crates/spate/tests` start their servers once per binary and share them across that binary's tests.
  The connector suites start a server inside each test.

[Own every name, port and metric label](05-flaky-tests.md#own-every-name-port-and-metric-label) gives the rule that keeps tests apart.

## Show that a guard is tested

Run `cargo xtask mutants --since origin/main` before you push.
It mutates the code your diff changes and runs the suite once per mutant.
A mutant that survives is a branch no test can tell apart from the original.

The mutants config in `.cargo/mutants.toml` leaves out the `testing`-feature doubles that no shipped path reaches.
The run's summary names the mutant forms the tool does not generate, and those stay a review step.
[Mutants](01-commands.md#mutants) has the options.

## Check every interleaving

Use loom for a synchronization primitive.
A loom model runs the threads under loom's scheduler and asserts what must hold in every interleaving.
Run the models with `cargo xtask loom`.
The checkpoint tracker's model in [`spate-core/src/checkpoint/loom_tests.rs`](repo:crates/spate-core/src/checkpoint/loom_tests.rs) is one.
In every interleaving of a fail and three drops on three threads, the batch resolves exactly once, as `Failed`:

```rust
loom::model(|| {
    let (ack, recorder) = recorded_ack();
    let failing = ack.clone();
    let passing = ack.clone();

    let t1 = loom::thread::spawn(move || {
        failing.fail();
        drop(failing);
    });
    let t2 = loom::thread::spawn(move || drop(passing));
    drop(ack);
    t1.join().unwrap();
    t2.join().unwrap();

    let msgs = recorder.lock().unwrap();
    assert_eq!(msgs.len(), 1, "batch must resolve exactly once");
    assert_eq!(msgs[0].status, AckStatus::Failed, "worst status must win");
});
```

## Check an invariant over generated input

Use proptest for an invariant over generated inputs.
Check the code against a reference model that is too simple to be wrong.
The checkpoint tracker in [`spate-core/src/checkpoint/tracker.rs`](repo:crates/spate-core/src/checkpoint/tracker.rs) is held to one.
This is the test's shape, with the loop body left out:

```rust
proptest! {
    #[test]
    fn tracker_matches_reference_model(ops in ops()) {
        let mut tracker = PartitionTracker::new();
        let mut reference = Reference::default();
        for op in ops {
            // Apply `op` to both, and compare each answer the tracker gives.
        }
        prop_assert_eq!(tracker.advance(), reference.expected_advance());
        prop_assert_eq!(tracker.stalled(), reference.stalled);
    }
}
```

## Fuzz a parser

Give each parser of outside bytes a fuzz target.
The [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) crate in `fuzz/` has one libFuzzer target per boundary where the pipeline turns bytes into records or records into bytes.
Each target asserts a property, beyond the absence of a panic.

```sh
cargo xtask fuzz install
cargo xtask fuzz build
cargo xtask fuzz run avro_wire_confluent --secs 60
```

The libFuzzer instrumentation needs a nightly toolchain, so `cargo +nightly fuzz list` prints the targets.

A decoder that its crate keeps private is reached through the crate's off-by-default `testing` feature, in a `fuzz_seams` module.
The harness manifest, `fuzz/Cargo.toml`, enables the feature on each crate whose targets need it.

A pull request that touches `fuzz/`, `xtask/`, `ci.yml`, `fuzz.yml`, `.github/actions/`, the toolchain pins, or a crate the harness depends on builds every target and runs none.
The nightly workflow fuzzes each target for five minutes and keeps `fuzz/corpus` between nights.
A crash uploads its input as the `fuzz-artifacts` artifact and opens an issue titled `[bug] A fuzz target found a crashing input`.
Keep that input under `fuzz/corpus/<target>/` with the fix, so the case stays covered.

## Read a failing test's output

On CI, read the failure in the job's test report.
Each test job publishes a check named `Test results`, with a suffix for the arm64 and macOS jobs, and uploads its `junit.xml` as an artifact named `junit-nextest`, `junit-nextest-arm64` or `junit-nextest-macos`.

The `ci` profile prints a failing test's output as soon as it fails, and again in the summary at the end.
A wait that reaches `HANG` panics with `hung after 60s waiting for:` and the description passed to it, so write that description as the state you expected.

To see a component's logs in a failing test, run the code under `spate_test::show_logs`.
The harness prints those lines when the test fails.
The `capture_logs` function also returns the lines, for a test that asserts on them.

## Write the test's doc comment

A test's doc comment says what the test pins.
Add `Regression for #N.` when issue #N reports a defect in the code the test exercises, and the test fails without the fix.
A change that fixes the test itself, such as a flaky setup, adds no such line.

## Related

- [Tests that do not flake](05-flaky-tests.md) — the rules a test follows to pass on every runner.
- [Testing pipelines](../user-guide/03-guides/testing-pipelines.mdx) — the `spate-test` doubles from a user's side.
- [Commands and gates](01-commands.md) — the test runner, its profiles and the container suites.
