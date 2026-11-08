---
description: "How a test is written here: act on signalled state, own the environment, let no elapsed time decide a result, and pick the harness that fits the code."
---

# Writing a test

A test here pins one property of the code and passes on every run in which
that property holds. The four rules below keep it that way. Each one carries
the shape that fails, as a sketch, and a test in the tree that holds, rendered
from its source so it stays current. Then the harnesses, in the order to reach
for them, and what the process-wide state in a test binary demands.

## Where a test lives

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronization primitives, the Kafka mock cluster from
[`MockKafka`](repo:crates/spate-kafka/tests/support/mock.rs) and the ClickHouse
mocks for connector behavior that does not need a container.

Helpers that only this repository's tests need, such as reading a pinned image,
belong in the unpublished `spate-test-support` crate under `test-support/`.

## The four rules

### Act only on a state the code has signalled

The code under test publishes the state a test needs: a watch, a notify, a
channel, a metric or a log line. The test awaits that signal, or polls it with
`spate_test::wait_until`, and acts or asserts on nothing it has not seen. A
reading that can also hold while the code is still working is no signal.

The shape that fails, shortened to a sketch:

```rust
h.pause_clickhouse();
h.produce(&topic, second_wave);
wait_until(HANG, "backpressure", || paused_gauge(&pipeline) == 1.0);
pipeline.stop(); // the gauge also reads 1 under a healthy sink
```

The shape that holds, from the drain-outage scenario in
[`crates/spate/tests/e2e_drain_outage.rs`](repo:crates/spate/tests/e2e_drain_outage.rs):
it waits on a count only the outage can produce.

```rust file=crates/spate/tests/e2e_drain_outage.rs region=outage_signal title="e2e_drain_outage.rs"
```

The publishing side keeps the same order. The state is complete before the
signal that announces it, so a fake that exposes its probe before it stores
what the test will act on hands the test a window in which there is nothing to
read. The hand-back feed fake in the engine's own tests fills `held` before it
publishes `closes`:

```rust file=crates/spate-core/src/pipeline/fakes.rs region=held_before_closes title="fakes.rs"
```

A retryable answer is not yet the state a test asserts on. The test reads
again past it, as a caller does, and asserts on the first answer that is not
retryable. A broker can answer a topic it has just created as unknown, so a
test that creates a topic waits until the broker's metadata lists it.

### A test owns its environment

Its names, ports, clusters, topics, tables and metric labels are its own.
Nothing a test binds, names or reads is shared with another test in the binary
or with another binary on the machine, and nothing it relies on comes from the
host: no fixed port, no kernel behavior, no image pulled last week.

The shape that fails, shortened to a sketch:

```rust
let listener = TcpListener::bind("127.0.0.1:39184").unwrap(); // another test, or the host, may hold it
let series = metric_series(&body, "spate_sink_inflight_batches", &[]); // every pipeline in the process
```

The shape that holds. A port the kernel chose, released before use, where a
refused connection is the point:

```rust file=crates/spate-kafka/src/testutil.rs region=refused_broker title="testutil.rs"
```

A name no other call in the process returns:

```rust file=crates/spate-test/src/fixtures.rs region=unique_name title="fixtures.rs"
```

And a premise kept as an assertion, where the test needs work on every
partition and hashed ids would not promise it:

```rust file=crates/spate-kafka/tests/feed_kafka.rs region=fixed_ids title="feed_kafka.rs"
```

### Real elapsed time decides no result

A wall-clock bound catches a hang. It sits far above any latency a loaded
machine shows, and reaching it says the test hung: pass `spate_test::HANG` to
`wait_until` and to the waits on the `spate-test` handles. Nothing asserts on
how long something took on the real clock, and nothing sleeps a fixed interval
and then assumes the code got there.

A timing property of the code is tested on a clock the test moves. Code on OS
threads or a multi-thread runtime holds a `SharedClock`, and a test hands it a
`TestClock`. Code on a current-thread tokio runtime reading `tokio::time` runs
under `#[tokio::test(start_paused = true)]`. A fixed sleep belongs only in such
a test, on that clock.

The shape that fails, shortened to a sketch:

```rust
let deadline = Instant::now() + Duration::from_secs(10);
while !fetched() && Instant::now() < deadline {
    thread::sleep(Duration::from_millis(5));
}
assert!(fetched()); // an idle machine passes this and a loaded one fails it
```

The shape that holds, from the Kafka source's tests in
[`crates/spate-kafka/src/source.rs`](repo:crates/spate-kafka/src/source.rs):
the hold runs on a frozen clock the test hands the source, moved to one
millisecond before the hold's end and then to it, so the assertion reads the
code's own timer. The public seam for the same injection is
`KafkaSource::with_clock`, behind the crate's `testing` feature.

```rust file=crates/spate-kafka/src/source.rs region=hold_on_frozen_clock title="source.rs"
```

Moving a test clock has an order of its own. The code reads the clock when it
arms a timer, so an advance that lands before that read moves a deadline the
code has not set yet. On the tokio clock, `has_slept_until` says the timer is
armed:

```rust file=crates/spate-core/src/sink/pool_tests.rs region=armed_before_advance title="pool_tests.rs"
```

Where the clock has no such probe, the test waits for the pass the code makes
after reading it. The engine's controller tests wait for one poll after a hint
before they move time:

```rust file=crates/spate-core/src/pipeline/tests.rs region=poll_after_hint title="tests.rs"
```

Under paused tokio time, `tokio::time::advance` moves every timer at once, and
the test reads what the code did with the time:

```rust file=crates/spate-sqs/src/feed/tests.rs region=advance_paused_time title="tests.rs"
```

`spate-core` cannot depend on `spate-test`, so its tests poll with the
`wait_for` of their own in `pipeline/fakes.rs`, whose bound each caller
passes. The rule applies to that bound as it does to `HANG`; the regions above
pass five seconds there, and a hang bound is what the rule asks for in its
place.

### An interleaving failure is checked for a product defect first

A test that asserts an order of events and fails once is read before it is
called flaky. The first question is which side is wrong: the test assumed an
order the code never promised, or the code broke one it did. A wider bound, a
retry or an `#[ignore]` answers neither, and each hides a defect as readily
as a flake.

Reproduce it before deciding. The stress runner repeats one test:

```sh
cargo nextest run -p spate-kafka --all-features --locked \
  -E 'test(a_later_assignment_does_not_extend_the_hold)' --stress-count 200
```

Read its summary line for the count that failed; with fail-fast off, nextest
before 0.9.148 exits 0 on a failed stress run. A failure that reproduces gets an
issue naming the state the test acted on and the signal that should have
preceded it, and the fix lands on whichever side was wrong.

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
generation. A test whose result needs every member in one state waits for each
member's signal. The group timers run on real time, so a test with more than one
consumer settles the group on the signal the source publishes for it before it
asserts:

```rust file=crates/spate-kafka/tests/mock_cluster.rs region=settle_group title="mock_cluster.rs"
```

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
