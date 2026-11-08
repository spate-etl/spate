---
description: "Rules that keep a test passing on every CI runner, how CI differs from your machine, and what to do when a test fails there."
---

# Tests that do not flake

A flaky test passes on one run and fails on another with no change to the code.
The rules below keep a test deterministic.
After them come how CI differs from your machine, and what to do when a test fails there.
[Writing a test](02-testing.md) covers where a test lives and which harness to use.

## Rules

### Wait for a signal the code publishes

Before a test acts or asserts, wait for a signal that only the expected state can produce.
The code under test publishes it as a watch, a notify, a channel, a metric or a log line.
Await the signal, or poll it with `spate_test::wait_until`.

**Why:** the test and the code run at the same time.
On another machine the code reaches the state at a different moment, so a test that did not wait for the signal acts too early.

A reading that can also be true while the code is still working is not such a signal.
In this sketch, the backpressure gauge reads 1 when the sink is down, and also when a healthy sink is full:

```rust
h.pause_clickhouse();
h.produce(&topic, second_wave);
wait_until(HANG, "backpressure", || paused_gauge(&pipeline) == 1.0);
pipeline.stop(); // the gauge also reads 1 under a healthy sink
```

A count that only the expected state can produce is such a signal.
In a sink outage, the in-flight batches rise above the shard's cap only when a batch waits behind a write the frozen sink never answers:

```rust
// Over the cap means a sealed batch is queued behind a write the frozen
// sink never answers. The pause gauge also reads 1 under a healthy sink.
let cap = params.inflight_max_per_shard as f64;
wait_until(HANG, "a stall caused by the outage", || {
    let (_, body) = http_get(pipeline.admin, "/metrics");
    metric_series(&body, "spate_sink_inflight_batches", &[])
        .iter()
        .any(|s| s.value > cap)
});
```

A total can also be reached early.
One partition can write 10,000 rows before another has started, so a test that needs rows on every partition waits for both the count and the partitions:

```rust
wait_until(HANG, "mid-stream rows on every partition", || {
    h.count(&table) >= 10_000 && h.partitions_written(&table) == partitions
});
```

When a test needs several consumers in one state, wait for each consumer's signal.
The Kafka mock's group timers run on real time, so the tests in [`mock_cluster.rs`](repo:crates/spate-kafka/tests/mock_cluster.rs) that start more than one consumer settle the group on the signal the source publishes:

```rust
wait_until(HANG, "every consumer in a stable group", || {
    driver.step(Duration::from_millis(100));
    let healthy = metric_series(&driver.statistics(), "spate_kafka_group_healthy", &[]);
    driver.partitions() == want
        && healthy.len() == consumers
        && healthy.iter().all(|s| s.value == 1.0)
});
```

When the code returns a retryable error, read again, as a real caller would.
Assert on the first answer that is not retryable.
A test that checks for the retryable error itself asserts on it directly.

After creating a topic, wait until the broker lists it.
A broker can report a topic it has just created as missing for a while.
The Kafka test support's `create_visible` creates the topics and waits for each one.

### Store the state before you send the signal

When you write a fake, store the state a test will read before you publish the signal that announces it.

**Why:** a test that sees the signal acts on the state at once.
If the fake sent the signal first, the test could find nothing to read.

The hand-back feed fake in [`spate-core/src/pipeline/fakes.rs`](repo:crates/spate-core/src/pipeline/fakes.rs) fills `held` before it pushes to `closes`, because a test takes a non-empty `closes` as its signal to use `held`:

```rust
// A test takes a non-empty `closes` as its signal to use `held`, so `held`
// fills first.
self.held.lock().unwrap().push(confirmer);
self.closes.lock().unwrap().push(within);
```

### Check that something did not happen by waiting past it

To check that something did not happen, wait for an event that shows the code has gone past the point where it would have happened.
Then check the result once.

**Why:** a fixed wait proves nothing on a slow machine.
The thing the test rules out may not have had time to happen.

The Kafka test [`pause_stops_delivery_and_resume_recovers_gapless`](repo:crates/spate-kafka/tests/mock_cluster.rs) counts the Fetch requests the mock cluster receives.
Each broker connection in librdkafka sends one Fetch at a time and handles its reply before it sends the next.
This means that once two more Fetch requests arrive after the test produces its records, at least one fetch has completed in full.
The test can then check that the paused lane received nothing:

```rust
// One Fetch is in flight per broker, and its reply is handled before the
// next is sent, so the second Fetch from here was sent after the produce
// was answered.
let fetched = cluster.received(RDKafkaApiKey::Fetch);
wait_until(HANG, "a Fetch sent after the produce is answered", || {
    serve_events(&mut source, &mut errors);
    cluster.received(RDKafkaApiKey::Fetch) >= fetched + 2
});
assert!(lanes[paused].poll(64, Duration::ZERO).expect("poll").is_none());
```

### Let no real elapsed time decide a result, except a broker timer

Pass `spate_test::HANG` as the bound of every wait, and never assert on how long something took on the real clock.
The one exception is a librdkafka or broker timer, at the end of this section.

**Why:** a bound sized for your machine fails on a slower one with nothing wrong.
`HANG` sits far above any latency a test should see, so reaching it means the test hung.

Pass `HANG` to `wait_until` and to the waits on the `spate-test` handles.
Never sleep a fixed interval and then assume the code got there.
This sketch passes on an idle machine and fails on a loaded one:

```rust
let deadline = Instant::now() + Duration::from_secs(10);
while !fetched() && Instant::now() < deadline {
    thread::sleep(Duration::from_millis(5));
}
assert!(fetched());
```

Test a timing property of the code on a clock the test moves.
Code on OS threads or a multi-thread runtime holds a `SharedClock`, and the test hands it a `TestClock`.
Code on a current-thread tokio runtime that reads `tokio::time` runs under `#[tokio::test(start_paused = true)]`.
In [`spate-kafka/src/source.rs`](repo:crates/spate-kafka/src/source.rs), the Kafka source's hold runs on a frozen clock that the test moves to one millisecond before the hold's end and then to the end:

```rust
let clock = freeze(&mut source);
push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
poll_quiet(&mut source);

clock.advance(ASSIGN_HOLD - Duration::from_millis(1));
poll_quiet(&mut source);
assert!(matches!(source.members[0].phase, Phase::Held(_)));

clock.advance(Duration::from_millis(1));
poll_quiet(&mut source);
assert!(matches!(source.members[0].phase, Phase::Assigned));
```

The `spate-core` crate cannot depend on `spate-test`, so its tests poll with their own `wait_for` in `pipeline/fakes.rs`.
Each caller passes the bound, and the rule applies to it as it does to `HANG`.

Real time is allowed in one case: the property is a librdkafka or broker timer running out, such as `session.timeout.ms` or a broker's `segment.ms`.
No test clock controls those timers, so the test has to let real time pass.
It may wait past the timer, or assert that at least that much time has passed, and a comment starting `Elapsed time is the property` names the timer.
A slow machine can only make elapsed time longer.
This means a minimum on elapsed time fails only when the code is wrong.
The Kafka feed test [`a_unit_whose_worker_is_killed_mid_read_is_delivered_again`](repo:crates/spate-kafka/tests/feed_kafka.rs) asserts that worker B read the unit only after worker A's session had lapsed:

```rust
// Elapsed time is the property: B is assigned the partition only once A's
// session lapses on the broker.
let waited = killed.elapsed();
assert!(waited >= SESSION - HEARTBEAT, "B read the unit inside A's session");
```

A maximum on elapsed time is still not allowed, and neither is a fixed wait that assumes nothing happened.
On a slow machine the first fails with nothing wrong, and the second can pass without checking anything.

### Set the code's own deadlines out of reach

When the code under test has a deadline of its own, set it far beyond `HANG` unless the deadline is what the test checks.

**Why:** the code's deadline can end the wait first.
Its error can then look like the one the test waits for, and the test passes having checked nothing.

The Kafka source's startup deadline reports the last consumer error, so the TLS handshake tests in [`spate-kafka/src/source.rs`](repo:crates/spate-kafka/src/source.rs) set the deadline to an hour:

```rust
// Out of real time's reach: the startup deadline's error carries the last
// consumer error, so only the handshake error may end it.
cfg.startup_timeout = Duration::from_hours(1);
```

### Move a test clock only after the code has read it

Before you advance a test clock, wait until the code has armed the timer you mean to move.

**Why:** the code reads the clock when it arms a timer.
An advance that lands before that read moves a deadline the code has not set yet.

On the tokio clock, `has_slept_until` says the timer is armed, so check it before you advance:

```rust
let t0 = clock.now();
let drain = tokio::spawn(pool.drain(deadline));
wait_until(HANG, "the worker arms its deadline", || {
    clock.has_slept_until(t0 + deadline)
});
clock.advance(deadline);
```

Where the clock has no such probe, wait for the pass the code makes after it reads the clock.
A controller test waits for one poll after a hint before it moves time:

```rust
let before = h.shared.lock().unwrap().hinted_at;
h.script.lock().unwrap().push_back(Script::CommitReady(vec![partition]));
wait_until(HANG, "a poll after the hint", || {
    let log = h.shared.lock().unwrap();
    log.hinted_at != before && log.hinted_at.is_some_and(|at| log.poll_timeouts.len() > at + 1)
});
clock.advance(window);
```

Under paused tokio time, `tokio::time::advance` moves every timer at once.
The SQS feed tests in [`spate-sqs/src/feed/tests.rs`](repo:crates/spate-sqs/src/feed/tests.rs) wait for the code's step, then advance:

```rust
feed.fail(first.receipt, "handed back").unwrap();
until("the fail is sent", || queue.visibility().len() == 1).await;
tokio::time::advance(HOLD - secs(70)).await;
```

### Own every name, port and metric label

Give every topic, table, group, bucket, port and metric label in a test a value that no other test uses.

**Why:** nextest runs many tests at once, and a binary's tests share one process under `cargo test`.
A fixed name or port can be held by another test, or by the host.

This sketch takes a fixed port and reads a metric that every pipeline in the process writes:

```rust
let listener = TcpListener::bind("127.0.0.1:39184").unwrap();
let series = metric_series(&body, "spate_sink_inflight_batches", &[]);
```

Use a port the kernel chose.
Where a refused connection is the point, bind a port and release it before use, as [`spate-kafka/src/testutil.rs`](repo:crates/spate-kafka/src/testutil.rs) does:

```rust
let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
let addr = listener.local_addr().expect("addr");
drop(listener);
```

Name each resource with `spate_test::unique_name`, from [`spate-test/src/fixtures.rs`](repo:crates/spate-test/src/fixtures.rs), which never returns the same name twice in one process:

```rust
let group = spate_test::unique_name("two-workers");
```

A gauge series has one live owner per process ([INV-10](../INVARIANTS.md)).
Give each fixture its own `pipeline` and `component` labels from `unique_name`, and filter an exposition on your own label before you read a value from it.

Rely on nothing the host provides, such as kernel behavior or an image pulled earlier.
Where a test needs a premise, assert it.
The bounded-job test in [`spate-kafka/tests/feed_kafka.rs`](repo:crates/spate-kafka/tests/feed_kafka.rs) needs work on both partitions, so it asserts that its units reached both:

```rust
assert!(
    (0..2).all(|p| support::log_end(&brokers, WORK, p) > 0),
    "the units span both partitions"
);
```

### Keep other work out of the window under test

Before the window a test checks, stop or hold every other actor that could act in it: a periodic timer, a background task, a child process.

**Why:** a timer or task that runs inside the window can do the work the test expects from the code under test.
The test then fails, or passes for the wrong reason.

The revocation test in [`spate-core/src/pipeline/tests.rs`](repo:crates/spate-core/src/pipeline/tests.rs) sets the commit interval to a minute, so that only the revocation drain can commit:

```rust
// No commit tick fires during the test, so only the revocation drain can
// commit.
config.checkpoint.interval = Duration::from_secs(60);
```

A child process can exit before the test writes to it.
The gungraun harness in [`xtask/tests/gungraun.rs`](repo:xtask/tests/gungraun.rs) accepts a broken pipe on that write, and checks what the child received through another channel:

```rust
if let Err(e) = child.stdin.take().unwrap().write_all(SENTINEL.as_bytes()) {
    assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe, "{e}");
}
```

### Assert only what the code promises

When the code allows more than one outcome, accept each one it allows.

**Why:** an outcome that depends on timing inside a client or a broker can change from run to run.
A test that asserts one of them fails whenever the other happens.

A write to a missing topic fails as `Fatal` or as `Retryable`, depending on when librdkafka's metadata answer arrives.
The sink test in [`spate-kafka/tests/sink_mock_cluster.rs`](repo:crates/spate-kafka/tests/sink_mock_cluster.rs) accepts both and checks the reason that goes with each:

```rust
let first = write().await;
let hint = match first.class() {
    ErrorClass::Fatal => "does not exist",
    ErrorClass::Retryable => "Unknown topic",
    other => panic!("first: {other:?}: {first}"),
};
assert!(first.reason().contains(hint), "first: {first}");
```

Work that runs concurrently, such as partitions or tasks, finishes in no fixed order unless the code promises one.
Wait for each piece of work you need.

## How CI differs from your machine

CI runs the workspace suite in up to three jobs, under the `ci` nextest profile:

| Job | Runner | Command |
|---|---|---|
| `Test and coverage` | `ubuntu-latest` | `cargo llvm-cov nextest --profile ci --workspace --all-features --locked` |
| `Test (arm64)` | `ubuntu-26.04-arm` | `cargo nextest run --profile ci --workspace --all-features --locked` |
| `Test (macOS)` | `macos-26` | `cargo nextest run --profile ci --workspace --all-features --locked` |

On a pull request, `Test (macOS)` runs only when the change touches a build input such as `Cargo.lock` or the nextest config.
In the merge queue it runs on every entry that builds.
This means a test can pass on your pull request and fail on macOS in the merge queue.

`Test and coverage` runs a build with coverage instrumentation, so its timing differs from the other two jobs.
Each test runs in a process of its own under nextest, which by default runs as many tests at once as the runner has hyperthreads.

Each of these changes when a thread gets to run.
A test that waits on signals passes in every job.
A test that depends on timing fails in whichever job times it differently.

The `ci` profile warns about a test after 60 seconds and terminates it after four such periods.
A wait that reaches `HANG` panics before that, with the message `hung after 60s waiting for:` and the wait's description.

The container suites run in their own jobs under the `docker` profile, which never terminates a test.
A cold image pull can take longer than any timeout worth setting.

To run a crate's tests the way the coverage job does:

```sh
cargo llvm-cov nextest --profile ci -p spate-kafka --all-features --locked
```

## When a test fails on CI

### Read it for a product defect first

Read a failing test before you call it flaky.
The first question is which side is wrong: the test assumed something the code never promised, or the code broke a promise.

**Why:** a wider bound, a retry or an `#[ignore]` answers neither question.
Each one can hide a real defect as well as a flake.

### Reproduce it

Run the test repeatedly with the stress runner to reproduce the failure:

```sh
cargo nextest run -p spate-kafka --all-features --locked \
  -E 'test(a_later_assignment_does_not_extend_the_hold)' --stress-count 200
```

Use nextest 0.9.148 or later for a stress run.
Earlier versions report only the last iteration's result when fail-fast is off, so a failed run can exit 0.

A stress run reproduces a failure.
A stress run that passes proves nothing about a fix.

### Fix it through a seam

Fix the test by giving it a signal or a clock it controls.
Add no retry, no `#[ignore]` and no wider bound.

**Why:** the `ci` profile does not retry a failed test, so each flake fails the pull request or merge-queue entry it lands in.
The one retry in CI is on the Ceph RGW lane of the S3 container suite.

### Report it

If you cannot fix it in your pull request, open an issue.
Name the state the test acted on and the signal that should have come first.
The fix goes to whichever side was wrong.

## Related

- [Writing a test](02-testing.md) — where a test lives, the harnesses, and what each tool proves.
- [Commands and gates](01-commands.md) — the test profiles and the container suites.
- [Spate invariants](../INVARIANTS.md) — INV-10, which the metric-label rule answers to.
