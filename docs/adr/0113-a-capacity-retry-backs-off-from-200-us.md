---
description: "A push a full shard queue refused is retried after 200 µs, doubling to blocked_retry. It wakes on a timer, and the full-events counter rises faster."
---

# ADR-0113 — A pipeline thread retries a full shard queue on a backoff from 200 µs

- **Status:** accepted
- **Date:** 2026-11-25
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

ADR-0111 made a shard-queue rejection retry the push while no shard worker
waits for a write permit. The pipeline thread slept `RuntimeOptions::blocked_retry`,
2 ms, before each retry in `drive_batch`, and the drain's `flush_until` slept a
fixed 2 ms. A shard worker drains its queue in passes far shorter than that, so
a contended thread spent most of each retry asleep with room in the queue. With
32 threads on 1 or 8 shards, the source fell behind its offered rate, and the
unthrottled ceiling stayed near 110M rows/s. The question was how a pipeline
thread waits for room in a full shard queue.

## Considered options

- A backoff that starts at 200 µs and doubles to `blocked_retry`
- The same backoff from 50 µs
- A shorter fixed sleep of 200 µs
- Waking a blocked thread when its shard worker frees room, in three shapes:
  tokio's `reserve`, a barging unpark of one waiter per freed slot, and a
  chained unpark that passes the wake from waiter to waiter
- Keep the fixed 2 ms sleep

## Decision outcome

Chosen option: "a backoff that starts at 200 µs and doubles to
`blocked_retry`", because it kept up with every paced arm and raised the
unthrottled ceiling at the least CPU per row of the options that did.

`drive_batch` and `flush_until` share one schedule, built from
`spate_core::backoff::Backoff` on the first capacity block of a call and
dropped when the call returns. A not-ready block still waits `blocked_retry`.
`flush_until` also clamps each wait to the time left before its deadline.

The barging unpark cost more CPU per row than the backoff on every arm, and
its ceiling was lower. tokio's `reserve` hands each freed slot to the oldest
waiter, so while one thread waits every new `try_send` fails and joins the
queue. Its rejections rose 20 times on one shard. The chained unpark fell
behind on one shard. The 50 µs start reached a ceiling 4% higher than 200 µs,
at a quarter more CPU per row on one shard. A flat 200 µs sleep refused far
more chunks than the backoff on a saturated store.

`spate_queue_full_events_total` still counts each refused `try_send`. Counting
once per refused chunk was weighed and dropped, because a stopped shard worker
would then leave the counter flat after each thread's first refusal. The
operator pages read a stopped worker from a rising counter.

### Consequences

- Good, because a source of 32 threads on 1 or 8 shards keeps up where it fell
  behind, and the unthrottled ceiling on 8 shards rises by about a third.
- Good, because each wait is still bounded by `blocked_retry`, and a stop is
  read on every retry as before.
- Bad, because a thread retrying a contended queue costs more CPU per row:
  about a fifth more on one shard, where the rate also rose.
- Bad, because `spate_queue_full_events_total` rises faster under the same
  contention, up to 3.8 times on one shard. An alert on its rate may need a
  higher threshold.
- Bad, because a blocked thread still wakes on a timer. A shard worker that
  frees room at once waits for the next step of the schedule.
- Neutral, because INV-2 holds: every send is still `try_send`, and only the
  sleep between retries changes.

### Confirmation

`crates/spate-core/src/pipeline/driver.rs` tests the schedule
(`retry_wait_backs_off_a_capacity_block_to_blocked_retry`) and that a capacity
block retries long before `blocked_retry`
(`a_capacity_block_retries_before_blocked_retry`). No repository bench reaches
the blocked retry. The counted tier leaves blocked paths out, and the
wall-clock `pipeline_wall` cases blocked at most once per run when counted.

## Evidence

Measured by a queue benchmark rig this repository does not carry: a paced or
unthrottled source of 32 pipeline threads, 512 KiB shard queues, and a sink that
models its writes with every twentieth write four times slower. Each run read a
20-second window after 5 seconds of warm-up.

On Linux, in a Docker VM on an 18-core Apple host, legs interleaved and the
first of four passes discarded. Unchanged code against the 200 µs backoff
against the barging unpark, in rows per second and process CPU ms per million
rows:

- 1 shard, offered 48.4M: 38.2M to 38.9M, behind in every pass, at 60 to 62;
  48.4M at 73 to 75; 48.4M at 85 to 89.
- 8 shards, offered 118.7M: 106.6M to 109.6M, behind, at 56 to 57; 118.6M at
  60 to 61; 118.6M at 67 to 68.
- 32 shards with 4 write permits, offered 89.7M: 89.5M to 89.6M for all three,
  at 83 to 85, 85 to 87 and 88 to 89.
- 8 shards, unthrottled: 108.4M to 111.8M at 56 to 57; 146.9M to 149.5M at 59
  to 61; 140.8M to 142.8M at 67 to 68.
- 1 shard against a store capped at 500 MB/s: 3.0M to 3.4M for all three, at
  106 to 110, 105 to 106 and 125 to 131.

An earlier Linux run, made before the change ADR-0112 records, compared 50 µs
with 200 µs. On 1 shard paced both kept up, at 94 to 95 against 74 to 76 CPU ms per million
rows. On 8 shards unthrottled, 50 µs reached 155.4M to 156.4M against 148.6M
to 150.4M. Neither arm paused, so ADR-0112 does not bear on them.

tokio's `reserve`, the chained unpark and the flat 200 µs sleep ran on macOS
only, on the same host, three interleaved passes each. On 1 shard paced,
`reserve` refused 7.2M chunks against 0.36M, and the chained unpark settled at
45.7M to 45.9M of 48.4M offered. On the saturated store, the flat sleep refused
416k to 434k chunks against the 50 µs backoff's 78k to 79k.

These runs do not show the cost on a bare-metal Linux host, or at more than
32 pipeline threads.

## More information

- Landed in the pull request that adds `retry_wait` to
  `crates/spate-core/src/pipeline/driver.rs`.
- [ADR-0111](0111-a-queue-rejection-pauses-only-under-sink-pressure.md) decides
  that such a rejection retries, and names the fixed 2 ms sleep this record
  replaces. ADR-0111 stays in force.
- [ADR-0112](0112-a-pause-under-sink-pressure-ends-when-a-permit-frees.md)
  decides when a pause under sink pressure ends.
- The behavior is described in
  [Backpressure](../user-guide/02-concepts/03-backpressure.mdx#detecting-the-pressure-bounded-queues-and-the-byte-budget).
