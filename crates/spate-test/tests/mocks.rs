//! Contract tests for the spate-test mocks, driven the way a pipeline
//! runtime (or a framework user's test) drives them.

use spate_core::checkpoint::Checkpointer;
use spate_core::deser::Deserializer;
use spate_core::error::SinkError;
use spate_core::record::{Flow, PartitionId, RawPayload};
use spate_core::sink::{RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::{
    EmitCollector, ReplicaTag, ScriptedResult, TestDeserializer, TestEncoder, WriteOutcome,
    capture_writer, decode_rows, memory_source, raw_payload, record, test_ack,
};
use std::time::Duration;

const P0: PartitionId = PartitionId(0);
const P1: PartitionId = PartitionId(7);
const L0: LaneId = LaneId(0);
const L1: LaneId = LaneId(1);

fn opened_source() -> (
    spate_test::MemorySource,
    spate_test::SourceHandle,
    Checkpointer,
) {
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle())).unwrap();
    cp.begin_epoch(&[P0, P1], 1);
    (source, handle, cp)
}

fn assigned_lanes(
    source: &mut spate_test::MemorySource,
    handle: &spate_test::SourceHandle,
    lanes: &[(LaneId, PartitionId)],
) -> Vec<spate_test::MemoryLane> {
    handle.assign_lanes(lanes);
    match source.poll_events(Duration::from_millis(100)).unwrap() {
        SourceEvent::LanesAssigned(lanes) => lanes,
        other => panic!("expected assignment, got {other:?}"),
    }
}

#[test]
fn assignment_event_delivers_configured_lanes() {
    let (mut source, handle, _cp) = opened_source();
    assert!(handle.is_open());
    let lanes = assigned_lanes(&mut source, &handle, &[(L0, P0), (L1, P1)]);
    assert_eq!(lanes.len(), 2);
    assert_eq!(lanes[0].id(), L0);
    assert_eq!(lanes[0].partition(), P0);
    assert_eq!(lanes[1].id(), L1);
    assert_eq!(lanes[1].partition(), P1);
}

#[test]
fn poll_events_times_out_to_idle() {
    let (mut source, _handle, _cp) = opened_source();
    match source.poll_events(Duration::from_millis(10)).unwrap() {
        SourceEvent::Idle => {}
        other => panic!("expected idle, got {other:?}"),
    }
}

#[test]
fn poll_events_before_open_is_a_fatal_error() {
    let (mut source, _handle) = memory_source();
    assert!(source.poll_events(Duration::ZERO).is_err());
}

#[test]
fn push_poll_yields_monotonic_offsets_keys_and_timestamps() {
    let (mut source, handle, _cp) = opened_source();
    let mut lanes = assigned_lanes(&mut source, &handle, &[(L0, P0), (L1, P1)]);

    assert_eq!(handle.push(P0, Some(b"k0"), b"a"), 0);
    assert_eq!(handle.push_many(P0, [b"b", b"c"]), vec![1, 2]);
    assert_eq!(handle.push_at(P1, None, b"x", Some(999)), 0);

    let mut batch = lanes[0]
        .poll(512, Duration::from_millis(100))
        .unwrap()
        .unwrap();
    let first = batch.next_payload().unwrap();
    assert_eq!(first.bytes, b"a");
    assert_eq!(first.key, Some(&b"k0"[..]));
    assert_eq!(first.offset, 0);
    assert_eq!(first.timestamp_ms, 0, "default timestamp is the offset");
    assert_eq!(batch.next_payload().unwrap().offset, 1);
    assert_eq!(batch.next_payload().unwrap().offset, 2);
    assert!(batch.next_payload().is_none());
    drop(batch);

    let mut batch = lanes[1]
        .poll(512, Duration::from_millis(100))
        .unwrap()
        .unwrap();
    let only = batch.next_payload().unwrap();
    assert_eq!(only.partition, P1);
    assert_eq!(only.timestamp_ms, 999);
}

#[test]
fn max_records_bounds_each_batch() {
    let (mut source, handle, _cp) = opened_source();
    let mut lanes = assigned_lanes(&mut source, &handle, &[(L0, P0)]);
    handle.push_many(P0, [&b"1"[..], b"2", b"3", b"4", b"5"]);

    let mut seen = Vec::new();
    while let Some(mut batch) = lanes[0].poll(2, Duration::ZERO).unwrap() {
        let mut n = 0;
        while let Some(raw) = batch.next_payload() {
            seen.push(raw.offset);
            n += 1;
        }
        assert!(n <= 2, "batch exceeded max_records");
    }
    assert_eq!(seen, vec![0, 1, 2, 3, 4]);
}

#[test]
fn ack_round_trip_advances_checkpointer_watermark() {
    let (mut source, handle, mut cp) = opened_source();
    let mut lanes = assigned_lanes(&mut source, &handle, &[(L0, P0)]);
    handle.push_many(P0, [&b"1"[..], b"2", b"3", b"4"]);

    // Two batches; drop them out of order. The watermark stays contiguous.
    let batch1 = lanes[0].poll(2, Duration::ZERO).unwrap().unwrap();
    let ack1 = batch1.ack().clone();
    drop(batch1);
    let batch2 = lanes[0].poll(2, Duration::ZERO).unwrap().unwrap();
    let ack2 = batch2.ack().clone();
    drop(batch2);

    drop(ack2); // second batch resolves first
    cp.drain();
    assert_eq!(cp.take_watermarks(), vec![], "gap: batch 1 outstanding");

    drop(ack1);
    cp.drain();
    let marks = cp.take_watermarks();
    assert_eq!(marks, vec![(P0, 4)], "offsets 0..=3 all acknowledged");

    source.commit(&marks).unwrap();
    assert_eq!(handle.committed(), vec![(P0, 4)]);
    assert_eq!(handle.last_committed(P0), Some(4));
}

#[test]
fn failed_ack_stalls_the_watermark() {
    let (mut source, handle, mut cp) = opened_source();
    let mut lanes = assigned_lanes(&mut source, &handle, &[(L0, P0)]);
    handle.push(P0, None, b"poison");

    let batch = lanes[0].poll(16, Duration::ZERO).unwrap().unwrap();
    batch.ack().fail();
    drop(batch);
    cp.drain();
    assert_eq!(cp.take_watermarks(), vec![]);
    assert_eq!(cp.stalled_partitions().len(), 1);
}

#[test]
fn pause_is_honored_and_resume_wakes() {
    let (mut source, handle, _cp) = opened_source();
    let mut lanes = assigned_lanes(&mut source, &handle, &[(L0, P0)]);
    handle.push(P0, None, b"queued");

    source.pause(&[L0]).unwrap();
    assert_eq!(handle.paused_lanes(), vec![L0]);
    assert!(
        lanes[0]
            .poll(16, Duration::from_millis(10))
            .unwrap()
            .is_none(),
        "paused lane yields nothing"
    );

    source.resume(&[L0]).unwrap();
    assert!(handle.paused_lanes().is_empty());
    assert!(lanes[0].poll(16, Duration::ZERO).unwrap().is_some());
}

#[test]
fn revoke_probe_tracks_arrivals() {
    let (mut source, handle, _cp) = opened_source();
    let _lanes = assigned_lanes(&mut source, &handle, &[(L0, P0), (L1, P1)]);

    let probe = handle.revoke_lanes(&[L0, L1]);
    assert!(!probe.completed());
    assert_eq!(probe.remaining(), 2);

    let SourceEvent::LanesRevoked { lanes, barrier } =
        source.poll_events(Duration::from_millis(100)).unwrap()
    else {
        panic!("expected revocation");
    };
    assert_eq!(lanes, vec![L0, L1]);
    barrier.arrive(); // one arrival per revoked lane
    assert!(!probe.completed());
    barrier.arrive();
    assert!(probe.completed());
    assert!(probe.wait(Duration::ZERO));
}

#[test]
fn flush_commits_is_counted() {
    let (mut source, handle, _cp) = opened_source();
    assert_eq!(handle.flush_commits_calls(), 0);
    source.flush_commits().unwrap();
    source.flush_commits().unwrap();
    assert_eq!(handle.flush_commits_calls(), 2);
}

#[test]
#[should_panic(expected = "already active")]
fn assigning_a_duplicate_lane_panics() {
    let (mut source, handle, _cp) = opened_source();
    let _lanes = assigned_lanes(&mut source, &handle, &[(L0, P0)]);
    handle.assign_lanes(&[(L0, P1)]);
}

#[test]
#[should_panic(expected = "not active")]
fn revoking_an_unknown_lane_panics() {
    let (_source, handle, _cp) = opened_source();
    let _ = handle.revoke_lanes(&[LaneId(42)]);
}

#[test]
#[should_panic(expected = "not active")]
fn pausing_an_unknown_lane_panics() {
    let (mut source, _handle, _cp) = opened_source();
    source.pause(&[LaneId(42)]).unwrap();
}

// ---- sink mocks ----

fn sealed(token: &str, payload: &[u8]) -> SealedBatch {
    SealedBatch {
        frames: vec![bytes::Bytes::copy_from_slice(payload)],
        rows: 1,
        bytes: payload.len() as u64,
        dedup_token: token.to_owned(),
    }
}

#[tokio::test]
async fn unscripted_writes_succeed_and_are_captured() {
    let (writer, script) = capture_writer();
    let ep = ReplicaTag {
        shard: 3,
        replica: 1,
    };
    writer
        .write_batch(&ep, &sealed("t-1", b"abc"))
        .await
        .unwrap();

    let writes = script.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].shard, 3);
    assert_eq!(writes[0].replica, 1);
    assert_eq!(writes[0].rows, 1);
    assert_eq!(writes[0].bytes, 3);
    assert_eq!(writes[0].dedup_token, "t-1");
    assert_eq!(writes[0].payload, b"abc");
}

#[tokio::test]
async fn endpoint_scripts_take_precedence_over_global_fifo() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::retryable("global"));
    script.enqueue_for(0, 0, WriteOutcome::fatal("endpoint"));

    let ep00 = ReplicaTag {
        shard: 0,
        replica: 0,
    };
    let ep01 = ReplicaTag {
        shard: 0,
        replica: 1,
    };

    let err = writer
        .write_batch(&ep00, &sealed("a", b""))
        .await
        .unwrap_err();
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, spate_core::error::ErrorClass::Fatal);
            assert_eq!(reason, "endpoint");
        }
        other => panic!("unexpected error {other:?}"),
    }

    let err = writer
        .write_batch(&ep01, &sealed("b", b""))
        .await
        .unwrap_err();
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, spate_core::error::ErrorClass::Retryable);
            assert_eq!(reason, "global");
        }
        other => panic!("unexpected error {other:?}"),
    }

    // Scripts exhausted: back to the default success.
    writer.write_batch(&ep00, &sealed("c", b"")).await.unwrap();
    assert_eq!(script.writes().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn delays_run_on_tokio_time() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().after(Duration::from_secs(30)));
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };

    let started = tokio::time::Instant::now();
    writer.write_batch(&ep, &sealed("slow", b"")).await.unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::from_secs(30),
        "paused clock advanced by the delay"
    );
}

/// Poll `fut` once and report what it returned.
async fn poll_once<F: std::future::Future>(
    mut fut: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx))).await
}

/// Pins that a held write stays open across any amount of tokio time until
/// the script releases it, that a release reaches only the writes held when
/// it was called, and that each write reads `Pending` until it resolves.
#[tokio::test(start_paused = true)]
async fn a_held_write_resolves_only_when_released() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().held());
    script.enqueue_global(WriteOutcome::retryable("later").held());
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };

    let first_batch = sealed("held-1", b"");
    let mut first = std::pin::pin!(writer.write_batch(&ep, &first_batch));
    assert!(poll_once(first.as_mut()).await.is_pending());
    assert_eq!(script.held_writes(), 1);
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(
        poll_once(first.as_mut()).await.is_pending(),
        "time does not release a held write"
    );
    assert_eq!(script.writes()[0].result, ScriptedResult::Pending);

    script.release_held();
    first.await.unwrap();
    assert_eq!(script.held_writes(), 0);
    assert_eq!(script.writes()[0].result, ScriptedResult::Ok);

    let second_batch = sealed("held-2", b"");
    let mut second = std::pin::pin!(writer.write_batch(&ep, &second_batch));
    assert!(
        poll_once(second.as_mut()).await.is_pending(),
        "an earlier release does not reach a later hold"
    );
    assert_eq!(script.writes()[1].result, ScriptedResult::Pending);
    script.release_held();
    assert!(
        second.await.is_err(),
        "the scripted result follows the hold"
    );
    let writes = script.writes();
    assert_eq!(writes.len(), 2);
    assert_eq!(
        writes[1].result,
        ScriptedResult::Retryable("later".to_owned())
    );
}

/// Pins that a delayed write reads `Pending` for its whole delay and its
/// scripted result once it returns.
#[tokio::test(start_paused = true)]
async fn a_delayed_write_reads_as_pending_until_its_delay_ends() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().after(Duration::from_secs(30)));
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };
    let batch = sealed("slow", b"");
    let mut write = std::pin::pin!(writer.write_batch(&ep, &batch));
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert_eq!(script.writes()[0].result, ScriptedResult::Pending);
    tokio::time::advance(Duration::from_secs(30) - Duration::from_millis(1)).await;
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert_eq!(
        script.writes()[0].result,
        ScriptedResult::Pending,
        "a write still in its delay read as resolved"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    write.await.unwrap();
    assert_eq!(script.writes()[0].result, ScriptedResult::Ok);
}

/// Pins that writes resolving out of start order each record their own
/// result, in the order they started.
#[tokio::test(start_paused = true)]
async fn writes_resolving_out_of_order_keep_their_own_results() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().after(Duration::from_secs(30)));
    script.enqueue_global(WriteOutcome::retryable("r"));
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };
    let slow_batch = sealed("slow", b"");
    let fast_batch = sealed("fast", b"");
    let mut slow = std::pin::pin!(writer.write_batch(&ep, &slow_batch));
    assert!(poll_once(slow.as_mut()).await.is_pending());
    assert!(writer.write_batch(&ep, &fast_batch).await.is_err());
    let writes = script.writes();
    assert_eq!(writes[0].dedup_token, "slow");
    assert_eq!(writes[0].result, ScriptedResult::Pending);
    assert_eq!(writes[1].result, ScriptedResult::Retryable("r".to_owned()));
    tokio::time::advance(Duration::from_secs(30)).await;
    slow.await.unwrap();
    let writes = script.writes();
    assert_eq!(writes[0].result, ScriptedResult::Ok);
    assert_eq!(writes[1].result, ScriptedResult::Retryable("r".to_owned()));
}

fn pending_outcome() -> WriteOutcome {
    let mut outcome = WriteOutcome::ok();
    outcome.result = ScriptedResult::Pending;
    outcome
}

/// Pins that `enqueue_global` refuses an outcome scripted to resolve as
/// `Pending`, at the call that scripts it.
#[test]
#[should_panic(expected = "cannot be scripted to resolve as Pending")]
fn enqueue_global_refuses_a_pending_outcome() {
    capture_writer().1.enqueue_global(pending_outcome());
}

/// Pins that `enqueue_for` refuses an outcome scripted to resolve as
/// `Pending`, at the call that scripts it.
#[test]
#[should_panic(expected = "cannot be scripted to resolve as Pending")]
fn enqueue_for_refuses_a_pending_outcome() {
    capture_writer().1.enqueue_for(0, 0, pending_outcome());
}

#[tokio::test]
async fn probes_fail_and_heal() {
    let (writer, script) = capture_writer();
    let ep = ReplicaTag {
        shard: 1,
        replica: 0,
    };
    writer.probe(&ep).await.unwrap();
    script.fail_probe(1, 0, "replica down");
    assert!(writer.probe(&ep).await.is_err());
    script.heal_probe(1, 0);
    writer.probe(&ep).await.unwrap();
}

// ---- encoder / deserializer / collector ----

#[test]
fn encoder_round_trips_rows() {
    let mut buf = bytes::BytesMut::new();
    for payload in [&b"one"[..], b"", b"three"] {
        TestEncoder
            .encode(&record(payload.to_vec()), &mut buf)
            .unwrap();
    }
    assert_eq!(
        decode_rows(&buf),
        vec![b"one".to_vec(), Vec::new(), b"three".to_vec()]
    );
}

#[test]
#[should_panic(expected = "truncated")]
fn decode_rows_panics_on_truncation() {
    let mut buf = bytes::BytesMut::new();
    TestEncoder
        .encode(&record(b"whole".to_vec()), &mut buf)
        .unwrap();
    let _ = decode_rows(&buf[..buf.len() - 1]);
}

#[test]
fn deserializer_modes() {
    let ack = test_ack();
    let raw = RawPayload {
        offset: 5,
        timestamp_ms: 5,
        ..raw_payload(b"a,b,,c")
    };

    let mut out = EmitCollector::new();
    TestDeserializer::passthrough()
        .deserialize(&raw, &ack, &mut out)
        .unwrap();
    assert_eq!(out.payloads(), vec![b"a,b,,c".to_vec()]);
    assert_eq!(out.records[0].meta.offset, 5, "meta flows through");

    let mut out = EmitCollector::new();
    TestDeserializer::split_on(b',')
        .deserialize(&raw, &ack, &mut out)
        .unwrap();
    assert_eq!(
        out.payloads(),
        vec![b"a".to_vec(), b"b".to_vec(), Vec::new(), b"c".to_vec()],
        "empty segments are emitted"
    );

    let mut out = EmitCollector::new();
    let err = TestDeserializer::fail_on_prefix(&b"a,"[..])
        .deserialize(&raw, &ack, &mut out)
        .unwrap_err();
    assert!(err.to_string().contains("test marker"));
    assert!(out.records.is_empty());
}

#[test]
fn blocked_collector_stops_split_emission() {
    let ack = test_ack();
    let raw = raw_payload(b"1|2|3|4");
    let mut out = EmitCollector::blocking_after(2);
    TestDeserializer::split_on(b'|')
        .deserialize(&raw, &ack, &mut out)
        .unwrap();
    assert_eq!(out.payloads(), vec![b"1".to_vec(), b"2".to_vec()]);
    assert_eq!(out.rejected, 1, "emission stopped at the first Blocked");
}

#[test]
fn collector_reports_flow() {
    let mut out = EmitCollector::blocking_after(0);
    use spate_core::deser::EmitRecord;
    assert_eq!(out.emit(record(b"x".to_vec())), Flow::Blocked);
    assert!(out.records.is_empty());
}

/// Pins that a held write dropped before its release no longer counts as
/// held, and stays recorded as `Pending`.
#[tokio::test]
async fn a_dropped_held_write_is_no_longer_held() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().held());
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };
    let batch = sealed("held", b"");
    let mut write = Box::pin(writer.write_batch(&ep, &batch));
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert_eq!(script.held_writes(), 1);
    drop(write);
    assert_eq!(script.held_writes(), 0);
    let writes = script.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].result, ScriptedResult::Pending);
}

/// Pins that a delayed write dropped before its delay ends stays recorded
/// as `Pending`.
#[tokio::test(start_paused = true)]
async fn a_dropped_delayed_write_stays_pending() {
    let (writer, script) = capture_writer();
    script.enqueue_global(WriteOutcome::ok().after(Duration::from_secs(30)));
    let ep = ReplicaTag {
        shard: 0,
        replica: 0,
    };
    let batch = sealed("slow", b"");
    let mut write = Box::pin(writer.write_batch(&ep, &batch));
    assert!(poll_once(write.as_mut()).await.is_pending());
    drop(write);
    tokio::time::advance(Duration::from_secs(30)).await;
    let writes = script.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].result, ScriptedResult::Pending);
}
