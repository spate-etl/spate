# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and versions follow
[semantic versioning](https://semver.org/) — with the pre-1.0 caveat that a
breaking change ships in a **minor** bump, not a major one.

All crate versions in lockstep, so one entry covers the whole release.
There is a single supported version at a time: the newest `0.x` minor.

Entries are written by hand, one per change, as fragments under
[`changelog.d/`](changelog.d) and assembled here at release time. Commit
subjects say what changed; a release note says what it means for somebody
upgrading, and the second is what belongs here.

## [Unreleased]

## [0.4.0] — 2026-11-01

### Added

- **A bounded feed job ends on its own** (`spate-core`, `spate-s3`, `spate-sqs`, `spate-kafka`)

  A job whose `feed:` section names a `location` ends once its lister run's end marker names the same run at the start and the end of a settling window over which every read of the feed, at most 20 seconds apart, finds no unit ready, delayed or held, and the worker holds none. The worker then reads the feed's dead-letter destination: empty ends the run completed, and any unit there fails it with an error that includes `bounded feed job stalled:` and names the destination and its count, so the process exits non-zero. The `feed:` section's `settling_window` sets the window, five minutes by default, from 20 seconds to 24 hours; it also bounds the wait for the job fingerprint. A solo S3 run on the in-process feed the runtime built ends once its listing is read and the feed is empty, with no window. A feed that cannot report its depth fails a bounded job at startup. The SQS feed reports the approximate message counts of its queue and its dead-letter queue, and refuses a `settling_window` below one minute, since SQS's counts take at least a minute to settle. The Kafka feed reports its group's lag on the work and retry topics, read through a consumer that never joins the group, and the dead-letter topic's end offsets.
  ([#1330])
- **A stopwatch and a wall-clock reader in `spate_core::clock`** (`spate-core`)

  `spate_core::clock::Stopwatch` measures how far a `Clock` moves between two reads, and exposes no `Instant`, so a measurement cannot feed a deadline. `spate_core::clock::wall_now` returns the system's wall-clock time, for timestamps and identifiers only. A duration measured with `Stopwatch` on an injected `Clock` follows that clock, so a test that moves the clock measures the moved time.
  ([#1508])
- **A stopwatch on the tokio clock** (`spate-core`)

  `spate_core::clock::tokio::Stopwatch` measures how far a tokio-timeline `Clock` moves between two reads, and exposes no `Instant`, so a measurement cannot feed a deadline. It mirrors `spate_core::clock::Stopwatch` for code on the tokio runtime.
  ([#1509])
- **Seeded hash map aliases in `spate_core::hash`** (`spate-core`)

  `FastMap`, `FastSet`, `QualityMap` and `QualitySet` are std maps and sets over hashers seeded per instance, for operators and connectors that keep a map on the record path. `Fast*` is for keys the pipeline produces, and `Quality*` for keys an outside party chooses.
  ([#1565])
- **A shared clock holder in `spate_core::clock`** (`spate-core`)

  `spate_core::clock::SharedClock` is an `Arc` of a `Clock` that is also `Send`, `Sync` and `RefUnwindSafe`, so a public type that holds one keeps those traits. `spate_core::clock::SyncClock` wraps a `SharedClock` and implements `Debug`, which `Clock` does not require, so a type that holds one can derive `Debug`. `SyncClock::system()` holds `SystemClock`, and `SyncClock::new` holds any shared clock, such as a test clock.
  ([#1519])
- **Task and thread handles in `spate_core::task`** (`spate-core`)

  `spate_core::task::spawn` and `spawn_on` start a tokio task and return a `TaskHandle`. Dropping a `TaskHandle` aborts its task, and awaiting it returns the task's output or a `JoinError`. `TaskHandle::detach` lets a task run on with no owner. `spate_core::task::spawn_thread` starts a named OS thread and returns a `ThreadHandle`, which joins the thread when dropped. Each spawn takes a name, and a panic in the task or thread is logged at `ERROR` with that name. `spate_core::task::panic_text` returns the text of a panic payload.
  ([#1556])
- **Breaking:** **A feed backend receives a `Meter` of its own** (`spate-core`)

  `Feed::attach_metrics` hands a feed backend a `FeedMetricsCtx`. Its `meter` is a `Meter` for the backend's own metric families, under `spate_<backend>_feed_*` with `component="feed"` and the backend's key as `component_type`. The runtime calls it before the source is handed the feed. Calls made before then, and a feed the runtime is not given, get no `Meter`. The context's `per_partition_detail` is the pipeline's `metrics.per_partition_detail`, which gates the backend's own per-partition families.

  `feed` joins `ROLE_SEGMENTS`, so a `Meter` local name that starts with `feed_` panics, as one that starts with `source_` or `sink_` does. The rule holds in every namespace, including the `spate_custom_*` names `ChainCtx::meter` registers. v0.3.0 accepted such names, so a connector or pipeline author who registered one must rename it.
  ([#1516])
- **Breaking:** **The runtime publishes the feed's dead-letter depth** (`spate-core`)

  `spate_feed_dead_letter_units` reports how many units the feed's dead-letter destination holds. Each instance reads it at startup and then once a minute, whether or not the job ends on its own, so a job that runs until a shutdown has a dead-letter alert. On a backend whose depth read is a request to its service, such as SQS, that is one request per instance each minute. An alert on `spate_coordination_splits_quarantined` from v0.3.0 can move to this gauge, aggregated across instances with `max`. On the Kafka feed the gauge never falls, so alert on a rise there.

  `RuntimeOptions::dead_letter_read_interval` sets how often the runtime reads the depth, and `None` turns the reads off. Code that builds `RuntimeOptions` with a struct literal must set the new field or end with `..RuntimeOptions::default()`. A feed backend's `read_depth` may be called from a thread other than the source's. A test that scripts or counts dead-letter depth reads on a `spate-test` `FeedScript` also sees the runtime's reads, so it sets `dead_letter_read_interval` to `None`.
  ([#1515])
- **A stopping worker reads its feed units to a reserve before the drain deadline** (`spate-core`, `spate-kafka`, `spate-s3`, `spate-sqs`, `spate-test`)

  On a graceful stop, a source whose feed hands back receives no more units and keeps reading the ones it holds until each is read to its end or until 5 s before `checkpoint.drain_timeout` runs out. It then hands back the units it has not read to its end, for another instance to read, at once on a feed that leases units. The drain deadline counts from the stop. The drivers and the sink drain end one second before it, and after the final commit the runtime closes the feed within what is left. Startup refuses a `checkpoint.drain_timeout` of 5 s or less with such a feed. Sources and feeds that do not opt in stop as before. A feed backend opts in through `Feed::hands_back`, `Feed::stop_receiving`, `Feed::hand_back` and `Feed::close`, and a source through `Source::reading_on_stop` and `Source::stop_reading`; `spate_core::feed::STOP_RESERVE` is the 5 s reserve, and `FeedScript::hand_back_on_stop` lets a test play such a feed. On such a feed the S3 source keeps each unit whose every object and byte range it has read to its end, for the final commit to acknowledge, fails back each unit with an object failure as before, and hands back the rest, whose records read before the stop are read again. The SQS feed hands back: it sets each unit the worker did not finish, and each unit it received and had not handed out, visible at once, keeps every unit it holds extended until it is acknowledged or handed back, and sends its queued deletes when the runtime closes it. The Kafka feed hands back: when the stop begins it pauses its partitions and records no attempt for a unit still at its gate, a handed-back unit stays held, and when the runtime closes it, it commits each partition up to its first unfinished unit and then leaves the group, so the next owner reads what it did not finish.
  ([#1350])
- **Breaking:** **The runtime records work feed calls under `spate_feed_*`** (`spate-core`)

  When a pipeline's source reads a work feed named by the `feed:` section, the runtime records the calls the source makes on it, whatever the backend. The series sit under `spate_feed_*`, with `component="feed"` and the backend's key as `component_type`, and `docs/METRICS.md` describes each family.

  `feed` is a reserved metric namespace. `Meter::with_namespace("feed", …)` panics, as it does for the other framework roots, and a source or sink whose `component_type` is `feed` receives no `Meter`. A connector or pipeline that registered its own `spate_feed_*` families must move them to another namespace.
  ([#1514])
- **A feed backend can label its families with the feed's operations** (`spate-core`)

  `spate_core::metrics::FeedOp` names the calls a source makes on its feed, and `FeedOp::label` gives the `op` label value the `spate_feed_*` families use. A feed backend that counts something per call labels its own family with it, so the family joins the framework's on `op`. `FeedOp::ALL` lists every call in declaration order.
  ([#1594])
- **A `feed:` section selects the work feed** (`spate-core`, `spate-s3`, `spate-test`)

  A pipeline file can name the feed its source reads with a top-level `feed:` section: one backend key, plus `units_held`, `attempt_cap`, `location`, a job location for a source that reads one, and `settling_window`, the settling window of a job that names one; the in-process feed refuses the last two, and `settling_window` is refused below 20 seconds and above 24 hours. The section is `spate_core::config::FeedSection`, and the runtime hands it to the source through `Source::configure_feed`, whose default refuses the section. The runtime builds the in-process feed itself for `feed: { memory: {} }`, through `MemoryFeed::from_section`. Any other backend is built in code and passed to `Pipeline::feed`, and the runtime refuses a feed whose `Feed::backend` key differs from the section's. `MemoryFeed::with_attempt_cap` sets the delivery on which the in-process feed dead-letters a failed unit. The S3 source reads work units from the feed the section names, with `units_held` bounding its read parallelism (by default twice the pipeline thread count). A unit the lister publishes names one object, one byte range of one object, or up to 16 small objects read whole in turn, and a notification message is one unit with one part per non-empty created object; a whole-object unit whose descriptor carries an ETag, over an object that qualifies for byte ranges, is read in ranges, each range in progress taking one of `units_held`. On a durable feed it lists nothing: it reads the units and storage notifications published to the feed, and publishes only the notification unit it re-enqueues after an overwrite, capping those re-enqueues at `attempt_cap`. On the in-process feed it lists its own prefix into the feed, and code can publish more units to a `MemoryFeed` it keeps a handle to and passes to `Pipeline::feed`, in which case the run ends only at a shutdown. Units from another process need a durable backend. A pipeline with no `feed:` section fails to start with the S3 source.
  ([#1172])
- **A feed trait for queue-fed sources** (`spate-core`, `spate-test`)

  `spate_core::feed::Feed` is the contract a source uses to read work units from a queue. A source receives units, renews the ones it holds, and acknowledges or fails each one. A publisher adds units and reads a `Confirmation` that reports when the feed has stored each one. `spate_core::feed::memory::MemoryFeed` is an in-process feed for one instance. It keeps nothing across a restart and moves a unit that fails on its tenth delivery to an in-memory dead-letter list. A backend that can count what it holds answers `Feed::read_depth`, and `spate_core::feed::EndCheck` decides from those counts when a bounded job has read all of its work. `Source::configure_feed` learns from a `FeedOrigin` whether the runtime built the feed or code passed it in. `Source::renew_held` lets a source renew the units it holds while a revocation waits for its drain. `spate_test::scripted_feed` lets a test script redelivery, duplicate delivery, dead letters, publish outcomes, depth reads, and refusals of the next `receive`, `renew`, `acknowledge` or `fail` with a given `FeedErrorKind`.
  ([#1123])
- **Held writes in the capture sink** (`spate-test`)

  `WriteOutcome::held()` scripts a capture-sink write that stays open until the test calls `SinkScript::release_held()`. No amount of real or tokio time releases it, so a test can check what a pipeline does while a write is outstanding without timing the write. `SinkScript::held_writes()` reports how many writes are waiting. Release every held write before you shut the run down, because a shutdown waits for them.
  ([#1124])
- **A gauge for how busy the I/O runtime is** (`spate-core`)

  Each pipeline exports `spate_pipeline_io_busy_ratio`, the fraction of time the I/O runtime's async workers spent running tasks, from 0 to 1, averaged over the trailing 5 minutes (since start while shorter) and updated every 5 seconds. Previously no metric showed when those workers were the limit, so a saturated I/O runtime looked like a slow sink. When a full 5-minute window averages 0.9 or above, the pipeline logs one warning that names `pipeline.io_threads`, and logs it again only after the average has read below 0.9 for a full 5 minutes. A worker records its busy time about every 31 polls, so once the window is full a reading can be off by a few percent for polls under about 500 ms and by about 10% at 1 s polls. Longer polls read late and unevenly. The gauge does not count the blocking thread pool.
  ([#1148])
- **A bounded feed job checks its job fingerprint at a job location** (`spate-s3`)

  When the `feed:` section names a `location`, the S3 source treats the job as bounded. Before it receives a unit, each worker reads the job fingerprint at that object-store prefix and compares it with its own. The fingerprint covers the unit format version, the source `url`, `compression`, `split_target_bytes` and the framer's delimiter. A worker whose fingerprint differs fails startup with both in the error, and one that finds none within the `feed:` section's `settling_window`, five minutes by default, exits non-zero. A location is written as `s3://<bucket>/<prefix>`, `s3a://<bucket>/<prefix>` or `file://`, and the source refuses any other form, one that carries userinfo, a query or a fragment, and one at or under its own prefix.
  ([#1302])
- **A Kafka feed backend** (`spate-kafka`, `spate`)

  `spate_kafka::feed::KafkaFeed` reads work units from Kafka topics. `KafkaFeed::connect` builds it from a `feed: { kafka: … }` section, with `brokers`, `group_id`, the work `topic`, `retry_topic`, `dead_letter_topic`, `attempt_topic`, `delivery_timeout` (default 30 s), `statistics_interval` (default 5 s) and an `rdkafka:` passthrough, and code passes it to `Pipeline::feed`. Every worker reads the work and retry topics as one consumer group with eager assignment, and a publish writes a unit to the work topic with the unit's id as the key. Before a unit is read, the feed writes its attempt count to the compacted attempt topic as `{"v":1,"attempts":n}`, keyed by the publish identity `<topic>/<partition>/<offset>`, and the unit is read with that count once the write is confirmed. Every read counts, including one whose worker dies, though two copies of one publish read at the same time, as after a rebalance, can count once between them. A unit whose recorded attempts have reached the section's `attempt_cap` (default 10) goes to the dead-letter topic without being read. An acknowledged unit's attempt record is tombstoned, and its offset is committed once every record in it is acknowledged and the tombstone is confirmed. A failed unit is written to the retry topic, or to the dead-letter topic at the cap, with `spate.origin` and `spate.deliveries` headers, and its offset is committed only once that write is confirmed, and for the dead-letter topic once the tombstone after it is. A message with no key is a unit named by its position, `<topic>/<partition>/<offset>`, and a message whose key is not UTF-8 goes to the dead-letter topic at once. The feed never creates a topic. `KafkaFeed::connect` reads the four topics' metadata and configuration first, within 30 seconds, and refuses a missing topic, a work, retry or dead-letter topic whose `cleanup.policy` includes `compact`, an attempt topic whose `cleanup.policy` is anything but `compact` or that reports no partitions, and a work topic whose `message.timestamp.type` is not `LogAppendTime`, so the work topic's retention counts from when the broker appends each message, also for a store that stamps its notifications with timestamp 0. For a topic that goes missing after the feed connects, a write librdkafka still holds when it marks the topic missing stops the pipeline. After that, a publish to the missing work topic is reported to its caller as retryable, and an attempt, tombstone, retry or dead-letter write is sent again until the topic exists, with its partition committing nothing past it meanwhile. On a graceful stop the runtime closes the feed within what is left of `checkpoint.drain_timeout`: it waits up to half of that for its writes in flight, commits each partition up to its first unfinished unit and leaves the group; a feed dropped without a close waits up to 1 s. Startup refuses a `checkpoint.drain_timeout` of 5 s or less with the feed. `KafkaFeedPublisher::connect` builds a publish-only handle from the same section, for a process that only publishes, such as a lister run: it runs the same startup checks, then writes units to the work topic through one producer with the feed's settings, joins no group, reads no unit and writes no attempt record. Its `receive` fails as Fatal, and dropping it reports each publish it has not confirmed as retryable. Its identity needs `Describe` and `DescribeConfigs` on the four topics and `Write` on the work topic. A credential a broker rejects, or a TLS handshake either side rejects, after the feed or the handle connected, as its producer reports it, stops it with the reason, and so does a fatal producer error such as a refused idempotent producer id. `rdkafka:` refuses the properties the feed owns, such as `group.id`, `acks` and `partition.assignment.strategy`, `group.instance.id`, since a static member sends no LeaveGroup when it closes, and `enable.gapless.guarantee`, since a gap would stop the feed's producer. Each of the feed's six clients sends a `client.id` of `spate-feed-<role>`, such as `spate-feed-consumer`, unless `rdkafka:` sets one, which then applies to all six. A bounded job reads the feed's depth: the group's lag on the work and retry topics, read through a consumer that never joins the group, and the dead-letter topic's end offsets, which count a record retention has since deleted. The feed counts its confirmed retry and dead-letter writes, the latter by `reason`, and times its attempt writes under `spate_kafka_feed_*`, where its consumer's and producer's librdkafka statistics also register, labelled by `client`; the consumer's per-partition series carry `topic` and follow `metrics.per_partition_detail`.
  ([#1211])
- **A lister run publishes a bounded job's units** (`spate-s3`)

  `S3Source::list_once` lists the source's prefix once and publishes the listed objects to a durable feed that `main()` builds from the `feed:` section, small ones packed into shared units toward `split_target_bytes` and large qualifying ones cut into byte ranges. It reads the workers' own pipeline file and builds no sink. It keeps its checkpoint at the job location and resumes after it, so a run that stops publishes the units after the last checkpoint again. Once the feed has confirmed every unit and the last checkpoint is written, it writes `end` at the location, holding an id fresh to the run; a run that fails writes none, except when the store kept the marker and its reply was lost. A unit the feed rejects as retryable is published again, up to five attempts, and one still rejected or refused fails the run. On the in-process feed a run lists its own prefix, keeps no checkpoint, lists and reads the prefix from the beginning after a restart, and says so in a startup WARN. Two lister runs at two locations publish every unit twice, and nothing detects it.
  ([#1326])
- **The S3 source reads a work unit that names several objects** (`spate-s3`)

  On the feed path, a `SplitDescriptor` may name two or more objects. The S3 source reads them whole, one after another in descriptor order, on one lane, and acknowledges the unit once after the last is read. If one of them fails, the whole unit is failed back to the feed and is dead-lettered at its cap with every object it names. A descriptor that names no object, or more than `spate_s3::MAX_UNIT_MEMBERS` (4,096), is failed back. `spate_s3_source_objects_remaining` counts each object of such a unit.
  ([#1484])
- **The S3 source reads storage notifications from its feed** (`spate-s3`)

  On the feed path, the S3 source reads storage notifications as well as the units a lister publishes. It reads an S3 `Records[]` message, the same records inside the envelope RustFS sends, and the S3 test event, which it acknowledges with nothing to read. Each `ObjectCreated:*` record becomes one part of the message's unit. Its key is decoded as `application/x-www-form-urlencoded`, or read as written when its record's `eventSource` is `ceph:s3`, and its ETag is sent quoted in `If-Match`. Records whose `eventName` creates nothing are dropped, and so are created objects of size 0 in the source's bucket, under its prefix and with a key that decodes or is read as written. The message is failed back to the feed, which dead-letters it at its cap, when any record has no `eventName` or one that is not a string, when any created record lacks `s3.bucket.name` or `s3.object.key`, carries a field of the wrong type, names another bucket, a key outside the source's prefix, no size, or a decoded key that does not decode, or when a created object of one byte or more has a key that `object_store` cannot read under its own name, an ETag that `If-Match` cannot carry, or a `versionId` longer than 1,024 bytes or holding a space or a control byte. A record whose `versionId` is a string other than `""` and `"null"` reads that version. The message is acknowledged once, after every part has been read. An object announced by both a listing and a notification is read twice. A `404` on a notification unit's read fails the unit back, and a `403` fails the pipeline. A notification unit whose object is overwritten under its ETag pin, on a read that names no version, delivers the records read before the overwrite and is re-enqueued through the feed at the key's current ETag, with a re-enqueue count one higher, and is acknowledged once the feed has stored the new unit and the sink has acknowledged every record read. At the `feed:` section's `attempt_cap`, 10 by default, the unit is acknowledged without a re-enqueue. `spate_s3_source_objects_overwritten_total` counts each overwritten read, and `spate_s3_source_overwrites_dropped_total` each one acknowledged at the cap.
  ([#1284])
- **A source learns the pipeline thread count at `open`** (`spate-core`)

  `SourceCtx::threads` carries the number of pipeline threads the runtime resolved, the value `spate_pipeline_threads` reports, whether `pipeline.threads` sets it or the host derives it. A custom source can size its read parallelism from it. It is `None` when a source is opened by hand, as in a test, and `SourceCtx::with_threads` sets it there.
  ([#1476])
- **A source lane can report that it has finished** (`spate-core`)

  `SourceLane::finished` lets a lane say it has reached the end of its input. The pipeline thread then stops polling it, and a thread whose lanes have all finished waits on its control channel until a lane is stopped, dropped or handed new work. Lanes that do not implement it are polled as before.
  ([#1479])
- **`spate-sqs`, an Amazon SQS feed backend** (`spate-sqs`, `spate`)

  A new crate, `spate-sqs`, behind the `spate` feature `sqs`, reads work units from an SQS standard queue. `SqsFeed::connect` builds the feed from a `feed: { sqs: … }` section, with `queue_url`, `visibility` (default 60 s, renewed every third of it), `region` and `endpoint`, and code passes it to `Pipeline::feed`. A received unit stays hidden while its reader renews it, and the feed clamps every extension to the 12 hours SQS holds a message from the receive that issued its receipt handle. A unit is deleted once every record in it is acknowledged, and a failed unit is made visible at once, so the queue's redrive policy moves a unit that keeps failing to its dead-letter queue. A unit's body travels as the message text and its id in the `spate.unit` message attribute. `attempt_cap` is refused on `sqs`, where the queue's `maxReceiveCount` counts deliveries. `SqsFeed::connect` refuses a FIFO queue, a queue with no redrive policy, a dead-letter queue it cannot read or that keeps messages no longer than the queue, and a queue of the two that does not report its message counts. The feed reports the queue's and the dead-letter queue's approximate message counts to a bounded job's end check, and refuses a `settling_window` below one minute. The identity needs `sqs:GetQueueAttributes` on the dead-letter queue too. The feed counts the extensions SQS refuses by error code, the fails it drops past their deadline, and the calls it sends again by operation, under `spate_sqs_feed_extensions_refused_total`, `spate_sqs_feed_fails_dropped_total` and `spate_sqs_feed_retries_total`.
  ([#1173])

### Changed

- **`AdminServer::run` closes every open connection when it returns** (`spate-core`)

  When `AdminServer::run` returns, it closes every connection it accepted, including a kept-alive connection and one whose request is still in progress. In previous versions each connection ran on a detached task that kept serving after `run` returned, until the client closed it or the tokio runtime shut down. A client that holds an admin connection open sees it close when the pipeline stops serving the admin endpoints. A connection whose request panics is logged at `ERROR` with the message `admin connection panicked`.
  ([#1556])
- **Avro decoding hashes schema ids and names with a seeded quality hasher** (`spate-avro`)

  The schema cache, the deserializer's held readers, the registry fetcher and the named-type index hash their keys with the quality variant of a seeded hasher. In previous versions they used std's SipHash. On the decode benchmarks a schema with recursive named references costs about 20–24% fewer instructions, and a payload whose schema id is cached as failed, such as an id the registry does not know, costs about 20% fewer. Resolving against a reader schema costs up to 2% fewer.
  ([#1569])
- **Breaking:** **A registry password without a username fails at load** (`spate-avro`)

  The Avro deserializer rejects a `registry` section that sets `password` without `username` when the configuration loads, and the error names both fields. In previous versions the section loaded and the password was never sent. A registry that requires authentication then rejected the requests, and the pipeline stopped the first time it needed a schema from the registry. A registry that needs no authentication accepted the requests, so such a pipeline ran and now fails to start. Set `registry.username`, or remove `registry.password` if the registry needs no authentication. A username without a password is still accepted.
  ([#1153])
- **Schema registry task panics are logged** (`spate-avro`)

  When the Avro deserializer's schema registry fetcher or its startup pre-warm panics, the panic is logged at `ERROR` with the message `background task panicked` and the task name `avro-registry-fetcher` or `avro-registry-prewarm`. In previous versions such a panic was only printed to standard error by the default panic hook, so it did not appear in the pipeline's `tracing` output. A panic while fetching one schema is logged as `registry fetch task panicked` with the panic's text. The tasks run and end as before.
  ([#1557])
- **Breaking:** **A captured write reads `Pending` until it resolves** (`spate-test`)

  `SinkScript::writes()` lists each write when it starts, with `CapturedWrite.result` set to the new `ScriptedResult::Pending` until the write returns. A write still in its `.after()` delay, a held write, and one whose future was dropped before it resolved read `Pending`. In previous versions a write scripted with `.after()` read its scripted result from the moment it started. A test that counts `result == ScriptedResult::Ok` as rows written no longer counts a write that has not finished. A test that reads a delayed write's scripted result from `writes()` before the write returns reads `Pending` instead. Read the result after the write returns.
  ([#1422])
- **Breaking:** **The ClickHouse sink compresses insert bodies on pipeline threads** (`spate-clickhouse`)

  With `compression: lz4` or `zstd`, the sink compresses each chunk on the pipeline thread when the chunk seals. It reuses one zstd context per thread. Previously the I/O runtime compressed every frame inside the send, and a retry compressed it again. A retry now resends the stored bytes. `compression: off` sends the same request as before. `settings: { decompress: … }` is now rejected, because the sink sets it. Under `lz4` or `zstd`, server error 271 means the server could not decode a block, and it now stops the sink instead of being retried. A stored block that passes its checksum and fails to decode, read by a materialized view during the insert, also answers 271 and stops the sink under `lz4` or `zstd`; this is rare. Under `compression: off`, 271 is retried as before. On a host where the pipeline threads are already the limit, compression now uses their CPU.

  A manually assembled pipeline must attach the sink's codec. Take `parts.frame_codec` from `into_parts()` and pass it to `ShardQueues::with_frame_codec`, or call `ClickHouseSink::frame_codec()` before `into_parts`. A caller that uses `ClickHouseWriter::write_batch` directly must code its frames with that codec. Otherwise the sink records a fatal error and sends no request.
  ([#1142])
- **LowCardinality columns hash their values with a seeded quality hasher** (`spate-clickhouse`)

  The native encoder's per-block LowCardinality dictionary hashes each value with the quality variant of its seeded hasher, which spreads every input bit into the bits the table indexes on. On the encode benchmarks this costs about 1% more instructions on x86_64 and 2% on aarch64 for a schema built around LowCardinality columns, and under 0.2% on the other encode cases. The encoded bytes are unchanged.
  ([#1567])
- **ClickHouse query responses arrive uncompressed** (`spate-clickhouse`)

  The ClickHouse sink asks for its query responses uncompressed. These are the schema check, the `Distributed` guard and the breaker's `SELECT 1` probe. Previously the sink asked for them in the codec `compression` names, and the server compressed them when its `enable_http_compression` setting was on. Insert bodies are unchanged: `compression` still applies to them. The responses are a few rows, so throughput does not change.
  ([#1451])
- **A `CommitReady` partition is committed until its lanes leave** (`spate-core`)

  A partition named in `SourceEvent::CommitReady` is committed on a 1 ms cadence for one `checkpoint.interval`. After that, it is committed on each control-plane poll that finds its acknowledged position advanced, until its lanes are retired or revoked or a new assignment begins. The periodic commit still covers the partition and retries a commit that failed. An acknowledgment of its final records that resolves after the first interval is committed within one event poll timeout. In previous versions the partition returned to the periodic commit after one `checkpoint.interval`, so such an acknowledgment waited up to one more interval. The positions committed are unchanged, and a source that never emits the hint keeps the periodic commit.
  ([#1435])
- **Breaking:** **`ExitReport.final_watermarks` lists only the partitions the source still holds** (`spate-core`)

  `ExitReport.final_watermarks` lists the last committed watermark of the partitions the source held at the final commit. A partition whose lanes were retired or revoked during the run is left out. In previous versions the list kept every partition committed during the run, so a Kafka consumer's report named partitions a rebalance had moved to another member. A partition that the next assignment hands back keeps its entry when no lanes were added since its revocation, and is otherwise listed once it commits again. What the source commits is unchanged.

  A source that emits `SourceEvent::LanesAdded` must give each added lane a partition above every partition admitted to the current epoch so far, revoked and retired ones included. `Checkpointer::extend_epoch` rejects a partition at or below the highest one admitted, and the run fails from the `checkpoint` component. In previous versions it rejected only a partition seen earlier in the epoch, and remembered every one for the rest of the epoch. A source that numbers its partitions in ascending order needs no change; a returning partition still needs a new epoch through `SourceEvent::LanesAssigned`.
  ([#1415])
- **Breaking:** **A sink can code each sealed chunk on the pipeline thread** (`spate-core`, `spate-test`)

  A sink can supply a `FrameCodec` through `SinkParts::with_frame_codec`. The pipeline thread runs the codec once on each chunk when the chunk seals. The shard worker sends the coded bytes unchanged, including on retries. The in-flight budget and `batch.max_bytes` still count the bytes before coding, so sizing rules do not change. `EncodedChunk` has a new public field, `raw_len`, the frame's length before coding. Code that builds an `EncodedChunk` with a struct literal must set it.

  A manually assembled pipeline attaches a sink's codec with `ShardQueues::with_frame_codec(..)`, using `parts.frame_codec`, before it hands the queues to chains. `CaptureSink::with_frame_codec` lets tests attach a codec to the capturing sink.
  ([#1137])
- **The derived pipeline thread count keeps fewer cores for the I/O runtime** (`spate-core`)

  When `pipeline.threads` is unset, the pipeline now runs `available_parallelism` minus one core for every two `io_threads` workers (rounded up), minus one core for the controller. Previously it subtracted one core for each I/O worker. With the default `io_threads: 2`, a 4-core host now runs 2 pipeline threads instead of 1, and a 32-core host runs 30 instead of 29. Compression for a sink that supplies a frame codec runs on the pipeline threads, so the I/O workers no longer carry it. Pipelines that set `pipeline.threads` are unaffected. To keep the previous count, set `pipeline.threads` to it.
  ([#1144])
- **Rejecting duplicate JSON keys costs fewer instructions** (`spate-json`)

  With `reject_duplicate_keys: true`, the guard that tracks each object's keys hashes them with a seeded quality hasher. In previous versions it used std's SipHash. On the decode benchmarks a clean document decoded with the guard on costs 8–14% fewer instructions, of which the guard's own share falls by about 19–40%. Rejecting a duplicate at the end of a wide object costs about 36–40% fewer. The error a duplicate raises is unchanged.
  ([#1566])
- **Breaking:** **`MetricsHandle::spawn_upkeep` returns a `TaskHandle`** (`spate-core`)

  `MetricsHandle::spawn_upkeep` returns a `spate_core::task::TaskHandle<()>`, and dropping that handle stops the upkeep task. In previous versions it returned a `tokio::task::JoinHandle<()>`, and the task kept running after the handle was dropped. Code that discards the returned handle stops the upkeep at once. Keep the handle for as long as histograms and process metrics should stay current. The pipeline runtime holds its own upkeep handle until the run ends.
  ([#1556])
- **`spate_queue_depth` falls as the sink drains its queue** (`spate-core`)

  The shard worker samples `spate_queue_depth` after each read from its queue, as well as the senders after each send. Previously only the senders sampled it, so once sends stopped it held the depth read at the last send even after the worker had emptied the queue. That reading was the queue's capacity when the last send found the queue full, and could be as low as 1 when the worker had kept up with the senders. A dashboard or alert that reads a depth stuck at capacity as a sign of backpressure sees it drop once the worker next reads from the queue. A send that lands as the worker empties the queue can leave its own reading in place until the next send. The reading still holds at capacity while the sink cannot take another batch, and a queue closed at shutdown keeps its last reading.
  ([#1306])
- **The S3 source reads a lane's next window while it hands on the current one** (`spate-s3`)

  When a lane starts handing on a GET window, the source starts the GET for the next window. At the last window of an object in a unit, it reads the first window of the next object instead. In previous versions the next GET started only once the current window's last chunks fit the lane's hand-off of four chunks, so a lane with the default `chunk_bytes` of 512 KiB could wait on its next GET. The read-ahead no longer depends on `chunk_bytes`, and a lane still has at most one GET in flight. When each window spans at least four chunks, as at the defaults, a lane can hold three windows, about `3 × units_held × prefetch_bytes` in total, or 48 MiB per pipeline thread at the defaults, so a host with 30 pipeline threads can hold about 1.4 GiB. In previous versions a run read 8 lanes of two windows each by default, about 128 MiB whatever its pipeline thread count. Shorter windows leave more of them queued. Lower `prefetch_bytes` or `units_held` to bound the read-ahead.
  ([#1505])
- **Breaking:** **An unrecognized `store` key fails at load** (`spate-s3`)

  For an S3 URL (`s3://`, `s3a://`, or an `https://` URL on an `amazonaws.com` or `r2.cloudflarestorage.com` host), a key under `store` that `object_store` does not recognize as an S3 option fails pipeline load, and the error names the key. In previous versions, the source skipped the key without a message. A misspelled `endpoint` therefore built a client for the default AWS endpoint and sent it requests signed with the configured credentials. Each request carried the access key ID and any session token. A configuration that carries such a key loads on v0.3.0 and fails on this version: correct the key's spelling or remove it. Keys under a `file://` URL are not checked.
  ([#1237])
- **Breaking:** **A sink writer receives its `Meter` through a `SinkMetricsCtx`** (`spate-core`)

  `ShardWriter::attach_metrics` takes a `SinkMetricsCtx`, whose `meter` is the `Meter` for the writer's own `spate_<component_type>_sink_*` families. The builder calls it once before the writer is shared, as before. In previous versions it received the `Option<Meter>` directly.

  An override becomes `fn attach_metrics(&mut self, cx: SinkMetricsCtx)` and reads `cx.meter`. A direct call wraps its argument in `SinkMetricsCtx::new`. A writer that takes the default needs no change.
  ([#1614])
- **Breaking:** **The split branch's bundle and handle are renamed** (`spate-core`)

  The bundle that `ChainCtx::sink` returns for one split branch is `spate::ops::SplitTarget`, and the typed handle that `SplitBuilder::add` returns is `spate::ops::SplitBranch<F>`. In previous versions they were `spate::ops::SinkCtx` and `spate::ops::Sink<F>`, and the old names are removed. `ChainCtx::sink`, `SplitBuilder::add`, `SplitEmitter::emit`, `SplitTarget::new` and `SplitTarget::with_chunk` keep their names and behavior. A pipeline that passes `ctx.sink(..)` into `add` and emits the returned handle compiles unchanged.

  To upgrade, change the names where code writes them: a split assembled by hand with `SinkCtx::new`, and a handle stored in a typed field or signature. The same paths apply without the facade, under `spate_core::ops`.
  ([#1615])
- **Dependency requirements** (`spate-avro`, `spate-clickhouse`, `spate-coordination`, `spate-core`, `spate-kafka`, `spate-s3`, `spate-sqs`, `spate-test`)

  The published manifests of these crates carry new requirements for the
  dependencies listed below. Cargo resolves your lockfile against these
  requirements, so a raised requirement can raise the version your project builds
  with. Each line shows the requirement in this release first, then the
  requirement in the previous release. A crate can enable more features than a
  line shows.

  - `async-nats` (`spate-coordination`): no longer a dependency. Previously `0.50`.
  - `aws-config` (`spate-sqs`): `1.13` without default features, with features `credentials-process`, `default-https-client`, `rt-tokio`, `sso`. New in this release.
  - `aws-credential-types` (`spate-sqs`): `1.3.1`. New in this release.
  - `aws-sdk-sqs` (`spate-sqs`): `1.114` without default features, with features `default-https-client`, `rt-tokio`. New in this release.
  - `aws-smithy-http-client` (`spate-sqs`): `1.5.1` without default features, with features `rustls-aws-lc`. New in this release.
  - `base64` (`spate-s3`): `0.23.1`. Previously `0.22.1`.
  - `cityhash-rs` (`spate-clickhouse`): `1.0.1`. New in this release.
  - `clickhouse` (`spate-clickhouse`): `0.15.3` without default features. Previously `0.15`.
  - `lz4_flex` (`spate-clickhouse`): `0.14.0` without default features, with features `std`. New in this release.
  - `object_store` (`spate-s3`): `0.14.1`. Previously `0.14`.
  - `quick-xml` (`spate-s3`): `0.41.0` with features `overlapped-lists`, `serialize`. New in this release.
  - `rdkafka` (`spate-kafka`): `0.39` with features `zstd`. Previously `0.39`.
  - `tracing-subscriber` (`spate-core`, `spate-test`): `0.3.19` without default features. Previously `0.3.8` without default features.
  - `twox-hash` (`spate-clickhouse`): `2.1.4` without default features, with features `xxhash64`. Previously `2.1` without default features, with features `xxhash64`.
  - `webpki-root-certs` (`spate-sqs`): `1.0.10`. New in this release.
  - `webpki-roots` (`spate-avro`, `spate-clickhouse`): `1.0.10`. Previously `1.0.8`.
  - `zstd` (`spate-s3`): `0.14.3` without default features. Previously `0.13`.
  - `zstd-sys` (`spate-kafka`): `2.1.2` without default features. New in this release.

### Removed

- **Breaking:** **`spate-s3` reads only through a feed** (`spate-s3`)

  The S3 source reads work units and storage notifications from the feed that the pipeline's `feed:` section names. A pipeline with an S3 source and no `feed:` section fails to start, and the error names the section to add. `feed: { memory: {} }` runs the source in one process, which lists its own prefix. That feed keeps nothing across a restart, so a restart lists and reads the whole prefix again.

  `S3Source::with_coordinator` and the `refresh_listing` source key are removed. A source file that sets `refresh_listing`, to `false` as well, fails to load: remove the key. `split_target_bytes` remains: objects at or below it are packed into units of at most 16 objects, larger ones that qualify for byte ranges are cut into ranges of at most it, and any other is one unit. A re-listing of an open prefix has no replacement: a continuous job reads storage notifications, and a bounded job lists once.

  `split_id_for` and `split_id_for_range` now return `Result<String, SplitError>`, and `SplitDescriptor::encode` and `SplitDescriptor::decode` return `SplitError` in place of `CoordinationError`. `SplitError` carries the reason in its `reason` field. The ids they mint are unchanged.

  The `spate_s3_source_objects_listed_total` counter is removed, since nothing increments it. Remove it from dashboards and alerts.
  ([#1396])
- **Breaking:** **`spate-coordination` and the coordination traits are removed** (`spate`, `spate-core`, `spate-coordination`, `spate-test`)

  The `spate-coordination` crate is removed with its memory and NATS stores. A work feed replaces it: the S3 source reads its input only through a feed, which a `feed:` section names. The `spate` features `coordination` and `coordination-nats` and the `spate::coordination` path are removed, so a `Cargo.toml` that names either feature no longer resolves.

  `spate_core::coordination` is removed with every item in it, among them `SplitPlanner`, `SplitCoordinator`, `SplitSource`, `CoordinationDriver`, `SplitId`, `SplitSpec`, `SplitPlan`, `LeaseEpoch` and `CoordinationError`. `spate-test` no longer exports `scripted_coordinator`, `ScriptedCoordinator` or `CoordinatorScript`. `Source::configure_coordination` and `Source::commit_final` are removed, so a source that overrides either stops compiling; the final commit of a stop is a call to `Source::commit`. `PipelineConfig::coordination` and `CoordinationSection` are removed, along with `CoordinationMetrics`, `AcquireReason`, `ReplanOutcome`, `RevocationOutcome`, `SplitLossReason` and `StoreOp`.

  A pipeline file that keeps a `coordination:` section, in any spelling, fails to load. The section and the two `Source` methods first shipped in v0.3.0. The 23 `spate_coordination_*` metric families are removed, so update any dashboard or alert that reads them; the `coordination` prefix stays reserved for custom metric namespaces.

  A coordinated job started on v0.3.0 cannot resume on this release. It has to finish on v0.3.0, or run again through a feed, which lists the prefix again, so a sink without idempotence sees those records twice.
  ([#1410])

### Fixed

- **Breaking:** **A current-thread I/O runtime fails startup** (`spate-core`)

  `PipelineRuntime::run` returns `StartError::Config` when the runtime passed to `PipelineRuntime::with_io_runtime` is a current-thread runtime. In previous versions, `run` accepted it. Tasks on that runtime, such as sink workers, the admin server and the upkeep task, ran only during brief startup and drain steps, and not while records flowed. Build the I/O runtime with `tokio::runtime::Builder::new_multi_thread`. Pipelines built with `Pipeline::from_config` are not affected.
  ([#1158])
- **Breaking:** **The Kafka sink no longer creates a missing topic, and the Kafka source and sink refuse more `rdkafka:` keys** (`spate-kafka`)

  The sink's producer and readiness probe set `allow.auto.create.topics=false`. In previous versions they kept librdkafka's producer default, so a sink pointed at a missing topic created it with the broker's defaults wherever the broker allows auto-creation, as Apache Kafka does by default, and the readiness probe's metadata read was enough to do it. The probe reports the missing topic as fatal, and a write to it fails. `rdkafka:` refuses `allow.auto.create.topics` on the sink and the source, and the source refuses `topic.auto.commit.enable`, `topic.enable.auto.commit` and `topic.auto.commit.interval.ms`, which the consumer never read. Every Kafka client sends a `client.id` of `spate-<section>-<role>`, such as `spate-sink-producer`, unless `rdkafka:` sets one. In previous versions each sent librdkafka's default, `rdkafka`, so a broker quota set on `rdkafka` stops matching. A configuration that carries one of the refused keys could load on v0.3.0 and fails at load on this version: remove the key.
  ([#1517])
- **A connection dropped during the SASL handshake in the Kafka sink** (`spate-kafka`)

  The Kafka sink no longer takes a connection that drops during the SASL mechanism handshake for a rejected credential. A write whose deliveries time out after such a drop is retryable, and a failed readiness probe no longer names the drop as a rejection by a broker. In previous versions, librdkafka's authentication error for the drop was recorded as a rejection, so a write whose deliveries timed out in the following window was fatal. This could happen when a broker, or a proxy in front of it, restarted while the producer was logging in. A broker that refuses the login, or answers the handshake with a mechanism it does not support, is still a rejection.
  ([#1217])
- **Breaking:** **The Kafka sink refuses an owned property written with a `topic.` prefix** (`spate-kafka`)

  An `rdkafka:` key that librdkafka reads as an owned property, such as `topic.acks`, `topic.request.required.acks` or `topic.message.timeout.ms`, fails pipeline load with the same reason as the unprefixed key, and the error names the key as written. A `topic.compression.codec` or `topic.compression.type` key beside the typed `compression` field fails as a conflict. In previous versions, the sink accepted these keys. A `topic.message.timeout.ms` or `topic.delivery.timeout.ms` value could replace the typed `delivery_timeout`, depending on the order the client applied the keys. A `topic.compression.codec` or `topic.compression.type` value other than `inherit` always replaced the typed `compression`. A `topic.acks` or `topic.request.required.acks` value other than `all` made pipeline load fail on some runs, when librdkafka refused to create the producer. A configuration that carries such a key could load on v0.3.0 and fails at load on this version: remove the key, or use the typed field that owns it.
  ([#1242])
- **Breaking:** **The Kafka source stops on a corrupt message** (`spate-kafka`)

  A message librdkafka reports as corrupt (`BadMessage`, as `check.crcs: "true"` detects) fails the pipeline with a fatal error. Records read before it that the sink has not yet written are not acknowledged. In previous versions the source retried the error, and could commit past the corrupt message. For message format v0 or v1, librdkafka skipped the offset when the same fetch held later messages, so the record never reached the sink and the group's committed offset passed it. For a record batch in format v2, the source either retried forever behind a healthy probe or, under `auto.offset.reset: latest`, committed past the corrupt batch and the valid records after it.

  After a restart, a partition with a committed offset is read from it, and the pipeline stops again at the corrupt message, because librdkafka checks the same data again. A partition with no committed offset starts where `auto.offset.reset` says. Under `earliest` the pipeline stops again at the same message. Under `latest`, the default, it starts at the end of the partition and never reads the corrupt message or the unwritten records before it. To resume, move the group's committed offset past the corrupt message, which skips that record. librdkafka checks CRCs only when `check.crcs` is `true`, which is not its default.

  A fatal error librdkafka reports for a partition after records in the same poll also fails the pipeline, such as `TopicAuthorizationFailed` after the consumer loses Read on the topic. In previous versions the source dropped a `TopicAuthorizationFailed` met this way, and the partition could stop reading with no error reported.
  ([#1265])
- **Breaking:** **The Kafka source reads `topic.auto.offset.reset` as its reset policy** (`spate-kafka`)

  A partition with no committed offset starts where `topic.auto.offset.reset` says, as it does under `auto.offset.reset`. In previous versions the source read only `auto.offset.reset` and started such a partition at its end, while librdkafka applied the prefixed value. A new consumer group configured with `topic.auto.offset.reset: earliest` skipped every record already in the topic. A configuration that sets both `auto.offset.reset` and `topic.auto.offset.reset` fails at load. It could load on v0.3.0: remove one of the two keys.
  ([#1274])
- **Kafka reads and writes zstd batches** (`spate-kafka`)

  The Kafka source and the Kafka feed read zstd-compressed batches, and the Kafka sink accepts `compression: zstd`. In previous versions librdkafka was built without zstd. The source retried a zstd batch until the topic's retention deleted it: it logged a warning, read nothing further from the partition, and the health probes stayed healthy. While the batch was still in the partition, no record was lost where the group had committed an offset for the partition. The committed offset did not move past the batch, and an upgraded pipeline on the same group resumes from it. A partition the group had never committed starts under `auto.offset.reset`. With the default, `latest`, an upgraded pipeline skips the zstd batch and every record written before it starts reading the partition. With `earliest`, it reads the partition from the start. Once retention deleted the batch, the source moved on by itself under `latest` or `earliest`. With `latest` it skipped every record still in the partition. With `earliest` it lost only the records retention deleted. With `error` it logged a warning and read nothing further from the partition. The sink failed to create its producer for `compression: zstd`. A batch in a codec librdkafka cannot read fails the pipeline with a fatal error, before any record after it is delivered, and so does a batch in a message format it does not know, read from its first record. librdkafka's error text for it is part of the error.
  ([#1256])
- **Native blocks reach `chunk.target_bytes`** (`spate-clickhouse`)

  A Native block closes at most 15 rows after its encoded size reaches `chunk.target_bytes`. In previous versions, the encoder over-estimated the block it was building, so blocks were sealed smaller than the target. The gap grew with each `LowCardinality` column. A table with four `LowCardinality(String)` columns and an `Array(LowCardinality(String))` column got blocks of about 42 KB at the default 64 KiB target. The same setting now gives fewer, larger blocks, so each insert carries fewer block headers and compressed frames. If you raised `chunk.target_bytes` to make up for small blocks, you can lower it again.
  ([#1161])
- **Each pipeline thread sends a sink shard its chunks in seal order** (`spate-core`)

  Each pipeline thread sends a sink shard's worker its chunks in the order it sealed them. In previous versions, a chunk sealed while an earlier chunk for the same shard waited for queue space could be sent first, when the shard's queue freed a slot before the waiting chunk was retried. Writes for one shard can still complete out of order while several batches are in flight, up to `inflight.max_per_shard`. Chunks from different pipeline threads can reach a shard in any order. No record was lost or duplicated, because each chunk carries its own acknowledgments.
  ([#1145])
- **A panicking sink probe turns readiness off** (`spate-core`)

  When the sink connectivity probe panics, the pipeline marks the sinks not connected, so `/readyz` returns 503 and stays there. The panic is logged at `ERROR` with the task name `sink-probe`. In previous versions the probe stopped running after a panic and `/readyz` kept the answer of the last successful probe, so a pod could stay ready with no check behind it.
  ([#1556])
- **A dropped S3 lane stops reading its objects** (`spate-s3`)

  Dropping an S3 lane ends the task that reads its objects, also while that task waits to retry a failed read. In previous versions the task kept running after its lane was gone, so a read that kept failing went on retrying until it ran out of attempts, after a revocation or after the pipeline stopped. The task holds no acknowledgements, so ending it loses no records and replays none. `spate_s3_source_get_retries_total` counts a retry once its backoff ends, so it counts only the attempts actually made beyond the first. In previous versions it also counted a retry for the final failure of an exhausted attempt budget, and for a task that ended while it waited.
  ([#1558])
- **A pipeline thread no longer polls S3 lanes that have read their input to the end** (`spate-s3`)

  A thread whose S3 lanes have all reached the end of their input waits on its control channel until they are retired. In previous versions it polled those lanes in a loop and ran a full core until the sink acknowledged their last records, which for small objects is up to one `batch.linger`.
  ([#1480])
- **Objects with reserved characters in the key are read** (`spate-s3`)

  The S3 source reads every listed object under the key the listing returned. In previous versions, the source requested a different key for an object whose key contained any non-ASCII character or one of `%` `#` `[` `]` `{` `}` `<` `>` `^` `~` `|` `\` `` ` `` `"` `*` `?`. The store answered that request with "not found". The split was retried, then quarantined at its attempt cap, and a bounded job ended `Stalled` with that object unread. Keys made only of other printable ASCII characters were not affected.
  ([#1115])
- **An S3 source error cuts the store's error body to a bound** (`spate-s3`)

  An error from a failed object read or backfill listing carries at most 512 characters of the store's response body, with `…` where the body was cut. The request and the HTTP status ahead of the body are kept whole, and a TLS rejection behind the text is still named in full. In previous versions, such an error, and the log line that reported it, carried the store's whole response body, of any size. The error's class, and what the source does with the object or the listing, are unchanged.
  ([#1351])
- **A key the S3 client cannot address poisons a split of its own** (`spate-s3`)

  The S3 source lists every object under the prefix even when some keys cannot be read through the S3 client. Such a key has an empty segment (`d//x.ndjson`), a `.` or `..` segment, or an ASCII control character, or starts or ends with `/`. Each object with such a key is planned in a split of its own, which poisons on its first read with the key in its reason, so a bounded job reads every other object and then ends stalled. An object with such a key and no bytes, such as a folder marker `dir/`, is not planned. In previous versions, one key with an empty, `.` or `..` segment or a control character failed the pipeline at its first listing. An object with bytes whose key started or ended with `/` was read under the key without it, pinned to its own listed ETag, so the read failed and poisoned its split and every object packed with it unless an object under that key had the same ETag.
  ([#1169])

### Security

- **Unknown schema ids no longer grow the schema cache without bound** (`spate-avro`)

  The schema cache removes a negative entry once its `negative_cache_ttl` has passed, and holds at most 1,024 negative entries, dropping the oldest for a new one. In previous versions an expired negative entry stayed in the map, so each distinct schema id the registry did not know added an entry for the life of the process. Each new entry also copied the whole map under the lock that schema lookups wait on, so a producer writing records with many distinct unknown ids to a Confluent wire-format topic could slow schema resolution for the pipeline. A dropped entry costs at most one more registry fetch for that id. The `registry reports schema id unknown` warning is rate-limited, so records naming many unknown ids no longer log one line each. Records whose schema is already cached were not affected.
  ([#1626])
- **A small compressed object no longer drives memory without bound** (`spate-s3`)

  The S3 source decodes a gzip or zstd chunk in steps of about 256 KiB of output and moves each step's records into the batch before decoding the next. In previous versions one chunk, up to `source.s3.chunk_bytes` of compressed input, was decoded in full and every record it produced was queued at once. An object of a few kilobytes that expands to many short records could hold gigabytes of memory and stop the pipeline process. Anyone able to write an object under the source's prefix could trigger this, and the object was read again each time its unit was retried. The zstd decoder accepts a window of at most 128 MiB per lane, which is the zstd library's own default; a frame that declares a larger window fails its unit as undecodable. Uncompressed objects were not affected.
  ([#1625])
- **The Mozilla root bundle no longer trusts emSign ECC Root CA - G3** (`spate-avro`, `spate-clickhouse`)

  The fallback root bundle these crates use requires `webpki-roots` 1.0.10, which removes the `emSign ECC Root CA - G3` root (RUSTSEC-2026-0381). The private keys of several intermediates under that root are public, so anyone can issue a certificate through them for any host name. In previous versions the crates accepted `webpki-roots` 1.0.8 and 1.0.9, which include the root. The bundle is used only when the system trust store yields no certificates, such as in a container image without CA certificates. It applies to the Avro schema registry client and the ClickHouse sink over `https`. A deployment in that situation could accept a forged certificate for those servers. Run `cargo update -p webpki-roots` if your lockfile resolves an older 1.x release. A trust store read from the system is not affected by this change; keep the operating system's CA certificates up to date.
  ([#1319])
- **zstd objects decode with a patched decoder** (`spate-s3`, `spate-clickhouse`)

  The S3 source decodes `.zst` objects with `zstd` 0.14.3 and `zstd-safe` 8.2.0. In previous versions it used `zstd` 0.13.3 and `zstd-safe` 7.2.4, whose streaming decoder could carry up to 32 KiB of uninitialized heap memory into its output when decoding failed inside a legacy (zstd v0.4 to v0.7) frame (RUSTSEC-2026-0403). Whoever can write an object under the source's prefix can supply such a frame. An object holding one fails its unit as undecodable either way. The ClickHouse sink's compressor moves to the same release, and the sink no longer depends on `zstd` 0.13.3. The compressor is not affected by the advisory.
  ([#1451])

### Contributors

- Marcus Kainth
- Aaron
- Danny Ward

[#1115]: https://github.com/spate-etl/spate/pull/1115
[#1123]: https://github.com/spate-etl/spate/pull/1123
[#1124]: https://github.com/spate-etl/spate/pull/1124
[#1137]: https://github.com/spate-etl/spate/pull/1137
[#1142]: https://github.com/spate-etl/spate/pull/1142
[#1144]: https://github.com/spate-etl/spate/pull/1144
[#1145]: https://github.com/spate-etl/spate/pull/1145
[#1148]: https://github.com/spate-etl/spate/pull/1148
[#1153]: https://github.com/spate-etl/spate/pull/1153
[#1158]: https://github.com/spate-etl/spate/pull/1158
[#1161]: https://github.com/spate-etl/spate/pull/1161
[#1169]: https://github.com/spate-etl/spate/pull/1169
[#1172]: https://github.com/spate-etl/spate/pull/1172
[#1173]: https://github.com/spate-etl/spate/pull/1173
[#1211]: https://github.com/spate-etl/spate/pull/1211
[#1217]: https://github.com/spate-etl/spate/pull/1217
[#1237]: https://github.com/spate-etl/spate/pull/1237
[#1242]: https://github.com/spate-etl/spate/pull/1242
[#1256]: https://github.com/spate-etl/spate/pull/1256
[#1265]: https://github.com/spate-etl/spate/pull/1265
[#1274]: https://github.com/spate-etl/spate/pull/1274
[#1284]: https://github.com/spate-etl/spate/pull/1284
[#1302]: https://github.com/spate-etl/spate/pull/1302
[#1306]: https://github.com/spate-etl/spate/pull/1306
[#1319]: https://github.com/spate-etl/spate/pull/1319
[#1326]: https://github.com/spate-etl/spate/pull/1326
[#1330]: https://github.com/spate-etl/spate/pull/1330
[#1350]: https://github.com/spate-etl/spate/pull/1350
[#1351]: https://github.com/spate-etl/spate/pull/1351
[#1396]: https://github.com/spate-etl/spate/pull/1396
[#1410]: https://github.com/spate-etl/spate/pull/1410
[#1415]: https://github.com/spate-etl/spate/pull/1415
[#1422]: https://github.com/spate-etl/spate/pull/1422
[#1435]: https://github.com/spate-etl/spate/pull/1435
[#1451]: https://github.com/spate-etl/spate/pull/1451
[#1476]: https://github.com/spate-etl/spate/pull/1476
[#1479]: https://github.com/spate-etl/spate/pull/1479
[#1480]: https://github.com/spate-etl/spate/pull/1480
[#1484]: https://github.com/spate-etl/spate/pull/1484
[#1505]: https://github.com/spate-etl/spate/pull/1505
[#1508]: https://github.com/spate-etl/spate/pull/1508
[#1509]: https://github.com/spate-etl/spate/pull/1509
[#1514]: https://github.com/spate-etl/spate/pull/1514
[#1515]: https://github.com/spate-etl/spate/pull/1515
[#1516]: https://github.com/spate-etl/spate/pull/1516
[#1517]: https://github.com/spate-etl/spate/pull/1517
[#1519]: https://github.com/spate-etl/spate/pull/1519
[#1556]: https://github.com/spate-etl/spate/pull/1556
[#1557]: https://github.com/spate-etl/spate/pull/1557
[#1558]: https://github.com/spate-etl/spate/pull/1558
[#1565]: https://github.com/spate-etl/spate/pull/1565
[#1566]: https://github.com/spate-etl/spate/pull/1566
[#1567]: https://github.com/spate-etl/spate/pull/1567
[#1569]: https://github.com/spate-etl/spate/pull/1569
[#1594]: https://github.com/spate-etl/spate/pull/1594
[#1614]: https://github.com/spate-etl/spate/pull/1614
[#1615]: https://github.com/spate-etl/spate/pull/1615
[#1625]: https://github.com/spate-etl/spate/pull/1625
[#1626]: https://github.com/spate-etl/spate/pull/1626

## [0.3.0] — 2026-10-11

### Added

- **Admin server address** (`spate-core`)

  The pipeline now logs the admin server's bound address at `INFO` with the
  message `admin server listening`. Previously, startup did not report this
  address, so configuring `admin.listen` with port `0` left the automatically
  assigned port out of the logs. You can use the logged address to reach
  `/metrics`, `/healthz`, and `/readyz`.
  ([#338])
- **Breaking:** **The schema registry trusts a private CA with `registry.tls.root_ca`** (`spate-avro`)

  The Avro deserializer's `registry.tls.root_ca` setting names a PEM bundle of
  root CAs. The registry client trusts them in addition to the system trust
  store. In previous versions a registry whose certificate a private CA signed was
  trusted only through the host's store, or through `SSL_CERT_FILE` or
  `SSL_CERT_DIR`, which on Linux and other Unix systems replace that store.

  The setting requires an `https://` registry URL. A file that cannot be read,
  holds no certificate or holds a malformed one fails the deserializer at
  startup.

  `RegistrySection` is `#[non_exhaustive]` and gains a `tls` field of the new
  type `TlsSection`. Code that builds a `RegistrySection { .. }` literal no longer
  compiles. Build it with `RegistrySection::new(url)` and set `username`,
  `password` and `tls` as fields.
  ([#757])
- **ClickHouse row derive** (`spate-clickhouse-derive`)

  `#[derive(ClickHouseRow)]` now implements `spate_clickhouse::ClickHouseRow` for
  a row struct and generates its insert column list in field declaration order.
  It supports `#[serde(rename = "...")]` for names such as `outer.inner` in a
  flattened `Nested` table. Duplicate or malformed column names are compile
  errors. The derive also rejects `#[serde(flatten)]`, container-level
  `#[serde(rename_all = "...")]`, and `#[serde(skip_serializing_if = "...")]`.
  ([#406])
- **A solo run takes its coordinator tuning from the file** (`spate-coordination`, `spate-s3`)

  `coordination: { store: { memory: {} } }` selects the in-process coordination
  store, and the section's tuning then applies to a solo run. Set `max_in_flight`
  there to raise the S3 source's read parallelism, which is one lane per
  in-flight split and defaults to 8. In previous versions a solo S3 run always
  used the default tuning, and only a coordinator built in code and passed to
  `S3Source::with_coordinator` could change it. The in-process store is never
  shared between processes: progress is lost when the process exits, and each
  instance configured with it reads the whole input.
  ([#868])
- **A `coordination:` section configures the coordination store** (`spate-core`, `spate-coordination`, `spate-s3`)

  The pipeline file takes an optional top-level `coordination:` section. It names
  the store under `store:` (`nats`, with the fields of `NatsConfig`) and sets the
  coordinator's tuning, such as `instance_id` and `lease_duration`. The S3 source
  builds its coordinator from the section when the pipeline starts, so a
  deployment changes its NATS servers, credentials or identity without a rebuild.
  In previous versions these settings were Rust code passed to
  `S3Source::with_coordinator`, which still works for a coordinator built in
  code. Setting both the section and `with_coordinator` fails startup, and so
  does a `coordination:` section on a source that does not coordinate. NATS
  `credentials` also deserialize from a single-key map naming the mechanism
  (`user_password`, `token` or `creds_file`); the tagged form (`!token …`) still
  parses. Custom sources accept the section by overriding the new
  `Source::configure_coordination` method.
  ([#782])
- **Split lease headroom histogram** (`spate-coordination`, `spate-core`)

  A coordinated worker now records `spate_coordination_split_lease_headroom_seconds`
  each time a split-lease renewal is confirmed. The value is the time left before
  the worker would self-fence the split, on the worker's clock, and 0 when none
  was left. Alert on its low buckets to see a lease running short before
  `split_losses_total{reason="starved"}` counts. `CoordinationMetrics::split_lease_headroom`
  records it.
  ([#982])
- **Coordination stores can declare a polled watch** (`spate-coordination`)

  `CoordinationStore::watch_mode` returns `WatchMode::Push` by default. A store
  whose watch lists its prefix at an interval returns `WatchMode::Polled`, and
  the coordinator then watches only the assignment records, the plan record and a
  verdict marker in the durable keyspace. It reads what that watch does not
  carry: a worker reads the records of a split it was assigned but never saw, and
  every interval the leader re-reads each assigned runnable split that shows no
  lease and reads any spec it lacks. Only the leader reconciles, over the split
  records. A new leader lists every split and spec record before it plans or
  publishes. The first worker to report the job terminal writes a durable
  `verdict` key, which tells the others to list the split records and report too.
  A polled interval of zero, or not below `lease_duration`, is rejected at startup.
  ([#812])
- **Find an error in a source chain** (`spate-core`)

  `spate::error::find_source::<T>` returns the first `T` in an error's source
  chain, the error itself included. At each `std::io::Error` it also follows the
  error the `io::Error` wraps, which `io::Error::source` skips. A custom
  connector can use it to find the TLS library's error behind its client's
  wrappers and classify a rejected handshake as `Fatal`.
  ([#720])
- **Decoder access for fuzz testing** (`spate-s3`, `spate-coordination`)

  The optional `testing` feature now exposes a `fuzz_seams` module for testing
  private decoders. These functions cover the composite offset codec, object
  framing with gzip and zstd decompression, and coordination record and key
  parsing. The `fuzz/` harness uses them to test malformed input. The feature is
  disabled by default, is not enabled by the `spate` facade, and has no semantic
  versioning compatibility guarantee.
  ([#322])
- **Kafka partition fetch state** (`spate-kafka`)

  With `metrics.per_partition_detail: true`, the Kafka source now publishes
  `spate_kafka_source_partition_not_fetching`. It reads `1` when an assigned
  partition's librdkafka fetch state is not `active` and `0` when it is active.
  Previously, the available metrics could not distinguish a partition stopped
  locally from one waiting for a leader or offset lookup. If a partition remains
  inactive across statistics windows, the source also logs its fetch state and
  offsets, even when partition metrics are disabled. Use the gauge to identify
  partitions that are not fetching and the log to investigate the reason.
  ([#343])
- **Waiting on captured logs** (`spate-test`)

  `LogCapture` now provides `wait_for` and `wait_for_line`. `wait_for` polls a
  check until it returns a value, on the same cadence as `wait_until`.
  `wait_for_line` returns the first captured line containing a given text. On
  timeout, both panic with every line captured so far, so a failing test shows
  what the component logged.
  ([#792])
- **Breaking:** **The S3 source reads a split that covers a byte range of one object** (`spate-s3`, `spate-core`, `spate-json`)

  A split descriptor can name a byte range of one object. Build it with
  `SplitDescriptor::with_range` and a `SplitRange`, and mint its id with
  `split_id_for_range`. The S3 source reads such a split and emits the records
  that start inside the range. It skips the bytes before the first of those
  records, and reads past the range's end to finish the last one. Ranges that
  cover an object deliver each of its records once.

  A ranged split needs an uncompressed object with an ETag, and a framer that
  names the byte a record can start after. `RecordFramer` has a new method,
  `resync_delimiter`, which returns `None` unless a framer overrides it.
  `NdjsonFramer` returns `\n`. The pipeline stops on a ranged split whose
  delimiter differs from the framer's, or whose object the source's
  `compression` setting decodes.

  The split descriptor format is now version 2, and the version is part of the
  job fingerprint. A coordinated S3 job that started on a previous version
  cannot resume on this one: the worker stops at startup with
  `job fingerprint mismatch: this worker is configured as ... but the store
  prefix belongs to ...`. Finish the job on the previous version, or give it a
  new store prefix. For the NATS store that is a new `job` value, in the
  `coordination:` section or on `NatsConfig` in code.
  A custom store needs a new prefix of its own. A new prefix starts from
  nothing and delivers every record again. A solo job, with neither a
  `coordination:` section nor `with_coordinator`, keeps no progress across
  restarts and is not affected.
  ([#858])
- **Fixtures for stage and config tests** (`spate-test`)

  `spate-test` now provides `record`, `raw_payload` and `test_ack` to build the
  input of a deserializer or encoder under test. The records and payloads carry
  zeroed metadata, and a record's acknowledgment handle discards its resolution.
  `component_config` builds a `ComponentConfig` from a YAML body, and
  `unique_name` returns a pipeline or component name no other call in the process
  returns, so tests sharing one binary do not claim the same gauge series.
  `spate-test` now depends on `serde_yaml` (the `yaml_serde` package), which
  `spate-core` already builds.
  ([#786])
- **Log capture for tests** (`spate-test`)

  `spate-test` now provides `capture_logs` and `show_logs` for asserting on what
  a component logged, plus the `LogCapture` writer behind them for a test that
  installs a subscriber of its own. `capture_logs` runs a closure under a
  `tracing` subscriber and returns the lines it formatted. `show_logs` writes
  those lines to the test harness, which prints them when the test fails. Both
  take the maximum level as a parameter and install the subscriber on the calling
  thread only, so tests sharing one process do not capture each other's output.
  `spate-test` now carries `tracing` and `tracing-subscriber` as ordinary
  dependencies, so both are built by anything that depends on it; previously they
  were dev-dependencies.
  ([#572])
- **Metric assertions for tests** (`spate-test`)

  `spate-test` now provides `render_metrics`, which runs a closure under a
  recorder local to the calling thread and returns the Prometheus text
  exposition. Histograms render with the same buckets as the installed exporter.
  `metric_value`, `metric_sum` and `metric_series` read series back out of that
  text by sample name and a subset of labels in any order. `metric_value` returns
  `None` when no series matches and panics when more than one does, so a test can
  tell an absent series from one that reads zero.
  ([#791])
- **Classify a rustls rejection with one macro** (`spate-core`)

  `spate::tls_rejection!(rustls, &err)` returns the TLS rejection in an error's
  source chain, if there is one, as an `Option<&rustls::Error>`. A rejection is
  a certificate the client failed to verify, an alert listed in
  `spate::error::TLS_REJECTION_ALERTS`, or a peer that shares no protocol
  version, cipher suite or other handshake parameter with the client. The first
  argument is the path to your own `rustls` crate, such as `async_nats::rustls`,
  so spate-core takes no rustls dependency. A custom connector can use it to
  class a rejected handshake `Fatal`.
  ([#733])

### Changed

- **Avro map allocation** (`spate-avro`)

  `AvroDatumDeserializer` now uses the first block's entry count to reserve space
  when decoding a map into a `HashMap`. Previously, the map started with no
  reserved space and grew as entries arrived. This reduces allocation and
  rehashing while decoding the first block; later blocks may still require the
  table to grow.

  Malformed input can cause a reservation before decoding fails. The size hint
  is limited by the bytes remaining in the payload, and serde also caps the
  number of entries it reserves. For `HashMap<String, i64>`, a sufficiently large
  malformed payload can cause an allocation of about 2 MiB.

  A custom visitor that reads no entries now rejects a malformed first block
  header or a count above the decoding budget. Previously, that visitor could
  accept the datum without checking the header. Array decoding is unchanged.
  ([#540])
- **A schema registry URL that is not `http://` or `https://` fails at load** (`spate-avro`)

  The Avro deserializer fails at startup when `registry.url` does not parse as a
  URL or its scheme is not `http` or `https`, such as `localhost:8081` without a
  scheme. The error does not repeat the URL, which may carry credentials. In
  previous versions the deserializer started, and every schema fetch failed and
  was retried without limit while the batch was held.
  ([#873])
- **Breaking:** **Registry URL credentials** (`spate-avro`)

  Registry URLs with credentials fail at configuration load, including URLs
  used without separate credential fields. Remove credentials from `registry.url`
  and set `registry.username` and `registry.password`. Use decoded credential
  text in these fields if the URL used percent encoding. Previously, URL
  credentials were accepted and could produce duplicate Authorization headers
  when separate credentials were also configured.
  ([#927])
- **Breaking:** **ClickHouse insert columns from the row type**
  (`spate-clickhouse`)

  The ClickHouse sink now takes its insert column list from the row type, and
  `#[derive(ClickHouseRow)]` generates that list in field declaration order.
  Previously, the column list was supplied separately in the sink configuration.
  The derive reports duplicate or malformed names at compile time; those errors
  were previously detected when the sink was built. Use `#[serde(rename)]` for
  column names such as `tags.key` that cannot be Rust field names.

  Remove `columns` from `sink: { clickhouse: ... }` and from calls to
  `ClickHouseSinkConfig::new`. The `from_component_config` and `build` functions
  now return a `ClickHouseSinkBuilder`; await its
  `.with_row::<Owned<YourRow>>()` method to obtain the runnable sink.
  ([#407])
- **Breaking:** **ClickHouse schema checks on every sink** (`spate-clickhouse`)

  The ClickHouse sink now fetches the table schema from every replica when it is
  built, and `format: rowbinary` sends column names and types using
  `RowBinaryWithNamesAndTypes`. Previously, `validate_schema` could disable type
  checks, and RowBinary inserts carried no type information. The server now
  checks the header on every insert, so changing a column's decimal scale or
  `DateTime64` precision while a pipeline is running causes an error instead of
  silently interpreting values with the new scale.

  Remove `validate_schema` from the sink configuration. Await
  `ClickHouseSinkBuilder::with_row` to fetch the schema and obtain the sink;
  this replaces `ClickHouseSink::validate_schema`. Construct the encoder with
  `ClickHouseEncoder::with_schema` and pass `sink.schema()` to it. Call
  `sink.native_schema()` without awaiting it. Remove
  `input_format_with_names_use_header` and `input_format_with_types_use_header`
  from custom `settings`; the sink manages them and rejects overrides.

  Row type checks now also apply to pipelines previously configured with
  `validate_schema: off` or `names`, and `NativeSchema::from_columns` validates
  declared types as well as names. A field with the same byte width as its
  column can therefore fail on the first record if its type is incompatible.
  For example, use an `i64` for a raw `DateTime64` value or a matching wrapper
  such as `DateTime64Millis`; a `u64` is rejected.
  ([#413])
- **Breaking:** **A ClickHouse URL with credentials is rejected at load** (`spate-clickhouse`)

  A replica URL in `shards` or a `distributed_check.endpoint` URL that carries a
  user or password, such as `http://svc:secret@ch-0:8123`, fails at load. In
  previous versions the sink accepted such a URL but never sent those
  credentials. Requests authenticated as the configured `user`, or as the
  server's `default` user when `user` was unset. Move the credentials into `user`
  and `password`.
  ([#772])
- **Breaking:** **Injectable clocks move to `spate_core::clock`** (`spate-core`, `spate-coordination`)

  The clock traits live in one module. `spate::clock::{Clock, SystemClock}` is
  the clock for synchronous code, and `spate::clock::tokio::{Clock, Sleep,
  SystemClock}` is the clock for code on the tokio timer. In previous versions
  these were `spate::backpressure::{Clock, MonotonicClock}` and, with the
  `coordination` feature, `spate::coordination::{Clock, Sleep, SystemClock}`.
  The old paths are removed.

  To upgrade, change the imports. Code that names `MonotonicClock` renames it to
  `SystemClock`, which is also the default clock of `WatermarkController`. The
  traits and their behavior are unchanged. The same paths apply without the
  facade, under `spate_core::clock`.
  ([#696])
- **Coordination log targets** (`spate-coordination`)

  Most log events from the coordination task carry a target below
  `spate_coordination::task` that names their area. For example,
  `assignment published` uses `spate_coordination::task::assignment`, and lease
  and claim events use `spate_coordination::task::claim`. In previous versions
  these events used the target `spate_coordination::task`. `RUST_LOG` directives
  match targets by prefix, so `spate_coordination=debug` and
  `spate_coordination::task=debug` still select every event. A log query or alert
  that matches the exact target `spate_coordination::task` needs to match that
  prefix instead.
  ([#897])
- **A watch delete orders above what its store handle saw** (`spate-coordination`)

  The revision of a `WatchEvent::Delete` that a store handle's watch reports
  follows a narrower rule. It is above every revision the key held before the
  deletion that this handle, or a clone of it, returned from a create, update,
  read or listing, or that a watch of it delivered. It can sit at or below a
  revision another handle wrote that this handle never saw. A store whose
  watch is polled can report such a delete. In previous versions, the
  `CoordinationStore` contract required it to be above every revision the key
  held. The NATS and in-memory stores still meet that stronger rule, and a
  custom store that met it needs no change.
  ([#968])
- **Object keys in the duplicate-key guard** (`spate-json`)

  With `reject_duplicate_keys: true`, decoding now reads each object key straight
  from the payload, and copies only a key written with an escape. Previously,
  every key was copied out of the payload before the check saw it. A document
  whose keys carry no escapes therefore runs the check with no per-key heap
  allocation. Duplicate detection at every depth, the error message and the
  `duplicate_key` metric label are unchanged.
  ([#591])
- **Duplicate-key guard allocation** (`spate-json`)

  With `reject_duplicate_keys: true`, decoding now moves each object key into the
  set that checks for repeats. Previously, the set stored a copy and the original
  was dropped, so every key cost one extra allocation. Documents with many object
  keys therefore decode with fewer heap allocations. Duplicate detection at every
  depth, the error message and the `duplicate_key` metric label are unchanged.
  ([#542])
- **Minimum Rust version** (all crates)

  Building Spate now requires Rust 1.96 or newer. Previously, the minimum
  supported version was Rust 1.94. Update older build and CI toolchains before
  upgrading Spate.
  ([#491])
- **Breaking:** **The NATS store rejects an existing bucket with a different
  replica count** (`spate-coordination`)

  The NATS coordination store fails startup with a fatal error when the job's
  existing state or lease bucket has a replica count other than `replicas`. The
  message names the bucket and both counts. In previous versions the store used
  the existing bucket and ignored `replicas`, so changing `replicas` for a job
  whose buckets existed had no effect. To change the replica count, run
  `nats kv edit <bucket> --replicas <n>` on both buckets and set `replicas` to
  the same value. To keep the current count, set `replicas` to the count the
  error names.
  ([#965])
- **NATS listings read a prefix in one pass** (`spate-coordination`)

  The NATS coordination store lists a prefix with one streamed read of the
  latest message for each key under it. In previous versions, a listing read the
  name of every key in the bucket and then fetched each matching key with its
  own request, one after another. The reconcile pass lists the whole state
  bucket, and the plan recount and the completion check list every split record,
  so on a job with many splits each of them took seconds to minutes. A listing
  returns each key at most once.
  ([#977])
- **Breaking:** **Portable descriptor budget** (`spate-s3`)

  The split planner rejects descriptors above 294,720 raw bytes so the complete
  encoded spec fits the portable 384 KiB stored-value budget, which every
  built-in store accepts. Earlier versions allowed raw descriptors up to 400 KiB,
  and the NATS store refused the largest of them once encoded. Jobs whose listed
  metadata produces larger descriptors must reduce that metadata before
  upgrading.
  ([#896])
- **Replan seeding** (`spate-coordination`)

  A plan run writes only the splits the leader has not already seen in the store,
  and keeps up to 64 of those writes in flight. Previously every run sent two
  create requests for every split the planner returned, one at a time, and the
  store refused all but the new ones. With `refresh_listing: true` on a large
  prefix, a replan now costs store writes in proportion to the new splits. A
  custom `CoordinationStore` receives these creates concurrently on one handle.
  `spate_coordination_splits_planned_total` now also counts the splits a run
  created before its seeding failed; previously those splits were never counted.
  A split record deleted from the store by hand is no longer re-created while the
  leader still holds it in its view. The listing that recounts the plan after
  each run is unchanged.
  ([#650])
- **Breaking:** **The S3 source reads a large object as several byte-range splits** (`spate-s3`)

  The planner cuts an uncompressed object above `split_target_bytes` into byte
  ranges of at most the target, one split each, so several workers read it at
  once. This applies when the object has an ETag, is at most 50,000 GiB, and
  the framer returns a delimiter from `RecordFramer::resync_delimiter`, as
  `NdjsonFramer` does. Previously one lane read every such object from start to
  end. Compressed objects, and objects under a framer that returns `None`, are
  still read whole.

  The packing version changes, so every split id changes, and the framer's
  delimiter joins the job fingerprint. Every worker of a job needs a framer that
  declares the same delimiter. A coordinated job started on a previous version
  cannot resume on this one. The entry "The S3 source reads a split that covers
  a byte range of one object" gives the startup error and how to migrate.

  The records of one object no longer arrive in object order, because its ranges
  are read in parallel. `spate_s3_source_objects_completed_total` and
  `spate_s3_source_objects_remaining` count each byte range as one object, so
  completed objects can exceed `spate_s3_source_objects_listed_total`.
  ([#862])
- **Breaking:** **SimpleAggregateFunction column support** (`spate-clickhouse`)

  With `format: native`, the sink now supports `SimpleAggregateFunction(f, T)`
  columns whenever the Native encoder supports the stored type `T`. Previously,
  these columns were rejected as an unknown type when the encoder was built.
  You can now insert their values using the same row field types as for `T`.

  With `format: rowbinary`, the first-record check now validates the row field
  against `T`; previously, that type check was skipped. Incompatible fields
  therefore fail on the first record. A column storing `Nullable(U)` now accepts
  a compatible `Option` field, which the previous check rejected.
  ([#486])
- **Dependency requirements** (`spate-avro`, `spate-clickhouse`, `spate-clickhouse-derive`, `spate-coordination`, `spate-core`, `spate-json`, `spate-kafka`, `spate-s3`)

  The published manifests of these crates carry new requirements for the
  dependencies listed below. Cargo resolves your lockfile against these
  requirements, so a raised requirement can raise the version your project builds
  with. Each line shows the requirement in this release first, then the
  requirement in the previous release. A crate can enable more features than a
  line shows.

  - `async-nats` (`spate-coordination`): `0.50`. Previously `0.49`.
  - `http-body-util` (`spate-core`): `0.1.3`. Previously `0.1.2`.
  - `hyper-rustls` (`spate-clickhouse`): `0.27.9` without default features, with features `http1`, `tls12`. New in this release.
  - `hyper-util` (`spate-core`): `0.1.16`. Previously `0.1.12`.
  - `openssl-sys` (`spate-kafka`): `0.9.108`. New in this release.
  - `proc-macro-crate` (`spate-clickhouse-derive`): `3`. New in this release.
  - `proc-macro2` (`spate-clickhouse-derive`): `1.0.106`. New in this release.
  - `quote` (`spate-clickhouse-derive`): `1.0.44`. New in this release.
  - `reqwest` (`spate-avro`): `0.13.4` without default features. New in this release.
  - `rustls` (`spate-avro`, `spate-clickhouse`, `spate-s3`): `0.23.45` without default features, with features `aws_lc_rs`, `std`, `tls12`. New in this release.
  - `rustls-native-certs` (`spate-avro`, `spate-clickhouse`, `spate-coordination`): `0.8.4`. New in this release.
  - `schema_registry_converter` (`spate-avro`): no longer a dependency. Previously `4.9` without default features.
  - `simd-json` (`spate-json`): `0.18.1`. Previously `0.17.3`.
  - `syn` (`spate-clickhouse-derive`): `3` with features `full`. New in this release.
  - `url` (`spate-s3`): `2.5.4`. Previously `2.5`.
  - `webpki-roots` (`spate-avro`, `spate-clickhouse`, `spate-coordination`): `1.0.8`. New in this release.

### Deprecated

- **Split coordination is deprecated** (`spate`, `spate-coordination`, `spate-core`, `spate-s3`, `spate-test`)

  `spate-coordination` and every public item it defines are deprecated, and so
  are `spate`'s `coordination` and `coordination-nats` features. So are
  `spate-core`'s `SplitPlanner`, `SplitCoordinator`, `SplitSource`,
  `CoordinationDriver`, `CoordinationSection`, `Source::configure_coordination`
  and `Source::commit_final`, `S3Source::with_coordinator`, and `spate-test`'s
  `scripted_coordinator`, `ScriptedCoordinator` and `CoordinatorScript`. Code
  that names these items or calls the two methods warns at build time.
  Overriding `commit_final` does not warn. A pipeline started with a
  `coordination:` section logs a warning, including a solo run that uses the
  section for tuning. Version 0.4.0 removes all of this in favour of a work feed
  ([#1088]), so a coordinated job started on 0.3.0 has to finish on 0.3.0 or run
  again through the feed. [#1111] lists the known defects in the deprecated
  crate.
  ([#1109])

### Removed

- **Breaking:** **The `full` feature** (`spate`)

  The `spate` crate no longer declares a `full` feature, so each build names the
  connectors it uses. In previous versions, `full` enabled `avro`, `json`,
  `kafka`, `clickhouse`, `coordination-nats` and `s3`. A dependency that still
  names `full` fails to resolve.

  To keep the same set, replace `features = ["full"]` with
  `features = ["avro", "json", "kafka", "clickhouse", "coordination-nats", "s3"]`.
  Most pipelines need fewer. A Kafka, Avro and ClickHouse pipeline needs
  `features = ["kafka", "clickhouse", "avro"]`, and does not compile the NATS
  and object storage dependencies.
  ([#607])

### Fixed

- **A schema registry that rejects the client stops the pipeline** (`spate-core`, `spate-avro`)

  The Avro deserializer stops the pipeline when the registry answers `401` or
  `403`, or when its certificate fails verification. The error names the registry
  URL and the status or certificate error. In previous versions both were retried
  indefinitely: the batch was held, the source paused, and a warning was the only
  signal. The pipeline stops at the first payload whose schema is not cached,
  whatever the deserializer's `ErrorPolicy`.

  A deserializer reports such a failure with the new `DeserError::Fatal`. The
  chain fails the batch under either policy and counts the payload in
  `spate_deser_records_total{outcome="error"}`, never as a skip-policy drop.
  ([#669])
- **A registry schema that is not Avro no longer holds the batch** (`spate-avro`)

  When the schema registry returns a schema whose type is not `AVRO`, such as
  `PROTOBUF` or `JSON`, the Avro deserializer reports the payload as
  `SchemaUnavailable` and applies its `ErrorPolicy`: `Skip` drops and counts the
  record, and `Fail` stops the pipeline. The id is cached as unusable for
  `negative_cache_ttl`. A subject in `prewarm_subjects` with such a schema is
  skipped. In previous versions such a schema was retried without limit while the
  batch was held. A pre-warmed subject with such a schema was cached, and its
  records decoded with no error when the schema text was also a valid Avro schema.
  ([#873])
- **A TLS alert that rejects the handshake stops the pipeline in the Avro deserializer** (`spate-avro`)

  The Avro deserializer stops the pipeline when the schema registry rejects the
  TLS handshake with an alert such as `handshake_failure` or `protocol_version`,
  and when client and registry share no protocol version, cipher suite or other
  handshake parameter. The error names the registry URL and the alert or the
  mismatch. In previous versions these were retried without limit while the batch
  was held. A registry that requires a client certificate stops the pipeline as
  well, including a TLS 1.3 registry that refuses the client after the
  handshake. That refusal can take a few fetches of the schema to stop the
  pipeline, each after a backoff that starts at 200 ms and doubles up to 5 s. Other alerts, such as `internal_error`, stay transient.
  ([#726])
- **The Avro registry client starts with an empty system trust store** (`spate-avro`)

  The Avro deserializer builds its schema registry client when a host has no CA
  certificates. On Linux and other Unix systems, an `https://` registry is then
  verified against the Mozilla root bundle compiled into the binary, and a warning
  is logged. In previous versions the deserializer panicked at startup on such a
  host, as on a container image without a CA package, even with an `http://`
  registry. A client that cannot be built returns `AvroConfigError::Registry`.
  macOS and Windows verify `https://` registries with the operating system as
  before.
  ([#635])
- **Breaking:** **Truncated datum rejection** (`spate-avro`)

  The value and two-pass serde deserializers reject truncated string bodies,
  boolean fields, and union indexes as malformed. These payloads previously
  could emit records containing null values. They follow the configured Skip
  or Fail record error policy. Pipelines using Fail can stop on payloads
  previously accepted.
  ([#921])
- **Breaking:** **Pending batches per partition** (`spate-core`)

  With `metrics.per_partition_detail: true`,
  `spate_checkpoint_pending_batches` now publishes a pending-batch count for
  each tracked partition. Previously, the setting produced no partition-labeled
  series for this metric. Counts update during regular commit cycles, and a
  partition's series is set to zero after it leaves the assignment. After a
  coordinated source finishes reading a split, extra commit passes update only
  the aggregate. Partition counts can then trail it by up to one commit interval.

  The series without a `partition` label still reports the maximum across
  partitions. Select it with `{partition=""}` when you need that aggregate:
  summing the bare metric name now includes both the maximum and the individual
  counts. Review dashboards and recording rules that use this metric. With the
  setting disabled, only the aggregate is published.
  ([#349])
- **ClickHouse certificate verification failures stop the pipeline** (`spate-clickhouse`)

  A replica certificate that fails TLS verification is `ErrorClass::Fatal`, and
  the error message carries the rustls reason, such as `UnknownIssuer`. At
  startup, the schema check and `distributed_check` fail with that reason. In
  previous versions every such failure read `network error: client error
  (Connect)` and was retryable. A shard kept writing through its other replicas
  while the circuit breaker held back the failing one. Under the default
  `retry.max_attempts: 0`, a shard with no replica left retried for the life of
  the process. A single replica whose certificate stops verifying mid-run now
  abandons the batch, and the pipeline fails once `checkpoint.stalled_fail_after`
  elapses.
  ([#630])
- **Codec errors stop the pipeline** (`spate-clickhouse`)

  The ClickHouse sink classifies the client's `Compression` and `Decompression`
  errors as `ErrorClass::Fatal`. The batch is abandoned on the first failure, the
  partition stalls, and the pipeline exits `Failed` once
  `checkpoint.stalled_fail_after` elapses (120 seconds by default). Previously
  both were retryable, so under the default `retry.max_attempts: 0` the shard
  reattempted the same request for the life of the process while holding its
  in-flight slot, and reported no failure.

  Neither error is reachable from a write on the default `compression: lz4` with
  `clickhouse` 0.15.2, so pipelines on that setting behave the same. The client
  compresses the request body under `lz4`, and the LZ4 encoder has no failure it
  can report. The zstd encoder can report one, so `compression: zstd` is where
  this change takes effect. Decoding failures stay off the write path, because
  the insert response is read uncompressed.
  ([#570])
- **Default compression works on ClickHouse 26.9** (`spate-clickhouse`)

  Under `compression: lz4`, the default, the ClickHouse sink sends
  `network_compression_method=LZ4` on every request, so the server compresses its
  responses with LZ4. Previously a sink on `lz4` failed to start against ClickHouse
  26.9 and later. Those servers compress responses with ZSTD by default, and the
  sink's schema fetch failed with `decompression error: incorrect magic number`.
  The setting changes nothing on servers before 26.9, and `compression: zstd` and
  `compression: off` send nothing new.
  ([#796])
- **ClickHouse verifies `https://` replicas against the system trust store** (`spate-clickhouse`)

  The ClickHouse sink verifies an `https://` replica against the system trust
  store, or against the certificates that `SSL_CERT_FILE` or `SSL_CERT_DIR` name.
  The new `tls.root_ca` setting adds a PEM bundle of private CAs to that store. In
  previous versions the sink trusted only the Mozilla root bundle compiled into
  the binary, so no configuration could reach a replica whose certificate a
  private CA signed. The Mozilla bundle is still used when the system store has
  no certificates. A host whose store has certificates, but not the CA of a
  replica, fails the TLS handshake. Add that CA to the store or to `tls.root_ca`.
  ([#626])
- **A TLS alert that rejects the handshake is fatal in the ClickHouse sink** (`spate-clickhouse`, `spate-core`, `spate-test`)

  The ClickHouse sink fails a write as fatal when the server rejects the TLS
  handshake with an alert such as `handshake_failure`, `protocol_version` or,
  under TLS 1.3, `certificate_required`, and when client and server share no
  protocol version, cipher suite or other handshake parameter. The error names
  the alert or the mismatch. In previous
  versions these were retryable, and the sink retried them without limit under
  the default retry settings. Other alerts, such as `internal_error`, stay
  retryable.

  `spate::error::TLS_REJECTION_ALERTS` lists the alert numbers that count as a
  rejection, for custom connectors to classify with. `spate_test::tls_alert_server`
  starts a local server that answers every TLS client with one chosen alert.
  ([#722])
- **A split lease whose renewal reply was lost is renewed again in the same
  heartbeat** (`spate-coordination`)

  When a split's lease renewal loses its compare-and-swap to this worker's own
  earlier renewal, the worker adopts that renewal and renews the lease again in
  the same heartbeat. When the other store calls in that heartbeat and the one
  before it answer promptly, the lease is renewed before it expires. The
  self-fence counts the adopted renewal from the first renewal that failed since
  the last successful one. In previous versions the next renewal waited for the
  following heartbeat. When the earlier reply was lost to `op_timeout`, that
  renewal could come after the lease expired. The split was then claimed again at
  a new epoch and used up a delivery attempt. The self-fence counted from the
  adoption, so a worker whose watch had not yet reported the expiry kept the split
  for longer after its lease expired.
  ([#919])
- **Slow store reads no longer stop lease renewals** (`spate-coordination`)

  Reconcile listings, the verdict listing and a leader's seeding run beside the
  coordinator's loop, so heartbeats, renewals and commits continue while they
  read. In previous versions they ran inline. A listing or seeding run longer than
  `lease_duration` stopped every renewal, the worker's leases expired and peers
  took its splits, and a leader seeding a large plan lost its leadership partway.
  Reconcile ticks are now jittered, and the first lands anywhere in the first
  `reconcile_interval`, so a fleet started together no longer lists together.
  ([#811])
- **A commit after a commit whose reply was lost stores its own state** (`spate-coordination`)

  When a commit applies but its reply is lost, the next commit for the split may
  carry the same watermark with different resume state. The worker now stores
  that commit's state before it returns success, and a completing commit also
  releases the lease. In previous versions, when both commits completed the
  split or neither did, and the next commit ran before the worker saw the
  earlier one, the next commit returned success while the store kept the earlier
  commit's state. A split that was not complete then resumed from that state
  under its next owner. Sources whose resume state is fully determined by the
  watermark were not affected.
  ([#994])
- **A coordinator dropped while holding splits can hand them back through a store that opens connections** (`spate-coordination`)

  Dropping a coordinator that still holds splits hands them back on a private
  runtime, which now has IO enabled. In previous versions that runtime had timers
  only, so a custom store whose client opens a connection per request panicked in
  `Drop`. The built-in stores were not affected.
  ([#809])
- **A retryable store error during leader election leaves the coordinator running** (`spate-coordination`)

  A newly elected leader whose plan-record read fails with a retryable store
  error, such as a timeout, now gives the leadership back and keeps running, and
  the next election tries again. The read follows a plan-record write that lost
  its compare-and-swap, to another worker's write or to the leader's own earlier
  write whose reply was lost. In previous versions the coordinator stopped on
  that error: every later coordinator call failed, and other workers took its
  splits over once their leases expired.
  ([#884])
- **Failure report after a completed split** (`spate-coordination`)

  A failure report for a split whose completing commit this worker already landed
  hands the lease back and returns `Fenced` without naming a peer. No `Lost`
  event is queued. A completing commit can apply while its reply is lost, so the
  worker still holds the previous revision when the report arrives. The report
  returned `Fenced` for a peer, queued `Lost` and left the lease key in place
  until its TTL expired.
  ([#935])
- **Breaking:** **Failure reports name the tenancy they report**
  (`spate-core`, `spate-coordination`, `spate-test`, `spate-s3`)

  `SplitCoordinator::fail` and `CoordinationDriver::fail` take the `LeaseEpoch`
  of the tenancy being reported. When this instance no longer holds the split
  under that epoch, `SplitCoordinator::fail` returns `Fenced`: it writes nothing,
  uses up no delivery attempt, and emits no `Lost` event. The one exception is a
  report sent again after the same tenancy's earlier report applied and its reply
  was lost. It returns `Ok` and counts in
  `spate_coordination_split_failures_total`, as the first send would have.
  `CoordinationDriver::fail` drops a report for a tenancy this instance no longer
  holds and returns `Ok(())`. Previously a report named only the split. A report
  made for an earlier tenancy (a gain rejected on resume, or poison from the lane
  of a lost tenancy) therefore failed the split's current tenancy. It used up a
  delivery attempt and released the current tenancy's lease. After a rejected
  gain, this instance kept reading the split.

  To upgrade, a source that calls `CoordinationDriver::fail` passes the `epoch`
  of the `SplitOpening` whose lane found the poison. Code that calls
  `SplitCoordinator::fail` directly passes the `epoch` of the
  `CoordinationEvent::Gained` that started the tenancy. A custom
  `SplitCoordinator` compares the epoch with the epoch it holds the split under.
  On a mismatch it returns `Fenced`, writes nothing, and emits no `Lost`.
  `CoordinatorScript::failed` returns `(SplitId, LeaseEpoch, String)`.
  ([#955])
- **A fatal store error after startup stops the coordinator** (`spate-coordination`)

  The coordinator stops with the store's error when a lease renewal, claim,
  assignment write, seed, listing or release fails with an error the store reports
  as fatal. In previous versions only a commit did. Every other path logged the
  error and retried on the next tick, so the worker kept running without doing any
  work.
  ([#809])
- **A new leader does not run the planner over a final plan**
  (`spate-coordination`)

  A worker that takes leadership after the plan is final now fences the plan
  record and assigns the existing splits without running the planner. A
  leadership change therefore adds no splits to a final plan. A new leader over
  an open plan runs the planner as before.

  In previous versions every new leader ran the planner once, including over a
  final plan. That run repeated the source's enumeration: for the S3 source, a
  full listing of the prefix. If objects had been added under the prefix since
  the plan became final, the run could also add a split that grouped them with
  objects already read. The job could then report completion with that split
  unread. When the listing was unchanged, `spate_coordination_replans_total`
  recorded a `noop` run.
  ([#951])
- **A graceful stop hands back leadership and membership** (`spate-core`, `spate-coordination`)

  On a graceful stop, a coordinated source departs its job before the pipeline
  stops its I/O runtime. It hands back its splits, its leadership and its
  presence, even when it holds no split, so peers and later runs take over
  without waiting out a lease. In previous versions the I/O runtime stopped
  first: held splits came back through direct store writes, and the leader and
  presence keys stayed until their lease expired. A graceful rolling restart
  therefore moves work to the remaining instances at once.

  The departure is bounded by `coordination.op_timeout`. The new
  `SplitCoordinator::depart` carries it, and `CoordinationDriver::release` calls
  it. `spate-test`'s `CoordinatorScript::departed` reports whether it ran.
  ([#856])
- **A leader keeps updating its own assignment after an older-generation write to
  its record** (`spate-coordination`)

  When the leader's own `assign.{instance}` record is overwritten at an older
  generation, the leader ignores it as an instruction and caches its revision. The
  next publish to its own key wins. In previous versions the revision stayed
  stale, every later publish to the key lost its compare-and-swap, and the leader
  could not assign itself a split that became available.
  ([#948])
- **A worker's leftover lease no longer quarantines a released split**
  (`spate-coordination`)

  A worker that finds its own live lease on a split whose record names no owner
  now claims the split without using a delivery attempt. Such a lease is left
  behind when a claim's lease write applies but its reply is lost, or when the
  lease delete fails after a release or a failure report cleared the record. In
  previous versions the worker counted that claim as a fast reclaim after a
  restart and used an attempt. A split handed back one attempt below
  `max_attempts` was then quarantined with no new failure, and with
  `max_attempts: 1` a split that had never run could be quarantined. These claims
  now count under the `create` or `reassigned` reason of
  `spate_coordination_acquisitions_total`.
  ([#950])
- **A worker that releases every split it holds stays out of the fleet** (`spate-coordination`)

  A `StoreCoordinator` that hands back its last split through `release` leaves
  the fleet, and its presence key stays deleted while the process keeps running.
  In previous versions its next heartbeat recreated the key. Peers logged
  `peer joined` for it, and the leader kept assigning it splits that it never
  claimed. Those splits waited until the process exited and the key expired.
  ([#861])
- **A departed worker's splits are reassigned when `rebalance_delay` ends** (`spate-coordination`)

  The leader reassigns a departed worker's splits at its first step after
  `rebalance_delay` elapses. In previous versions it also waited for the next
  reconcile tick, adding up to `reconcile_interval` (default 30s) to every
  reassignment after a departure. A failed assignment write is retried on the
  next step, and a deleted assignment record is republished at once; in previous
  versions both could wait for a reconcile.
  ([#809])
- **A coordinator applies the watch events it has already received before it
  acts on them** (`spate-coordination`)

  The coordinator applies up to 257 watch events that are already waiting, then
  runs one pass over the result. A leader writes each member's assignment at most
  once for those events. In previous versions the coordinator ran a full pass
  after each event in turn, so a leader could rewrite one member's assignment once
  per event. On a store with a push watch, such as NATS, a busy fleet could leave
  the leader's view behind the store, and a worker that finished a split waited
  that much longer for its next one.
  ([#958])
- **A release after this worker's own failure report counts as forced**
  (`spate-coordination`)

  When a worker releases a split after its own failure report, and the report did
  not quarantine the split, the release now settles a revocation in progress as
  `forced` and does not count `spate_coordination_releases_total`, whether or not
  the worker had seen the report. A departure counts such a split the same way.
  In previous versions, a release or `release_drained` after a failure report
  whose reply was lost, once the worker had seen the report, counted a release
  and settled the revocation as `drained`.
  ([#917])
- **A failure report sent again after its reply was lost counts once** (`spate-coordination`)

  When a failure report applies but its reply is lost, the call returns a
  retryable error and the report may be sent again. The repeated report now ends
  the tenancy as the first one would have: the lease is deleted, no lost split is
  reported, the call succeeds, and the split is charged one delivery attempt. In
  previous versions the repeated report was reported as fenced, with the split
  lost and its lease left until it expired, or, once the worker had seen the
  first report, was written again and charged a second attempt. That could
  quarantine the split after one failure. When the store's read of the record
  lags the write the report lost to, the repeated report is still reported as
  fenced and the lease expires.
  ([#942])
- **A quarantining failure report sent again after its reply was lost counts once** (`spate-coordination`)

  When a failure report that quarantines the split applies but its reply is lost,
  the call returns a retryable error and the report may be sent again, including
  after further sends that also failed. The repeated report now returns success,
  reports no lost split, counts no fenced loss, and the split is charged one
  delivery attempt. The lease is deleted by the repeated report, or by the next
  heartbeat when the worker has already seen the first report; a worker that
  shuts down before that heartbeat leaves the lease to expire. When the store's
  read of the record lags the write the report lost to, the repeated report is
  still reported as fenced and the lease expires. A quarantine written by another
  worker after this worker's lease expired is still reported as lost. In previous
  versions the repeated report was reported as fenced, with the split lost and
  its lease left until it expired.
  ([#943])
- **A failure report the coordinator answers as retryable is offered again** (`spate-core`)

  When a source hands a split back as failed and the coordinator answers with a
  retryable error, such as a store write that timed out, the report is now kept
  and offered on each poll until the coordinator accepts it, and the split's lane
  is retired then. In previous versions the report was dropped along with any
  later reports in the same batch, and the split stayed held and renewed with no
  failure ever recorded. The S3 source hit this for an object that cannot be
  read. `spate-test`'s `ScriptedCoordinator` gains `fail_next_report` to script
  the answer to a failure report.
  ([#903])
- **A throttled seed write no longer holds back the rest of the plan** (`spate-coordination`)

  When the store rejects a split write with a retryable error, such as
  throttling, the leader pauses briefly and continues the same plan run. Splits
  it has already written become assignable straight away. In previous versions
  the run stopped at the first such error, and the rest of the plan waited for
  the next `replan_interval`, 60 seconds by default, while workers sat idle. A
  run that seeds no split for a whole `replan_interval` while its writes fail
  stops, and the next run retries.
  ([#872])
- **A split lease survives one failed renewal** (`spate-coordination`)

  A coordinated worker renews every split lease it holds at every heartbeat, and
  arms the next heartbeat 0.8 to 1.2 renewal intervals after the last one
  finishes. A lease survives one failed renewal when the worker's store calls from
  its last successful renewal to the retry, the failed call included, take under a
  fifth of `lease_duration` in total. A renewal that times out spends
  `op_timeout`, a third of the lease at the defaults, so it can still cost the
  lease. In previous versions a heartbeat that came less than one renewal interval
  after the last renewal skipped the split. If the next renewal then failed, the
  lease could expire before the following heartbeat, in about two in seven such
  failures at any `lease_duration`. The worker lost the split as `starved` and
  claimed it again at a new epoch, which used a delivery attempt and replayed the
  uncommitted tail. Split-lease renewal writes rise to one per held split per
  heartbeat.
  ([#996])
- **A worker no longer claims a split that finished or ran out of attempts while its view lagged** (`spate-coordination`)

  A worker that takes over a split after its lease vanishes re-reads the split's
  record first, and now acts on what it reads. It leaves a completed or
  quarantined split alone, and parks one that has reached `max_attempts` instead
  of claiming it. In previous versions it could claim such a split anyway when the
  record changed faster than its watch delivered it, emitting `Gained` for finished
  work or running a split past its attempt limit. A takeover whose first write
  loses to the previous owner's late commit now counts its delivery attempt on
  the retry, which previous versions dropped. A worker also ignores leader and
  presence updates older than ones it already holds, and a plan record older than
  the one it read at startup.
  ([#809])
- **A commit made under an ended tenancy no longer lands in the next one**
  (`spate-coordination`)

  A `StoreCoordinator` commit now belongs to the tenancy of the latest `Gained`
  event that `poll` returned for the split. When that tenancy has ended, the
  commit returns `Fenced` and writes nothing, even if this worker has claimed the
  split again since. In previous versions, a commit still queued when a worker
  paused for longer than its lease could be written into the worker's next
  tenancy of the same split. That tenancy started from the progress it read at
  the claim, so its next commit had a lower watermark and failed with a Fatal
  error that named the source. Sources that use `CoordinationDriver` need no
  change.
  ([#1026])
- **Breaking:** **A stop ends coordination waits on an unresponsive store**
  (`spate-core`, `spate-coordination`, `spate-s3`)

  When a stop begins, the S3 source stops waiting on the coordination store and
  the drain starts. A commit tick or revocation commit that is waiting gives up,
  and the final commit sends its positions under its one
  `coordination.op_timeout` budget. A failure report, a revocation hand-back or a
  revocation decline that is waiting gives up the same way. In previous versions
  the stop waited up to three times `op_timeout` for each held split before the
  drain started, so a store that stopped answering shortly before SIGTERM could
  hold the stop past the grace period. A custom coordinated source gets the same
  behavior when its `open` passes the new `SourceCtx::stop` to the new
  `CoordinationDriver::set_stop`.

  A failure report that the stop cuts short before it is sent uses no delivery
  attempt, and the departure hands the split back. A commit or report that was
  already sent may still land after the stop.

  `CoordinationDriver::commit` now returns a retryable error when the store
  defers the commit of any split. The runtime keeps every position of that commit
  pending and sends it again on the next tick or in the final commit. Each such
  tick counts as `error` in `spate_checkpoint_commits_total`, and
  `ExitReport.final_watermarks` lists a deferred position only after a later
  commit stores it, apart from a partition the source no longer holds. In
  previous versions the commit returned `Ok`, the tick counted as `ok`, and the
  exit report could list a position whose commit the store had deferred. An
  alert on the `error` outcome of that counter sees a store outage. A test that
  scripts a `Retryable` commit answer through `spate-test`'s scripted coordinator
  now sees that error from the driver's `commit`.
  ([#969])
- **A split lease that a later claim or release replaced keeps no lane**
  (`spate-coordination`)

  The leader counts a split lease toward its owner's lanes only while the split's
  progress record names that owner, or while the lease is a claim ahead of the
  record. A lease from another owner that the record has moved past, or from a
  released split, no longer holds a lane. In previous versions the leader could
  still see such a lease after the record showed the next claim, because leases
  and records reach the leader separately. Its old owner kept a lane for it, so
  the leader could move the split back to that owner and revoke it from the
  worker that had just claimed it. The old owner could also look full, and the
  leader then revoked one of the old owner's other splits and moved it to another
  worker. No records were lost. A revoked split that could not drain cleanly
  replayed its uncommitted records under the next worker.
  ([#954])
- **A shared `instance_id` is fatal on a split's last attempt** (`spate-coordination`)

  Two live workers that share an `instance_id` now stop with the
  shared-instance-id fatal error when the split they meet on has one delivery
  attempt left. In previous versions the second worker quarantined the split and
  the first dropped it, and neither reported an error, so a bounded job could end
  stalled on a healthy split. The split now stays runnable while the worker that
  holds it is running. On NATS, a worker whose lease-watch snapshot omits the
  holder's lease can still quarantine the split without an error. A worker
  restarted under its predecessor's `instance_id` now quarantines a split on its
  last attempt only after the predecessor's lease expires, as the store or the
  worker measures it. In previous versions it quarantined the split at once.
  Splits with attempts left are reclaimed at once, as before.
  ([#1047])
- **Assigned splits stay with their worker until the claim is seen** (`spate-coordination`)

  The leader keeps a split with the worker it last assigned it to while that
  worker's claim is on its way to the leader, as long as the worker is still a
  member with a free lane. In previous versions the leader treated such a split as
  unowned. When another split completed, it could assign the split to a different
  worker, and a worker that had already claimed it was asked to give it back. On
  a large fleet, or on a store with slow change notification, these
  revocations fed each other, lowered throughput and increased store writes. No
  records were lost. A revoked split that could not drain cleanly replayed its
  uncommitted records under the next worker.
  ([#821])
- **A claim whose record write's reply was lost keeps the split**
  (`spate-coordination`)

  When a claim's record write fails with a retryable error, the worker now reads
  the record back and keeps the split if the record read back shows the claim. In
  previous versions the worker deleted its new lease and then claimed its own
  record again as expired at the next epoch, which used up a delivery attempt.
  ([#915])
- **Breaking:** **A leader keeps its election when its plan-record write's
  reply is lost** (`spate-coordination`)

  A newly elected leader whose write to the plan record applies, but whose
  reply is lost, now reads the record back, finds its own write and keeps the
  leadership. The plan record names the worker that won the election, by its
  `instance_id` and a value unique to the process. In previous versions the
  leader gave the leadership back, and the next election advanced the plan
  generation a second time. No split progress was lost either way.

  A worker on 0.2 stops with a fatal error when it reads a plan record this
  version wrote. The error contains `plan record: not a schema-3 record` and
  names the unknown field `elector`. This version reads plan records that 0.2
  wrote.

  To upgrade, stop every 0.2 worker on a job before starting this version on
  it. Otherwise the remaining 0.2 workers stop once an upgraded worker leads,
  and their splits move to upgraded workers. Records may replay, as after any
  takeover. To return to 0.2, run it under a new job name. A bounded job then
  runs again from the beginning.
  ([#966])
- **A failed election write and a lost leader-key delete are read back** (`spate-coordination`)

  When the write that creates the leader key returns an error or reports a lost
  race, the worker reads the key back and leads if the key holds the value it just
  wrote. When a worker gives up leadership and the delete of the leader key loses
  its compare-and-swap, the worker reads the key back, up to three times, and
  deletes it while it still names this worker. The first heartbeat is now
  scheduled before the worker's first election, so a slow first election no longer
  delays the first renewal of the presence key and the leader key. In previous
  versions the leader key stayed after an election write or a leader renewal whose
  reply was lost, and the fleet had no leader until it expired, up to one
  `lease_duration`. A first election that took most of a lease could also let the
  presence key expire before its first renewal.
  ([#898])
- **A leader keeps leading when a renewal applied but its reply was lost** (`spate-coordination`)

  When a leader's renewal compare-and-swap loses, the leader reads the leader
  key back. If the key still names this worker, the leader renews it again from
  the revision it read, in the same heartbeat. This covers a renewal that reached
  the store while its reply was lost, including a reply lost to `op_timeout`. In
  previous versions the leader gave up leadership while the key still named it,
  and the fleet had no leader until the key expired, up to one `lease_duration`.
  The `leadership renewal fenced; demoting` warning appears only when the
  read-back finds another worker's key or no key.
  ([#887])
- **A commit after a commit whose reply was lost keeps the split** (`spate-coordination`)

  When a commit or a failure report loses its compare-and-swap, the worker now
  reads the split record back. If the record is this worker's own, from an
  earlier commit whose reply was lost, the worker writes again on top of it. For
  a commit, a read older than the write that won, or one that fails with a
  retryable error, returns a retryable error and the worker keeps the split. For
  a failure report, the same read is reported as fenced and the split as lost, as
  in previous versions, unless the record is this worker's own completed one. The
  lease is then released, no lost split is reported, and the report is fenced
  without naming a peer. A commit after this worker's own completing commit
  releases the lease, reports no lost split, and returns success if it completes
  at the same watermark, with its own state stored, and fenced otherwise. In
  previous versions, when the next commit or failure report ran before the watch
  delivered the earlier commit, it was reported as fenced, and the split was
  claimed again at a new epoch, which used up a delivery attempt. A commit with
  the same watermark and completion as the earlier one was the exception and
  returned success. On a store whose watch is polled, split records do
  not arrive on the watch, and these cases are covered the same way.
  ([#901])
- **A release after a write whose reply was lost hands the split back**
  (`spate-coordination`)

  When a release's compare-and-swap loses, the release reads the split record
  back. If the record shows this worker's own earlier commit, the release clears
  the owner on top of it and keeps the committed watermark. If it shows this
  worker's own failure report that did not quarantine the split, the release
  deletes the lease. A split's lease is deleted the same way after a lease renewal
  whose reply was lost, on release, completion and failure; if the lease read
  answers from before that renewal, the next heartbeat reads the lease and deletes
  it again. In previous versions such a release was counted as fenced. After a
  commit, the owner and the lease stayed, so another worker waited out the lease
  and took the split over as expired, which used up a delivery attempt; after a
  failure report, the lease stayed until it expired. A release after a failure
  report that quarantined the split is still counted as fenced and leaves the
  lease until it expires. A forced revocation that ends this way counts
  `spate_coordination_split_losses_total{reason="revoked"}` where it counted
  `reason="fenced"`. A read-back that fails or returns an older record still
  leaves the owner set and deletes the lease; a read-back that finds no record is
  still treated as fenced.
  ([#888])
- **Leader cleanup retries** (`spate-coordination`)

  A worker giving up leadership retries transient leader-key delete failures
  within a bounded cleanup period. Previously, one transient failure could
  leave the key blocking another election until its TTL expired. Persistent
  failures still leave the key to expire. No configuration changes are needed.
  ([#920])
- **A split handed back at teardown keeps its delivery attempts** (`spate-coordination`)

  When a `StoreCoordinator` is dropped while it holds splits, or finds its
  background task gone on `release`, it releases those splits directly in the
  store. A worker that claims one of them during that release consumes no
  delivery attempt. In previous versions the claim could land after the lease was
  deleted but before the owner was cleared. It then counted as a takeover from a
  dead owner and consumed an attempt, and a split one attempt short of
  `max_attempts` was quarantined. Repeated teardowns could quarantine a split that
  never failed.
  ([#762])
- **Chunk buffer recovery after an encoder error** (`spate-core`)

  When `RowEncoder::finish_chunk` returns an error, the pipeline now removes any
  bytes that call appended to the chunk buffer. Previously, those bytes remained
  in the buffer and could be sent before a complete block if another attempt
  succeeded before shutdown. Removing them prevents a failed attempt's partial
  output from corrupting a later frame.

  Custom encoders must retain enough state to finalize the same chunk again,
  including any rows added before the next attempt. The pipeline keeps the row
  count but discards the failed call's output, so an encoder that moved rows into
  the buffer must encode them again. The bundled encoders are unaffected: the
  ClickHouse Native encoder rejects a failed block before writing bytes, and row
  formats use the default `finish_chunk` implementation.
  ([#385])
- **The S3 source's final commit at shutdown ends within `op_timeout`**
  (`spate-core`, `spate-coordination`, `spate-s3`, `spate-test`)

  The S3 source now runs its final commit at shutdown under one
  `coordination.op_timeout` budget for all its splits together, and splits it
  does not reach in that time may replay under their next owner. In previous
  versions that commit waited up to three times `op_timeout` for each split in
  turn, so a store that stopped answering at shutdown could hold the stop past
  the grace period. A custom coordinated source gets the same bound when it
  forwards the new `Source::commit_final` to `CoordinationDriver::commit_final`,
  and `CoordinatorScript::final_commits` in `spate-test` shows whether it does.

  A final commit that leaves splits uncommitted counts as `error` in
  `spate_checkpoint_commits_total`, and `ExitReport.final_watermarks` does not
  list their new positions. In previous versions both reported that commit as
  successful.
  ([#961])
- **First fatal error reported** (`spate-core`)

  When several stages fail within one payload, the pipeline now reports the
  fatal error recorded first. Previously, a later failure in a stage closer to
  the source could replace the original error in the run result and exit report.
  This also applies to split-sink branches and to a deserializer that fails
  after a stage has already recorded a fatal error for the same payload.
  Reporting the error clears the other recorded errors, preventing a later batch
  from failing again on an error left over from that payload.
  ([#369])
- **First shard finalization error reported** (`spate-core`)

  When several shards fail to finalize their chunks during one flush, the
  pipeline now reports the first error encountered in shard processing order.
  Previously, each failure replaced the recorded error, so the run result and
  exit report contained the last shard's error. Shards that finalize successfully
  still send their chunks even if another shard fails. Multiple shard failures
  still contribute at most one fatal error to the stage's
  `spate_operator_errors_total` counter.
  ([#367])
- **Consistent record framing after a decode error** (`spate-s3`, `spate-core`)

  The object framer now preserves completed records when decoding a chunk or
  validating the end of an object fails. Previously, an error could discard
  records completed during that call, making the framer's output depend on how
  the object was split into chunks. Preserving these records keeps record
  indexes consistent when the same byte stream is read again. Source delivery
  is unchanged: the source quarantines a failed object and discards its
  undelivered records.

  Custom `RecordFramer` implementations must check size limits against the
  record accumulated so far and make completed records available before
  returning an error. This keeps both the failure position and the completed
  record sequence independent of chunk boundaries.
  ([#329])
- **A refused poison report no longer outlives its split** (`spate-core`)

  When a source rejects a split's carried progress on resume and the coordinator
  refuses the poison report with a retryable error, the report is re-offered on
  each poll. The queued report is now dropped when the split is lost or
  quarantined. In previous versions it was still offered after the split was
  regained at a new epoch, and the coordinator charged it to the new tenancy: it
  consumed a delivery attempt and released the lease while the lane kept reading
  the split.
  ([#905])
- **Breaking:** **In-flight byte budget usage** (`spate-core`)

  `spate_backpressure_inflight_bytes` now reports the in-flight byte budget's
  current usage, sampled once per controller pass. Previously, it always
  reported zero, even when the pipeline held data in flight. Review alerts and
  dashboards that use this metric because they now receive actual usage values.

  The metric is published once per pipeline with `component="runtime"` and
  `component_type="pipeline"`. Queries grouping the `spate_backpressure_*`
  metrics by component therefore see this additional label set.
  `BackpressureMetrics::set_inflight_bytes` has been removed; remove calls to it
  because the runtime publishes the value automatically.
  ([#336])
- **Int256 values for Decimal256 columns** (`spate-clickhouse`)

  The sink's type check now accepts an `Int256` field for a `Decimal(P, S)`
  column with precision from 39 to 76, including `Decimal256(S)`. Previously,
  the check rejected this documented pairing. This allows rows containing wide
  decimal values to pass schema validation. The row must still supply the value
  scaled by `10^S`; an `Int256` carries no scale information for the checker to
  validate.
  ([#411])
- **Kafka client creation errors no longer print rejected values** (`spate-kafka`)

  A Kafka source or sink startup error names the `rdkafka:` property librdkafka
  rejected and its result code, and does not include the value. In previous
  versions a rejected value, including one interpolated from `${VAR}`, appeared in
  the error twice: once from rdkafka and once quoted in librdkafka's reason.

  librdkafka's reason text is no longer part of the message, so a startup failure
  such as an `enable.idempotence` conflict with `max.in.flight` or `retries`
  reports `Client creation error (librdkafka detail withheld)` and names no
  property.
  ([#906])
- **The Kafka source's assignment deadline error names the last consumer error** (`spate-kafka`)

  When `startup_timeout` or `assignment_timeout` passes, the error that stops
  the pipeline ends with the last consumer error the source saw and how long
  ago it arrived, for example
  `last consumer error 4.976206s ago: consumer poll: … Connection refused`.
  `AllBrokersDown` is skipped, so the error names the broker failure behind it.
  In previous versions the error named only the deadline, the topic and the
  brokers or the group, and the reason was only in the log. An accepted
  assignment clears the recorded error.
  ([#729])
- **Kafka client logs** (`spate-kafka`)

  Kafka sources and sinks forward librdkafka warnings, info and debug lines to
  `tracing` when the subscriber enables them. Set a librdkafka `debug` category
  and `RUST_LOG=info,librdkafka=debug` for detailed diagnostics.
  ([#666])
- **A Kafka partition no longer stops fetching after an assignment** (`spate-kafka`)

  The Kafka source reads the group's committed offsets before it accepts an
  assignment, and starts every partition from a known offset. A partition with no
  committed offset starts where `auto.offset.reset` says. In previous versions a
  pause or resume that landed while librdkafka was starting a partition could
  cancel the start. The partition then sent no records, with no error, until the
  next rebalance. `spate_kafka_source_partition_not_fetching` read 1 for it
  throughout.

  `spate_source_lag_records` appears for a partition as soon as the group has
  committed an offset for it, without waiting for this member's first commit.
  Two cases still take the old path: a partition whose committed offset could
  not be read, which the source logs as a warning, and a partition with no
  committed offset under `auto.offset.reset: error`.
  ([#702])
- **Breaking:** **Kafka metrics follow partition ownership** (`spate-kafka`)

  `spate_kafka_source_partition_fetch_queue_messages` and
  `spate_kafka_source_partition_lag_stored_records` now update only for
  partitions assigned to the current consumer. Their existing series are set
  to zero when a rebalance moves a partition to another consumer.
  Previously, the fetch-queue metric included partitions the consumer had never
  owned, and stored lag could retain an old value after ownership changed.
  Summing stored lag across consumers could therefore count the same partition
  more than once.

  Review dashboards and recording rules that use these metrics. Series for
  previously owned partitions remain present with a value of zero, so counting
  series does not measure the current assignment size. No series is created for
  a partition the consumer has never owned.
  ([#344])
- **The Kafka sink logs each librdkafka error once** (`spate-kafka`)

  The Kafka sink logs one `librdkafka` warning for each error librdkafka
  reports. In previous versions it logged each one twice, so a count of these
  warnings read double. librdkafka's own `FAIL` log line for a broker failure
  still appears beside the warning. The Kafka source is unchanged.
  ([#773])
- **A rejected SASL login or TLS handshake fails the Kafka sink's batch** (`spate-kafka`)

  The Kafka sink fails a batch as fatal when its deliveries time out after a
  broker rejected the connection: a SASL authentication failure, a broker
  certificate the client cannot verify, or a TLS alert such as
  `handshake failure`. The rejection counts for `delivery_timeout` + 35s +
  `reconnect.backoff.max.ms` after it arrives. The `sink write failed` and
  `sink probe failed` warnings end with how long ago the rejection arrived and
  librdkafka's text. The pipeline fails once `checkpoint.stalled_fail_after`
  elapses. In previous versions these deliveries timed out as retryable. Under
  the default retry settings the pipeline kept running with nothing written, and
  the reason was only in the `librdkafka` log lines.
  ([#734])
- **A consumer librdkafka has failed stops the Kafka source** (`spate-kafka`)

  The Kafka source stops the pipeline when librdkafka reports a fatal consumer
  error, such as a static member fenced by another member with the same
  `group.instance.id`. The error carries librdkafka's text and, for a fenced
  member, the configured `group.instance.id`. In previous versions the source
  classified the error as retryable and kept polling a consumer that could no
  longer deliver, with no deadline to end it.

  A fenced member no longer owns its partitions, so records it has not committed
  replay on another member. Its shutdown logs `final commit flush failed; offsets
  will replay` for the same reason.
  ([#768])
- **A rejected TLS handshake stops the Kafka source** (`spate-kafka`)

  The Kafka source stops the pipeline when it cannot verify a broker's
  certificate, or when a broker rejects the TLS handshake with an alert such as
  `handshake failure`, `protocol version` or, under TLS 1.3,
  `certificate required`. The exit report carries librdkafka's text, such as
  `certificate verify failed` or `SSL alert number 40`. In previous versions
  these failures were retryable. A source that could not connect failed only at
  `startup_timeout` or `assignment_timeout`, with an error that did not name
  TLS. Other TLS failures,
  such as a connection reset during the handshake or an `internal error` alert,
  stay retryable.
  ([#729])
- **Kafka rejects OpenSSL-only `builtin.features` flags without TLS** (`spate-kafka`)

  In a build without the `kafka-tls` feature, the Kafka source and sink reject a
  `builtin.features` setting in their `rdkafka` settings that names `ssl`,
  `sasl_scram` or `sasl_oauthbearer`. The check runs when the configuration is
  loaded, and the error asks for a rebuild with `kafka-tls`. In previous versions
  the configuration loaded, and the pipeline failed at startup with a librdkafka
  error saying OpenSSL was not available. Builds with `kafka-tls` are unaffected.
  ([#618])
- **Kafka rejects the unsecured OAUTHBEARER token handler without TLS** (`spate-kafka`)

  In a build without the `kafka-tls` feature, the Kafka source and sink reject
  `enable.sasl.oauthbearer.unsecure.jwt` in their `rdkafka` settings when the
  configuration is loaded. The error asks for a rebuild with `kafka-tls`, as it
  does for other TLS and SASL settings. In previous versions the configuration
  loaded, and the pipeline failed at startup with a librdkafka error saying
  OpenSSL was not available. Builds with `kafka-tls` are unaffected.
  ([#616])
- **Kafka TLS trusts the system CA bundle with vendored OpenSSL** (`spate-kafka`)

  With the `kafka-tls` feature and its default vendored OpenSSL, the Kafka source
  and sink set `ssl.ca.location: probe` when their `rdkafka` settings name
  neither `ssl.ca.location` nor `ssl.ca.pem` and neither `SSL_CERT_FILE` nor
  `SSL_CERT_DIR` is set. librdkafka then loads the first standard CA bundle it
  finds, such as `/etc/ssl/certs/ca-certificates.crt`, and falls back to
  `/usr/local/ssl` when it finds none. In previous versions, on Linux and BSD,
  these builds read only `/usr/local/ssl` and the two variables, so a broker
  certificate signed by a system-trusted CA failed verification. If you placed a
  CA under `/usr/local/ssl`, set `ssl.ca.location` to its path. Probing stops at
  the first bundle, so set `ssl.ca.location` to a directory when that bundle
  lacks your CA. A build linked against the system OpenSSL
  (`OPENSSL_NO_VENDOR=1`) keeps librdkafka's native CA default, and macOS and
  Windows behave as before.
  ([#621])
- **`MemoryStore` keeps reporting lease expiry after the runtime that first used it stops** (`spate-coordination`)

  Expired ephemeral keys reach watchers for the life of the store. In previous
  versions the expiry sweeper ran on the runtime of the first call that touched
  the ephemeral keyspace and stopped with it. Coordinators on other runtimes
  sharing the store then learned about expired leases only from their reconcile
  listings, so a takeover waited up to `reconcile_interval` longer.
  ([#809])
- **Complete counter name list** (`spate-core`)

  `metrics::names::COUNTERS` now includes `spate_sink_drain_overrun_total`.
  Previously, the counter was recorded and documented but missing from this
  list. Code that enumerates counter names through `COUNTERS` can now discover
  it.
  ([#336])
- **Adopted async state bucket warning** (`spate-coordination`)

  The worker warns when a pre-created state bucket uses `persist_mode: async`.
  Acknowledged writes can be lost despite `sync_interval: always`; operators
  should provision production state buckets with default stream persistence.
  Previously, adoption did not report this condition. The worker preserves the
  existing stream settings and continues serving operations.
  ([#934])
- **A rejected NATS credential or certificate stops the pipeline** (`spate-coordination`)

  The NATS coordination store fails the first connection with a fatal error when
  the server rejects the credentials, when the server certificate fails
  verification, or when the server rejects the client certificate. The message
  names the cause, such as `authorization violation` or `UnknownIssuer`. In
  previous versions each of these was retried until the coordinator's startup
  budget ran out, and the pipeline then stopped with the budget error. An
  unreachable server is still retried within that budget.
  ([#708])
- **The NATS state bucket no longer grows with every restart** (`spate-coordination`)

  A key deleted from the NATS coordination store's state bucket leaves a marker
  that the server removes one `lease_duration` later. In previous versions every
  marker stayed, and each coordinator start added at least one, so reconcile
  listings slowed as the bucket grew. On a bucket an earlier version created, a
  worker enables per-message TTLs at startup. This needs the workers'
  credentials to allow `$JS.API.STREAM.UPDATE` on the bucket's stream; without
  it the worker logs a warning and keeps the old behavior. Markers written
  before the upgrade, or by workers not yet upgraded, stay until removed, for
  example with `nats kv compact` while the job is stopped.
  ([#670])
- **NATS store: a guarded delete of an absent key wins** (`spate-coordination`)

  `NatsStore::delete` with an expected revision returns `CasOutcome::Won` when
  the key is absent, whether it was never written, deleted or expired. This
  matches `MemoryStore` and the `CoordinationStore::delete` documentation. In
  previous versions it returned `CasOutcome::Lost`. As a result, a leader
  retried the delete of a departed instance's assignment record on every step
  until a watch event or the reconcile listing dropped it. Custom stores should
  return `Won` in the same case.
  ([#660])
- **NATS listings keep keys rewritten while they run** (`spate-coordination`)

  The NATS coordination store returns every key that holds a value for the whole
  of a listing, including keys rewritten while the listing runs. In previous
  versions, a listing could end without a key whose value was replaced during it,
  such as a lease renewed by its holder. With `replicas` above 1, it could also
  miss a key that a lagging replica still held as deleted. A worker that read such
  a listing treated the live lease as gone: it could drop a split it still held,
  or a peer could quarantine the split at its last delivery attempt. Records were
  not lost; the split's uncommitted records were delivered again and a delivery
  attempt was spent.
  ([#1071])
- **NATS listings and watch snapshots end when their keys expire** (`spate-coordination`)

  The NATS coordination store fails a key listing or a watch snapshot as
  retryable when no message arrives for a quarter of `lease_duration`. The
  reconcile pass and the watch then start again on their own. In previous
  versions, when the last keys a listing had yet to receive expired first, the
  listing waited forever and the worker's coordination loop stopped with it.
  Lease keys expire all the time, so a job with many leases in flight was
  exposed.
  ([#665])
- **The NATS store connects when `servers` lists more than one server** (`spate-coordination`)

  The NATS coordination store adds every entry in `servers` to its connection
  pool. In previous versions it joined the entries into one URL. Most lists with
  two or more entries then failed to parse, and the coordinator retried until its
  startup budget ran out. Some websocket lists parsed as a single URL, and the
  store connected to the first server alone. A `servers` entry that is not a NATS
  URL, or that joins several servers with commas, fails startup with a fatal
  error naming the entry by index. In previous versions it was retried.
  ([#760])
- **A credential or certificate rejected on a reconnect stops the pipeline in the NATS store** (`spate-coordination`)

  The NATS coordination store fails with a fatal error, naming the rejection, once
  every server has failed since the client last connected, each with a rejection
  as its latest failure. That covers a
  rejected credential, a certificate either side rejects, a TLS alert such as
  `HandshakeFailure`, and no handshake parameter in common. In previous versions
  the client retried these for as long as the process ran, and every store
  operation failed retryably meanwhile. A server that refuses or times out the
  connection keeps the store retrying.
  ([#871])
- **The NATS store needs `sync_interval: always` in production** (`spate-coordination`)

  The NATS store page lists `sync_interval: always` in the `jetstream` block of
  every server as a production requirement. The documentation in previous versions
  did not mention it. With the server default, a write is acknowledged before it
  reaches disk, so an OS crash or power loss can lose coordination writes the
  server acknowledged, and a lost write can let a fenced worker commit. Add the
  setting to each server and restart it, because a configuration reload does not
  apply it.
  ([#810])
- **A TLS alert that rejects the handshake stops the pipeline in the NATS store** (`spate-coordination`)

  The NATS coordination store fails the first connection with a fatal error when
  the server rejects the TLS handshake with an alert such as `HandshakeFailure` or
  `ProtocolVersion`, and when client and server share no protocol version, cipher
  suite or other handshake parameter. The message names the alert or the
  mismatch. In previous versions each of these was retried until the
  coordinator's startup budget ran out. Other alerts, such as `InternalError`,
  stay retryable.
  ([#725])
- **NATS `tls.root_ca` adds to the system trust store** (`spate-coordination`)

  The NATS coordination store verifies servers against the system trust store,
  or against the certificates that `SSL_CERT_FILE` or `SSL_CERT_DIR` name, plus
  the PEM bundle in `tls.root_ca`. When the system store has no certificates, the
  Mozilla root bundle takes its place and the store logs a warning. In previous
  versions, setting `root_ca` replaced the system store, so a server signed by a
  system-trusted CA failed verification. If you used `root_ca` to trust only your
  private CA, the system roots are now trusted as well; set `SSL_CERT_FILE` to
  restrict trust for the whole process.

  A TLS connection no longer panics when the build also enables a connector that
  brings rustls's `aws-lc-rs` provider, such as the `s3` or `clickhouse` feature.

  A `root_ca`, `client_cert` or `client_key` file that cannot be read, a client
  certificate and key that do not match, and, when TLS is required, an unreadable
  system trust store now fail the first connection with a fatal error. In
  previous versions an unusable file was retried until the startup budget ran
  out.
  ([#636])
- **`NatsStore::watch` honours any key prefix** (`spate-coordination`)

  A watch on the NATS store delivers every key that starts with its prefix,
  including a prefix that ends partway through a dot-separated segment, such as
  `plan`. It also completes its snapshot at once when no live key has that prefix.
  In previous versions a prefix that did not end in `.` matched only the key equal
  to it, and a watch on a prefix with no live keys failed with "watch snapshot
  stalled" whenever the bucket held other keys. The coordinator watches every key,
  so a pipeline was not affected; code that calls the store's `watch` directly
  was.
  ([#805])
- **Breaking:** **Fatal operator errors and failed flushes** (`spate-core`)

  `spate_operator_errors_total{error_type="fatal"}` now counts fatal errors
  under the `component` label of the stage that stops the pipeline. Previously,
  the `fatal` and `retryable` series always reported zero; only `record_level`
  was updated. The fatal count covers `try_map` failures under
  `ErrorPolicy::Fail`, unmatched split records under `Fail`, and encoder errors
  under `Fail` or classified as `ErrorClass::Fatal`. Each stage instance counts
  at most one fatal error, so a stage's shared series counts how many pipeline
  threads encountered one.

  The metric no longer registers `error_type="retryable"`; queries selecting
  it now return no series. Sums over `spate_operator_errors_total` now include
  fatal errors, so review affected dashboards and recording rules.
  `spate_sink_errors_total` still reports all three error classes, and skipped
  encoder errors still count as `record_level` on the operator metric. Replace
  calls to `OperatorMetrics::errors` with `record_errors` or `fatal_error`, as
  appropriate.

  A failure in `RowEncoder::finish_chunk` now causes the run to fail, including
  during shutdown. Previously, the pipeline could report `Completed` and exit
  with code `0` despite leaving a batch unsent. Unsent records remain eligible
  for replay; the reported run status now reflects the failure.
  ([#353])
- **A rejected listing fails the S3 source at once** (`spate-s3`)

  The S3 source fails the pipeline on the first listing that the store answers
  with `401` or `403`, as it already did for an object read. The error names the
  status. In previous versions the planner retried such a listing on each replan
  tick and failed the pipeline only after eight consecutive failures. A wrong
  credential or a missing list permission took several replan intervals to
  report. Credentials that cannot be fetched, including a `401` or `403` from the
  credential endpoint, are still retried.
  ([#728])
- **A rejected TLS handshake fails the S3 source** (`spate-s3`)

  The S3 source fails the pipeline when the store or the credential endpoint
  rejects the TLS handshake. This covers a certificate that fails verification, an
  alert such as `HandshakeFailure` or `CertificateRequired`, and a server that
  shares no protocol version, cipher suite or other handshake parameter with the
  client. The error names the alert, the mismatch or the verification failure. In
  previous versions a listing was retried until the planner's budget of eight
  attempts ran out, and a read was retried until its split was quarantined. Other
  TLS failures, such as an `InternalError` alert, are still retried.
  ([#735])
- **Stricter JSON validation with simd** (`spate-json`)

  With the `simd` feature enabled, decoding now rejects a NUL byte after a number,
  underscore separators in numbers, and nesting deeper than 1024 levels.
  Earlier `simd-json` versions accepted these malformed numbers and enforced no
  nesting limit. These inputs produce a malformed decode error, so pipelines that
  accepted them may skip the affected payload or fail according to their
  configured error policy.
  ([#400])
- **Breaking:** **A sink failure names the sink and its error** (`spate-core`)

  When a failed sink batch stalls a partition past
  `checkpoint.stalled_fail_after`, the pipeline failure names each sink that
  abandoned a batch and the error that ended its latest one. The drained-exit
  failure of a bounded source does the same. The `abandoning sink batch` log
  line carries the sink and the reason. In previous versions the failure named
  only the partition, and the cause was only in an earlier `sink write failed`
  warning.

  Code that assembles a pipeline by hand must change. `SinkPool::spawn` takes the
  sink's name and a `SinkFailures` register after the pipeline name, and
  `SinkRuntime` has a `failures` field. Create one `SinkFailures`, pass a clone
  to every `SinkPool::spawn`, and put it in `SinkRuntime`. Pipelines built with
  `Pipeline` need no change.
  ([#721])
- **Split branches stop receiving records after a fatal error** (`spate-core`)

  Once a split-sink branch records a fatal error, the split now stops forwarding
  records from that payload to every branch. Previously, other branches could
  continue encoding records and sending chunks before the failure stopped the
  pipeline. The batch still fails and is eligible for replay, but its remaining
  records no longer cause additional writes through sibling branches.

  Those records no longer increase the split's `spate_operator_records_out_total`
  or a branch's `spate_operator_records_in_total`. They also cannot add fatal
  errors in other branches or count as unmatched records through
  `spate_operator_records_dropped_total{reason="unrouted"}` or the split's fatal
  error counter. The split's `spate_operator_records_in_total` still counts
  records it receives.
  ([#370])
- **The coordination store probe checks guarded delete** (`spate-coordination`)

  At startup, a `StoreCoordinator` checks that a delete carrying an expected
  revision loses at a stale revision and wins at the current one, in both
  keyspaces. A store that fails either check stops the start with a fatal error
  naming guarded delete. In previous versions the probe discarded the result of
  its delete, so a custom `CoordinationStore` whose `delete` ignored the expected
  revision started normally. Such a store could remove an assignment, lease or
  leadership record another instance had just rewritten. The NATS and in-memory
  stores pass the check and are unaffected.

  Each start probes under its own key, `_probe.{instance_id}.{nonce}`. Two live
  processes sharing an `instance_id` stop with the duplicate `instance_id` error.
  In previous versions they shared one probe key, and a start could instead fail
  with an error saying the store cannot host coordination. A start that dies
  mid-probe leaves its durable probe key behind, and the coordinator ignores it.
  ([#647])

### Security

- **Config errors no longer print the rejected value** (`spate-core`, `spate-clickhouse`)

  A config error names the field and the type it expected, and does not print the
  value it rejected. In previous versions a value of the wrong type appeared in
  the error message, for example ``invalid type: integer `918273645`, expected a
  string``. Because `${VAR}` interpolation runs before parsing, that value could
  be a secret. This applies to pipeline parse errors, connector config errors and
  the ClickHouse `compression` setting. An unknown enum value is also no longer
  repeated, so a typo reads as `unknown variant, expected …` and the field path
  shows where it is.
  ([#775])
- **Breaking:** **NATS `ws://` servers with TLS configured** (`spate-coordination`)

  `NatsStore::new` now returns a fatal error when `NatsConfig::tls` is set and a
  server in `servers` uses `ws://`. In previous versions this configuration was
  accepted, and the store connected to that server over a plain websocket, so
  credentials and coordination records were sent unencrypted. Change the server
  URL to `wss://`, or remove `tls` if the connection is not meant to use TLS.
  ([#756])
- **Config `Debug` output redacts credentials** (`spate-core`, `spate-avro`, `spate-clickhouse`, `spate-kafka`, `spate-coordination`, `spate-s3`)

  Formatting a config with `{:?}` renders every credential as `<redacted>`. In
  previous versions it printed the Avro registry password, the ClickHouse
  password and `settings` values, every `rdkafka:` value, and every value in a
  `PipelineConfig` connector section. URLs in the registry, ClickHouse replica
  and NATS server settings show `<redacted>` in place of userinfo and of any
  query or fragment. This applies to `Debug` output, ClickHouse startup error
  messages and the ClickHouse `replica` metric label. A dashboard that filters
  on a replica URL carrying userinfo or a query needs the redacted form.

  The public `spate_core::config::redact` module provides the same helpers for
  the `Debug` impl of a custom connector's config.
  ([#763])

### Contributors

- Marcus Kainth
- whiteye

[#322]: https://github.com/spate-etl/spate/pull/322
[#329]: https://github.com/spate-etl/spate/pull/329
[#336]: https://github.com/spate-etl/spate/pull/336
[#338]: https://github.com/spate-etl/spate/pull/338
[#343]: https://github.com/spate-etl/spate/pull/343
[#344]: https://github.com/spate-etl/spate/pull/344
[#349]: https://github.com/spate-etl/spate/pull/349
[#353]: https://github.com/spate-etl/spate/pull/353
[#367]: https://github.com/spate-etl/spate/pull/367
[#369]: https://github.com/spate-etl/spate/pull/369
[#370]: https://github.com/spate-etl/spate/pull/370
[#385]: https://github.com/spate-etl/spate/pull/385
[#400]: https://github.com/spate-etl/spate/pull/400
[#406]: https://github.com/spate-etl/spate/pull/406
[#407]: https://github.com/spate-etl/spate/pull/407
[#411]: https://github.com/spate-etl/spate/pull/411
[#413]: https://github.com/spate-etl/spate/pull/413
[#486]: https://github.com/spate-etl/spate/pull/486
[#491]: https://github.com/spate-etl/spate/pull/491
[#540]: https://github.com/spate-etl/spate/pull/540
[#542]: https://github.com/spate-etl/spate/pull/542
[#570]: https://github.com/spate-etl/spate/pull/570
[#572]: https://github.com/spate-etl/spate/pull/572
[#591]: https://github.com/spate-etl/spate/pull/591
[#607]: https://github.com/spate-etl/spate/pull/607
[#616]: https://github.com/spate-etl/spate/pull/616
[#618]: https://github.com/spate-etl/spate/pull/618
[#621]: https://github.com/spate-etl/spate/pull/621
[#626]: https://github.com/spate-etl/spate/pull/626
[#630]: https://github.com/spate-etl/spate/pull/630
[#635]: https://github.com/spate-etl/spate/pull/635
[#636]: https://github.com/spate-etl/spate/pull/636
[#647]: https://github.com/spate-etl/spate/pull/647
[#650]: https://github.com/spate-etl/spate/pull/650
[#660]: https://github.com/spate-etl/spate/pull/660
[#665]: https://github.com/spate-etl/spate/pull/665
[#666]: https://github.com/spate-etl/spate/pull/666
[#669]: https://github.com/spate-etl/spate/pull/669
[#670]: https://github.com/spate-etl/spate/pull/670
[#696]: https://github.com/spate-etl/spate/pull/696
[#702]: https://github.com/spate-etl/spate/pull/702
[#708]: https://github.com/spate-etl/spate/pull/708
[#720]: https://github.com/spate-etl/spate/pull/720
[#721]: https://github.com/spate-etl/spate/pull/721
[#722]: https://github.com/spate-etl/spate/pull/722
[#725]: https://github.com/spate-etl/spate/pull/725
[#726]: https://github.com/spate-etl/spate/pull/726
[#728]: https://github.com/spate-etl/spate/pull/728
[#729]: https://github.com/spate-etl/spate/pull/729
[#733]: https://github.com/spate-etl/spate/pull/733
[#734]: https://github.com/spate-etl/spate/pull/734
[#735]: https://github.com/spate-etl/spate/pull/735
[#756]: https://github.com/spate-etl/spate/pull/756
[#757]: https://github.com/spate-etl/spate/pull/757
[#760]: https://github.com/spate-etl/spate/pull/760
[#762]: https://github.com/spate-etl/spate/pull/762
[#763]: https://github.com/spate-etl/spate/pull/763
[#768]: https://github.com/spate-etl/spate/pull/768
[#772]: https://github.com/spate-etl/spate/pull/772
[#773]: https://github.com/spate-etl/spate/pull/773
[#775]: https://github.com/spate-etl/spate/pull/775
[#782]: https://github.com/spate-etl/spate/pull/782
[#786]: https://github.com/spate-etl/spate/pull/786
[#791]: https://github.com/spate-etl/spate/pull/791
[#792]: https://github.com/spate-etl/spate/pull/792
[#796]: https://github.com/spate-etl/spate/pull/796
[#805]: https://github.com/spate-etl/spate/pull/805
[#809]: https://github.com/spate-etl/spate/pull/809
[#810]: https://github.com/spate-etl/spate/pull/810
[#811]: https://github.com/spate-etl/spate/pull/811
[#812]: https://github.com/spate-etl/spate/pull/812
[#821]: https://github.com/spate-etl/spate/pull/821
[#856]: https://github.com/spate-etl/spate/pull/856
[#858]: https://github.com/spate-etl/spate/pull/858
[#861]: https://github.com/spate-etl/spate/pull/861
[#862]: https://github.com/spate-etl/spate/pull/862
[#868]: https://github.com/spate-etl/spate/pull/868
[#871]: https://github.com/spate-etl/spate/pull/871
[#872]: https://github.com/spate-etl/spate/pull/872
[#873]: https://github.com/spate-etl/spate/pull/873
[#884]: https://github.com/spate-etl/spate/pull/884
[#887]: https://github.com/spate-etl/spate/pull/887
[#888]: https://github.com/spate-etl/spate/pull/888
[#896]: https://github.com/spate-etl/spate/pull/896
[#897]: https://github.com/spate-etl/spate/pull/897
[#898]: https://github.com/spate-etl/spate/pull/898
[#901]: https://github.com/spate-etl/spate/pull/901
[#903]: https://github.com/spate-etl/spate/pull/903
[#905]: https://github.com/spate-etl/spate/pull/905
[#906]: https://github.com/spate-etl/spate/pull/906
[#915]: https://github.com/spate-etl/spate/pull/915
[#917]: https://github.com/spate-etl/spate/pull/917
[#919]: https://github.com/spate-etl/spate/pull/919
[#920]: https://github.com/spate-etl/spate/pull/920
[#921]: https://github.com/spate-etl/spate/pull/921
[#927]: https://github.com/spate-etl/spate/pull/927
[#934]: https://github.com/spate-etl/spate/pull/934
[#935]: https://github.com/spate-etl/spate/pull/935
[#942]: https://github.com/spate-etl/spate/pull/942
[#943]: https://github.com/spate-etl/spate/pull/943
[#948]: https://github.com/spate-etl/spate/pull/948
[#950]: https://github.com/spate-etl/spate/pull/950
[#951]: https://github.com/spate-etl/spate/pull/951
[#954]: https://github.com/spate-etl/spate/pull/954
[#955]: https://github.com/spate-etl/spate/pull/955
[#958]: https://github.com/spate-etl/spate/pull/958
[#961]: https://github.com/spate-etl/spate/pull/961
[#965]: https://github.com/spate-etl/spate/pull/965
[#966]: https://github.com/spate-etl/spate/pull/966
[#968]: https://github.com/spate-etl/spate/pull/968
[#969]: https://github.com/spate-etl/spate/pull/969
[#977]: https://github.com/spate-etl/spate/pull/977
[#982]: https://github.com/spate-etl/spate/pull/982
[#994]: https://github.com/spate-etl/spate/pull/994
[#996]: https://github.com/spate-etl/spate/pull/996
[#1026]: https://github.com/spate-etl/spate/pull/1026
[#1047]: https://github.com/spate-etl/spate/pull/1047
[#1071]: https://github.com/spate-etl/spate/pull/1071
[#1088]: https://github.com/spate-etl/spate/pull/1088
[#1109]: https://github.com/spate-etl/spate/pull/1109
[#1111]: https://github.com/spate-etl/spate/pull/1111

## [0.2.0] — 2026-08-22

### Added

- **A deadline for a member that never gets its partitions back**
  (`spate-kafka`) — the Kafka source's `assignment_timeout` (default `5m`, `0s`
  disables it) reports a fatal error once the member has held no partitions for
  that long, so a pipeline whose group stops re-forming exits and is restarted
  with a fresh consumer instead of idling at zero ingest while lag climbs. The
  deadline counts from the moment ownership is released, and any accepted
  assignment clears it, an empty one included, so a member in a group with more
  members than partitions keeps running. `startup_timeout` still covers the
  window before the first assignment.
  ([#291])
- **Typed Avro datum decoding** (`spate-avro`) — `AvroDeserializerBuilder::build_datum`
  and `build_serde_datum` return an `AvroDatumDeserializer` that decodes a datum
  straight into a typed record in a single pass, rather than materializing a
  dynamic `Value` first. Reach for it when the record shape is known at compile
  time; the `Value`-based path is unchanged and remains the one to use when it is
  not. ([#31])
- **Synthetic data source** (`spate-datagen`, `spate` feature `datagen`) — a
  built-in storefront dataset of orders, payments and refunds, so a pipeline runs
  with no broker, bucket or coordination store. Each lane owns a disjoint slice
  of the order-id space and its own bounded ring of open orders, so a payment
  always references an order on the same partition at a lower offset with no
  coordination on the record path. `count` drains the pipeline to a clean exit;
  `tick_interval: 0s` runs unthrottled. It keeps no durable progress — a restart
  regenerates the whole stream — and is a demo and test source, not a production
  one.
  ([#146])
- **Nameable `map_rec` bounds** (`spate-core`) — `MapFn` and `TryMapFn`, the two
  traits that carry the bound on `ChainBuilder::map_rec` and `try_map_rec`, are
  exported from `spate_core::ops` (`spate::ops` on the facade). A record family
  that borrows the source buffer transforms through those two methods. Naming
  the bound is what a helper generic over one stage needs.
  ([#272])

### Changed

- **Breaking:** **The admin server has its own `admin:` section** (`spate-core`)
  — `metrics.listen` moves to `admin.listen`, and `MetricsSettings::listen` is
  gone. One server carries `/metrics`, `/healthz` and `/readyz`, so its address
  belongs beside neither the exporter that supplies one of them nor the probes
  that need no exporter at all. `admin: { listen: none }` is new and runs no
  server, which is what lets two pipelines share a host without one of them
  naming a port it never wanted. A file still carrying `metrics.listen` is
  rejected at load, naming the key.

  Three further breaks for anyone below the YAML layer: `PipelineConfig` gains a
  public `admin` field, so an exhaustive struct literal needs `..Default::default()`
  or the new field; `AdminServer::bind` takes `Option<RenderFn>`, where `None`
  serves the probes without `/metrics`; and `MetricsHandle::exports()` is new,
  reporting whether a handle has an exposition to render.
  ([#191])
- **Breaking:** **`apache-avro` moves to 0.22** (`spate-avro`, `spate-datagen`).
  The schema parser is stricter — duplicate union members, duplicate field names,
  duplicate enum symbols, a `record`/`enum`/`fixed` used as a type name, and an
  out-of-range decimal precision are all rejected where 0.21 accepted them — so a
  schema running today can stop compiling on upgrade, surfacing as
  `SchemaUnavailable` per record for a registry id and at build time for a fixed
  schema.

  A `uuid` logical type now carries its backing. A 16-byte `fixed` backing
  decoded to nothing usable and decodes correctly now; a `bytes` backing was not
  recognized as a `uuid` at all, so such a field moves from `AvroValue::Bytes` to
  `AvroValue::Uuid` and a borrowed `&[u8]` target for it stops decoding.
  `AvroValue` is a re-export of `apache_avro::types::Value`, so the
  dynamically-typed path takes that crate's own breaking changes with this bump;
  the typed paths do not.
  ([#248])
- **Writer-schema resolution held across payloads** (`spate-avro`) — `build_value()`
  and `build_serde::<T>()` rebuild the schema's named-type lookup once per writer
  schema id instead of once per record, keeping a reader per id and reusing it. On
  a cross-framework rig whose payloads each carry one record, that is worth 6.9% of
  end-to-end throughput and 4.8% of per-row CPU. A payload carrying a batch
  amortizes the work across its rows already and does not move. Each chain lane
  keeps up to 64 readers and displaces one to admit another, so a stream carrying
  more schema ids than that still decodes and still costs bounded memory.

  `build_datum()` and `build_serde_datum::<T>()` never went through that decoder
  and are unchanged — they remain the throughput path by a wide margin, and are
  still the answer if Avro decode is what bounds your pipeline.
  ([#253])
- **Breaking:** **`breaker.open_for` is validated at load** (`spate-core`,
  `spate-kafka`, `spate-clickhouse`). `breaker.open_for: 0s`, and any value above a
  year, now fail startup. Both previously loaded and ran degenerately — a zero
  quarantine re-probed on the very next check — because the connectors validated
  only `failure_threshold` and `half_open_probes`. A pipeline relying on either has
  to set a real duration.

  `half_open_probes: 0` is unaffected in practice: both connectors already rejected
  it, and what changes is the framework's own normalization of it. The rules now
  live on `BreakerConfig::validate` beside `RetryConfig::validate`, with
  `BreakerConfig::MAX_OPEN_FOR` as the bound and `BreakerConfigError` naming the
  failure — `open_for` is stamped into a deadline, and `Instant + Duration` panics
  rather than saturating, so it needed a load-time limit it had nowhere. ([#35])
- **Breaking:** **The coordination-seam example is `custom_coordinated_source`**
  (`spate`) — the `coordinated_pipeline` example target is gone; run it as
  `cargo run -p spate --features coordination --example custom_coordinated_source`.
  The name says what the example is for: writing a coordination-aware source from
  scratch, as distinct from `s3_coordinated_backfill`, which consumes coordination
  through a connector that already implements it.
  ([`d3eb4a9`](https://github.com/spate-etl/spate/commit/d3eb4a9b76950ccd5c61b12bc6cf3a83bd7a793c))
- **Breaking:** **The JSON example is `json_skip_bad_records`** (`spate`) — the
  `json_ndjson_memory` example target is gone; run it as
  `cargo run -p spate --features json --example json_skip_bad_records`. The name
  says what the example teaches — a malformed record is skipped and counted in
  `spate_json_deser_records_dropped_total` rather than stopping the pipeline —
  instead of the NDJSON-in-memory plumbing it happens to use to show it.
  ([`4338fc5`](https://github.com/spate-etl/spate/commit/4338fc507ed235228d9f3fc517a50caa326f6769))
- **Breaking:** **Connector, sink-pool and coordination configuration structs are
  `#[non_exhaustive]`** (`spate-core`, `spate-kafka`, `spate-clickhouse`,
  `spate-s3`, `spate-coordination`) — from outside the defining crate a struct
  literal, a functional update (`..Default::default()`) and an exhaustive pattern
  all stop compiling. Programmatic assembly starts from `new` where a field has no
  default (`KafkaSourceConfig`, `KafkaSinkConfig`, `ClickHouseSinkConfig`,
  `ShardConfig`, `DistributedCheckSection`, `S3SourceConfig`, and `SinkPoolConfig`
  over its four sections) and from `default()` where every field has one
  (`BatchConfig`, `InflightConfig`, `RetryConfig`, `BreakerConfig`,
  `TimeoutSection`, `CoordinationConfig`), then assigns the fields it wants, which
  stay public. Both sink `Compression` enums, and ClickHouse's `Format` and
  `SchemaValidation`, are sealed too, so a `match` over one needs a wildcard arm.
  Loading a pipeline from YAML is unaffected, and a knob or a codec added after
  this ships in an additive release.
  ([#266])
- **Breaking:** **The framework configuration sections are `#[non_exhaustive]`**
  (`spate-core`) — `PipelineConfig`, `PipelineSection`, `CheckpointSection`,
  `BackpressureSection`, `AdminSection` and `MetricsSection` join the connector
  configs sealed before them, so from outside the crate a struct literal, a
  functional update (`..Default::default()`) and an exhaustive pattern all stop
  compiling. A config built in code starts from `PipelineConfig::new(pipeline,
  source, sink)`, or `PipelineConfig::new_multi_sink(pipeline, source, sinks)`
  for a `sinks:` map, with the `pipeline` section from
  `PipelineSection::new(name)`, the four optional sections from `default()` and a
  deserializer from `PipelineConfig::with_deserializer`, then assigns the fields
  it wants, which stay public. Each of those entry points tags the component it
  takes with its section, so a bad connector body reports the same
  `source.<type>.<key>` error path it reports when the config was loaded from
  YAML. Loading a pipeline from YAML is unaffected, and a section or a key added
  after this ships arrives in an additive release.
  ([#284])
- **Breaking:** **`startup_timeout: 0s` disables the startup deadline**
  (`spate-kafka`) — a zero there leaves the Kafka source waiting for its first
  partition assignment for as long as the group takes, which is what
  `assignment_timeout: 0s` already means for a member that loses one. It
  previously failed the pipeline on the first poll, before any broker could
  answer. The two deadlines are one mechanism with two windows: the wait runs
  from `open` under `startup_timeout`, and from every later loss of the last
  partitions under `assignment_timeout`.
  ([#292])

### Fixed

- **`metrics.exporter: none` no longer takes a port it cannot use**
  (`spate-core`) — the admin server was bound unconditionally, so a pipeline
  configured for no metrics still occupied `0.0.0.0:9090`, and a second one on
  the same host failed to start with an I/O error rather than a message about
  the address. The bind follows `admin.listen`, and a failure to take an address
  reports that address as `StartError::AdminBind`.

  `/metrics` answers 404 where the pipeline has no exposition of its own to
  render, rather than 200 with an empty body — which reads to a scraper as a
  healthy target delivering nothing. That covers `exporter: none` and equally a
  recorder another library installed first, where the pipeline records into that
  recorder and cannot render it.
  ([#191])
- **An out-of-range narrowing Avro promotion is rejected instead of truncating
  silently** (`spate-avro`). A `reader_schema` declaring `int` for a field a
  writer wrote as `long` used to wrap the value: a quantity of `5_000_000_000`
  was delivered as `705_032_704`, with no error, nothing logged and no metric
  moved. The record now fails to decode and takes the deserializer's
  `ErrorPolicy` — dropped and counted on
  `spate_deser_records_dropped_total{reason="skip_policy"}` under `Skip`, fatal
  under `Fail`. A pipeline quietly writing wrong numbers starts reporting them
  instead.

  Only the range is checked, not the direction: a `long` that fits the reader's
  `int` still resolves, `double`→`float` is not checked at all and saturates to
  infinity, and `long`→`float`/`double` loses precision. The Avro connector page
  carries the table.
  ([#248])
- **A sink shard waiting for a circuit-breaker probe no longer consumes its retry
  ladder** (`spate-core`). With every replica half-open and its probe budget spent,
  the write loop fell back to the retry backoff — advancing the ladder while
  publishing no backoff gauge and incrementing no `spate_sink_retries_total`, so
  two batches contending for one probe ended up on different steps for reasons no
  metric explained. The wait now selects on a breaker wake alongside any real probe
  deadline. Reachable on the defaults, where `half_open_probes: 1` and
  `inflight.max_per_shard: 2` put two batches in exactly that contention. ([#34])
- **Coordinator links count toward broker health** (`spate-kafka`) — the
  `spate_kafka_source_broker_up` and `spate_kafka_sink_broker_up` gauges report a
  broker as up while any connection to it is up, including a logical coordinator
  link. A broker serving only as group coordinator previously rendered 0 for the
  life of the process — even while commits flowed through it — because the client
  never reopens a regular connection it has no fetch-reason for, so brokers-up
  panels undercounted after any coordinator-broker outage. ([#197])
- **The `clickhouse_aggregating_mv` example builds on default features**
  (`spate`) — it needs the `clickhouse` feature but declared no
  `required-features`, so `cargo build --examples` against a default-feature
  checkout failed to resolve `spate::clickhouse`. The example now carries a
  `[[example]]` entry gating it, and is built when the feature is enabled rather
  than always attempted.
  ([#137])
- **A peer joining says so, rather than reading as a fault**
  (`spate-coordination`) — an instance's startup probe writes and deletes a key
  in the durable keyspace every peer is already watching, and each of those
  deletes was reported as `durable record deleted externally`, the wording
  reserved for a record vanishing from under the coordinator. The probe key is
  recognized for what it is; a durable record deleted by something outside the
  process still warns.

  Fleet membership is on the log as well as in
  `spate_coordination_live_workers`: `peer joined` and `peer left` on every
  worker with the resulting live count, and `assignment published` on whichever
  worker holds leadership, once per rebalance rather than once per completed
  split. `split claimed`, `drain started` and `drain finished` sit a level below
  at `RUST_LOG=info,spate_coordination=debug`, and `spate_core::telemetry`
  states the level convention the rest of the framework is written to.
  ([#192])
- **A refused resume hands the split back** (`spate-core`) — a coordinated
  source that rejected carried progress from `SplitSource::validate_resume` had
  its gain discarded while the backend kept the lease renewed, so the split was
  held by an instance that never read it and never handed it back. A bounded job
  missing a split that way reaches neither `AllComplete` nor `Stalled`: it waits
  on work nobody is doing. The driver reports a rejected gain to the coordinator
  as poison, which consumes one delivery attempt, releases the lease for a peer,
  and quarantines the split at the attempt cap; a report the backend refuses is
  re-offered until it lands, which `spate_coordination_split_failures_total`
  counts. A rejection also stops discarding the coordination events polled
  alongside it, and a fatal rejection is no longer masked by a retryable one
  raised earlier in the same batch.

  Class a rejection `ErrorClass::Fatal` to end the run, as before. Any other
  class leaves the split to the coordinator and the pipeline keeps going, which
  is what that class always claimed to do.
  ([#193])
- **`validate_schema: full` checks a decimal wrapper's scale** (`spate-clickhouse`)
  — a `Decimal64<4>` field against a `Decimal(18, 2)` column was accepted, because
  the check compared only the wrapper's integer width against the column's
  precision. The widths agree, the insert succeeds, and every row lands 100× too
  large. The decimal wrappers now serialize under a name carrying their scale, so
  a disagreement fails fatally on the first record, before anything is inserted. A
  plain integer field declares no scale and still passes any decimal column of the
  matching width.
  ([#277])
- **`cargo test` compiles without `--all-features`** (`spate`) — the
  `e2e_drain_outage` target was undeclared, so it carried no `required-features`
  and failed on `spate::avro`, `spate::clickhouse` and `spate::kafka` under any
  smaller feature set. Its stanza declares `full`.
  ([#236])
- **A sink shard can no longer be left unwritable by a writer that panicked
  mid-probe** (`spate-core`). A replica's half-open probe budget was returned only
  by leaving `HalfOpen`, so a panic during a probe consumed the slot permanently
  and pinned the replica half-open for the life of the process — every later batch
  for that shard had nowhere to go. Picks now report whether they spent a slot, and
  the write task holds a guard that returns it if no outcome is ever reported. A
  related case let a slot released against an already-ended half-open run credit
  the current one, admitting `half_open_probes + 1` concurrent writes to the
  endpoint the breaker exists to shield; a release naming a run that has ended is
  now discarded. Both are reachable on the default `inflight.max_per_shard: 2`.
  ([#34])
- **A flattened `Nested` column can be written** (`spate-clickhouse`) — under
  ClickHouse's default `flatten_nested = 1` a `Nested` column is stored as
  parallel `outer.inner` columns, and naming one in `columns:` failed the sink's
  identifier check, which rejected any name containing a dot. There was no way
  around it, so a table with a `Nested` column could not be written in either
  format. A column name may now be a dotted path of identifiers. The check is
  otherwise unchanged: a backtick, a space and everything else are still
  rejected, so the name cannot escape its backtick quoting.
  ([#278])
- **`checkpoint.max_pending_batches` is a hard bound** (`spate-core`) — the
  per-partition pending-batch limit is enforced at the poll boundary: a
  partition at the ceiling has its lanes skipped until acknowledgments retire
  batches, so `spate_checkpoint_pending_batches` can no longer overshoot the
  configured value. Enforcement previously ran only on the commit tick, letting
  pending exceed the limit by fetch-rate × `checkpoint.interval` per partition —
  orders of magnitude under a post-rebalance replay. The ceiling is now
  per-partition in effect as well as in name: one partition at its limit no
  longer pauses its siblings, and a partition stalled behind a failed batch
  accumulates at most the configured replay before `stalled_fail_after` fails
  the pipeline. ([#200])
- **Rebalance errors no longer wedge the consumer** (`spate-kafka`) — a rebalance
  event that is neither an assignment nor a revocation is answered with
  `unassign`, as the client library's callback contract requires, instead of only
  being recorded. A member that received one previously stayed mid-rebalance for
  the life of the process — no rejoin, no fresh assignment, commits fenced —
  until a restart. The error is now classified like every other consumer error,
  so an authorization failure fails the pipeline fast while transient rebalance
  codes retry, and the affected lanes are drained through the ordinary
  revocation choreography first; their uncommitted work replays. A warning is
  also logged when rebalance events queue faster than they complete. ([#198])

### Contributors

- Marcus Kainth

[#31]: https://github.com/spate-etl/spate/pull/31
[#34]: https://github.com/spate-etl/spate/pull/34
[#35]: https://github.com/spate-etl/spate/pull/35
[#137]: https://github.com/spate-etl/spate/pull/137
[#146]: https://github.com/spate-etl/spate/pull/146
[#191]: https://github.com/spate-etl/spate/pull/191
[#192]: https://github.com/spate-etl/spate/pull/192
[#193]: https://github.com/spate-etl/spate/pull/193
[#197]: https://github.com/spate-etl/spate/pull/197
[#198]: https://github.com/spate-etl/spate/pull/198
[#200]: https://github.com/spate-etl/spate/pull/200
[#236]: https://github.com/spate-etl/spate/pull/236
[#248]: https://github.com/spate-etl/spate/pull/248
[#253]: https://github.com/spate-etl/spate/pull/253
[#266]: https://github.com/spate-etl/spate/pull/266
[#272]: https://github.com/spate-etl/spate/pull/272
[#277]: https://github.com/spate-etl/spate/pull/277
[#278]: https://github.com/spate-etl/spate/pull/278
[#284]: https://github.com/spate-etl/spate/pull/284
[#291]: https://github.com/spate-etl/spate/pull/291
[#292]: https://github.com/spate-etl/spate/pull/292

## [0.1.0] — 2026-07-27

First public release.

### Added

- **The engine** (`spate-core`) — an operator chain you write in Rust that
  compiles to a single monomorphized loop over zero-copy borrowed records, on
  CPU-pinned processing threads. Checkpoint-driven source commits with
  per-partition contiguity watermarks, sharded and replicated asynchronous
  sinks, backpressure that never blocks a source poll, YAML configuration with
  opaque per-connector sections, and a Prometheus exposition with an admin
  endpoint.
- **Kafka** (`spate-kafka`) — source and producer sink on `rdkafka`. One
  consumer per process, partitions fanned across pipeline threads as zero-copy
  lanes; offsets stored as watermarks advance and committed on an interval.
  Optional TLS and SASL behind a feature that vendors OpenSSL.
- **ClickHouse** (`spate-clickhouse`) — sink with both Native and RowBinary
  encoders, one deduplication-tokened `INSERT` per batch, replica rotation, and
  shard affinity for `Distributed` tables.
- **Object storage** (`spate-s3`) — coordinated backfill source. A leader plans
  a prefix into weighted splits; workers lease them with fenced per-split
  progress, resume is drift-checked against ETag pins, and the job terminates
  itself when the plan completes.
- **Formats** — Avro (`spate-avro`, Confluent wire format with a
  non-blocking schema-registry client) and JSON (`spate-json`, single, NDJSON
  and array framings, with an optional SIMD backend).
- **Coordination** (`spate-coordination`) — leader-computed sticky assignment
  over a pluggable store, with an in-memory backend and a NATS JetStream KV
  one. Cooperative drain on revocation, forced release behind a deadline so one
  wedged drain cannot pin a rebalance open.
- **Testing** (`spate-test`) — in-memory sources and sinks with scripting
  handles, so a pipeline can be tested with no infrastructure at all.

### Delivery semantics

At-least-once. A source watermark is never committed past unacknowledged data,
including across rebalances and shutdown. Records that fail are skipped or fail
the pipeline — there is no policy that drops one without counting it.

Duplicates remain possible after a crash: replay re-batches with new
boundaries, so rows can land twice. Design target tables to tolerate that.

### Known limitations

- Pre-1.0. The API will change, and a breaking change ships in a minor bump.
- Object-storage enumeration is a single listing pass; very large prefixes
  without an inventory feed are the case to watch.
- Connector configuration structs are not yet `#[non_exhaustive]`, so adding a
  field is a breaking change until they are.

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/spate-etl/spate/releases/tag/v0.4.0
[0.3.0]: https://github.com/spate-etl/spate/releases/tag/v0.3.0
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
[0.1.0]: https://github.com/spate-etl/spate/releases/tag/v0.1.0
