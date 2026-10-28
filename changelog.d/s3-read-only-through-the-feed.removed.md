**Breaking:** **`spate-s3` reads only through a feed** (`spate-s3`)

The S3 source reads work units and storage notifications from the feed that the pipeline's `feed:` section names. A pipeline with an S3 source and no `feed:` section fails to start, and the error names the section to add. `feed: { memory: {} }` runs the source in one process, which lists its own prefix. That feed keeps nothing across a restart, so a restart lists and reads the whole prefix again.

`S3Source::with_coordinator` and the `refresh_listing` source key are removed. A source file that sets `refresh_listing`, to `false` as well, fails to load: remove the key. `split_target_bytes` remains: objects at or below it are packed into units of at most 16 objects, larger ones that qualify for byte ranges are cut into ranges of at most it, and any other is one unit. A re-listing of an open prefix has no replacement: a continuous job reads storage notifications, and a bounded job lists once.

`split_id_for` and `split_id_for_range` now return `Result<String, SplitError>`, and `SplitDescriptor::encode` and `SplitDescriptor::decode` return `SplitError` in place of `CoordinationError`. `SplitError` carries the reason in its `reason` field. The ids they mint are unchanged.

The `spate_s3_source_objects_listed_total` counter is removed, since nothing increments it. Remove it from dashboards and alerts.
