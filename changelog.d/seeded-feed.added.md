**A feed filled in code can be passed as seeded, and `MemoryFeed` names its unsettled units** (`spate-core`)

`Pipeline::seeded_feed` and `PipelineRuntime::with_seeded_feed` pass a feed that code filled before the run and does not publish into afterwards. The source receives it as the new `FeedOrigin::Seeded`, which tells it that the feed holds the run's whole input. A feed passed to `Pipeline::feed` still arrives as `FeedOrigin::Given`.

`MemoryFeed::unsettled` returns the units that are neither acknowledged nor dead-lettered. Together with `MemoryFeed::dead_letters`, it names every unit a run did not finish. A run stopped by a shutdown still exits `ExitState::Completed`, so a caller that reports each unit as done or not reads these two lists. An id can appear more than once, because a source may re-enqueue a unit under its own id.
