**`SinkPool::drain` returns for any deadline** (`spate-core`)

A `drain` deadline longer than a year counts as a year. Previously a deadline past the clock's range, such as `Duration::MAX`, panicked. A pipeline built with `PipelineBuilder` bounds the deadline itself and is not affected.
