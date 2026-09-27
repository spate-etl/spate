**Breaking:** **A sink failure names the sink and its error** (`spate-core`)

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
