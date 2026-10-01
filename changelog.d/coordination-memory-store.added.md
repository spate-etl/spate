**A solo run takes its coordinator tuning from the file** (`spate-coordination`, `spate-s3`)

`coordination: { store: { memory: {} } }` selects the in-process coordination
store, and the section's tuning then applies to a solo run. Set `max_in_flight`
there to raise the S3 source's read parallelism, which is one lane per
in-flight split and defaults to 8. In previous versions a solo S3 run always
used the default tuning, and only a coordinator built in code and passed to
`S3Source::with_coordinator` could change it. The in-process store is never
shared between processes: progress is lost when the process exits, and each
instance configured with it reads the whole input.
