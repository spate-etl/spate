**Split coordination is deprecated** (`spate`, `spate-coordination`, `spate-core`, `spate-s3`, `spate-test`)

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
