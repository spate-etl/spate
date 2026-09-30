**A graceful stop hands back leadership and membership** (`spate-core`, `spate-coordination`)

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
