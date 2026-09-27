**The NATS store connects when `servers` lists more than one server** (`spate-coordination`)

The NATS coordination store adds every entry in `servers` to its connection
pool. In previous versions a list with two or more entries never connected, and
the coordinator retried until its startup budget ran out. A `servers` entry
that is not a NATS URL, or that joins several servers with commas, fails
startup with a fatal error naming the entry by index. In previous versions it
was retried the same way.
