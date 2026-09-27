**The NATS store connects when `servers` lists more than one server** (`spate-coordination`)

The NATS coordination store adds every entry in `servers` to its connection
pool. In previous versions it joined the entries into one URL. Most lists with
two or more entries then failed to parse, and the coordinator retried until its
startup budget ran out. Some websocket lists parsed as a single URL, and the
store connected to the first server alone. A `servers` entry that is not a NATS
URL, or that joins several servers with commas, fails startup with a fatal
error naming the entry by index. In previous versions it was retried.
