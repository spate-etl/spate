**A source that fails to open is not asked to flush its commits** (`spate-core`)

A run whose source fails to open logs only the open failure. Previously it also logged `final commit flush failed; offsets will replay`, though nothing had been read. The Kafka source's flush reported `source used before open()` there.
