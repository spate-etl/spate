**First fatal error reported** (`spate-core`)

When several stages fail within one payload, the pipeline now reports the
fatal error recorded first. Previously, a later failure in a stage closer to
the source could replace the original error in the run result and exit report.
This also applies to split-sink branches and to a deserializer that fails
after a stage has already recorded a fatal error for the same payload.
Reporting the error clears the other recorded errors, preventing a later batch
from failing again on an error left over from that payload.
