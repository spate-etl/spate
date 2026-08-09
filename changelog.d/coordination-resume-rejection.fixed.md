**A refused resume hands the split back** (`spate-core`) — a coordinated
source that rejected carried progress from `SplitSource::validate_resume` had
its gain discarded while the backend kept the lease renewed, so the split was
held by an instance that never read it and never handed it back. A bounded job
missing a split that way reaches neither `AllComplete` nor `Stalled`: it waits
on work nobody is doing. The driver reports a rejected gain to the coordinator
as poison, which consumes one delivery attempt, releases the lease for a peer,
and quarantines the split at the attempt cap; a report the backend refuses is
re-offered until it lands, which `spate_coordination_split_failures_total`
counts. A rejection also stops discarding the coordination events polled
alongside it, and a fatal rejection is no longer masked by a retryable one
raised earlier in the same batch.

Class a rejection `ErrorClass::Fatal` to end the run, as before. Any other
class leaves the split to the coordinator and the pipeline keeps going, which
is what that class always claimed to do.
