**Breaking:** **Failure reports name the tenancy they report**
(`spate-core`, `spate-coordination`, `spate-test`, `spate-s3`)

`SplitCoordinator::fail` and `CoordinationDriver::fail` take the `LeaseEpoch`
of the tenancy being reported. When this instance no longer holds the split
under that epoch, `SplitCoordinator::fail` returns `Fenced`: it writes nothing,
uses up no delivery attempt, and emits no `Lost` event. The one exception is a
report sent again after the same tenancy's earlier report applied and its reply
was lost. It returns `Ok` and counts in
`spate_coordination_split_failures_total`, as the first send would have.
`CoordinationDriver::fail` drops a report for a tenancy this instance no longer
holds and returns `Ok(())`. Previously a report named only the split. A report
made for an earlier tenancy (a gain rejected on resume, or poison from the lane
of a lost tenancy) therefore failed the split's current tenancy. It used up a
delivery attempt and released the current tenancy's lease. After a rejected
gain, this instance kept reading the split.

To upgrade, a source that calls `CoordinationDriver::fail` passes the `epoch`
of the `SplitOpening` whose lane found the poison. Code that calls
`SplitCoordinator::fail` directly passes the `epoch` of the
`CoordinationEvent::Gained` that started the tenancy. A custom
`SplitCoordinator` compares the epoch with the epoch it holds the split under.
On a mismatch it returns `Fenced`, writes nothing, and emits no `Lost`.
`CoordinatorScript::failed` returns `(SplitId, LeaseEpoch, String)`.
