**A quarantining failure report sent again after its reply was lost counts once** (`spate-coordination`)

When a failure report that quarantines the split applies but its reply is lost,
the call returns a retryable error and the report may be sent again, including
after further sends that also failed. The repeated report now returns success,
reports no lost split, counts no fenced loss, and the split is charged one
delivery attempt. The lease is deleted by the repeated report, or by the next
heartbeat when the worker has already seen the first report; a worker that
shuts down before that heartbeat leaves the lease to expire. When the store's
read of the record lags the write the report lost to, the repeated report is
still reported as fenced and the lease expires. A quarantine written by another
worker after this worker's lease expired is still reported as lost. In previous
versions the repeated report was reported as fenced, with the split lost and
its lease left until it expired.
