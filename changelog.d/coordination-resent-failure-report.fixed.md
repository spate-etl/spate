**A failure report sent again after its reply was lost counts once** (`spate-coordination`)

When a failure report applies but its reply is lost, the call returns a
retryable error and the report may be sent again. The repeated report now ends
the tenancy as the first one would have: the lease is deleted, no lost split is
reported, the call succeeds, and the split is charged one delivery attempt. In
previous versions the repeated report was reported as fenced, with the split
lost and its lease left until it expired, or, once the worker had seen the
first report, was written again and charged a second attempt. That could
quarantine the split after one failure. When the store's read of the record
lags the write the report lost to, the repeated report is still reported as
fenced and the lease expires.
