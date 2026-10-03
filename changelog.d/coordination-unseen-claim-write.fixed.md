**A claim whose record write's reply was lost keeps the split**
(`spate-coordination`)

When a claim's record write fails with a retryable error, the worker now reads
the record back and keeps the split if the record read back shows the claim. In
previous versions the worker deleted its new lease and then claimed its own
record again as expired at the next epoch, which used up a delivery attempt.
