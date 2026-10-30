**A dropped S3 lane stops reading its objects** (`spate-s3`)

Dropping an S3 lane ends the task that reads its objects, also while that task waits to retry a failed read. In previous versions the task kept running after its lane was gone, so a read that kept failing went on retrying until it ran out of attempts, after a revocation or after the pipeline stopped. The task holds no acknowledgements, so ending it loses no records and replays none.
