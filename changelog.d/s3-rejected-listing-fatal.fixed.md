**A rejected listing fails the S3 source at once** (`spate-s3`)

The S3 source fails the pipeline on the first listing that the store answers
with `401` or `403`, as it already did for an object read. The error names the
status. In previous versions the planner retried such a listing on each replan
tick and failed the pipeline only after eight consecutive failures. A wrong
credential or a missing list permission took several replan intervals to
report. Credentials that cannot be fetched, including a `401` or `403` from the
credential endpoint, are still retried.
