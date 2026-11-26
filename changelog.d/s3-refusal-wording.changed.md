**Three S3 source refusals follow the wording other connectors use** (`spate-s3`)

`chunk_bytes: 0` now reads `source.s3.chunk_bytes: must be greater than zero`, where it read `must not be zero`, the `prefetch_bytes` refusal names `chunk_bytes` in backticks, and an `s3://` feed `location` refused for a `store` key the S3 client does not know names that key in backticks, such as ``feed: `location` s3://ops/jobs/a/: `source.s3.store."endpont"` is not an object_store S3 option, …``. The same values are refused as before.
