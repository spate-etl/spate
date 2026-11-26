**Two S3 source refusals follow the wording other connectors use** (`spate-s3`)

`chunk_bytes: 0` now reads `source.s3.chunk_bytes: must be greater than zero`, where it read `must not be zero`, and the `prefetch_bytes` refusal names `chunk_bytes` in backticks. The same values are refused as before.
