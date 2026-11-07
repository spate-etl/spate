**A refused S3 read classifies by the store's status whatever the key holds** (`spate-s3`)

A notification unit's read is treated as an overwrite on a `416` only when the store answered with that status. In previous versions a `403` or `404` read of a key holding the text `Server returned non-2xx status code: 416 `, or a `503` whose response body held it, was taken for a `416`. A `403` then left the run going and failed the unit back to the feed instead of failing the pipeline, and a `404` could be reported as an overwrite.
