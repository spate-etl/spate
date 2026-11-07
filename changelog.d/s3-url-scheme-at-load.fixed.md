**Breaking:** **The S3 source refuses a URL scheme it cannot read at pipeline load** (`spate-s3`)

A `source.s3.url` outside `s3://`, `s3a://`, `file://` and `https://` on an S3 host, such as `gs://`, `az://` or `https://` on another host, fails pipeline load with an error that names `source.s3.url` and says how to reach an S3-compatible store. In previous versions it loaded and failed when the source started, unless the binary enabled the `object_store` `gcp` or `azure` feature itself, in which case the source read that store.

A `source.s3.url` whose prefix has an empty segment, a control character or bytes that are not UTF-8 also fails pipeline load.
