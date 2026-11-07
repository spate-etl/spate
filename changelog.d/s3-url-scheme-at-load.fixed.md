**The S3 source refuses a URL scheme it cannot read at pipeline load** (`spate-s3`)

A `source.s3.url` outside S3 URLs and `file://`, such as `gs://`, `az://` or a non-S3 `https://`, fails pipeline load with an error that names `source.s3.url` and the schemes the source reads. Previously it loaded and failed when the source started.
