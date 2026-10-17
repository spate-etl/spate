**An unrecognized `store` key fails at load** (`spate-s3`)

For an `s3://` URL, a key under `store` that `object_store` does not recognize as an S3 option fails pipeline load, and the error names the key. In previous versions, the source skipped the key without a message. A misspelled `endpoint` therefore built a client for the default AWS endpoint and sent the configured credentials there. Keys under a `file://` URL are not checked.
