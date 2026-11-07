**Breaking:** **The S3 source refuses credentials in its URL or store endpoint at pipeline load** (`spate-s3`)

A `source.s3.url` carrying userinfo or a query, or a `store` endpoint carrying userinfo, fails pipeline load with an error that names the field, keeps the password out, and says credentials go under `store` or the environment. In previous versions such a password loaded, was never sent, and could appear in the error a failed run returned and in its log.
