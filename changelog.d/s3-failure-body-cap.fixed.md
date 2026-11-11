**S3 source and an oversized error answer** (`spate-s3`)

The S3 source now reads at most 64 KiB of the body of a store's failure answer. In previous versions the client read the whole body into a buffer sized from the answer's declared `Content-Length`. An answer declaring a very large length made the process panic or abort with no error naming the store, and an answer with a body of hundreds of megabytes could exhaust the process's memory. The error the source reports is unchanged: it carries the status and the first 512 characters of the body. A successful listing page is still read whole.
