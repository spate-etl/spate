**An S3 source error cuts the store's error body to a bound** (`spate-s3`)

An error from a failed object read or backfill listing carries at most 512 characters of the store's response body, with `…` where the body was cut. The request and the HTTP status ahead of the body are kept whole, and a TLS rejection behind the text is still named in full. In previous versions, such an error, and the log line that reported it, carried the store's whole response body, of any size. The error's class, and what the source does with the object or the listing, are unchanged.
