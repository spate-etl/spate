**S3 listing whose continuation token does not advance** (`spate-s3`)

An S3 lister run now fails with a fatal error when the store answers a listing page with the continuation token the request carried. In previous versions the client asked for the same page without end: the lister published and read the same objects again, held a CPU core and never finished. Only a defective store, a proxy in front of one, or a rewritten plaintext connection answers a listing this way. The supported stores start each page past the last key of the one before.
