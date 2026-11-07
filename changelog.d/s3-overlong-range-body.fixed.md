**An S3 ranged read stops at the range it asked for** (`spate-s3`)

A ranged read whose response body runs past the requested range fails the object as undecodable once the body passes the range, and the error names the key, the offset and the requested length. In previous versions the fetcher buffered the whole body and handed the extra bytes on as object content, so a store that answered outside the protocol could break the read-ahead memory bound and corrupt the records framed from that window. A conforming store sends exactly the range and sees no change.
