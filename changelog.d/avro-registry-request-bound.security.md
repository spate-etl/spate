**The Avro registry fetcher bounds its queue of requested schema ids** (`spate-avro`)

A schema id that is waiting for a fetch is queued once, and at most 1,024 ids wait at a time. In previous versions, every replay of a payload whose schema was not cached added a request to an unbounded queue, and the fetcher stopped reading that queue while four fetches were running. While a registry held its requests open, payloads naming several distinct unknown schema ids grew the queue until the process ran out of memory. No action is needed.
