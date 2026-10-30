**Avro decoding hashes schema ids and names with a seeded quality hasher** (`spate-avro`)

The schema cache, the deserializer's held readers, the registry fetcher and the named-type index hash their keys with the quality variant of a seeded hasher. In previous versions they used std's SipHash. On the decode benchmarks a schema with recursive named references costs about 20–24% fewer instructions, and a payload whose schema id is cached as failed, such as an id the registry does not know, costs about 20% fewer. Resolving against a reader schema costs up to 2% fewer.
