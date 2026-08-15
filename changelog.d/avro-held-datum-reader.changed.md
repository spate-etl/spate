**Avro decoding no longer re-resolves the writer schema on every payload**
(`spate-avro`). `build_value()` and `build_serde::<T>()` rebuilt the schema's
named-type lookup for each record they decoded; the deserializer now keeps a
reader per writer schema id and reuses it. Worth 6.9% end-to-end throughput and
4.8% of per-row CPU on the cross-framework rig, with nothing to change in a
pipeline to get it.

It shows where one payload carries one record. A payload carrying a batch
already amortized that work across its rows and does not move.
`build_datum()` and `build_serde_datum::<T>()` never went through that decoder
and are unchanged — they remain the throughput path by a wide margin, and are
still the answer if Avro decode is what bounds your pipeline.
