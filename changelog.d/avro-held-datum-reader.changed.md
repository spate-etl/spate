**Writer-schema resolution held across payloads** (`spate-avro`) — `build_value()`
and `build_serde::<T>()` rebuild the schema's named-type lookup once per writer
schema id instead of once per record, keeping a reader per id and reusing it. On
a cross-framework rig whose payloads each carry one record, that is worth 6.9% of
end-to-end throughput and 4.8% of per-row CPU. A payload carrying a batch
amortizes the work across its rows already and does not move. Each chain lane
keeps up to 64 readers and displaces one to admit another, so a stream carrying
more schema ids than that still decodes and still costs bounded memory.

`build_datum()` and `build_serde_datum::<T>()` never went through that decoder
and are unchanged — they remain the throughput path by a wide margin, and are
still the answer if Avro decode is what bounds your pipeline.
