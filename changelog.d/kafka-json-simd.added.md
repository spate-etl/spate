**Opt-in `simd-json` encoding for the Kafka sink** (`spate-kafka`)

With feature `kafka-json-simd` on the `spate` facade, `KafkaJsonEncoder::with_simd_json` serializes a sink's records with `simd-json`, which checks string bytes 16 or 32 at a time. It spends fewer instructions than `serde_json` on records that carry long text, and more on records of short strings and numbers. Only a sink whose encoder calls it changes; every other sink keeps `serde_json`.

The output is valid JSON that decodes to the same value, with these differences: an `f32` is written as the `f64` it widens to, an exponent carries no `+`, a map with `bool`, `f32`, `f64` or `Option` keys fails the record, and an error carries `simd-json`'s message. A record type that holds `serde_json::value::RawValue`, or a `serde_json::Value` while `serde_json`'s `arbitrary_precision` feature is on, is written wrong with no error, so such a type stays on the default encoder.
