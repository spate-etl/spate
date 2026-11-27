**Breaking:** **A JSON deserializer decodes with simd-json only when its builder asks for it** (`spate-json`, `spate-kafka`, `spate`)

`JsonDeserializerBuilder::with_simd_json` makes the deserializers built from that builder decode with `simd-json`. Every other deserializer decodes with `serde_json`, in any build. The method needs the `simd-json` feature of `spate-json`, which the `json-simd` feature of the `spate` facade turns on. In previous versions that feature was named `simd` on `spate-json`, and it switched every JSON deserializer in the build to `simd-json`.

To keep decoding with `simd-json`, call `.with_simd_json()` on the builder of each deserializer that should use it. A build that turns on `json-simd` and makes no call decodes with `serde_json`, which is faster than `simd-json` on number-heavy documents. The JSON connector guide's Backends section compares the two parsers.

`spate_json::BACKEND_ID` and `spate::json::BACKEND_ID` are removed, because a build no longer has one parser. The `json-simd` feature of `spate-kafka` is renamed `simd-json`, and the `kafka-json-simd` feature of the `spate` facade is unchanged.
