**The Avro deserializer's refusals put other keys in backticks** (`spate-avro`)

A `registry.password` with no `registry.username`, a `registry.tls.root_ca` with an `http://` `registry.url`, and a `registry.url` carrying userinfo now name the other keys in backticks, such as ``deserializer.avro.registry.url: carries userinfo; set them in `registry.username` and `registry.password` ``. A missing `schema` in mode `raw` or `single_object` reads ``deserializer.avro.schema: is required in mode `raw` (`inline` or `path`)``. The same values are refused as before.
