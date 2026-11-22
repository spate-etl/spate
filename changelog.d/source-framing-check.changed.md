**Breaking:** **A pipeline does not start when a deserializer refuses the source's framing** (`spate-core`, `spate-test`)

`Deserializer` and `RunnableChain` have a new method with a default, `check_source_framing`. The runtime calls it once per chain when the pipeline starts, with the source's `FramingContract`. When it refuses, `run` returns `StartError::Config` with the refusal, before the source opens. The default accepts every contract, so a deserializer that keeps it starts as in previous versions. A deserializer that splits one payload into several records should refuse `FramingContract::PerRecord`, and a deserializer that wraps another should forward the method to the inner one.

`TestDeserializer::split_on` now refuses a `PerRecord` source, such as the S3 or datagen source, so a pipeline that pairs them fails at start. In previous versions it split each payload the source had already framed. Use `TestDeserializer::passthrough` under such a source.
