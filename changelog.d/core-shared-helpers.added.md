**`spate_core::error::chain`, `RateLimit::hot_path` and `spate_core::rng::SplitMix64` for connectors** (`spate-core`)

`chain(err)` renders an error and each error in its source chain, joined by `": "`. `RateLimit::hot_path()` is the limiter for a warning on the record path, five events per ten seconds, and is `const` so it can initialize a `static`. `SplitMix64::new(seed)` is a seeded generator whose `next_u64` yields the same stream for the same seed on every platform. It is not cryptographic. All three are also available under `spate::`.
