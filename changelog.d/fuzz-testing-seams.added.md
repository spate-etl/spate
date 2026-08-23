**Fuzz seams for the private decoders** (`spate-s3`, `spate-coordination`) — the
off-by-default `testing` feature carries a `fuzz_seams` module in each crate,
exporting the composite offset codec, the object framer with its gzip and zstd
decompression, and the coordination store's record and key parsers as
functions. The workspace's `fuzz/` harness drives them from three libFuzzer
targets. The feature stays outside the semver surface and the `spate` facade
never enables it, so a crate that does not ask for it sees no change.
