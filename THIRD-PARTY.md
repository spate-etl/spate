# Third-party licenses

**Generated file. Do not edit.** Regenerate with:

```sh
./scripts/attribution.sh
```

CI regenerates this and fails on any diff, so it cannot drift from `Cargo.lock`.

Spate itself is licensed under Apache-2.0 (see [LICENSE](LICENSE)). This file
inventories the **dependencies** it links, and the terms each is used under.
Where a dependency offers a choice (`MIT OR Apache-2.0` and similar), the term
listed here is the one elected. The priority order in `about.toml` decides, and
the rationale is documented there.

When reading it:

- **This is the union across every optional feature** (`--all-features`). A
  default build links a strict subset; the extra crates come from opt-in
  features such as `kafka-tls`, `coordination-nats` and `json-simd`. Attributing
  more than you distribute is safe, so this file is the superset.
- **Full license texts are not reproduced here**, to keep the file diffable.
  They are published at <https://spate.kainth.dev/licenses/>, generated
  from the same lockfile by the same tool.

Dev-dependencies are excluded: they are never distributed and so require no
attribution. Build-dependencies are included, since a build script can
contribute generated code to the shipped artifact, as can a proc-macro, so
compile-time-only crates are listed.

Spate's **own** crates (`spate`, `spate-core`, …) appear in the table too, each
under Apache-2.0. `cargo-about` only omits workspace members that are
unpublished. Every crate you receive from this project carries the same terms as
the repository.


## Summary

| License | Crates |
|---|---|
| `MIT` | 308 |
| `Apache-2.0` | 23 |
| `Unicode-3.0` | 19 |
| `ISC` | 6 |
| `BSD-3-Clause` | 5 |
| `CDLA-Permissive-2.0` | 2 |
| `Zlib` | 1 |

## Crates

One row per crate, grouped by the license elected for it and sorted by name
within each group. `cargo-about` records each crate's own license text
separately, since the copyright lines differ, so the full texts live on the site
rather than here.

This table is post-processed. `cargo-about` emits one row per license *text*,
grouping crates by which text they share, and a crate that ships several files
scanning as the same license has its text chosen by the order those files are
read off disk. Group membership, and so each row's position, then varies from
machine to machine. `scripts/attribution.sh` sorts the rows, collapses the
duplicates a crate with several notices produces, and recomputes the counts
above from what is left. The elected license ids are unaffected either way.

| Crate | Version | License |
|---|---|---|
| `apache-avro` | 0.22.0 | `Apache-2.0` |
| `async-nats` | 0.49.1 | `Apache-2.0` |
| `aws-lc-sys` | 0.42.0 | `Apache-2.0` |
| `bytesize` | 2.4.2 | `Apache-2.0` |
| `clang-sys` | 1.8.1 | `Apache-2.0` |
| `dunce` | 1.0.5 | `Apache-2.0` |
| `metrics-exporter-prometheus` | 0.18.3 | `Apache-2.0` |
| `nkeys` | 0.4.5 | `Apache-2.0` |
| `nuid` | 0.5.0 | `Apache-2.0` |
| `ring` | 0.17.14 | `Apache-2.0` |
| `ryu` | 1.0.23 | `Apache-2.0` |
| `sketches-ddsketch` | 0.3.1 | `Apache-2.0` |
| `spate` | 0.1.0 | `Apache-2.0` |
| `spate-avro` | 0.1.0 | `Apache-2.0` |
| `spate-clickhouse` | 0.1.0 | `Apache-2.0` |
| `spate-coordination` | 0.1.0 | `Apache-2.0` |
| `spate-core` | 0.1.0 | `Apache-2.0` |
| `spate-datagen` | 0.1.0 | `Apache-2.0` |
| `spate-json` | 0.1.0 | `Apache-2.0` |
| `spate-kafka` | 0.1.0 | `Apache-2.0` |
| `spate-s3` | 0.1.0 | `Apache-2.0` |
| `spate-test` | 0.1.0 | `Apache-2.0` |
| `sync_wrapper` | 1.0.2 | `Apache-2.0` |
| `aws-lc-sys` | 0.42.0 | `BSD-3-Clause` |
| `bindgen` | 0.72.1 | `BSD-3-Clause` |
| `curve25519-dalek` | 4.1.3 | `BSD-3-Clause` |
| `ed25519-dalek` | 2.2.0 | `BSD-3-Clause` |
| `subtle` | 2.6.1 | `BSD-3-Clause` |
| `webpki-roots` | 0.26.11 | `CDLA-Permissive-2.0` |
| `webpki-roots` | 1.0.8 | `CDLA-Permissive-2.0` |
| `aws-lc-rs` | 1.17.1 | `ISC` |
| `aws-lc-sys` | 0.42.0 | `ISC` |
| `libloading` | 0.8.9 | `ISC` |
| `ring` | 0.17.14 | `ISC` |
| `rustls-webpki` | 0.103.13 | `ISC` |
| `untrusted` | 0.9.0 | `ISC` |
| `adler2` | 2.0.1 | `MIT` |
| `aho-corasick` | 1.1.4 | `MIT` |
| `aliasable` | 0.1.3 | `MIT` |
| `allocator-api2` | 0.2.21 | `MIT` |
| `anstyle` | 1.0.14 | `MIT` |
| `arrayvec` | 0.7.8 | `MIT` |
| `async-trait` | 0.1.91 | `MIT` |
| `atomic-waker` | 1.1.2 | `MIT` |
| `autocfg` | 1.5.1 | `MIT` |
| `aws-lc-sys` | 0.42.0 | `MIT` |
| `base64` | 0.22.1 | `MIT` |
| `base64ct` | 1.8.3 | `MIT` |
| `bigdecimal` | 0.4.10 | `MIT` |
| `bit-set` | 0.8.0 | `MIT` |
| `bit-vec` | 0.8.0 | `MIT` |
| `bitflags` | 2.13.0 | `MIT` |
| `block-buffer` | 0.10.4 | `MIT` |
| `block-buffer` | 0.12.1 | `MIT` |
| `bnum` | 0.13.0 | `MIT` |
| `bon` | 3.9.3 | `MIT` |
| `bon-macros` | 3.9.3 | `MIT` |
| `bstr` | 1.12.3 | `MIT` |
| `byteorder` | 1.5.0 | `MIT` |
| `bytes` | 1.12.1 | `MIT` |
| `cc` | 1.2.66 | `MIT` |
| `cexpr` | 0.6.0 | `MIT` |
| `cfg-if` | 1.0.4 | `MIT` |
| `cfg_aliases` | 0.2.1 | `MIT` |
| `chacha20` | 0.10.1 | `MIT` |
| `chrono` | 0.4.45 | `MIT` |
| `cityhash-rs` | 1.0.1 | `MIT` |
| `clap` | 4.6.1 | `MIT` |
| `clap_builder` | 4.6.0 | `MIT` |
| `clap_lex` | 1.1.0 | `MIT` |
| `clickhouse` | 0.15.1 | `MIT` |
| `clickhouse-macros` | 0.3.0 | `MIT` |
| `clickhouse-types` | 0.1.2 | `MIT` |
| `cmake` | 0.1.58 | `MIT` |
| `const-oid` | 0.10.2 | `MIT` |
| `const-oid` | 0.9.6 | `MIT` |
| `core-foundation` | 0.10.1 | `MIT` |
| `core-foundation-sys` | 0.8.7 | `MIT` |
| `core_affinity` | 0.8.3 | `MIT` |
| `cpufeatures` | 0.2.17 | `MIT` |
| `cpufeatures` | 0.3.0 | `MIT` |
| `crc-fast` | 1.10.0 | `MIT` |
| `crc32fast` | 1.5.0 | `MIT` |
| `crossbeam-channel` | 0.5.16 | `MIT` |
| `crossbeam-epoch` | 0.9.20 | `MIT` |
| `crossbeam-utils` | 0.8.21 | `MIT` |
| `crypto-common` | 0.1.7 | `MIT` |
| `crypto-common` | 0.2.2 | `MIT` |
| `curve25519-dalek-derive` | 0.1.1 | `MIT` |
| `darling` | 0.23.0 | `MIT` |
| `darling_core` | 0.23.0 | `MIT` |
| `darling_macro` | 0.23.0 | `MIT` |
| `dashmap` | 6.2.1 | `MIT` |
| `data-encoding` | 2.11.0 | `MIT` |
| `der` | 0.7.10 | `MIT` |
| `deranged` | 0.5.8 | `MIT` |
| `digest` | 0.10.7 | `MIT` |
| `digest` | 0.11.3 | `MIT` |
| `displaydoc` | 0.2.6 | `MIT` |
| `ed25519` | 2.2.3 | `MIT` |
| `either` | 1.16.0 | `MIT` |
| `equivalent` | 1.0.2 | `MIT` |
| `errno` | 0.3.14 | `MIT` |
| `evmap` | 11.0.0 | `MIT` |
| `fastrand` | 2.4.1 | `MIT` |
| `find-msvc-tools` | 0.1.9 | `MIT` |
| `flate2` | 1.1.9 | `MIT` |
| `float-cmp` | 0.10.0 | `MIT` |
| `fnv` | 1.0.7 | `MIT` |
| `form_urlencoded` | 1.2.2 | `MIT` |
| `fs_extra` | 1.3.0 | `MIT` |
| `futures` | 0.3.32 | `MIT` |
| `futures-channel` | 0.3.32 | `MIT` |
| `futures-core` | 0.3.32 | `MIT` |
| `futures-executor` | 0.3.32 | `MIT` |
| `futures-io` | 0.3.32 | `MIT` |
| `futures-macro` | 0.3.32 | `MIT` |
| `futures-sink` | 0.3.32 | `MIT` |
| `futures-task` | 0.3.32 | `MIT` |
| `futures-util` | 0.3.32 | `MIT` |
| `generic-array` | 0.14.7 | `MIT` |
| `getrandom` | 0.2.17 | `MIT` |
| `getrandom` | 0.3.4 | `MIT` |
| `getrandom` | 0.4.3 | `MIT` |
| `glob` | 0.3.3 | `MIT` |
| `h2` | 0.4.16 | `MIT` |
| `halfbrown` | 0.4.0 | `MIT` |
| `hashbag` | 0.1.13 | `MIT` |
| `hashbrown` | 0.14.5 | `MIT` |
| `hashbrown` | 0.16.1 | `MIT` |
| `hashbrown` | 0.17.1 | `MIT` |
| `heck` | 0.4.1 | `MIT` |
| `heck` | 0.5.0 | `MIT` |
| `hex` | 0.4.3 | `MIT` |
| `higher-kinded-types` | 0.2.1 | `MIT` |
| `http` | 1.4.2 | `MIT` |
| `http-body` | 1.0.1 | `MIT` |
| `http-body-util` | 0.1.4 | `MIT` |
| `httparse` | 1.10.1 | `MIT` |
| `httpdate` | 1.0.3 | `MIT` |
| `humantime` | 2.4.0 | `MIT` |
| `humantime-serde` | 1.1.1 | `MIT` |
| `hybrid-array` | 0.4.13 | `MIT` |
| `hyper` | 1.10.1 | `MIT` |
| `hyper-rustls` | 0.27.9 | `MIT` |
| `hyper-util` | 0.1.20 | `MIT` |
| `iana-time-zone` | 0.1.65 | `MIT` |
| `ident_case` | 1.0.1 | `MIT` |
| `idna` | 1.1.0 | `MIT` |
| `idna_adapter` | 1.2.2 | `MIT` |
| `indexmap` | 2.14.0 | `MIT` |
| `ipnet` | 2.12.0 | `MIT` |
| `itertools` | 0.13.0 | `MIT` |
| `itertools` | 0.15.0 | `MIT` |
| `itoa` | 1.0.18 | `MIT` |
| `jobserver` | 0.1.34 | `MIT` |
| `lazy_static` | 1.5.0 | `MIT` |
| `left-right` | 0.11.7 | `MIT` |
| `libc` | 0.2.186 | `MIT` |
| `libm` | 0.2.16 | `MIT` |
| `libproc` | 0.14.11 | `MIT` |
| `libyaml-rs` | 0.3.0 | `MIT` |
| `libz-sys` | 1.1.29 | `MIT` |
| `linux-raw-sys` | 0.12.1 | `MIT` |
| `lock_api` | 0.4.14 | `MIT` |
| `log` | 0.4.33 | `MIT` |
| `lz4_flex` | 0.11.6 | `MIT` |
| `mach2` | 0.6.0 | `MIT` |
| `macro_rules_attribute` | 0.2.2 | `MIT` |
| `macro_rules_attribute-proc_macro` | 0.2.2 | `MIT` |
| `matchers` | 0.2.0 | `MIT` |
| `md-5` | 0.11.0 | `MIT` |
| `memchr` | 2.8.2 | `MIT` |
| `metrics` | 0.24.6 | `MIT` |
| `metrics-exporter-prometheus` | 0.18.3 | `MIT` |
| `metrics-process` | 2.4.3 | `MIT` |
| `metrics-util` | 0.20.4 | `MIT` |
| `minimal-lexical` | 0.2.1 | `MIT` |
| `miniz_oxide` | 0.8.9 | `MIT` |
| `miniz_oxide` | 0.9.1 | `MIT` |
| `mio` | 1.2.1 | `MIT` |
| `never-say-never` | 6.6.666 | `MIT` |
| `nix` | 0.31.3 | `MIT` |
| `nom` | 7.1.3 | `MIT` |
| `num-bigint` | 0.4.8 | `MIT` |
| `num-conv` | 0.2.2 | `MIT` |
| `num-integer` | 0.1.46 | `MIT` |
| `num-traits` | 0.2.19 | `MIT` |
| `num_cpus` | 1.17.0 | `MIT` |
| `num_enum` | 0.7.6 | `MIT` |
| `num_enum_derive` | 0.7.6 | `MIT` |
| `object_store` | 0.14.1 | `MIT` |
| `once_cell` | 1.21.4 | `MIT` |
| `openssl-probe` | 0.2.1 | `MIT` |
| `openssl-src` | 300.6.1+3.6.3 | `MIT` |
| `openssl-sys` | 0.9.117 | `MIT` |
| `ouroboros` | 0.18.5 | `MIT` |
| `ouroboros_macro` | 0.18.5 | `MIT` |
| `parking_lot` | 0.12.5 | `MIT` |
| `parking_lot_core` | 0.9.12 | `MIT` |
| `paste` | 1.0.15 | `MIT` |
| `pem-rfc7468` | 0.7.0 | `MIT` |
| `percent-encoding` | 2.3.2 | `MIT` |
| `pin-project` | 1.1.13 | `MIT` |
| `pin-project-internal` | 1.1.13 | `MIT` |
| `pin-project-lite` | 0.2.17 | `MIT` |
| `pkcs8` | 0.10.2 | `MIT` |
| `pkg-config` | 0.3.33 | `MIT` |
| `polonius-the-crab` | 0.5.0 | `MIT` |
| `portable-atomic` | 1.13.1 | `MIT` |
| `powerfmt` | 0.2.0 | `MIT` |
| `ppv-lite86` | 0.2.21 | `MIT` |
| `prettyplease` | 0.2.37 | `MIT` |
| `proc-macro-crate` | 3.5.0 | `MIT` |
| `proc-macro2` | 1.0.106 | `MIT` |
| `proc-macro2-diagnostics` | 0.10.1 | `MIT` |
| `procfs` | 0.18.0 | `MIT` |
| `procfs-core` | 0.18.0 | `MIT` |
| `proptest` | 1.11.0 | `MIT` |
| `quanta` | 0.12.6 | `MIT` |
| `quick-error` | 1.2.3 | `MIT` |
| `quick-xml` | 0.41.0 | `MIT` |
| `quote` | 1.0.46 | `MIT` |
| `rand` | 0.10.2 | `MIT` |
| `rand` | 0.8.6 | `MIT` |
| `rand` | 0.9.4 | `MIT` |
| `rand_chacha` | 0.3.1 | `MIT` |
| `rand_chacha` | 0.9.0 | `MIT` |
| `rand_core` | 0.10.1 | `MIT` |
| `rand_core` | 0.6.4 | `MIT` |
| `rand_core` | 0.9.5 | `MIT` |
| `rand_xorshift` | 0.4.0 | `MIT` |
| `rand_xoshiro` | 0.7.0 | `MIT` |
| `rapidhash` | 4.5.1 | `MIT` |
| `raw-cpuid` | 11.6.0 | `MIT` |
| `rdkafka` | 0.39.0 | `MIT` |
| `rdkafka-sys` | 4.10.0+2.12.1 | `MIT` |
| `ref-cast` | 1.0.25 | `MIT` |
| `ref-cast-impl` | 1.0.25 | `MIT` |
| `regex` | 1.12.4 | `MIT` |
| `regex-automata` | 0.4.14 | `MIT` |
| `regex-lite` | 0.1.9 | `MIT` |
| `regex-syntax` | 0.8.11 | `MIT` |
| `reqwest` | 0.13.4 | `MIT` |
| `rlimit` | 0.11.0 | `MIT` |
| `rust_decimal` | 1.42.1 | `MIT` |
| `rustc-hash` | 2.1.3 | `MIT` |
| `rustc_version` | 0.4.1 | `MIT` |
| `rustix` | 1.1.4 | `MIT` |
| `rustls` | 0.23.41 | `MIT` |
| `rustls-native-certs` | 0.8.4 | `MIT` |
| `rustls-pki-types` | 1.15.0 | `MIT` |
| `rustls-platform-verifier` | 0.7.0 | `MIT` |
| `rustversion` | 1.0.22 | `MIT` |
| `rusty-fork` | 0.3.1 | `MIT` |
| `same-file` | 1.0.6 | `MIT` |
| `schema_registry_converter` | 4.10.0 | `MIT` |
| `scopeguard` | 1.2.0 | `MIT` |
| `security-framework` | 3.7.0 | `MIT` |
| `security-framework-sys` | 2.17.0 | `MIT` |
| `semver` | 1.0.28 | `MIT` |
| `serde` | 1.0.229 | `MIT` |
| `serde_bytes` | 0.11.19 | `MIT` |
| `serde_core` | 1.0.229 | `MIT` |
| `serde_derive` | 1.0.229 | `MIT` |
| `serde_derive_internals` | 0.29.1 | `MIT` |
| `serde_json` | 1.0.151 | `MIT` |
| `serde_nanos` | 0.1.4 | `MIT` |
| `serde_path_to_error` | 0.1.20 | `MIT` |
| `serde_repr` | 0.1.21 | `MIT` |
| `serde_urlencoded` | 0.7.1 | `MIT` |
| `sha2` | 0.10.9 | `MIT` |
| `sha2` | 0.11.0 | `MIT` |
| `sharded-slab` | 0.1.7 | `MIT` |
| `shlex` | 1.3.0 | `MIT` |
| `shlex` | 2.0.1 | `MIT` |
| `signal-hook-registry` | 1.4.8 | `MIT` |
| `signatory` | 0.27.1 | `MIT` |
| `signature` | 2.2.0 | `MIT` |
| `simd-adler32` | 0.3.9 | `MIT` |
| `simd-json` | 0.17.3 | `MIT` |
| `simdutf8` | 0.1.5 | `MIT` |
| `slab` | 0.4.12 | `MIT` |
| `smallvec` | 1.15.2 | `MIT` |
| `socket2` | 0.6.4 | `MIT` |
| `spin` | 0.10.1 | `MIT` |
| `spki` | 0.7.3 | `MIT` |
| `stable_deref_trait` | 1.2.1 | `MIT` |
| `static_assertions` | 1.1.0 | `MIT` |
| `strsim` | 0.11.1 | `MIT` |
| `strum` | 0.28.0 | `MIT` |
| `strum_macros` | 0.28.0 | `MIT` |
| `syn` | 2.0.118 | `MIT` |
| `syn` | 3.0.3 | `MIT` |
| `synstructure` | 0.13.2 | `MIT` |
| `tempfile` | 3.27.0 | `MIT` |
| `thiserror` | 2.0.19 | `MIT` |
| `thiserror-impl` | 2.0.19 | `MIT` |
| `thread_local` | 1.1.9 | `MIT` |
| `time` | 0.3.54 | `MIT` |
| `time-core` | 0.1.9 | `MIT` |
| `time-macros` | 0.2.32 | `MIT` |
| `tokio` | 1.53.1 | `MIT` |
| `tokio-macros` | 2.7.0 | `MIT` |
| `tokio-rustls` | 0.26.4 | `MIT` |
| `tokio-stream` | 0.1.18 | `MIT` |
| `tokio-util` | 0.7.18 | `MIT` |
| `tokio-websockets` | 0.10.1 | `MIT` |
| `toml_datetime` | 1.1.1+spec-1.1.0 | `MIT` |
| `toml_edit` | 0.25.12+spec-1.1.0 | `MIT` |
| `toml_parser` | 1.1.2+spec-1.1.0 | `MIT` |
| `tower` | 0.5.3 | `MIT` |
| `tower-http` | 0.6.11 | `MIT` |
| `tower-layer` | 0.3.3 | `MIT` |
| `tower-service` | 0.3.3 | `MIT` |
| `tracing` | 0.1.44 | `MIT` |
| `tracing-attributes` | 0.1.31 | `MIT` |
| `tracing-core` | 0.1.36 | `MIT` |
| `tracing-serde` | 0.2.0 | `MIT` |
| `tracing-subscriber` | 0.3.23 | `MIT` |
| `try-lock` | 0.2.5 | `MIT` |
| `tryhard` | 0.5.2 | `MIT` |
| `twox-hash` | 2.1.3 | `MIT` |
| `typenum` | 1.20.1 | `MIT` |
| `unarray` | 0.1.4 | `MIT` |
| `unicode-ident` | 1.0.24 | `MIT` |
| `url` | 2.5.8 | `MIT` |
| `utf8_iter` | 1.0.4 | `MIT` |
| `uuid` | 1.24.0 | `MIT` |
| `value-trait` | 0.12.2 | `MIT` |
| `vcpkg` | 0.2.15 | `MIT` |
| `version_check` | 0.9.5 | `MIT` |
| `wait-timeout` | 0.2.1 | `MIT` |
| `walkdir` | 2.5.0 | `MIT` |
| `want` | 0.3.1 | `MIT` |
| `winnow` | 1.0.3 | `MIT` |
| `yaml_serde` | 0.10.4 | `MIT` |
| `yansi` | 1.0.1 | `MIT` |
| `zerocopy` | 0.8.52 | `MIT` |
| `zeroize` | 1.9.0 | `MIT` |
| `zmij` | 1.0.21 | `MIT` |
| `zstd` | 0.13.3 | `MIT` |
| `zstd-safe` | 7.2.4 | `MIT` |
| `zstd-sys` | 2.0.16+zstd.1.5.7 | `MIT` |
| `icu_collections` | 2.2.0 | `Unicode-3.0` |
| `icu_locale_core` | 2.2.0 | `Unicode-3.0` |
| `icu_normalizer` | 2.2.0 | `Unicode-3.0` |
| `icu_normalizer_data` | 2.2.0 | `Unicode-3.0` |
| `icu_properties` | 2.2.0 | `Unicode-3.0` |
| `icu_properties_data` | 2.2.0 | `Unicode-3.0` |
| `icu_provider` | 2.2.0 | `Unicode-3.0` |
| `litemap` | 0.8.2 | `Unicode-3.0` |
| `potential_utf` | 0.1.5 | `Unicode-3.0` |
| `tinystr` | 0.8.3 | `Unicode-3.0` |
| `unicode-ident` | 1.0.24 | `Unicode-3.0` |
| `writeable` | 0.6.3 | `Unicode-3.0` |
| `yoke` | 0.8.3 | `Unicode-3.0` |
| `yoke-derive` | 0.8.2 | `Unicode-3.0` |
| `zerofrom` | 0.1.8 | `Unicode-3.0` |
| `zerofrom-derive` | 0.1.7 | `Unicode-3.0` |
| `zerotrie` | 0.2.4 | `Unicode-3.0` |
| `zerovec` | 0.11.6 | `Unicode-3.0` |
| `zerovec-derive` | 0.11.3 | `Unicode-3.0` |
| `foldhash` | 0.2.0 | `Zlib` |
