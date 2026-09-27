**Breaking:** **The schema registry trusts a private CA with `registry.tls.root_ca`** (`spate-avro`)

The Avro deserializer's `registry.tls.root_ca` setting names a PEM bundle of
root CAs. The registry client trusts them in addition to the system trust
store. In previous versions a registry whose certificate a private CA signed was
trusted only through the host's store, or through `SSL_CERT_FILE` or
`SSL_CERT_DIR`, which on Linux and other Unix systems replace that store.

The setting requires an `https://` registry URL. A file that cannot be read,
holds no certificate or holds a malformed one fails the deserializer at
startup.

`RegistrySection` is `#[non_exhaustive]` and gains a `tls` field of the new
type `TlsSection`. Code that builds a `RegistrySection { .. }` literal no longer
compiles. Build it with `RegistrySection::new(url)` and set `username`,
`password` and `tls` as fields.
