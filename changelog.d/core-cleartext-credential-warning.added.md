**Cleartext credential warning** (`spate-core`)

`SectionPath::warn_cleartext_credential` logs one warning when a configured credential is sent without TLS. The line names the credential's key and the setting that leaves the connection unencrypted. Both arguments are `'static` strings, so no value read from the config reaches the line. `SectionPath::deserializer` gives the path of a `deserializer:` section, beside `source`, `sink` and `feed`.
