**Cleartext credential warning** (`spate-core`)

`SectionPath::warn_cleartext_credential` logs one warning when a configured credential will be sent without TLS. The line names the credential's key and the setting that leaves the connection unencrypted, and never the value. `SectionPath::deserializer` gives the path of a `deserializer:` section, beside `source`, `sink` and `feed`.
