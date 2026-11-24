**`TlsSection` builds without the `tls-roots` feature** (`spate-core`)

`spate_core::tls::TlsSection`, also reachable as `spate::tls::TlsSection`, is available in every build. A connector can now take the `tls:` key without compiling the trust-root loader, which still needs `tls-roots`. `TlsSection` also derives `Eq`.
