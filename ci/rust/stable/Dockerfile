# The Rust toolchain image `spate-kafka`'s `tls_system_ca` builds its client in
# against the system OpenSSL, and runs that client in.
#
# It must ship libssl-dev, libssl3, ca-certificates and the `openssl` tool.
# Dependabot takes Rust releases on Debian 12. ../README.md has the account.

FROM rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea
