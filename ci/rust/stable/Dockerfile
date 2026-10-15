# The Rust toolchain image `spate-kafka`'s `tls_system_ca` builds its client in
# against the system OpenSSL, and runs that client in.
#
# It must ship libssl-dev, libssl3, ca-certificates and the `openssl` tool.
# Dependabot takes Rust releases on Debian 12. ../README.md has the account.

FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6
