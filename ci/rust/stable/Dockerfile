# The Rust toolchain image `spate-kafka`'s `tls_system_ca` builds its client in
# against the system OpenSSL, and runs that client in.
#
# It must ship libssl-dev, libssl3, ca-certificates and the `openssl` tool.
# Dependabot takes Rust releases on Debian 12. ../README.md has the account.

FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e
